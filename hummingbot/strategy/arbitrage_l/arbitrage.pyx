# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp
# distutils: extra_compile_args=-Wno-psabi
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False

import logging
from decimal import Decimal
from typing import List, Tuple, Optional
from libc.stdint cimport int64_t
from libc.math cimport fabs
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp.vector cimport vector
cimport cython
from libcpp.set cimport set as cpp_set
from cython.operator cimport(
    dereference as deref,
    postincrement as inc,
    address as address_of,
)

from hummingbot.core.clock cimport Clock

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import TradeType, OrderType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage_l.arbitrage_market_pair import ArbitrageLMarketPair
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.arbitrage_l.arbitrage_config_map import arbitrage_l_config_map
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.OrderBookEntry cimport OrderBookEntry as OrderBookEntryCPP
from hummingbot.strategy.arbitrage_l.position_balancer_handler cimport PositionBalancerHandler

# Constants - Now configurable via init_params
cdef:
    double DEFAULT_MIN_ORDER_USD = 15.0
    double DEFAULT_MAX_ORDER_USD = 1000.0
    double DEFAULT_RATE_CACHE_DURATION = 10.0

    size_t DEFAULT_MAX_TRACKED_ORDERS = 1000
    double DEFAULT_FILLED_ORDER_TIMEOUT = 3600.0  # 1 hour timeout for orders with fills
    double EPSILON = 1e-10
    double QUANTIZATION_EPSILON = 1e-12
    double RATE_LOG_INTERVAL = 300.0
    # NOTE: AGGRESSIVE_REFRESH_INTERVAL is defined in position_balancer_handler.pyx (5.0s default)

cdef class ArbitrageLStrategy(StrategyBase):
    """
    Limit order arbitrage strategy (LIMIT ORDERS ONLY).
    Uses doubles internally for performance, converts to Decimal only for external APIs.
    Places limit orders at worst (furthest) scanned prices for instant fills.
    Cancels unfilled orders after 300s timeout.
    """
    
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 4

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    def __cinit__(self):
        # Initialize defaults so attributes exist even if init_params isn't called yet
        self._market_pairs = []  # Initialize to empty list to avoid NoneType errors
        self._min_profitability = 0.01  # Default 1% (will be overridden in init_params)
        self._logging_options = 0  # No logging by default
        self._status_report_interval = 60.0  # Default 60 seconds
        self._next_trade_delay = 2.0  # Default 2 seconds
        self._order_timeout = 180.0  # Default 3 minutes
        self._filled_order_timeout = 3600.0  # Default 1 hour

        self._min_order_usd = 10.0  # Default minimum order size
        self._max_order_usd = 1000.0  # Default maximum order size
        self._rate_cache_duration = 10.0  # Default rate cache duration
        self._max_tracked_orders = 1000  # Default max tracked orders
        self._use_oracle_conversion_rate = False  # Default no oracle
        self._fixed_base_rate = 1.0  # Default conversion rate
        self._fixed_quote_rate = 1.0  # Default conversion rate
        self._last_global_trade_timestamp = 0.0
        self._last_failure_timestamps = {}
        self._position_balancer = None  # Will be initialized in init_params if enabled
        # Track whether a given order_id has received any fill events
        self._orders_with_fills = set()
        # Track when orders first received fills (for filled order timeout cleanup)
        self._order_fill_timestamps = {}  # order_id -> timestamp when first fill received
        # Keep recent order -> market pair mapping for late events
        self._recent_order_market_pair = {}
        # Track pending limit orders per market tuple - SEPARATE for buy/sell to allow parallel buy+sell
        self._pending_buy_orders_by_market = {}   # market_tuple -> set of buy order_ids
        self._pending_sell_orders_by_market = {}  # market_tuple -> set of sell order_ids
        # Track orders cancelled due to timeout (to avoid cooldown on timeout cancellations)
        self._timeout_cancelled_orders = set()
        # Track position balancer orders (to prevent main strategy from canceling them)
        self._position_balancer_orders = set()
        # Timers and caches
        self._all_markets_ready = False
        self._last_timestamp = 0
        self._status_debounce_until = 0
        self._last_cleanup_timestamp = 0
        self._last_conv_rates_logged = 0
        self._cached_base_rate = 1.0
        self._cached_quote_rate = 1.0
        self._last_rate_update = 0
        # Orchestrated mode flag (for multi-strategy orchestrator optimization)
        self._orchestrated_mode = False
        # Trade counter (strategy-specific, not affected by shared connectors)
        self._total_trades = 0

    def init_params(self,
                    market_pairs: List[ArbitrageLMarketPair],
                    min_profitability: Decimal,
                    logging_options: int = OPTION_LOG_STATUS_REPORT,
                    status_report_interval: float = 60.0,
                    next_trade_delay_interval: float = 2.0,
                    order_timeout: float = 180.0,
                    filled_order_timeout: float = DEFAULT_FILLED_ORDER_TIMEOUT,
                    use_oracle_conversion_rate: bool = False,
                    secondary_to_primary_base_conversion_rate: Decimal = Decimal("1"),
                    secondary_to_primary_quote_conversion_rate: Decimal = Decimal("1"),
                    hb_app_notification: bool = False,
                    min_order_usd: float = DEFAULT_MIN_ORDER_USD,
                    max_order_usd: float = DEFAULT_MAX_ORDER_USD,
                    rate_cache_duration: float = DEFAULT_RATE_CACHE_DURATION,

                    max_tracked_orders: int = DEFAULT_MAX_TRACKED_ORDERS,
                    # Position balancer - buy-in configuration
                    buy_in_enabled: bool = True,
                    buy_in_target_usd: float = 1100.0,
                    buy_in_spread_pct: object = "min",  # float or 'min'
                    # Position balancer - sell-off configuration
                    sell_off_enabled: bool = False,
                    sell_off_target_usd: float = 3000.0,
                    sell_off_spread_pct: object = "min",  # float or 'min'
                    # Position balancer - order management
                    position_balancer_refresh_interval: float = 60.0,
                    position_balancer_order_size_usd: float = 100.0,
                    orchestrated_mode: bool = False):
        """Initialize arbitrage strategy with configurable parameters"""
        
        if not market_pairs:
            raise ValueError("market_pairs must not be empty.")
        
        # Core configuration - store as doubles for performance
        self._market_pairs = market_pairs
        self._min_profitability = float(min_profitability)
        self._logging_options = logging_options
        
        # Timing configuration
        self._status_report_interval = status_report_interval
        self._next_trade_delay = next_trade_delay_interval
        self._order_timeout = order_timeout
        self._filled_order_timeout = filled_order_timeout

        
        # Thresholds
        self._min_order_usd = min_order_usd
        self._max_order_usd = max_order_usd
        self._rate_cache_duration = rate_cache_duration
        self._max_tracked_orders = max_tracked_orders
        
        # State tracking
        self._all_markets_ready = False
        self._last_timestamp = 0
        self._status_debounce_until = 0
        self._last_global_trade_timestamp = 0.0
        self._last_failure_timestamps = {}
        self._last_cleanup_timestamp = 0
        self._last_conv_rates_logged = 0
        
        # Conversion configuration - store as doubles
        self._use_oracle_conversion_rate = use_oracle_conversion_rate
        if not use_oracle_conversion_rate:
            self._fixed_base_rate = float(secondary_to_primary_base_conversion_rate)
            self._fixed_quote_rate = float(secondary_to_primary_quote_conversion_rate)
        else:
            self._fixed_base_rate = 1.0
            self._fixed_quote_rate = 1.0
        
        
        # Cache initialization
        self._cached_base_rate = 1.0
        self._cached_quote_rate = 1.0
        self._last_rate_update = 0
        
        
        
        # Clear order tracking
        self._order_timestamps.clear()
        self._completed_orders.clear()
        try:
            self._orders_with_fills.clear()
        except Exception:
            pass
        try:
            self._order_fill_timestamps.clear()
        except Exception:
            pass
        try:
            self._recent_order_market_pair.clear()
        except Exception:
            pass
        try:
            self._pending_buy_orders_by_market.clear()
        except Exception:
            pass
        try:
            self._pending_sell_orders_by_market.clear()
        except Exception:
            pass
        try:
            self._timeout_cancelled_orders.clear()
        except Exception:
            pass
        try:
            self._position_balancer_orders.clear()
        except Exception:
            pass

        # Position balancer handler (create if position balancer targets configured)
        # Create handler even if both modes disabled to allow runtime enabling
        if buy_in_target_usd > 0 or sell_off_target_usd > 0:
            self._position_balancer = PositionBalancerHandler(
                self,
                buy_in_enabled,
                buy_in_target_usd,
                buy_in_spread_pct,
                sell_off_enabled,
                sell_off_target_usd,
                sell_off_spread_pct,
                position_balancer_refresh_interval,
                position_balancer_order_size_usd)  # aggressive_refresh uses default from position_balancer_handler.pyx
        else:
            self._position_balancer = None

        # Orchestration mode (for multi-strategy orchestrator optimization)
        self._orchestrated_mode = orchestrated_mode

        # Optimization: Reserve capacity to avoid reallocations
        self._reusable_arb_opps.reserve(20)

        # Validate and add markets
        self._validate_configuration()
        
        cdef set all_markets = {
            market
            for market_pair in self._market_pairs
            for market in [market_pair.first.market, market_pair.second.market]
        }
        self.c_add_markets(list(all_markets))

    cdef void _validate_configuration(self):
        """Validate strategy configuration parameters"""
        if self._min_profitability < -1.0:
            raise ValueError("min_profitability cannot be less than -100%")
        if self._order_timeout <= 0:
            raise ValueError("order_timeout must be positive")
        if self._next_trade_delay < 0:
            raise ValueError("next_trade_delay cannot be negative")

    cdef c_stop(self, Clock clock):
        StrategyBase.c_stop(self, clock)
        self._order_timestamps.clear()
        self._completed_orders.clear()
        try:
            self._orders_with_fills.clear()
        except Exception:
            pass
        try:
            self._order_fill_timestamps.clear()
        except Exception:
            pass
        try:
            self._recent_order_market_pair.clear()
        except Exception:
            pass
        try:
            self._pending_buy_orders_by_market.clear()
        except Exception:
            pass
        try:
            self._pending_sell_orders_by_market.clear()
        except Exception:
            pass
        try:
            self._timeout_cancelled_orders.clear()
        except Exception:
            pass
        try:
            self._position_balancer_orders.clear()
        except Exception:
            pass
        self._position_balancer = None

    

    @property
    def min_profitability(self) -> Decimal:
        return Decimal(str(self._min_profitability))

    @min_profitability.setter
    def min_profitability(self, value: Decimal):
        self._min_profitability = float(value)

    @property
    def total_trades(self) -> int:
        return self._total_trades

    @property
    def tracked_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    @property
    def tracked_market_orders(self) -> List[Tuple[ExchangeBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    cdef inline string _to_cpp_str(self, object py_str):
        """Convert a Python str to libcpp.string efficiently."""
        return (<str>py_str).encode('utf-8')

    cdef inline double _conv_rate(self, object buy_market_tuple, object sell_market_tuple):
        """Fast path: sell->buy conversion; returns 1.0 for no-op, avoids oracle when disabled."""
        if (buy_market_tuple.base_asset == sell_market_tuple.base_asset and
            buy_market_tuple.quote_asset == sell_market_tuple.quote_asset):
            return 1.0
        if not self._use_oracle_conversion_rate and self._fixed_base_rate == 1.0 and self._fixed_quote_rate == 1.0:
            return 1.0
        return self.c_get_market_to_market_conversion_rate(buy_market_tuple, sell_market_tuple)

    cdef object c_safe_quantize_order_amount(self,
                                              ExchangeBase market,
                                              str trading_pair,
                                              object amount,
                                              object price):
        """
        Safe quantization with fallback.
        Tries c_quantize_order_amount with price, falls back to quantize_order_amount without price.
        """
        try:
            return market.c_quantize_order_amount(trading_pair, amount, price)
        except Exception:
            return market.quantize_order_amount(trading_pair, amount)

    cdef void c_remove_pending_order(self, object market_tuple, str order_id, str context="order"):
        """
        Remove order from pending buy/sell order tracking.
        Checks both pending buy and sell dicts and cleans up empty entries.

        Args:
            market_tuple: Market pair tuple to remove order from
            order_id: Order ID to remove
            context: Context string for error logging (e.g., "completed", "cancelled")
        """
        try:
            if market_tuple is not None:
                # Try removing from pending buy orders
                pending_buys = self._pending_buy_orders_by_market.get(market_tuple)
                if pending_buys is not None and order_id in pending_buys:
                    pending_buys.discard(order_id)
                    if len(pending_buys) == 0:
                        self._pending_buy_orders_by_market.pop(market_tuple, None)

                # Try removing from pending sell orders
                pending_sells = self._pending_sell_orders_by_market.get(market_tuple)
                if pending_sells is not None and order_id in pending_sells:
                    pending_sells.discard(order_id)
                    if len(pending_sells) == 0:
                        self._pending_sell_orders_by_market.pop(market_tuple, None)
        except Exception as e:
            if context:
                self.logger().warning(f"Failed to remove {context} order from pending tracking: {e}")

    cdef double c_get_conversion_rate(self, bint is_base_asset):
        """Get conversion rate for base or quote asset"""
        if not self._use_oracle_conversion_rate:
            return self._fixed_base_rate if is_base_asset else self._fixed_quote_rate
        
        cdef double current_time = self._current_timestamp
        
        # Update cache if expired
        if current_time - self._last_rate_update > self._rate_cache_duration:
            self.c_update_conversion_rates()
        
        return self._cached_base_rate if is_base_asset else self._cached_quote_rate
    
    cdef double c_get_market_to_market_conversion_rate(self, object buy_market_tuple, object sell_market_tuple):
        """Conversion to express sell market prices in buy market quote units."""
        # Fast path: if assets match, no conversion needed
        if (buy_market_tuple.base_asset == sell_market_tuple.base_asset and
            buy_market_tuple.quote_asset == sell_market_tuple.quote_asset):
            return 1.0
        
        cdef double base_conv = 1.0
        cdef double quote_conv = 1.0
        cdef double base_rate = 1.0
        cdef double quote_rate = 1.0
        cdef object primary_first = self._market_pairs[0].first
        cdef object primary_second = self._market_pairs[0].second

        # Base asset conversion (sell base -> buy base)
        if buy_market_tuple.base_asset != sell_market_tuple.base_asset:
            if (buy_market_tuple.base_asset == primary_first.base_asset and
                sell_market_tuple.base_asset == primary_second.base_asset):
                base_conv = self.c_get_conversion_rate(True)
            elif (buy_market_tuple.base_asset == primary_second.base_asset and
                  sell_market_tuple.base_asset == primary_first.base_asset):
                base_rate = self.c_get_conversion_rate(True)
                base_conv = 1.0 / base_rate if base_rate != 0 else 0.0
            else:
                if self._use_oracle_conversion_rate:
                    base_conv = float(RateOracle.get_instance().get_pair_rate(
                        f"{sell_market_tuple.base_asset}-{buy_market_tuple.base_asset}"))
                else:
                    base_conv = self._fixed_base_rate

        # Quote asset conversion (sell quote -> buy quote)
        if buy_market_tuple.quote_asset != sell_market_tuple.quote_asset:
            if (buy_market_tuple.quote_asset == primary_first.quote_asset and
                sell_market_tuple.quote_asset == primary_second.quote_asset):
                quote_conv = self.c_get_conversion_rate(False)
            elif (buy_market_tuple.quote_asset == primary_second.quote_asset and
                  sell_market_tuple.quote_asset == primary_first.quote_asset):
                quote_rate = self.c_get_conversion_rate(False)
                quote_conv = 1.0 / quote_rate if quote_rate != 0 else 0.0
            else:
                if self._use_oracle_conversion_rate:
                    quote_conv = float(RateOracle.get_instance().get_pair_rate(
                        f"{sell_market_tuple.quote_asset}-{buy_market_tuple.quote_asset}"))
                else:
                    quote_conv = self._fixed_quote_rate

        return quote_conv / base_conv if base_conv != 0 else 0.0
    
    cdef void c_update_conversion_rates(self):
        """Update cached conversion rates efficiently"""
        if not self._use_oracle_conversion_rate:
            # Use fixed rates
            self._cached_base_rate = self._fixed_base_rate
            self._cached_quote_rate = self._fixed_quote_rate
        else:
            # Get from oracle only if assets differ
            market_pair = self._market_pairs[0]
            
            # Base asset conversion
            if market_pair.first.base_asset != market_pair.second.base_asset:
                base_pair = f"{market_pair.second.base_asset}-{market_pair.first.base_asset}"
                self._cached_base_rate = float(RateOracle.get_instance().get_pair_rate(base_pair))
            else:
                self._cached_base_rate = 1.0
            
            # Quote asset conversion  
            if market_pair.first.quote_asset != market_pair.second.quote_asset:
                quote_pair = f"{market_pair.second.quote_asset}-{market_pair.first.quote_asset}"
                self._cached_quote_rate = float(RateOracle.get_instance().get_pair_rate(quote_pair))
            else:
                self._cached_quote_rate = 1.0
        
        self._last_rate_update = self._current_timestamp

    cdef tuple c_build_unique_tuples_assets_and_balance_map(self):
        """Build unique market tuples, assets dataframe and balance map uniformly for status/control paths."""
        cdef:
            list unique_tuples = []
            set seen_keys = set()
            object t
            object mp
            tuple key
            dict balance_map = {}
            object assets_df
            object rec
            str exch
            str asset_name
            object avail
        for mp in self._market_pairs:
            for t in [mp.first, mp.second]:
                key = (t.market.name, t.trading_pair)
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_tuples.append(t)
        assets_df = self.wallet_balance_data_frame(unique_tuples)
        try:
            for rec in assets_df.to_dict("records"):
                exch = str(rec.get("Exchange", ""))
                asset_name = str(rec.get("Asset", ""))
                avail = rec.get("Available Balance", 0)
                balance_map[(exch, asset_name)] = float(avail)
        except Exception:
            balance_map = {}
        return (unique_tuples, assets_df, balance_map)

    cdef inline bint c_books_ready_for_direction(self, object buy_market_tuple, object sell_market_tuple):
        """Cheap non-throwing readiness gate: buy side must have asks, sell side must have bids."""
        cdef ExchangeBase buy_ex = buy_market_tuple.market
        cdef ExchangeBase sell_ex = sell_market_tuple.market
        cdef OrderBook buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        cdef OrderBook sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)
        return (buy_ob._ask_book.size() > 0) and (sell_ob._bid_book.size() > 0)

    def format_status(self) -> str:
        """Format strategy status for display"""
        cdef:
            list lines = []
            list warning_lines = []
            list unique_tuples = []
            set seen_keys = set()
            object t
            object mp
            tuple prof
            double prof_buy_sell
            double best_prof = -1.0
            object best_pair = None
            list prof_lines = []
            tuple key

        try:
            # Aggregate unique market tuples across all ordered pairs
            for mp in self._market_pairs:
                for t in [mp.first, mp.second]:
                    key = (t.market.name, t.trading_pair)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        unique_tuples.append(t)

            # Warnings for network and balances
            warning_lines.extend(self.network_warning(unique_tuples))

            # Markets and assets snapshots (uniform builder)
            unique_tuples, assets_df, balance_map = self.c_build_unique_tuples_assets_and_balance_map()
            markets_df = self.market_status_data_frame(unique_tuples)
            lines.extend(["", "  Markets:"] + ["    " + line for line in str(markets_df).split("\n")])
            lines.extend(["", "  Assets:"] + ["    " + line for line in str(assets_df).split("\n")])

            # Profitability snapshot (buy first -> sell second for each ordered pair)
            lines.extend(["", "  Profitability snapshot (without fees):"])
            for mp in self._market_pairs:
                prof = self.c_calculate_profitability(mp)
                prof_buy_sell = prof[1] * 100  # buy first, sell second
                prof_lines.append(
                    f"    buy-{mp.first.market.name} sell-{mp.second.market.name}: {prof_buy_sell:+.4f}%")
                if prof[1] > best_prof:
                    best_prof = prof[1]
                    best_pair = mp

            if prof_lines:
                # Limit output if too many pairs
                max_lines = 12
                lines.extend(prof_lines[:max_lines])
                if len(prof_lines) > max_lines:
                    lines.append(f"    ... and {len(prof_lines) - max_lines} more pairs")

            if best_pair is not None:
                lines.append(
                    f"    best: buy-{best_pair.first.market.name} sell-{best_pair.second.market.name} -> {best_prof * 100:+.4f}%")

            # Position balancer status (delegate to handler)
            if self._position_balancer is not None:
                lines.extend(self._position_balancer.get_status_lines(unique_tuples, balance_map))

            # Pending orders
            if self.tracked_limit_orders or self.tracked_market_orders:
                lines.extend(["", "  Pending orders:"])
                total = len(self.tracked_limit_orders) + len(self.tracked_market_orders)
                lines.append(f"    Total: {total}")
            else:
                lines.extend(["", "  No pending orders."])

            warning_lines.extend(self.balance_warning(unique_tuples))

            if warning_lines:
                lines.extend(["", "  *** WARNINGS ***"] + warning_lines)
                
        except Exception as e:
            lines.append(f"  Error formatting status: {e}")

        return "\n".join(lines)

    cdef c_tick(self, double timestamp):
        """Main strategy tick - scan all ordered pairs and execute the best one"""
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report = ((current_tick > last_tick) and
                                  (self._logging_options & self.OPTION_LOG_STATUS_REPORT) and
                                  (timestamp >= self._status_debounce_until))
            object best_buy = None
            object best_sell = None
            tuple best_result
            double best_profitability = 0.0

        try:
            # Check market readiness 
            if not self._orchestrated_mode:
                # Normal mode: full readiness check with logging
                if not self.c_check_markets_ready(should_report):
                    return
            else:
                # Orchestrated mode: check connectivity but skip redundant logging/buy-in
                if not self.c_check_markets_ready_orchestrated():
                    return
                # Mark ready and do one-time position balancer check if not done yet
                if not self._all_markets_ready:
                    self._all_markets_ready = True
                    if self._position_balancer is not None:
                        self._position_balancer.c_scan_and_mark_completion()

            # Early check: skip orderbook scanning if global cooldown is active
            # This optimization avoids expensive orderbook iterations when we can't trade anyway
            if self._last_global_trade_timestamp > 0:
                time_left = (self._last_global_trade_timestamp +
                            self._next_trade_delay - self._current_timestamp)
                if time_left > 0:
                    # Still in global cooldown - skip orderbook scanning entirely
                    return

            # Find best opportunity across all ordered pairs (buy=first, sell=second)
            for market_pair in self._market_pairs:
                if not self.c_ready_for_new_orders([market_pair.first, market_pair.second]):
                    continue
                # Cheap non-throwing readiness check for direction (buy asks, sell bids)
                if not self.c_books_ready_for_direction(market_pair.first, market_pair.second):
                    continue
                best_result = self.c_find_best_profitable_amount(market_pair.first, market_pair.second)
                if best_result[0] <= 0:
                    continue

                if best_result[1] > best_profitability:
                    best_profitability = <double>best_result[1]
                    best_buy = market_pair.first
                    best_sell = market_pair.second
                    
            # Execute only the globally best profitable opportunity
            if best_buy is not None and best_sell is not None and best_profitability >= self._min_profitability:
                if self._position_balancer is not None and self._position_balancer.is_active:
                    # Try position balancing first (buy-in or sell-off)
                    if self._position_balancer.c_handle_position_balancing(best_buy, best_sell):
                        # Position balancing executed; skip regular arbitrage this tick
                        pass
                    else:
                        self.c_execute_arbitrage(best_buy, best_sell)
                else:
                    self.c_execute_arbitrage(best_buy, best_sell)
            elif self._position_balancer is not None and self._position_balancer.is_active and best_buy is not None and best_sell is not None:
                # Not enough edge for normal arbitrage. Still try position balancing.
                # Attempt with current best direction (respect pending/cool-off)
                placed = False
                if self.c_ready_for_new_orders([best_buy, best_sell]) and self.c_books_ready_for_direction(best_buy, best_sell):
                    placed = self._position_balancer.c_handle_position_balancing(best_buy, best_sell) or False
                # Also attempt reversed only if nothing was placed in current direction
                if (not placed) and self._position_balancer.is_active:
                    if self.c_ready_for_new_orders([best_sell, best_buy]) and self.c_books_ready_for_direction(best_sell, best_buy):
                        self._position_balancer.c_handle_position_balancing(best_sell, best_buy)
            elif self._position_balancer is not None and self._position_balancer.is_active and best_buy is None:
                # No arbitrageable pair found. Proactively scan all pairs for position balancing.
                for market_pair in self._market_pairs:
                    if self.c_ready_for_new_orders([market_pair.first, market_pair.second]) and self.c_books_ready_for_direction(market_pair.first, market_pair.second):
                        if self._position_balancer.c_handle_position_balancing(market_pair.first, market_pair.second):
                            break
            
            # Check ALL pending orders for timeouts every tick
            # This ensures canceled orders are detected even if their market is no longer being considered
            self.c_check_all_order_timeouts()

            # Check filled orders for extended timeout
            self.c_check_filled_order_timeouts()

            # Periodic maintenance
            if timestamp - self._last_cleanup_timestamp > 60.0:
                self.c_cleanup_old_orders()
                self._last_cleanup_timestamp = timestamp
           # Log conversion rates periodically if using oracle
            if (self._use_oracle_conversion_rate and 
                timestamp - self._last_conv_rates_logged > RATE_LOG_INTERVAL):
                self.c_log_conversion_rates()
                self._last_conv_rates_logged = timestamp

        finally:
            self._last_timestamp = timestamp

    cdef bint c_check_markets_ready(self, bint should_report):
        """Check if all markets are ready for trading"""
        if not self._all_markets_ready:
            # OPTIMIZATION: Use generator instead of list to avoid allocation
            self._all_markets_ready = all(market.ready for market in self._sb_markets)
            if not self._all_markets_ready:
                if should_report:
                    self.logger().warning("Markets not ready. No arbitrage trading permitted.")
                return False
            else:
                if should_report:
                    self.logger().info("Markets ready. Trading started.")
                    # Debounce status logs for a short window to let connectors settle
                    self._status_debounce_until = self._current_timestamp + 2.0
                # Run one-time position balancer completion check now that markets are ready
                if self._position_balancer is not None:
                    self.log_with_clock(logging.INFO, "Position balancer enabled at startup; performing completion check.")
                    self._position_balancer.c_scan_and_mark_completion()
                else:
                    self.log_with_clock(logging.INFO, "Position balancer disabled at startup.")
        
        # Check network status
        for market in self._sb_markets:
            if market.network_status is not NetworkStatus.CONNECTED:
                if should_report:
                    self.logger().warning("Markets not all online. No arbitrage trading permitted.")
                return False
        
        return True

    cdef bint c_check_markets_ready_orchestrated(self):
        """
        Orchestrated mode readiness check: connectivity only, no logging/buy-in.
        
        OPTIMIZATION: Single-pass check for both ready and network_status.
        Avoids list allocation and double iteration.
        """
        # Single-pass check: both ready AND connected
        for market in self._sb_markets:
            if not market.ready or market.network_status is not NetworkStatus.CONNECTED:
                return False
        return True

    cdef double c_get_reference_bid_for_asset(self, str asset_key):
        """Return a non-zero bid for the given base asset from any active market tuple, or 0.0 if none."""
        cdef:
            double last_bid = 0.0
            ExchangeBase _ex
            OrderBook _ob
            cpp_set[OrderBookEntryCPP].reverse_iterator _bid_it
        for mp in self._market_pairs:
            if last_bid > 0.0:
                break
            if mp.first.base_asset == asset_key:
                _ex = mp.first.market
                _ob = _ex.c_get_order_book(mp.first.trading_pair)
                if _ob._bid_book.size() > 0:
                    _bid_it = _ob._bid_book.rbegin()
                    last_bid = deref(_bid_it).getPrice()
                    if last_bid > 0.0:
                        break
            if mp.second.base_asset == asset_key:
                _ex = mp.second.market
                _ob = _ex.c_get_order_book(mp.second.trading_pair)
                if _ob._bid_book.size() > 0:
                    _bid_it = _ob._bid_book.rbegin()
                    last_bid = deref(_bid_it).getPrice()
                    if last_bid > 0.0:
                        break
        return last_bid if last_bid > 0.0 else 0.0


    cdef pair[int, double] c_top_of_book_profitable_get_conv(self,
                                                              object buy_market_tuple,
                                                              object sell_market_tuple,
                                                              double min_profitability):
        """Top-of-book gate. Returns (is_profitable, sell->buy conv_rate)."""
        cdef:
            double min_prof_threshold = 1.0 + min_profitability
            double top_bid
            double top_ask
            double top_bid_adj
            double top_ask_adj
            double conv_rate = 1.0
        # Compute conversion via unified helper (1.0 if no-op)
        conv_rate = self._conv_rate(buy_market_tuple, sell_market_tuple)

        # Read top-of-book via C-level order book to avoid exceptions
        cdef ExchangeBase buy_ex = buy_market_tuple.market
        cdef ExchangeBase sell_ex = sell_market_tuple.market
        cdef OrderBook buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        cdef OrderBook sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)
        if sell_ob._bid_book.size() == 0 or buy_ob._ask_book.size() == 0:
            return pair[int, double](False, conv_rate)
        cdef cpp_set[OrderBookEntryCPP].reverse_iterator bid_it = sell_ob._bid_book.rbegin()
        cdef cpp_set[OrderBookEntryCPP].iterator ask_it = buy_ob._ask_book.begin()
        top_bid = deref(bid_it).getPrice()
        top_ask = deref(ask_it).getPrice()
        if top_bid <= 0 or top_ask <= 0:
            return pair[int, double](False, conv_rate)
        # Apply conversion to sell side only (buy side conversion is 1.0 by convention)
        top_bid_adj = top_bid * conv_rate
        top_ask_adj = top_ask
        if top_bid_adj / top_ask_adj < min_prof_threshold:
            return pair[int, double](False, conv_rate)
        return pair[int, double](True, conv_rate)

    cdef tuple c_find_best_profitable_amount(self, object buy_market_tuple, object sell_market_tuple):
        """Find best profitable amount - clean and optimized"""
        cdef:
            ExchangeBase buy_market = buy_market_tuple.market
            ExchangeBase sell_market = sell_market_tuple.market
            double buy_quote_balance
            double sell_base_balance
            double conv_rate = 1.0
            pair[int, double] gate_res
            OrderBook buy_ob
            OrderBook sell_ob
            # vector[ArbOpportunity] profitable_orders  <-- Removed: using self._reusable_arb_opps
            double max_base_amount

        # Early uniform gate: skip any Python balance calls if top-of-book fails
        gate_res = self.c_top_of_book_profitable_get_conv(buy_market_tuple, sell_market_tuple, self._min_profitability)
        if not gate_res.first:
            return (0.0, 0.0, 0.0, 0.0)
        conv_rate = gate_res.second

        # Fetch balances only after passing the gate
        buy_quote_balance = float(buy_market.c_get_available_balance(buy_market_tuple.quote_asset))
        sell_base_balance = float(sell_market.c_get_available_balance(sell_market_tuple.base_asset))
        if buy_quote_balance <= EPSILON or sell_base_balance <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Calculate capacity limits once
        max_base_amount = self._calculate_capacity_limit(
            buy_market_tuple, sell_market_tuple,
            buy_quote_balance, sell_base_balance)
        
        if max_base_amount <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Get OrderBook objects (GIL required for extraction)
        buy_ob = buy_market.c_get_order_book(buy_market_tuple.trading_pair)
        sell_ob = sell_market.c_get_order_book(sell_market_tuple.trading_pair)

        # Reset reusable vector
        self._reusable_arb_opps.clear()

        # Get profitable orders (includes top-of-book check) with capacity-aware early-stop
        # Release GIL for scanning loop
        with nogil:
            c_find_profitable_arbitrage_orders(
                self._min_profitability,
                buy_ob._ask_book,
                sell_ob._bid_book,
                1.0,  # Buy conversion always 1.0
                conv_rate,
                max_base_amount,
                0.05,
                &self._reusable_arb_opps) # Pass address of reusable vector
        
        if self._reusable_arb_opps.empty():
            return (0.0, 0.0, 0.0, 0.0)
        
        # Aggregate profitable volume and track worst prices for limit orders
        cdef:
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds_orig = 0.0
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
            double worst_buy_price = 0.0    # Highest (worst) ask price for buy limit order
            double worst_sell_price = 0.0   # Lowest (worst) bid price for sell limit order
            double avg_sell_price_orig, avg_buy_price, profitability
            ArbOpportunity opp

        for i in range(self._reusable_arb_opps.size()):
            opp = self._reusable_arb_opps[i]
            bid_adj = opp.bid_price
            ask_adj = opp.ask_price
            orig_bid = opp.orig_bid_price
            orig_ask = opp.orig_ask_price
            amount = opp.amount

            # Apply constraints
            amount = min(amount,
                        max_base_amount - total_base,
                        (buy_quote_balance - total_cost) / ask_adj if ask_adj > 0 else 0)

            if amount <= EPSILON:
                continue

            # Track worst (furthest) prices for limit orders to ensure full fill
            # Buy side: use highest (worst) ask price we scanned
            if orig_ask > worst_buy_price:
                worst_buy_price = orig_ask
            # Sell side: use lowest (worst) bid price we scanned
            if worst_sell_price == 0.0 or orig_bid < worst_sell_price:
                worst_sell_price = orig_bid

            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds_orig += orig_bid * amount

            # Stop if we've used all capacity
            if total_base >= max_base_amount - EPSILON:
                break

        # Calculate results using worst prices for limit orders
        if total_base > EPSILON:
            # Use worst prices instead of averages for limit order placement
            # Profitability check still uses average prices for accuracy
            avg_sell_price_orig = total_proceeds_orig / total_base
            avg_buy_price = total_cost / total_base
            profitability = ((avg_sell_price_orig * conv_rate) / avg_buy_price - 1.0) if avg_buy_price > 0 else 0.0

            # Check minimum notional (in sell market quote currency)
            # NOTE: _min_order_usd should match quote currency units
            if avg_sell_price_orig * total_base >= self._min_order_usd:
                # Return worst prices for limit order placement (not averages)
                return (total_base, profitability, worst_sell_price, worst_buy_price)

        return (0.0, 0.0, 0.0, 0.0)


    cdef double _calculate_capacity_limit(self,
                                        object buy_market_tuple,
                                        object sell_market_tuple,
                                        double buy_quote_balance,
                                        double sell_base_balance):
        """Calculate maximum tradeable amount with quantization"""
        cdef:
            double capacity = sell_base_balance
            object quantized
            double approx_ask
        
        # Get approximate buy price for affordability check via C-level top-of-book
        cdef ExchangeBase _buy_ex = buy_market_tuple.market
        cdef OrderBook _buy_ob = _buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        cdef cpp_set[OrderBookEntryCPP].iterator _ask_it2
        if _buy_ob._ask_book.size() > 0:
            _ask_it2 = _buy_ob._ask_book.begin()
            approx_ask = deref(_ask_it2).getPrice()
        else:
            approx_ask = 0.0
        
        # Apply quote balance constraint
        if approx_ask > 0:
            if buy_quote_balance > 0:
                capacity = min(capacity, buy_quote_balance / approx_ask)
            
            # Apply max trade size constraint (no overhead: simple division using existing approx_ask)
            if self._max_order_usd > 0:
                capacity = min(capacity, self._max_order_usd / approx_ask)
        
        # Apply sell-side quantization
        quantized = sell_market_tuple.market.quantize_order_amount(
            sell_market_tuple.trading_pair,
            Decimal(str(max(0.0, capacity - 1e-12))))
        capacity = float(quantized) if quantized else 0.0
        
        # Apply buy-side quantization
        if capacity > 0:
            quantized = buy_market_tuple.market.quantize_order_amount(
                buy_market_tuple.trading_pair,
                Decimal(str(max(0.0, capacity - 1e-12))))
            capacity = float(quantized) if quantized else 0.0
        
        return capacity
 
 

    cdef void c_cleanup_old_orders(self):
        """Clean up old order tracking data"""
        cdef:
            double cutoff = self._current_timestamp - (self._order_timeout * 2)
            list to_remove = []
            string order_id_str
            double timestamp
            
        # Only clean up if we have orders to check
        if self._order_timestamps.size() == 0 and self._completed_orders.size() == 0:
            return
            
        # Find old entries in order timestamps
        for order_id_str, timestamp in self._order_timestamps:
            if timestamp < cutoff:
                to_remove.append(order_id_str)
        
        # Remove old entries from both tracking structures
        for order_id_str in to_remove:
            self._order_timestamps.erase(order_id_str)
            # Also remove from completed orders set
            self._completed_orders.erase(order_id_str)

            # Cleanup position balancer tracking (delegated to handler)
            try:
                oid = order_id_str.decode('utf-8')
                
                # CRITICAL FIX: Force cleanup of pending orders and order tracker
                # This prevents "resurrection" of stuck orders where they are removed from timestamps
                # but valid in tracker -> c_check_all_order_timeouts -> re-added to timestamps loop
                
                # 1. Attempt to find market pair for the order
                market_pair = self._sb_order_tracker.c_get_market_pair_from_order_id(oid)
                
                # 2. Stop tracking in strategy base (prevents it from showing up in active orders)
                if market_pair is not None:
                    self._sb_order_tracker.c_stop_tracking_limit_order(market_pair, oid)
                    
                # 3. Remove from local pending set (unblocks new orders)
                self.c_remove_pending_order(market_pair, oid, "forced_cleanup")
                
                if self._position_balancer is not None:
                    self._position_balancer.handle_old_order_cleanup(oid)
                    
                self.logger().warning(f"Force cleaned up stuck order {oid} (older than {self._order_timeout * 2}s)")
            except Exception:
                pass
        
        # Warn if too many tracked orders
        if self._order_timestamps.size() > self._max_tracked_orders:
            self.logger().warning(f"Tracked orders exceed limit: {self._order_timestamps.size()}")

        # Cleanup stale failure timestamps beyond cutoff
        try:
            keys_to_drop = []
            for mp_tuple, ts in self._last_failure_timestamps.items():
                if ts < cutoff:
                    keys_to_drop.append(mp_tuple)
            for k in keys_to_drop:
                self._last_failure_timestamps.pop(k, None)
        except Exception:
            pass

        # Cleanup stale recent order mappings for orders that were just cleaned
        try:
            for order_id_str in to_remove:
                try:
                    oid = order_id_str.decode('utf-8')
                    self._recent_order_market_pair.pop(oid, None)
                    # Also clean up timeout cancelled orders set
                    self._timeout_cancelled_orders.discard(oid)
                    # Also clean up fill timestamps
                    self._order_fill_timestamps.pop(oid, None)
                except Exception:
                    pass
        except Exception:
            pass

    cdef void c_log_conversion_rates(self):
        """Log conversion rates if they differ from 1:1"""
        # Skip logging if rates are exactly 1:1
        if (fabs(self._cached_base_rate - 1.0) <= EPSILON and 
            fabs(self._cached_quote_rate - 1.0) <= EPSILON):
            return
            
        if fabs(self._cached_base_rate - 1.0) > EPSILON:
            self.logger().info(f"Base conversion rate: {self._cached_base_rate:.6f}")
        if fabs(self._cached_quote_rate - 1.0) > EPSILON:
            self.logger().info(f"Quote conversion rate: {self._cached_quote_rate:.6f}")




    cdef bint c_ready_for_new_orders(self, list market_tuples):
        """
        Check if ready for new arbitrage orders.
        For arbitrage: market_tuples[0] = buy_market, market_tuples[1] = sell_market

        Smart logic:
        - Allow if buy_market has no pending BUY (pending SELL is OK)
        - Allow if sell_market has no pending SELL (pending BUY is OK)
        - This enables parallel trading: can do A->B and B->A simultaneously
        
        OPTIMIZATION: Reordered checks for most common rejection first (global cooldown),
        removed redundant len() checks, simplified logic.
        """
        cdef:
            double time_left
            object buy_market_tuple
            object sell_market_tuple
            set pending_buys
            set pending_sells
            str order_id

        # Early validation: need exactly 2 market tuples for arbitrage
        if len(market_tuples) != 2:
            return False

        # Global cooldown check FIRST - most common rejection reason
        # Checking this early avoids unnecessary dict lookups when in cooldown
        if self._last_global_trade_timestamp > 0:
            time_left = (self._last_global_trade_timestamp +
                    self._next_trade_delay - self._current_timestamp)
            if time_left > 0:
                return False

        buy_market_tuple = market_tuples[0]
        sell_market_tuple = market_tuples[1]

        # Check buy market: block only if it has pending BUY orders without fills
        # OPTIMIZATION: Removed redundant len() > 0 check - truthiness is sufficient
        try:
            pending_buys = self._pending_buy_orders_by_market.get(buy_market_tuple)
            if pending_buys:
                # Check if ANY pending buy lacks fills (fast rejection)
                for order_id in pending_buys:
                    if order_id not in self._orders_with_fills:
                        return False  # Block: buy market has unfilled buy order
        except Exception:
            pass

        # Check sell market: block only if it has pending SELL orders without fills
        try:
            pending_sells = self._pending_sell_orders_by_market.get(sell_market_tuple)
            if pending_sells:
                # Check if ANY pending sell lacks fills (fast rejection)
                for order_id in pending_sells:
                    if order_id not in self._orders_with_fills:
                        return False  # Block: sell market has unfilled sell order
        except Exception:
            pass

        # Failure cooldown window (treat placement errors like cancels/timeouts)
        for market_tuple in market_tuples:
            if market_tuple in self._last_failure_timestamps:
                time_left = (self._last_failure_timestamps[market_tuple] +
                        self._order_timeout - self._current_timestamp)
                if time_left > 0:
                    return False
                else:
                    # Cooldown expired - auto-clean
                    self._last_failure_timestamps.pop(market_tuple, None)

        return True

    cdef pair[double, double] c_calculate_profitability(self, object market_pair):
        """
        Calculate profitability for both arbitrage directions.
        
        OPTIMIZATION: Uses C-level order book access instead of Python get_price() calls.
        This avoids 4 Python method calls and 4 Decimal->float conversions per call.
        """
        cdef:
            ExchangeBase ex1 = market_pair.first.market
            ExchangeBase ex2 = market_pair.second.market
            OrderBook ob1 = ex1.c_get_order_book(market_pair.first.trading_pair)
            OrderBook ob2 = ex2.c_get_order_book(market_pair.second.trading_pair)
            double bid1, ask1, bid2, ask2
            double conv_rate = 1.0
            bint needs_conversion = False
            cpp_set[OrderBookEntryCPP].reverse_iterator bid_it
            cpp_set[OrderBookEntryCPP].iterator ask_it

        # Check if order books have data
        if ob1._bid_book.size() == 0 or ob1._ask_book.size() == 0:
            return pair[double, double](0.0, 0.0)
        if ob2._bid_book.size() == 0 or ob2._ask_book.size() == 0:
            return pair[double, double](0.0, 0.0)

        # Get prices from market 1 via C++ iterators (no Python/Decimal overhead)
        bid_it = ob1._bid_book.rbegin()
        ask_it = ob1._ask_book.begin()
        bid1 = deref(bid_it).getPrice()
        ask1 = deref(ask_it).getPrice()

        # Get prices from market 2 via C++ iterators
        bid_it = ob2._bid_book.rbegin()
        ask_it = ob2._ask_book.begin()
        bid2 = deref(bid_it).getPrice()
        ask2 = deref(ask_it).getPrice()

        # Sanity check - prices must be positive
        if bid1 <= 0 or ask1 <= 0 or bid2 <= 0 or ask2 <= 0:
            return pair[double, double](0.0, 0.0)
            
        # Check if conversion is needed
        if (market_pair.first.quote_asset != market_pair.second.quote_asset or
            market_pair.first.base_asset != market_pair.second.base_asset):
            # Only calculate conversion if not using fixed 1:1 rates
            if self._use_oracle_conversion_rate or self._fixed_base_rate != 1.0 or self._fixed_quote_rate != 1.0:
                conv_rate = self.c_get_market_to_market_conversion_rate(market_pair.first, market_pair.second)
                needs_conversion = True
        
        # Apply conversion only if needed
        if needs_conversion:
            bid2 *= conv_rate
            ask2 *= conv_rate
        
        # Calculate profitability without fees (fees considered in execution)
        # Direction 1: Buy from market2, sell to market1
        cdef double prof1 = (bid1 / ask2 - 1.0) if ask2 > 0 else -1.0
        # Direction 2: Buy from market1, sell to market2  
        cdef double prof2 = (bid2 / ask1 - 1.0) if ask1 > 0 else -1.0
        
        return pair[double, double](prof1, prof2)

    cdef c_execute_arbitrage(self, object buy_market_tuple, object sell_market_tuple):
        """
        Execute arbitrage trade

        """
        # CRITICAL SAFEGUARD: Prevent double execution at the same timestamp
        # This protects against multiple strategy instances or rapid tick cycles
        if self._last_global_trade_timestamp == self._current_timestamp:
            self.logger().warning(
                f"Skipping duplicate arbitrage execution at timestamp {self._current_timestamp:.3f} "
                f"(already executed this tick)")
            return

        # CRITICAL: Set timestamp IMMEDIATELY to prevent race condition
        # If we wait until line 1113, a second call could slip through the check above
        self._last_global_trade_timestamp = self._current_timestamp

        cdef:
            tuple result = self.c_find_best_profitable_amount(buy_market_tuple, sell_market_tuple)
            double amount = <double>result[0]
            double profitability = <double>result[1]
            double sell_price = <double>result[2]
            double buy_price = <double>result[3]
            double volume_usd
            ExchangeBase buy_market = buy_market_tuple.market
            ExchangeBase sell_market = sell_market_tuple.market
            string buy_id_str
            string sell_id_str
            # Declarations for quantization and safety caps
            object quantized_buy
            object quantized_sell
            object quantized_amount
            double sell_available_now
            object sell_cap_dec
            object quantized_sell_cap
            object quantized_buy_cap
            object buy_req
            object dec_eps
            double buy_quote_available_now
            double buy_required_quote
            double affordable_base
            object affordable_dec
            object q_buy2
            object q_sell2
            object sell_req2
            
        if amount <= 0:
            if self._logging_options & self.OPTION_LOG_INSUFFICIENT_ASSET:
                self.logger().info("Insufficient balance or no profitable amount found")
            return
        
        # Quantize amounts using c-level method when available (with epsilon and price), fallback to Python API
        # OPTIMIZATION: Use Decimal.from_float() instead of Decimal(str()) to avoid expensive string formatting.
        # This is safe because quantization handles small precision noise.
        cdef object buy_price_decimal = Decimal.from_float(buy_price)
        cdef object sell_price_decimal = Decimal.from_float(sell_price)
        
        # Calculate safe amount using float math first
        cdef double safe_amount_float = max(0.0, amount - QUANTIZATION_EPSILON)
        cdef object dec_safe_amount = Decimal.from_float(safe_amount_float)
        
        quantized_buy = self.c_safe_quantize_order_amount(buy_market, buy_market_tuple.trading_pair, dec_safe_amount, buy_price_decimal)
        quantized_sell = self.c_safe_quantize_order_amount(sell_market, sell_market_tuple.trading_pair, dec_safe_amount, sell_price_decimal)
        quantized_amount = min(quantized_buy, quantized_sell)
        
        # Safety cap: re-check sell venue's live available base and re-quantize to prevent oversold at submission time
        sell_available_now = float(sell_market.c_get_available_balance(sell_market_tuple.base_asset))
        
        # Only do extra work if available shrank below our planned amount
        # OPTIMIZATION: Compare floats directly to avoid Decimal overhead
        if sell_available_now + 1e-15 < float(quantized_amount):
            # Calculate cap using float math
            # safe_cap = max(0.0, sell_available_now - QUANTIZATION_EPSILON)
            # Use Decimal.from_float for speed
            sell_cap_dec = Decimal.from_float(max(0.0, sell_available_now - QUANTIZATION_EPSILON))
            
            quantized_sell_cap = self.c_safe_quantize_order_amount(sell_market, sell_market_tuple.trading_pair, sell_cap_dec, sell_price_decimal)
            
            # Re-quantize buy side to the capped amount
            # OPTIMIZATION: Avoid Decimal arithmetic where possible
            if quantized_sell_cap is None:
                buy_req = Decimal("0")
            else:
                 buy_req = quantized_sell_cap
            
            # Ensure epsilon safety for buy side (using Decimal subtract only once)
            dec_eps = Decimal("1e-12")
            if buy_req > dec_eps:
                buy_req = buy_req - dec_eps
            else:
                buy_req = Decimal("0")
                
            quantized_buy_cap = self.c_safe_quantize_order_amount(buy_market, buy_market_tuple.trading_pair, buy_req, buy_price_decimal)
            quantized_amount = min(quantized_amount, quantized_sell_cap, quantized_buy_cap)

        # Symmetric safety cap on buy venue: ensure we do not exceed current quote availability
        buy_quote_available_now = float(buy_market.c_get_available_balance(buy_market_tuple.quote_asset))
        buy_required_quote = float(quantized_amount) * buy_price
        
        if buy_quote_available_now + 1e-15 < buy_required_quote:
            # Reduce to affordable base amount and re-quantize both sides accordingly
            affordable_base = 0.0
            if buy_price > 0.0:
                affordable_base = max(0.0, buy_quote_available_now / buy_price)
            
            # OPTIMIZATION: Use Decimal.from_float
            affordable_dec = Decimal.from_float(max(0.0, affordable_base - QUANTIZATION_EPSILON))
            
            q_buy2 = self.c_safe_quantize_order_amount(buy_market, buy_market_tuple.trading_pair, affordable_dec, buy_price_decimal)
            
            # Align sell side to the reduced buy size
            sell_req2 = q_buy2 if q_buy2 is not None else Decimal("0")
            q_sell2 = self.c_safe_quantize_order_amount(sell_market, sell_market_tuple.trading_pair, sell_req2, sell_price_decimal)
            
            # Handle potential None from quantization
            if q_buy2 is not None and q_sell2 is not None:
                quantized_amount = min(quantized_amount, q_buy2, q_sell2)
            elif q_buy2 is not None:
                quantized_amount = min(quantized_amount, q_buy2)
            elif q_sell2 is not None:
                quantized_amount = min(quantized_amount, q_sell2)

        # Apply max_order_size cap from trading rules
        try:
            buy_trading_rule = buy_market._trading_rules.get(buy_market_tuple.trading_pair)
            sell_trading_rule = sell_market._trading_rules.get(sell_market_tuple.trading_pair)
            if buy_trading_rule is not None and buy_trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > buy_trading_rule.max_order_size:
                    quantized_amount = min(quantized_amount, buy_trading_rule.max_order_size)
            if sell_trading_rule is not None and sell_trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > sell_trading_rule.max_order_size:
                    quantized_amount = min(quantized_amount, sell_trading_rule.max_order_size)
        except Exception:
            pass  # Fail gracefully if trading rules unavailable; balance caps already applied

        # Check minimum order size after any safety caps
        # NOTE: volume_usd is notional value in sell market quote currency, not necessarily USD
        # _min_order_usd should be configured in same units as the quote currency
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self._min_order_usd:
            return
        # Declare variables before the if block (Cython requirement)
        cdef double order_start_time
        cdef object buy_order_type
        cdef object sell_order_type
        cdef double placement_latency

        if quantized_amount > Decimal("0"):
            # Log timing for latency monitoring
            order_start_time = self._current_timestamp

            # CRITICAL: Place both limit orders with minimal latency
            # The price is used as the limit price for the orders

            # Pre-calculate all parameters to minimize latency between orders
            # Use LIMIT order type for both buy and sell orders
            buy_order_type = OrderType.LIMIT
            sell_order_type = OrderType.LIMIT
            # Prices already prepared above as Decimal for quantization

            # Execute both orders in rapid succession
            # This is the best we can do in Cython without async support
            # The actual network calls happen inside the exchange connectors
            try:
                buy_order_id = self.c_buy_with_specific_market(
                    buy_market_tuple, quantized_amount,
                    order_type=buy_order_type,
                    price=buy_price_decimal,
                    expiration_seconds=self._next_trade_delay)
            except Exception as e:
                # CRITICAL: Set failure timestamp to enforce cooldown
                self._last_failure_timestamps[buy_market_tuple] = self._current_timestamp
                self.logger().warning(f"Error submitting buy order to {buy_market.name}: {e}")
                return

            # Immediately place the sell order - minimal delay
            try:
                sell_order_id = self.c_sell_with_specific_market(
                    sell_market_tuple, quantized_amount,
                    order_type=sell_order_type,
                    price=sell_price_decimal,
                    expiration_seconds=self._next_trade_delay)
            except Exception as e:
                # CRITICAL: Set failure timestamp to enforce cooldown
                # Note: Buy order may have been placed - will timeout/cancel naturally
                self._last_failure_timestamps[sell_market_tuple] = self._current_timestamp
                self.logger().warning(f"Error submitting sell order to {sell_market.name}: {e}")
                return

            # Track orders
            buy_id_str = self._to_cpp_str(buy_order_id)
            sell_id_str = self._to_cpp_str(sell_order_id)

            self._order_timestamps[buy_id_str] = order_start_time
            self._order_timestamps[sell_id_str] = order_start_time

            # Track pending orders per market - SEPARATE by side (buy/sell) for smart parallel trading
            try:
                # Track BUY order on buy market
                if buy_market_tuple not in self._pending_buy_orders_by_market:
                    self._pending_buy_orders_by_market[buy_market_tuple] = set()
                self._pending_buy_orders_by_market[buy_market_tuple].add(buy_order_id)

                # Track SELL order on sell market
                if sell_market_tuple not in self._pending_sell_orders_by_market:
                    self._pending_sell_orders_by_market[sell_market_tuple] = set()
                self._pending_sell_orders_by_market[sell_market_tuple].add(sell_order_id)
            except Exception as e:
                self.logger().warning(f"Failed to track pending orders by market: {e}")

    cdef void c_handle_order_completion(self, object order_event, bint is_buy) except *:
        """Unified order completion handler"""
        cdef:
            str order_id = order_event.order_id
            object market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            double time_elapsed
            string order_id_str = self._to_cpp_str(order_id)
            str order_type = "Buy" if is_buy else "Sell"
            object cooldown_tuple = market_pair_tuple
            
        if market_pair_tuple is None:
            # Try to get from recent mapping for late events
            try:
                cooldown_tuple = self._recent_order_market_pair.get(order_id)
            except Exception:
                cooldown_tuple = None
            if cooldown_tuple is None:
                return

        # Check if we've already processed this order's completion
        # This prevents duplicate logging for partial fills
        if self._completed_orders.find(order_id_str) != self._completed_orders.end():
            # Already processed - this is normal for limit orders (late exchange confirmation)
            return
            
        try:
            # Mark this order as completed (first fill counts as completed)
            self._completed_orders.insert(order_id_str)
            
            # Increment trade counter (only for this strategy's orders)
            self._total_trades += 1

            # Clean fill marker since completion supersedes partials
            try:
                self._orders_with_fills.discard(order_id)
                self._order_fill_timestamps.pop(order_id, None)
            except Exception:
                pass

            # If any cooldown was set earlier for this market tuple due to cancel/timeout, remove it now
            if cooldown_tuple is not None and cooldown_tuple in self._last_failure_timestamps:
                self._last_failure_timestamps.pop(cooldown_tuple, None)
                self.logger().info(f"Completion detected for {order_id} - removing cooldown on {cooldown_tuple[0].name}")

            # Remove from pending orders - check both buy and sell dicts
            self.c_remove_pending_order(market_pair_tuple, order_id, "completed")

            # Check completion time - safely handle None market_pair_tuple
            if self._order_timestamps.find(order_id_str) != self._order_timestamps.end():
                time_elapsed = self._current_timestamp - self._order_timestamps[order_id_str]
                if market_pair_tuple is not None:
                    self.logger().info(f"{market_pair_tuple[0].name}: {order_type} order {order_id} completed in {time_elapsed:.2f}s")
                else:
                    self.logger().info(f"Unknown market: {order_type} order {order_id} completed in {time_elapsed:.2f}s")
                # CRITICAL: Do NOT erase timestamp here. Keep it as tombstone for garbage collection.
                # self._order_timestamps.erase(order_id_str)

            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(
                    logging.DEBUG,
                    f"{order_type} order completed on {market_pair_tuple[0].name}: {order_id}")

            # Cleanup position balancer tracking (delegated to handler)
            if self._position_balancer is not None:
                self._position_balancer.handle_order_completion(order_id, is_buy)

        except Exception as e:
            self.logger().error(f"Error handling {order_type.lower()} order completion: {e}", exc_info=True)

    cdef c_did_fill_order(self, object order_filled_event):
        """Track that an order has received at least one fill."""
        cdef:
            str order_id = order_filled_event.order_id
            object order_type = order_filled_event.order_type
            string order_id_str = self._to_cpp_str(order_id)
            object market_pair_tuple
            double filled_amount
        # Track fills for any order type (we only change cooldown logic for MARKET)
        try:
            self._orders_with_fills.add(order_id)
            # Track the timestamp when this order first received a fill (for filled order timeout)
            if order_id not in self._order_fill_timestamps:
                self._order_fill_timestamps[order_id] = self._current_timestamp
        except Exception:
            pass

        # Notify position balancer of fill for partial fill tracking
        if self._position_balancer is not None:
            try:
                filled_amount = float(order_filled_event.amount)
                self._position_balancer.handle_order_fill(order_id, filled_amount)
            except Exception:
                pass

        # If we previously enforced cooldown due to cancel/timeout, remove it upon seeing fills
        market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_pair_tuple is None:
            try:
                market_pair_tuple = self._recent_order_market_pair.get(order_id)
            except Exception:
                market_pair_tuple = None
        
        if market_pair_tuple is not None and market_pair_tuple in self._last_failure_timestamps:
            self._last_failure_timestamps.pop(market_pair_tuple, None)
            self.logger().info(f"Late fills detected for {order_id} - removing cooldown on {market_pair_tuple[0].name}")

    cdef c_did_cancel_order_tracker(self, object order_cancelled_event):
        """Handle cancelled limit orders"""
        cdef:
            str order_id = order_cancelled_event.order_id
            string order_id_str = self._to_cpp_str(order_id)
            object market_pair = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            bint was_already_completed = False

        if market_pair is None:
            # Try to get from recent mapping for late events
            try:
                market_pair = self._recent_order_market_pair.get(order_id)
            except Exception:
                pass
            if market_pair is None:
                return

        # Check if order was already marked as completed (completion event arrived first)
        # This prevents race condition where cancel arrives after completion
        was_already_completed = (self._completed_orders.find(order_id_str) != self._completed_orders.end())

        # Full cleanup for cancelled orders calling logic
        # CRITICAL: Do NOT erase timestamp. Keep it as tombstone.
        # self._order_timestamps.erase(order_id_str)
        # CRITICAL: Mark as completed (tombstone) to prevent resurrection
        self._completed_orders.insert(order_id_str)
        # Clean up fill timestamp tracking
        try:
            self._order_fill_timestamps.pop(order_id, None)
        except Exception:
            pass

        # Stop tracking the order - use LIMIT order tracking since this strategy uses limit orders only
        # Remember mapping for possible late events
        try:
            self._recent_order_market_pair[order_id] = market_pair
        except Exception:
            pass
        self._sb_order_tracker.c_stop_tracking_limit_order(market_pair, order_id)

        # Cleanup position balancer tracking (delegated to handler)
        if self._position_balancer is not None:
            self._position_balancer.handle_order_cancellation(order_id)

        # Remove from pending orders tracking - check both buy and sell dicts
        self.c_remove_pending_order(market_pair, order_id, "cancelled")

        # Decide cooldown/logging based on completion status and cancellation reason
        if was_already_completed:
            # Order was already marked as completed before cancel event arrived
            # This is a race condition - treat as successful, no cooldown
            self.logger().info(
                f"Limit order {order_id} on {market_pair[0].name if market_pair else 'unknown'} completed before cancel event - treating as complete (no cooldown)")
        elif order_id in self._timeout_cancelled_orders:
            # Order was cancelled due to timeout - no cooldown needed
            self._timeout_cancelled_orders.discard(order_id)  # Clean up
            self.logger().info(
                f"Limit order {order_id} on {market_pair[0].name if market_pair else 'unknown'} was TIMEOUT-CANCELLED - no cooldown enforced")
        else:
            # Limit order cancelled naturally (by exchange or market conditions) - enforce cooldown
            self._last_failure_timestamps[market_pair] = self._current_timestamp
            self.logger().warning(
                f"Limit order {order_id} on {market_pair[0].name if market_pair else 'unknown'} was NATURALLY CANCELLED - cooldown enforced")

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        """Handle buy order completion"""
        self.c_handle_order_completion(buy_order_completed_event, True)
    
    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        """Handle sell order completion"""
        self.c_handle_order_completion(sell_order_completed_event, False)

    cdef c_did_fail_order(self, object order_failed_event):
        """On order placement failure, gate new orders for the affected market tuple for _order_timeout."""
        cdef:
            str order_id = order_failed_event.order_id
            object market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        
        # Always clean up tracking for failed orders to prevent stuck state
        self._sb_order_tracker.c_stop_tracking_limit_order(market_pair_tuple, order_id)
        self.c_remove_pending_order(market_pair_tuple, order_id, "failed")
        
        if market_pair_tuple is None:
            return

        # CRITICAL: Set failure timestamp to enforce cooldown
        self._last_failure_timestamps[market_pair_tuple] = self._current_timestamp
        self.logger().warning(
            f"Order placement failed on {market_pair_tuple[0].name} for {market_pair_tuple.trading_pair}; "
            f"cooling down for {self._order_timeout:.0f}s")

    cdef void c_check_all_order_timeouts(self):
        """
        Check and cancel timed out limit orders (300s timeout).
        
        OPTIMIZATION: Collect timed-out orders first, then process them.
        This avoids creating list copies while iterating and is safer for dict modification.
        """
        cdef:
            double time_elapsed
            double timeout_threshold = self._order_timeout
            string order_id_str
            object order_id
            object market_tuple
            dict all_market_orders
            dict market_orders
            list orders_to_cancel = []  # Collect (market_tuple, order_id, time_elapsed) tuples

        # Get ALL orders (limit orders in our case) across ALL market tuples
        try:
            all_market_orders = self._sb_order_tracker.c_get_limit_orders()
        except Exception:
            return

        # Phase 1: Identify orders to cancel (no modifications during iteration)
        for market_tuple, market_orders in all_market_orders.items():
            if not market_orders:
                continue
            for order_id in market_orders:
                order_id_str = self._to_cpp_str(order_id)

                # Track new orders
                if self._order_timestamps.find(order_id_str) == self._order_timestamps.end():
                    # CRITICAL: Check if this is a zombie order (already completed/cancelled but resurrected by tracker)
                    if self._completed_orders.find(order_id_str) != self._completed_orders.end():
                        # Force stop tracking to kill zombie
                        self._sb_order_tracker.c_stop_tracking_limit_order(market_tuple, order_id)
                        continue
                        
                    self._order_timestamps[order_id_str] = self._current_timestamp

                time_elapsed = self._current_timestamp - self._order_timestamps[order_id_str]

                # Check for timeout
                if time_elapsed > timeout_threshold:
                    # Skip position balancer orders - they have their own refresh interval
                    if order_id in self._position_balancer_orders:
                        continue

                    # Skip orders that have partial fills - they are handled by c_check_filled_order_timeouts
                    # using the existing framework tracking
                    if order_id in self._order_fill_timestamps:
                        continue

                    # Check if this order is already being cancelled to prevent duplicates
                    if self._sb_order_tracker.c_has_in_flight_cancel(order_id):
                        continue

                    # Collect for cancellation
                    orders_to_cancel.append((market_tuple, order_id, time_elapsed))

        # Phase 2: Cancel collected orders (safe to modify now)
        for market_tuple, order_id, time_elapsed in orders_to_cancel:
            order_id_str = self._to_cpp_str(order_id)
            
            # Mark this order as timeout-cancelled to prevent cooldown enforcement
            self._timeout_cancelled_orders.add(order_id)
            
            # CRITICAL: Actually CANCEL the limit order on the exchange
            try:
                self.c_cancel_order(market_tuple, order_id)
                self.logger().warning(f"Limit order {order_id} on {market_tuple[0].name} timed out after {time_elapsed:.2f}s - CANCELLING order")
            except Exception as e:
                self.logger().error(f"Failed to cancel timed out order {order_id}: {e}")
                # Remove from timeout set if cancel failed
                self._timeout_cancelled_orders.discard(order_id)
                continue

            # Clean up tracking
            # CRITICAL: Do NOT erase from timestamps/completed. Keep tombstones to prevent resurrection.
            # self._order_timestamps.erase(order_id_str)
            # self._completed_orders.erase(order_id_str)
            self._completed_orders.insert(order_id_str)

            # CRITICAL: Stop tracking the order immediately to prevent re-processing
            # before the cancel event arrives (prevents repeated cancel attempts)
            try:
                self._recent_order_market_pair[order_id] = market_tuple
            except Exception:
                pass
            self._sb_order_tracker.c_stop_tracking_limit_order(market_tuple, order_id)

            # Cleanup position balancer tracking (delegated to handler)
            if self._position_balancer is not None:
                self._position_balancer.handle_order_timeout(order_id)

            # Remove from pending orders tracking - check both buy and sell dicts
            self.c_remove_pending_order(market_tuple, order_id, "")

            # Enforce cooldown for this market
            self._last_failure_timestamps[market_tuple] = self._current_timestamp

    cdef void c_check_filled_order_timeouts(self):
        """
        Check and cancel filled orders that exceed filled_order_timeout.
        This allows orders with partial fills to remain open longer than unfilled orders.
        
        OPTIMIZATION: Collect orders to cancel first, then process them.
        This avoids creating list copies and is safer for dict modification.
        """
        cdef:
            str order_id
            double fill_time
            double time_since_fill
            object market_tuple
            dict all_market_orders
            list orders_to_cancel = []  # (order_id, market_tuple, time_since_fill)
            list orders_to_cleanup = []  # order_ids no longer tracked
            string order_id_str

        # Skip if no orders with fills to check
        if not self._order_fill_timestamps:
            return

        # Get ALL tracked limit orders
        try:
            all_market_orders = self._sb_order_tracker.c_get_limit_orders()
        except Exception:
            return

        # Phase 1: Identify orders to cancel (no modifications during iteration)
        for order_id, fill_time in self._order_fill_timestamps.items():
            time_since_fill = self._current_timestamp - fill_time

            # Check if filled order has exceeded its timeout
            if time_since_fill > self._filled_order_timeout:
                # Skip position balancer orders - they have their own refresh interval
                if order_id in self._position_balancer_orders:
                    continue

                # Find the market tuple for this order
                market_tuple = None
                for mt, orders in all_market_orders.items():
                    if order_id in orders:
                        market_tuple = mt
                        break

                if market_tuple is not None:
                    # Check if already being cancelled
                    if self._sb_order_tracker.c_has_in_flight_cancel(order_id):
                        continue
                    orders_to_cancel.append((order_id, market_tuple, time_since_fill))
                else:
                    # Order no longer tracked, mark for cleanup
                    orders_to_cleanup.append(order_id)

        # Phase 2: Cancel collected orders (safe to modify now)
        for order_id, market_tuple, time_since_fill in orders_to_cancel:
            # Mark as timeout-cancelled to avoid cooldown
            self._timeout_cancelled_orders.add(order_id)

            # Cancel the order
            try:
                self.c_cancel_order(market_tuple, order_id)
                self.logger().info(
                    f"Filled limit order {order_id} on {market_tuple[0].name} exceeded "
                    f"filled order timeout ({self._filled_order_timeout:.0f}s) after "
                    f"{time_since_fill:.2f}s - CANCELLING order")
            except Exception as e:
                self.logger().error(f"Failed to cancel filled order {order_id}: {e}")
                self._timeout_cancelled_orders.discard(order_id)
                continue

            # Clean up tracking
            try:
                order_id_str = self._to_cpp_str(order_id)
                # CRITICAL: Keep tombstone to prevent resurrection
                # self._order_timestamps.erase(order_id_str)
                # self._completed_orders.erase(order_id_str)
                self._completed_orders.insert(order_id_str)
                self._recent_order_market_pair[order_id] = market_tuple
            except Exception:
                pass

            # Stop tracking the order
            self._sb_order_tracker.c_stop_tracking_limit_order(market_tuple, order_id)

            # Cleanup position balancer tracking
            if self._position_balancer is not None:
                self._position_balancer.handle_order_timeout(order_id)

            # Remove from pending orders tracking
            self.c_remove_pending_order(market_tuple, order_id, "filled_timeout")

            # Clean up fill timestamp and set
            self._order_fill_timestamps.pop(order_id, None)
            self._orders_with_fills.discard(order_id)

            # Do NOT enforce cooldown for filled order timeouts (they got fills, just didn't complete)

        # Phase 3: Clean up orders that are no longer tracked
        for order_id in orders_to_cleanup:
            self._order_fill_timestamps.pop(order_id, None)


cdef void c_find_profitable_arbitrage_orders(
    double min_profitability,
    cpp_set[OrderBookEntryCPP] &buy_asks,
    cpp_set[OrderBookEntryCPP] &sell_bids,
    double buy_conversion_rate,
    double sell_conversion_rate,
    double target_base_amount,
    double overshoot_ratio,
    vector[ArbOpportunity] *output_vector) noexcept nogil:
    """
    Find profitable arbitrage opportunities between two markets.
    Populates output_vector with ArbOpportunity structs.
    Executes without GIL.
    """
    cdef:
        double min_prof_threshold = 1.0 + min_profitability
        int max_levels = 20
        int levels_processed = 0
        double bid_leftover = 0.0
        double ask_leftover = 0.0
        double step_amount = 0.0
        double cumulative_base = 0.0
        double overshoot_stop = 0.0
        cpp_set[OrderBookEntryCPP].reverse_iterator bid_it
        cpp_set[OrderBookEntryCPP].reverse_iterator bid_end
        cpp_set[OrderBookEntryCPP].iterator ask_it
        cpp_set[OrderBookEntryCPP].iterator ask_end
        OrderBookEntryCPP bid_entry
        OrderBookEntryCPP ask_entry
        double orig_bid_price
        double orig_ask_price
        double bid_price
        double ask_price
        ArbOpportunity opp
        
    # Prepare capacity-aware stopping condition (optional)
    if target_base_amount > 0.0:
        overshoot_stop = target_base_amount * (1.0 + overshoot_ratio)

    # Now scan the books (C-level iteration)
    
    bid_it = sell_bids.rbegin()
    bid_end = sell_bids.rend()
    ask_it = buy_asks.begin()
    ask_end = buy_asks.end()

    if bid_it == bid_end or ask_it == ask_end:
        return

    bid_entry = deref(bid_it)
    ask_entry = deref(ask_it)

    bid_leftover = bid_entry.getAmount()
    ask_leftover = ask_entry.getAmount()

    while levels_processed < max_levels and bid_it != bid_end and ask_it != ask_end:
        # Get prices (original, unconverted)
        orig_bid_price = bid_entry.getPrice()
        orig_ask_price = ask_entry.getPrice()

        # Sanity check
        if orig_bid_price <= 0 or orig_ask_price <= 0:
            break

        # Apply conversion once per level
        bid_price = orig_bid_price * sell_conversion_rate
        ask_price = orig_ask_price * buy_conversion_rate

        # Check profitability threshold
        if bid_price / ask_price < min_prof_threshold:
            break

        # Calculate step amount
        step_amount = bid_leftover if bid_leftover <= ask_leftover else ask_leftover

        if step_amount > EPSILON:
            opp.bid_price = bid_price
            opp.ask_price = ask_price
            opp.orig_bid_price = orig_bid_price
            opp.orig_ask_price = orig_ask_price
            opp.amount = step_amount
            output_vector.push_back(opp)

            # Capacity-aware early stop
            cumulative_base += step_amount
            if overshoot_stop > 0.0 and cumulative_base >= overshoot_stop:
                break

        # Advance to next level based on which side exhausted
        if bid_leftover <= ask_leftover:
            inc(bid_it)
            levels_processed += 1
            if bid_it == bid_end:
                break
            bid_entry = deref(bid_it)
            bid_leftover = bid_entry.getAmount()
            ask_leftover -= step_amount
        else:
            inc(ask_it)
            levels_processed += 1  
            if ask_it == ask_end:
                break
            ask_entry = deref(ask_it)
            ask_leftover = ask_entry.getAmount()
            bid_leftover -= step_amount