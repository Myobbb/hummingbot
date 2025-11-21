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
    double DEFAULT_RATE_CACHE_DURATION = 10.0
    double DEFAULT_ORDER_WARNING_DELAY = 10.0
    size_t DEFAULT_MAX_TRACKED_ORDERS = 1000
    double DEFAULT_FILLED_ORDER_TIMEOUT = 3600.0  # 1 hour timeout for orders with fills
    double EPSILON = 1e-10
    double QUANTIZATION_EPSILON = 1e-12
    double RATE_LOG_INTERVAL = 300.0

 

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
        self._order_warning_delay = 60.0  # Default 1 minute
        self._min_order_usd = 10.0  # Default minimum order size
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
                    rate_cache_duration: float = DEFAULT_RATE_CACHE_DURATION,
                    order_warning_delay: float = DEFAULT_ORDER_WARNING_DELAY,
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
        self._order_warning_delay = order_warning_delay
        
        # Thresholds
        self._min_order_usd = min_order_usd
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
            self._pending_limit_orders_by_market.clear()
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
                position_balancer_order_size_usd)
        else:
            self._position_balancer = None

        # Orchestration mode (for multi-strategy orchestrator optimization)
        self._orchestrated_mode = orchestrated_mode

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

    

    @property
    def min_profitability(self) -> Decimal:
        return Decimal(str(self._min_profitability))

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
                base_conv = float(RateOracle.get_instance().get_pair_rate(
                    f"{sell_market_tuple.base_asset}-{buy_market_tuple.base_asset}"))

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
                quote_conv = float(RateOracle.get_instance().get_pair_rate(
                    f"{sell_market_tuple.quote_asset}-{buy_market_tuple.quote_asset}"))

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
            self._all_markets_ready = all([market.ready for market in self._sb_markets])
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
        
        This ensures individual strategies still detect disconnections in orchestrated mode
        while avoiding redundant logging that the orchestrator handles.
        """
        # Check basic market readiness
        if not all([market.ready for market in self._sb_markets]):
            return False
        
        # Check network connectivity - this is crucial for disconnection detection
        for market in self._sb_markets:
            if market.network_status is not NetworkStatus.CONNECTED:
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
            vector[ArbOpportunity] profitable_orders
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
                &profitable_orders)
        
        if profitable_orders.size() == 0:
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

        for i in range(profitable_orders.size()):
            opp = profitable_orders[i]
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
        if approx_ask > 0 and buy_quote_balance > 0:
            capacity = min(capacity, buy_quote_balance / approx_ask)
        
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
 
 
 

    cdef tuple c_find_best_buyin_amount(self,
                                        object buy_market_tuple,
                                        object sell_market_tuple,
                                        double buy_quote_balance,
                                        double max_spend_quote,
                                        double min_profitability):
        """
        Compute best buy-only amount using cross-market price edge, ignoring sell-side base balance limits.
        Caps spend by both available quote balance and provided max_spend_quote.
        Returns (amount_base, profitability, avg_sell_price_orig, avg_buy_price).
        """
        cdef:
            double conv_rate = 1.0
            double spend_cap = min(buy_quote_balance, max_spend_quote)
            pair[int, double] gate_res2
            OrderBook buy_ob
            OrderBook sell_ob
            vector[ArbOpportunity] profitable_orders
            double buy_cap_base = 0.0

        if spend_cap <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Early uniform gate for buy-in; reuse conv_rate
        gate_res2 = self.c_top_of_book_profitable_get_conv(buy_market_tuple, sell_market_tuple, min_profitability)
        if not gate_res2.first:
            return (0.0, 0.0, 0.0, 0.0)
        conv_rate = gate_res2.second

        # Determine an upper bound on base we might buy given spend_cap and quantization on buy side
        cdef double approx_ask = 0.0
        cdef ExchangeBase _buy_ex3 = buy_market_tuple.market
        cdef OrderBook _buy_ob3 = _buy_ex3.c_get_order_book(buy_market_tuple.trading_pair)
        cdef cpp_set[OrderBookEntryCPP].iterator _ask_it3
        if _buy_ob3._ask_book.size() > 0:
            _ask_it3 = _buy_ob3._ask_book.begin()
            approx_ask = deref(_ask_it3).getPrice()
        
        if approx_ask > 0.0 and spend_cap > 0.0:
            buy_cap_base = spend_cap / approx_ask
            q = self.c_safe_quantize_order_amount(buy_market_tuple.market, buy_market_tuple.trading_pair, Decimal(str(max(0.0, buy_cap_base - QUANTIZATION_EPSILON))), Decimal(str(approx_ask)))
            if q is not None:
                buy_cap_base = float(q)
            else:
                buy_cap_base = 0.0

        # Get OrderBook objects (GIL required)
        buy_ob = _buy_ex3.c_get_order_book(buy_market_tuple.trading_pair)
        sell_ob = sell_market_tuple.market.c_get_order_book(sell_market_tuple.trading_pair)

        # Use buy-in profitability threshold here (not the main arbitrage threshold), with capacity-aware early-stop
        # Release GIL
        with nogil:
            c_find_profitable_arbitrage_orders(
                min_profitability,
                buy_ob._ask_book,
                sell_ob._bid_book,
                1.0,
                conv_rate,
                buy_cap_base,
                0.05,
                &profitable_orders)

        if profitable_orders.size() == 0:
            return (0.0, 0.0, 0.0, 0.0)

        cdef:
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds = 0.0
            double total_proceeds_orig = 0.0
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
            double remaining_quote
            double avg_bid_adj, avg_bid_orig, avg_ask, profitability
            double worst_buy_price = 0.0  # Highest (worst) ask price for buy limit order
            ArbOpportunity opp

        for i in range(profitable_orders.size()):
            opp = profitable_orders[i]
            bid_adj = opp.bid_price
            ask_adj = opp.ask_price
            orig_bid = opp.orig_bid_price
            orig_ask = opp.orig_ask_price
            amount = opp.amount

            remaining_quote = spend_cap - total_cost
            if remaining_quote <= EPSILON:
                break
            if ask_adj > 0:
                amount = min(amount, remaining_quote / ask_adj)
            if amount <= EPSILON:
                continue

            # Track worst (furthest) buy price for limit order to ensure full fill
            if orig_ask > worst_buy_price:
                worst_buy_price = orig_ask

            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds += bid_adj * amount
            total_proceeds_orig += orig_bid * amount

        if total_base > EPSILON and total_cost > EPSILON:
            avg_bid_adj = total_proceeds / total_base
            avg_bid_orig = total_proceeds_orig / total_base
            avg_ask = total_cost / total_base
            profitability = (avg_bid_adj / avg_ask - 1.0) if avg_ask > EPSILON else 0.0
            # Return worst buy price for limit order placement (not average)
            return (total_base, profitability, avg_bid_orig, worst_buy_price)

        return (0.0, 0.0, 0.0, 0.0)

    

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
        
        # Remove old entries #from both tracking structures
        for order_id_str in to_remove:
            self._order_timestamps.erase(order_id_str)
            # Also remove from completed orders set
            self._completed_orders.erase(order_id_str)

            # Cleanup position balancer tracking (delegated to handler)
            try:
                oid = order_id_str.decode('utf-8')
                if self._position_balancer is not None and oid is not None:
                    self._position_balancer.handle_old_order_cleanup(oid)
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
        """Check if ready for new orders"""
        cdef:
            double time_elapsed
            string order_id_str
            object order_id
            
        # Check pending orders for provided markets
        for market_tuple in market_tuples:
            # Check both limit and market orders
            for orders in [self._sb_order_tracker.c_get_limit_orders().get(market_tuple, {}),
                          self._sb_order_tracker.c_get_market_orders().get(market_tuple, {})]:
                if orders:
                    for order_id in orders:
                        order_id_str = self._to_cpp_str(order_id)
                        
                        # Track new orders
                        if self._order_timestamps.find(order_id_str) == self._order_timestamps.end():
                            self._order_timestamps[order_id_str] = self._current_timestamp
                        
                        time_elapsed = self._current_timestamp - self._order_timestamps[order_id_str]
                        
                        # Check timeout
                        if time_elapsed > self._order_timeout:
                            # Don't cancel position balancer orders here - they have their own lifecycle
                            if order_id in self._position_balancer_orders:
                                continue

                            self.logger().warning(f"Order {order_id} timed out after {time_elapsed:.2f}s - forcibly removing from tracker")
                            self._order_timestamps.erase(order_id_str)
                            
                            # Try to cancel on exchange (may or may not succeed)
                            try:
                                self.c_cancel_order(market_tuple, order_id)
                            except:
                                pass  # Continue even if cancel fails
                            
                            # Force removal from tracker to unblock trading
                            self._sb_order_tracker.c_stop_tracking_limit_order(market_tuple, order_id)
                            self._sb_order_tracker.c_stop_tracking_market_order(market_tuple, order_id)
                            continue
                        else:
                            # Still waiting
                            if time_elapsed > self._order_warning_delay:
                                self.logger().warning(f"Order {order_id} pending for {time_elapsed:.2f}s")
                            return False
        
        return True

    cdef pair[double, double] c_calculate_profitability(self, object market_pair):
        """Calculate profitability for both arbitrage directions"""
        cdef:
            double bid1 = float(market_pair.first.get_price(False))
            double ask1 = float(market_pair.first.get_price(True))
            double bid2 = float(market_pair.second.get_price(False))
            double ask2 = float(market_pair.second.get_price(True))
            double conv_rate = 1.0
            bint needs_conversion = False
            
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
        """Execute arbitrage trade"""
        # CRITICAL: Set timestamp IMMEDIATELY to prevent race condition
        self._last_global_trade_timestamp = self._current_timestamp

        cdef:
            tuple result = self.c_find_best_profitable_amount(buy_market_tuple, sell_market_tuple)
            double amount = <double>result[0]
            double profitability = <double>result[1]
            double sell_price = <double>result[2]  # Worst sell price (limit)
            double buy_price = <double>result[3]   # Worst buy price (limit)
            double volume_usd
            ExchangeBase buy_market = buy_market_tuple.market
            ExchangeBase sell_market = sell_market_tuple.market
            string buy_id_str
            string sell_id_str
            
        if amount <= 0:
            if self._logging_options & self.OPTION_LOG_INSUFFICIENT_ASSET:
                self.logger().info("Insufficient balance or no profitable amount found")
            return
        
        # Convert to Decimal for market operations
        dec_amount = Decimal(str(amount))
        
        # Quantize amounts
        quantized_buy = buy_market.c_quantize_order_amount(buy_market_tuple.trading_pair, dec_amount)
        quantized_sell = sell_market.c_quantize_order_amount(sell_market_tuple.trading_pair, dec_amount)
        quantized_amount = min(quantized_buy, quantized_sell)
        
        # Check minimum order size
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self._min_order_usd:
            return
        
        # Declare all variables before the if block (Cython requirement)
        cdef double order_start_time
        cdef object buy_order_type = OrderType.LIMIT
        cdef object sell_order_type = OrderType.LIMIT
        cdef object buy_price_decimal
        cdef object sell_price_decimal
        cdef double placement_latency
        cdef str buy_order_id
        cdef str sell_order_id
        
        if quantized_amount > Decimal("0"):
            # Log timing for latency monitoring
            order_start_time = self._current_timestamp
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                self.log_with_clock(
                    logging.INFO,
                    f"Executing arbitrage: buy {quantized_amount:.8f} {buy_market_tuple.trading_pair} "
                    f"@ {buy_market.name}, sell @ {sell_market.name}, "
                    f"profitability: {profitability * 100:.2f}%")
            
            # Quantize prices
            buy_price_decimal = buy_market.quantize_order_price(buy_market_tuple.trading_pair, Decimal(str(buy_price)))
            sell_price_decimal = sell_market.quantize_order_price(sell_market_tuple.trading_pair, Decimal(str(sell_price)))
            
            # Execute both orders in rapid succession
            try:
                buy_order_id = self.c_buy_with_specific_market(
                    buy_market_tuple, quantized_amount,
                    order_type=buy_order_type,
                    price=buy_price_decimal,
                    expiration_seconds=self._next_trade_delay)
            except Exception as e:
                self._last_failure_timestamps[buy_market_tuple] = self._current_timestamp
                self.logger().warning(f"Error submitting buy limit order: {e}")
                return

            try:
                sell_order_id = self.c_sell_with_specific_market(
                    sell_market_tuple, quantized_amount,
                    order_type=sell_order_type,
                    price=sell_price_decimal,
                    expiration_seconds=self._next_trade_delay)
            except Exception as e:
                self._last_failure_timestamps[sell_market_tuple] = self._current_timestamp
                self.logger().warning(f"Error submitting sell limit order: {e}")
                # Note: We already placed the buy order! In a real production strategy, 
                # we might want to cancel it or let it ride. Here we proceed.
                return
            
            # Track orders
            buy_id_str = self._to_cpp_str(buy_order_id)
            sell_id_str = self._to_cpp_str(sell_order_id)
            self._order_timestamps[buy_id_str] = order_start_time
            self._order_timestamps[sell_id_str] = order_start_time
            
            # Log order placement latency for monitoring
            placement_latency = self._current_timestamp - order_start_time
            if placement_latency > 0.1:  # Log if latency exceeds 100ms
                self.logger().warning(
                    f"High order placement latency detected: {placement_latency:.3f}s. "
                    f"Consider colocating servers or optimizing network connectivity.")

    cdef void c_handle_order_completion(self, object order_event, bint is_buy) except *:
        """Unified order completion handler"""
        cdef:
            str order_id = order_event.order_id
            object market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            double time_elapsed
            string order_id_str = self._to_cpp_str(order_id)
            str order_type = "Buy" if is_buy else "Sell"
            
        # Also notify position balancer
        if self._position_balancer is not None:
            self._position_balancer.handle_order_completion(order_id, is_buy)
            
        # Remove from position balancer tracking if present
        self._position_balancer_orders.discard(order_id)

        if market_pair_tuple is None:
            # Might be a position balancer order or old order
            if order_id_str in self._order_timestamps:
                self._order_timestamps.erase(order_id_str)
            return
            
        try:
            # Check completion time
            if self._order_timestamps.find(order_id_str) != self._order_timestamps.end():
                time_elapsed = self._current_timestamp - self._order_timestamps[order_id_str]
                self.logger().info(f"{order_type} order {order_id} completed in {time_elapsed:.2f}s")
                self._order_timestamps.erase(order_id_str)
            
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(
                    logging.INFO,
                    f"{order_type} order completed on {market_pair_tuple[0].name}: {order_id}")
                
        except Exception as e:
            self.logger().error(f"Error handling {order_type.lower()} order completion: {e}", exc_info=True)

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        """Handle buy order completion"""
        self.c_handle_order_completion(buy_order_completed_event, True)
    
    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        """Handle sell order completion"""
        self.c_handle_order_completion(sell_order_completed_event, False)

    cdef c_did_fill_order(self, object order_filled_event):
        """Handle order fill event"""
        cdef:
            str order_id = order_filled_event.order_id
            double filled_amount = order_filled_event.amount
        
        # Notify position balancer
        if self._position_balancer is not None:
            self._position_balancer.handle_order_fill(order_id, filled_amount)
            
        # Track fill for timeout logic
        if order_id not in self._orders_with_fills:
            self._orders_with_fills.add(order_id)
            self._order_fill_timestamps[order_id] = self._current_timestamp

    cdef c_did_fail_order(self, object order_failed_event):
        """Handle order failure"""
        cdef:
            str order_id = order_failed_event.order_id
            string order_id_str = self._to_cpp_str(order_id)
            
        # Notify position balancer
        if self._position_balancer is not None:
            self._position_balancer.handle_order_completion(order_id, True) # Assume buy for cleanup
            self._position_balancer.handle_order_completion(order_id, False) # Assume sell for cleanup
            self._position_balancer_orders.discard(order_id)

        if self._order_timestamps.find(order_id_str) != self._order_timestamps.end():
            self._order_timestamps.erase(order_id_str)

    cdef c_did_cancel_order_tracker(self, object order_cancelled_event):
        """Handle order cancellation"""
        cdef:
            str order_id = order_cancelled_event.order_id
            string order_id_str = self._to_cpp_str(order_id)
            
        # Notify position balancer
        if self._position_balancer is not None:
            self._position_balancer.handle_order_cancellation(order_id)
            self._position_balancer_orders.discard(order_id)

        if self._order_timestamps.find(order_id_str) != self._order_timestamps.end():
            self._order_timestamps.erase(order_id_str)
            
        # Clean up fill tracking
        self._orders_with_fills.discard(order_id)
        self._order_fill_timestamps.pop(order_id, None)

    cdef void c_check_all_order_timeouts(self):
        """Check ALL pending orders for timeouts"""
        cdef:
            double time_elapsed
            string order_id_str
            double timestamp
            vector[string] to_remove
            str order_id
            
        for order_id_str, timestamp in self._order_timestamps:
            time_elapsed = self._current_timestamp - timestamp
            
            if time_elapsed > self._order_timeout:
                order_id = order_id_str.decode('utf-8')
                
                # Skip position balancer orders
                if order_id in self._position_balancer_orders:
                    continue
                    
                # Skip orders that have received fills (handled by filled timeout)
                if order_id in self._orders_with_fills:
                    continue
                
                self.logger().warning(f"Order {order_id} timed out after {time_elapsed:.2f}s")
                to_remove.push_back(order_id_str)
                
                # Mark as timeout cancelled
                self._timeout_cancelled_orders.add(order_id)
                
                # Force cancel
                # Need to find market tuple for this order
                market_pair = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
                if market_pair is not None:
                    try:
                        self.c_cancel_order(market_pair, order_id)
                    except Exception:
                        pass
        
        # Remove from tracking
        for order_id_str in to_remove:
            self._order_timestamps.erase(order_id_str)
            
    cdef void c_check_filled_order_timeouts(self):
        """Check filled orders for extended timeout"""
        cdef:
            double time_elapsed
            str order_id
            double timestamp
            list to_remove = []
            string order_id_str
            
        for order_id, timestamp in self._order_fill_timestamps.items():
            time_elapsed = self._current_timestamp - timestamp
            
            if time_elapsed > self._filled_order_timeout:
                self.logger().warning(f"Filled order {order_id} timed out after {time_elapsed:.2f}s")
                to_remove.append(order_id)
                
                # Mark as timeout cancelled
                self._timeout_cancelled_orders.add(order_id)
                
                # Force cancel
                market_pair = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
                if market_pair is not None:
                    try:
                        self.c_cancel_order(market_pair, order_id)
                    except Exception:
                        pass
                        
                # Clean up tracking
                order_id_str = self._to_cpp_str(order_id)
                self._order_timestamps.erase(order_id_str)

        # Remove from fill timestamps
        for order_id in to_remove:
            self._order_fill_timestamps.pop(order_id, None)
            self._orders_with_fills.discard(order_id)


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
        OrderBookEntryCPP *bid_entry_ptr
        OrderBookEntryCPP *ask_entry_ptr
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

    bid_entry_ptr = &deref(bid_it)
    ask_entry_ptr = &deref(ask_it)

    bid_leftover = bid_entry_ptr.getAmount()
    ask_leftover = ask_entry_ptr.getAmount()

    while levels_processed < max_levels and bid_it != bid_end and ask_it != ask_end:
        # Get prices (original, unconverted)
        orig_bid_price = bid_entry_ptr.getPrice()
        orig_ask_price = ask_entry_ptr.getPrice()

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
            bid_entry_ptr = &deref(bid_it)
            bid_leftover = bid_entry_ptr.getAmount()
            ask_leftover -= step_amount
        else:
            inc(ask_it)
            levels_processed += 1  
            if ask_it == ask_end:
                break
            ask_entry_ptr = &deref(ask_it)
            ask_leftover = ask_entry_ptr.getAmount()
            bid_leftover -= step_amount