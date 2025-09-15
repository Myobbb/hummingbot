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
cimport cython
from libcpp.set cimport set as cpp_set
from cython.operator cimport(
    dereference as deref,
    postincrement as inc,
)

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import TradeType, OrderType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage_m.arbitrage_market_pair import ArbitrageMMarketPair
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.arbitrage_m.arbitrage_config_map import arbitrage_m_config_map
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.OrderBookEntry cimport OrderBookEntry

# Constants - Now configurable via init_params
cdef:
    double DEFAULT_MIN_ORDER_USD = 15.0
    double DEFAULT_RATE_CACHE_DURATION = 10.0
    double DEFAULT_ORDER_WARNING_DELAY = 10.0
    size_t DEFAULT_MAX_TRACKED_ORDERS = 1000
    double EPSILON = 1e-10
    double RATE_LOG_INTERVAL = 300.0

 


cdef class ArbitrageMStrategy(StrategyBase):
    """
    Optimized arbitrage strategy with clean, uniform implementation.
    Uses doubles internally for performance, converts to Decimal only for external APIs.
    """
    
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 4

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    def init_params(self,
                    market_pairs: List[ArbitrageMMarketPair],
                    min_profitability: Decimal,
                    logging_options: int = OPTION_LOG_ORDER_COMPLETED,
                    status_report_interval: float = 60.0,
                    next_trade_delay_interval: float = 3.0,
                    order_timeout: float = 600.0,
                    use_oracle_conversion_rate: bool = False,
                    secondary_to_primary_base_conversion_rate: Decimal = Decimal("1"),
                    secondary_to_primary_quote_conversion_rate: Decimal = Decimal("1"),
                    hb_app_notification: bool = False,
                    min_order_usd: float = DEFAULT_MIN_ORDER_USD,
                    rate_cache_duration: float = DEFAULT_RATE_CACHE_DURATION,
                    order_warning_delay: float = DEFAULT_ORDER_WARNING_DELAY,
                    max_tracked_orders: int = DEFAULT_MAX_TRACKED_ORDERS,
                    buy_in_enabled: bool = True,
                    buy_in_target_usd: float = 100.0,
                    buy_in_min_profitability: float = 0.005):
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
        self._order_warning_delay = order_warning_delay
        
        # Thresholds
        self._min_order_usd = min_order_usd
        self._rate_cache_duration = rate_cache_duration
        self._max_tracked_orders = max_tracked_orders
        
        # State tracking
        self._all_markets_ready = False
        self._last_timestamp = 0
        self._status_debounce_until = 0
        self._last_trade_timestamps = {}
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

        # Buy-in params/state
        self._buy_in_enabled = buy_in_enabled
        self._buy_in_target_usd = buy_in_target_usd
        self._buy_in_min_profitability = buy_in_min_profitability
        self._buy_in_completed = False
        
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
        cdef object primary_first = self._market_pairs[0].first
        cdef object primary_second = self._market_pairs[0].second

        # Base asset conversion (sell base -> buy base)
        if buy_market_tuple.base_asset != sell_market_tuple.base_asset:
            if (buy_market_tuple.base_asset == primary_first.base_asset and
                sell_market_tuple.base_asset == primary_second.base_asset):
                base_conv = self.c_get_conversion_rate(True)
            elif (buy_market_tuple.base_asset == primary_second.base_asset and
                  sell_market_tuple.base_asset == primary_first.base_asset):
                base_conv = 1.0 / self.c_get_conversion_rate(True) if self.c_get_conversion_rate(True) != 0 else 0.0
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
                quote_conv = 1.0 / self.c_get_conversion_rate(False) if self.c_get_conversion_rate(False) != 0 else 0.0
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

            # Buy-in status (aggregate per base asset across active markets)
            if self._buy_in_enabled:
                # Collect base assets from active market pairs (no unique tuple logic)
                try:
                    aset = set()
                    for mp in self._market_pairs:
                        aset.add(mp.first.base_asset)
                        aset.add(mp.second.base_asset)
                    base_assets = sorted(aset)
                except Exception:
                    base_assets = []
                agg_lines = []
                any_pending = False
                for a in base_assets:
                    # Get a reference bid from any active tuple with this base asset
                    bid = self.c_get_reference_bid_for_asset(a)
                    # Aggregate available base balance using the same source as the Assets table (no fallback)
                    total_base = 0.0
                    for t in unique_tuples:
                        if t.base_asset == a:
                            total_base += float(balance_map.get((t.market.name, a), 0.0))
                    total_value = total_base * bid
                    # Status should reflect the permanent global flag, not current valuation
                    is_pending = (not self._buy_in_completed)
                    if is_pending:
                        any_pending = True
                    agg_lines.append(f"    {a}: base={total_base:.6f} value={total_value:.6f} ({'pending' if is_pending else 'completed'})")
                if any_pending and self._buy_in_enabled:
                    lines.extend(["", f"  Buy-in: target={self._buy_in_target_usd:.6f} min_prof={self._buy_in_min_profitability * 100:.2f}%"]) 
                    lines.extend(agg_lines)

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
            if not self.c_check_markets_ready(should_report):
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
                if self._buy_in_enabled:
                    # Quick skip if buy-in already completed
                    if not self._buy_in_completed:
                        if self.c_handle_buy_in(best_buy, best_sell):
                            # buy-in executed; skip regular arbitrage this tick
                            pass
                        else:
                            self.c_execute_arbitrage(best_buy, best_sell)
                    else:
                        self.c_execute_arbitrage(best_buy, best_sell)
                else:
                    self.c_execute_arbitrage(best_buy, best_sell)
            elif self._buy_in_enabled and best_buy is not None and best_sell is not None:
                # Not enough edge for normal arbitrage. Still try buy-in using its own threshold,
                # and also try the reversed pairing in case that direction offers cheaper buys.
                if not self._buy_in_completed:
                    # Attempt with current best direction
                    if self.c_books_ready_for_direction(best_buy, best_sell):
                        self.c_handle_buy_in(best_buy, best_sell)
                # Also attempt reversed (sell,buy) to source cheapest asks elsewhere for the same base asset
                if not self._buy_in_completed:
                    if self.c_books_ready_for_direction(best_sell, best_buy):
                        self.c_handle_buy_in(best_sell, best_buy)
            elif self._buy_in_enabled and best_buy is None:
                # No arbitrageable pair found (likely due to zero sell-side base). Proactively scan all ordered
                # pairs to try buy-in on any asset/venue where shortfall and edge allow it.
                if not self._buy_in_completed:
                    for market_pair in self._market_pairs:
                        if self.c_books_ready_for_direction(market_pair.first, market_pair.second):
                            if self.c_handle_buy_in(market_pair.first, market_pair.second):
                                break

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
                # Run one-time global buy-in completion check now that markets are ready (always log once)
                if self._buy_in_enabled:
                    self.log_with_clock(logging.INFO, "Buy-in config enabled at startup; performing completion check.")
                    self.c_scan_and_mark_buyin_completion()
                else:
                    self.log_with_clock(logging.INFO, "Buy-in config disabled at startup.")
        
        # Check network status
        for market in self._sb_markets:
            if market.network_status is not NetworkStatus.CONNECTED:
                if should_report:
                    self.logger().warning("Markets not all online. No arbitrage trading permitted.")
                return False
        
        return True
    cdef double c_get_reference_bid_for_asset(self, str asset_key):
        """Return a non-zero bid for the given base asset from any active market tuple, or 0.0 if none."""
        cdef:
            double last_bid = 0.0
            ExchangeBase _ex
            OrderBook _ob
            cpp_set[OrderBookEntry].reverse_iterator _bid_it
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


    cdef void c_handle_order_completion(self, object order_event, bint is_buy) except *:
        """Unified order completion handler"""
        cdef:
            str order_id = order_event.order_id
            object market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            double time_elapsed
            string order_id_str = self._to_cpp_str(order_id)
            str order_type = "Buy" if is_buy else "Sell"
            
        if market_pair_tuple is None:
            self.logger().warning(f"{order_type} order {order_id} completed but market pair not found")
            return
            
        try:
            self._last_trade_timestamps[market_pair_tuple] = self._current_timestamp
            
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

    cdef bint c_ready_for_new_orders(self, list market_tuples):
        """Check if ready for new orders - simplified"""
        cdef:
            double time_elapsed
            string order_id_str
            object order_id
            
        # Check pending orders
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
                            continue  # Skip to next order, don't return False for this timed-out orde
                        else:
                            # Still waiting
                            if time_elapsed > self._order_warning_delay:
                                self.logger().warning(f"Order {order_id} pending for {time_elapsed:.2f}s")
                            return False
            
            # Check cool-off period
            if market_tuple in self._last_trade_timestamps:
                time_left = (self._last_trade_timestamps[market_tuple] + 
                           self._next_trade_delay - self._current_timestamp)
                if time_left > 0:
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
            
        # Compute conversion rate only if assets differ; callee returns 1.0 in no-op cases
        if (market_pair.first.quote_asset != market_pair.second.quote_asset or
            market_pair.first.base_asset != market_pair.second.base_asset):
            conv_rate = self.c_get_market_to_market_conversion_rate(market_pair.first, market_pair.second)
        # Apply conversion (sell-side and buy-side)
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
            
        if amount <= 0:
            if self._logging_options & self.OPTION_LOG_INSUFFICIENT_ASSET:
                self.logger().info("Insufficient balance or no profitable amount found")
            return
        
        # Convert to Decimal for market operations
        dec_amount = Decimal(str(amount))
        
        # Quantize amounts
        # Use connector Python API for quantization to support all exchanges
        quantized_buy = buy_market.quantize_order_amount(buy_market_tuple.trading_pair, dec_amount)
        quantized_sell = sell_market.quantize_order_amount(sell_market_tuple.trading_pair, dec_amount)
        quantized_amount = min(quantized_buy, quantized_sell)
        
        # Check minimum order size
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self._min_order_usd:
            return
        # Declare variables before the if block (Cython requirement)
        cdef double order_start_time
        cdef object buy_order_type
        cdef object sell_order_type
        cdef object buy_price_decimal
        cdef object sell_price_decimal
        cdef double placement_latency

        if quantized_amount > Decimal("0"):
            # Log timing for latency monitoring
            order_start_time = self._current_timestamp
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                self.log_with_clock(
                    logging.INFO,
                    f"Executing arbitrage: buy {quantized_amount:.8f} {buy_market_tuple.trading_pair} "
                    f"@ {buy_market.name}, sell @ {sell_market.name}, "
                    f"profitability: {profitability * 100:.2f}%")
            
            # CRITICAL: Place both orders with minimal latency
            # The price is passed even for market orders as some connectors use it
            # to calculate the correct amount (especially for quote currency market orders)
            
            # Pre-calculate all parameters to minimize latency between orders
            buy_order_type = buy_market.get_taker_order_type()
            sell_order_type = sell_market.get_taker_order_type()
            buy_price_decimal = Decimal(str(buy_price))
            sell_price_decimal = Decimal(str(sell_price))
            
            # Execute both orders in rapid succession
            # This is the best we can do in Cython without async support
            # The actual network calls happen inside the exchange connectors
            buy_order_id = self.c_buy_with_specific_market(
                buy_market_tuple, quantized_amount,
                order_type=buy_order_type,
                price=buy_price_decimal,
                expiration_seconds=self._next_trade_delay)
            
            # Immediately place the sell order - minimal delay
            sell_order_id = self.c_sell_with_specific_market(
                sell_market_tuple, quantized_amount,
                order_type=sell_order_type,
                price=sell_price_decimal,
                expiration_seconds=self._next_trade_delay)
            
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
            bint needs_conversion = False
        # Compute conversion rate only if assets differ; callee returns 1.0 in no-op cases
        if (buy_market_tuple.quote_asset != sell_market_tuple.quote_asset or
            buy_market_tuple.base_asset != sell_market_tuple.base_asset):
            conv_rate = self.c_get_market_to_market_conversion_rate(buy_market_tuple, sell_market_tuple)
        needs_conversion = (fabs(conv_rate - 1.0) > EPSILON)

        # Read top-of-book via C-level order book to avoid exceptions
        cdef ExchangeBase buy_ex = buy_market_tuple.market
        cdef ExchangeBase sell_ex = sell_market_tuple.market
        cdef OrderBook buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        cdef OrderBook sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)
        if sell_ob._bid_book.size() == 0 or buy_ob._ask_book.size() == 0:
            return pair[int, double](False, conv_rate)
        cdef cpp_set[OrderBookEntry].reverse_iterator bid_it = sell_ob._bid_book.rbegin()
        cdef cpp_set[OrderBookEntry].iterator ask_it = buy_ob._ask_book.begin()
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
        # Early exit if no balance
        buy_quote_balance = float(buy_market.c_get_available_balance(buy_market_tuple.quote_asset))
        sell_base_balance = float(sell_market.c_get_available_balance(sell_market_tuple.base_asset))
        if buy_quote_balance <= EPSILON or sell_base_balance <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Early uniform gate: skip deeper work if top-of-book fails, and reuse conv_rate
        gate_res = self.c_top_of_book_profitable_get_conv(buy_market_tuple, sell_market_tuple, self._min_profitability)
        if not gate_res.first:
            return (0.0, 0.0, 0.0, 0.0)
        conv_rate = gate_res.second

        # conv_rate already obtained from top-of-book gate
        
        # Calculate capacity limits once
        cdef double max_base_amount = self._calculate_capacity_limit(
            buy_market_tuple, sell_market_tuple,
            buy_quote_balance, sell_base_balance)
        
        if max_base_amount <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Get profitable orders (includes top-of-book check) with capacity-aware early-stop
        profitable_orders = c_find_profitable_arbitrage_orders(
            self._min_profitability,
            buy_market_tuple,
            sell_market_tuple,
            1.0,  # Buy conversion always 1.0
            conv_rate,
            max_base_amount,
            0.05,
            False)
        
        if not profitable_orders:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Aggregate profitable volume
        cdef:
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds_orig = 0.0
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
        
        for bid_adj, ask_adj, orig_bid, orig_ask, amount in profitable_orders:
            # Apply constraints
            amount = min(amount, 
                        max_base_amount - total_base,
                        (buy_quote_balance - total_cost) / ask_adj if ask_adj > 0 else 0)
            
            if amount <= EPSILON:
                continue
            
            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds_orig += orig_bid * amount
            
            # Stop if we've used all capacity
            if total_base >= max_base_amount - EPSILON:
                break
        
        # Calculate results
        if total_base > EPSILON:
            avg_sell_price_orig = total_proceeds_orig / total_base
            avg_buy_price = total_cost / total_base
            profitability = ((avg_sell_price_orig * conv_rate) / avg_buy_price - 1.0) if avg_buy_price > 0 else 0.0
            
            # Check minimum notional
            if avg_sell_price_orig * total_base >= self._min_order_usd:
                return (total_base, profitability, avg_sell_price_orig, avg_buy_price)
        
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
        cdef cpp_set[OrderBookEntry].iterator _ask_it2
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
 
 
 
    cdef void c_maybe_disable_buy_in(self):
        """Disable buy-in globally for this run once target is reached (do not mutate config)."""
        if self._buy_in_completed and self._buy_in_enabled:
            self._buy_in_enabled = False
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.log_with_clock(logging.INFO, "Buy-in completed. Disabling buy-in for this session.")

    cdef void c_scan_and_mark_buyin_completion(self):
        """Re-evaluate the base asset against target and disable buy-in when done."""
        if not self._buy_in_enabled or self._buy_in_completed:
            return
        cdef:
            str asset_key
            double last_bid = 0.0
            double base_bal
            pair[double, double] val_short
            double current_value_quote
            double shortfall
            list unique_tuples
            object assets_df
            dict balance_map
        # Determine the single base asset from the first market pair
        asset_key = self._market_pairs[0].first.base_asset
        # Get a non-zero bid from any active tuple with this base asset
        last_bid = self.c_get_reference_bid_for_asset(asset_key)
        # Build balances from the same source as status for consistency (shared helper)
        unique_tuples, assets_df, balance_map = self.c_build_unique_tuples_assets_and_balance_map()
        # Sum base using the same map
        base_bal = 0.0
        for t in unique_tuples:
            if t.base_asset == asset_key:
                base_bal += float(balance_map.get((t.market.name, asset_key), 0.0))
        val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        current_value_quote = val_short.first
        shortfall = val_short.second
        # Concise info log about startup buy-in state and decision
        try:
            decision = "disable" if (current_value_quote >= self._buy_in_target_usd or (shortfall > 0 and shortfall < self._min_order_usd)) else "keep"
            self.log_with_clock(
                logging.INFO,
                f"Buy-in check: asset={asset_key} base={base_bal:.6f} bid={last_bid:.6f} value={current_value_quote:.6f} target={self._buy_in_target_usd:.6f} -> {decision}")
        except Exception:
            pass
        if self.c_try_mark_complete_buy_in(asset_key, current_value_quote, shortfall):
            pass
        self.c_maybe_disable_buy_in()

    cdef pair[double, double] c_compute_value_and_shortfall(self,
                                                           double base_balance,
                                                           double last_bid):
        """Return (current_value_quote, shortfall)."""
        cdef double current_value_quote = base_balance * last_bid
        cdef double shortfall = 0.0
        if current_value_quote < self._buy_in_target_usd:
            shortfall = self._buy_in_target_usd - current_value_quote
        else:
            shortfall = 0.0
        return pair[double, double](current_value_quote, shortfall)

    cdef double c_get_aggregated_base_balance(self, str asset):
        """Aggregate base balance consistently using the same source as startup status (assets_df/balance_map)."""
        cdef:
            double total = 0.0
            list unique_tuples
            object assets_df
            dict balance_map
            object t
        try:
            unique_tuples, assets_df, balance_map = self.c_build_unique_tuples_assets_and_balance_map()
            for t in unique_tuples:
                if t.base_asset == asset:
                    total += float(balance_map.get((t.market.name, asset), 0.0))
        except Exception:
            return 0.0
        return total

    cdef bint c_try_mark_complete_buy_in(self,
                                         str pair,
                                         double current_value_quote,
                                         double shortfall):
        """Mark buy-in as completed if at target or remaining < min notional (single asset)."""
        if current_value_quote >= self._buy_in_target_usd:
            self._buy_in_completed = True
            self.c_maybe_disable_buy_in()
            return True
        if shortfall > 0 and shortfall < self._min_order_usd:
            self._buy_in_completed = True
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.log_with_clock(logging.INFO, f"Buy-in considered complete on {pair}: shortfall {shortfall:.6f} < min notional {self._min_order_usd:.6f}")
            self.c_maybe_disable_buy_in()
            return True
        return False

    cdef bint c_handle_buy_in(self, object buy_market_tuple, object sell_market_tuple):
        """
        If base asset holdings on buy_market_tuple are below target (in that market's quote units), place a taker buy
        using the same amount-finding logic as arbitrage execution, gated by a separate buy-in profitability.
        Returns True if a buy-in was placed; False otherwise.
        """
        if not self._buy_in_enabled:
            return False
        cdef:
            str pair_str = buy_market_tuple.trading_pair
            ExchangeBase market = buy_market_tuple.market #object market = buy_market_tuple.market
            double base_bal = 0.0
            double quote_bal = float(market.c_get_available_balance(buy_market_tuple.quote_asset))
            double best_amount
            double best_prof
            double sell_price
            double buy_price
            tuple res
            str asset_key = buy_market_tuple.base_asset

        if self._buy_in_completed:
            return False

        # Evaluate progress vs target and early complete (aggregate base across all markets)
        # Get a reliable bid for the asset from any active tuple to avoid zero-bid stalls
        cdef double last_bid = self.c_get_reference_bid_for_asset(asset_key)
        base_bal = self.c_get_aggregated_base_balance(asset_key)
        cdef pair[double, double] val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        cdef double current_value_quote = val_short.first
        cdef double shortfall = val_short.second
        if self.c_try_mark_complete_buy_in(pair_str, current_value_quote, shortfall):
            return False

        # Require quote balance to spend and a minimal edge
        if quote_bal <= 0:
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.log_with_clock(logging.INFO, f"Buy-in skipped on {pair_str}: no quote balance to spend")
            return False

        res = self.c_find_best_buyin_amount(
            buy_market_tuple,
            sell_market_tuple,
            quote_bal,
            shortfall
        )
        best_amount = <double>res[0]
        best_prof = <double>res[1]
        sell_price = <double>res[2]
        buy_price = <double>res[3]

        if best_amount <= 0 or best_prof < self._buy_in_min_profitability:
            return False

        # Place only the buy leg on buy market
        cdef object order_type = market.get_taker_order_type()
        cdef object quantized_amount = market.quantize_order_amount(buy_market_tuple.trading_pair, Decimal(str(best_amount)))
        # Ensure not exceeding available quote after quantization
        cdef double max_affordable = 0.0
        if buy_price > 0:
            max_affordable = quote_bal / buy_price
        if float(quantized_amount) > max_affordable:
            quantized_amount = market.quantize_order_amount(buy_market_tuple.trading_pair, Decimal(str(max(0.0, max_affordable - 1e-12))))
        if quantized_amount <= Decimal("0"):
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.log_with_clock(logging.INFO, f"Buy-in skipped on {pair_str}: quantized amount is zero after affordability check")
            return False

        # Enforce minimum notional like normal arbitrage orders
        cdef double volume_usd = float(quantized_amount) * buy_price
        if volume_usd < self._min_order_usd:
            # If we cannot place a valid minimum-size order, mark complete only if remaining shortfall is under min
            if self.c_try_mark_complete_buy_in(pair_str, current_value_quote, shortfall):
                return False
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.log_with_clock(logging.INFO, f"Buy-in skipped on {pair_str}: order notional {volume_usd:.6f} < min {self._min_order_usd:.6f}")
            return False

        buy_order_id = self.c_buy_with_specific_market(
            buy_market_tuple,
            quantized_amount,
            order_type=order_type,
            price=Decimal(str(buy_price)),
            expiration_seconds=self._next_trade_delay)

        # Track order timestamp for housekeeping
        cdef string buy_id_str = self._to_cpp_str(buy_order_id)
        self._order_timestamps[buy_id_str] = self._current_timestamp

        # Check if target reached after placing (aggregate across all markets)
        # Use the same reliable bid lookup as above
        last_bid = self.c_get_reference_bid_for_asset(asset_key)
        base_bal = self.c_get_aggregated_base_balance(asset_key)
        val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        current_value_quote = val_short.first
        shortfall = val_short.second
        if self.c_try_mark_complete_buy_in(pair_str, current_value_quote, shortfall):
            self.c_maybe_disable_buy_in()

        return True

    cdef tuple c_find_best_buyin_amount(self,
                                        object buy_market_tuple,
                                        object sell_market_tuple,
                                        double buy_quote_balance,
                                        double max_spend_quote):
        """
        Compute best buy-only amount using cross-market price edge, ignoring sell-side base balance limits.
        Caps spend by both available quote balance and provided max_spend_quote.
        Returns (amount_base, profitability, avg_sell_price_orig, avg_buy_price).
        """
        cdef:
            double conv_rate = 1.0
            double spend_cap = min(buy_quote_balance, max_spend_quote)
        if spend_cap <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Early uniform gate for buy-in; reuse conv_rate
        gate_res2 = self.c_top_of_book_profitable_get_conv(buy_market_tuple, sell_market_tuple, self._buy_in_min_profitability)
        if not gate_res2.first:
            return (0.0, 0.0, 0.0, 0.0)
        conv_rate = gate_res2.second

        # conv_rate already obtained from top-of-book gate

        # Determine an upper bound on base we might buy given spend_cap and quantization on buy side
        cdef double approx_ask = 0.0
        cdef ExchangeBase _buy_ex3 = buy_market_tuple.market
        cdef OrderBook _buy_ob3 = _buy_ex3.c_get_order_book(buy_market_tuple.trading_pair)
        cdef cpp_set[OrderBookEntry].iterator _ask_it3
        if _buy_ob3._ask_book.size() > 0:
            _ask_it3 = _buy_ob3._ask_book.begin()
            approx_ask = deref(_ask_it3).getPrice()
        cdef double buy_cap_base = 0.0
        if approx_ask > 0.0 and spend_cap > 0.0:
            buy_cap_base = spend_cap / approx_ask
            q = buy_market_tuple.market.quantize_order_amount(
                buy_market_tuple.trading_pair,
                Decimal(str(max(0.0, buy_cap_base - 1e-12))))
            if q is not None:
                buy_cap_base = float(q)
            else:
                buy_cap_base = 0.0

        # Use buy-in profitability threshold here (not the main arbitrage threshold), with capacity-aware early-stop
        profitable_orders = c_find_profitable_arbitrage_orders(
            self._buy_in_min_profitability,
            buy_market_tuple,
            sell_market_tuple,
            1.0,
            conv_rate,
            buy_cap_base,
            0.05,
            False)

        if not profitable_orders:
            return (0.0, 0.0, 0.0, 0.0)

        cdef:
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds = 0.0
            double total_proceeds_orig = 0.0
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
            double remaining_quote

        for bid_adj, ask_adj, orig_bid, orig_ask, amount in profitable_orders:
            remaining_quote = spend_cap - total_cost
            if remaining_quote <= EPSILON:
                break
            if ask_adj > 0:
                amount = min(amount, remaining_quote / ask_adj)
            if amount <= EPSILON:
                continue
            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds += bid_adj * amount
            total_proceeds_orig += orig_bid * amount

        if total_base > EPSILON and total_cost > EPSILON:
            avg_bid_adj = total_proceeds / total_base
            avg_bid_orig = total_proceeds_orig / total_base
            avg_ask = total_cost / total_base
            profitability = (avg_bid_adj / avg_ask - 1.0) if avg_ask > EPSILON else 0.0
            return (total_base, profitability, avg_bid_orig, avg_ask)

        return (0.0, 0.0, 0.0, 0.0)

    

    cdef void c_cleanup_old_orders(self):
        """Clean up old order tracking data"""
        cdef:
            double cutoff = self._current_timestamp - (self._order_timeout * 2)
            list to_remove = []
            string order_id_str
            double timestamp
            
        # Only clean up if we have orders to check
        if self._order_timestamps.size() == 0:
            return
            
        # Find old entries
        for order_id_str, timestamp in self._order_timestamps:
            if timestamp < cutoff:
                to_remove.append(order_id_str)
        
        # Remove old entries
        for order_id_str in to_remove:
            self._order_timestamps.erase(order_id_str)
        
        # Warn if too many tracked orders
        if self._order_timestamps.size() > self._max_tracked_orders:
            self.logger().warning(f"Tracked orders exceed limit: {self._order_timestamps.size()}")

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



cdef list c_find_profitable_arbitrage_orders(
    double min_profitability,
    object buy_market_tuple,
    object sell_market_tuple,
    double buy_conversion_rate,
    double sell_conversion_rate,
    double target_base_amount,
    double overshoot_ratio,
    bint perform_top_check):
    """
    Find profitable arbitrage opportunities between two markets.
    Returns list of (bid_adj, ask_adj, bid_orig, ask_orig, amount).
    """
    cdef:
        double min_prof_threshold = 1.0 + min_profitability
        # Conversion flags not needed; always apply provided conversion rates
        list profitable_orders = []
        int max_levels = 20  # Reduced from 100
        int levels_processed = 0
        double bid_leftover = 0.0
        double ask_leftover = 0.0
        double step_amount = 0.0
        double cumulative_base = 0.0
        double overshoot_stop = 0.0
        # Hoisted C-level declarations for Cython (not allowed inside try:)
        ExchangeBase buy_ex
        ExchangeBase sell_ex
        OrderBook buy_ob
        OrderBook sell_ob
        cpp_set[OrderBookEntry].reverse_iterator bid_it
        cpp_set[OrderBookEntry].reverse_iterator bid_end
        cpp_set[OrderBookEntry].iterator ask_it
        cpp_set[OrderBookEntry].iterator ask_end
        OrderBookEntry bid_entry
        OrderBookEntry ask_entry
        double orig_bid_price
        double orig_ask_price
        double bid_price
        double ask_price
        
    # Optional top-of-book profitability check (callers can pre-gate to avoid duplicate work)
    if perform_top_check:
        try:
            top_bid = float(sell_market_tuple.get_price(False))
            top_ask = float(buy_market_tuple.get_price(True))
            top_bid_adj = top_bid * sell_conversion_rate
            top_ask_adj = top_ask * buy_conversion_rate
            if top_bid_adj / top_ask_adj < min_prof_threshold:
                return []
        except Exception:
            return []
    
    # Prepare capacity-aware stopping condition (optional)
    if target_base_amount > 0.0:
        overshoot_stop = target_base_amount * (1.0 + overshoot_ratio)

    # Now scan the books (C-level iteration to avoid Python iterator overhead)
    try:
        buy_ex = <ExchangeBase> buy_market_tuple.market
        sell_ex = <ExchangeBase> sell_market_tuple.market
        buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)

        bid_it = sell_ob._bid_book.rbegin()
        bid_end = sell_ob._bid_book.rend()
        ask_it = buy_ob._ask_book.begin()
        ask_end = buy_ob._ask_book.end()

        if bid_it == bid_end or ask_it == ask_end:
            return []

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
                profitable_orders.append((
                    bid_price,      # Adjusted bid
                    ask_price,      # Adjusted ask
                    orig_bid_price, # Original bid
                    orig_ask_price, # Original ask
                    step_amount
                ))

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
                if ask_it == ask_end:
                    break
                ask_entry = deref(ask_it)
                ask_leftover = ask_entry.getAmount()
                bid_leftover -= step_amount

    except Exception:
        return []
    
    return profitable_orders