# distutils: language=c++
# distutils: extra_compile_args=-Wno-psabi
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False

import logging
from decimal import Decimal
import pandas as pd
from typing import List, Tuple
from libc.stdint cimport int64_t
from libc.math cimport fabs
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair
from cython.operator cimport dereference, preincrement
cimport cython

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage.arbitrage_market_pair import ArbitrageMarketPair
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.client.performance import PerformanceMetrics

# Constants
cdef:
    object s_decimal_0 = Decimal(0)
    object s_decimal_1 = Decimal(1)
    double RATE_CACHE_DURATION = 10.0  # Cache conversion rates for 10 seconds
    double MIN_ORDER_USD = 10.0  # Minimum order size in USD
    double EPSILON = 1e-10  # Small value for float comparisons
    double RATE_LOG_INTERVAL = 300.0  # Log conversion rates every 5 minutes
    double ORDER_WARNING_DELAY = 10.0  # Warn if order pending longer than this
    size_t MAX_TRACKED_ORDERS = 1000  # Maximum tracked orders to prevent memory leaks

as_logger = None


cdef class ArbitrageStrategy(StrategyBase):
    
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_PROFITABILITY_STEP = 1 << 3
    OPTION_LOG_FULL_PROFITABILITY_STEP = 1 << 4
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 5
    OPTION_LOG_ALL = 0xfffffffffffffff

    @classmethod
    def logger(cls):
        global as_logger
        if as_logger is None:
            as_logger = logging.getLogger(__name__)
        return as_logger

    def init_params(self,
                    market_pairs: List[ArbitrageMarketPair],
                    min_profitability: Decimal,
                    logging_options: int = OPTION_LOG_ORDER_COMPLETED,
                    status_report_interval: float = 60.0,
                    next_trade_delay_interval: float = 3.0,
                    failed_order_tolerance: int = 1,
                    order_timeout: float = 600.0,
                    use_oracle_conversion_rate: bool = False,
                    secondary_to_primary_base_conversion_rate: Decimal = Decimal("1"),
                    secondary_to_primary_quote_conversion_rate: Decimal = Decimal("1"),
                    hb_app_notification: bool = False):
        """Initialize arbitrage strategy parameters"""
        if not market_pairs:
            raise ValueError("market_pairs must not be empty.")
        
        # Core configuration
        self._logging_options = logging_options
        self._market_pairs = market_pairs
        self._min_profitability = min_profitability
        self._min_profitability_float = float(min_profitability)  # Cache for fast comparison
        
        # Timing configuration
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._next_trade_delay = next_trade_delay_interval
        self._order_timeout = order_timeout
        
        # State tracking
        self._all_markets_ready = False
        self._last_trade_timestamps = {}
        self._failed_order_tolerance = failed_order_tolerance
        self._cool_off_logged = False
        self._current_profitability = ()
        self._current_profitability_fast = pair[double, double](0.0, 0.0)
        
        # Conversion rate configuration
        self._use_oracle_conversion_rate = use_oracle_conversion_rate
        self._secondary_to_primary_base_conversion_rate = secondary_to_primary_base_conversion_rate
        self._secondary_to_primary_quote_conversion_rate = secondary_to_primary_quote_conversion_rate
        
        # Cache initialization
        self._last_conv_rates_logged = 0
        self._cached_base_rate = 1.0
        self._cached_quote_rate = 1.0
        self._cached_market_rate = 1.0
        self._last_rate_update = 0
        # Top-of-book cache
        self._tob_first_bid = 0.0
        self._tob_first_ask = 0.0
        self._tob_second_bid = 0.0
        self._tob_second_ask = 0.0
        self._tob_timestamp = -1.0
        # Memory management
        self._last_cleanup_timestamp = 0
        self._max_tracked_orders = MAX_TRACKED_ORDERS
        
        # Notifications
        self._hb_app_notification = hb_app_notification
        
        # Clear C++ map for order timestamps
        self._order_timestamps_cpp.clear()
        
        # Validate configuration
        self._validate_configuration()

        # Add markets
        cdef set all_markets = {
            market
            for market_pair in self._market_pairs
            for market in [market_pair.first.market, market_pair.second.market]
        }
        self.c_add_markets(list(all_markets))
    
    cdef void _validate_configuration(self):
        """Validate strategy configuration parameters"""
        if self._min_profitability < Decimal("-1"):
            raise ValueError("min_profitability cannot be less than -100%")
        if self._order_timeout <= 0:
            raise ValueError("order_timeout must be positive")
        if self._failed_order_tolerance < 0:
            raise ValueError("failed_order_tolerance cannot be negative")
        if self._next_trade_delay < 0:
            raise ValueError("next_trade_delay cannot be negative")

    @property
    def min_profitability(self) -> Decimal:
        return self._min_profitability

    @property
    def use_oracle_conversion_rate(self) -> bool:
        return self._use_oracle_conversion_rate

    @property
    def tracked_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    @property
    def tracked_market_orders(self) -> List[Tuple[ExchangeBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    @property
    def tracked_limit_orders_data_frame(self) -> pd.DataFrame:
        return self._sb_order_tracker.tracked_limit_orders_data_frame

    @property
    def tracked_market_orders_data_frame(self) -> pd.DataFrame:
        return self._sb_order_tracker.tracked_market_orders_data_frame

    cdef void c_update_cached_rates(self):
        """Update cached conversion rates if expired"""
        cdef double current_time = self._current_timestamp
        
        if current_time - self._last_rate_update < RATE_CACHE_DURATION:
            return
            
        if not self._use_oracle_conversion_rate:
            self._cached_base_rate = float(self._secondary_to_primary_base_conversion_rate)
            self._cached_quote_rate = float(self._secondary_to_primary_quote_conversion_rate)
        else:
            # Get rates from oracle
            if self._market_pairs[0].second.quote_asset != self._market_pairs[0].first.quote_asset:
                quote_pair = f"{self._market_pairs[0].second.quote_asset}-{self._market_pairs[0].first.quote_asset}"
                self._cached_quote_rate = float(RateOracle.get_instance().get_pair_rate(quote_pair))
            else:
                self._cached_quote_rate = 1.0
                
            if self._market_pairs[0].second.base_asset != self._market_pairs[0].first.base_asset:
                base_pair = f"{self._market_pairs[0].second.base_asset}-{self._market_pairs[0].first.base_asset}"
                self._cached_base_rate = float(RateOracle.get_instance().get_pair_rate(base_pair))
            else:
                self._cached_base_rate = 1.0
        
        # Update market conversion rate
        if self._cached_base_rate > EPSILON:
            self._cached_market_rate = self._cached_quote_rate / self._cached_base_rate
        else:
            self._cached_market_rate = 1.0
            
        self._last_rate_update = current_time

    cdef double c_get_cached_market_rate(self, object market_info):
        """Get cached market conversion rate"""
        # Primary market always has rate 1.0
        if market_info == self._market_pairs[0].first:
            return 1.0
        # Secondary market uses cached conversion rate
        return self._cached_market_rate

    cdef void c_update_top_of_book_cache(self, object market_pair):
        """Cache top-of-book prices (as doubles) once per tick for both markets."""
        if self._tob_timestamp == self._current_timestamp:
            return
        self._tob_first_bid = float(market_pair.first.get_price(False))
        self._tob_first_ask = float(market_pair.first.get_price(True))
        self._tob_second_bid = float(market_pair.second.get_price(False))
        self._tob_second_ask = float(market_pair.second.get_price(True))
        self._tob_timestamp = self._current_timestamp

    def get_second_to_first_conversion_rate(self) -> Tuple[str, str, Decimal, str, str, Decimal]:
        """Get conversion rates from secondary to primary market"""
        self.c_update_cached_rates()
        
        first_market = self._market_pairs[0].first
        second_market = self._market_pairs[0].second
        
        quote_pair = f"{second_market.quote_asset}-{first_market.quote_asset}"
        base_pair = f"{second_market.base_asset}-{first_market.base_asset}"
        
        rate_source = "oracle" if self._use_oracle_conversion_rate else "fixed"
        
        return (quote_pair, rate_source, Decimal(str(self._cached_quote_rate)),
                base_pair, rate_source, Decimal(str(self._cached_base_rate)))

    def log_conversion_rates(self):
        """Log conversion rates if they differ from 1:1"""
        if not self._use_oracle_conversion_rate:
            return
            
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_second_to_first_conversion_rate()
            
        if fabs(self._cached_quote_rate - 1.0) > EPSILON:
            self.logger().info(f"{quote_pair} ({quote_rate_source}) rate: {PerformanceMetrics.smart_round(quote_rate)}")
        if fabs(self._cached_base_rate - 1.0) > EPSILON:
            self.logger().info(f"{base_pair} ({base_rate_source}) rate: {PerformanceMetrics.smart_round(base_rate)}")

    def oracle_status_df(self):
        """Generate DataFrame with oracle/conversion rate status"""
        if not self._use_oracle_conversion_rate:
            return pd.DataFrame(columns=["Source", "Pair", "Rate"])
            
        columns = ["Source", "Pair", "Rate"]
        data = []
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_second_to_first_conversion_rate()
            
        if quote_pair.split("-")[0] != quote_pair.split("-")[1]:
            data.append([quote_rate_source, quote_pair, PerformanceMetrics.smart_round(quote_rate)])
        if base_pair.split("-")[0] != base_pair.split("-")[1]:
            data.append([base_rate_source, base_pair, PerformanceMetrics.smart_round(base_rate)])
            
        return pd.DataFrame(data=data, columns=columns)

    def format_status(self) -> str:
        """Format strategy status for display"""
        cdef:
            list lines = []
            list warning_lines = []
            
        try:
            for market_pair in self._market_pairs:
                warning_lines.extend(self.network_warning([market_pair.first, market_pair.second]))

                markets_df = self.market_status_data_frame([market_pair.first, market_pair.second])
                lines.extend(["", "  Markets:"] + ["    " + line for line in str(markets_df).split("\n")])

                # Show conversion rates if using oracle
                if self._use_oracle_conversion_rate:
                    oracle_df = self.oracle_status_df()
                    if not oracle_df.empty:
                        lines.extend(["", "  Rate conversion:"] + ["    " + line for line in str(oracle_df).split("\n")])

                assets_df = self.wallet_balance_data_frame([market_pair.first, market_pair.second])
                lines.extend(["", "  Assets:"] + ["    " + line for line in str(assets_df).split("\n")])

                # Show profitability with better formatting
                lines.extend(["", "  Profitability (without fees):"])
                
                # Direction 1: Buy first market, sell second market
                prof1 = self._current_profitability_fast.first * 100
                lines.append(f"    {market_pair.first.trading_pair} → {market_pair.second.trading_pair}: {prof1:+.4f}%")
                
                # Direction 2: Buy second market, sell first market  
                prof2 = self._current_profitability_fast.second * 100
                lines.append(f"    {market_pair.second.trading_pair} → {market_pair.first.trading_pair}: {prof2:+.4f}%")

                # Show pending orders
                tracked_limit_orders = self.tracked_limit_orders
                tracked_market_orders = self.tracked_market_orders

                if tracked_limit_orders or tracked_market_orders:
                    lines.extend(["", "  Pending orders:"])
                    total_orders = len(tracked_limit_orders) + len(tracked_market_orders)
                    lines.append(f"    Total: {total_orders} ({len(tracked_limit_orders)} limit, {len(tracked_market_orders)} market)")
                    
                    # Show order details if not too many
                    if total_orders <= 10:
                        if tracked_limit_orders:
                            lines.extend(["    " + line for line in str(self.tracked_limit_orders_data_frame).split("\n")])
                        if tracked_market_orders:
                            lines.extend(["    " + line for line in str(self.tracked_market_orders_data_frame).split("\n")])
                else:
                    lines.extend(["", "  No pending orders."])
                    
                # Add memory stats if verbose logging
                if self._logging_options & self.OPTION_LOG_FULL_PROFITABILITY_STEP:
                    mem_stats = self.get_memory_stats()
                    lines.extend(["", "  Memory stats:",
                                f"    Tracked orders: {mem_stats['tracked_orders']}/{mem_stats['max_orders']}",
                                f"    Cache age: {mem_stats['cache_age']:.1f}s"])

                warning_lines.extend(self.balance_warning([market_pair.first, market_pair.second]))

            if warning_lines:
                lines.extend(["", "  *** WARNINGS ***"] + warning_lines)
                
        except Exception as e:
            lines.append(f"  Error formatting status: {e}")
            self.logger().error("Error in format_status", exc_info=True)

        return "\n".join(lines)

    def notify_hb_app(self, msg: str):
        """Send notification to HummingBot app if enabled"""
        if self._hb_app_notification:
            super().notify_hb_app(msg)

    @cython.cdivision(True)
    cdef bint c_all_markets_ready(self):
        """Check if all markets are ready"""
        # Note: In real implementation, this would need GIL for market.ready check
        # Simplified for demonstration
        return self._all_markets_ready

    cdef c_tick(self, double timestamp):
        """Main strategy tick"""
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        try:
            # Check market readiness
            if not self._all_markets_ready:
                self._all_markets_ready = all([market.ready for market in self._sb_markets])
                if not self._all_markets_ready:
                    if should_report_warnings:
                        self.logger().warning("Markets not ready. No arbitrage trading permitted.")
                    return
                elif should_report_warnings:
                    self.logger().info("Markets ready. Trading started.")

            # Check network connectivity
            for market in self._sb_markets:
                if market.network_status is not NetworkStatus.CONNECTED:
                    if should_report_warnings:
                        self.logger().warning("Markets not all online. No arbitrage trading permitted.")
                    return

            # Update cached rates periodically
            self.c_update_cached_rates()

            # Process each market pair
            for market_pair in self._market_pairs:
                self.c_process_market_pair(market_pair)
                
            # Periodic cleanup (every 60 seconds)
            if self._current_timestamp - self._last_cleanup_timestamp > 60.0:
                self.c_cleanup_old_orders()
                
                # Log conversion rates periodically
            if self._use_oracle_conversion_rate and self._last_conv_rates_logged + RATE_LOG_INTERVAL < timestamp:
                self.log_conversion_rates()
                self._last_conv_rates_logged = timestamp
                
        finally:
            self._last_timestamp = timestamp

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        """Handle buy order completion"""
        cdef:
            object buy_order = buy_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(buy_order.order_id)
            string order_id_str = buy_order.order_id.encode('utf-8')
            double time_elapsed
            
        if market_trading_pair_tuple is None:
            self.logger().warning(f"Buy order {buy_order.order_id} completed but market pair not found")
            return
            
        try:
            self._last_trade_timestamps[market_trading_pair_tuple] = self._current_timestamp
            
            # Check and log order completion time
            if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                self.logger().info(f"Buy order {buy_order.order_id} completed in {time_elapsed:.2f}s")
                self._order_timestamps_cpp.erase(order_id_str)
            
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                   f"Buy order completed on {market_trading_pair_tuple[0].name}: {buy_order.order_id}")
                self.notify_hb_app_with_timestamp(
                    f"{buy_order.base_asset_amount:.8f} {buy_order.base_asset}-{buy_order.quote_asset} "
                    f"buy completed on {market_trading_pair_tuple[0].name}")
        except Exception as e:
            self.logger().error(f"Error handling buy order completion: {e}", exc_info=True)
    
    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        """Handle sell order completion"""
        cdef:
            object sell_order = sell_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(sell_order.order_id)
            string order_id_str = sell_order.order_id.encode('utf-8')
            double time_elapsed
            
        if market_trading_pair_tuple is None:
            self.logger().warning(f"Sell order {sell_order.order_id} completed but market pair not found")
            return
            
        try:
            self._last_trade_timestamps[market_trading_pair_tuple] = self._current_timestamp
            
            # Check and log order completion time
            if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                self.logger().info(f"Sell order {sell_order.order_id} completed in {time_elapsed:.2f}s")
                self._order_timestamps_cpp.erase(order_id_str)
            
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                   f"Sell order completed on {market_trading_pair_tuple[0].name}: {sell_order.order_id}")
                self.notify_hb_app_with_timestamp(
                    f"{sell_order.base_asset_amount:.8f} {sell_order.base_asset}-{sell_order.quote_asset} "
                    f"sell completed on {market_trading_pair_tuple[0].name}")
        except Exception as e:
            self.logger().error(f"Error handling sell order completion: {e}", exc_info=True)
                
    cdef c_did_cancel_order(self, object cancel_event):
        """Handle order cancellation"""
        cdef:
            str order_id = cancel_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            
        if market_trading_pair_tuple is not None:
            self.log_with_clock(logging.INFO,
                               f"Order canceled on {market_trading_pair_tuple[0].name}: {order_id}")

    @cython.cdivision(True)
    cdef pair[double, double] c_calculate_profitability_fast(self, object market_pair):
        """Fast profitability calculation using cached rates and doubles"""
        cdef:
            double market_1_bid = self._tob_first_bid
            double market_1_ask = self._tob_first_ask
            double market_2_bid = self._cached_market_rate * self._tob_second_bid
            double market_2_ask = self._cached_market_rate * self._tob_second_ask
            double prof_buy_2_sell_1
            double prof_buy_1_sell_2

        # Ensure we have valid prices
        if market_1_bid <= 0 or market_1_ask <= 0 or market_2_bid <= 0 or market_2_ask <= 0:
            return pair[double, double](0.0, 0.0)

        # Calculate profitability for both directions
        # Direction 1: Buy on market 2, sell on market 1
        prof_buy_2_sell_1 = market_1_bid / market_2_ask - 1.0
        # Direction 2: Buy on market 1, sell on market 2
        prof_buy_1_sell_2 = market_2_bid / market_1_ask - 1.0

        return pair[double, double](prof_buy_2_sell_1, prof_buy_1_sell_2)

    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair):
        """Calculate arbitrage profitability with Decimal precision"""
        cdef:
            object market_1_bid_price = market_pair.first.get_price(False)
            object market_1_ask_price = market_pair.first.get_price(True)
            object market_2_bid_price = self.market_conversion_rate(market_pair.second) * \
                market_pair.second.get_price(False)
            object market_2_ask_price = self.market_conversion_rate(market_pair.second) * \
                market_pair.second.get_price(True)
                
        profitability_buy_2_sell_1 = market_1_bid_price / market_2_ask_price - 1
        profitability_buy_1_sell_2 = market_2_bid_price / market_1_ask_price - 1
        return profitability_buy_2_sell_1, profitability_buy_1_sell_2

    cdef bint c_ready_for_new_orders(self, list market_trading_pair_tuples):
        """Check if ready for new orders with optimized timeout handling"""
        cdef:
            double time_left, time_elapsed, ready_to_trade_time
            dict tracked_taker_orders
            string order_id_str
            list keys_to_remove = []
            object order_id, order

        # Iterate limit and market orders separately to avoid dict merges
        for market_trading_pair_tuple in market_trading_pair_tuples:
            # Check limit orders for this tuple
            orders = self._sb_order_tracker.c_get_limit_orders().get(market_trading_pair_tuple, {})
            if orders:
                for order_id, order in orders.items():
                    order_id_str = order_id.encode('utf-8')
                    
                    # Initialize timestamp if new order
                    if self._order_timestamps_cpp.find(order_id_str) == self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp[order_id_str] = self._current_timestamp
                        self.logger().info(f"Tracking new order: {order_id}")

                    time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                    
                    # Handle timeout
                    if time_elapsed > self._order_timeout:
                        self.logger().warning(
                            f"Order {order_id} timed out after {time_elapsed:.2f}s "
                            f"(timeout: {self._order_timeout}s)")
                        keys_to_remove.append((market_trading_pair_tuple, order_id))
                    else:
                        # Still waiting for order
                        log_level = logging.WARNING if time_elapsed > ORDER_WARNING_DELAY else logging.INFO
                        self.log_with_clock(log_level,
                                          f"Order {order_id} pending for {time_elapsed:.2f}s")
                        return False

                # Clean up timed-out limit orders
                for mkt_tuple, order_id in keys_to_remove:
                    order_id_str = order_id.encode('utf-8')
                    if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp.erase(order_id_str)
                    self._sb_order_tracker.c_stop_tracking_limit_order(mkt_tuple, order_id)
                keys_to_remove.clear()

            # Check market orders for this tuple
            orders = self._sb_order_tracker.c_get_market_orders().get(market_trading_pair_tuple, {})
            if orders:
                for order_id, order in orders.items():
                    order_id_str = order_id.encode('utf-8')
                    if self._order_timestamps_cpp.find(order_id_str) == self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp[order_id_str] = self._current_timestamp
                        self.logger().info(f"Tracking new order: {order_id}")

                    time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                    if time_elapsed > self._order_timeout:
                        self.logger().warning(
                            f"Order {order_id} timed out after {time_elapsed:.2f}s "
                            f"(timeout: {self._order_timeout}s)")
                        keys_to_remove.append((market_trading_pair_tuple, order_id))
                    else:
                        log_level = logging.WARNING if time_elapsed > ORDER_WARNING_DELAY else logging.INFO
                        self.log_with_clock(log_level,
                                          f"Order {order_id} pending for {time_elapsed:.2f}s")
                        return False

                # Clean up timed-out market orders
                for mkt_tuple, order_id in keys_to_remove:
                    order_id_str = order_id.encode('utf-8')
                    if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp.erase(order_id_str)
                    self._sb_order_tracker.c_stop_tracking_market_order(mkt_tuple, order_id)
                keys_to_remove.clear()

            # Check cool-off period
            ready_to_trade_time = self._last_trade_timestamps.get(market_trading_pair_tuple, 0) + self._next_trade_delay
            if market_trading_pair_tuple in self._last_trade_timestamps and ready_to_trade_time > self._current_timestamp:
                time_left = ready_to_trade_time - self._current_timestamp
                if not self._cool_off_logged:
                    self.log_with_clock(logging.INFO,
                                       f"Cool-off on {market_trading_pair_tuple.market.name}: {int(time_left)}s remaining")
                    self._cool_off_logged = True
                return False

        if self._cool_off_logged:
            self.log_with_clock(logging.INFO, "Cool-off complete. Ready for new orders.")
            self._cool_off_logged = False

        return True

    cdef c_process_market_pair(self, object market_pair):
        """Process a market pair for arbitrage opportunities"""
        if not self.c_ready_for_new_orders([market_pair.first, market_pair.second]):
            return

        # Update top-of-book cache once per tick for both legs
        self.c_update_top_of_book_cache(market_pair)

        # Fast profitability check using doubles
        self._current_profitability_fast = self.c_calculate_profitability_fast(market_pair)
        
        # Early exit if not profitable
        if (self._current_profitability_fast.first < self._min_profitability_float and
            self._current_profitability_fast.second < self._min_profitability_float):
            return
        
        # Log profitability if verbose logging enabled
        if self._logging_options & self.OPTION_LOG_PROFITABILITY_STEP:
            self.logger().debug(
                f"Profitability: buy {market_pair.first.trading_pair} sell {market_pair.second.trading_pair}: "
                f"{self._current_profitability_fast.first:.4%}, "
                f"buy {market_pair.second.trading_pair} sell {market_pair.first.trading_pair}: "
                f"{self._current_profitability_fast.second:.4%}")
        
        # Calculate precise profitability for execution
        self._current_profitability = self.c_calculate_arbitrage_top_order_profitability(market_pair)

        # Execute the more profitable direction
        if self._current_profitability[1] > self._current_profitability[0]:
            self.c_process_market_pair_inner(market_pair.first, market_pair.second)
        else:
            self.c_process_market_pair_inner(market_pair.second, market_pair.first)

    cdef c_process_market_pair_inner(self, object buy_market_trading_pair_tuple, 
                                     object sell_market_trading_pair_tuple):
        """Execute arbitrage trades with enhanced error handling"""
        cdef:
            object best_amount, best_profitability, sell_price, buy_price
            object quantized_buy_amount, quantized_sell_amount, quantized_order_amount
            double volume_usd
            ExchangeBase buy_market = buy_market_trading_pair_tuple.market
            ExchangeBase sell_market = sell_market_trading_pair_tuple.market
            string buy_order_id_str, sell_order_id_str

        try:
            # Find best profitable amount
            best_amount, best_profitability, sell_price, buy_price = self.c_find_best_profitable_amount(
                buy_market_trading_pair_tuple, sell_market_trading_pair_tuple)
            
            if best_amount <= s_decimal_0:
                if self._logging_options & self.OPTION_LOG_INSUFFICIENT_ASSET:
                    self.logger().info("Insufficient balance or no profitable amount found")
                return
            
            # Quantize amounts
            quantized_buy_amount = buy_market.c_quantize_order_amount(
                buy_market_trading_pair_tuple.trading_pair, best_amount)
            quantized_sell_amount = sell_market.c_quantize_order_amount(
                sell_market_trading_pair_tuple.trading_pair, best_amount)
            quantized_order_amount = min(quantized_buy_amount, quantized_sell_amount)
            
            # Check minimum order size
            volume_usd = float(quantized_order_amount * sell_price)
            if volume_usd < MIN_ORDER_USD:
                return
                
            if quantized_order_amount > s_decimal_0:
                if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                    self.log_with_clock(logging.INFO,
                        f"Executing arbitrage: buy {quantized_order_amount:.8f} {buy_market_trading_pair_tuple.trading_pair} "
                        f"@ {buy_market_trading_pair_tuple.market.name}, "
                        f"sell @ {sell_market_trading_pair_tuple.market.name}, "
                        f"profitability: {float(best_profitability - 1) * 100:.2f}%")
                
                # Get order types
                buy_order_type = buy_market_trading_pair_tuple.market.get_taker_order_type()
                sell_order_type = sell_market_trading_pair_tuple.market.get_taker_order_type()
            
                # Place orders
                buy_order_id = self.c_buy_with_specific_market(
                    buy_market_trading_pair_tuple, quantized_order_amount,
                    order_type=buy_order_type, price=buy_price, 
                    expiration_seconds=self._next_trade_delay)
                sell_order_id = self.c_sell_with_specific_market(
                    sell_market_trading_pair_tuple, quantized_order_amount,
                    order_type=sell_order_type, price=sell_price, 
                    expiration_seconds=self._next_trade_delay)
                
                # Track order timestamps
                buy_order_id_str = buy_order_id.encode('utf-8')
                sell_order_id_str = sell_order_id.encode('utf-8')
                self._order_timestamps_cpp[buy_order_id_str] = self._current_timestamp
                self._order_timestamps_cpp[sell_order_id_str] = self._current_timestamp
                
                self.logger().info(f"Orders placed: buy={buy_order_id}, sell={sell_order_id}")
                
                if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                    self.logger().info(self.format_status())
                    
        except Exception as e:
            self.logger().error(
                f"Error executing arbitrage between {buy_market_trading_pair_tuple.market.name} "
                f"and {sell_market_trading_pair_tuple.market.name}: {e}", 
                exc_info=True)

    @staticmethod
    def find_profitable_arbitrage_orders(min_profitability: Decimal,
                                         buy_market_trading_pair: MarketTradingPairTuple,
                                         sell_market_trading_pair: MarketTradingPairTuple,
                                         buy_market_conversion_rate,
                                         sell_market_conversion_rate):
        """Public interface for finding profitable orders"""
        return c_find_profitable_arbitrage_orders(
            min_profitability,
            buy_market_trading_pair,
            sell_market_trading_pair,
            buy_market_conversion_rate,
            sell_market_conversion_rate)

    def market_conversion_rate(self, market_info: MarketTradingPairTuple) -> Decimal:
        """Get market conversion rate"""
        self.c_update_cached_rates()
        if market_info == self._market_pairs[0].first:
            return s_decimal_1
        return Decimal(str(self._cached_market_rate))

    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair_tuple, 
                                            object sell_market_trading_pair_tuple):
        """Find best profitable amount with balance checks"""
        cdef:
            double conversion_rate
            
        # Update cached rates if needed
        self.c_update_cached_rates()
        
        # Get appropriate conversion rate
        conversion_rate = self.c_get_cached_market_rate(sell_market_trading_pair_tuple)
        
        # Use fast implementation
        return self.c_find_best_profitable_amount_fast_no_fees(
            buy_market_trading_pair_tuple,
            sell_market_trading_pair_tuple,
            1.0,  # Buy market always has conversion rate 1.0
            conversion_rate)

    cdef tuple c_find_best_profitable_amount_fast_no_fees(self,
                                                         object buy_market_trading_pair_tuple,
                                                         object sell_market_trading_pair_tuple,
                                                         double buy_conversion_rate,
                                                         double sell_conversion_rate):
        """Optimized amount finder with balance checks and min profitability filter"""
        cdef:
            double min_prof_threshold = 1.0 + self._min_profitability_float
            double total_base = 0.0
            double bid_leftover = 0.0
            double ask_leftover = 0.0
            double step_amount = 0.0
            double vwap_buy_cost = 0.0
            double vwap_sell_proc = 0.0
            double bid_price_adj, ask_price_adj
            double buy_quote_bal
            double sell_base_bal
            object best_amount = s_decimal_0
            object best_prof = s_decimal_0
            object best_bid = s_decimal_0
            object best_ask = s_decimal_0
            object current_bid = None
            object current_ask = None
            ExchangeBase buy_market = buy_market_trading_pair_tuple.market
            ExchangeBase sell_market = sell_market_trading_pair_tuple.market
            bint has_profitable_amount = False

        # Read balances once per tick as doubles
        buy_quote_bal = float(buy_market.c_get_available_balance(buy_market_trading_pair_tuple.quote_asset))
        sell_base_bal = float(sell_market.c_get_available_balance(sell_market_trading_pair_tuple.base_asset))

        bid_it = sell_market_trading_pair_tuple.order_book_bid_entries()
        ask_it = buy_market_trading_pair_tuple.order_book_ask_entries()

        try:
            while True:
                if bid_leftover <= EPSILON and ask_leftover <= EPSILON:
                    current_bid = next(bid_it)
                    current_ask = next(ask_it)
                    ask_leftover = float(current_ask.amount)
                    bid_leftover = float(current_bid.amount)
                elif bid_leftover > EPSILON and ask_leftover <= EPSILON:
                    current_ask = next(ask_it)
                    ask_leftover = float(current_ask.amount)
                elif ask_leftover > EPSILON and bid_leftover <= EPSILON:
                    current_bid = next(bid_it)
                    bid_leftover = float(current_bid.amount)

                bid_price_adj = float(current_bid.price) * sell_conversion_rate
                ask_price_adj = float(current_ask.price) * buy_conversion_rate

                if bid_price_adj <= ask_price_adj:
                    break
                if bid_price_adj / ask_price_adj < min_prof_threshold:
                    break

                step_amount = min(bid_leftover, ask_leftover)
                if step_amount <= EPSILON:
                    continue

                total_base += step_amount
                vwap_buy_cost += ask_price_adj * step_amount
                vwap_sell_proc += bid_price_adj * step_amount

                # Balance checks against tick-level balances
                if (buy_quote_bal < vwap_buy_cost) or (sell_base_bal < total_base):
                    # Clip to available balances
                    if vwap_buy_cost > 0.0 and total_base > 0.0:
                        avg_ask = vwap_buy_cost / total_base
                        buy_limited = buy_quote_bal / avg_ask
                    else:
                        buy_limited = 0.0
                    limited = min(sell_base_bal, buy_limited)
                    if limited > EPSILON and total_base > 0.0:
                        avg_bid = vwap_sell_proc / total_base
                        avg_ask = vwap_buy_cost / total_base
                        best_amount = Decimal(str(limited))
                        best_prof = Decimal(str(avg_bid / avg_ask)) if avg_ask > 0.0 else s_decimal_0
                        best_bid = Decimal(str(avg_bid))
                        best_ask = Decimal(str(avg_ask))
                    break

                # Update best as we go
                if vwap_buy_cost > 0.0 and total_base > 0.0:
                    ratio = (vwap_sell_proc / vwap_buy_cost)
                    if ratio > min_prof_threshold:
                        avg_bid = vwap_sell_proc / total_base
                        avg_ask = vwap_buy_cost / total_base
                        best_amount = Decimal(str(total_base))
                        best_prof = Decimal(str(ratio))
                        best_bid = Decimal(str(avg_bid))
                        best_ask = Decimal(str(avg_ask))

                ask_leftover -= step_amount
                bid_leftover -= step_amount

        except StopIteration:
            pass

        return best_amount, best_prof, best_bid, best_ask

    # Public methods for unit tests
    def find_best_profitable_amount(self, buy_market: MarketTradingPairTuple, 
                                   sell_market: MarketTradingPairTuple):
        return self.c_find_best_profitable_amount(buy_market, sell_market)

    def ready_for_new_orders(self, market_pair):
        return self.c_ready_for_new_orders(market_pair)
    
    cdef void c_cleanup_old_orders(self):
        """Clean up old order tracking data to prevent memory leaks"""
        cdef:
            double cutoff_time = self._current_timestamp - (self._order_timeout * 2)
            list keys_to_remove = []
            string order_id_str
            object order_id
            
        # Check both limit and market orders for cleanup
        all_orders = {}
        for market_pair, orders in self._sb_order_tracker.c_get_limit_orders().items():
            all_orders.update(orders)
        for market_pair, orders in self._sb_order_tracker.c_get_market_orders().items():
            all_orders.update(orders)
            
        # Find tracked orders that are no longer in order tracker
        for order_id in list(all_orders.keys()):
            order_id_str = order_id.encode('utf-8')
            if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                if self._order_timestamps_cpp[order_id_str] < cutoff_time:
                    keys_to_remove.append(order_id_str)
                    
        # Clean up old entries
        for order_id_str in keys_to_remove:
            self._order_timestamps_cpp.erase(order_id_str)
            
        # Log if we have too many tracked orders
        if self._order_timestamps_cpp.size() > self._max_tracked_orders:
            self.logger().warning(
                f"Order tracking exceeds limit: {self._order_timestamps_cpp.size()}/{self._max_tracked_orders}. "
                f"Consider reducing order frequency or increasing timeout.")
                
        self._last_cleanup_timestamp = self._current_timestamp

    def get_memory_stats(self) -> dict:
        """Get memory usage statistics"""
        cdef size_t tracked_count = self._order_timestamps_cpp.size()
        return {
            "tracked_orders": tracked_count,
            "max_orders": self._max_tracked_orders,
            "cache_age": self._current_timestamp - self._last_rate_update,
            "last_cleanup": self._current_timestamp - self._last_cleanup_timestamp
        }


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef list c_find_profitable_arbitrage_orders_fast(double min_profitability,
                                                  object buy_market_trading_pair_tuple,
                                                  object sell_market_trading_pair_tuple,
                                                  double buy_conversion_rate,
                                                  double sell_conversion_rate):
    """Optimized order finding using doubles for performance"""
    cdef:
        double step_amount = 0.0
        double bid_leftover = 0.0
        double ask_leftover = 0.0
        double bid_price_adj, ask_price_adj
        double min_prof_threshold = 1.0 + min_profitability
        object current_bid = None
        object current_ask = None
        list profitable_orders = []

    bid_it = sell_market_trading_pair_tuple.order_book_bid_entries()
    ask_it = buy_market_trading_pair_tuple.order_book_ask_entries()

    try:
        while True:
            # Advance iterators as needed
            if bid_leftover <= EPSILON and ask_leftover <= EPSILON:
                current_bid = next(bid_it)
                current_ask = next(ask_it)
                ask_leftover = float(current_ask.amount)
                bid_leftover = float(current_bid.amount)
            elif bid_leftover > EPSILON and ask_leftover <= EPSILON:
                current_ask = next(ask_it)
                ask_leftover = float(current_ask.amount)
            elif ask_leftover > EPSILON and bid_leftover <= EPSILON:
                current_bid = next(bid_it)
                bid_leftover = float(current_bid.amount)

            # Apply conversion and check profitability
            bid_price_adj = float(current_bid.price) * sell_conversion_rate
            ask_price_adj = float(current_ask.price) * buy_conversion_rate
            
            # Early exit conditions
            if bid_price_adj <= ask_price_adj:
                break
            if min_profitability < 0 and bid_price_adj / ask_price_adj < min_prof_threshold:
                break

            step_amount = min(bid_leftover, ask_leftover)
            if step_amount <= EPSILON:
                continue

            # Store as Decimal for compatibility
            profitable_orders.append((
                Decimal(str(bid_price_adj)),
                Decimal(str(ask_price_adj)),
                current_bid.price,
                current_ask.price,
                Decimal(str(step_amount))))

            ask_leftover -= step_amount
            bid_leftover -= step_amount

    except StopIteration:
        pass

    return profitable_orders


cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate):
    """Original function with Decimal precision for complex conversions"""
    cdef:
        object step_amount = s_decimal_0
        object bid_leftover = s_decimal_0
        object ask_leftover = s_decimal_0
        object current_bid = None
        object current_ask = None
        object bid_price_adj, ask_price_adj
        list profitable_orders = []

    bid_it = sell_market_trading_pair_tuple.order_book_bid_entries()
    ask_it = buy_market_trading_pair_tuple.order_book_ask_entries()

    try:
        while True:
            # Advance iterators
            if bid_leftover == s_decimal_0 and ask_leftover == s_decimal_0:
                current_bid = next(bid_it)
                current_ask = next(ask_it)
                ask_leftover = current_ask.amount
                bid_leftover = current_bid.amount
            elif bid_leftover > s_decimal_0 and ask_leftover == s_decimal_0:
                current_ask = next(ask_it)
                ask_leftover = current_ask.amount
            elif ask_leftover > s_decimal_0 and bid_leftover == s_decimal_0:
                current_bid = next(bid_it)
                bid_leftover = current_bid.amount
            elif bid_leftover > s_decimal_0 and ask_leftover > s_decimal_0:
                pass
            else:
                break

            # Apply conversion rates
            bid_price_adj = current_bid.price * sell_market_conversion_rate
            ask_price_adj = current_ask.price * buy_market_conversion_rate
            
            # Check profitability
            if bid_price_adj < ask_price_adj:
                break
            if min_profitability < 0 and bid_price_adj/ask_price_adj < (s_decimal_1 + min_profitability):
                break

            step_amount = min(bid_leftover, ask_leftover)
            if step_amount == s_decimal_0:
                continue

            profitable_orders.append((
                bid_price_adj,
                ask_price_adj,
                current_bid.price,
                current_ask.price,
                step_amount))

            ask_leftover -= step_amount
            bid_leftover -= step_amount

    except StopIteration:
        pass

    return profitable_orders