# distutils: language=c++
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

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import TradeType, OrderType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage.arbitrage_market_pair import ArbitrageMarketPair
from hummingbot.core.rate_oracle.rate_oracle import RateOracle

# Constants
cdef:
    double DEFAULT_MIN_ORDER_USD = 10.0
    double DEFAULT_RATE_CACHE_DURATION = 10.0
    double DEFAULT_ORDER_WARNING_DELAY = 10.0
    size_t DEFAULT_MAX_TRACKED_ORDERS = 1000
    double EPSILON = 1e-10
    double RATE_LOG_INTERVAL = 300.0

as_logger = None


cdef class ArbitrageStrategy(StrategyBase):
    """
    Optimized arbitrage strategy with clean, uniform implementation.
    Uses doubles internally for performance, converts to Decimal only for external APIs.
    """
    
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_PROFITABILITY_STEP = 1 << 3
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 4
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
                    order_timeout: float = 600.0,
                    use_oracle_conversion_rate: bool = False,
                    secondary_to_primary_base_conversion_rate: Decimal = Decimal("1"),
                    secondary_to_primary_quote_conversion_rate: Decimal = Decimal("1"),
                    hb_app_notification: bool = False,
                    min_order_usd: float = DEFAULT_MIN_ORDER_USD,
                    rate_cache_duration: float = DEFAULT_RATE_CACHE_DURATION,
                    order_warning_delay: float = DEFAULT_ORDER_WARNING_DELAY,
                    max_tracked_orders: int = DEFAULT_MAX_TRACKED_ORDERS):
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
        
        # Profitability tracking
        self._current_profitability = pair[double, double](0.0, 0.0)
        
        # Notifications
        self._hb_app_notification = hb_app_notification
        
        # Clear order tracking
        self._order_timestamps.clear()
        
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

    cdef bint c_needs_conversion(self):
        """Check if conversion is needed - centralized logic"""
        if not self._use_oracle_conversion_rate:
            return self._fixed_base_rate != 1.0 or self._fixed_quote_rate != 1.0
        
        market_pair = self._market_pairs[0]
        return (market_pair.first.base_asset != market_pair.second.base_asset or
                market_pair.first.quote_asset != market_pair.second.quote_asset)

    cdef double c_get_conversion_rate(self, bint is_base_asset):
        """Get conversion rate for base or quote asset"""
        # Fast path: no conversion needed
        if not self.c_needs_conversion():
            return 1.0
        
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
        
        # Fast path: no conversion configured
        if not self.c_needs_conversion():
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

                assets_df = self.wallet_balance_data_frame([market_pair.first, market_pair.second])
                lines.extend(["", "  Assets:"] + ["    " + line for line in str(assets_df).split("\n")])

                # Show profitability
                lines.extend(["", "  Profitability (without fees):"])
                prof1 = self._current_profitability.first * 100
                prof2 = self._current_profitability.second * 100
                lines.append(f"    {market_pair.first.trading_pair} → {market_pair.second.trading_pair}: {prof1:+.4f}%")
                lines.append(f"    {market_pair.second.trading_pair} → {market_pair.first.trading_pair}: {prof2:+.4f}%")

                # Show pending orders
                if self.tracked_limit_orders or self.tracked_market_orders:
                    lines.extend(["", "  Pending orders:"])
                    total = len(self.tracked_limit_orders) + len(self.tracked_market_orders)
                    lines.append(f"    Total: {total}")
                else:
                    lines.extend(["", "  No pending orders."])

                warning_lines.extend(self.balance_warning([market_pair.first, market_pair.second]))

            if warning_lines:
                lines.extend(["", "  *** WARNINGS ***"] + warning_lines)
                
        except Exception as e:
            lines.append(f"  Error formatting status: {e}")

        return "\n".join(lines)

    cdef c_tick(self, double timestamp):
        """Main strategy tick - simplified and optimized"""
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report = ((current_tick > last_tick) and
                                  (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        
        try:
            # Check market readiness
            if not self.c_check_markets_ready(should_report):
                return
            
            # Process each market pair
            for market_pair in self._market_pairs:
                self.c_process_market_pair(market_pair)
            
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
            elif should_report:
                self.logger().info("Markets ready. Trading started.")
        
        # Check network status
        for market in self._sb_markets:
            if market.network_status is not NetworkStatus.CONNECTED:
                if should_report:
                    self.logger().warning("Markets not all online. No arbitrage trading permitted.")
                return False
        
        return True

    cdef void c_handle_order_completion(self, object order_event, bint is_buy) except *:
        """Unified order completion handler"""
        cdef:
            str order_id = order_event.order_id
            object market_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            double time_elapsed
            string order_id_str = order_id.encode('utf-8')
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
                        order_id_str = order_id.encode('utf-8')
                        
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
                            continue  # Skip to next order
                        
                        # Still waiting - warn if delayed
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

    cdef c_process_market_pair(self, object market_pair):
        """Process a market pair for arbitrage opportunities"""
        if not self.c_ready_for_new_orders([market_pair.first, market_pair.second]):
            return
        
        # Calculate profitability
        self._current_profitability = self.c_calculate_profitability(market_pair)
        
        # Check if profitable
        if (self._current_profitability.first < self._min_profitability and
            self._current_profitability.second < self._min_profitability):
            return
        
        # Log if verbose
        if self._logging_options & self.OPTION_LOG_PROFITABILITY_STEP:
            self.logger().debug(
                f"Profitability: {market_pair.first.trading_pair}→{market_pair.second.trading_pair}: "
                f"{self._current_profitability.first:.4%}, "
                f"{market_pair.second.trading_pair}→{market_pair.first.trading_pair}: "
                f"{self._current_profitability.second:.4%}")
        
        # Execute the more profitable direction immediately
        if self._current_profitability.second > self._current_profitability.first:
            self.c_execute_arbitrage(market_pair.first, market_pair.second)
        else:
            self.c_execute_arbitrage(market_pair.second, market_pair.first)

    cdef pair[double, double] c_calculate_profitability(self, object market_pair):
        """Calculate profitability for both arbitrage directions"""
        cdef:
            double bid1 = float(market_pair.first.get_price(False))
            double ask1 = float(market_pair.first.get_price(True))
            double bid2 = float(market_pair.second.get_price(False))
            double ask2 = float(market_pair.second.get_price(True))
            double conv_rate = 1.0
            
        # Sanity check - prices must be positive
        if bid1 <= 0 or ask1 <= 0 or bid2 <= 0 or ask2 <= 0:
            return pair[double, double](0.0, 0.0)
            
        # Get conversion rate if needed
        if self.c_needs_conversion():
            conv_rate = self.c_get_market_to_market_conversion_rate(market_pair.first, market_pair.second)
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
        quantized_buy = buy_market.c_quantize_order_amount(buy_market_tuple.trading_pair, dec_amount)
        quantized_sell = sell_market.c_quantize_order_amount(sell_market_tuple.trading_pair, dec_amount)
        quantized_amount = min(quantized_buy, quantized_sell)
        
        # Check minimum order size
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self._min_order_usd:
            return
        
        if quantized_amount > Decimal("0"):
            # Log order creation
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                self.log_with_clock(
                    logging.INFO,
                    f"Executing arbitrage: buy {quantized_amount:.8f} {buy_market_tuple.trading_pair} "
                    f"@ {buy_market.name}, sell @ {sell_market.name}, "
                    f"profitability: {profitability * 100:.2f}%")
            
            # Place both orders
            buy_order_type = buy_market.get_taker_order_type()
            sell_order_type = sell_market.get_taker_order_type()
            
            buy_order_id = self.c_buy_with_specific_market(
                buy_market_tuple, quantized_amount,
                order_type=buy_order_type,
                price=Decimal(str(buy_price)),
                expiration_seconds=self._next_trade_delay)
            
            sell_order_id = self.c_sell_with_specific_market(
                sell_market_tuple, quantized_amount,
                order_type=sell_order_type,
                price=Decimal(str(sell_price)),
                expiration_seconds=self._next_trade_delay)
            
            # Track orders
            buy_id_str = buy_order_id.encode('utf-8')
            sell_id_str = sell_order_id.encode('utf-8')
            self._order_timestamps[buy_id_str] = self._current_timestamp
            self._order_timestamps[sell_id_str] = self._current_timestamp

    cdef tuple c_find_best_profitable_amount(self, object buy_market_tuple, object sell_market_tuple):
        """Find best profitable amount - simplified and optimized"""
        cdef:
            ExchangeBase buy_market = buy_market_tuple.market
            ExchangeBase sell_market = sell_market_tuple.market
            double buy_quote_balance = float(buy_market.c_get_available_balance(buy_market_tuple.quote_asset))
            double sell_base_balance = float(sell_market.c_get_available_balance(sell_market_tuple.base_asset))
            double conv_rate = 1.0
            
        # Early exit if no balance
        if buy_quote_balance <= 0 or sell_base_balance <= 0:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Get conversion rate if needed
        if self.c_needs_conversion():
            conv_rate = self.c_get_market_to_market_conversion_rate(buy_market_tuple, sell_market_tuple)
        
        # Find profitable orders
        profitable_orders = c_find_profitable_arbitrage_orders(
            self._min_profitability,
            buy_market_tuple,
            sell_market_tuple,
            1.0,  # Buy conversion always 1.0 (primary market)
            conv_rate)
        
        if not profitable_orders:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Calculate best amount considering balances
        cdef:
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds = 0.0
            double total_proceeds_orig = 0.0
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
            double remaining_quote, remaining_base
            
        for bid_adj, ask_adj, orig_bid, orig_ask, amount in profitable_orders:
            # Calculate remaining capacity
            remaining_quote = buy_quote_balance - total_cost
            remaining_base = sell_base_balance - total_base
            
            # Early exit if no capacity left
            if remaining_quote <= EPSILON or remaining_base <= EPSILON:
                break
                
            # Adjust amount to available capacity
            if ask_adj > 0:
                amount = min(amount, remaining_quote / ask_adj, remaining_base)
            else:
                amount = min(amount, remaining_base)
                
            if amount <= EPSILON:
                continue
            
            # Update totals
            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds += bid_adj * amount
            total_proceeds_orig += orig_bid * amount
        
        # Calculate results
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
        if fabs(self._cached_base_rate - 1.0) > EPSILON:
            self.logger().info(f"Base conversion rate: {self._cached_base_rate:.6f}")
        if fabs(self._cached_quote_rate - 1.0) > EPSILON:
            self.logger().info(f"Quote conversion rate: {self._cached_quote_rate:.6f}")


# Single optimized function for finding profitable orders
cdef list c_find_profitable_arbitrage_orders(
    double min_profitability,
    object buy_market_tuple,
    object sell_market_tuple,
    double buy_conversion_rate,
    double sell_conversion_rate):
    """
    Find profitable arbitrage opportunities between two markets.
    Uses doubles for performance, applies conversion rates only when != 1.0
    """
    cdef:
        double step_amount = 0.0
        double bid_leftover = 0.0
        double ask_leftover = 0.0
        double bid_price, ask_price
        double orig_bid_price, orig_ask_price
        double min_prof_threshold = 1.0 + min_profitability
        bint needs_conversion = (fabs(buy_conversion_rate - 1.0) > EPSILON or 
                                fabs(sell_conversion_rate - 1.0) > EPSILON)
        object current_bid = None
        object current_ask = None
        list profitable_orders = []
        int orders_found = 0
        int max_orders = 100  # Limit number of orders for performance

    bid_it = sell_market_tuple.order_book_bid_entries()
    ask_it = buy_market_tuple.order_book_ask_entries()

    try:
        while orders_found < max_orders:
            # Advance iterators efficiently
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

            # Store original prices
            orig_bid_price = float(current_bid.price)
            orig_ask_price = float(current_ask.price)
            
            # Apply conversion if needed
            if needs_conversion:
                bid_price = orig_bid_price * sell_conversion_rate
                ask_price = orig_ask_price * buy_conversion_rate
            else:
                # Fast path - no conversion
                bid_price = orig_bid_price
                ask_price = orig_ask_price
            
            # Early exit conditions
            if bid_price <= ask_price:
                break
            if ask_price <= EPSILON:  # Avoid division by zero
                break
            if bid_price / ask_price < min_prof_threshold:
                break
            
            # Additional sanity checks for real-world robustness
            if bid_price <= 0 or ask_price <= 0:
                break
            if orig_bid_price <= 0 or orig_ask_price <= 0:
                break

            step_amount = min(bid_leftover, ask_leftover)
            if step_amount <= EPSILON:
                continue

            # Store results
            profitable_orders.append((
                bid_price,
                ask_price,
                orig_bid_price,
                orig_ask_price,
                step_amount))
            
            orders_found += 1
            ask_leftover -= step_amount
            bid_leftover -= step_amount

    except StopIteration:
        pass

    return profitable_orders