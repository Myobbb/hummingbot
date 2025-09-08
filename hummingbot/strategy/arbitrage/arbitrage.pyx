# distutils: language=c++
import logging
from decimal import Decimal
import pandas as pd
from typing import (
    List,
    Tuple,
)
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
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

s_decimal_0 = Decimal(0)
s_decimal_1 = Decimal(1)
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
        """
        :param market_pairs: list of arbitrage market pairs
        :param min_profitability: minimum profitability limit
        :param logging_options: select the types of logs to output
        :param status_report_interval: how often to report network connection warnings
        :param next_trade_delay_interval: cool off period between trades
        :param failed_order_tolerance: number of failed orders to force stop
        :param order_timeout: timeout for pending orders
        :param use_oracle_conversion_rate: whether to use oracle for cross-currency conversion
        :param secondary_to_primary_base_conversion_rate: manual conversion rate for base
        :param secondary_to_primary_quote_conversion_rate: manual conversion rate for quote
        :param hb_app_notification: enable client notifications
        """
        if len(market_pairs) <= 0:
            raise ValueError(f"market_pairs must not be empty.")
        
        self._logging_options = logging_options
        self._market_pairs = market_pairs
        self._min_profitability = min_profitability
        self._all_markets_ready = False
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._next_trade_delay = next_trade_delay_interval
        self._last_trade_timestamps = {}
        self._failed_order_tolerance = failed_order_tolerance
        self._cool_off_logged = False
        self._current_profitability = ()
        self._use_oracle_conversion_rate = use_oracle_conversion_rate
        self._secondary_to_primary_base_conversion_rate = secondary_to_primary_base_conversion_rate
        self._secondary_to_primary_quote_conversion_rate = secondary_to_primary_quote_conversion_rate
        self._last_conv_rates_logged = 0
        self._hb_app_notification = hb_app_notification
        self._order_timeout = order_timeout
        
        # Clear C++ map for order timestamps
        self._order_timestamps_cpp.clear()

        cdef:
            set all_markets = {
                market
                for market_pair in self._market_pairs
                for market in [market_pair.first.market, market_pair.second.market]
            }

        self.c_add_markets(list(all_markets))

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

    def get_second_to_first_conversion_rate(self) -> Tuple[str, Decimal, str, Decimal]:
        """
        Find conversion rates from secondary market to primary market
        """
        quote_rate = s_decimal_1
        quote_pair = f"{self._market_pairs[0].second.quote_asset}-{self._market_pairs[0].first.quote_asset}"
        quote_rate_source = "fixed"
        
        if self._use_oracle_conversion_rate:
            if self._market_pairs[0].second.quote_asset != self._market_pairs[0].first.quote_asset:
                quote_rate_source = RateOracle.get_instance().source.name
                quote_rate = RateOracle.get_instance().get_pair_rate(quote_pair)
        else:
            quote_rate = self._secondary_to_primary_quote_conversion_rate
            
        base_rate = s_decimal_1
        base_pair = f"{self._market_pairs[0].second.base_asset}-{self._market_pairs[0].first.base_asset}"
        base_rate_source = "fixed"
        
        if self._use_oracle_conversion_rate:
            if self._market_pairs[0].second.base_asset != self._market_pairs[0].first.base_asset:
                base_rate_source = RateOracle.get_instance().source.name
                base_rate = RateOracle.get_instance().get_pair_rate(base_pair)
        else:
            base_rate = self._secondary_to_primary_base_conversion_rate
            
        return quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate

    def log_conversion_rates(self):
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_second_to_first_conversion_rate()
        if quote_pair.split("-")[0] != quote_pair.split("-")[1] and quote_rate != s_decimal_1:
            self.logger().info(f"{quote_pair} ({quote_rate_source}) conversion rate: {PerformanceMetrics.smart_round(quote_rate)}")
        if base_pair.split("-")[0] != base_pair.split("-")[1] and base_rate != s_decimal_1:
            self.logger().info(f"{base_pair} ({base_rate_source}) conversion rate: {PerformanceMetrics.smart_round(base_rate)}")

    def oracle_status_df(self):
        columns = ["Source", "Pair", "Rate"]
        data = []
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_second_to_first_conversion_rate()
        if quote_pair.split("-")[0] != quote_pair.split("-")[1]:
            data.extend([
                [quote_rate_source, quote_pair, PerformanceMetrics.smart_round(quote_rate)],
            ])
        if base_pair.split("-")[0] != base_pair.split("-")[1]:
            data.extend([
                [base_rate_source, base_pair, PerformanceMetrics.smart_round(base_rate)],
            ])
        return pd.DataFrame(data=data, columns=columns)

    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []
            
        for market_pair in self._market_pairs:
            warning_lines.extend(self.network_warning([market_pair.first, market_pair.second]))

            markets_df = self.market_status_data_frame([market_pair.first, market_pair.second])
            lines.extend(["", "  Markets:"] +
                         ["    " + line for line in str(markets_df).split("\n")])

            oracle_df = self.oracle_status_df()
            if not oracle_df.empty:
                lines.extend(["", "  Rate conversion:"] +
                             ["    " + line for line in str(oracle_df).split("\n")])

            assets_df = self.wallet_balance_data_frame([market_pair.first, market_pair.second])
            lines.extend(["", "  Assets:"] +
                         ["    " + line for line in str(assets_df).split("\n")])

            lines.extend(
                ["", "  Profitability(without fees):"] +
                [f"    take bid on {market_pair.first.market.name}, "
                 f"take ask on {market_pair.second.market.name}: {round(self._current_profitability[0] * 100, 4)} %"] +
                [f"    take ask on {market_pair.first.market.name}, "
                 f"take bid on {market_pair.second.market.name}: {round(self._current_profitability[1] * 100, 4)} %"])

            tracked_limit_orders = self.tracked_limit_orders
            tracked_market_orders = self.tracked_market_orders

            if len(tracked_limit_orders) > 0 or len(tracked_market_orders) > 0:
                tracked_limit_orders_df = self.tracked_limit_orders_data_frame
                tracked_market_orders_df = self.tracked_market_orders_data_frame
                df_limit_lines = (str(tracked_limit_orders_df).split("\n")
                                  if len(tracked_limit_orders) > 0
                                  else list())
                df_market_lines = (str(tracked_market_orders_df).split("\n")
                                   if len(tracked_market_orders) > 0
                                   else list())
                lines.extend(["", "  Pending limit orders:"] +
                             ["    " + line for line in df_limit_lines] +
                             ["    " + line for line in df_market_lines])
            else:
                lines.extend(["", "  No pending limit orders."])

            warning_lines.extend(self.balance_warning([market_pair.first, market_pair.second]))

        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def notify_hb_app(self, msg: str):
        if self._hb_app_notification:
            super().notify_hb_app(msg)

    cdef bint c_all_markets_ready(self):
        """Optimized market ready check with early exit"""
        for market in self._sb_markets:
            if not market.ready:
                return False
        return True

    cdef c_tick(self, double timestamp):
        """Clock tick entry point"""
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        try:
            if not self._all_markets_ready:
                self._all_markets_ready = self.c_all_markets_ready()
                if not self._all_markets_ready:
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not ready. No arbitrage trading is permitted.")
                    return
                else:
                    if should_report_warnings:
                        self.logger().info(f"Markets are ready. Trading started.")

            # Optimized network status check with early exit
            for market in self._sb_markets:
                if market.network_status is not NetworkStatus.CONNECTED:
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not all online. No arbitrage trading is permitted.")
                    return

            for market_pair in self._market_pairs:
                self.c_process_market_pair(market_pair)
                
            # Log conversion rates every 5 minutes if using oracle
            if self._use_oracle_conversion_rate and self._last_conv_rates_logged + (60. * 5) < self._current_timestamp:
                self.log_conversion_rates()
                self._last_conv_rates_logged = self._current_timestamp
                
        finally:
            self._last_timestamp = timestamp

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        cdef:
            object buy_order = buy_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(buy_order.order_id)
            string order_id_str
            
        if market_trading_pair_tuple is not None:
            self._last_trade_timestamps[market_trading_pair_tuple] = self._current_timestamp
            
            # Use C++ map for O(1) lookup
            order_id_str = buy_order.order_id.encode('utf-8')
            if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                self.logger().info(f"Buy order {buy_order.order_id} completed after {time_elapsed:.2f} seconds.")
                self._order_timestamps_cpp.erase(order_id_str)
            
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit buy order completed on {market_trading_pair_tuple[0].name}: {buy_order.order_id}")
                self.notify_hb_app_with_timestamp(f"{buy_order.base_asset_amount:.8f} {buy_order.base_asset}-{buy_order.quote_asset} buy limit order completed on {market_trading_pair_tuple[0].name}")
    
    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        cdef:
            object sell_order = sell_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(sell_order.order_id)
            string order_id_str
            
        if market_trading_pair_tuple is not None:
            self._last_trade_timestamps[market_trading_pair_tuple] = self._current_timestamp
            
            # Use C++ map for O(1) lookup
            order_id_str = sell_order.order_id.encode('utf-8')
            if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                self.logger().info(f"Sell order {sell_order.order_id} completed after {time_elapsed:.2f} seconds.")
                self._order_timestamps_cpp.erase(order_id_str)
            
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit sell order completed on {market_trading_pair_tuple[0].name}: {sell_order.order_id}")
                self.notify_hb_app_with_timestamp(f"{sell_order.base_asset_amount:.8f} {sell_order.base_asset}-{sell_order.quote_asset} sell limit order completed on {market_trading_pair_tuple[0].name}")
                
    cdef c_did_cancel_order(self, object cancel_event):
        """Output log for cancelled order"""
        cdef:
            str order_id = cancel_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            self.log_with_clock(logging.INFO,
                                f"Market order canceled on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair):
        """
        Calculate the profitability of crossing the exchanges in both directions.
        Maintains Decimal precision for financial accuracy.
        """
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
        """
        Check whether we are ready for making new arbitrage orders.
        Uses C++ unordered_map for O(1) order timestamp operations.
        """
        cdef:
            double time_left
            dict tracked_taker_orders = {**self._sb_order_tracker.c_get_limit_orders(), 
                                         **self._sb_order_tracker.c_get_market_orders()}
            string order_id_str
            double time_elapsed
            list keys_to_remove = []

        for market_trading_pair_tuple in market_trading_pair_tuples:
            if len(tracked_taker_orders.get(market_trading_pair_tuple, {})) > 0:
                for order_id, order in tracked_taker_orders[market_trading_pair_tuple].items():
                    order_id_str = order_id.encode('utf-8')
                    
                    # Check if order is new
                    if self._order_timestamps_cpp.find(order_id_str) == self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp[order_id_str] = self._current_timestamp
                        self.logger().info(f"New pending order detected: {order_id}. Starting timer.")

                    time_elapsed = self._current_timestamp - self._order_timestamps_cpp[order_id_str]
                    if time_elapsed > self._order_timeout:
                        self.logger().warning(f"Order {order_id} has been pending for {time_elapsed:.2f} seconds, exceeds {self._order_timeout}s timeout. Considering completed.")
                        keys_to_remove.append(order_id)
                    else:
                        if time_elapsed > 10:
                            self.logger().warning(f"Order {order_id} has been pending for {time_elapsed:.2f} seconds. Waiting for completion or timeout.")
                        else:
                            self.logger().info(f"Order {order_id} has been pending for {time_elapsed:.2f} seconds. Waiting for completion or timeout.")
                        return False

                for order_id in keys_to_remove:
                    order_id_str = order_id.encode('utf-8')
                    if self._order_timestamps_cpp.find(order_id_str) != self._order_timestamps_cpp.end():
                        self._order_timestamps_cpp.erase(order_id_str)
                    self._sb_order_tracker.c_stop_tracking_limit_order(market_trading_pair_tuple, order_id)
                    self._sb_order_tracker.c_stop_tracking_market_order(market_trading_pair_tuple, order_id)

            # Cool off interval check
            ready_to_trade_time = self._last_trade_timestamps.get(market_trading_pair_tuple, 0) + self._next_trade_delay
            if market_trading_pair_tuple in self._last_trade_timestamps and ready_to_trade_time > self._current_timestamp:
                time_left = ready_to_trade_time - self._current_timestamp
                if not self._cool_off_logged:
                    self.log_with_clock(
                        logging.INFO,
                        f"Cooling off from previous trade on {market_trading_pair_tuple.market.name}. "
                        f"Resuming in {int(time_left)} seconds."
                    )
                    self._cool_off_logged = True
                return False

        if self._cool_off_logged:
            self.log_with_clock(
                logging.INFO,
                f"Cool off completed. Arbitrage strategy is now ready for new orders."
            )
            self._cool_off_logged = False

        return True

    cdef c_process_market_pair(self, object market_pair):
        """
        Checks which direction is more profitable and executes.
        """
        if not self.c_ready_for_new_orders([market_pair.first, market_pair.second]):
            return

        self._current_profitability = self.c_calculate_arbitrage_top_order_profitability(market_pair)

        if (self._current_profitability[1] < self._min_profitability and
                self._current_profitability[0] < self._min_profitability):
            return

        if self._current_profitability[1] > self._current_profitability[0]:
            # Buy on market_1, sell on market_2
            self.c_process_market_pair_inner(market_pair.first, market_pair.second)
        else:
            # Buy on market_2, sell on market_1
            self.c_process_market_pair_inner(market_pair.second, market_pair.first)

    cdef c_process_market_pair_inner(self, object buy_market_trading_pair_tuple, object sell_market_trading_pair_tuple):
        """
        Executes arbitrage trades for the input market pair.
        """
        cdef:
            object quantized_buy_amount
            object quantized_sell_amount
            object quantized_order_amount
            object best_amount = s_decimal_0
            object best_profitability = s_decimal_0
            object sell_price = s_decimal_0
            object buy_price = s_decimal_0
            ExchangeBase buy_market = buy_market_trading_pair_tuple.market
            ExchangeBase sell_market = sell_market_trading_pair_tuple.market
            string buy_order_id_str
            string sell_order_id_str

        best_amount, best_profitability, sell_price, buy_price = self.c_find_best_profitable_amount(
            buy_market_trading_pair_tuple, sell_market_trading_pair_tuple
        )
        quantized_buy_amount = buy_market.c_quantize_order_amount(buy_market_trading_pair_tuple.trading_pair, best_amount)
        quantized_sell_amount = sell_market.c_quantize_order_amount(sell_market_trading_pair_tuple.trading_pair, best_amount)
        quantized_order_amount = min(quantized_buy_amount, quantized_sell_amount)
        
        volume_in_USD = quantized_order_amount * sell_price
        
        # Filter small notional orders (minimum $10 USD)
        if volume_in_USD < 10:
            return
            
        if quantized_order_amount:
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                self.log_with_clock(logging.INFO,
                                    f"Executing market order buy of {buy_market_trading_pair_tuple.trading_pair} "
                                    f"at {buy_market_trading_pair_tuple.market.name} "
                                    f"and sell of {sell_market_trading_pair_tuple.trading_pair} "
                                    f"at {sell_market_trading_pair_tuple.market.name} "
                                    f"with amount {quantized_order_amount}, "
                                    f"and profitability {best_profitability}")
            
            buy_order_type = buy_market_trading_pair_tuple.market.get_taker_order_type()
            sell_order_type = sell_market_trading_pair_tuple.market.get_taker_order_type()
        
            # Place orders
            buy_order_id = self.c_buy_with_specific_market(buy_market_trading_pair_tuple, quantized_order_amount,
                                        order_type=buy_order_type, price=buy_price, expiration_seconds=self._next_trade_delay)
            sell_order_id = self.c_sell_with_specific_market(sell_market_trading_pair_tuple, quantized_order_amount,
                                         order_type=sell_order_type, price=sell_price, expiration_seconds=self._next_trade_delay)
            
            self.logger().info(f"Placed buy order {buy_order_id} and sell order {sell_order_id}. Starting timer.")
            
            # Store timestamps in C++ map
            buy_order_id_str = buy_order_id.encode('utf-8')
            sell_order_id_str = sell_order_id.encode('utf-8')
            self._order_timestamps_cpp[buy_order_id_str] = self._current_timestamp
            self._order_timestamps_cpp[sell_order_id_str] = self._current_timestamp
        
            if self._logging_options & self.OPTION_LOG_STATUS_REPORT:
                self.logger().info(self.format_status())

    @staticmethod
    def find_profitable_arbitrage_orders(min_profitability: Decimal,
                                         buy_market_trading_pair: MarketTradingPairTuple,
                                         sell_market_trading_pair: MarketTradingPairTuple,
                                         buy_market_conversion_rate,
                                         sell_market_conversion_rate):
        return c_find_profitable_arbitrage_orders(min_profitability,
                                                  buy_market_trading_pair,
                                                  sell_market_trading_pair,
                                                  buy_market_conversion_rate,
                                                  sell_market_conversion_rate)

    def market_conversion_rate(self, market_info: MarketTradingPairTuple) -> Decimal:
        if market_info == self._market_pairs[0].first:
            return s_decimal_1
        elif market_info == self._market_pairs[0].second:
            _, _, quote_rate, _, _, base_rate = self.get_second_to_first_conversion_rate()
            return quote_rate / base_rate
        return s_decimal_1

    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair_tuple, object sell_market_trading_pair_tuple):
        """
        Calculate optimal order size and profitability with fees accounted for.
        """
        cdef:
            object total_bid_value = s_decimal_0
            object total_ask_value = s_decimal_0
            object total_bid_value_adjusted = s_decimal_0
            object total_ask_value_adjusted = s_decimal_0
            object total_previous_step_base_amount = s_decimal_0
            object bid_price = s_decimal_0
            object ask_price = s_decimal_0
            object profitability
            object best_profitable_order_amount = s_decimal_0
            object best_profitable_order_profitability = s_decimal_0
            object buy_fee
            object sell_fee
            object total_sell_flat_fees
            object total_buy_flat_fees
            object net_sell_proceeds
            object net_buy_costs
            object buy_market_quote_balance
            object sell_market_base_balance
            object vwap_buy_cost = s_decimal_0
            object vwap_sell_proceeds = s_decimal_0
            object remaining_amount
            object step_take
            ExchangeBase buy_market = buy_market_trading_pair_tuple.market
            ExchangeBase sell_market = sell_market_trading_pair_tuple.market
            bint use_optimized

        buy_market_conversion_rate = self.market_conversion_rate(buy_market_trading_pair_tuple)
        sell_market_conversion_rate = self.market_conversion_rate(sell_market_trading_pair_tuple)
        
        # Use optimized function when no conversion is needed
        use_optimized = (buy_market_conversion_rate == s_decimal_1 and 
                        sell_market_conversion_rate == s_decimal_1)
        
        if use_optimized:
            profitable_orders = c_find_profitable_arbitrage_orders_no_conversion(
                self._min_profitability,
                buy_market_trading_pair_tuple,
                sell_market_trading_pair_tuple)
        else:
            profitable_orders = c_find_profitable_arbitrage_orders(
                self._min_profitability,
                buy_market_trading_pair_tuple,
                sell_market_trading_pair_tuple,
                buy_market_conversion_rate,
                sell_market_conversion_rate)

        # Check if each step meets profit level after fees and is within wallet balance
        for bid_price_adjusted, ask_price_adjusted, bid_price, ask_price, amount in profitable_orders:
            buy_fee = buy_market.c_get_fee(
                buy_market_trading_pair_tuple.base_asset,
                buy_market_trading_pair_tuple.quote_asset,
                buy_market_trading_pair_tuple.market.get_taker_order_type(),
                TradeType.BUY,
                total_previous_step_base_amount + amount,
                ask_price
            )
            sell_fee = sell_market.c_get_fee(
                sell_market_trading_pair_tuple.base_asset,
                sell_market_trading_pair_tuple.quote_asset,
                sell_market_trading_pair_tuple.market.get_taker_order_type(),
                TradeType.SELL,
                total_previous_step_base_amount + amount,
                bid_price
            )
            
            # Accumulated flat fees
            total_buy_flat_fees = self.c_sum_flat_fees(buy_market_trading_pair_tuple.quote_asset, buy_fee.flat_fees)
            total_sell_flat_fees = self.c_sum_flat_fees(sell_market_trading_pair_tuple.quote_asset, sell_fee.flat_fees)

            # Accumulated profitability with fees
            total_bid_value_adjusted += bid_price_adjusted * amount
            total_ask_value_adjusted += ask_price_adjusted * amount
            net_sell_proceeds = total_bid_value_adjusted * (s_decimal_1 - sell_fee.percent) - total_sell_flat_fees
            net_buy_costs = total_ask_value_adjusted * (s_decimal_1 + buy_fee.percent) + total_buy_flat_fees
            profitability = net_sell_proceeds / net_buy_costs

            # If current step is within minimum profitability
            if profitability > (s_decimal_1 + self._min_profitability):
                best_profitable_order_amount = total_previous_step_base_amount + amount
                best_profitable_order_profitability = profitability

            if self._logging_options & self.OPTION_LOG_PROFITABILITY_STEP:
                self.log_with_clock(logging.DEBUG, f"Total profitability with fees: {profitability}, "
                                                   f"Current step profitability: {bid_price/ask_price},"
                                                   f"bid, ask price, amount: {bid_price, ask_price, amount}")
            
            buy_market_quote_balance = buy_market.c_get_available_balance(buy_market_trading_pair_tuple.quote_asset)
            sell_market_base_balance = sell_market.c_get_available_balance(sell_market_trading_pair_tuple.base_asset)
            
            # Stop if insufficient balance
            if (buy_market_quote_balance < net_buy_costs or
                    sell_market_base_balance < (total_previous_step_base_amount + amount)):
                # Use previous step as best profitable order if below min profitability
                if profitability < (s_decimal_1 + self._min_profitability):
                    break
                if self._logging_options & self.OPTION_LOG_INSUFFICIENT_ASSET:
                    self.log_with_clock(logging.DEBUG,
                                        f"Not enough asset to complete this step. "
                                        f"Quote asset needed: {net_buy_costs}. "
                                        f"Quote asset available balance: {buy_market_quote_balance}. "
                                        f"Base asset needed: {total_previous_step_base_amount + amount}. "
                                        f"Base asset available balance: {sell_market_base_balance}. ")

                # Adjust for available balance
                buy_market_adjusted_order_size = ((buy_market_quote_balance / ask_price - total_buy_flat_fees) /
                                                  (s_decimal_1 + buy_fee.percent))
                best_profitable_order_amount = min(sell_market_base_balance, buy_market_adjusted_order_size)
                best_profitable_order_profitability = profitability
                break

            total_bid_value += bid_price * amount
            total_ask_value += ask_price * amount
            total_previous_step_base_amount += amount

        if self._logging_options & self.OPTION_LOG_FULL_PROFITABILITY_STEP:
            self.log_with_clock(
                logging.DEBUG,
                "\n" + pd.DataFrame(
                    data=[
                        [b_price_adjusted/a_price_adjusted,
                         b_price_adjusted, a_price_adjusted, b_price, a_price, amount]
                        for b_price_adjusted, a_price_adjusted, b_price, a_price, amount in profitable_orders],
                    columns=['raw_profitability', 'bid_price_adjusted', 'ask_price_adjusted',
                             'bid_price', 'ask_price', 'step_amount']
                ).to_string()
            )

        # Calculate VWAP for the chosen amount
        if best_profitable_order_amount > s_decimal_0 and len(profitable_orders) > 0:
            remaining_amount = best_profitable_order_amount
            vwap_buy_cost = s_decimal_0
            vwap_sell_proceeds = s_decimal_0
            for b_price_adjusted, a_price_adjusted, b_price_raw, a_price_raw, step_amount in profitable_orders:
                if remaining_amount <= s_decimal_0:
                    break
                step_take = step_amount if step_amount <= remaining_amount else remaining_amount
                vwap_buy_cost += a_price_raw * step_take
                vwap_sell_proceeds += b_price_raw * step_take
                remaining_amount -= step_take
            if best_profitable_order_amount > s_decimal_0:
                bid_price = vwap_sell_proceeds / best_profitable_order_amount
                ask_price = vwap_buy_cost / best_profitable_order_amount

        return best_profitable_order_amount, best_profitable_order_profitability, bid_price, ask_price

    # Public methods for unit tests
    def find_best_profitable_amount(self, buy_market: MarketTradingPairTuple, sell_market: MarketTradingPairTuple):
        return self.c_find_best_profitable_amount(buy_market, sell_market)

    def ready_for_new_orders(self, market_pair):
        return self.c_ready_for_new_orders(market_pair)


cdef list c_find_profitable_arbitrage_orders_no_conversion(object min_profitability,
                                                           object buy_market_trading_pair_tuple,
                                                           object sell_market_trading_pair_tuple):
    """
    Optimized version for same-currency pairs (no conversion needed).
    Maintains Decimal precision for financial accuracy.
    """
    cdef:
        object step_amount = s_decimal_0
        object bid_leftover_amount = s_decimal_0
        object ask_leftover_amount = s_decimal_0
        object current_bid = None
        object current_ask = None

    profitable_orders = []
    bid_it = sell_market_trading_pair_tuple.order_book_bid_entries()
    ask_it = buy_market_trading_pair_tuple.order_book_ask_entries()

    try:
        while True:
            if bid_leftover_amount == s_decimal_0 and ask_leftover_amount == s_decimal_0:
                current_bid = next(bid_it)
                current_ask = next(ask_it)
                ask_leftover_amount = current_ask.amount
                bid_leftover_amount = current_bid.amount

            elif bid_leftover_amount > s_decimal_0 and ask_leftover_amount == s_decimal_0:
                current_ask = next(ask_it)
                ask_leftover_amount = current_ask.amount

            elif ask_leftover_amount > s_decimal_0 and bid_leftover_amount == s_decimal_0:
                current_bid = next(bid_it)
                bid_leftover_amount = current_bid.amount

            elif bid_leftover_amount > s_decimal_0 and ask_leftover_amount > s_decimal_0:
                pass
            else:
                break

            # Direct price comparison (no conversion)
            if current_bid.price < current_ask.price:
                break
            
            # Check profitability threshold
            if min_profitability < 0 and current_bid.price/current_ask.price < (s_decimal_1 + min_profitability):
                break

            step_amount = min(bid_leftover_amount, ask_leftover_amount)

            # Skip zero amounts
            if step_amount == s_decimal_0:
                continue

            profitable_orders.append((current_bid.price,  # bid_price_adjusted
                                     current_ask.price,  # ask_price_adjusted
                                     current_bid.price,  # bid_price
                                     current_ask.price,  # ask_price
                                     step_amount))       # amount

            ask_leftover_amount -= step_amount
            bid_leftover_amount -= step_amount

    except StopIteration:
        pass

    return profitable_orders


cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate):
    """
    Original function with conversion rate support.
    """
    cdef:
        object step_amount = s_decimal_0
        object bid_leftover_amount = s_decimal_0
        object ask_leftover_amount = s_decimal_0
        object current_bid = None
        object current_ask = None
        object current_bid_price_adjusted
        object current_ask_price_adjusted

    profitable_orders = []
    bid_it = sell_market_trading_pair_tuple.order_book_bid_entries()
    ask_it = buy_market_trading_pair_tuple.order_book_ask_entries()

    try:
        while True:
            if bid_leftover_amount == s_decimal_0 and ask_leftover_amount == s_decimal_0:
                current_bid = next(bid_it)
                current_ask = next(ask_it)
                ask_leftover_amount = current_ask.amount
                bid_leftover_amount = current_bid.amount

            elif bid_leftover_amount > s_decimal_0 and ask_leftover_amount == s_decimal_0:
                current_ask = next(ask_it)
                ask_leftover_amount = current_ask.amount

            elif ask_leftover_amount > s_decimal_0 and bid_leftover_amount == s_decimal_0:
                current_bid = next(bid_it)
                bid_leftover_amount = current_bid.amount

            elif bid_leftover_amount > s_decimal_0 and ask_leftover_amount > s_decimal_0:
                pass
            else:
                break

            # Apply conversion rates
            current_bid_price_adjusted = current_bid.price * sell_market_conversion_rate
            current_ask_price_adjusted = current_ask.price * buy_market_conversion_rate
            
            # Check if arbitrage is possible
            if current_bid_price_adjusted < current_ask_price_adjusted:
                break
            
            # Check profitability threshold
            if min_profitability < 0 and current_bid_price_adjusted/current_ask_price_adjusted < (s_decimal_1 + min_profitability):
                break

            step_amount = min(bid_leftover_amount, ask_leftover_amount)

            # Skip zero amounts
            if step_amount == s_decimal_0:
                continue

            profitable_orders.append((current_bid_price_adjusted,
                                      current_ask_price_adjusted,
                                      current_bid.price,
                                      current_ask.price,
                                      step_amount))

            ask_leftover_amount -= step_amount
            bid_leftover_amount -= step_amount

    except StopIteration:
        pass

    return profitable_orders