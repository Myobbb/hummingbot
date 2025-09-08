# distutils: language=c++
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string

cdef class ArbitrageStrategy(StrategyBase):
    cdef:
        list _market_pairs
        bint _all_markets_ready
        object _min_profitability
        double _status_report_interval
        double _last_timestamp
        dict _last_trade_timestamps
        double _next_trade_delay
        int64_t _logging_options
        int _failed_order_tolerance
        bint _cool_off_logged
        bint _use_oracle_conversion_rate
        object _secondary_to_primary_base_conversion_rate
        object _secondary_to_primary_quote_conversion_rate
        bint _hb_app_notification
        tuple _current_profitability
        double _last_conv_rates_logged
        float _order_timeout
        # Single C++ map for order timestamps (removed Python dict redundancy)
        unordered_map[string, double] _order_timestamps_cpp

    cdef bint c_all_markets_ready(self)
    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair)
    cdef c_process_market_pair(self, object market_pair)
    cdef c_process_market_pair_inner(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)

# Optimized version for same-currency pairs (no conversion)
cdef list c_find_profitable_arbitrage_orders_no_conversion(object min_profitability,
                                                           object buy_market_trading_pair_tuple,
                                                           object sell_market_trading_pair_tuple)

# Original function with conversion rates
cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate)