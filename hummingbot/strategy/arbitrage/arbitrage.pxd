# distutils: language=c++
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t

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
        # Add the new attributes here
        dict _order_placement_timestamps
        float _order_timeout
        # Throttle logs for long-pending orders to cut per-tick overhead
        dict _pending_order_last_log_ts

    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair)
    cdef c_process_market_pair(self, object market_pair)
    cdef c_process_market_pair_inner(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)
    cdef tuple c_find_best_profitable_amount_stream(self, object buy_market_trading_pair_tuple, object sell_market_trading_pair_tuple)

cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate)
