# distutils: language=c++
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair

cdef class ArbitrageStrategy(StrategyBase):
    cdef:
        list _market_pairs
        bint _all_markets_ready
        object _min_profitability
        double _min_profitability_float
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
        pair[double, double] _current_profitability_fast
        double _last_conv_rates_logged
        float _order_timeout
        # Cached conversion rates and timestamps
        double _cached_base_rate
        double _cached_quote_rate
        double _cached_market_rate
        double _last_rate_update
        # Memory management
        double _last_cleanup_timestamp
        int _max_tracked_orders
        # Top-of-book cache for current tick (raw prices)
        double _tob_first_bid
        double _tob_first_ask
        double _tob_second_bid
        double _tob_second_ask
        double _tob_timestamp
        # Single C++ map for order timestamps (removed Python dict redundancy)
        unordered_map[string, double] _order_timestamps_cpp

    cdef bint c_all_markets_ready(self)
    cdef void c_update_cached_rates(self)
    cdef void c_update_top_of_book_cache(self, object market_pair)
    cdef double c_get_cached_market_rate(self, object market_info)
    cdef void _validate_configuration(self)
    cdef void c_cleanup_old_orders(self)
    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair)
    cdef pair[double, double] c_calculate_profitability_fast(self, object market_pair)
    cdef c_process_market_pair(self, object market_pair)
    cdef c_process_market_pair_inner(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)
    cdef c_did_complete_buy_order(self, object buy_order_completed_event)
    cdef c_did_complete_sell_order(self, object sell_order_completed_event)
    cdef c_did_cancel_order(self, object cancel_event)
    cdef tuple c_find_best_profitable_amount_fast_no_fees(self,
                                                         object buy_market_trading_pair_tuple,
                                                         object sell_market_trading_pair_tuple,
                                                         double buy_conversion_rate,
                                                         double sell_conversion_rate)

# Optimized version when no conversion is required (fast path)
cdef list c_find_profitable_arbitrage_orders_fast(double min_profitability,
                                                  object buy_market_trading_pair_tuple,
                                                  object sell_market_trading_pair_tuple,
                                                  double buy_conversion_rate,
                                                  double sell_conversion_rate)

# Original function with conversion rates
cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate)