# distutils: language=c++
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair

cdef class ArbitrageStrategy(StrategyBase):
    cdef:
        # Core strategy configuration
        list _market_pairs
        object _min_profitability
        double _min_profitability_float  # Cached float version for fast comparison
        int64_t _logging_options
        int _failed_order_tolerance
        bint _hb_app_notification
        
        # Timing and state tracking
        double _status_report_interval
        double _last_timestamp
        double _next_trade_delay
        double _order_timeout
        double _last_conv_rates_logged
        dict _last_trade_timestamps
        
        # State flags
        bint _all_markets_ready
        bint _cool_off_logged
        
        # Conversion rate configuration
        bint _use_oracle_conversion_rate
        object _secondary_to_primary_base_conversion_rate
        object _secondary_to_primary_quote_conversion_rate
        
        # Cached conversion rates (updated periodically)
        double _cached_base_rate
        double _cached_quote_rate
        double _cached_market_rate
        double _last_rate_update
        
        # Current profitability tracking
        pair[double, double] _current_profitability_fast  # Using C++ pair for speed
        tuple _current_profitability  # Python tuple for compatibility
        
        # Order tracking with C++ map
        unordered_map[string, double] _order_timestamps_cpp
        
        # Performance optimization: cached order book snapshots
        double _last_orderbook_snapshot
        object _cached_buy_orderbook
        object _cached_sell_orderbook

    # Optimized internal methods
    cdef bint c_all_markets_ready(self) nogil
    cdef pair[double, double] c_calculate_profitability_fast(self, object market_pair) nogil
    cdef tuple c_calculate_arbitrage_top_order_profitability(self, object market_pair)
    cdef c_process_market_pair(self, object market_pair)
    cdef c_process_market_pair_inner(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef tuple c_find_best_profitable_amount(self, object buy_market_trading_pair, object sell_market_trading_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)
    cdef void c_update_cached_rates(self)
    cdef double c_get_cached_market_rate(self, object market_info) nogil
    cdef void c_cleanup_old_orders(self, double current_time, bint force=*)
    cdef void c_clear_stale_caches(self, double current_time)

# Optimized order finding functions
cdef list c_find_profitable_arbitrage_orders_fast(double min_profitability,
                                                  object buy_market_trading_pair_tuple,
                                                  object sell_market_trading_pair_tuple,
                                                  double buy_conversion_rate,
                                                  double sell_conversion_rate)

# Legacy function for compatibility
cdef list c_find_profitable_arbitrage_orders(object min_profitability,
                                             object buy_market_trading_pair_tuple,
                                             object sell_market_trading_pair_tuple,
                                             object buy_market_conversion_rate,
                                             object sell_market_conversion_rate)