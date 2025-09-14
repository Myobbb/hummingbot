# distutils: language=c++
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair

cdef class ArbitrageMStrategy(StrategyBase):
    """
    Optimized arbitrage strategy header.
    Simplified member variables and cleaner interface.
    """
    cdef:
        # Core configuration
        list _market_pairs
        double _min_profitability
        int64_t _logging_options
        
        # Timing configuration
        double _status_report_interval
        double _next_trade_delay
        double _order_timeout
        double _order_warning_delay
        
        # Thresholds
        double _min_order_usd
        double _rate_cache_duration
        size_t _max_tracked_orders
        
        # State tracking
        bint _all_markets_ready
        double _last_timestamp
        double _status_debounce_until
        dict _last_trade_timestamps
        double _last_cleanup_timestamp
        double _last_conv_rates_logged
        
        # Conversion configuration
        bint _use_oracle_conversion_rate
        double _fixed_base_rate
        double _fixed_quote_rate
        
        # Cached rates
        double _cached_base_rate
        double _cached_quote_rate
        double _last_rate_update
        
        # Current profitability
        pair[double, double] _current_profitability
        # Buy-in params/state
        bint _buy_in_enabled
        double _buy_in_target_usd
        double _buy_in_min_profitability
        dict _buy_in_completed_by_asset  # key: base_asset -> bool
        
        # Notifications
        bint _hb_app_notification
        
        # Order tracking - single unified map
        unordered_map[string, double] _order_timestamps

    # Core methods
    cdef void _validate_configuration(self)
    cdef bint c_check_markets_ready(self, bint should_report)
    cdef bint c_ready_for_new_orders(self, list market_tuples)
    
    # Conversion rate methods
    cdef double c_get_conversion_rate(self, bint is_base_asset)
    cdef void c_update_conversion_rates(self)
    cdef void c_log_conversion_rates(self)
    cdef double c_get_market_to_market_conversion_rate(self, object buy_market_tuple, object sell_market_tuple)
    
    # Trading logic
    cdef c_process_market_pair(self, object market_pair)
    cdef bint c_handle_buy_in(self, object buy_market_tuple, object sell_market_tuple)
    cdef tuple c_find_best_buyin_amount(self,
                                        object buy_market_tuple,
                                        object sell_market_tuple,
                                        double buy_quote_balance,
                                        double max_spend_quote)
    cdef void c_maybe_disable_buy_in(self)
    cdef pair[int, double] c_top_of_book_profitable_get_conv(self,
                                                             object buy_market_tuple,
                                                             object sell_market_tuple,
                                                             double min_profitability)
    cdef pair[double, double] c_compute_value_and_shortfall(self,
                                                            double base_balance,
                                                            double last_bid)
    cdef double c_get_quote_to_usd_rate(self, str quote_asset)
    cdef double c_compute_global_value_usd(self, object buy_market_tuple)
    cdef pair[double, double] c_compute_global_value_and_shortfall(self, object buy_market_tuple)
    cdef bint c_try_mark_complete_buy_in(self,
                                         str asset_key,
                                         str pair,
                                         double current_value_quote,
                                         double shortfall)
    cdef pair[double, double] c_calculate_profitability(self, object market_pair)
    cdef c_execute_arbitrage(self, object buy_market_tuple, object sell_market_tuple)
    cdef tuple c_find_best_profitable_amount(self, object buy_market_tuple, object sell_market_tuple)
    cdef double _calculate_capacity_limit(self,
                                          object buy_market_tuple,
                                          object sell_market_tuple,
                                          double buy_quote_balance,
                                          double sell_base_balance)
    
    # Event handlers - unified
    cdef void c_handle_order_completion(self, object order_event, bint is_buy) except *
    cdef c_did_complete_buy_order(self, object buy_order_completed_event)
    cdef c_did_complete_sell_order(self, object sell_order_completed_event)
    
    # Maintenance
    cdef void c_cleanup_old_orders(self)

# Single optimized function for finding profitable orders
cdef list c_find_profitable_arbitrage_orders(
    double min_profitability,
    object buy_market_tuple,
    object sell_market_tuple,
    double buy_conversion_rate,
    double sell_conversion_rate,
    double target_base_amount,
    double overshoot_ratio)