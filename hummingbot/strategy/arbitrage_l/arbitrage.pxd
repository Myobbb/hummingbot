# distutils: language=c++
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp.set cimport set as cpp_set

cdef class ArbitrageLStrategy(StrategyBase):
    """
    Limit order arbitrage strategy header.
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
        double _last_global_trade_timestamp
        dict _last_failure_timestamps
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
        
        # Buy-in handler (None when disabled)
        object _buy_in_handler


        # Order tracking - single unified map
        unordered_map[string, double] _order_timestamps
        # Track completed orders to avoid duplicate completion logging
        cpp_set[string] _completed_orders
        # Track orders that have received at least one fill (Python set of order_ids)
        set _orders_with_fills
        # Recent order -> market pair mapping to resolve late events after tracker cleanup
        dict _recent_order_market_pair
        # Track pending limit orders per market tuple - SEPARATE for buy and sell to allow parallel buy+sell
        dict _pending_buy_orders_by_market   # market_tuple -> set of buy order_ids
        dict _pending_sell_orders_by_market  # market_tuple -> set of sell order_ids
        # Track orders cancelled due to timeout (to avoid cooldown on timeout cancellations)
        set _timeout_cancelled_orders

        # Orchestration mode flag (for multi-strategy orchestrator optimization)
        bint _orchestrated_mode

    # Core methods
    cdef void _validate_configuration(self)
    cdef bint c_check_markets_ready(self, bint should_report)
    cdef bint c_check_markets_ready_orchestrated(self)
    cdef bint c_ready_for_new_orders(self, list market_tuples)
    cdef string _to_cpp_str(self, object py_str)

    # Helper methods
    cdef object c_safe_quantize_order_amount(self, object market, str trading_pair, object amount, object price)
    cdef void c_remove_pending_order(self, object market_tuple, str order_id, str context)
    
    # Conversion rate methods
    cdef double _conv_rate(self, object buy_market_tuple, object sell_market_tuple)
    cdef double c_get_conversion_rate(self, bint is_base_asset)
    cdef void c_update_conversion_rates(self)
    cdef tuple c_build_unique_tuples_assets_and_balance_map(self)
    cdef bint c_books_ready_for_direction(self, object buy_market_tuple, object sell_market_tuple)
    cdef void c_log_conversion_rates(self)
    cdef double c_get_market_to_market_conversion_rate(self, object buy_market_tuple, object sell_market_tuple)
    
    # Trading logic
    cdef double c_get_reference_bid_for_asset(self, str asset_key)
    cdef tuple c_find_best_buyin_amount(self,
                                        object buy_market_tuple,
                                        object sell_market_tuple,
                                        double buy_quote_balance,
                                        double max_spend_quote,
                                        double min_profitability)
    cdef pair[int, double] c_top_of_book_profitable_get_conv(self,
                                                             object buy_market_tuple,
                                                             object sell_market_tuple,
                                                             double min_profitability)
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
    cdef c_did_fail_order(self, object order_failed_event)
    cdef c_did_complete_buy_order(self, object buy_order_completed_event)
    cdef c_did_complete_sell_order(self, object sell_order_completed_event)
    cdef c_did_cancel_order_tracker(self, object order_cancelled_event)
    
    # Maintenance
    cdef void c_check_all_order_timeouts(self)
    cdef void c_cleanup_old_orders(self)

# Single optimized function for finding profitable orders
cdef list c_find_profitable_arbitrage_orders(
    double min_profitability,
    object buy_market_tuple,
    object sell_market_tuple,
    double buy_conversion_rate,
    double sell_conversion_rate,
    double target_base_amount,
    double overshoot_ratio,
    bint perform_top_check)