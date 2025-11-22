# distutils: language=c++
from libcpp.pair cimport pair
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.arbitrage_l.arbitrage cimport ArbitrageLStrategy
cdef class PositionBalancerHandler:
    """
    Universal position balancing handler for ArbitrageL strategy.
    Supports both buying (when below target) and selling (when above target).
    """
    cdef:
        ArbitrageLStrategy strategy
        # Buy-in configuration
        bint _buy_enabled
        double _buy_target_usd
        double _buy_spread_pct
        bint _buy_spread_is_min
        # Sell-off configuration
        bint _sell_enabled
        double _sell_target_usd
        double _sell_spread_pct
        bint _sell_spread_is_min
        # Order size configuration
        double _order_size_usd
        # Completion tracking
        bint _buy_completed
        bint _sell_completed
        # Pending order tracking
        dict _pending_buy_by_asset
        dict _pending_sell_by_asset
        dict _pending_buy_orders
        dict _pending_sell_orders
        # Limit order refresh
        double _limit_refresh_interval
        dict _last_buy_order_time
        dict _last_sell_order_time
        dict _active_buy_orders
        dict _active_sell_orders
        dict _active_buy_order_details
        dict _active_sell_order_details
        dict _buy_cancel_request_time
        dict _sell_cancel_request_time
        dict _last_buy_completion_time
        dict _last_sell_completion_time
        # Asset alias support (for cross-exchange pairs with different token names)
        dict _asset_aliases
        dict _canonical_asset

    # Core methods
    cdef void _build_asset_aliases(self)
    cdef str _get_canonical_asset(self, str asset)
    cdef list _get_all_asset_aliases(self, str asset)
    cdef void c_cancel_all_buy_orders(self)
    cdef void c_cancel_all_sell_orders(self)
    cdef void c_maybe_disable_buy(self)
    cdef void c_maybe_disable_sell(self)
    cdef double c_get_pending_buy_base(self, str asset)
    cdef double c_get_pending_sell_base(self, str asset)
    cdef pair[double, double] c_compute_value_and_buy_shortfall(self, double base_balance, double last_bid)
    cdef pair[double, double] c_compute_value_and_sell_excess(self, double base_balance, double last_bid)
    cdef double c_get_aggregated_base_balance(self, str asset)
    cdef double c_get_actual_base_balance(self, str asset)
    cdef double c_get_adjusted_base_balance(self, str asset)
    cdef bint c_try_mark_buy_complete(self, str pair, double current_value_quote, double shortfall)
    cdef bint c_try_mark_sell_complete(self, str pair, double current_value_quote, double excess)
    cdef void c_scan_and_mark_completion(self)
    cdef object c_find_best_buy_market(self, str asset)
    cdef object c_find_best_sell_market(self, str asset)
    # Helper methods for cancellation logic
    cdef bint c_check_stuck_cancel(self, str order_id, str asset, bint is_buy, double current_time)
    cdef tuple c_get_orderbook_prices(self, OrderBook ob)
    cdef double c_get_effective_reference_price(self, OrderBook ob, double top_price, double order_price, 
                                                  double min_price_increment, bint is_buy)
    cdef tuple c_check_immediate_frontrun(self, double current_top_price, double order_price, bint is_buy)
    cdef tuple c_check_large_gap_immediate(self, double order_price, double expected_price, 
                                            double min_price_increment, bint is_buy)
    cdef double c_calculate_ideal_order_price(self, double effective_ref_price, double spread_pct, 
                                               bint spread_is_min, double min_price_increment, bint is_buy)
    cdef tuple c_check_price_divergence(self, double order_price, double ideal_price, bint spread_is_min, 
                                         double min_price_increment, bint got_valid_second_level)
    cdef void c_cancel_stale_orders(self, str asset)
    cdef void _cancel_buy_order(self, str asset, str order_id, str reason)
    cdef void _cancel_sell_order(self, str asset, str order_id, str reason)
    cdef bint c_handle_position_balancing(self, object buy_market_tuple, object sell_market_tuple)
    cdef bint c_execute_buy_limit(self, object buy_market_tuple, object sell_market_tuple)
    cdef bint c_execute_sell_limit(self, object buy_market_tuple, object sell_market_tuple)