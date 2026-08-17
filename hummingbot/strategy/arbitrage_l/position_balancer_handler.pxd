# distutils: language=c++
from libcpp.pair cimport pair
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
        dict _last_buy_cancel_time
        dict _last_buy_cancel_cooldown
        dict _last_sell_cancel_time
        dict _last_sell_cancel_cooldown
        dict _last_sell_insuf_bal_time
        dict _buy_cancel_streak
        dict _sell_cancel_streak
        # Fill-pressure adaptive post-fill wait (trailing-stop-like spacing)
        dict _last_fill_completion_time   # canonical_asset -> timestamp of last FULL completion (fills only)
        dict _post_fill_extra_wait        # canonical_asset -> seconds to add to DEFAULT_COMPLETION_COOLDOWN
        # Step-up-into-gap throttle
        dict _last_step_up_time           # canonical_asset -> timestamp of last step-up cancel (pre-detection throttle)
        # Log throttle for the per-tick min_tick=0 warning ("exchange:pair" -> last-logged timestamp)
        dict _last_min_tick_warn_time
        # Second-level refuge state (per canonical asset) — truthy = resting under the wall (2nd-best),
        # undercut suppressed; the VALUE is the intended park depth (foreign levels we sat behind)
        dict _in_refuge_sell
        dict _in_refuge_buy
        # Price of the order that JUST went away (asset -> (price, timestamp)), so the placement
        # path can still skip our own level while the local book lags the cancel — see c_own_recent_price
        dict _last_gone_buy_price
        dict _last_gone_sell_price
        # Asset alias support (for cross-exchange pairs with different token names)
        dict _asset_aliases
        dict _canonical_asset

    # Core methods
    cdef double c_get_min_tick(self, object market_tuple)
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
    cdef bint c_market_in_failure_cooldown(self, object market_tuple)
    cdef object c_find_best_buy_market(self, str asset)
    cdef object c_find_best_sell_market(self, str asset, bint prefer_fuller_venue=*)
    # Helper methods for cancellation logic
    cdef bint c_check_stuck_cancel(self, str order_id, str asset, bint is_buy, double current_time, bint force_short_timeout=*)
    cdef tuple c_check_immediate_conditions(self, str asset, bint is_buy, double order_age,
                                            double frontrun_delay=*)
    cdef double c_own_recent_price(self, str asset, bint is_buy)
    cdef tuple c_refuge_wall(self, object ob, double own_price, double min_tick, bint is_buy)
    cdef double c_first_foreign_beyond(self, object order_ob, double order_price,
                                       double min_tick, bint is_buy)
    cdef int c_refuge_foreign_below(self, str asset, bint is_buy)
    cdef void c_cancel_stale_orders(self, str asset)
    cdef void _cancel_buy_order(self, str asset, str order_id, str reason, bint reactive=*)
    cdef void _cancel_sell_order(self, str asset, str order_id, str reason, bint reactive=*)
    cdef bint c_handle_position_balancing(self, object buy_market_tuple, object sell_market_tuple)
    cdef bint c_execute_buy_limit(self, object buy_market_tuple, object sell_market_tuple)
    cdef bint c_execute_sell_limit(self, object buy_market_tuple, object sell_market_tuple)