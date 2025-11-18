# distutils: language=c++
from libcpp.pair cimport pair

cdef class ArbitrageBuyInHandler:
    """
    Cython header for buy-in handler.
    """
    cdef:
        object strategy
        bint _enabled
        double _target_usd
        double _min_profitability
        bint _completed
        dict _pending_by_asset
        dict _pending_orders

    # Core methods
    cdef void c_maybe_disable(self)
    cdef double c_get_pending_base(self, str asset)
    cdef pair[double, double] c_compute_value_and_shortfall(self, double base_balance, double last_bid)
    cdef double c_get_aggregated_base_balance(self, str asset)
    cdef double c_get_adjusted_base_balance(self, str asset)
    cdef bint c_try_mark_complete(self, str pair, double current_value_quote, double shortfall)
    cdef void c_scan_and_mark_completion(self)
    cdef bint c_handle_buy_in(self, object buy_market_tuple, object sell_market_tuple)
