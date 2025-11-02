# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook

cdef class ArbitrageMHelpers:
    """
    High-performance helpers for arbitrage_m V2 controller
    Reuses optimized C-level logic from V1 strategy
    """

    cdef double _min_profitability
    cdef double _min_order_usd

    cdef tuple c_find_best_profitable_amount(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double buy_quote_balance,
        double sell_base_balance,
        double conversion_rate
    )

    cdef tuple c_calculate_profitability(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate
    )

    cdef bint c_check_top_of_book_profitable(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate
    )
