# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False

"""
High-performance helpers for arbitrage_m V2 controller.
Extracted from V1 arbitrage.pyx to preserve Cython optimization.
"""

from decimal import Decimal
from libcpp.set cimport set as cpp_set
from cython.operator cimport dereference as deref, postincrement as inc

from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.OrderBookEntry cimport OrderBookEntry

cdef double EPSILON = 1e-10
cdef double QUANTIZATION_EPSILON = 1e-12


cdef class ArbitrageMHelpers:
    """
    High-performance arbitrage calculations using Cython.
    Preserves V1 optimization for V2 framework.
    """

    def __init__(self, double min_profitability=0.01, double min_order_usd=15.0):
        self._min_profitability = min_profitability
        self._min_order_usd = min_order_usd

    def find_best_profitable_amount(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double buy_quote_balance,
        double sell_base_balance,
        double conversion_rate=1.0
    ):
        """
        Python wrapper for C-level function.
        Returns: (amount, profitability, sell_price, buy_price)
        """
        return self.c_find_best_profitable_amount(
            buy_market_tuple,
            sell_market_tuple,
            buy_quote_balance,
            sell_base_balance,
            conversion_rate
        )

    def calculate_profitability(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate=1.0
    ):
        """
        Python wrapper for profitability calculation.
        Returns: (prof_buy_sell, prof_sell_buy) - profitability in both directions
        """
        return self.c_calculate_profitability(
            buy_market_tuple,
            sell_market_tuple,
            conversion_rate
        )

    def check_top_of_book_profitable(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate=1.0
    ):
        """
        Quick top-of-book profitability check.
        Returns: True if profitable at top of book
        """
        return self.c_check_top_of_book_profitable(
            buy_market_tuple,
            sell_market_tuple,
            conversion_rate
        )

    cdef bint c_check_top_of_book_profitable(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate
    ):
        """
        Fast top-of-book profitability gate.
        Extracted from V1: arbitrage.pyx:1026-1059
        """
        cdef:
            double min_prof_threshold = 1.0 + self._min_profitability
            double top_bid, top_ask
            double top_bid_adj, top_ask_adj
            ExchangeBase buy_ex = buy_market_tuple.market
            ExchangeBase sell_ex = sell_market_tuple.market
            OrderBook buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
            OrderBook sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)
            cpp_set[OrderBookEntry].reverse_iterator bid_it
            cpp_set[OrderBookEntry].iterator ask_it

        # Check if books have data
        if sell_ob._bid_book.size() == 0 or buy_ob._ask_book.size() == 0:
            return False

        # Get top of book
        bid_it = sell_ob._bid_book.rbegin()
        ask_it = buy_ob._ask_book.begin()
        top_bid = deref(bid_it).getPrice()
        top_ask = deref(ask_it).getPrice()

        if top_bid <= 0 or top_ask <= 0:
            return False

        # Apply conversion
        top_bid_adj = top_bid * conversion_rate
        top_ask_adj = top_ask

        # Check profitability
        if top_bid_adj / top_ask_adj < min_prof_threshold:
            return False

        return True

    cdef tuple c_calculate_profitability(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double conversion_rate
    ):
        """
        Calculate profitability for both directions.
        Extracted from V1: arbitrage.pyx:787-812
        """
        cdef:
            double bid1, ask1, bid2, ask2
            double prof1, prof2

        try:
            bid1 = float(buy_market_tuple.get_price(False))
            ask1 = float(buy_market_tuple.get_price(True))
            bid2 = float(sell_market_tuple.get_price(False))
            ask2 = float(sell_market_tuple.get_price(True))
        except Exception:
            return (0.0, 0.0)

        # Sanity check
        if bid1 <= 0 or ask1 <= 0 or bid2 <= 0 or ask2 <= 0:
            return (0.0, 0.0)

        # Apply conversion (sell-side)
        bid2 *= conversion_rate
        ask2 *= conversion_rate

        # Calculate profitability
        # Direction 1: Buy from market2 (sell), sell to market1 (buy)
        prof1 = (bid1 / ask2 - 1.0) if ask2 > 0 else -1.0
        # Direction 2: Buy from market1 (buy), sell to market2 (sell)
        prof2 = (bid2 / ask1 - 1.0) if ask1 > 0 else -1.0

        return (prof1, prof2)

    cdef tuple c_find_best_profitable_amount(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double buy_quote_balance,
        double sell_base_balance,
        double conversion_rate
    ):
        """
        Find best profitable arbitrage amount.
        Extracted and simplified from V1: arbitrage.pyx:1061-1143
        """
        cdef:
            ExchangeBase buy_market = buy_market_tuple.market
            ExchangeBase sell_market = sell_market_tuple.market
            OrderBook buy_ob = buy_market.c_get_order_book(buy_market_tuple.trading_pair)
            OrderBook sell_ob = sell_market.c_get_order_book(sell_market_tuple.trading_pair)
            list profitable_orders
            double total_base = 0.0
            double total_cost = 0.0
            double total_proceeds_orig = 0.0
            double avg_sell_price_orig, avg_buy_price, profitability
            double bid_adj, ask_adj, orig_bid, orig_ask, amount
            double max_base_amount

        # Early gate: top-of-book check
        if not self.c_check_top_of_book_profitable(buy_market_tuple, sell_market_tuple, conversion_rate):
            return (0.0, 0.0, 0.0, 0.0)

        # Balance checks
        if buy_quote_balance <= EPSILON or sell_base_balance <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Calculate capacity limit
        max_base_amount = self._calculate_capacity_limit(
            buy_market_tuple,
            sell_market_tuple,
            buy_quote_balance,
            sell_base_balance
        )

        if max_base_amount <= EPSILON:
            return (0.0, 0.0, 0.0, 0.0)

        # Get profitable orders from order books
        profitable_orders = c_find_profitable_arbitrage_orders(
            self._min_profitability,
            buy_market_tuple,
            sell_market_tuple,
            1.0,  # Buy conversion always 1.0
            conversion_rate,
            max_base_amount,
            0.05,
            False  # Skip top check (already done)
        )

        if not profitable_orders:
            return (0.0, 0.0, 0.0, 0.0)

        # Aggregate profitable volume
        for bid_adj, ask_adj, orig_bid, orig_ask, amount in profitable_orders:
            # Apply constraints
            amount = min(
                amount,
                max_base_amount - total_base,
                (buy_quote_balance - total_cost) / ask_adj if ask_adj > 0 else 0
            )

            if amount <= EPSILON:
                continue

            total_base += amount
            total_cost += ask_adj * amount
            total_proceeds_orig += orig_bid * amount

            # Stop if capacity exhausted
            if total_base >= max_base_amount - EPSILON:
                break

        # Calculate results
        if total_base > EPSILON:
            avg_sell_price_orig = total_proceeds_orig / total_base
            avg_buy_price = total_cost / total_base
            profitability = ((avg_sell_price_orig * conversion_rate) / avg_buy_price - 1.0) if avg_buy_price > 0 else 0.0

            # Check minimum notional
            if avg_sell_price_orig * total_base >= self._min_order_usd:
                return (total_base, profitability, avg_sell_price_orig, avg_buy_price)

        return (0.0, 0.0, 0.0, 0.0)

    cdef double _calculate_capacity_limit(
        self,
        object buy_market_tuple,
        object sell_market_tuple,
        double buy_quote_balance,
        double sell_base_balance
    ):
        """
        Calculate maximum tradeable amount.
        Extracted from V1: arbitrage.pyx:1146-1184
        """
        cdef:
            double capacity = sell_base_balance
            object quantized
            double approx_ask
            ExchangeBase buy_ex = buy_market_tuple.market
            OrderBook buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
            cpp_set[OrderBookEntry].iterator ask_it

        # Get approximate buy price
        if buy_ob._ask_book.size() > 0:
            ask_it = buy_ob._ask_book.begin()
            approx_ask = deref(ask_it).getPrice()
        else:
            approx_ask = 0.0

        # Apply quote balance constraint
        if approx_ask > 0 and buy_quote_balance > 0:
            capacity = min(capacity, buy_quote_balance / approx_ask)

        # Apply sell-side quantization
        quantized = sell_market_tuple.market.quantize_order_amount(
            sell_market_tuple.trading_pair,
            Decimal(str(max(0.0, capacity - QUANTIZATION_EPSILON)))
        )
        capacity = float(quantized) if quantized else 0.0

        # Apply buy-side quantization
        if capacity > 0:
            quantized = buy_market_tuple.market.quantize_order_amount(
                buy_market_tuple.trading_pair,
                Decimal(str(max(0.0, capacity - QUANTIZATION_EPSILON)))
            )
            capacity = float(quantized) if quantized else 0.0

        return capacity


cdef list c_find_profitable_arbitrage_orders(
    double min_profitability,
    object buy_market_tuple,
    object sell_market_tuple,
    double buy_conversion_rate,
    double sell_conversion_rate,
    double target_base_amount,
    double overshoot_ratio,
    bint perform_top_check
):
    """
    Find profitable arbitrage opportunities in order books.
    Extracted from V1: arbitrage.pyx:1604-1736

    Returns list of (bid_adj, ask_adj, bid_orig, ask_orig, amount)
    """
    cdef:
        double min_prof_threshold = 1.0 + min_profitability
        list profitable_orders = []
        int max_levels = 20
        int levels_processed = 0
        double bid_leftover = 0.0
        double ask_leftover = 0.0
        double step_amount = 0.0
        double cumulative_base = 0.0
        double overshoot_stop = 0.0
        ExchangeBase buy_ex
        ExchangeBase sell_ex
        OrderBook buy_ob
        OrderBook sell_ob
        cpp_set[OrderBookEntry].reverse_iterator bid_it
        cpp_set[OrderBookEntry].reverse_iterator bid_end
        cpp_set[OrderBookEntry].iterator ask_it
        cpp_set[OrderBookEntry].iterator ask_end
        OrderBookEntry bid_entry
        OrderBookEntry ask_entry
        double orig_bid_price
        double orig_ask_price
        double bid_price
        double ask_price

    # Optional top-of-book check
    if perform_top_check:
        try:
            top_bid = float(sell_market_tuple.get_price(False))
            top_ask = float(buy_market_tuple.get_price(True))
            top_bid_adj = top_bid * sell_conversion_rate
            top_ask_adj = top_ask * buy_conversion_rate
            if top_bid_adj / top_ask_adj < min_prof_threshold:
                return []
        except Exception:
            return []

    # Capacity-aware stopping
    if target_base_amount > 0.0:
        overshoot_stop = target_base_amount * (1.0 + overshoot_ratio)

    # Scan order books
    try:
        buy_ex = <ExchangeBase> buy_market_tuple.market
        sell_ex = <ExchangeBase> sell_market_tuple.market
        buy_ob = buy_ex.c_get_order_book(buy_market_tuple.trading_pair)
        sell_ob = sell_ex.c_get_order_book(sell_market_tuple.trading_pair)

        bid_it = sell_ob._bid_book.rbegin()
        bid_end = sell_ob._bid_book.rend()
        ask_it = buy_ob._ask_book.begin()
        ask_end = buy_ob._ask_book.end()

        if bid_it == bid_end or ask_it == ask_end:
            return []

        bid_entry = deref(bid_it)
        ask_entry = deref(ask_it)

        bid_leftover = bid_entry.getAmount()
        ask_leftover = ask_entry.getAmount()

        while levels_processed < max_levels and bid_it != bid_end and ask_it != ask_end:
            # Get prices
            orig_bid_price = bid_entry.getPrice()
            orig_ask_price = ask_entry.getPrice()

            if orig_bid_price <= 0 or orig_ask_price <= 0:
                break

            # Apply conversion
            bid_price = orig_bid_price * sell_conversion_rate
            ask_price = orig_ask_price * buy_conversion_rate

            # Check profitability
            if bid_price / ask_price < min_prof_threshold:
                break

            # Calculate step amount
            step_amount = bid_leftover if bid_leftover <= ask_leftover else ask_leftover

            if step_amount > EPSILON:
                profitable_orders.append((
                    bid_price,
                    ask_price,
                    orig_bid_price,
                    orig_ask_price,
                    step_amount
                ))

                # Early stop if capacity reached
                cumulative_base += step_amount
                if overshoot_stop > 0.0 and cumulative_base >= overshoot_stop:
                    break

            # Advance to next level
            if bid_leftover <= ask_leftover:
                inc(bid_it)
                levels_processed += 1
                if bid_it == bid_end:
                    break
                bid_entry = deref(bid_it)
                bid_leftover = bid_entry.getAmount()
                ask_leftover -= step_amount
            else:
                inc(ask_it)
                levels_processed += 1
                if ask_it == ask_end:
                    break
                ask_entry = deref(ask_it)
                ask_leftover = ask_entry.getAmount()
                bid_leftover -= step_amount

    except Exception:
        return []

    return profitable_orders
