# distutils: language=c++
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False

"""
Buy-in handler for ArbitrageL strategy.

Manages buy-in logic to ensure sufficient base asset holdings before executing arbitrage trades.
"""

import logging
from decimal import Decimal
from libc.stdint cimport int64_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.set cimport set as cpp_set
from cython.operator cimport dereference as deref

from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.OrderBookEntry cimport OrderBookEntry

cdef double QUANTIZATION_EPSILON = 1e-12
cdef double EPSILON = 1e-10


cdef class ArbitrageBuyInHandler:
    """
    Handles buy-in logic for arbitrage strategy.

    Buy-in ensures the strategy has sufficient base asset holdings (in quote value terms)
    before executing arbitrage. It places limit buy orders when holdings fall below target.
    """

    def __init__(self,
                 object strategy,
                 bint enabled,
                 double target_usd,
                 double min_profitability):
        """
        Initialize buy-in handler.

        Args:
            strategy: Reference to ArbitrageLStrategy instance
            enabled: Whether buy-in is enabled
            target_usd: Target base asset value in quote currency
            min_profitability: Minimum profitability threshold for buy-in trades
        """
        self.strategy = strategy
        self._enabled = enabled
        self._target_usd = target_usd
        self._min_profitability = min_profitability
        self._completed = False
        self._pending_by_asset = {}  # asset -> base amount
        self._pending_orders = {}    # order_id -> (asset, amount)

    @property
    def is_active(self):
        """Returns True if buy-in is enabled and not completed."""
        return self._enabled and not self._completed

    @property
    def is_enabled(self):
        """Returns True if buy-in is enabled (regardless of completion)."""
        return self._enabled

    @property
    def is_completed(self):
        """Returns True if buy-in target has been reached."""
        return self._completed

    cdef void c_maybe_disable(self):
        """Disable buy-in globally for this run once target is reached."""
        if self._completed and self._enabled:
            self._enabled = False
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy-in completed. Disabling buy-in for this session.")

    cdef double c_get_pending_base(self, str asset):
        """Return in-flight buy-in base amount for asset."""
        try:
            return <double> self._pending_by_asset.get(asset, 0.0)
        except Exception:
            return 0.0

    cdef pair[double, double] c_compute_value_and_shortfall(self,
                                                            double base_balance,
                                                            double last_bid):
        """Return (current_value_quote, shortfall)."""
        cdef double current_value = base_balance * last_bid
        cdef double shortfall = 0.0
        if current_value < self._target_usd:
            shortfall = self._target_usd - current_value
        return pair[double, double](current_value, shortfall)

    cdef double c_get_aggregated_base_balance(self, str asset):
        """Aggregate base balance using the same source as status (assets_df/balance_map)."""
        cdef:
            double total = 0.0
            list unique_tuples
            object assets_df
            dict balance_map
            object t
        try:
            unique_tuples, assets_df, balance_map = self.strategy.c_build_unique_tuples_assets_and_balance_map()
            for t in unique_tuples:
                if t.base_asset == asset:
                    total += float(balance_map.get((t.market.name, asset), 0.0))
        except Exception:
            return 0.0
        return total

    cdef double c_get_adjusted_base_balance(self, str asset):
        """Aggregated base balance plus pending buy-in base."""
        cdef double agg = self.c_get_aggregated_base_balance(asset)
        cdef double pend = self.c_get_pending_base(asset)
        return agg + pend

    cdef bint c_try_mark_complete(self,
                                  str pair,
                                  double current_value_quote,
                                  double shortfall):
        """Mark buy-in as completed if at target or remaining < min notional."""
        if current_value_quote >= self._target_usd:
            self._completed = True
            self.c_maybe_disable()
            return True
        if shortfall > 0 and shortfall < self.strategy._min_order_usd:
            self._completed = True
            self.strategy.log_with_clock(
                logging.INFO,
                f"Buy-in considered complete on {pair}: "
                f"shortfall {shortfall:.6f} < min notional {self.strategy._min_order_usd:.6f}")
            self.c_maybe_disable()
            return True
        return False

    cdef void c_scan_and_mark_completion(self):
        """Re-evaluate the base asset against target and disable buy-in when done."""
        if not self._enabled or self._completed:
            return

        cdef:
            str asset_key
            double last_bid = 0.0
            double base_bal
            pair[double, double] val_short
            double current_value_quote
            double shortfall
            list unique_tuples
            object assets_df
            dict balance_map

        # Determine the single base asset from the first market pair
        asset_key = self.strategy._market_pairs[0].first.base_asset

        # Get a non-zero bid from any active tuple with this base asset
        last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)

        # Build balances from the same source as status for consistency
        unique_tuples, assets_df, balance_map = self.strategy.c_build_unique_tuples_assets_and_balance_map()

        # Sum base using the same map
        base_bal = 0.0
        for t in unique_tuples:
            if t.base_asset == asset_key:
                base_bal += float(balance_map.get((t.market.name, asset_key), 0.0))

        # Include any in-flight buy-in base to avoid stale underestimation
        base_bal += self.c_get_pending_base(asset_key)
        val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        current_value_quote = val_short.first
        shortfall = val_short.second

        # Concise info log about startup buy-in state and decision
        try:
            decision = "disable" if (current_value_quote >= self._target_usd or
                                    (shortfall > 0 and shortfall < self.strategy._min_order_usd)) else "keep"
            self.strategy.log_with_clock(
                logging.INFO,
                f"Buy-in check: asset={asset_key} base={base_bal:.6f} bid={last_bid:.6f} "
                f"value={current_value_quote:.6f} target={self._target_usd:.6f} -> {decision}")
        except Exception:
            pass

        if self.c_try_mark_complete(asset_key, current_value_quote, shortfall):
            pass
        self.c_maybe_disable()

    def handle_order_completion(self, str order_id, bint is_buy):
        """Clean up pending buy-in tracking on order completion."""
        if not is_buy:
            return
        try:
            pend = self._pending_orders.pop(order_id, None)
            if pend is not None:
                asset_key, amt = pend
                self._pending_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_by_asset.get(asset_key, 0.0)) - float(amt))
                if self._pending_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_by_asset.pop(asset_key, None)
        except Exception:
            pass

    def handle_order_cancellation(self, str order_id):
        """Clean up pending buy-in tracking on order cancellation."""
        try:
            pend = self._pending_orders.pop(order_id, None)
            if pend is not None:
                asset_key, amt = pend
                self._pending_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_by_asset.get(asset_key, 0.0)) - float(amt))
                if self._pending_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_by_asset.pop(asset_key, None)
        except Exception:
            pass

    def handle_order_timeout(self, str order_id):
        """Clean up pending buy-in tracking on order timeout."""
        try:
            pend = self._pending_orders.pop(order_id, None)
            if pend is not None:
                asset_key, amt = pend
                self._pending_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_by_asset.get(asset_key, 0.0)) - float(amt))
                if self._pending_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_by_asset.pop(asset_key, None)
        except Exception:
            pass

    def handle_old_order_cleanup(self, str order_id):
        """Clean up pending buy-in tracking during old order cleanup."""
        try:
            pend = self._pending_orders.pop(order_id, None)
            if pend is not None:
                asset_key, amt = pend
                self._pending_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_by_asset.get(asset_key, 0.0)) - float(amt))
                if self._pending_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_by_asset.pop(asset_key, None)
        except Exception:
            pass

    def get_status_lines(self, list unique_tuples, dict balance_map):
        """Get buy-in status lines for format_status()."""
        if not self._enabled:
            return []

        lines = []
        try:
            # Collect base assets from active market pairs
            aset = set()
            for mp in self.strategy._market_pairs:
                aset.add(mp.first.base_asset)
                aset.add(mp.second.base_asset)
            base_assets = sorted(aset)
        except Exception:
            base_assets = []

        agg_lines = []
        any_pending = False
        for a in base_assets:
            # Get a reference bid from any active tuple with this base asset
            bid = self.strategy.c_get_reference_bid_for_asset(a)
            # Aggregate available base balance
            total_base = 0.0
            for t in unique_tuples:
                if t.base_asset == a:
                    total_base += float(balance_map.get((t.market.name, a), 0.0))
            total_value = total_base * bid
            # Status should reflect the permanent global flag, not current valuation
            is_pending = (not self._completed)
            if is_pending:
                any_pending = True
            agg_lines.append(
                f"    {a}: base={total_base:.6f} value={total_value:.6f} "
                f"({'pending' if is_pending else 'completed'})")

        if any_pending and self._enabled:
            lines.extend([
                "",
                f"  Buy-in: target={self._target_usd:.6f} min_prof={self._min_profitability * 100:.2f}%"
            ])
            lines.extend(agg_lines)

        return lines

    cdef bint c_handle_buy_in(self, object buy_market_tuple, object sell_market_tuple):
        """
        Execute buy-in trade if needed.
        Returns True if a buy-in was placed; False otherwise.
        """
        # CRITICAL SAFEGUARD: Prevent double execution
        if self.strategy._last_global_trade_timestamp == self.strategy._current_timestamp:
            return False

        # CRITICAL: Set timestamp IMMEDIATELY to prevent race condition
        self.strategy._last_global_trade_timestamp = self.strategy._current_timestamp

        if not self._enabled:
            return False

        cdef:
            str pair_str = buy_market_tuple.trading_pair
            object market = buy_market_tuple.market
            double base_bal = 0.0
            double quote_bal = float(market.c_get_available_balance(buy_market_tuple.quote_asset))
            double best_amount
            double best_prof
            double sell_price
            double buy_price
            tuple res
            str asset_key = buy_market_tuple.base_asset

        if self._completed:
            return False

        # Evaluate progress vs target and early complete
        cdef double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
        base_bal = self.c_get_adjusted_base_balance(asset_key)
        cdef pair[double, double] val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        cdef double current_value_quote = val_short.first
        cdef double shortfall = val_short.second

        if self.c_try_mark_complete(pair_str, current_value_quote, shortfall):
            return False

        # Require quote balance to spend
        if quote_bal <= 0:
            return False

        # Delegate to strategy for orderbook scanning (avoid circular dependency)
        res = self.strategy.c_find_best_buyin_amount(
            buy_market_tuple,
            sell_market_tuple,
            quote_bal,
            shortfall,
            self._min_profitability
        )
        best_amount = <double>res[0]
        best_prof = <double>res[1]
        sell_price = <double>res[2]
        buy_price = <double>res[3]

        if best_amount <= 0 or best_prof < self._min_profitability:
            return False

        # Place buy order
        cdef object order_type = OrderType.LIMIT
        cdef object quantized_amount
        cdef object dec_safe_amount = Decimal(str(max(0.0, best_amount - QUANTIZATION_EPSILON)))

        quantized_amount = self.strategy.c_safe_quantize_order_amount(
            market, buy_market_tuple.trading_pair, dec_safe_amount, Decimal(str(buy_price)))

        # Ensure not exceeding available quote after quantization
        cdef double max_affordable = 0.0
        if buy_price > 0:
            max_affordable = quote_bal / buy_price
        if float(quantized_amount) > max_affordable:
            quantized_amount = self.strategy.c_safe_quantize_order_amount(
                market, buy_market_tuple.trading_pair,
                Decimal(str(max(0.0, max_affordable - QUANTIZATION_EPSILON))),
                Decimal(str(buy_price)))

        if quantized_amount <= Decimal("0"):
            return False

        # Apply max_order_size cap from trading rules
        try:
            buy_trading_rule = market._trading_rules.get(buy_market_tuple.trading_pair)
            if buy_trading_rule is not None and buy_trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > buy_trading_rule.max_order_size:
                    quantized_amount = min(quantized_amount, buy_trading_rule.max_order_size)
        except Exception:
            pass

        # Enforce minimum notional
        cdef double volume_usd = float(quantized_amount) * buy_price
        if volume_usd < self.strategy._min_order_usd:
            if self.c_try_mark_complete(pair_str, current_value_quote, shortfall):
                return False
            return False

        try:
            buy_order_id = self.strategy.c_buy_with_specific_market(
                buy_market_tuple,
                quantized_amount,
                order_type=order_type,
                price=Decimal(str(buy_price)),
                expiration_seconds=self.strategy._next_trade_delay)
        except Exception as e:
            # Set failure timestamp to enforce cooldown
            self.strategy._last_failure_timestamps[buy_market_tuple] = self.strategy._current_timestamp
            self.strategy.logger().warning(f"Error submitting buy-in order to {market.name}: {e}")
            return False

        # Track order timestamp
        cdef string buy_id_str = self.strategy._to_cpp_str(buy_order_id)
        self.strategy._order_timestamps[buy_id_str] = self.strategy._current_timestamp

        # Track pending BUY order
        try:
            if buy_market_tuple not in self.strategy._pending_buy_orders_by_market:
                self.strategy._pending_buy_orders_by_market[buy_market_tuple] = set()
            self.strategy._pending_buy_orders_by_market[buy_market_tuple].add(buy_order_id)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track pending buy-in order by market: {e}")

        # Track pending base
        try:
            self._pending_orders[buy_order_id] = (asset_key, float(quantized_amount))
            self._pending_by_asset[asset_key] = (
                float(self._pending_by_asset.get(asset_key, 0.0)) + float(quantized_amount))
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track pending buy-in for order {buy_order_id}: {e}")

        # Check if target reached after placing
        last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
        base_bal = self.c_get_adjusted_base_balance(asset_key)
        val_short = self.c_compute_value_and_shortfall(base_bal, last_bid)
        current_value_quote = val_short.first
        shortfall = val_short.second
        if self.c_try_mark_complete(pair_str, current_value_quote, shortfall):
            self.c_maybe_disable()

        return True
