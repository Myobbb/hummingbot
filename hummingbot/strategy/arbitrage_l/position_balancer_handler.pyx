# distutils: language=c++
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False

"""
Position balancer handler for ArbitrageL strategy.

Manages position balancing to maintain target asset holdings:
- Buys when holdings fall below target (buy-in)
- Sells when holdings exceed target (sell-off)
- Uses limit orders with configurable spread from top bid/ask
"""

import logging
from decimal import Decimal
from libc.stdint cimport int64_t
from libcpp.pair cimport pair
from libcpp.string cimport string

from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.order_book cimport OrderBook

cdef double QUANTIZATION_EPSILON = 1e-12
cdef double EPSILON = 1e-10


cdef class PositionBalancerHandler:
    """
    Handles position balancing logic for arbitrage strategy.

    Position balancing ensures the strategy maintains target asset holdings:
    - Buy-in: When asset value < buy_target_usd, place buy limit orders
    - Sell-off: When asset value > sell_target_usd, place sell limit orders

    Uses limit orders with configurable spread from top bid/ask (order_book_alignment pattern).
    """

    def __init__(self,
                 object strategy,
                 bint buy_enabled,
                 double buy_target_usd,
                 double buy_spread_pct,
                 bint sell_enabled,
                 double sell_target_usd,
                 double sell_spread_pct,
                 double limit_refresh_interval,
                 double order_size_usd=100.0):
        """
        Initialize position balancer handler.

        Args:
            strategy: Reference to ArbitrageLStrategy instance
            buy_enabled: Whether buy-in is enabled
            buy_target_usd: Target minimum asset value in quote currency
            buy_spread_pct: Spread percentage below top bid for buy orders (e.g., 0.1 = 0.1%)
            sell_enabled: Whether sell-off is enabled
            sell_target_usd: Target maximum asset value in quote currency
            sell_spread_pct: Spread percentage above top ask for sell orders (e.g., 0.1 = 0.1%)
            limit_refresh_interval: How often to cancel and replace limit orders (seconds)
            order_size_usd: Maximum order size in USD per order (default: 100.0)
        """
        self.strategy = strategy

        # Buy-in configuration
        self._buy_enabled = buy_enabled
        self._buy_target_usd = buy_target_usd
        self._buy_spread_pct = buy_spread_pct / 100.0  # Convert to decimal

        # Sell-off configuration
        self._sell_enabled = sell_enabled
        self._sell_target_usd = sell_target_usd
        self._sell_spread_pct = sell_spread_pct / 100.0  # Convert to decimal

        # Order size configuration
        self._order_size_usd = order_size_usd

        # Completion tracking
        self._buy_completed = False
        self._sell_completed = False

        # Pending order tracking (separate for buy/sell)
        self._pending_buy_by_asset = {}   # asset -> base amount pending
        self._pending_sell_by_asset = {}  # asset -> base amount pending
        self._pending_buy_orders = {}     # order_id -> (asset, amount)
        self._pending_sell_orders = {}    # order_id -> (asset, amount)

        # Limit order refresh
        self._limit_refresh_interval = limit_refresh_interval
        self._last_buy_order_time = {}    # asset -> timestamp
        self._last_sell_order_time = {}   # asset -> timestamp
        self._active_buy_orders = {}      # asset -> order_id
        self._active_sell_orders = {}     # asset -> order_id

    @property
    def is_buy_active(self):
        """Returns True if buy-in is enabled and not completed."""
        return self._buy_enabled and not self._buy_completed

    @property
    def is_sell_active(self):
        """Returns True if sell-off is enabled and not completed."""
        return self._sell_enabled and not self._sell_completed

    @property
    def is_active(self):
        """Returns True if either buy or sell is active."""
        return self.is_buy_active or self.is_sell_active

    @property
    def is_buy_enabled(self):
        """Returns True if buy-in is enabled (regardless of completion)."""
        return self._buy_enabled

    @property
    def is_sell_enabled(self):
        """Returns True if sell-off is enabled (regardless of completion)."""
        return self._sell_enabled

    @property
    def is_buy_completed(self):
        """Returns True if buy-in target has been reached."""
        return self._buy_completed

    @property
    def is_sell_completed(self):
        """Returns True if sell-off target has been reached."""
        return self._sell_completed

    cdef void c_maybe_disable_buy(self):
        """Disable buy-in globally once target is reached."""
        if self._buy_completed and self._buy_enabled:
            self._buy_enabled = False
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy-in target reached. Disabling buy-in for this session.")

    cdef void c_maybe_disable_sell(self):
        """Disable sell-off globally once target is reached."""
        if self._sell_completed and self._sell_enabled:
            self._sell_enabled = False
            self.strategy.log_with_clock(
                logging.INFO,
                "Sell-off target reached. Disabling sell-off for this session.")

    cdef double c_get_pending_buy_base(self, str asset):
        """Return in-flight buy order base amount for asset."""
        try:
            return <double> self._pending_buy_by_asset.get(asset, 0.0)
        except Exception:
            return 0.0

    cdef double c_get_pending_sell_base(self, str asset):
        """Return in-flight sell order base amount for asset."""
        try:
            return <double> self._pending_sell_by_asset.get(asset, 0.0)
        except Exception:
            return 0.0

    cdef pair[double, double] c_compute_value_and_buy_shortfall(self,
                                                                 double base_balance,
                                                                 double last_bid):
        """
        Compute current value and buy shortfall.
        Returns (current_value_quote, shortfall).
        Shortfall is positive if we need to buy more.
        """
        cdef double current_value = base_balance * last_bid
        cdef double shortfall = 0.0
        if current_value < self._buy_target_usd:
            shortfall = self._buy_target_usd - current_value
        return pair[double, double](current_value, shortfall)

    cdef pair[double, double] c_compute_value_and_sell_excess(self,
                                                               double base_balance,
                                                               double last_bid):
        """
        Compute current value and sell excess.
        Returns (current_value_quote, excess).
        Excess is positive if we need to sell some.
        """
        cdef double current_value = base_balance * last_bid
        cdef double excess = 0.0
        if current_value > self._sell_target_usd:
            excess = current_value - self._sell_target_usd
        return pair[double, double](current_value, excess)

    cdef double c_get_aggregated_base_balance(self, str asset):
        """Aggregate base balance using the same source as status (balance_map)."""
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
        """
        Get adjusted base balance accounting for pending orders.
        For buy target checking: add pending buys
        For sell target checking: subtract pending sells
        """
        cdef double agg = self.c_get_aggregated_base_balance(asset)
        cdef double pending_buy = self.c_get_pending_buy_base(asset)
        cdef double pending_sell = self.c_get_pending_sell_base(asset)
        # Net balance = actual + pending buys - pending sells
        return agg + pending_buy - pending_sell

    cdef bint c_try_mark_buy_complete(self,
                                      str pair,
                                      double current_value_quote,
                                      double shortfall):
        """Mark buy-in as completed if at target or remaining < min notional."""
        if current_value_quote >= self._buy_target_usd:
            self._buy_completed = True
            self.c_maybe_disable_buy()
            return True
        if shortfall > 0 and shortfall < self.strategy._min_order_usd:
            self._buy_completed = True
            self.strategy.log_with_clock(
                logging.INFO,
                f"Buy-in considered complete on {pair}: "
                f"shortfall {shortfall:.6f} < min notional {self.strategy._min_order_usd:.6f}")
            self.c_maybe_disable_buy()
            return True
        return False

    cdef bint c_try_mark_sell_complete(self,
                                       str pair,
                                       double current_value_quote,
                                       double excess):
        """Mark sell-off as completed if at target or remaining < min notional."""
        if current_value_quote <= self._sell_target_usd:
            self._sell_completed = True
            self.c_maybe_disable_sell()
            return True
        if excess > 0 and excess < self.strategy._min_order_usd:
            self._sell_completed = True
            self.strategy.log_with_clock(
                logging.INFO,
                f"Sell-off considered complete on {pair}: "
                f"excess {excess:.6f} < min notional {self.strategy._min_order_usd:.6f}")
            self.c_maybe_disable_sell()
            return True
        return False

    cdef void c_scan_and_mark_completion(self):
        """Re-evaluate asset balance and complete buy/sell if targets reached."""
        cdef:
            str asset_key
            double last_bid = 0.0
            double base_bal
            pair[double, double] val_short
            pair[double, double] val_excess
            double current_value_quote
            double shortfall
            double excess
            list unique_tuples
            object assets_df
            dict balance_map

        # Skip if both disabled
        if not self._buy_enabled and not self._sell_enabled:
            return

        # Determine the base asset from the first market pair
        asset_key = self.strategy._market_pairs[0].first.base_asset

        # Get reference bid
        last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)

        # Build balances
        unique_tuples, assets_df, balance_map = self.strategy.c_build_unique_tuples_assets_and_balance_map()

        # Sum base balance
        base_bal = 0.0
        for t in unique_tuples:
            if t.base_asset == asset_key:
                base_bal += float(balance_map.get((t.market.name, asset_key), 0.0))

        # Include pending orders
        base_bal += self.c_get_pending_buy_base(asset_key)
        base_bal -= self.c_get_pending_sell_base(asset_key)

        # Check buy completion
        if self._buy_enabled and not self._buy_completed:
            val_short = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
            current_value_quote = val_short.first
            shortfall = val_short.second

            try:
                decision = "complete" if (current_value_quote >= self._buy_target_usd or
                                         (shortfall > 0 and shortfall < self.strategy._min_order_usd)) else "active"
                self.strategy.log_with_clock(
                    logging.INFO,
                    f"Buy-in check: asset={asset_key} base={base_bal:.6f} bid={last_bid:.6f} "
                    f"value={current_value_quote:.6f} target={self._buy_target_usd:.6f} -> {decision}")
            except Exception:
                pass

            self.c_try_mark_buy_complete(asset_key, current_value_quote, shortfall)

        # Check sell completion
        if self._sell_enabled and not self._sell_completed:
            val_excess = self.c_compute_value_and_sell_excess(base_bal, last_bid)
            current_value_quote = val_excess.first
            excess = val_excess.second

            try:
                decision = "complete" if (current_value_quote <= self._sell_target_usd or
                                         (excess > 0 and excess < self.strategy._min_order_usd)) else "active"
                self.strategy.log_with_clock(
                    logging.INFO,
                    f"Sell-off check: asset={asset_key} base={base_bal:.6f} bid={last_bid:.6f} "
                    f"value={current_value_quote:.6f} target={self._sell_target_usd:.6f} -> {decision}")
            except Exception:
                pass

            self.c_try_mark_sell_complete(asset_key, current_value_quote, excess)

        self.c_maybe_disable_buy()
        self.c_maybe_disable_sell()

    def handle_order_completion(self, str order_id, bint is_buy):
        """Clean up pending order tracking on order completion."""
        try:
            # Remove from position balancer tracking set
            self.strategy._position_balancer_orders.discard(order_id)

            if is_buy:
                pend = self._pending_buy_orders.pop(order_id, None)
                if pend is not None:
                    asset_key, amt = pend
                    self._pending_buy_by_asset[asset_key] = max(
                        0.0,
                        float(self._pending_buy_by_asset.get(asset_key, 0.0)) - float(amt))
                    if self._pending_buy_by_asset.get(asset_key, 0.0) <= 1e-15:
                        self._pending_buy_by_asset.pop(asset_key, None)
                    # Remove from active tracking
                    if self._active_buy_orders.get(asset_key) == order_id:
                        self._active_buy_orders.pop(asset_key, None)
            else:
                pend = self._pending_sell_orders.pop(order_id, None)
                if pend is not None:
                    asset_key, amt = pend
                    self._pending_sell_by_asset[asset_key] = max(
                        0.0,
                        float(self._pending_sell_by_asset.get(asset_key, 0.0)) - float(amt))
                    if self._pending_sell_by_asset.get(asset_key, 0.0) <= 1e-15:
                        self._pending_sell_by_asset.pop(asset_key, None)
                    # Remove from active tracking
                    if self._active_sell_orders.get(asset_key) == order_id:
                        self._active_sell_orders.pop(asset_key, None)
        except Exception:
            pass

    def handle_order_cancellation(self, str order_id):
        """Clean up pending order tracking on order cancellation."""
        # Try both buy and sell
        self.handle_order_completion(order_id, True)
        self.handle_order_completion(order_id, False)

    def handle_order_timeout(self, str order_id):
        """Clean up pending order tracking on order timeout."""
        self.handle_order_completion(order_id, True)
        self.handle_order_completion(order_id, False)

    def handle_old_order_cleanup(self, str order_id):
        """Clean up pending order tracking during old order cleanup."""
        self.handle_order_completion(order_id, True)
        self.handle_order_completion(order_id, False)

    def get_status_lines(self, list unique_tuples, dict balance_map):
        """Get position balancer status lines for format_status()."""
        if not self._buy_enabled and not self._sell_enabled:
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
        any_active = False
        for a in base_assets:
            # Get reference bid
            bid = self.strategy.c_get_reference_bid_for_asset(a)
            # Aggregate base balance
            total_base = 0.0
            for t in unique_tuples:
                if t.base_asset == a:
                    total_base += float(balance_map.get((t.market.name, a), 0.0))
            total_value = total_base * bid

            # Status flags
            buy_status = "active" if (self._buy_enabled and not self._buy_completed) else "completed"
            sell_status = "active" if (self._sell_enabled and not self._sell_completed) else "completed"

            if buy_status == "active" or sell_status == "active":
                any_active = True

            status_str = f"buy:{buy_status}"
            if self._sell_enabled:
                status_str += f" sell:{sell_status}"

            agg_lines.append(
                f"    {a}: base={total_base:.6f} value={total_value:.6f} ({status_str})")

        if any_active:
            header = "  Position Balancer:"
            if self._buy_enabled and not self._buy_completed:
                header += f" buy_target={self._buy_target_usd:.6f}"
            if self._sell_enabled and not self._sell_completed:
                header += f" sell_target={self._sell_target_usd:.6f}"

            lines.extend([
                "",
                header
            ])
            lines.extend(agg_lines)

        return lines

    cdef object c_find_best_buy_market(self, str asset):
        """
        Find the best market to place a buy order for the given asset.
        Returns the market tuple with the highest bid price.
        """
        cdef:
            object best_market = None
            double best_bid = 0.0
            double current_bid
            OrderBook ob
            object mp
            object market_tuple

        try:
            for mp in self.strategy._market_pairs:
                # Check both first and second markets for this asset
                for market_tuple in [mp.first, mp.second]:
                    if market_tuple.base_asset == asset:
                        try:
                            ob = market_tuple.market.c_get_order_book(market_tuple.trading_pair)
                            current_bid = ob._best_bid

                            if current_bid > best_bid:
                                best_bid = current_bid
                                best_market = market_tuple
                        except Exception:
                            continue
        except Exception as e:
            self.strategy.logger().warning(f"Error finding best buy market for {asset}: {e}")

        return best_market

    cdef object c_find_best_sell_market(self, str asset):
        """
        Find the best market to place a sell order for the given asset.
        Returns the market tuple with the lowest ask price.
        """
        cdef:
            object best_market = None
            double best_ask = 1e100  # Large number
            double current_ask
            OrderBook ob
            object mp
            object market_tuple

        try:
            for mp in self.strategy._market_pairs:
                # Check both first and second markets for this asset
                for market_tuple in [mp.first, mp.second]:
                    if market_tuple.base_asset == asset:
                        try:
                            ob = market_tuple.market.c_get_order_book(market_tuple.trading_pair)
                            current_ask = ob._best_ask

                            if current_ask < best_ask and current_ask > 0:
                                best_ask = current_ask
                                best_market = market_tuple
                        except Exception:
                            continue
        except Exception as e:
            self.strategy.logger().warning(f"Error finding best sell market for {asset}: {e}")

        return best_market

    cdef void c_cancel_stale_orders(self, str asset):
        """Cancel stale buy/sell limit orders for refresh."""
        cdef double current_time = self.strategy._current_timestamp

        # Check buy orders
        if asset in self._active_buy_orders:
            last_time = self._last_buy_order_time.get(asset, 0.0)
            if current_time - last_time > self._limit_refresh_interval:
                order_id = self._active_buy_orders.get(asset)
                if order_id:
                    try:
                        # Find market tuple for this order
                        for mp in self.strategy._market_pairs:
                            if mp.first.base_asset == asset:
                                # Mark as timeout-cancelled to prevent cooldown enforcement
                                self.strategy._timeout_cancelled_orders.add(order_id)
                                # Remove from position balancer tracking to prevent main timeout check
                                self.strategy._position_balancer_orders.discard(order_id)

                                self.strategy.c_cancel_order(mp.first, order_id)
                                self.strategy.logger().info(
                                    f"Cancelled stale buy order {order_id} for {asset} (refresh)")
                                break
                    except Exception as e:
                        self.strategy.logger().warning(f"Failed to cancel stale buy order: {e}")

        # Check sell orders
        if asset in self._active_sell_orders:
            last_time = self._last_sell_order_time.get(asset, 0.0)
            if current_time - last_time > self._limit_refresh_interval:
                order_id = self._active_sell_orders.get(asset)
                if order_id:
                    try:
                        # Find market tuple for this order
                        for mp in self.strategy._market_pairs:
                            if mp.first.base_asset == asset:
                                # Mark as timeout-cancelled to prevent cooldown enforcement
                                self.strategy._timeout_cancelled_orders.add(order_id)
                                # Remove from position balancer tracking to prevent main timeout check
                                self.strategy._position_balancer_orders.discard(order_id)

                                self.strategy.c_cancel_order(mp.first, order_id)
                                self.strategy.logger().info(
                                    f"Cancelled stale sell order {order_id} for {asset} (refresh)")
                                break
                    except Exception as e:
                        self.strategy.logger().warning(f"Failed to cancel stale sell order: {e}")

    cdef bint c_handle_position_balancing(self, object buy_market_tuple, object sell_market_tuple):
        """
        Main entry point for position balancing.
        Decides whether to buy or sell based on current position.
        Returns True if an order was placed.

        Note: buy_market_tuple and sell_market_tuple params are kept for compatibility
        but we now select the best market internally based on bid/ask prices.
        """
        # CRITICAL SAFEGUARD: Prevent double execution
        if self.strategy._last_global_trade_timestamp == self.strategy._current_timestamp:
            return False

        if not self._buy_enabled and not self._sell_enabled:
            return False

        cdef:
            str asset_key = buy_market_tuple.base_asset
            double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
            double base_bal = self.c_get_adjusted_base_balance(asset_key)
            pair[double, double] val_result
            double current_value
            double shortfall_or_excess
            bint placed = False
            object selected_buy_market = None
            object selected_sell_market = None

        # Cancel stale orders for refresh
        self.c_cancel_stale_orders(asset_key)

        # Check if we need to buy
        if self._buy_enabled and not self._buy_completed:
            val_result = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
            current_value = val_result.first
            shortfall_or_excess = val_result.second

            if shortfall_or_excess > 0:
                # Check if already have pending buy order
                if asset_key not in self._active_buy_orders:
                    if not self.c_try_mark_buy_complete(asset_key, current_value, shortfall_or_excess):
                        # Find the best market to buy on (highest bid)
                        selected_buy_market = self.c_find_best_buy_market(asset_key)
                        if selected_buy_market is not None:
                            # For sell market, just use the other market from the pair
                            # (not critical since we're only buying)
                            selected_sell_market = sell_market_tuple
                            placed = self.c_execute_buy_limit(selected_buy_market, selected_sell_market)
                            if placed:
                                return True

        # Check if we need to sell
        if self._sell_enabled and not self._sell_completed:
            val_result = self.c_compute_value_and_sell_excess(base_bal, last_bid)
            current_value = val_result.first
            shortfall_or_excess = val_result.second

            if shortfall_or_excess > 0:
                # Check if already have pending sell order
                if asset_key not in self._active_sell_orders:
                    if not self.c_try_mark_sell_complete(asset_key, current_value, shortfall_or_excess):
                        # Find the best market to sell on (lowest ask)
                        selected_sell_market = self.c_find_best_sell_market(asset_key)
                        if selected_sell_market is not None:
                            # For buy market, just use the other market from the pair
                            # (not critical since we're only selling)
                            selected_buy_market = buy_market_tuple
                            placed = self.c_execute_sell_limit(selected_buy_market, selected_sell_market)
                            if placed:
                                return True

        return False

    cdef bint c_execute_buy_limit(self, object buy_market_tuple, object sell_market_tuple):
        """
        Execute buy using limit order with spread from top bid.
        Pattern: Place limit buy at (top_bid * (1 - spread_pct))
        """
        # CRITICAL: Set timestamp IMMEDIATELY to prevent race condition
        self.strategy._last_global_trade_timestamp = self.strategy._current_timestamp

        cdef:
            str asset_key = buy_market_tuple.base_asset
            ExchangeBase market = buy_market_tuple.market
            double quote_bal = float(market.c_get_available_balance(buy_market_tuple.quote_asset))
            double base_bal = self.c_get_adjusted_base_balance(asset_key)
            double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
            pair[double, double] val_short = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
            double current_value_quote = val_short.first
            double shortfall = val_short.second

        # Check if still needed
        if self.c_try_mark_buy_complete(asset_key, current_value_quote, shortfall):
            return False

        if quote_bal <= 0:
            return False

        # Get top bid price from cached order book value
        cdef:
            OrderBook ob = market.c_get_order_book(buy_market_tuple.trading_pair)
            double top_bid
            double buy_price
            double max_affordable_base
            double amount_to_buy
            object quantized_amount
            object order_type = OrderType.LIMIT
            double volume_usd
            str buy_order_id
            string buy_id_str

        try:
            top_bid = ob._best_bid
        except Exception:
            return False

        if top_bid <= 0:
            return False

        # Calculate limit price: top_bid * (1 - spread_pct)
        buy_price = top_bid * (1.0 - self._buy_spread_pct)

        # Calculate amount based on shortfall, available quote, and order size limit
        max_affordable_base = quote_bal / buy_price if buy_price > 0 else 0.0
        max_order_base = self._order_size_usd / buy_price if buy_price > 0 else 0.0
        amount_to_buy = min(
            shortfall / last_bid if last_bid > 0 else 0.0,
            max_affordable_base,
            max_order_base
        )

        if amount_to_buy <= EPSILON:
            return False

        # Quantize
        quantized_amount = self.strategy.c_safe_quantize_order_amount(
            market, buy_market_tuple.trading_pair,
            Decimal(str(max(0.0, amount_to_buy - QUANTIZATION_EPSILON))),
            Decimal(str(buy_price)))

        if quantized_amount <= Decimal("0"):
            return False

        # Apply max_order_size cap
        try:
            trading_rule = market._trading_rules.get(buy_market_tuple.trading_pair)
            if trading_rule is not None and trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > trading_rule.max_order_size:
                    quantized_amount = trading_rule.max_order_size
        except Exception:
            pass

        # Check minimum notional
        volume_usd = float(quantized_amount) * buy_price
        if volume_usd < self.strategy._min_order_usd:
            # Too small, mark complete
            if self.c_try_mark_buy_complete(asset_key, current_value_quote, shortfall):
                return False
            return False

        # Place limit buy order
        try:
            buy_order_id = self.strategy.c_buy_with_specific_market(
                buy_market_tuple,
                quantized_amount,
                order_type=order_type,
                price=Decimal(str(buy_price)),
                expiration_seconds=self.strategy._next_trade_delay)
        except Exception as e:
            self.strategy._last_failure_timestamps[buy_market_tuple] = self.strategy._current_timestamp
            self.strategy.logger().warning(f"Error submitting buy limit order to {market.name}: {e}")
            return False

        # Track order
        buy_id_str = self.strategy._to_cpp_str(buy_order_id)
        self.strategy._order_timestamps[buy_id_str] = self.strategy._current_timestamp

        # Track pending BUY order in strategy
        try:
            if buy_market_tuple not in self.strategy._pending_buy_orders_by_market:
                self.strategy._pending_buy_orders_by_market[buy_market_tuple] = set()
            self.strategy._pending_buy_orders_by_market[buy_market_tuple].add(buy_order_id)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track pending buy order by market: {e}")

        # Mark as position balancer order to prevent main strategy timeout cancellation
        try:
            self.strategy._position_balancer_orders.add(buy_order_id)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to mark order as position balancer order: {e}")

        # Track in balancer
        try:
            self._pending_buy_orders[buy_order_id] = (asset_key, float(quantized_amount))
            self._pending_buy_by_asset[asset_key] = (
                float(self._pending_buy_by_asset.get(asset_key, 0.0)) + float(quantized_amount))
            self._active_buy_orders[asset_key] = buy_order_id
            self._last_buy_order_time[asset_key] = self.strategy._current_timestamp
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track buy limit order {buy_order_id}: {e}")

        self.strategy.logger().info(
            f"Placed buy limit order {buy_order_id} for {float(quantized_amount):.6f} {asset_key} "
            f"at {buy_price:.8f} (spread: {self._buy_spread_pct * 100:.2f}%)")

        # Check if target reached
        base_bal = self.c_get_adjusted_base_balance(asset_key)
        val_short = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
        current_value_quote = val_short.first
        shortfall = val_short.second
        if self.c_try_mark_buy_complete(asset_key, current_value_quote, shortfall):
            self.c_maybe_disable_buy()

        return True

    cdef bint c_execute_sell_limit(self, object buy_market_tuple, object sell_market_tuple):
        """
        Execute sell using limit order with spread from top ask.
        Pattern: Place limit sell at (top_ask * (1 + spread_pct))
        """
        # CRITICAL: Set timestamp IMMEDIATELY to prevent race condition
        self.strategy._last_global_trade_timestamp = self.strategy._current_timestamp

        cdef:
            str asset_key = sell_market_tuple.base_asset
            ExchangeBase market = sell_market_tuple.market
            double base_bal_raw = float(market.c_get_available_balance(sell_market_tuple.base_asset))
            double base_bal = self.c_get_adjusted_base_balance(asset_key)
            double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
            pair[double, double] val_excess = self.c_compute_value_and_sell_excess(base_bal, last_bid)
            double current_value_quote = val_excess.first
            double excess = val_excess.second

        # Check if still needed
        if self.c_try_mark_sell_complete(asset_key, current_value_quote, excess):
            return False

        if base_bal_raw <= 0:
            return False

        # Get top ask price from cached order book value
        cdef:
            OrderBook ob = market.c_get_order_book(sell_market_tuple.trading_pair)
            double top_ask
            double sell_price
            double amount_to_sell
            object quantized_amount
            object order_type = OrderType.LIMIT
            double volume_usd
            str sell_order_id
            string sell_id_str

        try:
            top_ask = ob._best_ask
        except Exception:
            return False

        if top_ask <= 0:
            return False

        # Calculate limit price: top_ask * (1 + spread_pct)
        sell_price = top_ask * (1.0 + self._sell_spread_pct)

        # Calculate amount based on excess, available base, and order size limit
        max_order_base = self._order_size_usd / sell_price if sell_price > 0 else 0.0
        amount_to_sell = min(
            excess / last_bid if last_bid > 0 else 0.0,
            base_bal_raw,
            max_order_base
        )

        if amount_to_sell <= EPSILON:
            return False

        # Quantize
        quantized_amount = self.strategy.c_safe_quantize_order_amount(
            market, sell_market_tuple.trading_pair,
            Decimal(str(max(0.0, amount_to_sell - QUANTIZATION_EPSILON))),
            Decimal(str(sell_price)))

        if quantized_amount <= Decimal("0"):
            return False

        # Apply max_order_size cap
        try:
            trading_rule = market._trading_rules.get(sell_market_tuple.trading_pair)
            if trading_rule is not None and trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > trading_rule.max_order_size:
                    quantized_amount = trading_rule.max_order_size
        except Exception:
            pass

        # Check minimum notional
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self.strategy._min_order_usd:
            # Too small, mark complete
            if self.c_try_mark_sell_complete(asset_key, current_value_quote, excess):
                return False
            return False

        # Place limit sell order
        try:
            sell_order_id = self.strategy.c_sell_with_specific_market(
                sell_market_tuple,
                quantized_amount,
                order_type=order_type,
                price=Decimal(str(sell_price)),
                expiration_seconds=self.strategy._next_trade_delay)
        except Exception as e:
            self.strategy._last_failure_timestamps[sell_market_tuple] = self.strategy._current_timestamp
            self.strategy.logger().warning(f"Error submitting sell limit order to {market.name}: {e}")
            return False

        # Track order
        sell_id_str = self.strategy._to_cpp_str(sell_order_id)
        self.strategy._order_timestamps[sell_id_str] = self.strategy._current_timestamp

        # Track pending SELL order in strategy
        try:
            if sell_market_tuple not in self.strategy._pending_sell_orders_by_market:
                self.strategy._pending_sell_orders_by_market[sell_market_tuple] = set()
            self.strategy._pending_sell_orders_by_market[sell_market_tuple].add(sell_order_id)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track pending sell order by market: {e}")

        # Mark as position balancer order to prevent main strategy timeout cancellation
        try:
            self.strategy._position_balancer_orders.add(sell_order_id)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to mark order as position balancer order: {e}")

        # Track in balancer
        try:
            self._pending_sell_orders[sell_order_id] = (asset_key, float(quantized_amount))
            self._pending_sell_by_asset[asset_key] = (
                float(self._pending_sell_by_asset.get(asset_key, 0.0)) + float(quantized_amount))
            self._active_sell_orders[asset_key] = sell_order_id
            self._last_sell_order_time[asset_key] = self.strategy._current_timestamp
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track sell limit order {sell_order_id}: {e}")

        self.strategy.logger().info(
            f"Placed sell limit order {sell_order_id} for {float(quantized_amount):.6f} {asset_key} "
            f"at {sell_price:.8f} (spread: {self._sell_spread_pct * 100:.2f}%)")

        # Check if target reached
        base_bal = self.c_get_adjusted_base_balance(asset_key)
        val_excess = self.c_compute_value_and_sell_excess(base_bal, last_bid)
        current_value_quote = val_excess.first
        excess = val_excess.second
        if self.c_try_mark_sell_complete(asset_key, current_value_quote, excess):
            self.c_maybe_disable_sell()

        return True