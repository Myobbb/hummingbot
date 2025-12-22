# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp
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
from cython.operator cimport dereference as deref

from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.order_book cimport OrderBook

s_decimal_zero = Decimal(0)
s_decimal_nan = Decimal("NaN")
EPSILON = 1e-9
QUANTIZATION_EPSILON = 1e-9

# =============================================================================
# POSITION BALANCER TIMING & THRESHOLD CONSTANTS
# All timing and threshold values are consolidated here for easy tweaking
# =============================================================================

# --- Order Hanging Intervals (seconds) ---
# How long orders hang before being refreshed (cancelled and replaced)
cdef double DEFAULT_LIMIT_REFRESH_INTERVAL = 60.0       # Default refresh for min/set_spread modes
cdef double DEFAULT_AGGRESSIVE_REFRESH_INTERVAL = 5.0   # Refresh for aggressive (0%) mode with partial fills
cdef double DEFAULT_COMPLETION_COOLDOWN = 2.0            # Cooldown after order completion before placing new order

# --- Stuck Cancel Detection ---
cdef double STUCK_CANCEL_MULTIPLIER = 2.0  # Multiplier for stuck cancel detection (2x refresh interval)

# --- Price Threshold Constants ---
cdef double TICK_TOLERANCE = 0.9            # Tolerance for detecting if order matches top of book (90% of tick)
cdef double HALF_TICK_TOLERANCE = 0.5       # Half-tick tolerance for price matching
cdef double LARGE_GAP_THRESHOLD = 1.9       # Multi-tick gap threshold for immediate cancellation (2 ticks)
cdef double AGGRESSIVE_MODE_TOLERANCE = 0.999  # Price tolerance for aggressive mode (0.1%)
cdef double PRICE_DIVERGENCE_THRESHOLD_PCT = 0.01  # 1% threshold for percentage mode divergence

# --- Better Market Switch ---
# Tolerance for triggering immediate market switch (price difference as ratio)
# e.g., 0.0001 = 0.01% = switch if other market is 0.01% better
cdef double BETTER_MARKET_SWITCH_TOLERANCE = 0.0001  # 0.01% - switch immediately if another market is better

# Min mode hysteresis: only switch if new market is at least 0.1% better
# This prevents flip-flopping between markets with nearly identical effective prices
cdef double MIN_MODE_SWITCH_HYSTERESIS = 0.001  # 0.1% - require meaningful improvement before switching


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
                 object buy_spread_pct,  # float or 'min'
                 bint sell_enabled,
                 double sell_target_usd,
                 object sell_spread_pct,  # float or 'min'
                 double limit_refresh_interval,
                 double order_size_usd=100.0,
                 double aggressive_refresh_interval=5.0):
        """
        Initialize position balancer handler.

        Args:
            strategy: Reference to ArbitrageLStrategy instance
            buy_enabled: Whether buy-in is enabled
            buy_target_usd: Target minimum asset value in quote currency
            buy_spread_pct: Spread percentage or 'min' for minimum tick
                - float (e.g., 0.1 = 0.1% above top bid)
                - 'min' = one minimum tick above top bid (maker order frontrunning)
            sell_enabled: Whether sell-off is enabled
            sell_target_usd: Target maximum asset value in quote currency
            sell_spread_pct: Spread percentage or 'min' for minimum tick
                - float (e.g., 0.1 = 0.1% below top ask)
                - 'min' = one minimum tick below top ask (maker order frontrunning)
            limit_refresh_interval: How often to cancel and replace limit orders (seconds)
            order_size_usd: Maximum order size in USD per order (default: 100.0)
        """
        self.strategy = strategy

        # Buy-in configuration
        self._buy_enabled = buy_enabled
        self._buy_target_usd = buy_target_usd
        # Handle 'min' or numeric spread
        if isinstance(buy_spread_pct, str) and buy_spread_pct.lower() == 'min':
            self._buy_spread_pct = -1.0  # Special flag for minimum tick
            self._buy_spread_is_min = True
        else:
            self._buy_spread_pct = float(buy_spread_pct) / 100.0  # Convert to decimal
            self._buy_spread_is_min = False

        # Sell-off configuration
        self._sell_enabled = sell_enabled
        self._sell_target_usd = sell_target_usd
        # Handle 'min' or numeric spread
        if isinstance(sell_spread_pct, str) and sell_spread_pct.lower() == 'min':
            self._sell_spread_pct = -1.0  # Special flag for minimum tick
            self._sell_spread_is_min = True
        else:
            self._sell_spread_pct = float(sell_spread_pct) / 100.0  # Convert to decimal
            self._sell_spread_is_min = False

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
        self._aggressive_refresh_interval = aggressive_refresh_interval
        self._last_buy_order_time = {}    # asset -> timestamp
        self._last_sell_order_time = {}   # asset -> timestamp
        self._active_buy_orders = {}      # asset -> order_id
        self._active_sell_orders = {}     # asset -> order_id
        self._active_buy_order_details = {}   # asset -> (market_tuple, price)
        self._active_sell_order_details = {}  # asset -> (market_tuple, price)
        self._buy_cancel_request_time = {}    # asset -> timestamp when cancel was requested
        self._sell_cancel_request_time = {}   # asset -> timestamp when cancel was requested
        self._last_buy_completion_time = {}   # asset -> timestamp when buy order completed
        self._last_sell_completion_time = {}  # asset -> timestamp when sell order completed

        # Asset alias support (for cross-exchange pairs with different token names)
        # Dictionary mapping asset name to list of aliases (including itself)
        self._asset_aliases = {}
        # Dictionary mapping alias to canonical asset name
        self._canonical_asset = {}
        self._build_asset_aliases()
        
        # Cache for invariant trading rules
        self._min_price_increment_cache = {}

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

    cdef void _build_asset_aliases(self):
        """
        Build asset alias mappings for cross-exchange pairs with different token names.

        When base assets have 1:1 conversion (no oracle, conversion_rate=1.0), they're treated
        as the same asset with different names (e.g., NODE on kucoin, NODEOPS on htx).

        This enables:
        - Aggregating balances across both names (NODE + NODEOPS total)
        - Managing orders for both assets as a unified position
        - Preventing timeout issues from unmanaged "other" asset orders
        """
        try:
            # Check if using 1:1 base conversion (no oracle, rate = 1.0)
            if self.strategy._use_oracle_conversion_rate:
                return  # Oracle mode - assets are independent

            if abs(self.strategy._fixed_base_rate - 1.0) > 1e-9:
                return  # Non-1:1 rate - assets are independent

            # Collect unique base assets from all market pairs
            assets = set()
            for mp in self.strategy._market_pairs:
                assets.add(mp.first.base_asset)
                assets.add(mp.second.base_asset)

            if len(assets) <= 1:
                return  # Only one asset - no aliases needed

            # Multiple assets with 1:1 conversion - treat as aliases
            # Use alphabetically first asset as canonical name
            assets_list = sorted(list(assets))
            canonical = assets_list[0]

            self._asset_aliases[canonical] = assets_list
            for asset in assets_list:
                self._canonical_asset[asset] = canonical

            self.strategy.log_with_clock(
                logging.INFO,
                f"Position balancer: Detected asset aliases {assets_list} "
                f"(1:1 conversion, using '{canonical}' as canonical name)")
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to build asset aliases: {e}")

    cdef str _get_canonical_asset(self, str asset):
        """Get canonical asset name for an asset (or return itself if no alias)."""
        return self._canonical_asset.get(asset, asset)

    cdef list _get_all_asset_aliases(self, str asset):
        """Get all aliases for an asset (including itself)."""
        canonical = self._get_canonical_asset(asset)
        return self._asset_aliases.get(canonical, [asset])

    cdef void c_cancel_all_buy_orders(self):
        """
        Cancel all active buy orders and clean up tracking.

        Uses the same pattern as individual order cancellation:
        - Mark orders in _timeout_cancelled_orders to prevent cooldown
        - Send cancel requests
        - Wait for cancel events to clean up tracking (via handle_order_cancellation)

        This ensures consistent state management and prevents race conditions.
        """
        cdef:
            list assets_to_cancel = list(self._active_buy_orders.keys())
            str asset
            str order_id

        for asset in assets_to_cancel:
            order_id = self._active_buy_orders.get(asset)
            if order_id:
                # Use the centralized cancel method for consistency
                self._cancel_buy_order(asset, order_id, "target reached / mode disabled")

        # NOTE: We do NOT clear tracking dictionaries here!
        # Each order's tracking will be cleaned up when its cancel event arrives
        # via handle_order_cancellation() -> handle_order_completion()
        # This prevents race conditions and ensures atomic cleanup.

    cdef void c_cancel_all_sell_orders(self):
        """
        Cancel all active sell orders and clean up tracking.

        Uses the same pattern as individual order cancellation:
        - Mark orders in _timeout_cancelled_orders to prevent cooldown
        - Send cancel requests
        - Wait for cancel events to clean up tracking (via handle_order_cancellation)

        This ensures consistent state management and prevents race conditions.
        """
        cdef:
            list assets_to_cancel = list(self._active_sell_orders.keys())
            str asset
            str order_id

        for asset in assets_to_cancel:
            order_id = self._active_sell_orders.get(asset)
            if order_id:
                # Use the centralized cancel method for consistency
                self._cancel_sell_order(asset, order_id, "target reached / mode disabled")

        # NOTE: We do NOT clear tracking dictionaries here!
        # Each order's tracking will be cleaned up when its cancel event arrives
        # via handle_order_cancellation() -> handle_order_completion()
        # This prevents race conditions and ensures atomic cleanup.

    cdef void c_maybe_disable_buy(self):
        """Disable buy-in globally once target is reached, cancel orders, and clean up."""
        if self._buy_completed and self._buy_enabled:
            self._buy_enabled = False
            # Cancel all active buy orders
            self.c_cancel_all_buy_orders()
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy-in target reached. Cancelled all buy orders and disabled buy-in for this session.")

    cdef void c_maybe_disable_sell(self):
        """Disable sell-off globally once target is reached, cancel orders, and clean up."""
        if self._sell_completed and self._sell_enabled:
            self._sell_enabled = False
            # Cancel all active sell orders
            self.c_cancel_all_sell_orders()
            self.strategy.log_with_clock(
                logging.INFO,
                "Sell-off target reached. Cancelled all sell orders and disabled sell-off for this session.")

    cdef double c_get_pending_buy_base(self, str asset):
        """
        Return in-flight buy order base amount for asset.
        For assets with aliases (e.g., NODE/NODEOPS), sums across all aliases.
        """
        cdef:
            double total = 0.0
            list aliases
            str alias
        try:
            aliases = self._get_all_asset_aliases(asset)
            for alias in aliases:
                total += <double> self._pending_buy_by_asset.get(alias, 0.0)
            return total
        except Exception:
            return 0.0

    cdef double c_get_pending_sell_base(self, str asset):
        """
        Return in-flight sell order base amount for asset.
        For assets with aliases (e.g., NODE/NODEOPS), sums across all aliases.
        """
        cdef:
            double total = 0.0
            list aliases
            str alias
        try:
            aliases = self._get_all_asset_aliases(asset)
            for alias in aliases:
                total += <double> self._pending_sell_by_asset.get(alias, 0.0)
            return total
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
        """
        Aggregate base balance using the same source as status (balance_map).
        For assets with aliases (e.g., NODE/NODEOPS), sums across all aliases.
        OPTIMIZED: Fetches directly from markets to avoid DataFrame overhead.
        """
        cdef:
            double total = 0.0
            list aliases
            set checked_keys = set()
            object mp
            object market_tuple
            tuple key
            
        try:
            aliases = self._get_all_asset_aliases(asset)
            for mp in self.strategy._market_pairs:
                for market_tuple in [mp.first, mp.second]:
                    if market_tuple.base_asset in aliases:
                        # Ensure we don't double count the same market+asset combo
                        key = (market_tuple.market, market_tuple.base_asset)
                        if key not in checked_keys:
                            checked_keys.add(key)
                            # Use get_available_balance to match original behavior (only free funds)
                            total += float(market_tuple.market.get_available_balance(market_tuple.base_asset))
        except Exception:
            return 0.0
        return total

    cdef double c_get_actual_base_balance(self, str asset):
        """
        Get ACTUAL base balance without including pending unfilled orders.
        This is used for target completion checking.
        Only counts what's actually in the wallet, not what's in open orders.
        """
        return self.c_get_aggregated_base_balance(asset)

    cdef double c_get_adjusted_base_balance(self, str asset):
        """
        Get adjusted base balance accounting for pending orders.
        For buy target checking: add pending buys
        For sell target checking: subtract pending sells

        This is used for deciding whether to place NEW orders,
        to avoid over-ordering when we already have pending orders.
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
            str canonical_asset
            double last_bid = 0.0
            double base_bal
            double pending_buy
            double pending_sell
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
        canonical_asset = self._get_canonical_asset(asset_key)

        # Get reference bid using canonical asset
        last_bid = self.strategy.c_get_reference_bid_for_asset(canonical_asset)

        # Build balances
        # OPTIMIZATION: c_get_actual_base_balance now fetches directly from connectors
        # We don't need to build the full DataFrame/balance_map here
        # unique_tuples, assets_df, balance_map = self.strategy.c_build_unique_tuples_assets_and_balance_map()

        # Sum base balance across ALL aliases - USE ACTUAL BALANCE for target completion checking
        # Do NOT include pending orders here, only count what's actually filled
        base_bal = self.c_get_actual_base_balance(canonical_asset)

        # Check buy completion
        if self._buy_enabled and not self._buy_completed:
            val_short = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
            current_value_quote = val_short.first
            shortfall = val_short.second

            try:
                decision = "complete" if (current_value_quote >= self._buy_target_usd or
                                         (shortfall > 0 and shortfall < self.strategy._min_order_usd)) else "active"
                pending_buy = self.c_get_pending_buy_base(canonical_asset)
                self.strategy.log_with_clock(
                    logging.INFO,
                    f"Buy-in check: asset={canonical_asset} actual_base={base_bal:.6f} pending={pending_buy:.6f} "
                    f"bid={last_bid:.6f} value={current_value_quote:.6f} target={self._buy_target_usd:.6f} -> {decision}")
            except Exception:
                pass

            self.c_try_mark_buy_complete(canonical_asset, current_value_quote, shortfall)

        # Check sell completion
        if self._sell_enabled and not self._sell_completed:
            val_excess = self.c_compute_value_and_sell_excess(base_bal, last_bid)
            current_value_quote = val_excess.first
            excess = val_excess.second

            try:
                decision = "complete" if (current_value_quote <= self._sell_target_usd or
                                         (excess > 0 and excess < self.strategy._min_order_usd)) else "active"
                pending_sell = self.c_get_pending_sell_base(canonical_asset)
                self.strategy.log_with_clock(
                    logging.INFO,
                    f"Sell-off check: asset={canonical_asset} actual_base={base_bal:.6f} pending={pending_sell:.6f} "
                    f"bid={last_bid:.6f} value={current_value_quote:.6f} target={self._sell_target_usd:.6f} -> {decision}")
            except Exception:
                pass

            self.c_try_mark_sell_complete(canonical_asset, current_value_quote, excess)

        self.c_maybe_disable_buy()
        self.c_maybe_disable_sell()

    def handle_order_fill(self, str order_id, double filled_amount):
        """
        Update pending amounts when order receives a fill.
        This ensures accurate balance tracking for partial fills.
        """
        try:
            # Check if this is a buy order
            if order_id in self._pending_buy_orders:
                asset_key, total_amt, prev_filled = self._pending_buy_orders[order_id]
                new_filled = prev_filled + filled_amount
                self._pending_buy_orders[order_id] = (asset_key, total_amt, new_filled)
                # Subtract filled amount from pending (it's now in our balance)
                self._pending_buy_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_buy_by_asset.get(asset_key, 0.0)) - filled_amount)
                if self._pending_buy_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_buy_by_asset.pop(asset_key, None)
            # Check if this is a sell order
            elif order_id in self._pending_sell_orders:
                asset_key, total_amt, prev_filled = self._pending_sell_orders[order_id]
                new_filled = prev_filled + filled_amount
                self._pending_sell_orders[order_id] = (asset_key, total_amt, new_filled)
                # Subtract filled amount from pending (we've sold it)
                self._pending_sell_by_asset[asset_key] = max(
                    0.0,
                    float(self._pending_sell_by_asset.get(asset_key, 0.0)) - filled_amount)
                if self._pending_sell_by_asset.get(asset_key, 0.0) <= 1e-15:
                    self._pending_sell_by_asset.pop(asset_key, None)
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to handle fill for {order_id}: {e}")

    def handle_order_completion(self, str order_id, bint is_buy):
        """
        Clean up pending order tracking on order completion.
        Only subtracts unfilled amount (filled amounts already subtracted in handle_order_fill).
        """
        try:
            # Remove from position balancer tracking set
            self.strategy._position_balancer_orders.discard(order_id)

            if is_buy:
                pend = self._pending_buy_orders.pop(order_id, None)
                if pend is not None:
                    asset_key, total_amt, filled_amt = pend
                    unfilled_amt = total_amt - filled_amt
                    # Only subtract unfilled amount (filled was already subtracted in handle_order_fill)
                    if unfilled_amt > 0:
                        self._pending_buy_by_asset[asset_key] = max(
                            0.0,
                            float(self._pending_buy_by_asset.get(asset_key, 0.0)) - unfilled_amt)
                        if self._pending_buy_by_asset.get(asset_key, 0.0) <= 1e-15:
                            self._pending_buy_by_asset.pop(asset_key, None)
                    # Remove from active tracking
                    if self._active_buy_orders.get(asset_key) == order_id:
                        self._active_buy_orders.pop(asset_key, None)
                        # Also remove order details
                        self._active_buy_order_details.pop(asset_key, None)
                        # Clean up cancel request time
                        self._buy_cancel_request_time.pop(asset_key, None)
                        # Record completion time for cooldown
                        self._last_buy_completion_time[asset_key] = self.strategy._current_timestamp
            else:
                pend = self._pending_sell_orders.pop(order_id, None)
                if pend is not None:
                    asset_key, total_amt, filled_amt = pend
                    unfilled_amt = total_amt - filled_amt
                    # Only subtract unfilled amount (filled was already subtracted in handle_order_fill)
                    if unfilled_amt > 0:
                        self._pending_sell_by_asset[asset_key] = max(
                            0.0,
                            float(self._pending_sell_by_asset.get(asset_key, 0.0)) - unfilled_amt)
                        if self._pending_sell_by_asset.get(asset_key, 0.0) <= 1e-15:
                            self._pending_sell_by_asset.pop(asset_key, None)
                    # Remove from active tracking
                    if self._active_sell_orders.get(asset_key) == order_id:
                        self._active_sell_orders.pop(asset_key, None)
                        # Also remove order details
                        self._active_sell_order_details.pop(asset_key, None)
                        # Clean up cancel request time
                        self._sell_cancel_request_time.pop(asset_key, None)
                        # Record completion time for cooldown
                        self._last_sell_completion_time[asset_key] = self.strategy._current_timestamp
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to handle completion for {order_id}: {e}")

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
        For assets with aliases (e.g., NODE/NODEOPS), considers ALL alias markets
        and selects the best one regardless of name.

        - Aggressive (0%): Select market with LOWEST ASK (taker)
        - Percentage (>0%): Select market with LOWEST BID (maker above bid)
        - 'min' mode: Select market with LOWEST EFFECTIVE BUY PRICE (bid + min_tick)
          This accounts for different min_price_increment across markets.
        """
        cdef:
            object best_market = None
            double best_price = 1e100  # Start with very high number
            double current_price
            double current_bid
            double min_tick
            OrderBook ob
            object mp
            object market_tuple
            list asset_aliases
            bint use_ask_price = (not self._buy_spread_is_min and self._buy_spread_pct == 0.0)
            bint use_effective_price = self._buy_spread_is_min  # 'min' mode needs effective price
            object trading_rule

        # Get all aliases for this asset (includes asset itself)
        asset_aliases = self._get_all_asset_aliases(asset)

        try:
            for mp in self.strategy._market_pairs:
                # Check both first and second markets for ANY asset alias
                for market_tuple in [mp.first, mp.second]:
                    if market_tuple.base_asset in asset_aliases:
                        try:
                            # Use C-level get_order_book for compatibility with all exchanges
                            ob = (<ExchangeBase>market_tuple.market).c_get_order_book(market_tuple.trading_pair)

                            if use_ask_price:
                                # Aggressive mode (0%): select market with LOWEST ASK (taker)
                                if ob._ask_book.size() > 0:
                                    current_price = float(deref(ob._ask_book.begin()).getPrice())
                                else:
                                    continue
                            elif use_effective_price:
                                # 'min' mode: calculate effective frontrun price (bid + min_tick)
                                # This accounts for different min_price_increment across markets
                                if ob._bid_book.size() > 0:
                                    current_bid = float(deref(ob._bid_book.rbegin()).getPrice())
                                    # Get min_price_increment for this market
                                    min_tick = 0.0
                                    trading_rule = market_tuple.market._trading_rules.get(market_tuple.trading_pair)
                                    if trading_rule is not None and trading_rule.min_price_increment is not None:
                                        min_tick = float(trading_rule.min_price_increment)
                                    if min_tick > 0:
                                        current_price = current_bid + min_tick  # Effective buy price
                                    else:
                                        current_price = current_bid  # Fallback to raw bid
                                else:
                                    continue
                            else:
                                # Percentage mode (>0%): select market with LOWEST BID (maker)
                                if ob._bid_book.size() > 0:
                                    current_price = float(deref(ob._bid_book.rbegin()).getPrice())
                                else:
                                    continue

                            if current_price < best_price and current_price > 0:
                                best_price = current_price
                                best_market = market_tuple
                        except Exception:
                            continue
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Error finding best buy market for {asset} (aliases: {asset_aliases}): {e}")

        return best_market

    cdef object c_find_best_sell_market(self, str asset):
        """
        Find the best market to place a sell order for the given asset.
        For assets with aliases (e.g., NODE/NODEOPS), considers ALL alias markets
        and selects the best one regardless of name.

        - Aggressive (0%): Select market with HIGHEST BID (taker)
        - Percentage (>0%): Select market with HIGHEST ASK (maker below ask)
        - 'min' mode: Select market with HIGHEST EFFECTIVE SELL PRICE (ask - min_tick)
          This accounts for different min_price_increment across markets.
        """
        cdef:
            object best_market = None
            double best_price = 0.0
            double current_price
            double current_ask
            double min_tick
            OrderBook ob
            object mp
            object market_tuple
            list asset_aliases
            bint use_bid_price = (not self._sell_spread_is_min and self._sell_spread_pct == 0.0)
            bint use_effective_price = self._sell_spread_is_min  # 'min' mode needs effective price
            object trading_rule

        # Get all aliases for this asset (includes asset itself)
        asset_aliases = self._get_all_asset_aliases(asset)

        try:
            for mp in self.strategy._market_pairs:
                # Check both first and second markets for ANY asset alias
                for market_tuple in [mp.first, mp.second]:
                    if market_tuple.base_asset in asset_aliases:
                        try:
                            # Use C-level get_order_book for compatibility with all exchanges
                            ob = (<ExchangeBase>market_tuple.market).c_get_order_book(market_tuple.trading_pair)

                            if use_bid_price:
                                # Aggressive mode (0%): select market with HIGHEST BID (taker)
                                if ob._bid_book.size() > 0:
                                    current_price = float(deref(ob._bid_book.rbegin()).getPrice())
                                else:
                                    continue
                            elif use_effective_price:
                                # 'min' mode: calculate effective frontrun price (ask - min_tick)
                                # This accounts for different min_price_increment across markets
                                if ob._ask_book.size() > 0:
                                    current_ask = float(deref(ob._ask_book.begin()).getPrice())
                                    # Get min_price_increment for this market
                                    min_tick = 0.0
                                    trading_rule = market_tuple.market._trading_rules.get(market_tuple.trading_pair)
                                    if trading_rule is not None and trading_rule.min_price_increment is not None:
                                        min_tick = float(trading_rule.min_price_increment)
                                    
                                    if min_tick > 0:
                                        current_price = current_ask - min_tick  # Effective sell price
                                    else:
                                        # WARNING: min_tick lookup failed - using raw ask (may cause wrong market selection)
                                        self.strategy.logger().warning(
                                            f"Position balancer: min_tick=0 for {market_tuple.market.name}:{market_tuple.trading_pair} - "
                                            f"falling back to raw ask. Check trading_rules!")
                                        current_price = current_ask  # Fallback to raw ask
                                else:
                                    continue
                            else:
                                # Percentage mode (>0%): select market with HIGHEST ASK (maker)
                                if ob._ask_book.size() > 0:
                                    current_price = float(deref(ob._ask_book.begin()).getPrice())
                                else:
                                    continue

                            if current_price > best_price:
                                # DEBUG: Log when a new best is found
                                if use_effective_price:
                                    self.strategy.logger().debug(
                                        f"[SELL EVAL] {market_tuple.market.name}: eff={current_price:.10f} > prev_best={best_price:.10f}, now best")
                                best_price = current_price
                                best_market = market_tuple
                            elif use_effective_price:
                                # Log when market is NOT selected
                                self.strategy.logger().debug(
                                    f"[SELL EVAL] {market_tuple.market.name}: eff={current_price:.10f} <= best={best_price:.10f}, skipped")
                        except Exception:
                            continue
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Error finding best sell market for {asset} (aliases: {asset_aliases}): {e}")

        return best_market

    # ========================================================================
    # HELPER METHODS FOR CANCELLATION LOGIC
    # ========================================================================

    cdef bint c_check_stuck_cancel(self, str order_id, str asset, bint is_buy, double current_time):
        """
        Detect and cleanup stuck cancellations.
        
        Returns True if stuck cancel was detected and cleaned up, False otherwise.
        """
        cdef:
            double cancel_request_time
            double time_since_cancel_request
            double timeout_threshold = self._limit_refresh_interval * STUCK_CANCEL_MULTIPLIER
        
        if order_id not in self.strategy._timeout_cancelled_orders:
            return False
        
        # Get cancel request time based on order type
        if is_buy:
            cancel_request_time = self._buy_cancel_request_time.get(asset, 0.0)
        else:
            cancel_request_time = self._sell_cancel_request_time.get(asset, 0.0)
        
        if cancel_request_time <= 0:
            return False
        
        time_since_cancel_request = current_time - cancel_request_time
        
        # Force cleanup if cancel has been pending for > 2x refresh interval
        if time_since_cancel_request > timeout_threshold:
            order_type = "buy" if is_buy else "sell"
            self.strategy.logger().warning(
                f"Position balancer: Stuck cancel detected for {order_type} order {order_id} "
                f"(pending {time_since_cancel_request:.0f}s > {timeout_threshold:.0f}s) - force cleanup")
            
            # Force cleanup by calling handle_order_cancellation
            self.handle_order_cancellation(order_id)
            
            # Clean up from timeout set
            self.strategy._timeout_cancelled_orders.discard(order_id)
            
            # Clean up cancel request time
            if is_buy:
                self._buy_cancel_request_time.pop(asset, None)
            else:
                self._sell_cancel_request_time.pop(asset, None)
            
            return True
        
        return False

    cdef tuple c_get_orderbook_prices(self, OrderBook ob):
        """
        Get bid/ask prices from orderbook with safety checks.
        
        Returns: (bid, ask) tuple
        """
        cdef:
            double bid = 0.0
            double ask = 0.0
        
        if ob._bid_book.size() > 0:
            bid = float(deref(ob._bid_book.rbegin()).getPrice())
        
        if ob._ask_book.size() > 0:
            ask = float(deref(ob._ask_book.begin()).getPrice())
        
        return (bid, ask)

    cdef double c_get_effective_reference_price(self, OrderBook ob, double top_price, double order_price, 
                                                  double min_price_increment, bint is_buy):
        """
        Calculate effective reference price (second level if top is our order).
        
        For buy orders: returns effective bid (second bid if top bid is our order)
        For sell orders: returns effective ask (second ask if top ask is our order)
        """
        cdef double effective_price = top_price
        
        if min_price_increment <= 0:
            return effective_price
        
        # Check if top price matches our order (within half-tick tolerance)
        if abs(top_price - order_price) < min_price_increment * HALF_TICK_TOLERANCE:
            try:
                if is_buy:
                    # Get second bid level
                    bid_entries = ob.bid_entries()
                    if len(bid_entries) >= 2:
                        effective_price = float(bid_entries[1].price)
                else:
                    # Get second ask level
                    ask_entries = ob.ask_entries()
                    if len(ask_entries) >= 2:
                        effective_price = float(ask_entries[1].price)
            except Exception:
                pass  # Fall back to top price
        
        return effective_price

    cdef tuple c_check_immediate_frontrun(self, double current_top_price, double order_price, bint is_buy):
        """
        Check for immediate frontrun/undercut condition.
        
        Returns: (should_cancel, reason) tuple
        """
        cdef:
            bint should_cancel = False
            str cancel_reason = ""
        
        if is_buy:
            # For buy orders: frontrun if someone bid higher
            if current_top_price > order_price:
                should_cancel = True
                cancel_reason = f"frontrun (immediate) - top bid {current_top_price:.8f} > our {order_price:.8f}"
        else:
            # For sell orders: undercut if someone asked lower
            if current_top_price < order_price:
                should_cancel = True
                cancel_reason = f"undercut (immediate) - top ask {current_top_price:.8f} < our {order_price:.8f}"
        
        return (should_cancel, cancel_reason)

    cdef tuple c_check_large_gap_immediate(self, double order_price, double expected_price, 
                                            double min_price_increment, bint is_buy):
        """
        Check for large price gaps requiring immediate action.
        
        Returns: (should_cancel, reason) tuple
        """
        cdef:
            bint should_cancel = False
            str cancel_reason = ""
            double price_gap
        
        if min_price_increment <= 0:
            return (False, "")
        
        price_gap = abs(order_price - expected_price)
        
        # Immediate cancel if gap >= 2 ticks (large gap, don't wait)
        if price_gap >= min_price_increment * LARGE_GAP_THRESHOLD:
            if is_buy:
                if order_price > expected_price:
                    should_cancel = True
                    cancel_reason = f"large gap (immediate) - order {order_price:.8f} vs optimal {expected_price:.8f}, gap {price_gap:.8f}"
                else:
                    should_cancel = True
                    cancel_reason = f"large gap down (immediate) - can buy cheaper at {expected_price:.8f} vs {order_price:.8f}"
            else:
                if order_price < expected_price:
                    should_cancel = True
                    cancel_reason = f"large gap (immediate) - order {order_price:.8f} vs optimal {expected_price:.8f}, gap {price_gap:.8f}"
                else:
                    should_cancel = True
                    cancel_reason = f"large gap up (immediate) - can sell higher at {expected_price:.8f} vs {order_price:.8f}"
        
        return (should_cancel, cancel_reason)

    cdef double c_calculate_ideal_order_price(self, double effective_ref_price, double spread_pct, 
                                               bint spread_is_min, double min_price_increment, bint is_buy):
        """
        Calculate ideal order price based on spread mode.
        
        Args:
            effective_ref_price: Reference price (effective bid for buy, effective ask for sell)
            spread_pct: Spread percentage (ignored if spread_is_min)
            spread_is_min: Whether using 'min' tick mode
            min_price_increment: Minimum price increment
            is_buy: True for buy orders, False for sell orders
        
        Returns: Ideal order price
        """
        cdef double ideal_price
        
        if spread_is_min:
            # Min tick mode
            if is_buy:
                ideal_price = effective_ref_price + min_price_increment
            else:
                ideal_price = effective_ref_price - min_price_increment
        else:
            # Percentage mode
            if is_buy:
                ideal_price = effective_ref_price * (1.0 + spread_pct)
            else:
                ideal_price = effective_ref_price * (1.0 - spread_pct)
        
        return ideal_price

    cdef tuple c_check_price_divergence(self, double order_price, double ideal_price, bint spread_is_min, 
                                         double min_price_increment, bint got_valid_second_level):
        """
        Check if order price diverged from ideal price.
        
        Returns: (should_cancel, reason) tuple
        """
        cdef:
            bint should_cancel = False
            str cancel_reason = ""
            double price_diff_abs = abs(ideal_price - order_price)
            double price_diff_pct = price_diff_abs / order_price if order_price > 0 else 0.0
            double cancel_threshold_abs
            double cancel_threshold_pct = 0.01  # 1% threshold for percentage mode
        
        if spread_is_min:
            # Min mode: use tick-based threshold
            # Only check divergence if we have a valid second level OR top isn't our order
            cancel_threshold_abs = min_price_increment * TICK_TOLERANCE
            if got_valid_second_level and price_diff_abs > cancel_threshold_abs:
                should_cancel = True
                cancel_reason = f"price diverged {price_diff_abs:.8f} (ideal {ideal_price:.8f} vs order {order_price:.8f})"
        else:
            # Percentage mode: use percentage threshold
            if price_diff_pct > cancel_threshold_pct:
                should_cancel = True
                cancel_reason = f"price diverged {price_diff_pct*100:.2f}% (ideal {ideal_price:.8f} vs order {order_price:.8f})"
        
        return (should_cancel, cancel_reason)

    cdef tuple c_check_immediate_conditions(self, str asset, bint is_buy):
        """
        UNIFIED immediate check for all conditions that require instant response.
        
        This combines:
        1. Better market available (ALL MODES)
        2. Frontrun/undercut detection (min/% modes only)
        3. Large gap detection (min mode only)
        
        All checks use the SAME orderbook snapshot to avoid race conditions
        and prevent duplicate fetches.
        
        Returns: (should_cancel, cancel_reason) tuple
        """
        cdef:
            bint should_cancel = False
            str cancel_reason = ""
            object current_best_market = None
            object order_market_tuple = None
            double order_price = 0.0
            double best_market_price = 0.0
            double order_market_price = 0.0
            double current_bid = 0.0
            double current_ask = 0.0
            double effective_ref_price = 0.0
            double expected_price = 0.0
            double min_price_increment = 0.0
            double gap_amount = 0.0
            tuple order_details
            OrderBook order_ob
            OrderBook best_ob
            str trading_pair
            bint spread_is_min
            double spread_pct
            bint is_maker_mode
        
        try:
            # Get order details
            if is_buy:
                order_details = self._active_buy_order_details.get(asset)
                spread_is_min = self._buy_spread_is_min
                spread_pct = self._buy_spread_pct
            else:
                order_details = self._active_sell_order_details.get(asset)
                spread_is_min = self._sell_spread_is_min
                spread_pct = self._sell_spread_pct
            
            if order_details is None:
                return (False, "")
            
            order_market_tuple, order_price = order_details
            is_maker_mode = spread_is_min or spread_pct > 0.0
            
            # Find current best market
            if is_buy:
                current_best_market = self.c_find_best_buy_market(asset)
            else:
                current_best_market = self.c_find_best_sell_market(asset)
            
            if current_best_market is None:
                return (False, "")
            
            # Fetch current order's market orderbook ONCE
            order_ob = (<ExchangeBase>order_market_tuple.market).c_get_order_book(order_market_tuple.trading_pair)
            
            # Get prices from order's market
            if order_ob._bid_book.size() > 0:
                current_bid = float(deref(order_ob._bid_book.rbegin()).getPrice())
            if order_ob._ask_book.size() > 0:
                current_ask = float(deref(order_ob._ask_book.begin()).getPrice())
            
            # Get min_price_increment (cached)
            trading_pair = order_market_tuple.trading_pair
            if trading_pair in self._min_price_increment_cache:
                min_price_increment = self._min_price_increment_cache[trading_pair]
            else:
                trading_rule = order_market_tuple.market._trading_rules.get(trading_pair)
                min_price_increment = 0.0
                if trading_rule is not None and trading_rule.min_price_increment is not None:
                    min_price_increment = float(trading_rule.min_price_increment)
                self._min_price_increment_cache[trading_pair] = min_price_increment
            
            # ================================================================
            # CHECK 1: Better market available (ALL MODES)
            # ================================================================
            if current_best_market.market.name != order_market_tuple.market.name:
                # Different market is now best - compare prices
                # For 'min' mode: compare effective frontrun prices with hysteresis
                # Only switch if new market is significantly better (0.1%) to prevent flip-flopping
                if spread_is_min:
                    # Get effective price of new best market
                    best_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                    best_min_tick = 0.0
                    best_trading_pair = current_best_market.trading_pair
                    if best_trading_pair in self._min_price_increment_cache:
                        best_min_tick = self._min_price_increment_cache[best_trading_pair]
                    else:
                        best_trading_rule = current_best_market.market._trading_rules.get(best_trading_pair)
                        if best_trading_rule is not None and best_trading_rule.min_price_increment is not None:
                            best_min_tick = float(best_trading_rule.min_price_increment)
                        self._min_price_increment_cache[best_trading_pair] = best_min_tick
                    
                    if is_buy:
                        if best_ob._bid_book.size() > 0 and best_min_tick > 0:
                            best_effective = float(deref(best_ob._bid_book.rbegin()).getPrice()) + best_min_tick
                            current_effective = order_price  # Our current order price IS our effective price
                            # For BUY: lower effective price is better
                            improvement = (current_effective - best_effective) / current_effective if current_effective > 0 else 0
                            if improvement > MIN_MODE_SWITCH_HYSTERESIS:
                                should_cancel = True
                                cancel_reason = f"better market - {current_best_market.market.name} effective={best_effective:.8f} vs current={current_effective:.8f} ({improvement*100:.2f}% better)"
                    else:
                        if best_ob._ask_book.size() > 0 and best_min_tick > 0:
                            best_effective = float(deref(best_ob._ask_book.begin()).getPrice()) - best_min_tick
                            current_effective = order_price  # Our current order price IS our effective price
                            # For SELL: higher effective price is better
                            improvement = (best_effective - current_effective) / current_effective if current_effective > 0 else 0
                            if improvement > MIN_MODE_SWITCH_HYSTERESIS:
                                should_cancel = True
                                cancel_reason = f"better market - {current_best_market.market.name} effective={best_effective:.8f} vs current={current_effective:.8f} ({improvement*100:.2f}% better)"
                else:
                    best_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                    
                    if is_buy:
                        if not is_maker_mode:
                            # Aggressive mode: compare asks (taker)
                            if best_ob._ask_book.size() > 0 and current_ask > 0:
                                best_market_price = float(deref(best_ob._ask_book.begin()).getPrice())
                                if best_market_price < current_ask * (1.0 - BETTER_MARKET_SWITCH_TOLERANCE):
                                    should_cancel = True
                                    cancel_reason = f"better market - {current_best_market.market.name} ask {best_market_price:.8f} < {order_market_tuple.market.name} ask {current_ask:.8f}"
                        else:
                            # Percentage maker mode: compare bids (we place above)
                            if best_ob._bid_book.size() > 0 and current_bid > 0:
                                best_market_price = float(deref(best_ob._bid_book.rbegin()).getPrice())
                                if best_market_price < current_bid * (1.0 - BETTER_MARKET_SWITCH_TOLERANCE):
                                    should_cancel = True
                                    cancel_reason = f"better market - {current_best_market.market.name} bid {best_market_price:.8f} < {order_market_tuple.market.name} bid {current_bid:.8f}"
                    else:  # SELL
                        if not is_maker_mode:
                            # Aggressive mode: compare bids (taker)
                            if best_ob._bid_book.size() > 0 and current_bid > 0:
                                best_market_price = float(deref(best_ob._bid_book.rbegin()).getPrice())
                                if best_market_price > current_bid * (1.0 + BETTER_MARKET_SWITCH_TOLERANCE):
                                    should_cancel = True
                                    cancel_reason = f"better market - {current_best_market.market.name} bid {best_market_price:.8f} > {order_market_tuple.market.name} bid {current_bid:.8f}"
                        else:
                            # Percentage maker mode: compare asks (we place below)
                            if best_ob._ask_book.size() > 0 and current_ask > 0:
                                best_market_price = float(deref(best_ob._ask_book.begin()).getPrice())
                                if best_market_price > current_ask * (1.0 + BETTER_MARKET_SWITCH_TOLERANCE):
                                    should_cancel = True
                                    cancel_reason = f"better market - {current_best_market.market.name} ask {best_market_price:.8f} > {order_market_tuple.market.name} ask {current_ask:.8f}"
            
            # ================================================================
            # CHECK 2 & 3: Frontrun + Large Gap (MAKER MODES ONLY)
            # Skip for aggressive mode (0%) - they don't compete for position
            # ================================================================
            if not should_cancel and is_maker_mode:
                if is_buy:
                    # CHECK 2: Frontrun - someone placed HIGHER bid than our order
                    if current_bid > order_price:
                        should_cancel = True
                        cancel_reason = f"frontrun (top bid {current_bid:.8f} > our {order_price:.8f})"
                    
                    # CHECK 3: Large gap detection (min mode only)
                    if not should_cancel and spread_is_min and min_price_increment > 0:
                        # Our order should be 1 tick above effective bid
                        effective_ref_price = self.c_get_effective_reference_price(
                            order_ob, current_bid, order_price, min_price_increment, True)
                        expected_price = effective_ref_price + min_price_increment
                        gap_amount = abs(order_price - expected_price)
                        if gap_amount > min_price_increment * LARGE_GAP_THRESHOLD:
                            should_cancel = True
                            cancel_reason = f"large gap {gap_amount:.8f} > {min_price_increment * LARGE_GAP_THRESHOLD:.8f}"
                else:  # SELL
                    # CHECK 2: Undercut - someone placed LOWER ask than our order
                    if current_ask < order_price:
                        should_cancel = True
                        cancel_reason = f"undercut (top ask {current_ask:.8f} < our {order_price:.8f})"
                    
                    # CHECK 3: Large gap detection (min mode only)
                    if not should_cancel and spread_is_min and min_price_increment > 0:
                        # Our order should be 1 tick below effective ask
                        effective_ref_price = self.c_get_effective_reference_price(
                            order_ob, current_ask, order_price, min_price_increment, False)
                        expected_price = effective_ref_price - min_price_increment
                        gap_amount = abs(order_price - expected_price)
                        if gap_amount > min_price_increment * LARGE_GAP_THRESHOLD:
                            should_cancel = True
                            cancel_reason = f"large gap {gap_amount:.8f} > {min_price_increment * LARGE_GAP_THRESHOLD:.8f}"
        
        except Exception as e:
            self.strategy.logger().debug(f"Position balancer: Immediate check failed: {e}")
        
        return (should_cancel, cancel_reason)



    cdef void c_cancel_stale_orders(self, str asset):
        """
        Cancel stale buy/sell limit orders for refresh.

        REAL-WORLD GOAL: Maintain competitive position in order book while getting best price.

        Smart cancellation triggers:
        1. Mode disabled → orphaned order cleanup
        2. Better market available → switch to higher liquidity/better price
        3. Got frontrun → someone placed more aggressive order, we're no longer at front
        4. Market moved in our favor → opportunity to get better price (gap detection)
           - BUY: market moved DOWN → can buy cheaper
           - SELL: market moved UP → can sell higher
        5. Significant spread gap → our order is too far from competitive price
        6. Stuck cancel detection → force cleanup if cancel pending too long (safety net)
        """
        cdef:
            double current_time = self.strategy._current_timestamp
            object current_best_market
            OrderBook current_ob
            object trading_rule
            tuple order_details
            str order_market_name
            double order_price
            double current_bid
            double current_ask
            double current_calculated_price
            double min_price_increment
            double price_diff_abs
            double price_diff_pct
            double cancel_threshold_abs
            double cancel_threshold_pct
            double time_since_order_placed
            bint process_buy_order
            bint process_sell_order
            bint should_cancel
            str cancel_reason
            str order_id

        # ========================================================================
        # CHECK BUY ORDERS
        # ========================================================================
        if asset in self._active_buy_orders:
            order_id = self._active_buy_orders.get(asset)
            if order_id:
                # Check if order is already in cancellation state
                should_process = True
                if order_id in self.strategy._timeout_cancelled_orders:
                    # Check for stuck cancel (force cleanup if needed)
                    self.c_check_stuck_cancel(order_id, asset, True, current_time)
                    should_process = False

                if should_process:
                    # Normal processing - order not in cancellation state
                    # Only process if we haven't already sent a cancel request
                    last_time = self._last_buy_order_time.get(asset, 0.0)
                    should_cancel = False
                    cancel_reason = ""

                    # Cancel if mode disabled (orphaned order)
                    if not self._buy_enabled:
                        should_cancel = True
                        cancel_reason = "mode disabled"

                    # UNIFIED IMMEDIATE CHECK: Better market + Frontrun + Gap detection
                    # All checks use same orderbook snapshot to avoid race conditions
                    if not should_cancel:
                        should_cancel, cancel_reason = self.c_check_immediate_conditions(asset, True)

                    # Check if refresh interval passed AND conditions changed
                    # Determine effective refresh interval (shorter for aggressive partial fills)
                    effective_interval = self._limit_refresh_interval
                    
                    # Aggressive mode (0% spread) and partial fill -> use aggressive interval
                    if (not self._buy_spread_is_min and self._buy_spread_pct == 0.0):
                        if order_id in self._pending_buy_orders:
                            # Unpack correctly: (asset_key, total, filled)
                            try:
                                _, _, filled_amt = self._pending_buy_orders[order_id]
                                if filled_amt > EPSILON:
                                    effective_interval = self._aggressive_refresh_interval
                            except Exception:
                                pass

                    if not should_cancel and current_time - last_time > effective_interval:
                        # Smart cancellation: only cancel if market/price changed
                        current_best_market = None
                        try:
                            # Find current best market
                            current_best_market = self.c_find_best_buy_market(asset)
                        except Exception as e:
                            self.strategy.logger().warning(f"Position balancer: Error finding best buy market: {e}")
                        if current_best_market is not None:
                            # Get order details (market_tuple, price)
                            order_details = self._active_buy_order_details.get(asset)
                            if order_details is not None:
                                order_market_tuple, order_price = order_details

                                # CONDITION 1: Check if different market became better
                                if current_best_market.market.name != order_market_tuple.market.name:
                                    # For min mode: apply hysteresis - only switch if significantly better
                                    if self._buy_spread_is_min:
                                        # Get effective price of new best market
                                        best_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                                        best_min_tick = 0.0
                                        best_tp = current_best_market.trading_pair
                                        if best_tp in self._min_price_increment_cache:
                                            best_min_tick = self._min_price_increment_cache[best_tp]
                                        else:
                                            best_tr = current_best_market.market._trading_rules.get(best_tp)
                                            if best_tr is not None and best_tr.min_price_increment is not None:
                                                best_min_tick = float(best_tr.min_price_increment)
                                            self._min_price_increment_cache[best_tp] = best_min_tick
                                        
                                        if best_ob._bid_book.size() > 0 and best_min_tick > 0:
                                            best_effective = float(deref(best_ob._bid_book.rbegin()).getPrice()) + best_min_tick
                                            current_effective = order_price
                                            # For BUY: lower effective price is better
                                            improvement = (current_effective - best_effective) / current_effective if current_effective > 0 else 0
                                            if improvement > MIN_MODE_SWITCH_HYSTERESIS:
                                                should_cancel = True
                                                cancel_reason = f"better market ({current_best_market.market.name} vs {order_market_tuple.market.name}, {improvement*100:.2f}% better)"
                                        # If improvement <= threshold, don't cancel - stay on current market
                                    else:
                                        should_cancel = True
                                        cancel_reason = f"better market ({current_best_market.market.name} vs {order_market_tuple.market.name})"
                                else:
                                    # Same market - evaluate if conditions changed
                                    try:
                                        current_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                                        
                                        if current_ob._bid_book.size() > 0:
                                            current_bid = float(deref(current_ob._bid_book.rbegin()).getPrice())
                                        else:
                                            current_bid = 0.0
                                            
                                        if current_ob._ask_book.size() > 0:
                                            current_ask = float(deref(current_ob._ask_book.begin()).getPrice())
                                        else:
                                            current_ask = 0.0

                                        # OPTIMIZATION: Fetch trading_rule ONCE and reuse (was fetched 3x before)
                                        # Use cache instead of dict lookup
                                        trading_pair = current_best_market.trading_pair
                                        if trading_pair in self._min_price_increment_cache:
                                            min_price_increment = self._min_price_increment_cache[trading_pair]
                                        else:
                                            trading_rule = current_best_market.market._trading_rules.get(trading_pair)
                                            min_price_increment = 0.0
                                            if trading_rule is not None and trading_rule.min_price_increment is not None:
                                                min_price_increment = float(trading_rule.min_price_increment)
                                            self._min_price_increment_cache[trading_pair] = min_price_increment

                                        # IMPORTANT: For 'min' mode gap detection, check if top bid is OUR order
                                        # Calculate effective_bid (second level if top is our order) using helper
                                        effective_bid = self.c_get_effective_reference_price(
                                            current_ob, current_bid, order_price, min_price_increment, True)
                                        
                                        # Track if we got a valid second level (not our own order)
                                        got_valid_second_level = (effective_bid != current_bid)

                                        # Calculate ideal order price using helper
                                        ideal_order_price = self.c_calculate_ideal_order_price(
                                            effective_bid, self._buy_spread_pct, self._buy_spread_is_min, 
                                            min_price_increment, True)

                                        # CONDITION 2: Got outbid - someone placed HIGHER bid than our order
                                        if self._buy_spread_is_min or self._buy_spread_pct > 0.0:
                                            if current_bid > order_price:
                                                should_cancel = True
                                                cancel_reason = f"outbid (top bid {current_bid:.8f} > our {order_price:.8f})"

                                        # CONDITION 2b: For 'min' mode, explicit 1-tick frontrun detection
                                        if not should_cancel and self._buy_spread_is_min and min_price_increment > 0:
                                            if got_valid_second_level:
                                                expected_price = effective_bid + min_price_increment
                                                price_misalignment = order_price - expected_price
                                                if abs(price_misalignment) >= min_price_increment * 0.95:
                                                    should_cancel = True
                                                    if price_misalignment > 0:
                                                        cancel_reason = f"1-tick frontrun detected (effective bid {effective_bid:.8f}, expected {expected_price:.8f}, actual {order_price:.8f})"
                                                    else:
                                                        cancel_reason = f"1-tick gap detected (effective bid {effective_bid:.8f}, expected {expected_price:.8f}, actual {order_price:.8f})"

                                        # CONDITION 3: Market moved DOWN - opportunity to buy cheaper (gap detection)
                                        if not should_cancel and ideal_order_price < order_price:
                                            gap_down = order_price - ideal_order_price
                                            if self._buy_spread_is_min:
                                                # Min mode: check absolute threshold, but only with valid second level
                                                cancel_threshold_abs = min_price_increment * TICK_TOLERANCE
                                                if got_valid_second_level and gap_down > cancel_threshold_abs:
                                                    should_cancel = True
                                                    cancel_reason = f"gap down {gap_down:.8f} > threshold {cancel_threshold_abs:.8f} (can buy cheaper)"
                                            else:
                                                # Percentage mode: check percentage threshold
                                                cancel_threshold_pct = max(0.001, self._buy_spread_pct * 0.5)
                                                gap_pct = gap_down / order_price if order_price > 0 else 0.0
                                                if gap_pct > cancel_threshold_pct:
                                                    should_cancel = True
                                                    cancel_reason = f"gap down {gap_pct*100:.2f}% (can buy cheaper at {ideal_order_price:.8f} vs {order_price:.8f})"

                                        # CONDITION 4: Market moved UP significantly - got closer to being filled
                                        if not should_cancel and self._buy_spread_pct == 0.0:
                                            if current_bid >= order_price * AGGRESSIVE_MODE_TOLERANCE:
                                                should_cancel = True
                                                cancel_reason = "market moved to our price (aggressive mode)"

                                        # CONDITION 5: Significant price divergence using helper
                                        if not should_cancel:
                                            should_cancel, cancel_reason = self.c_check_price_divergence(
                                                order_price, ideal_order_price, self._buy_spread_is_min,
                                                min_price_increment, got_valid_second_level)
                                    except Exception as e:
                                        self.strategy.logger().warning(f"Position balancer: Error checking buy order conditions: {e}")
                                        pass

                    if should_cancel:
                        self._cancel_buy_order(asset, order_id, cancel_reason)

        # ========================================================================
        # CHECK SELL ORDERS
        # ========================================================================
        if asset in self._active_sell_orders:
            order_id = self._active_sell_orders.get(asset)
            if order_id:
                # Check if order is already in cancellation state
                should_process = True
                if order_id in self.strategy._timeout_cancelled_orders:
                    # Check for stuck cancel (force cleanup if needed)
                    self.c_check_stuck_cancel(order_id, asset, False, current_time)
                    should_process = False

                if should_process:
                    # Normal processing - order not in cancellation state
                    # Only process if we haven't already sent a cancel request
                    last_time = self._last_sell_order_time.get(asset, 0.0)
                    should_cancel = False
                    cancel_reason = ""

                    # Cancel if mode disabled (orphaned order)
                    if not self._sell_enabled:
                        should_cancel = True
                        cancel_reason = "mode disabled"

                    # UNIFIED IMMEDIATE CHECK: Better market + Undercut + Gap detection
                    # All checks use same orderbook snapshot to avoid race conditions
                    if not should_cancel:
                        should_cancel, cancel_reason = self.c_check_immediate_conditions(asset, False)

                    # Check if refresh interval passed AND conditions changed
                    # Determine effective refresh interval (shorter for aggressive partial fills)
                    effective_interval = self._limit_refresh_interval
                    
                    # Aggressive mode (0% spread) and partial fill -> use aggressive interval
                    if (not self._sell_spread_is_min and self._sell_spread_pct == 0.0):
                        if order_id in self._pending_sell_orders:
                            # Unpack correctly: (asset_key, total, filled)
                            try:
                                _, _, filled_amt = self._pending_sell_orders[order_id]
                                if filled_amt > EPSILON:
                                    effective_interval = self._aggressive_refresh_interval
                            except Exception:
                                pass

                    if not should_cancel and current_time - last_time > effective_interval:
                        # Smart cancellation: only cancel if market/price changed
                        try:
                            # Find current best market
                            current_best_market = self.c_find_best_sell_market(asset)
                            if current_best_market is not None:
                                # Get order details (market_tuple, price)
                                order_details = self._active_sell_order_details.get(asset)
                                if order_details is not None:
                                    order_market_tuple, order_price = order_details

                                    # CONDITION 1: Check if different market became better
                                    if current_best_market.market.name != order_market_tuple.market.name:
                                        # For min mode: apply hysteresis - only switch if significantly better
                                        if self._sell_spread_is_min:
                                            # Get effective price of new best market
                                            best_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                                            best_min_tick = 0.0
                                            best_tp = current_best_market.trading_pair
                                            if best_tp in self._min_price_increment_cache:
                                                best_min_tick = self._min_price_increment_cache[best_tp]
                                            else:
                                                best_tr = current_best_market.market._trading_rules.get(best_tp)
                                                if best_tr is not None and best_tr.min_price_increment is not None:
                                                    best_min_tick = float(best_tr.min_price_increment)
                                                self._min_price_increment_cache[best_tp] = best_min_tick
                                            
                                            if best_ob._ask_book.size() > 0 and best_min_tick > 0:
                                                best_effective = float(deref(best_ob._ask_book.begin()).getPrice()) - best_min_tick
                                                current_effective = order_price
                                                # For SELL: higher effective price is better
                                                improvement = (best_effective - current_effective) / current_effective if current_effective > 0 else 0
                                                if improvement > MIN_MODE_SWITCH_HYSTERESIS:
                                                    should_cancel = True
                                                    cancel_reason = f"better market ({current_best_market.market.name} vs {order_market_tuple.market.name}, {improvement*100:.2f}% better)"
                                            # If improvement <= threshold, don't cancel - stay on current market
                                        else:
                                            should_cancel = True
                                            cancel_reason = f"better market ({current_best_market.market.name} vs {order_market_tuple.market.name})"
                                    else:
                                        # Same market - evaluate if conditions changed
                                        try:
                                            current_ob = (<ExchangeBase>current_best_market.market).c_get_order_book(current_best_market.trading_pair)
                                            
                                            # Get orderbook prices using helper
                                            current_bid, current_ask = self.c_get_orderbook_prices(current_ob)


                                            # Get min_price_increment (cached)
                                            trading_pair = current_best_market.trading_pair
                                            if trading_pair in self._min_price_increment_cache:
                                                min_price_increment = self._min_price_increment_cache[trading_pair]
                                            else:
                                                trading_rule = current_best_market.market._trading_rules.get(trading_pair)
                                                min_price_increment = 0.0
                                                if trading_rule is not None and trading_rule.min_price_increment is not None:
                                                    min_price_increment = float(trading_rule.min_price_increment)
                                                self._min_price_increment_cache[trading_pair] = min_price_increment

                                            # Calculate effective_ask (second level if top is our order) using helper
                                            effective_ask = self.c_get_effective_reference_price(
                                                current_ob, current_ask, order_price, min_price_increment, False)
                                            
                                            # Track if we got a valid second level (not our own order)
                                            got_valid_second_level = (effective_ask != current_ask)

                                            # Calculate ideal order price using helper
                                            ideal_order_price = self.c_calculate_ideal_order_price(
                                                effective_ask, self._sell_spread_pct, self._sell_spread_is_min, 
                                                min_price_increment, False)

                                            # CONDITION 2: Got undercut - someone placed LOWER ask than our order
                                            if self._sell_spread_is_min or self._sell_spread_pct > 0.0:
                                                if current_ask < order_price:
                                                    should_cancel = True
                                                    cancel_reason = f"undercut (top ask {current_ask:.8f} < our {order_price:.8f})"

                                            # CONDITION 2b: For 'min' mode, explicit 1-tick undercut detection
                                            if not should_cancel and self._sell_spread_is_min and min_price_increment > 0:
                                                if got_valid_second_level:
                                                    expected_price = effective_ask - min_price_increment
                                                    price_misalignment = expected_price - order_price
                                                    if abs(price_misalignment) >= min_price_increment * 0.95:
                                                        should_cancel = True
                                                        if price_misalignment > 0:
                                                            cancel_reason = f"1-tick undercut detected (effective ask {effective_ask:.8f}, expected {expected_price:.8f}, actual {order_price:.8f})"
                                                        else:
                                                            cancel_reason = f"1-tick gap detected (effective ask {effective_ask:.8f}, expected {expected_price:.8f}, actual {order_price:.8f})"

                                            # CONDITION 3: Market moved UP - opportunity to sell higher (gap detection)
                                            if not should_cancel and ideal_order_price > order_price:
                                                gap_up = ideal_order_price - order_price
                                                if self._sell_spread_is_min:
                                                    # Min mode: check absolute threshold, but only with valid second level
                                                    cancel_threshold_abs = min_price_increment * TICK_TOLERANCE
                                                    if got_valid_second_level and gap_up > cancel_threshold_abs:
                                                        should_cancel = True
                                                        cancel_reason = f"gap up {gap_up:.8f} > threshold {cancel_threshold_abs:.8f} (can sell higher)"
                                                else:
                                                    # Percentage mode: check percentage threshold
                                                    cancel_threshold_pct = max(0.001, self._sell_spread_pct * 0.5)
                                                    gap_pct = gap_up / order_price if order_price > 0 else 0.0
                                                    if gap_pct > cancel_threshold_pct:
                                                        should_cancel = True
                                                        cancel_reason = f"gap up {gap_pct*100:.2f}% (can sell higher at {ideal_order_price:.8f} vs {order_price:.8f})"

                                            # CONDITION 4: Market moved DOWN significantly - got closer to being filled
                                            if not should_cancel and self._sell_spread_pct == 0.0:
                                                if current_ask <= order_price * (2.0 - AGGRESSIVE_MODE_TOLERANCE):
                                                    should_cancel = True
                                                    cancel_reason = "market moved to our price (aggressive mode)"

                                            # CONDITION 5: Significant price divergence using helper
                                            if not should_cancel:
                                                should_cancel, cancel_reason = self.c_check_price_divergence(
                                                    order_price, ideal_order_price, self._sell_spread_is_min,
                                                    min_price_increment, got_valid_second_level)

                                        except Exception as e:
                                            self.strategy.logger().warning(f"Position balancer: Error checking sell order conditions: {e}")
                                            pass
                        except Exception as e:
                            self.strategy.logger().warning(f"Position balancer: Error finding best sell market: {e}")
                            pass

                    if should_cancel:
                        self._cancel_sell_order(asset, order_id, cancel_reason)

    cdef void _cancel_buy_order(self, str asset, str order_id, str reason):
        """
        Internal method to cancel a buy order with proper cleanup tracking.
        Ensures atomic cancellation with correct state management.

        Uses stored market_tuple for robust direct cancellation by order_id.
        """
        cdef:
            tuple order_details
            object market_tuple

        try:
            # Get stored market_tuple from order details for direct cancellation
            order_details = self._active_buy_order_details.get(asset)
            if order_details is None:
                self.strategy.logger().warning(
                    f"Position balancer: Cannot cancel buy order {order_id} - no order details found for {asset}")
                return

            market_tuple, _ = order_details  # Unpack (market_tuple, price)

            # Mark as timeout-cancelled to prevent cooldown enforcement
            # This allows position balancer to be more aggressive with refreshes
            self.strategy._timeout_cancelled_orders.add(order_id)

            # NOTE: Don't remove from _position_balancer_orders here!
            # Main strategy skips timeout checks for orders in this set.
            # Only remove when order actually completes/cancels (in handle_order_completion)
            # This prevents race conditions where we lose tracking if cancel is delayed.

            # Cancel the order using stored market_tuple (robust direct cancellation)
            self.strategy.c_cancel_order(market_tuple, order_id)

            # Track cancel request time for stuck cancel detection
            self._buy_cancel_request_time[asset] = self.strategy._current_timestamp

            self.strategy.logger().info(
                f"Position balancer: Cancelled buy order {order_id} for {asset} on {market_tuple.market.name} ({reason})")

            # NOTE: Don't remove from _active_buy_orders here!
            # Let handle_order_cancellation() clean it up when cancel event arrives.
            # This prevents tracking loss if cancel fails or is delayed.
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to cancel buy order {order_id}: {e}")
            # Clean up timeout marker if cancel failed
            self.strategy._timeout_cancelled_orders.discard(order_id)

    cdef void _cancel_sell_order(self, str asset, str order_id, str reason):
        """
        Internal method to cancel a sell order with proper cleanup tracking.
        Ensures atomic cancellation with correct state management.

        Uses stored market_tuple for robust direct cancellation by order_id.
        """
        cdef:
            tuple order_details
            object market_tuple

        try:
            # Get stored market_tuple from order details for direct cancellation
            order_details = self._active_sell_order_details.get(asset)
            if order_details is None:
                self.strategy.logger().warning(
                    f"Position balancer: Cannot cancel sell order {order_id} - no order details found for {asset}")
                return

            market_tuple, _ = order_details  # Unpack (market_tuple, price)

            # Mark as timeout-cancelled to prevent cooldown enforcement
            # This allows position balancer to be more aggressive with refreshes
            self.strategy._timeout_cancelled_orders.add(order_id)

            # NOTE: Don't remove from _position_balancer_orders here!
            # Main strategy skips timeout checks for orders in this set.
            # Only remove when order actually completes/cancels (in handle_order_completion)
            # This prevents race conditions where we lose tracking if cancel is delayed.

            # Cancel the order using stored market_tuple (robust direct cancellation)
            self.strategy.c_cancel_order(market_tuple, order_id)

            # Track cancel request time for stuck cancel detection
            self._sell_cancel_request_time[asset] = self.strategy._current_timestamp

            self.strategy.logger().info(
                f"Position balancer: Cancelled sell order {order_id} for {asset} on {market_tuple.market.name} ({reason})")

            # NOTE: Don't remove from _active_sell_orders here!
            # Let handle_order_cancellation() clean it up when cancel event arrives.
            # This prevents tracking loss if cancel fails or is delayed.
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to cancel sell order {order_id}: {e}")
            # Clean up timeout marker if cancel failed
            self.strategy._timeout_cancelled_orders.discard(order_id)

    cdef bint c_handle_position_balancing(self, object buy_market_tuple, object sell_market_tuple):
        """
        Main entry point for position balancing.
        Decides whether to buy or sell based on current position.
        Returns True if an order was placed.

        For asset aliases (e.g., NODE/NODEOPS with 1:1 conversion):
        - Aggregates balances across all aliases
        - Manages orders for all alias names
        - Prevents timeout issues from unmanaged aliases
        """
        # CRITICAL SAFEGUARD: Prevent double execution
        if self.strategy._last_global_trade_timestamp == self.strategy._current_timestamp:
            return False

        if not self._buy_enabled and not self._sell_enabled:
            return False

        cdef:
            str asset_key = buy_market_tuple.base_asset
            str canonical_asset = self._get_canonical_asset(asset_key)
            list asset_aliases = self._get_all_asset_aliases(asset_key)
            double last_bid = self.strategy.c_get_reference_bid_for_asset(canonical_asset)
            double base_bal = self.c_get_adjusted_base_balance(canonical_asset)
            str alias
            double base_bal_actual
            pair[double, double] val_result
            pair[double, double] val_result_actual
            double current_value
            double shortfall_or_excess
            bint placed = False
            bint has_active_order
            object selected_buy_market = None
            object selected_sell_market = None

        # Cancel stale orders for refresh - handle ALL aliases
        # This ensures orders for both NODE and NODEOPS (or any aliases) are refreshed
        for alias in asset_aliases:
            self.c_cancel_stale_orders(alias)

        # Check if we need to buy
        if self._buy_enabled and not self._buy_completed:
            val_result = self.c_compute_value_and_buy_shortfall(base_bal, last_bid)
            current_value = val_result.first
            shortfall_or_excess = val_result.second

            if shortfall_or_excess > 0:
                # Check if already have pending buy order for ANY alias
                # Only place new order if no aliases have active orders
                # EXCEPTION: If the active order has a pending cancel, we can place a new one
                has_active_order = False
                for alias in asset_aliases:
                    if alias in self._active_buy_orders:
                        existing_order_id = self._active_buy_orders[alias]
                        # Check if this order has a pending cancel
                        if existing_order_id in self.strategy._timeout_cancelled_orders:
                            # Order is being cancelled - force clear tracking to allow replacement
                            self.strategy.logger().info(
                                f"Position balancer: Active buy order {existing_order_id} has pending cancel - clearing for replacement")
                            self.handle_order_cancellation(existing_order_id)
                        else:
                            has_active_order = True
                            break

                if not has_active_order:
                    # Check 2-second cooldown after order completion
                    time_since_completion = self.strategy._current_timestamp - self._last_buy_completion_time.get(canonical_asset, 0.0)
                    if time_since_completion < DEFAULT_COMPLETION_COOLDOWN:
                        return False

                    # For completion check, use ACTUAL balance (not adjusted)
                    base_bal_actual = self.c_get_actual_base_balance(canonical_asset)
                    val_result_actual = self.c_compute_value_and_buy_shortfall(base_bal_actual, last_bid)
                    if not self.c_try_mark_buy_complete(canonical_asset, val_result_actual.first, val_result_actual.second):
                        # Find the best market to buy on (lowest ask)
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
                # Check if already have pending sell order for ANY alias
                # Only place new order if no aliases have active orders
                # EXCEPTION: If the active order has a pending cancel, we can place a new one
                has_active_order = False
                for alias in asset_aliases:
                    if alias in self._active_sell_orders:
                        existing_order_id = self._active_sell_orders[alias]
                        # Check if this order has a pending cancel
                        if existing_order_id in self.strategy._timeout_cancelled_orders:
                            # Order is being cancelled - force clear tracking to allow replacement
                            self.strategy.logger().info(
                                f"Position balancer: Active sell order {existing_order_id} has pending cancel - clearing for replacement")
                            self.handle_order_cancellation(existing_order_id)
                        else:
                            has_active_order = True
                            break

                if not has_active_order:
                    # Check 2-second cooldown after order completion
                    time_since_completion = self.strategy._current_timestamp - self._last_sell_completion_time.get(canonical_asset, 0.0)
                    if time_since_completion < DEFAULT_COMPLETION_COOLDOWN:
                        return False

                    # For completion check, use ACTUAL balance (not adjusted)
                    base_bal_actual = self.c_get_actual_base_balance(canonical_asset)
                    val_result_actual = self.c_compute_value_and_sell_excess(base_bal_actual, last_bid)
                    if not self.c_try_mark_sell_complete(canonical_asset, val_result_actual.first, val_result_actual.second):
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
            double base_bal_adjusted = self.c_get_adjusted_base_balance(asset_key)
            double base_bal_actual = self.c_get_actual_base_balance(asset_key)
            double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
            # Use adjusted balance for shortfall calculation (accounts for pending orders)
            pair[double, double] val_short_adjusted = self.c_compute_value_and_buy_shortfall(base_bal_adjusted, last_bid)
            double shortfall_adjusted = val_short_adjusted.second
            # Use actual balance for completion check (don't count unfilled orders)
            pair[double, double] val_short_actual = self.c_compute_value_and_buy_shortfall(base_bal_actual, last_bid)
            double current_value_quote = val_short_actual.first
            double shortfall = val_short_actual.second

        # Check if still needed - use ACTUAL balance to avoid premature completion
        if self.c_try_mark_buy_complete(asset_key, current_value_quote, shortfall):
            return False

        if quote_bal <= 0:
            return False

        # Get order book prices from market (we selected market with lowest ask)
        cdef:
            OrderBook ob
            double top_ask
            double top_bid
            double buy_price
            double max_affordable_base
            double amount_to_buy
            object quantized_amount
            object order_type = OrderType.LIMIT
            double volume_usd
            str buy_order_id
            string buy_id_str
            object trading_rule
            object min_price_increment
            # Variables for post-order completion check
            pair[double, double] val_short

        try:
            # Use C-level get_order_book for compatibility with all exchanges
            ob = market.c_get_order_book(buy_market_tuple.trading_pair)
            top_ask = float(deref(ob._ask_book.begin()).getPrice())
            top_bid = float(deref(ob._bid_book.rbegin()).getPrice())
        except Exception:
            return False

        if top_ask <= 0:
            return False

        # Calculate limit price based on spread mode
        if self._buy_spread_is_min:
            # 'min' mode: Place one tick above top bid (maker order)
            if top_bid <= 0:
                return False
            try:
                trading_rule = market._trading_rules.get(buy_market_tuple.trading_pair)
                if trading_rule is not None and trading_rule.min_price_increment is not None:
                    min_price_increment = float(trading_rule.min_price_increment)

                    # CRITICAL: Check if top bid is our own order being replaced
                    # If we're replacing an existing order, the old order might still be in the orderbook
                    # Use the second bid level to avoid placing above our own stale order
                    reference_bid = top_bid
                    if asset_key in self._active_buy_orders:
                        # We have an active order - check if top bid matches it
                        existing_order_details = self._active_buy_order_details.get(asset_key)
                        if existing_order_details is not None:
                            _, existing_price = existing_order_details
                            # If top bid matches our existing order (within tolerance), use second bid
                            if abs(top_bid - existing_price) < min_price_increment * 0.5:
                                try:
                                    bid_entries = ob.bid_entries()
                                    if len(bid_entries) >= 2:
                                        reference_bid = float(bid_entries[1].price)
                                        self.strategy.logger().debug(
                                            f"Position balancer: Top bid {top_bid:.8f} matches our existing order, "
                                            f"using second bid {reference_bid:.8f} for new order price")
                                except Exception as e:
                                    self.strategy.logger().debug(f"Could not get second bid level: {e}")

                    buy_price = reference_bid + min_price_increment
                    # Check if maker price would cross the spread (become taker)
                    if buy_price >= top_ask:
                        # IMPORTANT: Spread is too tight for maker order
                        # Log clear warning so user understands why using taker price
                        self.strategy.logger().warning(
                            f"Position balancer: Spread too tight for 'min' tick mode on {buy_market_tuple.trading_pair}. "
                            f"Calculated maker price {buy_price:.8f} >= ask {top_ask:.8f}. "
                            f"Using ask price (will pay taker fees instead of maker rebate).")
                        buy_price = top_ask  # Use taker price with clear warning
                else:
                    # No min_price_increment available, fall back to taker
                    self.strategy.logger().warning(
                        f"Position balancer: No min_price_increment for {buy_market_tuple.trading_pair}, using taker price")
                    buy_price = top_ask
            except Exception as e:
                self.strategy.logger().warning(
                    f"Position balancer: Error calculating 'min' mode price for {buy_market_tuple.trading_pair}: {e}, using taker")
                buy_price = top_ask  # Fall back to taker on error
        elif self._buy_spread_pct == 0.0:
            # Aggressive mode (0%): Buy at ask (taker) - this is intentional
            buy_price = top_ask
        else:
            # Percentage mode (>0%): Place above top bid (maker order)
            # buy_price = top_bid + (top_bid * spread_pct)
            if top_bid <= 0:
                return False
            buy_price = top_bid * (1.0 + self._buy_spread_pct)
            # Check if maker price would cross the spread (become taker)
            if buy_price >= top_ask:
                # IMPORTANT: Spread is too tight for this percentage
                # Log clear warning so user understands why using taker price
                self.strategy.logger().warning(
                    f"Position balancer: Spread too tight for {self._buy_spread_pct*100:.2f}% spread on {buy_market_tuple.trading_pair}. "
                    f"Calculated maker price {buy_price:.8f} >= ask {top_ask:.8f}. "
                    f"Using ask price (will pay taker fees instead of maker rebate).")
                buy_price = top_ask  # Use taker price with clear warning

        # Calculate amount based on shortfall, available quote, and order size limit
        # Use ADJUSTED shortfall to account for pending orders and avoid over-ordering
        max_affordable_base = quote_bal / buy_price if buy_price > 0 else 0.0
        max_order_base = self._order_size_usd / buy_price if buy_price > 0 else 0.0
        amount_to_buy = min(
            shortfall_adjusted / last_bid if last_bid > 0 else 0.0,
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

        # Quantize price to match exchange precision (prevents log/actual price mismatch)
        try:
            quantized_price = market.quantize_order_price(buy_market_tuple.trading_pair, Decimal(str(buy_price)))
            buy_price = float(quantized_price)
        except Exception:
            pass  # Fall back to original price if quantization fails

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

        # Track in balancer (asset_key, total_amount, filled_amount)
        try:
            # Safety check: warn if we're replacing an existing active order (shouldn't happen)
            if asset_key in self._active_buy_orders:
                old_order_id = self._active_buy_orders[asset_key]
                if old_order_id != buy_order_id:
                    self.strategy.logger().warning(
                        f"Position balancer: Replacing active buy order {old_order_id} with {buy_order_id} for {asset_key} "
                        f"(old order may not have been cleaned up properly)")

            self._pending_buy_orders[buy_order_id] = (asset_key, float(quantized_amount), 0.0)
            self._pending_buy_by_asset[asset_key] = (
                float(self._pending_buy_by_asset.get(asset_key, 0.0)) + float(quantized_amount))
            self._active_buy_orders[asset_key] = buy_order_id
            self._last_buy_order_time[asset_key] = self.strategy._current_timestamp
            # Store order details for smart cancellation (market_tuple, price)
            self._active_buy_order_details[asset_key] = (buy_market_tuple, buy_price)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track buy limit order {buy_order_id}: {e}")

        # Log order placement
        if self._buy_spread_is_min:
            self.strategy.logger().info(
                f"Placed buy limit order {buy_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {buy_price:.8f} (spread: min tick)")
        else:
            self.strategy.logger().info(
                f"Placed buy limit order {buy_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {buy_price:.8f} (spread: {self._buy_spread_pct * 100:.2f}%)")

        # Check if target reached - use ACTUAL balance (not adjusted)
        # to avoid marking as complete when order hasn't filled yet
        base_bal_actual = self.c_get_actual_base_balance(asset_key)
        val_short = self.c_compute_value_and_buy_shortfall(base_bal_actual, last_bid)
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
            double base_bal_adjusted = self.c_get_adjusted_base_balance(asset_key)
            double base_bal_actual = self.c_get_actual_base_balance(asset_key)
            double last_bid = self.strategy.c_get_reference_bid_for_asset(asset_key)
            # Use adjusted balance for excess calculation (accounts for pending orders)
            pair[double, double] val_excess_adjusted = self.c_compute_value_and_sell_excess(base_bal_adjusted, last_bid)
            double excess_adjusted = val_excess_adjusted.second
            # Use actual balance for completion check (don't count unfilled orders)
            pair[double, double] val_excess_actual = self.c_compute_value_and_sell_excess(base_bal_actual, last_bid)
            double current_value_quote = val_excess_actual.first
            double excess = val_excess_actual.second

        # Check if still needed - use ACTUAL balance to avoid premature completion
        if self.c_try_mark_sell_complete(asset_key, current_value_quote, excess):
            return False

        if base_bal_raw <= 0:
            return False

        # Get order book prices from market (we selected market with highest bid)
        cdef:
            OrderBook ob
            double top_bid
            double top_ask
            double sell_price
            double amount_to_sell
            object quantized_amount
            object order_type = OrderType.LIMIT
            double volume_usd
            str sell_order_id
            string sell_id_str
            object trading_rule
            object min_price_increment
            # Variables for post-order completion check
            pair[double, double] val_excess

        try:
            # Use C-level get_order_book for compatibility with all exchanges
            ob = market.c_get_order_book(sell_market_tuple.trading_pair)
            top_bid = float(deref(ob._bid_book.rbegin()).getPrice())
            top_ask = float(deref(ob._ask_book.begin()).getPrice())
        except Exception:
            return False

        if top_bid <= 0:
            return False

        # Calculate limit price based on spread mode
        if self._sell_spread_is_min:
            # 'min' mode: Place one tick below top ask (maker order)
            if top_ask <= 0:
                return False
            try:
                trading_rule = market._trading_rules.get(sell_market_tuple.trading_pair)
                if trading_rule is not None and trading_rule.min_price_increment is not None:
                    min_price_increment = float(trading_rule.min_price_increment)

                    # CRITICAL: Check if top ask is our own order being replaced
                    # If we're replacing an existing order, the old order might still be in the orderbook
                    # Use the second ask level to avoid placing below our own stale order
                    reference_ask = top_ask
                    if asset_key in self._active_sell_orders:
                        # We have an active order - check if top ask matches it
                        existing_order_details = self._active_sell_order_details.get(asset_key)
                        if existing_order_details is not None:
                            _, existing_price = existing_order_details
                            # If top ask matches our existing order (within tolerance), use second ask
                            if abs(top_ask - existing_price) < min_price_increment * 0.5:
                                try:
                                    ask_entries = ob.ask_entries()
                                    if len(ask_entries) >= 2:
                                        reference_ask = float(ask_entries[1].price)
                                        self.strategy.logger().debug(
                                            f"Position balancer: Top ask {top_ask:.8f} matches our existing order, "
                                            f"using second ask {reference_ask:.8f} for new order price")
                                except Exception as e:
                                    self.strategy.logger().debug(f"Could not get second ask level: {e}")

                    sell_price = reference_ask - min_price_increment
                    # Check if maker price would cross the spread (become taker)
                    if sell_price <= top_bid:
                        # IMPORTANT: Spread is too tight for maker order
                        # Log clear warning so user understands why using taker price
                        self.strategy.logger().warning(
                            f"Position balancer: Spread too tight for 'min' tick mode on {sell_market_tuple.trading_pair}. "
                            f"Calculated maker price {sell_price:.8f} <= bid {top_bid:.8f}. "
                            f"Using bid price (will pay taker fees instead of maker rebate).")
                        sell_price = top_bid  # Use taker price with clear warning
                else:
                    # No min_price_increment available, fall back to taker
                    self.strategy.logger().warning(
                        f"Position balancer: No min_price_increment for {sell_market_tuple.trading_pair}, using taker price")
                    sell_price = top_bid
            except Exception as e:
                self.strategy.logger().warning(
                    f"Position balancer: Error calculating 'min' mode price for {sell_market_tuple.trading_pair}: {e}, using taker")
                sell_price = top_bid  # Fall back to taker on error
        elif self._sell_spread_pct == 0.0:
            # Aggressive mode (0%): Sell at bid (taker) - this is intentional
            sell_price = top_bid
        else:
            # Percentage mode (>0%): Place below top ask (maker order)
            # sell_price = top_ask - (top_ask * spread_pct)
            if top_ask <= 0:
                return False
            sell_price = top_ask * (1.0 - self._sell_spread_pct)
            # Check if maker price would cross the spread (become taker)
            if sell_price <= top_bid:
                # IMPORTANT: Spread is too tight for this percentage
                # Log clear warning so user understands why using taker price
                self.strategy.logger().warning(
                    f"Position balancer: Spread too tight for {self._sell_spread_pct*100:.2f}% spread on {sell_market_tuple.trading_pair}. "
                    f"Calculated maker price {sell_price:.8f} <= bid {top_bid:.8f}. "
                    f"Using bid price (will pay taker fees instead of maker rebate).")
                sell_price = top_bid  # Use taker price with clear warning

        # Calculate amount based on excess, available base, and order size limit
        # Use ADJUSTED excess to account for pending orders and avoid over-ordering
        max_order_base = self._order_size_usd / sell_price if sell_price > 0 else 0.0
        amount_to_sell = min(
            excess_adjusted / last_bid if last_bid > 0 else 0.0,
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

        # Quantize price to match exchange precision (prevents log/actual price mismatch)
        try:
            quantized_price = market.quantize_order_price(sell_market_tuple.trading_pair, Decimal(str(sell_price)))
            sell_price = float(quantized_price)
        except Exception:
            pass  # Fall back to original price if quantization fails

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

        # Track in balancer (asset_key, total_amount, filled_amount)
        try:
            # Safety check: warn if we're replacing an existing active order (shouldn't happen)
            if asset_key in self._active_sell_orders:
                old_order_id = self._active_sell_orders[asset_key]
                if old_order_id != sell_order_id:
                    self.strategy.logger().warning(
                        f"Position balancer: Replacing active sell order {old_order_id} with {sell_order_id} for {asset_key} "
                        f"(old order may not have been cleaned up properly)")

            self._pending_sell_orders[sell_order_id] = (asset_key, float(quantized_amount), 0.0)
            self._pending_sell_by_asset[asset_key] = (
                float(self._pending_sell_by_asset.get(asset_key, 0.0)) + float(quantized_amount))
            self._active_sell_orders[asset_key] = sell_order_id
            self._last_sell_order_time[asset_key] = self.strategy._current_timestamp
            # Store order details for smart cancellation (market_tuple, price)
            self._active_sell_order_details[asset_key] = (sell_market_tuple, sell_price)
        except Exception as e:
            self.strategy.logger().warning(f"Failed to track sell limit order {sell_order_id}: {e}")

        # Log order placement
        if self._sell_spread_is_min:
            self.strategy.logger().info(
                f"Placed sell limit order {sell_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {sell_price:.8f} (spread: min tick)")
        else:
            self.strategy.logger().info(
                f"Placed sell limit order {sell_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {sell_price:.8f} (spread: {self._sell_spread_pct * 100:.2f}%)")

        # Check if target reached - use ACTUAL balance (not adjusted)
        # to avoid marking as complete when order hasn't filled yet
        base_bal_actual = self.c_get_actual_base_balance(asset_key)
        val_excess = self.c_compute_value_and_sell_excess(base_bal_actual, last_bid)
        current_value_quote = val_excess.first
        excess = val_excess.second
        if self.c_try_mark_sell_complete(asset_key, current_value_quote, excess):
            self.c_maybe_disable_sell()

        return True

    # Runtime control methods

    def enable_buy_in(self):
        """
        Enable buy-in mode and reset completion flag.
        Allows buy-in to restart after target was previously reached.
        Checks if target is already reached and immediately disables if so.
        """
        if not self._buy_enabled:
            self._buy_enabled = True
            self._buy_completed = False
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy-in mode enabled - position balancer will acquire assets to reach target")
            # Check if target is already reached and disable immediately if so
            self.c_scan_and_mark_completion()

    def disable_buy_in(self):
        """
        Disable buy-in mode and cancel all active buy orders.
        Cleans up all tracking and stops placing new buy orders.
        """
        if self._buy_enabled:
            self._buy_enabled = False
            self.c_cancel_all_buy_orders()
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy-in mode disabled - cancelled all buy orders and cleared tracking")

    def enable_sell_off(self):
        """
        Enable sell-off mode and reset completion flag.
        Allows sell-off to restart after target was previously reached.
        Checks if target is already reached and immediately disables if so.
        """
        if not self._sell_enabled:
            self._sell_enabled = True
            self._sell_completed = False
            self.strategy.log_with_clock(
                logging.INFO,
                "Sell-off mode enabled - position balancer will reduce assets to reach target")
            # Check if target is already reached and disable immediately if so
            self.c_scan_and_mark_completion()

    def disable_sell_off(self):
        """
        Disable sell-off mode and cancel all active sell orders.
        Cleans up all tracking and stops placing new sell orders.
        """
        if self._sell_enabled:
            self._sell_enabled = False
            self.c_cancel_all_sell_orders()
            self.strategy.log_with_clock(
                logging.INFO,
                "Sell-off mode disabled - cancelled all sell orders and cleared tracking")

    def set_buy_target(self, double target_usd):
        """
        Set the buy-in target value in USD.

        Args:
            target_usd: New target minimum asset value in quote currency
        """
        old_target = self._buy_target_usd
        self._buy_target_usd = target_usd
        self.strategy.log_with_clock(
            logging.INFO,
            f"Buy-in target updated: {old_target:.2f} -> {target_usd:.2f} USD")
        # Reset completion flag to allow re-evaluation
        if self._buy_enabled:
            self._buy_completed = False
            self.c_scan_and_mark_completion()

    def set_sell_target(self, double target_usd):
        """
        Set the sell-off target value in USD.

        Args:
            target_usd: New target maximum asset value in quote currency
        """
        old_target = self._sell_target_usd
        self._sell_target_usd = target_usd
        self.strategy.log_with_clock(
            logging.INFO,
            f"Sell-off target updated: {old_target:.2f} -> {target_usd:.2f} USD")
        # Reset completion flag to allow re-evaluation
        if self._sell_enabled:
            self._sell_completed = False
            self.c_scan_and_mark_completion()

    def set_buy_spread(self, object spread_pct):
        """
        Set the buy spread percentage or mode.

        Args:
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick
        """
        if isinstance(spread_pct, str) and spread_pct.lower() == 'min':
            self._buy_spread_pct = -1.0
            self._buy_spread_is_min = True
            self.strategy.log_with_clock(
                logging.INFO,
                "Buy spread updated to: min tick")
        else:
            old_spread = self._buy_spread_pct * 100.0 if not self._buy_spread_is_min else "min"
            self._buy_spread_pct = float(spread_pct) / 100.0
            self._buy_spread_is_min = False
            self.strategy.log_with_clock(
                logging.INFO,
                f"Buy spread updated: {old_spread} -> {spread_pct}%")

    def set_sell_spread(self, object spread_pct):
        """
        Set the sell spread percentage or mode.

        Args:
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick
        """
        if isinstance(spread_pct, str) and spread_pct.lower() == 'min':
            self._sell_spread_pct = -1.0
            self._sell_spread_is_min = True
            self.strategy.log_with_clock(
                logging.INFO,
                "Sell spread updated to: min tick")
        else:
            old_spread = self._sell_spread_pct * 100.0 if not self._sell_spread_is_min else "min"
            self._sell_spread_pct = float(spread_pct) / 100.0
            self._sell_spread_is_min = False
            self.strategy.log_with_clock(
                logging.INFO,
                f"Sell spread updated: {old_spread} -> {spread_pct}%")

    def set_order_size(self, double order_size_usd):
        """
        Set the maximum order size in USD.

        Args:
            order_size_usd: Maximum order size in USD per order
        """
        old_size = self._order_size_usd
        self._order_size_usd = order_size_usd
        self.strategy.log_with_clock(
            logging.INFO,
            f"Order size updated: {old_size:.2f} -> {order_size_usd:.2f} USD")

    def set_refresh_interval(self, double refresh_interval):
        """
        Set the limit order refresh interval in seconds.

        Args:
            refresh_interval: How often to cancel and replace limit orders (seconds)
        """
        old_interval = self._limit_refresh_interval
        self._limit_refresh_interval = refresh_interval
        self.strategy.log_with_clock(
            logging.INFO,
            f"Limit order refresh interval updated: {old_interval:.0f} -> {refresh_interval:.0f} seconds")

    def set_aggressive_refresh_interval(self, double refresh_interval):
        """
        Set the aggressive refresh interval in seconds.

        Args:
            refresh_interval: Refresh interval for aggressive partial fills (seconds)
        """
        old_interval = self._aggressive_refresh_interval
        self._aggressive_refresh_interval = refresh_interval
        self.strategy.log_with_clock(
            logging.INFO,
            f"Aggressive refresh interval updated: {old_interval:.0f} -> {refresh_interval:.0f} seconds")