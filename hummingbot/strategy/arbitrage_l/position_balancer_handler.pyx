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

# --- Fill-pressure adaptive post-fill wait ---
# Mimics a trailing stop on the "min" (top-of-book) strategy. A single fill is noise;
# a SECOND full completion arriving quickly after the first is direct evidence that
# buy/sell pressure is hitting our side. When that happens we extend the post-fill
# wait so we stop re-pinning the top of book every 2s and instead give price room to
# move in our favour before placing the next tranche.
#
# Signal: time between two consecutive FULL completions on the same canonical asset
# (recorded in c_record_fill_pressure, fired only from c_handle_order_completion —
# never from cancels). Proportional: the faster the second fill, the longer the wait.
#
#   extra = clamp(FILL_PRESSURE_WINDOW - inter_fill, 0, MAX_POST_FILL_EXTRA)
#   effective_wait = DEFAULT_COMPLETION_COOLDOWN + extra   # 2s floor .. ~10s ceiling
#
# A gap larger than FILL_PRESSURE_WINDOW resets extra to 0 (pressure cooled).
# The first fill (no prior timestamp) always yields extra=0.
cdef double FILL_PRESSURE_WINDOW = 12.0   # seconds — second completion within this window = pressure
cdef double MAX_POST_FILL_EXTRA  = 8.0    # seconds — cap on the added wait (2s base + 8s = 10s ceiling)

# --- Minimum order age before immediate-cancel checks fire ---
# Two thresholds because different events have different noise profiles:
#
# MIN_FRONTRUN_CHECK_DELAY (5s) — frontrun/undercut and large-gap detection.
#   These are real book events, not tick noise. Waiting 60s means sitting behind
#   someone else in the queue for a full minute. 5s is long enough to ignore a
#   single-tick bounce but short enough to reclaim top-of-book quickly.
#
# MIN_MARKET_SWITCH_DELAY (60s) — "better market" detection (different exchange).
#   Cross-exchange price differences are noisy; switching too quickly causes
#   flip-flopping. 60s matches the periodic refresh interval.
#
# 'mode disabled' cancels are always immediate regardless of either floor.
cdef double MIN_FRONTRUN_CHECK_DELAY = 5.0    # seconds — frontrun/undercut + large-gap
cdef double MIN_MARKET_SWITCH_DELAY  = 60.0   # seconds — better market (different exchange)

# --- Post-cancel cooldown before placing the next order ---
# Two tiers based on cancel reason:
#
# POST_CANCEL_COOLDOWN_REACTIVE (3s) — market moved against us (undercut/frontrun/large-gap).
#   We need to reprice quickly to reclaim top-of-book. 3s is enough to let the exchange
#   process the cancel and avoid a rapid-fire burst impression.
#
# POST_CANCEL_COOLDOWN_PROACTIVE (10s) — we initiated the cancel (periodic refresh,
#   better market, price divergence). No urgency; the existing order was not beaten.
#
# Both are distinct from DEFAULT_COMPLETION_COOLDOWN (2s, used after a fill).
cdef double POST_CANCEL_COOLDOWN_REACTIVE  = 3.0   # seconds — reactive: undercut/frontrun/large-gap
cdef double POST_CANCEL_COOLDOWN_PROACTIVE = 10.0  # seconds — proactive: refresh/better-market/divergence
# Legacy alias — kept for any external references; equals PROACTIVE value
cdef double POST_CANCEL_COOLDOWN = 10.0

# --- Stuck Cancel Detection ---
cdef double STUCK_CANCEL_MULTIPLIER = 2.0  # Multiplier for stuck cancel detection (2x refresh interval)

# --- Pending Cancel Wait ---
# How long to wait for cancel confirmation before allowing force cleanup
# This prevents orphaned orders on exchanges with slow cancel confirmations (e.g., BitMart)
cdef double PENDING_CANCEL_WAIT_SECONDS = 10.0  # Wait at least 10s for cancel confirmation

# --- Price Threshold Constants ---
cdef double TICK_TOLERANCE = 0.9            # Tolerance for detecting if order matches top of book (90% of tick)
cdef double HALF_TICK_TOLERANCE = 0.5       # Half-tick tolerance for price matching
cdef double LARGE_GAP_THRESHOLD = 1.9       # Multi-tick gap threshold for immediate cancellation (2 ticks)
cdef double AGGRESSIVE_MODE_TOLERANCE = 0.999  # Price tolerance for aggressive mode (0.1%)

# --- Step-up-into-gap (min mode only) ---
# When our order IS the top of book but the NEXT foreign level is far away (a big gap
# above a sell / below a buy), we are leaving price on the table: we could move toward
# that level and STILL be best-of-book. For sells we step the ask UP to (next_foreign_ask
# - 1 tick); for buys we step the bid DOWN to (next_foreign_bid + 1 tick). Stepping toward
# a gap is the ANTI-bid-war direction (we give counterparties a WORSE price), so it cannot
# trigger an undercut war.
#
# Spam control is deliberately FRONT-LOADED (throttle BEFORE we act, then act fast):
#   - STEP_UP_MIN_GAP_TICKS: only arm when the gap is at least this many ticks.
#   - STEP_UP_MIN_INTERVAL: per-asset min seconds between step-ups (the API-spam guard).
#     Checked *before* detection, so a flickering gap can't cause repeated cancels.
#   - STEP_UP_COOLDOWN: SHORT post-cancel cooldown (vs the 10s proactive default) so we get
#     back on the book quickly — the throttle already happened up front, no need to dwell off-book.
# Tagged PROACTIVE: the step-up cancel does NOT increment the undercut/frontrun streak.
cdef double STEP_UP_MIN_GAP_TICKS = 5.0     # min headroom (in ticks) to the next foreign level before stepping in
cdef double STEP_UP_MIN_INTERVAL  = 30.0    # per-asset min seconds between step-ups (pre-detection API-spam throttle)
cdef double STEP_UP_COOLDOWN       = 3.0    # short post-cancel cooldown for a step-up (minimize off-book time)

# --- Second-level refuge against a persistent penny-jumper (min mode only) ---
# Classic "don't chase the penny-jumper" problem: a reactive bot places exactly 1 tick
# inside us every cycle, anchoring to OUR price. Re-pricing to reclaim best-of-book just
# feeds the war — they re-anchor, and (worst case) price walks away from us with no fills.
# Standard market-making practice is to STOP chasing optimal queue position and instead
# accept a defined, passive position. Here: stop fighting for L1 and tuck in just under the
# WALL — the next genuine resting level above the jumper (the 2nd foreign ask for a sell /
# 2nd foreign bid for a buy). The jumper is left alone at the cheap top; the next organic
# market order lifts THEM, not us. When they fill/leave we are best-of-book again at a
# better price.
#
# State machine per canonical asset, with hysteresis to avoid flip-flop:
#   NORMAL --(undercut fires AND streak >= REFUGE_ARM_STREAK)--> REFUGE
#   REFUGE --(any fill -> streak resets, _in_refuge cleared in handle_order_fill)--> NORMAL
# While in REFUGE we SUPPRESS the undercut trigger (we intend to sit 2nd-best) and place at
# wall ∓ 1 tick instead of top ∓ 1 tick. Exiting only on a fill is safe: if the jumper simply
# leaves, we are already at wall-1tick == correct top placement, so NORMAL resumes seamlessly.
# (Future refinement: a "wall must be >= N ticks above the jumper" entry gate so we only take
# refuge when there's a distinct level to hide under — see position-balancer.md. For now we
# enter on streak alone; placement reads the 2nd level and falls back to top if there's no wall.)
cdef double REFUGE_ARM_STREAK = 10.0   # consecutive reactive undercuts before we stop chasing and take refuge
cdef double PRICE_DIVERGENCE_THRESHOLD_PCT = 0.01  # 1% threshold for percentage mode divergence

# --- Better Market Switch ---
# Tolerance for triggering immediate market switch (price difference as ratio)
# e.g., 0.0001 = 0.01% = switch if other market is 0.01% better
cdef double BETTER_MARKET_SWITCH_TOLERANCE = 0.0001  # 0.01% - switch immediately if another market is better

# Min mode hysteresis: only switch if new market is at least 0.1% better
# This prevents flip-flopping between markets with nearly identical effective prices
cdef double MIN_MODE_SWITCH_HYSTERESIS = 0.001  # 0.1% - require meaningful improvement before switching

# --- Insufficient balance retry cooldown ---
# When the best sell market has insufficient base balance to meet min_order_usd,
# suppress retries for this many seconds to avoid spamming every 2s tick.
cdef double INSUF_BAL_RETRY_COOLDOWN = 60.0  # 60s between retries on insufficient balance


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
        self._last_buy_cancel_time = {}       # canonical_asset -> timestamp when buy order was cancelled
        self._last_buy_cancel_cooldown = {}   # canonical_asset -> cooldown duration to apply (reactive vs proactive)
        self._last_sell_cancel_time = {}      # canonical_asset -> timestamp when sell order was cancelled
        self._last_sell_cancel_cooldown = {}  # canonical_asset -> cooldown duration to apply (reactive vs proactive)
        self._last_sell_insuf_bal_time = {}   # asset -> timestamp of last "sell skipped - insufficient balance" (for INSUF_BAL_RETRY_COOLDOWN)
        # Consecutive-cancel backoff: count cancels without a fill to detect stuck-undercutter loops
        self._buy_cancel_streak = {}          # canonical_asset -> int, resets on fill
        self._sell_cancel_streak = {}         # canonical_asset -> int, resets on fill
        # Fill-pressure adaptive post-fill wait (trailing-stop-like spacing on the top-of-book strategy)
        self._last_fill_completion_time = {}  # canonical_asset -> timestamp of last FULL completion (fills only, not cancels)
        self._post_fill_extra_wait = {}       # canonical_asset -> seconds added to DEFAULT_COMPLETION_COOLDOWN
        # Step-up-into-gap throttle: last time we stepped our order into a gap (per canonical asset).
        # Pre-detection gate (STEP_UP_MIN_INTERVAL) so a flickering gap can't cause repeated cancels.
        self._last_step_up_time = {}          # canonical_asset -> timestamp of last step-up cancel
        # Second-level refuge state (per canonical asset). True = we have stopped chasing the
        # penny-jumper and are resting under the wall (2nd-best). Set on detection, cleared on
        # any fill (handle_order_fill). While True, undercut is suppressed and placement targets
        # the wall instead of top-of-book.
        self._in_refuge_sell = {}             # canonical_asset -> bint (in sell-side refuge)
        self._in_refuge_buy = {}              # canonical_asset -> bint (in buy-side refuge)

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
        Also resets the consecutive-cancel streak — a fill means we're making progress.
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
                # Reset consecutive-cancel streak — a fill means progress
                canonical = self._get_canonical_asset(asset_key)
                self._buy_cancel_streak.pop(canonical, None)
                # Exit buy-side refuge: a fill means our 2nd-best tuck worked (or the frontrunner
                # cleared). Resume normal best-of-book chasing.
                self._in_refuge_buy.pop(canonical, None)
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
                # Reset consecutive-cancel streak — a fill means progress
                canonical = self._get_canonical_asset(asset_key)
                self._sell_cancel_streak.pop(canonical, None)
                # Exit sell-side refuge: a fill means our 2nd-best tuck worked (or the undercutter
                # cleared). Resume normal best-of-book chasing.
                self._in_refuge_sell.pop(canonical, None)
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
                        # Record completion time under canonical key — placement gate reads canonical
                        self._last_buy_completion_time[self._get_canonical_asset(asset_key)] = self.strategy._current_timestamp
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
                        # Record completion time under canonical key — placement gate reads canonical
                        self._last_sell_completion_time[self._get_canonical_asset(asset_key)] = self.strategy._current_timestamp
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to handle completion for {order_id}: {e}")

    def c_record_fill_pressure(self, str order_id, bint is_buy):
        """
        Trailing-stop-like adaptive post-fill wait.

        Fired ONLY from arbitrage.c_handle_order_completion (genuine fill completions,
        never cancels — those route through handle_order_cancellation). One completion is
        noise; a SECOND completion arriving quickly on the same canonical asset is direct
        evidence of buy/sell pressure on our side. When that happens we extend the post-fill
        wait so we stop re-pinning the top of book and give price room to move in our favour.

        MUST run BEFORE handle_order_completion (which pops the pending dict) so the order is
        still present here for the canonical-asset lookup.

            extra = clamp(FILL_PRESSURE_WINDOW - inter_fill, 0, MAX_POST_FILL_EXTRA)
            effective_wait = DEFAULT_COMPLETION_COOLDOWN + extra   # 2s floor .. ~10s ceiling

        First fill (no prior timestamp) -> extra=0. Gap > window -> extra=0 (pressure cooled).
        """
        cdef:
            double now = self.strategy._current_timestamp
            double prior
            double inter_fill = 0.0   # init: silences -Wmaybe-uninitialized; the first-fill branch (prior<=0) skips the log anyway
            double extra
            str asset_key
            str canonical
        try:
            if is_buy:
                pend = self._pending_buy_orders.get(order_id)
            else:
                pend = self._pending_sell_orders.get(order_id)
            if pend is None:
                return
            asset_key = pend[0]
            canonical = self._get_canonical_asset(asset_key)

            prior = self._last_fill_completion_time.get(canonical, 0.0)
            if prior <= 0.0:
                # First completion in this run for this asset — treat as noise.
                extra = 0.0
            else:
                inter_fill = now - prior
                # Proportional: the faster the second fill, the longer the added wait.
                extra = FILL_PRESSURE_WINDOW - inter_fill
                if extra < 0.0:
                    extra = 0.0          # gap exceeded window — pressure cooled, reset
                elif extra > MAX_POST_FILL_EXTRA:
                    extra = MAX_POST_FILL_EXTRA

            self._post_fill_extra_wait[canonical] = extra
            self._last_fill_completion_time[canonical] = now

            if extra > 0.0:
                self.strategy.logger().info(
                    f"Position balancer: fill-pressure armed [{canonical}] "
                    f"{'buy' if is_buy else 'sell'} — inter-fill {inter_fill:.2f}s "
                    f"-> +{extra:.2f}s wait (next placement gate {DEFAULT_COMPLETION_COOLDOWN + extra:.2f}s)")
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to record fill pressure for {order_id}: {e}")

    def handle_order_cancellation(self, str order_id):
        """Clean up pending order tracking on order cancellation."""
        # Record cancel time under the CANONICAL asset key so the placement gate
        # (which always reads canonical) never misses the cooldown on aliased assets.
        # The cooldown duration was already stored in _last_buy/sell_cancel_cooldown by
        # _cancel_buy/sell_order. For external cancels (exchange-initiated) we fall back
        # to POST_CANCEL_COOLDOWN (proactive/conservative default).
        ts = self.strategy._current_timestamp
        if order_id in self._pending_buy_orders:
            asset_key = self._pending_buy_orders[order_id][0]
            canonical = self._get_canonical_asset(asset_key)
            # Start cooldown clock NOW (when exchange confirms cancel, not when we sent the request).
            self._last_buy_cancel_time[canonical] = ts
            # Cooldown duration was already set by _cancel_buy_order for internal cancels.
            # For external/exchange-initiated cancels (not in dict), use conservative default
            # and count toward the streak.
            if canonical not in self._last_buy_cancel_cooldown:
                self._last_buy_cancel_cooldown[canonical] = POST_CANCEL_COOLDOWN_PROACTIVE
                self._buy_cancel_streak[canonical] = self._buy_cancel_streak.get(canonical, 0) + 1
            # else: _cancel_buy_order already wrote cooldown and incremented streak; leave both.
        elif order_id in self._pending_sell_orders:
            asset_key = self._pending_sell_orders[order_id][0]
            canonical = self._get_canonical_asset(asset_key)
            self._last_sell_cancel_time[canonical] = ts
            if canonical not in self._last_sell_cancel_cooldown:
                self._last_sell_cancel_cooldown[canonical] = POST_CANCEL_COOLDOWN_PROACTIVE
                self._sell_cancel_streak[canonical] = self._sell_cancel_streak.get(canonical, 0) + 1
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

        Only considers markets with sufficient quote balance (>= _min_order_usd) so that
        the selected market is always actionable. This prevents picking a cheap market that
        has zero USDT, which would cause a per-tick warning and give up without trying others.

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
            double market_quote_bal
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
                            # Skip markets without enough quote to place even a minimum order
                            market_quote_bal = float(market_tuple.market.get_available_balance(
                                market_tuple.quote_asset))
                            if market_quote_bal < self.strategy._min_order_usd:
                                continue

                            # Use C-level get_order_book for compatibility with all exchanges
                            ob = (<ExchangeBase>market_tuple.market).c_get_order_book(market_tuple.trading_pair)

                            if use_ask_price:
                                # Aggressive mode (0%): select market with LOWEST ASK (taker)
                                if ob._ask_book.size() > 0:
                                    current_price = float(deref(ob._ask_book.begin()).getPrice())
                                else:
                                    continue
                            elif use_effective_price:
                                # 'min' mode: calculate BEST ACHIEVABLE price on this market
                                # If spread is wide: use (bid + tick) as maker
                                # If spread is tight: use ask as taker
                                # Then compare across all markets to find best overall (lowest for buy)
                                if ob._bid_book.size() > 0 and ob._ask_book.size() > 0:
                                    current_bid = float(deref(ob._bid_book.rbegin()).getPrice())
                                    current_ask = float(deref(ob._ask_book.begin()).getPrice())
                                    # Get min_price_increment for this market
                                    min_tick = 0.0
                                    trading_rule = market_tuple.market._trading_rules.get(market_tuple.trading_pair)
                                    if trading_rule is not None and trading_rule.min_price_increment is not None:
                                        min_tick = float(trading_rule.min_price_increment)
                                    
                                    if min_tick > 0:
                                        maker_price = current_bid + min_tick  # Price if we place as maker
                                        # Choose best achievable: maker if viable, else taker
                                        if maker_price < current_ask:
                                            current_price = maker_price  # Maker order is viable
                                        else:
                                            current_price = current_ask  # Spread too tight, would be taker
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
            double current_bid
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
                                # 'min' mode: calculate BEST ACHIEVABLE price on this market
                                # If spread is wide: use (ask - tick) as maker
                                # If spread is tight: use bid as taker
                                # Then compare across all markets to find best overall
                                if ob._ask_book.size() > 0 and ob._bid_book.size() > 0:
                                    current_ask = float(deref(ob._ask_book.begin()).getPrice())
                                    current_bid = float(deref(ob._bid_book.rbegin()).getPrice())
                                    # Get min_price_increment for this market
                                    min_tick = 0.0
                                    trading_rule = market_tuple.market._trading_rules.get(market_tuple.trading_pair)
                                    if trading_rule is not None and trading_rule.min_price_increment is not None:
                                        min_tick = float(trading_rule.min_price_increment)
                                    
                                    if min_tick > 0:
                                        maker_price = current_ask - min_tick  # Price if we place as maker
                                        # Choose best achievable: maker if viable, else taker
                                        if maker_price > current_bid:
                                            current_price = maker_price  # Maker order is viable
                                        else:
                                            current_price = current_bid  # Spread too tight, would be taker
                                    else:
                                        # WARNING: min_tick lookup failed - using raw ask
                                        self.strategy.logger().warning(
                                            f"Position balancer: min_tick=0 for {market_tuple.market.name}:{market_tuple.trading_pair} - "
                                            f"falling back to raw ask. Check trading_rules!")
                                        current_price = current_ask
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

    cdef bint c_check_stuck_cancel(self, str order_id, str asset, bint is_buy, double current_time, bint force_short_timeout=False):
        """
        Detect and cleanup stuck cancellations.
        
        Args:
            order_id: The order ID to check
            asset: The asset key
            is_buy: True for buy orders, False for sell orders
            current_time: Current timestamp
            force_short_timeout: If True, use shorter PENDING_CANCEL_WAIT_SECONDS timeout
                               (for order replacement scenarios). Otherwise use the longer
                               STUCK_CANCEL_MULTIPLIER * refresh_interval.
        
        Returns True if stuck cancel was detected and cleaned up, False otherwise.
        """
        cdef:
            double cancel_request_time
            double time_since_cancel_request
            double timeout_threshold
        
        # Use shorter timeout for order replacement, longer for general stuck cancel detection
        if force_short_timeout:
            timeout_threshold = PENDING_CANCEL_WAIT_SECONDS
        else:
            timeout_threshold = self._limit_refresh_interval * STUCK_CANCEL_MULTIPLIER
        
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
        
        # Force cleanup if cancel has been pending for > timeout threshold
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
        Returns top_price unchanged.

        When our order IS the top (top_price ≈ order_price), effective_price == top_price
        so callers compute got_valid_second_level = False and skip checks that need a real
        external reference (conditions 2b and 3 in the periodic block).

        When our order is NOT the top (undercut/frontrunned), CHECK 2 in
        c_check_immediate_conditions fires before this is ever used for gap logic.

        NOTE: do NOT return the second book level here. The large-gap check in
        c_check_immediate_conditions uses this as reference: if we're at top and the
        second level is many ticks away (common on thin markets), returning it would
        compute a spurious gap and cancel every 5s.
        """
        return top_price

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

    cdef tuple c_check_immediate_conditions(self, str asset, bint is_buy, double order_age,
                                            double frontrun_delay=MIN_FRONTRUN_CHECK_DELAY):
        """
        UNIFIED immediate check for all conditions that require instant response.

        Applies two separate age thresholds to avoid false triggers on fresh orders:
          - Frontrun/undercut + large gap: frontrun_delay (default MIN_FRONTRUN_CHECK_DELAY=5s)
              Real book events — react quickly to reclaim top-of-book position.
              Caller may pass a larger value (e.g. 20s) when a consecutive-cancel streak is
              detected, to slow down chasing in stuck-undercutter loops.
          - Better market (different exchange): MIN_MARKET_SWITCH_DELAY (60s)
              Cross-exchange price differences are noisy; slow gate prevents flip-flopping.

        All active checks use the SAME orderbook snapshot to avoid race conditions.

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
            double next_ask = 0.0
            double next_bid = 0.0
            double headroom_ticks = 0.0
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

            # Fetch current order's market orderbook ONCE — used by CHECK 2 & 3
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
            # Gated by MIN_MARKET_SWITCH_DELAY — cross-exchange price differences
            # are noisy; switching too quickly causes flip-flopping.
            # The market scan (c_find_best_*_market) is intentionally deferred to
            # inside this gate: it's the most expensive call (iterates all pairs,
            # reads orderbooks) and is pointless before the 60s threshold.
            # ================================================================
            if order_age >= MIN_MARKET_SWITCH_DELAY:
                # Only now do we pay the cost of scanning all markets
                if is_buy:
                    current_best_market = self.c_find_best_buy_market(asset)
                else:
                    current_best_market = self.c_find_best_sell_market(asset)

            if current_best_market is not None and current_best_market.market.name != order_market_tuple.market.name:
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
            # Gated by MIN_FRONTRUN_CHECK_DELAY — these are real book events,
            # not tick noise. 5s is enough to ignore a single-tick bounce.
            # Skip for aggressive mode (0%) — they don't compete for position.
            # ================================================================
            if not should_cancel and is_maker_mode and order_age >= frontrun_delay:
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

                    # CHECK 4: Step-down into gap (min mode only) — mirror of the sell step-up.
                    # We ARE top bid but the next foreign bid is far below; move down to
                    # (next_bid + 1 tick) and stay best bid, buying cheaper. Only when NOT
                    # frontrun (we are top): current_bid ~= order_price. Pre-detection throttle
                    # (STEP_UP_MIN_INTERVAL) is the API-spam guard. Proactive (reason prefixed
                    # "step-up …" → caller does NOT mark reactive / streak-bump).
                    if (not should_cancel and spread_is_min and min_price_increment > 0
                            and current_bid <= order_price + min_price_increment * HALF_TICK_TOLERANCE
                            and (self.strategy._current_timestamp
                                 - self._last_step_up_time.get(self._get_canonical_asset(asset), 0.0))
                                >= STEP_UP_MIN_INTERVAL):
                        # NOTE: bid_entries() is a GENERATOR (yields OrderBookRow), not a list.
                        # entry[0] is our own order (top bid); entry[1] is the next foreign level.
                        next_bid = 0.0
                        try:
                            bid_iter = order_ob.bid_entries()
                            bid_lvl0 = next(bid_iter, None)
                            bid_lvl1 = next(bid_iter, None)
                            if bid_lvl1 is not None:
                                next_bid = float(bid_lvl1.price)
                        except Exception:
                            next_bid = 0.0
                        if next_bid > 0.0 and next_bid < order_price:
                            headroom_ticks = (order_price - next_bid) / min_price_increment
                            if headroom_ticks >= STEP_UP_MIN_GAP_TICKS:
                                should_cancel = True
                                cancel_reason = (f"step-up into gap (next bid {next_bid:.8f} is "
                                                 f"{headroom_ticks:.1f} ticks below our {order_price:.8f})")
                                self._last_step_up_time[self._get_canonical_asset(asset)] = self.strategy._current_timestamp
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

                    # CHECK 4: Step-up into gap (min mode only) — we ARE top ask but the next
                    # foreign ask is far above; move up to (next_ask - 1 tick) and stay best ask.
                    # Only when NOT undercut (we are top): current_ask ~= order_price.
                    # Pre-detection throttle (STEP_UP_MIN_INTERVAL) is the API-spam guard.
                    # Proactive (reason prefixed "step-up …" → caller does NOT mark reactive,
                    # so it never feeds the undercut streak / bid-war backoff).
                    if (not should_cancel and spread_is_min and min_price_increment > 0
                            and current_ask >= order_price - min_price_increment * HALF_TICK_TOLERANCE
                            and (self.strategy._current_timestamp
                                 - self._last_step_up_time.get(self._get_canonical_asset(asset), 0.0))
                                >= STEP_UP_MIN_INTERVAL):
                        # We're at (or above) top — read the next foreign ask level.
                        # NOTE: ask_entries() is a GENERATOR (yields OrderBookRow), not a list —
                        # it is NOT subscriptable and has no len(). Pull the first two rows by
                        # iterating. entry[0] is our own order (top); entry[1] is the next foreign level.
                        next_ask = 0.0
                        try:
                            ask_iter = order_ob.ask_entries()
                            ask_lvl0 = next(ask_iter, None)
                            ask_lvl1 = next(ask_iter, None)
                            if ask_lvl1 is not None:
                                next_ask = float(ask_lvl1.price)
                        except Exception:
                            next_ask = 0.0
                        if next_ask > order_price:
                            headroom_ticks = (next_ask - order_price) / min_price_increment
                            if headroom_ticks >= STEP_UP_MIN_GAP_TICKS:
                                should_cancel = True
                                cancel_reason = (f"step-up into gap (next ask {next_ask:.8f} is "
                                                 f"{headroom_ticks:.1f} ticks above our {order_price:.8f})")
                                self._last_step_up_time[self._get_canonical_asset(asset)] = self.strategy._current_timestamp

        except Exception as e:
            self.strategy.logger().debug(f"Position balancer: Immediate check failed: {e}")
        
        return (should_cancel, cancel_reason)



    cdef void c_cancel_stale_orders(self, str asset):
        """
        Cancel stale buy/sell limit orders for refresh.

        REAL-WORLD GOAL: Maintain competitive position in order book while getting best price.

        Every tick — via c_check_immediate_conditions (time-gated):
          - Frontrun/undercut: someone placed more aggressive order (fires after MIN_FRONTRUN_CHECK_DELAY=5s)
          - Large gap: order is > ~2 ticks away from top of book (fires after 5s)
          - Better market: different exchange became significantly better (fires after MIN_MARKET_SWITCH_DELAY=60s)

        Periodic — after effective_interval (default 60s), same orderbook snapshot:
          1. Mode disabled → orphaned order cleanup
          2b. 1-tick min-mode misalignment (needs got_valid_second_level)
          3.  Market moved in our favor → can get better price
          4.  Aggressive mode: market moved to our price
          5.  Significant price divergence (c_check_price_divergence)

        Stuck cancel detection runs unconditionally when order is in timeout_cancelled_orders.
        """
        cdef:
            double current_time = self.strategy._current_timestamp
            OrderBook current_ob
            object trading_rule
            tuple order_details
            double order_price
            double current_bid
            double current_ask
            double min_price_increment
            double cancel_threshold_abs
            double cancel_threshold_pct
            bint should_cancel
            bint is_reactive   # True when cancel is driven by a market event (undercut/frontrun/gap)
            str cancel_reason
            str order_id
            int cancel_streak
            double effective_frontrun_delay

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
                    is_reactive = False
                    cancel_reason = ""

                    # Consecutive-cancel streak backoff: after 5 cancels without a fill,
                    # extend the frontrun check delay to slow down chasing in stuck-undercutter loops.
                    # The streak is reset to 0 on any fill (handle_order_fill).
                    cancel_streak = self._buy_cancel_streak.get(self._get_canonical_asset(asset), 0)
                    if cancel_streak >= 5:
                        effective_frontrun_delay = MIN_FRONTRUN_CHECK_DELAY * 4.0  # 20s
                    else:
                        effective_frontrun_delay = MIN_FRONTRUN_CHECK_DELAY  # 5s

                    # Cancel if mode disabled (orphaned order)
                    if not self._buy_enabled:
                        should_cancel = True
                        cancel_reason = "mode disabled"

                    # UNIFIED IMMEDIATE CHECK: Better market + Frontrun + Gap detection
                    # Pass order age so the function can apply per-check delay thresholds.
                    # Uses effective_frontrun_delay (backed off when streak >= 5).
                    if not should_cancel:
                        should_cancel, cancel_reason = self.c_check_immediate_conditions(
                            asset, True, current_time - last_time, effective_frontrun_delay)

                    # ---- SECOND-LEVEL REFUGE (buy): don't chase the penny-jumper (mirror of sell) ----
                    # On "frontrun" (someone bid HIGHER than us): (a) already in refuge -> suppress and
                    # hold our wall order; (b) streak >= ARM and a valid wall (2nd foreign bid >= 2 ticks
                    # BELOW the frontrunner) exists -> enter refuge, allow this cancel to move us DOWN
                    # under the wall, retag "refuge". Cleared on any fill.
                    if (should_cancel and self._buy_spread_is_min
                            and cancel_reason.startswith("frontrun")):
                        refuge_canonical = self._get_canonical_asset(asset)
                        if self._in_refuge_buy.get(refuge_canonical, False):
                            should_cancel = False
                            cancel_reason = ""
                        elif cancel_streak >= REFUGE_ARM_STREAK:
                            # If no real wall exists, placement reads the 2nd bid and falls back to
                            # normal top-of-book; a wall-distance entry gate is a future refinement.
                            self._in_refuge_buy[refuge_canonical] = True
                            cancel_reason = "refuge (taking 2nd-best under wall — stop chasing penny-jumper)"
                            self.strategy.logger().info(
                                f"Position balancer: BUY refuge ARMED for {refuge_canonical} "
                                f"(streak={cancel_streak}) — retreating under the wall, frontrun now suppressed")

                    # Immediate-check cancels are reactive (market event) EXCEPT:
                    #   - "mode disabled": housekeeping
                    #   - "step-up …": we voluntarily reprice into a gap (no one beat us) — proactive,
                    #     so it takes the 10s cooldown and does NOT bump the frontrun streak.
                    #   - "refuge …": deliberate move under the wall — proactive, no streak bump.
                    if (should_cancel and cancel_reason
                            and not cancel_reason.startswith("mode disabled")
                            and not cancel_reason.startswith("step-up")
                            and not cancel_reason.startswith("refuge")):
                        is_reactive = True

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
                        # Periodic refresh: c_check_immediate_conditions already handles frontrun/gap
                        # (after 5s) and better-market (after 60s) every tick, so here we only run
                        # the checks that need a full orderbook snapshot and are not time-gated:
                        #   2b: 1-tick min-mode frontrun (needs got_valid_second_level)
                        #   3:  ideal-price gap (market moved down → can buy cheaper)
                        #   4:  aggressive mode — market moved to our price
                        #   5:  significant price divergence (c_check_price_divergence)
                        # Unconditional fallback: always refresh after effective_interval regardless
                        # of OB conditions (handles exchanges with sparse/stale books where no
                        # condition ever fires, leaving orders stranded until force-cleanup at 2×timeout).
                        order_details = self._active_buy_order_details.get(asset)
                        if order_details is not None:
                            order_market_tuple, order_price = order_details
                            try:
                                current_ob = (<ExchangeBase>order_market_tuple.market).c_get_order_book(order_market_tuple.trading_pair)

                                if current_ob._bid_book.size() > 0:
                                    current_bid = float(deref(current_ob._bid_book.rbegin()).getPrice())
                                else:
                                    current_bid = 0.0

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

                                # Calculate effective_bid (second level if top is OUR order)
                                effective_bid = self.c_get_effective_reference_price(
                                    current_ob, current_bid, order_price, min_price_increment, True)

                                # Track if we got a valid second level (not our own order)
                                got_valid_second_level = (effective_bid != current_bid)

                                # Calculate ideal order price
                                ideal_order_price = self.c_calculate_ideal_order_price(
                                    effective_bid, self._buy_spread_pct, self._buy_spread_is_min,
                                    min_price_increment, True)

                                # CONDITION 2b: 1-tick min-mode frontrun/gap (needs got_valid_second_level)
                                # Complements CHECK 2 in c_check_immediate_conditions which only checks
                                # current_bid > order_price; this catches the subtler 1-tick misalignment.
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

                                # CONDITION 3: Market moved DOWN — can buy cheaper
                                # Skip in aggressive mode (0%): ideal_order_price = effective_bid,
                                # but the order is placed at top_ask — the spread gap between bid
                                # and ask is not a "can buy cheaper" signal. Condition 4 handles
                                # aggressive-mode repricing correctly.
                                if not should_cancel and not (self._buy_spread_pct == 0.0 and not self._buy_spread_is_min) and ideal_order_price < order_price:
                                    gap_down = order_price - ideal_order_price
                                    if self._buy_spread_is_min:
                                        cancel_threshold_abs = min_price_increment * TICK_TOLERANCE
                                        if got_valid_second_level and gap_down > cancel_threshold_abs:
                                            should_cancel = True
                                            cancel_reason = f"gap down {gap_down:.8f} > threshold {cancel_threshold_abs:.8f} (can buy cheaper)"
                                    else:
                                        cancel_threshold_pct = max(0.001, self._buy_spread_pct * 0.5)
                                        gap_pct = gap_down / order_price if order_price > 0 else 0.0
                                        if gap_pct > cancel_threshold_pct:
                                            should_cancel = True
                                            cancel_reason = f"gap down {gap_pct*100:.2f}% (can buy cheaper at {ideal_order_price:.8f} vs {order_price:.8f})"

                                # CONDITION 4: Aggressive mode — market moved to our price
                                if not should_cancel and self._buy_spread_pct == 0.0:
                                    if current_bid >= order_price * AGGRESSIVE_MODE_TOLERANCE:
                                        should_cancel = True
                                        cancel_reason = "market moved to our price (aggressive mode)"

                                # CONDITION 5: Significant price divergence
                                # Skip in aggressive mode (0%): ideal_order_price is based on
                                # effective_bid but order is at top_ask; the spread gap is not
                                # divergence. Condition 4 handles aggressive-mode repricing.
                                if not should_cancel and not (self._buy_spread_pct == 0.0 and not self._buy_spread_is_min):
                                    should_cancel, cancel_reason = self.c_check_price_divergence(
                                        order_price, ideal_order_price, self._buy_spread_is_min,
                                        min_price_increment, got_valid_second_level)

                            except Exception as e:
                                self.strategy.logger().warning(f"Position balancer: Error checking buy order conditions: {e}")
                                # On OB error, fall back to unconditional refresh so orders don't get stranded.
                                if not should_cancel:
                                    should_cancel = True
                                    cancel_reason = "periodic refresh (OB error)"

                        else:
                            # No order details — unknown state, refresh unconditionally.
                            should_cancel = True
                            cancel_reason = "periodic refresh (no order details)"

                    if should_cancel:
                        # is_reactive is True only for immediate-check (market-event) cancels
                        self._cancel_buy_order(asset, order_id, cancel_reason, reactive=is_reactive)

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
                    is_reactive = False
                    cancel_reason = ""

                    # Consecutive-cancel streak backoff: after 5 cancels without a fill,
                    # extend the frontrun check delay to slow down chasing in stuck-undercutter loops.
                    # The streak is reset to 0 on any fill (handle_order_fill).
                    cancel_streak = self._sell_cancel_streak.get(self._get_canonical_asset(asset), 0)
                    if cancel_streak >= 5:
                        effective_frontrun_delay = MIN_FRONTRUN_CHECK_DELAY * 4.0  # 20s
                    else:
                        effective_frontrun_delay = MIN_FRONTRUN_CHECK_DELAY  # 5s

                    # Cancel if mode disabled (orphaned order)
                    if not self._sell_enabled:
                        should_cancel = True
                        cancel_reason = "mode disabled"

                    # UNIFIED IMMEDIATE CHECK: Better market + Undercut + Gap detection
                    # Pass order age so the function can apply per-check delay thresholds.
                    # Uses effective_frontrun_delay (backed off when streak >= 5).
                    if not should_cancel:
                        should_cancel, cancel_reason = self.c_check_immediate_conditions(
                            asset, False, current_time - last_time, effective_frontrun_delay)

                    # ---- SECOND-LEVEL REFUGE (sell): don't chase the penny-jumper ----
                    # When the immediate check says "undercut", decide between chasing (normal) and
                    # taking refuge under the wall. Two cases:
                    #   (a) ALREADY in refuge -> SUPPRESS the undercut (we intend to sit 2nd-best;
                    #       the jumper is below us by design). Hold our wall order.
                    #   (b) NOT in refuge yet, but the jumper has undercut us REFUGE_ARM_STREAK times
                    #       in a row AND a valid wall exists -> ENTER refuge: allow this cancel (so the
                    #       order can move UP), retag it "refuge" (proactive), and let placement put us
                    #       at wall-1tick. Subsequent cycles hit case (a).
                    # Refuge is cleared on any fill (handle_order_fill).
                    if (should_cancel and self._sell_spread_is_min
                            and cancel_reason.startswith("undercut")):
                        refuge_canonical = self._get_canonical_asset(asset)
                        if self._in_refuge_sell.get(refuge_canonical, False):
                            # (a) hold position — ignore the undercut entirely
                            should_cancel = False
                            cancel_reason = ""
                        elif cancel_streak >= REFUGE_ARM_STREAK:
                            # (b) enter refuge — move up under the wall on this cancel.
                            # If no real wall exists, placement reads the 2nd ask and falls back to
                            # normal top-of-book; a "wall must be >= N ticks above jumper" entry gate
                            # is a future refinement (see position-balancer.md).
                            self._in_refuge_sell[refuge_canonical] = True
                            cancel_reason = "refuge (taking 2nd-best under wall — stop chasing penny-jumper)"
                            self.strategy.logger().info(
                                f"Position balancer: SELL refuge ARMED for {refuge_canonical} "
                                f"(streak={cancel_streak}) — retreating under the wall, undercut now suppressed")

                    # Immediate-check cancels are reactive (market event) EXCEPT:
                    #   - "mode disabled": housekeeping
                    #   - "step-up …": we voluntarily reprice into a gap (no one beat us) — proactive,
                    #     so it takes the 10s cooldown and does NOT bump the undercut streak.
                    #   - "refuge …": deliberate move under the wall — proactive, no streak bump.
                    if (should_cancel and cancel_reason
                            and not cancel_reason.startswith("mode disabled")
                            and not cancel_reason.startswith("step-up")
                            and not cancel_reason.startswith("refuge")):
                        is_reactive = True

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
                        # Periodic refresh: c_check_immediate_conditions already handles undercut/gap
                        # (after 5s) and better-market (after 60s) every tick, so here we only run
                        # the checks that need a full orderbook snapshot and are not time-gated:
                        #   2b: 1-tick min-mode undercut (needs got_valid_second_level)
                        #   3:  ideal-price gap (market moved up → can sell higher)
                        #   4:  aggressive mode — market moved to our price
                        #   5:  significant price divergence (c_check_price_divergence)
                        # Unconditional fallback: always refresh after effective_interval regardless
                        # of OB conditions (handles exchanges with sparse/stale books where no
                        # condition ever fires, leaving orders stranded until force-cleanup at 2×timeout).
                        order_details = self._active_sell_order_details.get(asset)
                        if order_details is not None:
                            order_market_tuple, order_price = order_details
                            try:
                                current_ob = (<ExchangeBase>order_market_tuple.market).c_get_order_book(order_market_tuple.trading_pair)

                                current_bid, current_ask = self.c_get_orderbook_prices(current_ob)

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

                                # Calculate effective_ask (second level if top is OUR order)
                                effective_ask = self.c_get_effective_reference_price(
                                    current_ob, current_ask, order_price, min_price_increment, False)

                                # Track if we got a valid second level (not our own order)
                                got_valid_second_level = (effective_ask != current_ask)

                                # Calculate ideal order price
                                ideal_order_price = self.c_calculate_ideal_order_price(
                                    effective_ask, self._sell_spread_pct, self._sell_spread_is_min,
                                    min_price_increment, False)

                                # CONDITION 2b: 1-tick min-mode undercut/gap (needs got_valid_second_level)
                                # Complements CHECK 2 in c_check_immediate_conditions which only checks
                                # current_ask < order_price; this catches the subtler 1-tick misalignment.
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

                                # CONDITION 3: Market moved UP — can sell higher
                                # Skip in aggressive mode (0%): ideal_order_price = effective_ask,
                                # but the order is placed at top_bid — the spread gap between ask
                                # and bid is not a "can sell higher" signal. Condition 4 handles
                                # aggressive-mode repricing correctly.
                                if not should_cancel and not (self._sell_spread_pct == 0.0 and not self._sell_spread_is_min) and ideal_order_price > order_price:
                                    gap_up = ideal_order_price - order_price
                                    if self._sell_spread_is_min:
                                        cancel_threshold_abs = min_price_increment * TICK_TOLERANCE
                                        if got_valid_second_level and gap_up > cancel_threshold_abs:
                                            should_cancel = True
                                            cancel_reason = f"gap up {gap_up:.8f} > threshold {cancel_threshold_abs:.8f} (can sell higher)"
                                    else:
                                        cancel_threshold_pct = max(0.001, self._sell_spread_pct * 0.5)
                                        gap_pct = gap_up / order_price if order_price > 0 else 0.0
                                        if gap_pct > cancel_threshold_pct:
                                            should_cancel = True
                                            cancel_reason = f"gap up {gap_pct*100:.2f}% (can sell higher at {ideal_order_price:.8f} vs {order_price:.8f})"

                                # CONDITION 4: Aggressive mode — market moved to our price
                                if not should_cancel and self._sell_spread_pct == 0.0:
                                    if current_ask <= order_price * (2.0 - AGGRESSIVE_MODE_TOLERANCE):
                                        should_cancel = True
                                        cancel_reason = "market moved to our price (aggressive mode)"

                                # CONDITION 5: Significant price divergence
                                # Skip in aggressive mode (0%): ideal_order_price is based on
                                # effective_ask but order is at top_bid; the spread gap is not
                                # divergence. Condition 4 handles aggressive-mode repricing.
                                if not should_cancel and not (self._sell_spread_pct == 0.0 and not self._sell_spread_is_min):
                                    should_cancel, cancel_reason = self.c_check_price_divergence(
                                        order_price, ideal_order_price, self._sell_spread_is_min,
                                        min_price_increment, got_valid_second_level)

                            except Exception as e:
                                self.strategy.logger().warning(f"Position balancer: Error checking sell order conditions: {e}")
                                # On OB error, fall back to unconditional refresh so orders don't get stranded.
                                if not should_cancel:
                                    should_cancel = True
                                    cancel_reason = "periodic refresh (OB error)"

                        else:
                            # No order details — unknown state, refresh unconditionally.
                            should_cancel = True
                            cancel_reason = "periodic refresh (no order details)"

                    if should_cancel:
                        # is_reactive is True only for immediate-check (market-event) cancels
                        self._cancel_sell_order(asset, order_id, cancel_reason, reactive=is_reactive)

    cdef void _cancel_buy_order(self, str asset, str order_id, str reason, bint reactive=False):
        """
        Internal method to cancel a buy order with proper cleanup tracking.
        Ensures atomic cancellation with correct state management.

        Uses stored market_tuple for robust direct cancellation by order_id.

        Args:
            reactive: True if cancel is driven by a market event (undercut/frontrun/large-gap).
                      Uses POST_CANCEL_COOLDOWN_REACTIVE (3s) instead of PROACTIVE (10s).
        """
        cdef:
            tuple order_details
            object market_tuple
            str canonical
            double cooldown

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

            # Record cooldown to apply under canonical key so handle_order_cancellation
            # (alias-aware) picks it up correctly.
            canonical = self._get_canonical_asset(asset)
            # Step-up and refuge cancels are proactive (reactive=False) but get a SHORT cooldown
            # so we re-place quickly (into the gap / under the wall). For step-up the API-spam
            # throttle is the pre-detection STEP_UP_MIN_INTERVAL gate; for refuge it is a one-shot
            # transition (we then SUPPRESS undercut), so neither needs a long post-cancel wait.
            if reason.startswith("step-up") or reason.startswith("refuge"):
                cooldown = STEP_UP_COOLDOWN
            else:
                cooldown = POST_CANCEL_COOLDOWN_REACTIVE if reactive else POST_CANCEL_COOLDOWN_PROACTIVE
            self._last_buy_cancel_cooldown[canonical] = cooldown
            # Only increment streak for reactive (market-event) cancels.
            # Proactive refreshes (periodic, better-market, divergence, step-up) are scheduled
            # housekeeping — counting them would back off frontrun detection on thin
            # markets that refresh often but are rarely frontrun.
            if reactive:
                self._buy_cancel_streak[canonical] = self._buy_cancel_streak.get(canonical, 0) + 1

            self.strategy.logger().info(
                f"Position balancer: Cancelled buy order {order_id} for {asset} on {market_tuple.market.name} "
                f"({reason}) [cooldown={cooldown:.0f}s, streak={self._buy_cancel_streak.get(canonical, 0)}]")

            # NOTE: Don't remove from _active_buy_orders here!
            # Let handle_order_cancellation() clean it up when cancel event arrives.
            # This prevents tracking loss if cancel fails or is delayed.
        except Exception as e:
            self.strategy.logger().warning(f"Position balancer: Failed to cancel buy order {order_id}: {e}")
            # Clean up timeout marker if cancel failed
            self.strategy._timeout_cancelled_orders.discard(order_id)

    cdef void _cancel_sell_order(self, str asset, str order_id, str reason, bint reactive=False):
        """
        Internal method to cancel a sell order with proper cleanup tracking.
        Ensures atomic cancellation with correct state management.

        Uses stored market_tuple for robust direct cancellation by order_id.

        Args:
            reactive: True if cancel is driven by a market event (undercut/frontrun/large-gap).
                      Uses POST_CANCEL_COOLDOWN_REACTIVE (3s) instead of PROACTIVE (10s).
        """
        cdef:
            tuple order_details
            object market_tuple
            str canonical
            double cooldown

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

            # Record cooldown to apply under canonical key so handle_order_cancellation
            # (alias-aware) picks it up correctly.
            canonical = self._get_canonical_asset(asset)
            # Step-up and refuge cancels are proactive (reactive=False) but get a SHORT cooldown
            # so we re-place quickly (into the gap / under the wall). For step-up the API-spam
            # throttle is the pre-detection STEP_UP_MIN_INTERVAL gate; for refuge it is a one-shot
            # transition (we then SUPPRESS undercut), so neither needs a long post-cancel wait.
            if reason.startswith("step-up") or reason.startswith("refuge"):
                cooldown = STEP_UP_COOLDOWN
            else:
                cooldown = POST_CANCEL_COOLDOWN_REACTIVE if reactive else POST_CANCEL_COOLDOWN_PROACTIVE
            self._last_sell_cancel_cooldown[canonical] = cooldown
            # Only increment streak for reactive (market-event) cancels.
            # Proactive refreshes (periodic, better-market, divergence, step-up) are scheduled
            # housekeeping — counting them would back off undercut detection on thin
            # markets that refresh often but are rarely undercut.
            if reactive:
                self._sell_cancel_streak[canonical] = self._sell_cancel_streak.get(canonical, 0) + 1

            self.strategy.logger().info(
                f"Position balancer: Cancelled sell order {order_id} for {asset} on {market_tuple.market.name} "
                f"({reason}) [cooldown={cooldown:.0f}s, streak={self._sell_cancel_streak.get(canonical, 0)}]")

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
                # IMPORTANT: Orders with pending cancel are treated as STILL ACTIVE
                # until exchange confirms the cancel OR stuck cancel timeout is reached.
                # This prevents orphaned orders on exchanges with slow cancel confirmations.
                has_active_order = False
                for alias in asset_aliases:
                    if alias in self._active_buy_orders:
                        existing_order_id = self._active_buy_orders[alias]
                        # Check if this order has a pending cancel
                        if existing_order_id in self.strategy._timeout_cancelled_orders:
                            # Order has pending cancel - check if we've waited long enough
                            cancel_request_time = self._buy_cancel_request_time.get(alias, 0.0)
                            time_since_cancel = self.strategy._current_timestamp - cancel_request_time
                            
                            if time_since_cancel >= PENDING_CANCEL_WAIT_SECONDS:
                                # Force cleanup via stuck cancel mechanism after timeout
                                if self.c_check_stuck_cancel(existing_order_id, alias, True, self.strategy._current_timestamp, True):
                                    # Stuck cancel was cleaned up - continue to check other aliases
                                    continue
                            
                            # Cancel still pending, haven't waited long enough - block new order
                            self.strategy.logger().debug(
                                f"Position balancer: Buy order {existing_order_id} pending cancel "
                                f"({time_since_cancel:.1f}s/{PENDING_CANCEL_WAIT_SECONDS:.0f}s) - waiting for confirmation")
                            has_active_order = True
                            break
                        else:
                            has_active_order = True
                            break

                if not has_active_order:
                    # Post-fill cooldown: 2s base + adaptive fill-pressure extra (0..8s).
                    # The extra grows when consecutive fills arrive quickly (trailing-stop-like
                    # spacing); it's 0 after a single fill or once pressure cools. See c_record_fill_pressure.
                    time_since_completion = self.strategy._current_timestamp - self._last_buy_completion_time.get(canonical_asset, 0.0)
                    buy_extra_wait = self._post_fill_extra_wait.get(canonical_asset, 0.0)
                    if time_since_completion < DEFAULT_COMPLETION_COOLDOWN + buy_extra_wait:
                        # Log only when the adaptive extra is the binding constraint (base floor
                        # already cleared) — confirms the armed wait is actively spacing placement.
                        if buy_extra_wait > 0.0 and time_since_completion >= DEFAULT_COMPLETION_COOLDOWN:
                            self.strategy.logger().debug(
                                f"Position balancer: fill-pressure holding [{canonical_asset}] buy "
                                f"({time_since_completion:.1f}s/{DEFAULT_COMPLETION_COOLDOWN + buy_extra_wait:.1f}s)")
                    else:
                        # Check post-cancel cooldown — duration depends on why we cancelled:
                        #   reactive (undercut/frontrun/gap): POST_CANCEL_COOLDOWN_REACTIVE (3s)
                        #   proactive (refresh/better-market): POST_CANCEL_COOLDOWN_PROACTIVE (10s)
                        time_since_cancel = self.strategy._current_timestamp - self._last_buy_cancel_time.get(canonical_asset, 0.0)
                        required_cooldown = self._last_buy_cancel_cooldown.get(canonical_asset, POST_CANCEL_COOLDOWN_PROACTIVE)
                        if time_since_cancel >= required_cooldown:
                            # For completion check, use ACTUAL balance (not adjusted)
                            base_bal_actual = self.c_get_actual_base_balance(canonical_asset)
                            val_result_actual = self.c_compute_value_and_buy_shortfall(base_bal_actual, last_bid)
                            if not self.c_try_mark_buy_complete(canonical_asset, val_result_actual.first, val_result_actual.second):
                                # Find the best market to buy on (lowest ask)
                                selected_buy_market = self.c_find_best_buy_market(asset_key)
                                if selected_buy_market is not None:
                                    # Pre-check: ensure selected market has enough quote balance for min notional.
                                    # Subtract inflight buys (converted to quote) to handle stale balance cache.
                                    market_quote_bal = float(selected_buy_market.market.get_available_balance(
                                        selected_buy_market.quote_asset))
                                    pending_buy_quote = self.c_get_pending_buy_base(asset_key) * last_bid if last_bid > 0 else 0.0
                                    effective_quote_bal = max(0.0, market_quote_bal - pending_buy_quote)
                                    if effective_quote_bal >= self.strategy._min_order_usd:
                                        # For sell market, just use the other market from the pair
                                        # (not critical since we're only buying)
                                        selected_sell_market = sell_market_tuple
                                        placed = self.c_execute_buy_limit(selected_buy_market, selected_sell_market)
                                        if placed:
                                            return True
                                    else:
                                        pending_info = f", pending={pending_buy_quote:.2f}" if pending_buy_quote > 0 else ""
                                        self.strategy.logger().warning(
                                            f"Position balancer: {canonical_asset} buy skipped - "
                                            f"{selected_buy_market.market.name} insufficient quote "
                                            f"({effective_quote_bal:.2f}{pending_info} < min {self.strategy._min_order_usd:.2f})")

        # Check if we need to sell
        if self._sell_enabled and not self._sell_completed:
            val_result = self.c_compute_value_and_sell_excess(base_bal, last_bid)
            current_value = val_result.first
            shortfall_or_excess = val_result.second

            if shortfall_or_excess > 0:
                # Check if already have pending sell order for ANY alias
                # Only place new order if no aliases have active orders
                # IMPORTANT: Orders with pending cancel are treated as STILL ACTIVE
                # until exchange confirms the cancel OR stuck cancel timeout is reached.
                # This prevents orphaned orders on exchanges with slow cancel confirmations.
                has_active_order = False
                for alias in asset_aliases:
                    if alias in self._active_sell_orders:
                        existing_order_id = self._active_sell_orders[alias]
                        # Check if this order has a pending cancel
                        if existing_order_id in self.strategy._timeout_cancelled_orders:
                            # Order has pending cancel - check if we've waited long enough
                            cancel_request_time = self._sell_cancel_request_time.get(alias, 0.0)
                            time_since_cancel = self.strategy._current_timestamp - cancel_request_time
                            
                            if time_since_cancel >= PENDING_CANCEL_WAIT_SECONDS:
                                # Force cleanup via stuck cancel mechanism after timeout
                                if self.c_check_stuck_cancel(existing_order_id, alias, False, self.strategy._current_timestamp, True):
                                    # Stuck cancel was cleaned up - continue to check other aliases
                                    continue
                            
                            # Cancel still pending, haven't waited long enough - block new order
                            self.strategy.logger().debug(
                                f"Position balancer: Sell order {existing_order_id} pending cancel "
                                f"({time_since_cancel:.1f}s/{PENDING_CANCEL_WAIT_SECONDS:.0f}s) - waiting for confirmation")
                            has_active_order = True
                            break
                        else:
                            has_active_order = True
                            break

                if not has_active_order:
                    # Post-fill cooldown: 2s base + adaptive fill-pressure extra (0..8s).
                    # The extra grows when consecutive fills arrive quickly (trailing-stop-like
                    # spacing); it's 0 after a single fill or once pressure cools. See c_record_fill_pressure.
                    time_since_completion = self.strategy._current_timestamp - self._last_sell_completion_time.get(canonical_asset, 0.0)
                    sell_extra_wait = self._post_fill_extra_wait.get(canonical_asset, 0.0)
                    if time_since_completion < DEFAULT_COMPLETION_COOLDOWN + sell_extra_wait:
                        # Log only when the adaptive extra is the binding constraint (base floor
                        # already cleared) — confirms the armed wait is actively spacing placement.
                        if sell_extra_wait > 0.0 and time_since_completion >= DEFAULT_COMPLETION_COOLDOWN:
                            self.strategy.logger().debug(
                                f"Position balancer: fill-pressure holding [{canonical_asset}] sell "
                                f"({time_since_completion:.1f}s/{DEFAULT_COMPLETION_COOLDOWN + sell_extra_wait:.1f}s)")
                    else:
                        # Check post-cancel cooldown — duration depends on why we cancelled:
                        #   reactive (undercut/frontrun/gap): POST_CANCEL_COOLDOWN_REACTIVE (3s)
                        #   proactive (refresh/better-market): POST_CANCEL_COOLDOWN_PROACTIVE (10s)
                        time_since_cancel = self.strategy._current_timestamp - self._last_sell_cancel_time.get(canonical_asset, 0.0)
                        required_cooldown = self._last_sell_cancel_cooldown.get(canonical_asset, POST_CANCEL_COOLDOWN_PROACTIVE)
                        if time_since_cancel >= required_cooldown:
                            # Check insufficient-balance cooldown — suppresses retry spam when the best sell
                            # market has insufficient base balance. Only applies for non-zero target; sell-to-zero
                            # always proceeds so c_execute_sell_limit can handle dust specially.
                            insuf_bal_ok = True
                            if self._sell_target_usd > 0.0:
                                time_since_insuf_bal = self.strategy._current_timestamp - self._last_sell_insuf_bal_time.get(canonical_asset, 0.0)
                                if time_since_insuf_bal < INSUF_BAL_RETRY_COOLDOWN:
                                    insuf_bal_ok = False
                            if insuf_bal_ok:
                                # For completion check, use ACTUAL balance (not adjusted)
                                base_bal_actual = self.c_get_actual_base_balance(canonical_asset)
                                val_result_actual = self.c_compute_value_and_sell_excess(base_bal_actual, last_bid)
                                if not self.c_try_mark_sell_complete(canonical_asset, val_result_actual.first, val_result_actual.second):
                                    # Find the best market to sell on (highest bid)
                                    selected_sell_market = self.c_find_best_sell_market(asset_key)
                                    if selected_sell_market is not None:
                                        # Pre-check: ensure selected market has enough base balance for min notional.
                                        # Bypass for sell-to-zero: the full balance IS the order — let c_execute_sell_limit handle it.
                                        market_base_bal = float(selected_sell_market.market.get_available_balance(
                                            selected_sell_market.base_asset))
                                        # Subtract any inflight sell orders from the cached balance.
                                        # Exchange balance caches can lag by several seconds after order placement,
                                        # so an order placed moments ago may not yet appear as "frozen".
                                        # _pending_sell_by_asset tracks what we've already committed this session.
                                        pending_sell_base = self.c_get_pending_sell_base(asset_key)
                                        effective_base_bal = max(0.0, market_base_bal - pending_sell_base)
                                        if effective_base_bal * last_bid < self.strategy._min_order_usd and self._sell_target_usd > 0.0:
                                            # Best sell market has insufficient base balance — record timestamp so we
                                            # don't retry every 2s tick. Will recheck after INSUF_BAL_RETRY_COOLDOWN.
                                            self._last_sell_insuf_bal_time[canonical_asset] = self.strategy._current_timestamp
                                            pending_info = f", pending={pending_sell_base:.4f}" if pending_sell_base > 0 else ""
                                            self.strategy.logger().warning(
                                                f"Position balancer: {canonical_asset} sell skipped - "
                                                f"{selected_sell_market.market.name} insufficient balance "
                                                f"({effective_base_bal:.4f} {selected_sell_market.base_asset}"
                                                f"{pending_info} = ${effective_base_bal * last_bid:.2f} < min ${self.strategy._min_order_usd:.2f}), "
                                                f"retry in {INSUF_BAL_RETRY_COOLDOWN:.0f}s")
                                        else:
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
                            # If top bid matches our existing order (within tolerance), use second bid.
                            # NOTE: bid_entries() is a GENERATOR (yields OrderBookRow) — NOT subscriptable
                            # and no len(). Previous code used len()/[1] which always threw and silently
                            # fell back to top_bid (self-competing). Pull the second row via next().
                            if abs(top_bid - existing_price) < min_price_increment * 0.5:
                                try:
                                    _bid_iter = ob.bid_entries()
                                    _ = next(_bid_iter, None)              # level 0 (our own top)
                                    _bid_lvl1 = next(_bid_iter, None)      # level 1 (next foreign)
                                    if _bid_lvl1 is not None:
                                        reference_bid = float(_bid_lvl1.price)
                                        self.strategy.logger().debug(
                                            f"Position balancer: Top bid {top_bid:.8f} matches our existing order, "
                                            f"using second bid {reference_bid:.8f} for new order price")
                                except Exception as e:
                                    self.strategy.logger().debug(f"Could not get second bid level: {e}")

                    # SECOND-LEVEL REFUGE (buy mirror): rest under the WALL (2nd bid) — below the
                    # frontrunner — instead of re-overbidding up. Cancel has confirmed, so the book
                    # is [frontrunner@L1, wall@L2, …] and the wall is simply the 2nd bid.
                    if self._in_refuge_buy.get(self._get_canonical_asset(asset_key), False):
                        try:
                            _it = ob.bid_entries()
                            next(_it, None)               # L1 = frontrunner
                            _wall = next(_it, None)       # L2 = wall
                            if _wall is not None:
                                reference_bid = float(_wall.price)
                                self.strategy.logger().info(
                                    f"Position balancer: BUY refuge placement for {asset_key} — "
                                    f"resting above wall {reference_bid:.8f}")
                        except Exception as e:
                            self.strategy.logger().debug(f"Refuge wall read failed (buy): {e}")

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

        # Calculate amount based on shortfall, available quote, and order size limit.
        # Use ADJUSTED shortfall to account for pending orders and avoid over-ordering.
        # Also subtract inflight buy cost from quote_bal to handle stale balance cache.
        pending_buy_base = self.c_get_pending_buy_base(asset_key)
        pending_buy_quote = pending_buy_base * buy_price if buy_price > 0 else 0.0
        effective_quote_bal = max(0.0, quote_bal - pending_buy_quote)
        max_affordable_base = effective_quote_bal / buy_price if buy_price > 0 else 0.0
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

        # Place limit buy order (no expiration_seconds — limit orders are managed by the
        # balancer's own cancel/replace logic; passing _next_trade_delay here would set a
        # 2s expiration_ts that most connectors ignore but could cause issues on any that honour it)
        try:
            buy_order_id = self.strategy.c_buy_with_specific_market(
                buy_market_tuple,
                quantized_amount,
                order_type=order_type,
                price=Decimal(str(buy_price)))
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
                f"at {buy_price:.10f} (spread: min tick)")
        else:
            self.strategy.logger().info(
                f"Placed buy limit order {buy_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {buy_price:.10f} (spread: {self._buy_spread_pct * 100:.2f}%)")

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
                            # If top ask matches our existing order (within tolerance), use second ask.
                            # NOTE: ask_entries() is a GENERATOR (yields OrderBookRow) — NOT subscriptable
                            # and no len(). Previous code used len()/[1] which always threw and silently
                            # fell back to top_ask (self-competing). Pull the second row via next().
                            if abs(top_ask - existing_price) < min_price_increment * 0.5:
                                try:
                                    _ask_iter = ob.ask_entries()
                                    _ = next(_ask_iter, None)              # level 0 (our own top)
                                    _ask_lvl1 = next(_ask_iter, None)      # level 1 (next foreign)
                                    if _ask_lvl1 is not None:
                                        reference_ask = float(_ask_lvl1.price)
                                        self.strategy.logger().debug(
                                            f"Position balancer: Top ask {top_ask:.8f} matches our existing order, "
                                            f"using second ask {reference_ask:.8f} for new order price")
                                except Exception as e:
                                    self.strategy.logger().debug(f"Could not get second ask level: {e}")

                    # SECOND-LEVEL REFUGE: if active, rest under the WALL (2nd ask) instead of
                    # re-undercutting down. Our cancel has confirmed by now (3s cooldown) so the
                    # book is [jumper@L1, wall@L2, …] — the wall is simply the 2nd ask. (If our
                    # stale order were still top, the self-replace block above already moved
                    # reference_ask to L2, so this stays correct.)
                    if self._in_refuge_sell.get(self._get_canonical_asset(asset_key), False):
                        try:
                            _it = ob.ask_entries()
                            next(_it, None)               # L1 = jumper
                            _wall = next(_it, None)       # L2 = wall
                            if _wall is not None:
                                reference_ask = float(_wall.price)
                                self.strategy.logger().info(
                                    f"Position balancer: SELL refuge placement for {asset_key} — "
                                    f"resting under wall {reference_ask:.8f}")
                        except Exception as e:
                            self.strategy.logger().debug(f"Refuge wall read failed (sell): {e}")

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

        # Calculate amount based on excess, available base, and order size limit.
        # Use ADJUSTED excess to account for pending orders and avoid over-ordering.
        # Also cap against (base_bal_raw - pending) so that a stale exchange balance cache
        # (which may not yet reflect recently placed orders) cannot cause over-ordering.
        max_order_base = self._order_size_usd / sell_price if sell_price > 0 else 0.0
        pending_sell_base = self.c_get_pending_sell_base(asset_key)
        effective_raw = max(0.0, base_bal_raw - pending_sell_base)
        amount_to_sell = min(
            excess_adjusted / last_bid if last_bid > 0 else 0.0,
            effective_raw,
            max_order_base
        )

        if amount_to_sell <= EPSILON:
            self.strategy.logger().warning(
                f"Position balancer: Sell order blocked - amount too small. "
                f"amount_to_sell={amount_to_sell:.10f}, excess_adjusted={excess_adjusted:.6f}, "
                f"last_bid={last_bid:.8f}, base_bal_raw={base_bal_raw:.8f}, "
                f"pending_sell={pending_sell_base:.8f}, effective_raw={effective_raw:.8f}, "
                f"max_order_base={max_order_base:.6f}, sell_price={sell_price:.8f}")
            return False

        # Quantize
        quantized_amount = self.strategy.c_safe_quantize_order_amount(
            market, sell_market_tuple.trading_pair,
            Decimal(str(max(0.0, amount_to_sell - QUANTIZATION_EPSILON))),
            Decimal(str(sell_price)))

        if quantized_amount <= Decimal("0"):
            self.strategy.logger().warning(
                f"Position balancer: Sell order blocked - quantized to zero. "
                f"{sell_market_tuple.base_asset} on {market.name} "
                f"pre_quantize={amount_to_sell:.10f}, quantized={quantized_amount}")
            # For sell-to-zero: dust below the exchange lot size cannot be sold via any order.
            # Declare completion so the balancer doesn't spin forever on unsellable residue.
            if self._sell_target_usd == 0.0:
                self.c_try_mark_sell_complete(asset_key, current_value_quote, excess)
                self.c_maybe_disable_sell()
            return False

        # Apply max_order_size cap
        try:
            trading_rule = market._trading_rules.get(sell_market_tuple.trading_pair)
            if trading_rule is not None and trading_rule.max_order_size > Decimal("0"):
                if quantized_amount > trading_rule.max_order_size:
                    quantized_amount = trading_rule.max_order_size
        except Exception:
            pass

        # Check minimum notional.
        # Sell-to-zero exception: when target=0 and the full remaining balance is below the software
        # floor, send the order anyway — the exchange min notional (~$5–$10) is well below our $15
        # floor, so most exchanges will accept it. If an exchange rejects it, the error is caught in
        # c_sell_with_specific_market and we retry next tick. Completion never triggers for target=0
        # while balance remains > 0, so infinite retries are not possible after a real fill.
        # For any non-zero target, block as usual and mark complete.
        volume_usd = float(quantized_amount) * sell_price
        if volume_usd < self.strategy._min_order_usd:
            if self._sell_target_usd == 0.0:
                self.strategy.logger().info(
                    f"Position balancer: Sell-to-zero dust sweep for {asset_key} - "
                    f"sending {float(quantized_amount):.6f} (${volume_usd:.2f}) below software floor")
            else:
                self.strategy.logger().warning(
                    f"Position balancer: Sell order blocked - below min notional. "
                    f"volume_usd={volume_usd:.6f} < min_order_usd={self.strategy._min_order_usd:.6f}, "
                    f"quantized_amount={quantized_amount}, sell_price={sell_price:.8f}")
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

        # Place limit sell order (no expiration_seconds — same reasoning as buy above)
        try:
            sell_order_id = self.strategy.c_sell_with_specific_market(
                sell_market_tuple,
                quantized_amount,
                order_type=order_type,
                price=Decimal(str(sell_price)))
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
                f"at {sell_price:.10f} (spread: min tick)")
        else:
            self.strategy.logger().info(
                f"Placed sell limit order {sell_order_id} for {float(quantized_amount):.6f} {asset_key} "
                f"at {sell_price:.10f} (spread: {self._sell_spread_pct * 100:.2f}%)")

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
            # Fresh session must start in NORMAL mode (chase best bid), not stale refuge.
            self._in_refuge_buy.clear()
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
            # Fresh session must start in NORMAL mode (chase best ask), not stale refuge.
            # Refuge is only re-armed by a fresh streak of undercuts. (Unlike the streak/
            # completion dicts, a stale refuge flag would change behaviour — it would skip
            # L1-chasing and start passive — so we clear it explicitly here.)
            self._in_refuge_sell.clear()
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