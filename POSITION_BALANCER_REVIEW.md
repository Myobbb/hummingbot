# Position Balancer Handler - Comprehensive Review

## Overview
The position balancer manages buy-in (acquiring assets) and sell-off (reducing assets) operations for the ArbitrageL strategy using limit orders with configurable refresh intervals.

---

## 1. What Happens On Every Tick

### Entry Point
`c_handle_position_balancing()` is called from main strategy in 3 paths:
- **Path 1**: When profitable arbitrage found (position balancer runs first)
- **Path 2**: When markets found but no profitable arbitrage
- **Path 3**: When no markets found (scans all pairs proactively)

### Per-Tick Logic Flow

```
c_handle_position_balancing()
├── Check safeguards
│   ├── ❌ Skip if already executed this timestamp (_last_global_trade_timestamp)
│   └── ❌ Skip if both buy and sell disabled
│
├── Get current state
│   ├── Get asset key from market tuple
│   ├── Get reference bid price
│   └── Get adjusted balance (includes pending orders)
│
├── Cancel stale orders (c_cancel_stale_orders)
│   ├── Check if buy order > refresh_interval old → cancel & mark for re-place
│   └── Check if sell order > refresh_interval old → cancel & mark for re-place
│
├── Buy-in logic (if enabled and not completed)
│   ├── Calculate shortfall = target - current_value
│   ├── Skip if already have active buy order
│   ├── Check if should mark complete (target reached or < min_notional)
│   ├── Find best buy market (LOWEST ASK)
│   └── Execute buy limit order
│
└── Sell-off logic (if enabled and not completed)
    ├── Calculate excess = current_value - target
    ├── Skip if already have active sell order
    ├── Check if should mark complete (target reached or < min_notional)
    ├── Find best sell market (HIGHEST BID)
    └── Execute sell limit order
```

### Safeguards
✅ **Double execution prevention**: Checks `_last_global_trade_timestamp`
✅ **One order per asset**: Only places if no `_active_buy_orders[asset]` or `_active_sell_orders[asset]`
✅ **Stale order management**: Cancels orders older than `refresh_interval`

---

## 2. Position Balancer Refresh Interval

### Timer Behavior
The `position_balancer_refresh_interval` (e.g., 600.0 = 10 minutes) controls when orders are refreshed.

```python
# In c_cancel_stale_orders()
if current_time - last_order_time > self._limit_refresh_interval:
    # Cancel existing order
    # Mark as timeout-cancelled (no cooldown)
    # Remove from position_balancer_orders tracking
    # Next tick will place new order with updated market/price
```

### What Happens When Timer Expires

1. **Order Cancellation**
   - Old order is cancelled
   - Marked in `_timeout_cancelled_orders` (prevents cooldown)
   - Removed from `_position_balancer_orders` (allows main strategy timeout check)
   - Removed from `_active_buy_orders` or `_active_sell_orders`

2. **Next Tick**
   - No active order for asset
   - `c_find_best_buy_market()` or `c_find_best_sell_market()` runs
   - New order placed with current market conditions

### Purpose
- **Adapt to market changes**: Switch markets if prices shift
- **Update pricing**: Adjust limit price based on current orderbook
- **Prevent stale orders**: Ensure orders reflect current conditions

---

## 3. Target Reached Handling

### Completion Check Points

**Checked at 3 points:**
1. **Before placing order** (`c_try_mark_buy_complete`/`c_try_mark_sell_complete`)
2. **After placing order** (immediate re-check with updated pending amounts)
3. **On startup** (`c_scan_and_mark_completion`)

### Buy-In Completion Logic

```python
def c_try_mark_buy_complete():
    # Case 1: Target reached
    if current_value >= buy_target_usd:
        _buy_completed = True
        _buy_enabled = False  # Disable further attempts
        return True

    # Case 2: Remaining < min_notional (can't place more orders)
    if 0 < shortfall < strategy._min_order_usd:
        _buy_completed = True
        log("Buy-in considered complete: shortfall < min notional")
        _buy_enabled = False
        return True
```

### Sell-Off Completion Logic

```python
def c_try_mark_sell_complete():
    # Case 1: Target reached
    if current_value <= sell_target_usd:
        _sell_completed = True
        _sell_enabled = False
        return True

    # Case 2: Remaining < min_notional
    if 0 < excess < strategy._min_order_usd:
        _sell_completed = True
        log("Sell-off considered complete: excess < min notional")
        _sell_enabled = False
        return True
```

### After Completion

✅ No more orders placed (checked at top of `c_handle_position_balancing`)
✅ Status shows as "completed" in logs
✅ Module becomes dormant but still tracks existing orders
✅ Existing orders continue to be managed (cancellation, completion)

---

## 4. Partially Filled Orders

### Current Behavior

❌ **ISSUE IDENTIFIED**: Position balancer does NOT track partial fills properly

**What happens:**
1. Order placed for 4301.07 PHL
2. Partial fill: 2000 PHL filled, 2301.07 PHL remaining
3. Order completes (fully or cancelled)
4. `handle_order_completion()` subtracts FULL original amount (4301.07)
5. Balance accounting becomes incorrect

### Problem Code

```python
# position_balancer_handler.pyx:333-364
def handle_order_completion(self, str order_id, bint is_buy):
    if is_buy:
        pend = self._pending_buy_orders.pop(order_id, None)
        if pend is not None:
            asset_key, amt = pend
            # ❌ PROBLEM: Subtracts FULL amount, not filled amount
            self._pending_buy_by_asset[asset_key] -= float(amt)
```

### Impact

- **Adjusted balance calculation incorrect** after partial fills
- **May prevent placing new orders** (thinks more pending than reality)
- **May allow over-ordering** (if partial fills aren't subtracted from pending)

### Recommended Fix

```python
# Need to track actual filled amount per order
self._pending_buy_orders[order_id] = (asset_key, float(quantized_amount), 0.0)  # Add filled_amount

# On fill event (need to hook into c_did_fill_order)
def handle_order_fill(self, order_id, filled_amount):
    if order_id in self._pending_buy_orders:
        asset, total, filled = self._pending_buy_orders[order_id]
        self._pending_buy_orders[order_id] = (asset, total, filled + filled_amount)

# On completion, subtract only unfilled amount
unfilled = total_amount - filled_amount
self._pending_buy_by_asset[asset_key] -= unfilled
```

---

## 5. Hanging Orders

### Definition
Orders that remain open indefinitely without being filled or cancelled.

### Prevention Mechanisms

✅ **Refresh Interval**: Orders cancelled after `position_balancer_refresh_interval`
✅ **Main Strategy Timeout**: Orders cancelled after 180s (but position balancer orders are EXEMPT)
✅ **Filled Order Timeout**: Partially filled orders cancelled after `filled_order_timeout` (default 3600s)

### Current Protection

```python
# In c_cancel_stale_orders()
# Orders refreshed every position_balancer_refresh_interval
if current_time - last_order_time > self._limit_refresh_interval:
    cancel_order()
```

**Status**: ✅ Adequate protection against hanging orders

### Edge Case: Position Balancer Disabled Mid-Flight

**Scenario:**
1. Position balancer places order
2. Target reached (buy-in completes)
3. Order still open but position balancer is now `_buy_enabled = False`
4. Order will NOT be refreshed (refresh only runs if enabled)

**Current Behavior:**
- Order stays in `_position_balancer_orders` set
- Main strategy timeout check SKIPS it
- Order could hang indefinitely

**Recommended Fix:**
```python
# In c_cancel_stale_orders(), also check if mode is disabled
if not self._buy_enabled and asset in self._active_buy_orders:
    # Cancel orphaned order
    cancel_and_cleanup()
```

---

## 6. Cleanup After Buy-In/Sell-Off Complete

### What Should Happen

When buy-in or sell-off completes:
1. ✅ Set `_buy_completed = True` or `_sell_completed = True`
2. ✅ Set `_buy_enabled = False` or `_sell_enabled = False`
3. ✅ Stop placing new orders
4. ⚠️ Clean up any remaining open orders
5. ⚠️ Clear pending order tracking

### Current Behavior

```python
def c_maybe_disable_buy():
    if self._buy_completed and self._buy_enabled:
        self._buy_enabled = False
        log("Disabling buy-in for this session")
        # ❌ Does NOT cancel open orders
        # ❌ Does NOT clean up _active_buy_orders
```

### Issues Identified

❌ **Orphaned orders**: When buy-in completes, existing open orders are NOT cancelled
❌ **Stale tracking**: `_active_buy_orders` not cleared when completed
❌ **Pending amounts**: `_pending_buy_by_asset` not cleaned up

### Recommended Cleanup on Completion

```python
def c_maybe_disable_buy():
    if self._buy_completed and self._buy_enabled:
        self._buy_enabled = False

        # Clean up all active buy orders
        for asset in list(self._active_buy_orders.keys()):
            order_id = self._active_buy_orders.get(asset)
            if order_id:
                # Cancel the order
                for mp in self.strategy._market_pairs:
                    if mp.first.base_asset == asset:
                        self.strategy._timeout_cancelled_orders.add(order_id)
                        self.strategy._position_balancer_orders.discard(order_id)
                        self.strategy.c_cancel_order(mp.first, order_id)
                        break

        # Clear tracking
        self._active_buy_orders.clear()
        self._pending_buy_orders.clear()
        self._pending_buy_by_asset.clear()
        self._last_buy_order_time.clear()
```

---

## 7. Order Lifecycle Integration

### Order Placement
✅ Tracked in:
- `_active_buy_orders[asset] = order_id`
- `_pending_buy_orders[order_id] = (asset, amount)`
- `_pending_buy_by_asset[asset] += amount`
- `strategy._position_balancer_orders.add(order_id)`

### Order Fill (Partial)
❌ **NOT HOOKED**: Position balancer doesn't receive fill events
- Main strategy tracks fills in `_orders_with_fills`
- Position balancer needs `handle_order_fill()` method

### Order Completion
✅ Called from `arbitrage.pyx:749`
```python
self._position_balancer.handle_order_completion(order_id, is_buy)
```

### Order Cancellation
✅ Called from `arbitrage.pyx:822`
```python
self._position_balancer.handle_order_cancellation(order_id)
```

### Order Timeout
✅ Called from `arbitrage.pyx:935, 1018`
```python
self._position_balancer.handle_order_timeout(order_id)
```

---

## 8. Summary of Issues

### Critical Issues

1. **❌ Partial Fill Accounting**
   - Pending amounts not adjusted on partial fills
   - Can cause balance miscalculations
   - **Fix**: Hook into fill events, track filled amounts

2. **❌ Orphaned Orders on Completion**
   - Open orders not cancelled when buy-in/sell-off completes
   - Orders can hang indefinitely
   - **Fix**: Cancel all active orders in `c_maybe_disable_buy/sell()`

### Minor Issues

3. **⚠️ No Cleanup After Completion**
   - Tracking dicts not cleared when mode disabled
   - Memory leak over long sessions
   - **Fix**: Clear all tracking dicts in `c_maybe_disable_buy/sell()`

4. **⚠️ Disabled Mode Hanging Orders**
   - If mode disabled while order open, order never refreshed
   - Currently protected by main timeout but edge case exists
   - **Fix**: Check disabled mode in `c_cancel_stale_orders()`

### Design Strengths

✅ Proper market selection (lowest ask for buy, highest bid for sell)
✅ Refresh interval prevents stale orders
✅ Timeout protection from main strategy
✅ Double execution prevention
✅ Clean separation of buy/sell tracking
✅ Proper integration with main strategy lifecycle

---

## 9. Recommended Enhancements

### 1. Add Fill Event Handler

```python
def handle_order_fill(self, str order_id, double filled_amount):
    """Update pending amounts based on actual fills"""
    if order_id in self._pending_buy_orders:
        asset, total, prev_filled = self._pending_buy_orders[order_id]
        new_filled = prev_filled + filled_amount
        self._pending_buy_orders[order_id] = (asset, total, new_filled)
        # Adjust pending amount
        self._pending_buy_by_asset[asset] -= filled_amount
```

### 2. Clean Completion Handler

```python
def c_maybe_disable_buy(self):
    if self._buy_completed and self._buy_enabled:
        self._buy_enabled = False

        # Cancel all active buy orders
        self._cancel_all_active_buy_orders()

        # Clear all tracking
        self._active_buy_orders.clear()
        self._pending_buy_orders.clear()
        self._pending_buy_by_asset.clear()
        self._last_buy_order_time.clear()

        self.strategy.log_with_clock(
            logging.INFO,
            "Buy-in completed and cleaned up - all tracking cleared")
```

### 3. Orphaned Order Detection

```python
def c_cancel_stale_orders(self, str asset):
    # Existing refresh logic...

    # Also check if mode disabled but order still active
    if not self._buy_enabled and asset in self._active_buy_orders:
        order_id = self._active_buy_orders.get(asset)
        self._cancel_order_and_cleanup(order_id, asset, is_buy=True)
        self.strategy.logger().info(
            f"Cancelled orphaned buy order {order_id} (mode disabled)")
```

---

## 10. Testing Recommendations

### Test Case 1: Partial Fill
1. Place buy order for 4301 PHL
2. Simulate partial fill (2000 PHL)
3. Verify pending amount decreases by 2000
4. Cancel remaining order
5. Verify pending cleared correctly

### Test Case 2: Completion with Open Orders
1. Set target close to current holdings
2. Place order that will exceed target
3. Partial fill pushes over target
4. Verify completion triggered
5. Verify remaining order cancelled

### Test Case 3: Refresh Interval
1. Place buy order
2. Wait for refresh_interval
3. Verify order cancelled
4. Verify new order placed with updated price
5. Verify only one order active

### Test Case 4: Mode Disabled Mid-Flight
1. Place buy order
2. Manually trigger completion
3. Verify mode disabled
4. Verify open order handled
5. Verify tracking cleared

---

## Conclusion

**Overall Assessment**: The position balancer has a solid architecture with good safeguards against common issues. However, there are critical gaps in partial fill handling and completion cleanup that should be addressed for production use.

**Priority Fixes**:
1. 🔴 HIGH: Partial fill accounting
2. 🔴 HIGH: Cancel orders on completion
3. 🟡 MEDIUM: Clear tracking on completion
4. 🟡 MEDIUM: Handle orphaned orders when disabled
