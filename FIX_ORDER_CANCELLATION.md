# Fix: Market Orders With Partial Fills Being Cancelled

## Problem Statement

Market orders with partial fills are being cancelled incorrectly. The strategy uses `order.is_filled` which requires 100% fill, but for market orders on illiquid exchanges (like BingX), ANY fills should be treated as complete.

**Example from logs:**
- Order: 0.879 BULL
- Filled: 0.597324 BULL (68%)
- Result: ❌ CANCELLED (should be ✅ TREATED AS COMPLETE)

## Root Cause

Strategy checks `if not order.is_filled` which returns False for partial fills.

**Current buggy code:**
```python
if order is None or not order.is_filled:
    self.cancel(order_id)
    self.logger().warning(f"Order {order_id} was CANCELLED - cooldown enforced")
```

## Solution

For market orders: treat ANY fills as complete. Only cancel orders with ZERO fills.

**Fixed code:**
```python
if order is None or order.executed_amount_base == Decimal("0"):
    self.cancel(order_id)
    self.logger().warning(f"Order {order_id} was CANCELLED - cooldown enforced")
else:
    # Order has fills, treat as complete
    self.logger().info(
        f"Order {order_id} partially filled "
        f"({order.executed_amount_base}/{order.amount}), treating as complete"
    )
```

## Why This Works

- ✅ Orders with ANY fills: treated as complete (don't cancel)
- ✅ Orders with ZERO fills: cancelled (prevents spam arbitrage)
- ✅ Cooldown only applies to truly cancelled orders
- ✅ No false cancellations of partially filled orders

## Implementation

See `PULL_REQUEST.md` for the actual code changes.
