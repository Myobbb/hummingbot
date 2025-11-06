# Pull Request: Fix Market Order Cancellation Logic

## Title
Fix: Prevent cancellation of market orders with partial fills

## Description

### Problem
The arbitrage_m strategy incorrectly cancels market orders that have partial fills. On illiquid exchanges like BingX, market orders may not fill completely, but orders with ANY fills should be treated as complete to avoid:
- Unhedged positions from cancelled partially-filled orders
- False triggering of cooldown logic
- Loss of valid arbitrage opportunities

### Example Issue
```
Order placed: 0.879 BULL
Actual fills: 0.597324 BULL (68%)
Current behavior: ❌ Order CANCELLED (wrong)
Expected behavior: ✅ Order treated as COMPLETE (correct)
```

### Root Cause
Strategy checks `order.is_filled` (requires 100% fill) instead of checking for ANY fills.

### Solution
Change cancellation logic to only cancel orders with **ZERO** fills.

## Changes

### Option 1: Strategy-Level Fix (Recommended)

**File:** `hummingbot/strategy/arbitrage_m/arbitrage.py`

```python
# BEFORE (buggy)
async def monitor_order_timeout(self, order_id, connector_name):
    await asyncio.sleep(0.3)
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    if order is None or not order.is_filled:  # ← BUG: cancels partial fills
        self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")

# AFTER (fixed)
async def monitor_order_timeout(self, order_id, connector_name):
    await asyncio.sleep(0.3)
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    # Only cancel if order has NO fills at all
    if order is None or order.executed_amount_base == Decimal("0"):
        if order:
            self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")
    else:
        # Order has fills, treat as complete (don't cancel)
        self.logger().info(
            f"Order {order_id} on {connector_name} partially filled: "
            f"{order.executed_amount_base}/{order.amount} "
            f"({order.executed_amount_base/order.amount*100:.1f}%), treating as complete"
        )
```

### Option 2: Helper Property (Alternative)

**File:** `hummingbot/core/data_type/in_flight_order.py`

Add a helper property after the `is_filled` property (line 204):

```python
@property
def is_filled(self) -> bool:
    return (
        self.current_state == OrderState.FILLED
        or (self.amount != s_decimal_0
            and (math.isclose(self.executed_amount_base, self.amount)
                 or self.executed_amount_base >= self.amount)
            )
    )

@property
def has_any_fills(self) -> bool:
    """
    Returns True if the order has any fills at all, regardless of completion status.
    Useful for market orders where partial fills should be treated as complete.
    """
    return self.executed_amount_base > s_decimal_0
```

Then update strategy to use:
```python
if order is None or not order.has_any_fills:
    self.cancel(order_id)
```

## Testing

### Test Case 1: Order with partial fills
```python
order = InFlightOrder(...)
order.executed_amount_base = Decimal("0.597324")
order.amount = Decimal("0.879")

# OLD: order.is_filled → False → ORDER CANCELLED ❌
# NEW: order.executed_amount_base > 0 → True → ORDER KEPT ✅
```

### Test Case 2: Order with zero fills
```python
order = InFlightOrder(...)
order.executed_amount_base = Decimal("0")
order.amount = Decimal("0.879")

# Both OLD and NEW: ORDER CANCELLED ✅
```

### Test Case 3: Order fully filled
```python
order = InFlightOrder(...)
order.executed_amount_base = Decimal("0.879")
order.amount = Decimal("0.879")

# Both OLD and NEW: ORDER KEPT ✅
```

## Impact

- ✅ Fixes false cancellations of partially filled market orders
- ✅ Prevents unhedged positions from cancelled orders with fills
- ✅ Cooldown only triggers for orders with zero fills (actual failures)
- ✅ No impact on fully-filled orders (behavior unchanged)
- ✅ No impact on limit orders (if using Option 1)

## Checklist

- [ ] Code changes implemented
- [ ] Tested with BingX connector
- [ ] Tested with market orders on illiquid pairs
- [ ] Verified cooldown logic still prevents spam
- [ ] Confirmed no regression for fully-filled orders
- [ ] Updated logging to show partial fill information

## Related Issues

- Addresses underfill handling for market orders
- Improves arbitrage strategy reliability on illiquid exchanges
- Prevents false cooldown triggers
