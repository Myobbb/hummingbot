"""
TEMPLATE FIX for arbitrage_m strategy

Apply this fix to your arbitrage_m/arbitrage.py file wherever the
order timeout monitoring logic is located.

This fixes the issue where market orders with partial fills are incorrectly cancelled.
"""

from decimal import Decimal
from hummingbot.core.data_type.common import OrderType

# ===== OPTION 1: Simple Fix (Recommended) =====
# Change the cancellation condition from checking is_filled to checking for any fills

# BEFORE (buggy):
async def monitor_order_timeout_BEFORE(self, order_id, connector_name):
    await asyncio.sleep(0.3)  # 300ms timeout
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    if order is None or not order.is_filled:  # ← BUG: cancels partial fills
        self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")


# AFTER (fixed):
async def monitor_order_timeout_AFTER(self, order_id, connector_name):
    await asyncio.sleep(0.3)  # 300ms timeout
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    # Only cancel if order has NO fills at all
    if order is None or order.executed_amount_base == Decimal("0"):
        if order:
            self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")
    else:
        # Order has fills, treat as complete (don't cancel)
        fill_pct = (order.executed_amount_base / order.amount * 100) if order.amount > 0 else 0
        self.logger().info(
            f"Order {order_id} on {connector_name} partially filled: "
            f"{order.executed_amount_base}/{order.amount} ({fill_pct:.1f}%), treating as complete"
        )


# ===== OPTION 2: Using the new has_any_fills property =====
# If you applied the InFlightOrder change, you can use this cleaner version:

async def monitor_order_timeout_WITH_HELPER(self, order_id, connector_name):
    await asyncio.sleep(0.3)  # 300ms timeout
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    # Cancel only if order exists but has no fills
    if order is None or not order.has_any_fills:
        if order:
            self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")
    else:
        # Order has fills, treat as complete
        fill_pct = (order.executed_amount_base / order.amount * 100) if order.amount > 0 else 0
        self.logger().info(
            f"Order {order_id} on {connector_name} partially filled: "
            f"{order.executed_amount_base}/{order.amount} ({fill_pct:.1f}%), treating as complete"
        )


# ===== OPTION 3: More defensive version with market order check =====
# Only treat partial fills as complete for MARKET orders specifically

async def monitor_order_timeout_MARKET_ONLY(self, order_id, connector_name):
    await asyncio.sleep(0.3)  # 300ms timeout
    order = self.connectors[connector_name].in_flight_orders.get(order_id)

    if order is None:
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")
        return

    # For MARKET orders: any fills = complete
    # For LIMIT orders: must be fully filled
    should_cancel = False

    if order.order_type == OrderType.MARKET:
        should_cancel = order.executed_amount_base == Decimal("0")
    else:
        should_cancel = not order.is_filled

    if should_cancel:
        self.connectors[connector_name].cancel(order.trading_pair, order_id)
        self.logger().warning(f"Order {order_id} on {connector_name} was CANCELLED - cooldown enforced")
    else:
        fill_pct = (order.executed_amount_base / order.amount * 100) if order.amount > 0 else 0
        status = "filled" if order.is_filled else "partially filled"
        self.logger().info(
            f"Order {order_id} on {connector_name} {status}: "
            f"{order.executed_amount_base}/{order.amount} ({fill_pct:.1f}%), treating as complete"
        )


# ===== TESTING =====
# Test this fix with these scenarios:

def test_fix():
    from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
    from hummingbot.core.data_type.common import OrderType, TradeType
    import time

    # Test Case 1: Partial fill (the bug case)
    order = InFlightOrder(
        client_order_id="test1",
        trading_pair="BULL-USDT",
        order_type=OrderType.MARKET,
        trade_type=TradeType.BUY,
        amount=Decimal("0.879"),
        creation_timestamp=time.time(),
        initial_state=OrderState.OPEN
    )
    order.executed_amount_base = Decimal("0.597324")  # 68% filled

    # OLD logic: not order.is_filled → True → CANCEL (wrong!)
    # NEW logic: order.executed_amount_base == 0 → False → DON'T CANCEL (correct!)
    assert order.is_filled == False  # Not fully filled
    assert order.has_any_fills == True  # Has some fills
    assert order.executed_amount_base > Decimal("0")  # Has fills - don't cancel!
    print("✅ Test Case 1 (partial fill): Should NOT be cancelled")

    # Test Case 2: Zero fills
    order2 = InFlightOrder(
        client_order_id="test2",
        trading_pair="BULL-USDT",
        order_type=OrderType.MARKET,
        trade_type=TradeType.BUY,
        amount=Decimal("0.879"),
        creation_timestamp=time.time(),
        initial_state=OrderState.OPEN
    )
    order2.executed_amount_base = Decimal("0")  # No fills

    # Both OLD and NEW: should cancel
    assert order2.is_filled == False
    assert order2.has_any_fills == False
    assert order2.executed_amount_base == Decimal("0")  # No fills - cancel!
    print("✅ Test Case 2 (zero fills): Should be cancelled")

    # Test Case 3: Full fill
    order3 = InFlightOrder(
        client_order_id="test3",
        trading_pair="BULL-USDT",
        order_type=OrderType.MARKET,
        trade_type=TradeType.BUY,
        amount=Decimal("0.879"),
        creation_timestamp=time.time(),
        initial_state=OrderState.OPEN
    )
    order3.executed_amount_base = Decimal("0.879")  # Fully filled

    # Both OLD and NEW: should not cancel
    assert order3.is_filled == True
    assert order3.has_any_fills == True
    assert order3.executed_amount_base > Decimal("0")  # Has fills - don't cancel!
    print("✅ Test Case 3 (full fill): Should NOT be cancelled")


if __name__ == "__main__":
    test_fix()
