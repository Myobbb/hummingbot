"""MEXC WebSocket constants and mappings."""

from ...core.trading_data import OrderStatus

# MEXC WebSocket endpoints (protobuf channels)
MEXC_WS_PRIVATE_URL = "wss://wbs-api.mexc.com/ws"
USER_TRADES_ENDPOINT = "spot@private.deals.v3.api.pb"
USER_ORDERS_ENDPOINT = "spot@private.orders.v3.api.pb"
USER_BALANCE_ENDPOINT = "spot@private.account.v3.api.pb"

# MEXC order status mappings (numeric values from API)
ORDER_STATUS_MAP = {
    1: "OPEN",               # Not traded
    2: "FILLED",             # Fully traded  
    3: "PARTIALLY_FILLED",   # Partially traded
    4: "CANCELED",           # Canceled
    5: "PARTIALLY_CANCELED"  # Partially canceled
}

# Domain status mappings for internal use
DOMAIN_STATUS_MAP = {
    2: OrderStatus.FILLED,
    3: OrderStatus.PARTIALLY_FILLED,
    4: OrderStatus.CANCELLED,
    5: OrderStatus.PARTIALLY_FILLED
}

# Side mappings (numeric to string)
SIDE_MAP = {
    1: "BUY",
    2: "SELL"
}

# MEXC Balance Event Types (from 'o' field in spot@private.account.v3.api)
MEXC_EVENT_DEPOSIT = "DEPOSIT"                  # ✅ Verified in logs (14 occurrences)
MEXC_EVENT_WITHDRAW = "WITHDRAW"                # ✅ Verified in logs (24 occurrences)
MEXC_EVENT_WITHDRAW_FEE = "WITHDRAW_FEE"        # ✅ Verified in logs (12 occurrences) - mapped to generic
MEXC_EVENT_ENTRUST = "ENTRUST"                  # ✅ Verified in logs (873 occurrences)
MEXC_EVENT_ENTRUST_PLACE = "ENTRUST_PLACE"      # ✅ Verified in logs (261 occurrences)
MEXC_EVENT_ENTRUST_CANCEL = "ENTRUST_CANCEL"    # ✅ Verified in logs (19 occurrences)

# Note: Event filtering is handled at the unified event type level
# Raw event lists are not needed since mapping is done directly

# Mapping to unified event types
def get_mexc_unified_event_mapping():
    """Get MEXC event mapping to unified types."""
    from ...core.events import BalanceEventType
    
    return {
        MEXC_EVENT_DEPOSIT: BalanceEventType.DEPOSIT,           # ✅ Verified in logs (14 occurrences)
        MEXC_EVENT_WITHDRAW: BalanceEventType.WITHDRAWAL,       # ✅ Verified in logs (24 occurrences)
        # Treat generic account delta as a balance update for readability in UI/logs
        # No dedicated BALANCE_UPDATE enum in current domain; mark as UNKNOWN to avoid warnings
        "balance-update": BalanceEventType.UNKNOWN,
        # All other events (fees, trading, etc.) will default to UNKNOWN and be handled generically
    }