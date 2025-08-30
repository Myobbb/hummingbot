"""HTX (Huobi) WebSocket v2 constants and mappings.

References:
- Private WS v2: `wss://api.huobi.pro/ws/v2` (public) and `wss://api-aws.huobi.pro/ws/v2` (AWS)
"""

# Preferred private WebSocket URL per HTX docs
HTX_WS_PRIVATE_URL = "wss://api-aws.huobi.pro/ws/v2"

# Channel templates and wildcards
# Balances require an accountId suffix
HTX_CHANNEL_BALANCES_TEMPLATE = "accounts.update#{}"  # format with accountId
# Some WS v2 docs use mode variant instead of accountId
HTX_CHANNEL_BALANCES_MODE_TEMPLATE = "accounts.update#{}"  # format with mode (e.g., 2)
HTX_CHANNEL_BALANCES_MODE_DEFAULT = "accounts.update#2"
# Orders/trades: wildcard '*' to receive all symbols
HTX_CHANNEL_ORDERS_ALL = "orders#*"
HTX_CHANNEL_TRADES_ALL = "trade.clearing#*"

# All private channels that should be active (balance topic is templated per account)
ALL_PRIVATE_CHANNELS = [
    HTX_CHANNEL_ORDERS_ALL,
    HTX_CHANNEL_TRADES_ALL,
]

# WebSocket configuration
# Use a tighter ping cadence to keep NATs/load balancers from idling out (~10s)
WS_PING_INTERVAL = 10  # seconds


# Mapping to unified balance event types
def get_htx_unified_event_mapping():
    """Return HTX balance event mapping to unified types.

    HTX balance push payloads (accounts.update) include a change type that may be one of:
    'deposit', 'withdraw', 'trade', 'transfer', etc. We only specialize deposit/withdraw.
    """
    from ...core.events import BalanceEventType

    return {
        # Map deposit/withdraw explicitly; other events default to UNKNOWN
        "deposit": BalanceEventType.DEPOSIT,
        "withdraw": BalanceEventType.WITHDRAWAL,
    }


