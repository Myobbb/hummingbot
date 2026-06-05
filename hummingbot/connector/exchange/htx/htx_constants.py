# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "htx"
BROKER_ID = "AAc484720a"
DOMAIN = ""
MAX_CLIENT_ORDER_ID_LENGTH = 64


# Use AWS-hosted endpoints for improved stability
REST_URL = "https://api-aws.huobi.pro"
WS_PUBLIC_URL = "wss://api-aws.huobi.pro/ws"  # For trade.detail
WS_MBP_URL = "wss://api-aws.huobi.pro/feed"   # For MBP (Market By Price) incremental updates
WS_PRIVATE_URL = "wss://api-aws.huobi.pro/ws/v2"


WS_HEARTBEAT_TIME_INTERVAL = 20  # seconds

# Websocket event types
TRADE_CHANNEL_SUFFIX = "trade.detail"
ORDERBOOK_CHANNEL_SUFFIX = "mbp"  # MBP for orderbook updates

# Hybrid MBP orderbook configuration for maximum freshness:
# - mbp.5: tick-by-tick incremental updates for top 5 levels (ALL symbols, fastest)
# - mbp.refresh.20: 100ms full snapshots with 20 levels (ALL symbols, fills deeper depth)
# Both share same sequence space. Use mbp.5 for real-time, mbp.refresh.20 for sync/recovery.
MBP_INCREMENTAL_DEPTH = 5   # Tick-by-tick incremental (DIFF messages)
MBP_REFRESH_DEPTH = 20      # 100ms full snapshots (fills levels 6-20)

TRADE_INFO_URL = "/v1/settings/common/market-symbols"
DEPTH_URL = "/market/depth"
LAST_TRADE_URL = "/market/trade"

SERVER_TIME_URL = "/v1/common/timestamp"
ACCOUNT_ID_URL = "/v1/account/accounts"
ACCOUNT_BALANCE_URL = "/v1/account/accounts/{}/balance"
OPEN_ORDERS_URL = "/v1/order/openOrders"
ORDER_DETAIL_URL = "/v1/order/orders/{}"
ORDER_MATCHES_URL = "/v1/order/orders/{}/matchresults"
PLACE_ORDER_URL = "/v1/order/orders/place"
CANCEL_ORDER_URL = "/v1/order/orders/{}/submitcancel"
BATCH_CANCEL_URL = "/v1/order/orders/batchcancel"

HTX_ACCOUNT_UPDATE_TOPIC = "accounts.update#2"
HTX_ORDER_UPDATE_TOPIC = "orders#{}"
HTX_TRADE_DETAILS_TOPIC = "trade.clearing#{}"

HTX_SUBSCRIBE_TOPICS = {HTX_ORDER_UPDATE_TOPIC, HTX_ACCOUNT_UPDATE_TOPIC, HTX_TRADE_DETAILS_TOPIC}

WS_CONNECTION_LIMIT_ID = "WSConnection"
WS_REQUEST_LIMIT_ID = "WSRequest"
CANCEL_URL_LIMIT_ID = "cancelRequest"
ACCOUNT_BALANCE_LIMIT_ID = "accountBalance"
ORDER_DETAIL_LIMIT_ID = "orderDetail"
ORDER_MATCHES_LIMIT_ID = "orderMatch"

RATE_LIMITS = [
    RateLimit(WS_CONNECTION_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(WS_REQUEST_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(limit_id=TRADE_INFO_URL, limit=10, time_interval=1),
    RateLimit(limit_id=DEPTH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=LAST_TRADE_URL, limit=10, time_interval=1),
    RateLimit(limit_id=SERVER_TIME_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ACCOUNT_ID_URL, limit=100, time_interval=2),
    RateLimit(limit_id=ACCOUNT_BALANCE_LIMIT_ID, limit=100, time_interval=2),
    RateLimit(limit_id=ORDER_DETAIL_LIMIT_ID, limit=50, time_interval=2),
    RateLimit(limit_id=ORDER_MATCHES_LIMIT_ID, limit=50, time_interval=2),
    RateLimit(limit_id=PLACE_ORDER_URL, limit=100, time_interval=2),
    RateLimit(limit_id=CANCEL_URL_LIMIT_ID, limit=100, time_interval=2),
    RateLimit(limit_id=BATCH_CANCEL_URL, limit=50, time_interval=2),

]

# Order States
ORDER_STATE = {
    "rejected": OrderState.FAILED,
    "canceled": OrderState.CANCELED,
    "submitted": OrderState.OPEN,
    "partial-filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "partial-canceled": OrderState.CANCELED,
    "created": OrderState.PENDING_CREATE,
    "canceling": OrderState.PENDING_CANCEL
}

# Timeout (seconds) after last partial-filled update to auto-finalize as FILLED
# Keeps strategies from hanging when HTX never emits a final filled state.
PARTIAL_FINALIZE_TIMEOUT_S = 6.0


# ---------------------------------------------------------------------------
# HTX flash-order (ghost-wall) suppression — orderbook parse-path filter
# ---------------------------------------------------------------------------
# HTX spot books are dominated by colocated/in-house bot activity that produces
# untradeable false inside-quotes: (a) FLASH GHOST WALLS — a level placed+pulled
# between snapshots (gone next refresh, never fills); (b) FLASH GAP-FILLS on
# chronically-wide illiquid books (a normally-wide spread momentarily closed by a
# flash, faking a tight cross). arb_l fires on a single top-of-book read in the
# Cython hot path, so a flash that survives one mbp.refresh.20 snapshot can trigger
# a spurious arb leg that never fills on the HTX side.
#
# This filter runs in the connector's async MBP-parse path (OFF the hot path) and
# withholds an *improving* inside level until it has been confirmed across N
# consecutive raw mbp.refresh.20 snapshots (L2 persistence), with a stricter gate on
# chronically-wide (flash-prone) books (L4 wide-gap). Because mbp.refresh.20 fully
# resets the book every ~100ms, a genuine level re-enters within ~100ms once
# confirmed; the hot path then reads an already-clean book at zero added cost.
#
# Empirical basis + the validated multi-signal design it mirrors: the ws_book_checker
# (monitor) HTX 4-signal classifier — see Tracker_cex_cex/ws_book_checker/HTX_GHOST_FILTER.md
# and wiki trading/tracker-cex-cex/ws-book-checker.md. L1 (per-side dislocation) and
# L3 (trade.detail takeout/wash) are intentionally deferred; clean seams are left for them.
#
# DEFAULT OFF and per-pair gated so it ships dark and is enabled deliberately.

# Master switch. When False, the parse path is byte-for-byte the prior behavior.
FLASH_FILTER_ENABLED = True

# Per-trading-pair allowlist (e.g. {"BTC-USDT"}). Empty + ALL_PAIRS False => no pair
# is filtered even if FLASH_FILTER_ENABLED is True (extra safety belt).
FLASH_FILTER_PAIRS: set = set()
# If True, apply to every HTX pair (ignores FLASH_FILTER_PAIRS). Use once validated.
FLASH_FILTER_ALL_PAIRS = True

# L2 — min consecutive RAW mbp.refresh.20 snapshots an improving inside level must
# persist before it is allowed into the book. 2 => ~100ms of confirmation. The monitor
# uses 6 (it is a reporter; the connector clipping a real level is costlier, so start
# conservative). Receding/worse tops are NEVER withheld — only newly-improving ones.
FLASH_FILTER_PERSIST_RAW_SNAPS = 2

# L4 — baseline (EMA) HTX spread % at/above which a book is treated as chronically
# illiquid/flash-prone; on such books the persistence requirement is enforced even
# when not strictly "improving" (these are where flash gap-fills do the most damage).
FLASH_FILTER_WIDE_SPREAD_PCT = 3.0

# EMA smoothing for the rolling baseline-spread estimate (L4). Small => slow baseline.
FLASH_FILTER_SPREAD_EMA_ALPHA = 0.1

# Price-equality tolerance (fraction of price) for "same level persisted" comparisons,
# absorbing float noise. 0.0005 = 0.05%.
FLASH_FILTER_PRICE_EPS = 0.0005

# Emit a DEBUG line each time a level is withheld (audit trail; no hot-path cost).
FLASH_FILTER_LOG_SUPPRESSIONS = True
