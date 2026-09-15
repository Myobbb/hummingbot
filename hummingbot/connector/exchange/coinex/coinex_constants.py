from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "coinex"
DEFAULT_DOMAIN = "com"

# https://docs.coinex.com/api/v2/  §API Introduction
REST_URL = "https://api.coinex.com/v2"
WSS_URL = "wss://socket.coinex.com/v2/spot"

# CoinEx `client_id`: "only supports uppercase and lowercase letters, numbers, hyphens, and
# underscores in 32 bytes" (spot/order/http/put-order). The prefix must stay inside that charset.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# Spot order calls require market_type; only SPOT or MARGIN are valid there (enum.md#market_type).
MARKET_TYPE_SPOT = "SPOT"

# --- REST endpoints (paths are relative to REST_URL, i.e. they already sit under /v2) ---------
PUBLIC_PING_ENDPOINT = "/ping"
PUBLIC_TIME_ENDPOINT = "/time"
PUBLIC_MARKET_ENDPOINT = "/spot/market"          # trading rules + the symbol universe
PUBLIC_DEPTH_ENDPOINT = "/spot/depth"
PUBLIC_TICKER_ENDPOINT = "/spot/ticker"

ACCOUNT_BALANCE_ENDPOINT = "/assets/spot/balance"
PLACE_ORDER_ENDPOINT = "/spot/order"
CANCEL_ORDER_ENDPOINT = "/spot/cancel-order"
CANCEL_ORDER_BY_CLIENT_ID_ENDPOINT = "/spot/cancel-order-by-client-id"
ORDER_STATUS_ENDPOINT = "/spot/order-status"
PENDING_ORDER_ENDPOINT = "/spot/pending-order"
ORDER_DEALS_ENDPOINT = "/spot/order-deals"
# spot/deal/http/list-user-order-deals documents `limit` with a default of 10 and states no
# maximum, so the default page size is used and pages are walked instead of guessing a bigger one.
ORDER_DEALS_PAGE_SIZE = 10
ORDER_DEALS_MAX_PAGES = 20

# --- WebSocket methods -------------------------------------------------------------------------
WS_DEPTH_SUBSCRIBE = "depth.subscribe"
WS_DEPTH_UPDATE = "depth.update"
WS_DEALS_SUBSCRIBE = "deals.subscribe"
WS_DEALS_UPDATE = "deals.update"
WS_ORDER_SUBSCRIBE = "order.subscribe"
WS_ORDER_UPDATE = "order.update"
WS_BALANCE_SUBSCRIBE = "balance.subscribe"
WS_BALANCE_UPDATE = "balance.update"
# Per-fill push (spot/deal/ws/user-deals). Carries client_id + fee + role, which is everything a
# TradeUpdate needs, so fills arrive in real time instead of waiting for a REST poll.
WS_USER_DEALS_SUBSCRIBE = "user_deals.subscribe"
WS_USER_DEALS_UPDATE = "user_deals.update"
WS_SIGN_METHOD = "server.sign"
WS_PING_METHOD = "server.ping"

# depth.subscribe params are positional: [market, limit, interval, if_full]
# limit must be one of [5, 10, 20, 50]; interval "0" means no merging
# (spot/market/ws/market-depth). if_full=True makes EVERY push a complete book, which is why this
# connector only ever emits SNAPSHOT messages and never has to rebuild from diffs or verify the
# CRC32 checksum.
WS_DEPTH_LEVELS = 20
WS_DEPTH_INTERVAL = "0"
WS_DEPTH_FULL_PUSH = True

# Undocumented safeguard carried over from the P1 ws_book_checker adapter
# (Tracker_cex_cex/ws_book_checker/exchanges/coinex.py): CoinEx has no published cap on markets per
# depth.subscribe, but oversized subscribe frames were unreliable in production, so batch them.
WS_MAX_MARKETS_PER_SUBSCRIBE = 30

WS_HEARTBEAT_TIME_INTERVAL = 25
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 60

# --- Enum maps (enum.md) -----------------------------------------------------------------------
TRADE_TYPES = {
    TradeType.BUY: "buy",
    TradeType.SELL: "sell",
}
ORDER_TYPES = {
    OrderType.LIMIT: "limit",
    OrderType.MARKET: "market",
    OrderType.LIMIT_MAKER: "maker_only",
}
# enum.md#order_status documents exactly: open, part_filled, filled, part_canceled, canceled.
#
# HISTORY (re-verified 2026-09-11): until ~2026-09-07 the get-order-status response EXAMPLE
# returned "part_deal", which was absent from that enum — the two pages contradicted each other.
# CoinEx has since corrected the example to "part_filled", so the docs now agree and `part_deal`
# appears nowhere. It is KEPT here anyway: the docs were demonstrably wrong once, we have never
# seen this endpoint's live output, and mapping a string the API might still emit costs nothing
# while mis-reading it would mark a live order terminal. Any status outside this table falls back
# to a quantitative derivation from the filled/unfilled amounts rather than being guessed
# (see CoinexExchange._order_state_from_payload).
STATE_TYPES = {
    # documented in enum.md#order_status
    "open": OrderState.OPEN,
    "part_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "part_canceled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
    # retired from the docs 2026-09-07; retained defensively (see note above)
    "part_deal": OrderState.PARTIALLY_FILLED,
}

# order.update carries an `event` instead of a status (enum.md#order_event). `finish` means the
# order left the book — filled or canceled — so the two are separated by remaining amount.
WS_ORDER_EVENT_PUT = "put"
WS_ORDER_EVENT_UPDATE = "update"
WS_ORDER_EVENT_MODIFY = "modify"
WS_ORDER_EVENT_FINISH = "finish"

# error.md — CoinEx wraps every response in {code, data, message}; code 0 is success.
RET_CODE_OK = 0

# Order-not-found is NOT in the documented error table (still absent as of 2026-09-11). 3600 was
# observed LIVE — querying a non-existent order id returns `code 3600 — Order not found` — so it is
# recorded with its provenance and backed by a message-text fallback, so a reworded or additional
# code still classifies correctly.
RET_CODE_ORDER_NOT_FOUND = 3600

# Documented in error.md.
RET_CODE_SERVICE_UNAVAILABLE = 4001
RET_CODE_SERVICE_TIMEOUT = 4002
RET_CODE_INTERNAL_ERROR = 4003
RET_CODE_PARAMETER_ERROR = 4004
RET_CODE_ABNORMAL_ACCESS_ID = 4005
RET_CODE_SIGNATURE_FAILED = 4006
RET_CODE_IP_PROHIBITED = 4007
RET_CODE_ABNORMAL_SIGN_VALUE = 4008
RET_CODE_EXPIRED_REQUEST = 4010
RET_CODE_SIGNATURE_EXPIRED = 4017
RET_CODE_TOO_FREQUENT = 4213

# Only these two mean "our clock drifted" — Hummingbot re-syncs server time and retries when
# _is_request_exception_related_to_time_synchronizer returns True, so this set must be exact.
# Getting it wrong means a drifting clock never triggers a resync and every signed call keeps
# failing.
RET_CODES_TIME_SYNC = [RET_CODE_EXPIRED_REQUEST, RET_CODE_SIGNATURE_EXPIRED]

# `enum.md` order_status: "any canceled orders without execution will not be saved" — so a
# not-found lookup is a legitimate terminal state, not an error. See coinex_exchange.
ORDER_NOT_FOUND_MESSAGES = ["order not found", "order does not exist"]

# --- First-live-run audit logging ---------------------------------------------------------------
# Targeted logging for the FIRST live order run, aimed at the questions the docs cannot answer.
# Every line is tagged [CX-AUDIT] so the whole trail greps out of the orchestrator log:
#     grep 'CX-AUDIT' ~/hummingbot/logs/logs_test_multi.log
#
# Deliberately NOT logged: depth/balance/ping traffic (200ms cadence, nothing unknown), and
# anything carrying credentials — request headers are never touched.
#
# Set False once the unknowns below are settled; the connector behaves identically either way.
LIVE_AUDIT_LOGGING = True

# One-shot tags: logged the FIRST time each is observed, so a long run cannot flood the log.
AUDIT_ONCE_ORDER_KEYS = "order-update-key-set"
AUDIT_ONCE_DEAL_KEYS = "user-deal-key-set"
AUDIT_ONCE_PAGINATION = "fills-pagination-shape"

# --- Rate limits (rate-limit.md) ---------------------------------------------------------------
# Documented account short-cycle groups. Public market endpoints have no published per-group limit;
# only the global IP limit of 400/s is stated, so those get deliberately conservative values.
RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_PING_ENDPOINT, limit=20, time_interval=1),
    RateLimit(limit_id=PUBLIC_TIME_ENDPOINT, limit=20, time_interval=1),
    RateLimit(limit_id=PUBLIC_MARKET_ENDPOINT, limit=20, time_interval=1),
    RateLimit(limit_id=PUBLIC_DEPTH_ENDPOINT, limit=20, time_interval=1),
    RateLimit(limit_id=PUBLIC_TICKER_ENDPOINT, limit=20, time_interval=1),

    # "Place & edit spot orders" — 30r/1s
    RateLimit(limit_id=PLACE_ORDER_ENDPOINT, limit=30, time_interval=1),
    # "Cancel spot orders" — 60r/1s
    RateLimit(limit_id=CANCEL_ORDER_ENDPOINT, limit=60, time_interval=1),
    # "Batch cancel spot orders" — 40r/1s (the by-client_id path sits in this group)
    RateLimit(limit_id=CANCEL_ORDER_BY_CLIENT_ID_ENDPOINT, limit=40, time_interval=1),
    # "Query spot orders" — 50r/1s
    RateLimit(limit_id=ORDER_STATUS_ENDPOINT, limit=50, time_interval=1),
    RateLimit(limit_id=PENDING_ORDER_ENDPOINT, limit=50, time_interval=1),
    # "Query spot order history" — 10r/1s
    RateLimit(limit_id=ORDER_DEALS_ENDPOINT, limit=10, time_interval=1),
    # "Query spot accounts" — 10r/1s
    RateLimit(limit_id=ACCOUNT_BALANCE_ENDPOINT, limit=10, time_interval=1),
]
