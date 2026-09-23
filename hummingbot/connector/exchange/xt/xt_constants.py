from decimal import Decimal

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth: the SPOT docs (https://doc.xt.com/docs/spot/..., offline mirror at
# VS_code_projects/MDs/xt-api-v4/) and the pyxt SDK (github.com/kelvinxue/pyxt). Where the two
# disagree, or where the live API disagrees with both, the choice and its evidence are noted here.

EXCHANGE_NAME = "xt"
DEFAULT_DOMAIN = "com"

REST_URL = "https://sapi.xt.com"
WSS_PUBLIC_URL = "wss://stream.xt.com/public"
WSS_PRIVATE_URL = "wss://stream.xt.com/private"

# permessage-deflate window bits for the WS handshake. XT documents the request header
# `Sec-WebSocket-Extensions: permessage-deflate` (wss-general), and P1 proved it is not optional:
# nodes that do not grant it deliver ~40% of the push rate and fall further behind every second
# (2026-09-23, Tracker ws_book_checker/exchanges/xt.py). A connection that is not granted deflate
# is refused and retried, see xt_web_utils.XtWSConnection.
WS_COMPRESS = 15

# clientOrderId: SubmitOrder documents ^[a-zA-Z0-9_]{4,22}$, SubmitBatchOrder and the SDK say 32.
# The stricter one is used. get_new_client_order_id() then hashes the tail, which keeps ids unique.
ORDER_ID_MAX_LEN = 22
HBOT_ORDER_ID_PREFIX = "HBOT"

BIZ_TYPE_SPOT = "SPOT"

# --- REST paths (REST_URL has no path prefix, so these are also the signed paths) ---------------
SERVER_TIME_PATH = "/v4/public/time"
SYMBOL_PATH = "/v4/public/symbol"
DEPTH_PATH = "/v4/public/depth"
TICKER_PRICE_PATH = "/v4/public/ticker/price"

ORDER_PATH = "/v4/order"                        # POST place · GET query by orderId / clientOrderId
CANCEL_ORDER_PATH = "/v4/order/{order_id}"      # DELETE
OPEN_ORDER_PATH = "/v4/open-order"
TRADE_PATH = "/v4/trade"                        # own fills, paginated (hasNext / fromId / direction)
BALANCES_PATH = "/v4/balances"
BALANCE_PATH = "/v4/balance"                   # one currency; used only by the balance audit
WS_TOKEN_PATH = "/v4/ws-token"                  # listenKey for the private stream (valid 2 days)

# Throttler ids. /v4/order serves two verbs with different limits, and the cancel path embeds the
# order id, so these never use the bare path as their limit id.
PLACE_ORDER_LIMIT_ID = "POST/v4/order"
QUERY_ORDER_LIMIT_ID = "GET/v4/order"
CANCEL_ORDER_LIMIT_ID = "DELETE/v4/order"

# --- Signing (SignatureGeneration / SignatureInstructions) ---------------------------------------
# Live-verified 2026-09-23 with a read-only key: the documented `validate-*` headers and the SDK's
# `xt-validate-*` headers are both accepted, as are lowercase and uppercase hex. The documented form
# is used. The server rejects a timestamp older than recvwindow (AUTH_105); +2 s ahead was accepted.
AUTH_HEADER_PREFIX = "validate-"
AUTH_ALGORITHM = "HmacSHA256"
AUTH_RECV_WINDOW_MS = "5000"

# --- Responses (Access Description/ResponseCode) --------------------------------------------------
# Every response is HTTP 200 with {"rc": 0|1, "mc": <code>, "ma": [], "result": ...} — business
# errors included (verified live: AUTH_103, AUTH_105, ORDER_005, AUTH_106 all came back as 200).
RC_OK = 0
MC_SIGNATURE_ERROR = "AUTH_103"
MC_OUTDATED_MESSAGE = "AUTH_105"      # clock drift: the only code that means "re-sync time and retry"
MC_PERMISSION_DENIED = "AUTH_106"
MC_ORDER_NOT_FOUND = "ORDER_005"      # verified live for orderId, clientOrderId and the path form
MC_INSUFFICIENT_FUNDS = "ORDER_002"
MC_SYMBOL_NOT_API_TRADABLE = "SYMBOL_005"  # docs: "The symbol does not support trading via API"

TIME_SYNC_ERROR_CODES = [MC_OUTDATED_MESSAGE]

# --- Order enums (Access Description/PublicModule) ------------------------------------------------
TRADE_TYPES = {TradeType.BUY: "BUY", TradeType.SELL: "SELL"}
ORDER_TYPE_LIMIT = "LIMIT"
TIME_IN_FORCE_GTC = "GTC"

# NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED / EXPIRED (PublicModule, OrderChange).
# REJECTED = never accepted by the engine -> FAILED. EXPIRED = "canceled due to timeout or premium"
# -> CANCELED (any fills are carried by their own trade updates).
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.CANCELED,
}

# --- Public WebSocket (WebSocket Public/*) ---------------------------------------------------------
# depth_update@{symbol}: absolute quantities per level, `fi`..`i` sequence, 100 ms. Combined with a
# REST snapshot per "Orderbook manage"; see xt_api_order_book_data_source.
WS_DEPTH_UPDATE_TOPIC = "depth_update"
WS_TRADE_TOPIC = "trade"
WS_METHOD_SUBSCRIBE = "subscribe"
WS_METHOD_UNSUBSCRIBE = "unsubscribe"

# REST snapshot depth used to bootstrap a local book (docs: limit 500 in the procedure).
SNAPSHOT_DEPTH = 500
# Levels per side handed to Hummingbot's order book on every update. arb_l walks at most 20.
EMIT_DEPTH = 50
# Topics per subscribe request. XT publishes no cap; P1 runs 50 per connection in production.
WS_TOPICS_PER_REQUEST = 50

# Client sends text "ping", server answers text "pong", and disconnects after 60 s without a ping
# (Heartbeat). 20 s keeps a wide margin, as the P1 adapter does.
WS_HEARTBEAT_INTERVAL = 20
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 60

# --- Private WebSocket (WebSocket Private/*) -------------------------------------------------------
WS_PRIVATE_TOPIC_BALANCE = "balance"
WS_PRIVATE_TOPIC_ORDER = "order"
WS_PRIVATE_TOPIC_TRADE = "trade"
# The listenKey is valid 2 days and every POST /v4/ws-token call resets that (GetWsToken). It is
# refreshed well before expiry while the socket stays up.
WS_TOKEN_REFRESH_SECONDS = 12 * 60 * 60

# A private trade push carries the exchange order id only. If it arrives before the placement
# response has given the order that id, it is parked this long for the order push to link it.
PENDING_FILL_TTL_SECONDS = 30.0

# --- Fill accounting guards (xt_exchange._fills_not_yet_counted / _apply_push_fill) --------------
# Hummingbot de-duplicates fills by trade id only and never caps an order's executed amount. Until a
# live fill shows whether XT's WS and REST trade ids are one id space, fills are also matched by
# content, and REST rows only fill the gap to XT's own total.
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
FILL_MATCH_SECONDS = 5.0

# A placement that got no answer is looked up by client id after these delays (seconds) before it is
# allowed to fail; see xt_exchange._find_order_by_client_id.
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)

# --- Balance source (runbook §6.3) ------------------------------------------------------------------
# True = the WS balance push is authoritative between REST polls (Hummingbot's default). Set False if
# the fill test shows XT's balance push missing events (BitMart's stale-balance overbuy, 2026-07-24).
REAL_TIME_BALANCE_UPDATE = True
BALANCE_AUDIT_PUSHES = 5

# --- Fills pagination (Trade/QueryTrade) ------------------------------------------------------------
TRADE_PAGE_SIZE = 100          # documented max
TRADE_MAX_PAGES = 10

# --- First-live-run audit logging (same scheme as CoinEx's [CX-AUDIT]) -----------------------------
# Every line is tagged [XT-AUDIT]; scoped to what the docs cannot settle. Switch off once settled:
#     grep 'XT-AUDIT' ~/hummingbot/logs/logs_test_multi.log
LIVE_AUDIT_LOGGING = True
AUDIT_ONCE_ORDER_KEYS = "order-push-key-set"
AUDIT_ONCE_TRADE_KEYS = "trade-push-key-set"
AUDIT_ONCE_REST_TRADE_KEYS = "rest-trade-key-set"
AUDIT_ONCE_PAGINATION = "fills-pagination-shape"

# --- Rate limits (per-endpoint "Limit Flow Rules") ---------------------------------------------------
RATE_LIMITS = [
    RateLimit(limit_id=SERVER_TIME_PATH, limit=10, time_interval=1),      # not stated; conservative
    RateLimit(limit_id=SYMBOL_PATH, limit=10, time_interval=1),           # 10/s/ip
    RateLimit(limit_id=DEPTH_PATH, limit=10, time_interval=1),            # 10/s/ip
    RateLimit(limit_id=TICKER_PRICE_PATH, limit=10, time_interval=1),     # 10/s/ip
    RateLimit(limit_id=PLACE_ORDER_LIMIT_ID, limit=20, time_interval=1),  # 20/s/apikey
    RateLimit(limit_id=QUERY_ORDER_LIMIT_ID, limit=50, time_interval=1),  # 50/s/apikey
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=20, time_interval=1),  # "N/A"; kept at the place rate
    RateLimit(limit_id=OPEN_ORDER_PATH, limit=10, time_interval=1),       # 10/s/apikey
    RateLimit(limit_id=TRADE_PATH, limit=10, time_interval=1),            # not stated; conservative
    RateLimit(limit_id=BALANCES_PATH, limit=10, time_interval=1),         # 10/s/apikey
    RateLimit(limit_id=BALANCE_PATH, limit=10, time_interval=1),          # not stated; conservative
    RateLimit(limit_id=WS_TOKEN_PATH, limit=1, time_interval=10),         # 1 / 10 s / apikey
]
