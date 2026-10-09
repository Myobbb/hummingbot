from decimal import Decimal

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth: the SPOT docs (https://api-docs.poloniex.com/spot, offline mirror at
# VS_code_projects/MDs/poloniex-api/), the official SDK polo-sdk-python (mirrored in MDs/poloniex-api/sdk/), and the
# phase-0 probes on myserver (2026-10-09; wiki trading/exchanges/poloniex-api, "P2 + P3 onboarding"). Where they
# disagree, the choice and its evidence are noted here.

EXCHANGE_NAME = "poloniex"
DEFAULT_DOMAIN = "com"

REST_URL = "https://api.poloniex.com"
WSS_PUBLIC_URL = "wss://ws.poloniex.com/ws/public"
WSS_PRIVATE_URL = "wss://ws.poloniex.com/ws/private"

# clientOrderId: "Maximum 64-character length", [A-Za-z0-9_-] (Orders/Create Order); the Error Codes page's 21315 says
# "max length of 17 digits". Hummingbot's ids are alphanumeric; 32 is the length the placement probe proves.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths (REST_URL has no path prefix, so these are also the signed paths) ---------------------
SERVER_TIME_PATH = "/timestamp"                    # {"serverTime": ms}
MARKETS_PATH = "/markets"                          # every market + symbolTradeLimit
MARKET_PATH = "/markets/{symbol}"                  # one market (a one-element list; [] for an unknown symbol)
ORDER_BOOK_PATH = "/markets/{symbol}/orderBook"    # a CACHE 0.5-2 s behind the stream: bootstrap only
PRICES_PATH = "/markets/price"                     # every market's last trade price, one call
FEE_INFO_PATH = "/feeinfo"                         # makerRate / takerRate + specialFeeRates per symbol
BALANCES_PATH = "/accounts/balances"               # ?accountType=SPOT
ORDERS_PATH = "/orders"                            # POST place · GET the open orders
ORDER_PATH = "/orders/{order_id}"                  # GET status · DELETE cancel; "cid:<clientOrderId>" works for both
ORDER_TRADES_PATH = "/orders/{order_id}/trades"    # the order's fills (exchange id only)

# Throttler ids: a path with an id in it never is its own limit id.
PLACE_ORDER_LIMIT_ID = "POST/orders"
OPEN_ORDERS_LIMIT_ID = "GET/orders"
QUERY_ORDER_LIMIT_ID = "GET/orders/{id}"
CANCEL_ORDER_LIMIT_ID = "DELETE/orders/{id}"
ORDER_TRADES_LIMIT_ID = "GET/orders/{id}/trades"
ORDER_BOOK_LIMIT_ID = "GET/markets/{symbol}/orderBook"
MARKET_LIMIT_ID = "GET/markets/{symbol}"

# --- Signing (Spot Rest API/Overview; SDK polosdk/spot/rest/request.py) -----------------------------------
# Without a body: "METHOD\n<path>\n" + the sorted k=v pairs, signTimestamp among them, values URI-encoded as the SDK's
# encode_uri_component (quote, safe "~()*!'"). With a body: "METHOD\n<path>\nrequestBody=<the exact body>&signTimestamp=<ms>".
# HMAC-SHA256, base64. Headers key / signature / signTimestamp. WS auth: "GET\n/ws\nsignTimestamp=<ms>".
SIGN_SAFE_CHARS = "~()*!'"

# --- Responses -------------------------------------------------------------------------------------------
# A refusal is HTTP 4xx + {"code": <int>, "message": ...}; a success is the bare JSON object or list.
CODE_ORDER_NOT_FOUND = 21301          # live 2026-10-09: 404 on a read, 400 on a cancel; by id and by cid: alike
CODE_INVALID_SYMBOL = 24101
# A signTimestamp >= 300 s old: HTTP 401 "Signature has expired" (live 2026-10-09; -120 s passes, future times never
# refused). Refused at authentication, so a resend after a clock re-sync can't duplicate anything.
MSG_SIGNATURE_EXPIRED = "signature has expired"
# Order codes worth naming in a refusal's WARNING (Error Codes page).
REFUSAL_CODES = {
    "21709": "insufficient balance", "21312": "client order id already exists",
    "21315": "client order id too long", "21322": "amount below the minimum", "21330": "quantity below the minimum",
    "21350": "amount must be greater than 1 USDT", "21335": "price scale", "21336": "quantity scale",
    "21337": "amount scale", "21340": "maintenance mode", "21341": "post-only mode", "21352": "currency trading frozen",
    "21356": "price protection (> 20 % move)", "10041": "symbol frozen for trading", "21351": "account trading frozen",
    "21354": "account not verified for trading", "21353": "US customers not supported",
}

# --- Order enums -----------------------------------------------------------------------------------------
TRADE_TYPES = {TradeType.BUY: "BUY", TradeType.SELL: "SELL"}
ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_LIMIT_MAKER = "LIMIT_MAKER"
TIME_IN_FORCE_GTC = "GTC"

# Order Details / the orders push: NEW, PARTIALLY_FILLED, FILLED, PENDING_CANCEL, PARTIALLY_CANCELED, CANCELED, FAILED.
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "PARTIALLY_CANCELED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "FAILED": OrderState.FAILED,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)

# --- Public WebSocket (Spot WebSocket API/Market Data) -----------------------------------------------------
# book_lv2: a 20-level snapshot on subscribe, then level updates chained lastId -> id; a gap means re-subscribe.
# Measured on myserver 2026-10-09 (hmb_local_tools/poloniex/debug_poloniex_ws_cadence.py, 2 x 300 s, 12 markets): it
# reaches us before P1's `book` channel on 96 % of updates (p50 21 ms, p90 77 ms, p99 99 ms), identical top 20 at the
# same id on 23,916 of 23,916, 0 gaps, and no update ever left more than 20 levels on a side.
WS_BOOK_CHANNEL = "book_lv2"
# `book` (top-N snapshots every ~100 ms, the same SeqId) is subscribed at depth 5 as the lv2 book's live cross-check:
# when its top keeps differing from the local book's for BOOK_DIVERGENCE_S while it keeps advancing, the lv2 stream
# of that market is stale and the market is re-subscribed (a fresh snapshot).
WS_CHECK_CHANNEL = "book"
WS_CHECK_DEPTH = 5
BOOK_DIVERGENCE_S = 2.0
# One market's re-subscribes are at least this far apart: a fresh snapshot that is broken again is retried at this
# pace, never in a loop at socket speed (the market shows empty meanwhile).
RESUBSCRIBE_MIN_INTERVAL = 5.0
WS_TRADES_CHANNEL = "trades"
WS_EXCHANGE_CHANNEL = "exchange"                   # {"mm": "ON"|"OFF", "pom": "ON"|"OFF"} (live: lower-case keys)
WS_SYMBOLS_PER_REQUEST = 50                        # P1 subscribes 50 a message (300 in one tested)
# Levels per side kept and handed to Hummingbot's order book. lv2 maintains exactly the top 20.
BOOK_DEPTH = 20
# The client pings; the server ends a session silent for 30 s ({"event":"ping"} -> {"event":"pong"}, ~5 ms RTT from
# myserver). The ping is the only liveness signal of a half-open socket (every channel goes silent with it, the
# `book` cross-check too, and a quiet market sends nothing for minutes), so it is frequent: a dead socket is closed
# within WS_HEARTBEAT_INTERVAL + WS_PONG_TIMEOUT (10 s; was 25 s).
WS_HEARTBEAT_INTERVAL = 5
WS_PONG_TIMEOUT = 5
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 60

# Poloniex's per-market switch (GET /markets/{symbol}): tradable = state NORMAL and tradableStartTime passed (P1's
# rule). 5 of the 40 PAUSE markets keep a frozen two-sided book (2026-10-09), so a market that is off is shown EMPTY
# while its symbol and trading rule stay (non-negotiable 3). The `symbols` socket channel is no source: it read
# NORMAL for 25 of the 40 PAUSE markets. Exchange-wide maintenance / post-only mode (the `exchange` channel) empties
# every book while it lasts.
TRADING_SWITCH_INTERVAL = 30

# Reconnect pacing (XT's lesson: the listen loops reconnect at once after a ConnectionError).
WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Private WebSocket (Spot WebSocket API/Authentication, Orders, Balances) ---------------------------------
WS_AUTH_CHANNEL = "auth"
WS_ORDERS_CHANNEL = "orders"
WS_BALANCES_CHANNEL = "balances"
# Live 2026-10-09: subscribes sent with the auth event were answered "user must be authenticated!" (the ack came 5 ms
# later), so the subscribes wait for the ack.
WS_AUTH_TIMEOUT = 10

# --- Order safety (runbook §1.5) --------------------------------------------------------------------------
# A placement that got no answer is looked up by its client id (GET /orders/cid:<id>) after these delays.
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# Every accepted order gets a `place` push; none within this long means the private stream isn't delivering.
ORDER_PUSH_EXPECTED_WITHIN = 5.0
ORDER_PUSH_ALARM_INTERVAL = 300.0
# A FILLED / cancelled push whose fills fall short of filledQuantity is backfilled from REST after this grace.
FILL_BACKFILL_GRACE = 0.5
# A terminal state whose fills Poloniex's own trade rows don't show yet is held back (the order stays open to HMB, the
# next poll reads the rows again) for at most this long; then it is forwarded short, with a [PLX-ALARM].
TERMINAL_HOLD_MAX_S = 60.0
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
FILL_MATCH_SECONDS = 5.0

# --- Balance source (runbook §6.3) ------------------------------------------------------------------------
# The balances push carries `available` + `hold` after every change (place, cancel, match, deposit...), versioned per
# currency. WS-authoritative until the fill test shows otherwise.
REAL_TIME_BALANCE_UPDATE = True
BALANCE_AUDIT_PUSHES = 5

# --- First-live-run audit logging -------------------------------------------------------------------------------
# Every line is tagged [PLX-AUDIT]; scoped to what the docs could not settle. Money-guard firings always log as
# [PLX-ALARM] WARNINGs, whatever this says.
LIVE_AUDIT_LOGGING = True

# --- Rate limits (Spot Rest API/Overview "Rate Limits": per UID for VIP0 trading/account calls, per IP public) ------
RATE_LIMITS = [
    RateLimit(limit_id=SERVER_TIME_PATH, limit=200, time_interval=1),
    RateLimit(limit_id=MARKETS_PATH, limit=30, time_interval=1),
    RateLimit(limit_id=MARKET_LIMIT_ID, limit=200, time_interval=1),
    RateLimit(limit_id=ORDER_BOOK_LIMIT_ID, limit=200, time_interval=1),
    RateLimit(limit_id=PRICES_PATH, limit=30, time_interval=1),
    RateLimit(limit_id=FEE_INFO_PATH, limit=50, time_interval=1),
    RateLimit(limit_id=BALANCES_PATH, limit=50, time_interval=1),
    RateLimit(limit_id=PLACE_ORDER_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(limit_id=OPEN_ORDERS_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(limit_id=QUERY_ORDER_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(limit_id=ORDER_TRADES_LIMIT_ID, limit=50, time_interval=1),
]
