from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-11-17 from myserver with the asset_manager's
# read-only key: VS_code_projects/p2p3_nonkyc_onboarding_2026-11-17/probes/), NonKYC's own Hummingbot fork
# (VS_code_projects/MDs/nonkyc-api/raw/hb_nonkyc/), then the docs (OpenAPI 1.3.0 + WS API 1.1.0, same mirror).
# Wiki: trading/exchanges/nonkyc-api ("P2 + P3 onboarding", 2026-11-17).
#
# Structural facts (each one is why a choice below looks the way it does):
#   - REST base https://api.nonkyc.io/api/v2. Signed routes take X-API-KEY, X-API-NONCE (unix ms) and
#     X-API-SIGN = hex HMAC-SHA256(secret, apiKey + FULL url incl. query + body + nonce). The query IS signed:
#     a GET with params signed without them answers HTTP 401 "Not Authorized" (probed 2026-11-17).
#   - One market per coin: <BASE>/USDT on the wire, BASE-USDT in Hummingbot.
#   - The public socket pushes on change: BTC ~0.8 pushes/s, most markets silent for minutes. A push carries ONE
#     side of full-depth deltas (usually 1-2 levels, up to 10 seen) and a per-market `sequence` int; the snapshot
#     that answers a subscribe carries the same chain as a STRING. REST /market/orderbook's `sequence` is a
#     DIFFERENT chain (P1, 2026-11-14), so a REST book can never seed the WS book.
#   - Push lag from myserver (Tokyo) to the venue: p50 122 ms, p90 123 ms (167 samples, 2026-11-17). The venue's
#     origin is far from myserver; our side of the execution window is that lag, and it is the venue's, not ours.
#   - The private socket: `login` (HS256 over the NONCE ALONE), then subscribeReports (order + trade pushes, and an
#     `activeOrders` snapshot on the reply) and subscribeBalances (`currentBalances` snapshot, `balanceUpdate` pushes).
#     Every report carries userProvidedId (our client id), so a fill can beat the placement answer.
#   - Fills: the report's tradeFee is in the QUOTE asset (docs: "fee for trade in secondary currency"); REST trade
#     rows carry `fee` (quote) plus `alternateFee`/`alternateFeeAsset` when a platform token paid it.
#   - Not-found is code 20002 on HTTP 400 for BOTH the status read ("Order not found") and the cancel
#     ("Active order not found for cancellation"), and the order status route accepts the CLIENT id
#     (NonKYC's own fork queries /getorder/<client_order_id>; the live probe answers 20002 for an unknown one).
#   - Zero balances are NOT omitted: /balances lists all 262 assets, zeros included (probed 2026-11-17).

EXCHANGE_NAME = "nonkyc"
DEFAULT_DOMAIN = "com"

REST_URL = "https://api.nonkyc.io/api/v2"
# The docs' endpoint; api.nonkyc.io answers the same feeds (P1 runs ws.nonkyc.io).
WSS_URL = "wss://ws.nonkyc.io"

ORDER_ID_MAX_LEN = 32
# The fleet's prefix: the asset_manager's `exchange_patterns["NONKYC"]` and the P1 arb lanes key on it. NonKYC's own
# fork uses "HBOT-"; ours is "HBOT" like every venue we built (XT, Hotcoin, CoinW, BloFin, bexc).
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths ---------------------------------------------------------------------------------------------------
SERVER_TIME_PATH = "/time"                       # {"serverTime": <ms>}
MARKETS_PATH = "/market/getlist"                 # every market: symbol "X/USDT", rules, flags (~560 KB)
TICKERS_PATH = "/tickers"                        # every market's last price, one call
ORDERBOOK_PATH = "/market/orderbook"             # ?symbol=X_USDT&limit=N
BALANCES_PATH = "/balances"                      # signed: every asset, zeros included
PLACE_ORDER_PATH = "/createorder"                # signed POST
CANCEL_ORDER_PATH = "/cancelorder"               # signed POST {"id": <exchange id>}
ORDER_PATH = "/getorder/{order_id}"              # signed GET; the CLIENT id is accepted
TRADES_PATH = "/gettrades"                       # signed GET ?symbol=X_USDT
TRADES_SINCE_PATH = "/gettradessince"            # signed GET ?symbol&since(ms)&limit&skip

# --- Wallet (P3 reads them through the asset_manager's own client; recorded here for the venue page) ---------------
DEPOSIT_ADDRESS_PATH = "/getdepositaddress/{ticker}"   # the CHILD ticker: USDT-ERC20, BTC-MAIN ...
DEPOSITS_PATH = "/getdeposits"
WITHDRAWALS_PATH = "/getwithdrawals"
WITHDRAW_PATH = "/createwithdrawal"

# --- Order model --------------------------------------------------------------------------------------------------
SIDE = {TradeType.BUY: "buy", TradeType.SELL: "sell"}
ORDER_TYPE_LIMIT = "limit"
ORDER_TYPE_MARKET = "market"
# Every status the docs and the live examples show. A value outside the map is derived from the amounts, never guessed.
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "ACTIVE": OrderState.OPEN,
    "PARTLY FILLED": OrderState.PARTIALLY_FILLED,
    "PARTIALLY FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
}
TERMINAL_STATES = {OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED}
# An order the venue reports open after Hummingbot failed it is cancelled by its exchange id.
OPEN_STATES = {OrderState.OPEN, OrderState.PARTIALLY_FILLED, OrderState.PENDING_CANCEL}

CODE_ORDER_NOT_FOUND = 20002          # status read AND cancel; HTTP 400, description says which
CODE_BAD_REQUEST = 20001              # "Insufficient funds for order creation" / a field error
CODE_MARKET_NOT_FOUND = 2001          # market not found or paused
CODE_DUPLICATE_NONCE = 10001
CODE_AUTH = 401                       # plain-text "Not Authorized" (a bad signature or a clock skew)
CODE_TRADING_DISABLED = 403
CODE_ENGINE_OVERLOADED = 503

# --- WebSocket ----------------------------------------------------------------------------------------------------
WS_METHOD_SUBSCRIBE_ORDERBOOK = "subscribeOrderbook"
WS_METHOD_SUBSCRIBE_TRADES = "subscribeTrades"
WS_METHOD_LOGIN = "login"
WS_METHOD_SUBSCRIBE_REPORTS = "subscribeReports"
WS_METHOD_SUBSCRIBE_BALANCES = "subscribeBalances"
WS_FRAME_SNAPSHOT_BOOK = "snapshotOrderbook"
WS_FRAME_UPDATE_BOOK = "updateOrderbook"
WS_FRAME_SNAPSHOT_TRADES = "snapshotTrades"
WS_FRAME_UPDATE_TRADES = "updateTrades"
WS_FRAME_REPORT = "report"
WS_FRAME_ACTIVE_ORDERS = "activeOrders"
WS_FRAME_CURRENT_BALANCES = "currentBalances"
WS_FRAME_BALANCE_UPDATE = "balanceUpdate"
WS_AUTH_TIMEOUT = 10.0

# The book: subscribe with limit=100 (updates are full-depth, so the local book needs room), emit the top 20.
SUBSCRIBE_LIMIT = 100
EMIT_DEPTH = 20
# Adjacent frames with the same server timestamp were seen swapped (P1: seq +2 then -1 over 3,337 updates; the same
# pattern on KAS at myserver 2026-11-17). An out-of-order update waits for its predecessor up to this long; a step
# that stays unfilled is a lost frame, and the book is rebuilt from a fresh subscribe (which brings a new snapshot).
REORDER_HOLD_SEC = 2.0
# A book that grew past this many levels a side is trimmed back to the best half.
MAX_LEVELS_PER_SIDE = 2000
# The public socket's own keepalive: the server sends a WS PING every ~60 s and aiohttp answers it (autoping);
# aiohttp also pings after this much silence and closes the socket when no PONG arrives within half of it.
WS_HEARTBEAT_S = 20.0
WS_MAX_MSG_SIZE = 4 * 1024 * 1024
# A socket that dies this soon after connecting (or fails to connect) waits WS_RETRY_BASE, doubling to the cap;
# one that lived longer reconnects at once.
WS_RETRY_BASE = 2.0
WS_RETRY_CAP = 60.0
WS_RETRY_RESET_SEC = 30.0
WS_RETRY_MAX_EXPONENT = 6

# --- Orders / fills: the guards' knobs ----------------------------------------------------------------------------
# While any order is open the status poll runs every 10 s whatever the private stream does (Hotcoin's lesson).
SHORT_POLL_INTERVAL = 10.0
# A placement that got no answer is looked up by client id after these waits; two "Order not found" answers make it
# "absent" (still no proof -- the order stays pending for the poll's own not-found verdict).
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# A terminal push whose fills are not all booked yet waits this long for the trade push before a REST backfill.
FILL_BACKFILL_GRACE = 0.5
# A terminal state whose fill rows are still short is held, never forwarded short, for at most this long.
TERMINAL_HOLD_MAX_S = 5.0
# Two fills are the same fill when their quantity and price match and their times are within this many seconds.
FILL_MATCH_SECONDS = 2.0
# Quantities compared against the venue's totals allow this much rounding.
FILL_AMOUNT_TOLERANCE = Decimal("1e-9")
# A push for an order we placed is expected within this long; none means the private stream isn't delivering.
ORDER_PUSH_EXPECTED_WITHIN = 5.0
ORDER_PUSH_ALARM_INTERVAL = 60.0
# The first live run's audit lines ([NKYC-AUDIT]). Switched off once the open unknowns settle (runbook §1.6);
# money-guard firings log as [NKYC-ALARM] whatever this says.
LIVE_AUDIT_LOGGING = True
BALANCE_AUDIT_PUSHES = 5

# The balance push is the authority (docs: it fires whenever available or held changes -- order placed, filled,
# cancelled, deposit credited, withdrawal debited). Verified against REST during the fill test (§6.3).
REAL_TIME_BALANCE_UPDATE = True

# --- Rate limits --------------------------------------------------------------------------------------------------
# NonKYC's own Hummingbot fork budgets 6000 weight/min across the account, 50 orders per 10 s, 160,000 orders a day
# and 61,000 raw requests per 5 min. The per-route weights are the fork's. No public REST limit is documented
# (a 429 is "Too many requests").
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"
MAX_REQUEST = 5000
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=6000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=50, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=ORDERS_24HR, limit=160000, time_interval=ONE_DAY),
    RateLimit(limit_id=RAW_REQUESTS, limit=61000, time_interval=5 * ONE_MINUTE),
    RateLimit(limit_id=SERVER_TIME_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKERS_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=MARKETS_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDERBOOK_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 100),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=BALANCES_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADES_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADES_SINCE_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 20),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=PLACE_ORDER_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(ORDERS, 1),
                             LinkedLimitWeightPair(ORDERS_24HR, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(ORDERS, 1),
                             LinkedLimitWeightPair(ORDERS_24HR, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDER_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(ORDERS, 1),
                             LinkedLimitWeightPair(ORDERS_24HR, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]
