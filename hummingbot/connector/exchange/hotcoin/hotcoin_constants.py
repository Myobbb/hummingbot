from decimal import Decimal

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-10-05 from the Mac and from myserver), the
# docs (offline mirror VS_code_projects/MDs/hotcoin-api/; the CHINESE page zh_CN/spot-full.md is the complete
# one: clientOrderId, /v3/balance, minOrderAmount and statusCode 8 are missing from the English page), and the
# official SDK (github.com/hotcoinexchange/hotcoin-api-python, mirrored in MDs/hotcoin-api/sdk/). Where they
# disagree, the choice and its evidence are noted here.

EXCHANGE_NAME = "hotcoin"
DEFAULT_DOMAIN = "com"

REST_URL = "https://api.hotcoinfin.com"
# Signed requests sign this host, lowercase, whatever URL they are sent to (docs; SDK HOTCOIN_REST_SIGN).
REST_SIGN_HOST = "api.hotcoinfin.com"
# One WebSocket host for public and private topics; every server frame is binary gzip JSON (live).
WSS_URL = "wss://wss.hotcoinfin.com/trade/multiple"
WS_SIGNIN_HOST = "wss.hotcoinfin.com"
WS_SIGNIN_PATH = "signin"      # signed as-is, no leading slash (docs' worked string; SDK build_websocket_signin)

# clientOrderId: "max 64 characters" (Chinese docs, place order); the docs' own examples are 32 hex characters.
# 32 is used: HBOT ids are alphanumeric and stay unique at 32 (prefix + side + pair + nonce).
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths (REST_URL has no path prefix, so these are also the signed paths) -------------------------------
SYMBOLS_PATH = "/v1/common/symbols"     # markets + rules; 10/s
TICKER_PATH = "/v1/market/ticker"       # no symbol = every market in one call; 10/s
DEPTH_PATH = "/v1/depth"                # 100 levels a side, [price, qty] strings; 20/s
TRADES_PATH = "/v1/trade"               # public trades; 20/s. Also the server-time source (below)
BALANCE_PATH = "/v3/balance"            # free / frozen / total per asset (Chinese docs, SDK); 10/s
PLACE_ORDER_PATH = "/v1/order/place"    # 10/s
CANCEL_ORDER_PATH = "/v1/order/cancel"  # 10/s, asynchronous
ORDER_DETAIL_PATH = "/v1/order/detailById"   # 10/s
ORDER_LIST_PATH = "/v1/order/entrust"   # current + history, carries clientOrderId; 10/s

# Hotcoin has NO server-time endpoint: /v1/common/timestamp, /v1/time and six other shapes answer
# 10170 "API未开放" (live 2026-10-05). Every response envelope carries `time` in MILLISECONDS instead, errors
# included. The smallest public call is /v1/trade?count=1 (~250 bytes).
SERVER_TIME_PATH = TRADES_PATH
SERVER_TIME_PARAMS = {"symbol": "btc_usdt", "count": 1}

# --- Responses -----------------------------------------------------------------------------------------------
# Every answer is HTTP 200, business errors included: {"code": <int>, "msg": ..., "time": <ms>, "data": ...}.
# The ticker alone answers {"status": "ok", "ticker": [...], "timestamp": <s>}. Codes seen live 2026-10-05:
CODE_OK = 200
CODE_PARAM_ERROR = 1000          # missing parameter ("参数绑定异常【AccessKeyId】") and a stale timestamp
MSG_TIMESTAMP_OUT_OF_RANGE = "Timestamp out of range"   # code 1000: the only clock-drift answer
CODE_API_NOT_OPEN = 10170        # unknown path
CODE_INVALID_API_KEY = 10173     # "无效API KEY"
CODE_SYMBOL_INVALID = 40008      # "symbol有误..." (the symbol is wrong, not listed or disabled)
CODE_TOO_FREQUENT = 20724        # "API requests too frequent" (P1, 2026-09-27)
# Order not found on detailById / cancel: NOT YET SEEN. Filled from the read-only key probe (S4) before go-live;
# until then a not-found answer is handled as any other refusal (the order's status poll settles it).
ORDER_NOT_FOUND_CODES: tuple = ()
# Our own code for an answer that is code 200 with no data (detailById of an unknown order might answer so): it gets
# its own number, so S4 shows it as such, and it can join ORDER_NOT_FOUND_CODES if that is Hotcoin's not-found.
CODE_EMPTY_ANSWER = -1

# The signed Timestamp is accepted up to at least 60 s off the server clock and refused at 90 s
# (live 2026-10-05, a bogus key: the timestamp is checked before the key). ISO UTC with milliseconds, as the docs.

# --- Orders --------------------------------------------------------------------------------------------------
TRADE_TYPES = {TradeType.BUY: "buy", TradeType.SELL: "sell"}
MATCH_TYPE_LIMIT = 0             # 0 limit, 1 market, 7 post only, 8 FOK, 9 IOC (docs)

# statusCode: 1 unfilled, 2 partially filled, 3 filled, 4 cancel in progress, 5 cancelled,
# 8 partially filled then cancelled (Chinese docs; the English page lists 1-5 only). A code outside this map is
# derived from the amounts, never guessed (HotcoinExchange._order_state).
ORDER_STATE = {
    1: OrderState.OPEN,
    2: OrderState.PARTIALLY_FILLED,
    3: OrderState.FILLED,
    4: OrderState.PENDING_CANCEL,
    5: OrderState.CANCELED,
    8: OrderState.CANCELED,
}

# A placement that got no answer is looked up by clientOrderId in /v1/order/entrust after these delays (s)
# before it may fail (HotcoinExchange._find_order_by_client_id).
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# A placement request waits this long (s) for Hotcoin's answer; then the lookup above decides.
PLACE_ORDER_TIMEOUT = 10.0

# An order push that can't be matched yet (no clientOrderId echoed and the placement answer not back) is parked
# this long (s) for the placement answer to give the order its exchange id.
PENDING_PUSH_TTL_SECONDS = 30.0

# Every accepted order gets a `created` push (docs). An order with no push at all this many seconds after its
# placement answer means the private stream is not delivering (its topic acks prove nothing): the orders and
# balances are polled over REST at once, with an [HC-ALARM] at most every ORDER_PUSH_ALARM_INTERVAL s. Without
# this, the server's 5 s pings keep the stream looking alive and the REST poll stays at its 60 s interval.
ORDER_PUSH_EXPECTED_WITHIN = 3.0
ORDER_PUSH_ALARM_INTERVAL = 600.0
# Live 2026-10-06: created/trade pushes came for 2 of 7 orders (a cancel push did come). So while any order is open
# the status poll runs every SHORT_POLL_INTERVAL (10 s) instead of 60 s (HotcoinExchange._get_poll_interval).

# Hotcoin pushes an order's fill BEFORE the balance that holds it: 0.46 s and ~2.5 s later on 2026-10-06. A terminal
# update (FILLED, or CANCELED after fills) waits until the base asset's total balance shows the fills, at most this
# long (s); then balances are read over REST and the update is reported anyway, with an [HC-ALARM]. Reported first,
# the hold-band refreshed its total on completion, read AEON as 0 and bought it twice (2026-10-06).
FILL_BALANCE_WAIT_SECONDS = 5.0

# Fills: the order push and /v1/order/detailById carry the order's CUMULATIVE filled quantity, filled value and
# fee, never a trade id. Each new cumulative total becomes one fill of the difference
# (HotcoinExchange._fill_from_cumulative), so the push and the REST poll can never count a fill twice.
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
# A detailById answer is reused for this long (s) by the status poll that follows the fills poll.
ORDER_DETAIL_CACHE_SECONDS = 2.0

# --- Public WebSocket ------------------------------------------------------------------------------------------
# market.<s>.trade.depth: the WHOLE book (<=100 levels a side) on change. Measured from myserver 2026-10-05
# (120 s, 8 markets, hmb_local_tools/hotcoin/debug_hotcoin_ws_cadence.py): ~1 push/s on active markets
# (gap p50 0.87-1.22 s), arriving 37-40 ms after the push's own ts; quiet markets push only on change
# (TBK ~17 s apart). trade.bbo pushes at exactly the same moments (same gaps, +1 ms), so depth is already
# the fastest book channel and bbo adds nothing.
WS_DEPTH_TOPIC = "market.{symbol}.trade.depth"
WS_TRADE_TOPIC = "market.{symbol}.trade.detail"
# Levels per side handed to Hummingbot's order book on every push. arb_l walks at most 20.
EMIT_DEPTH = 50
# One topic per message: a list or a comma-joined `sub` is ACKed but never streams (P1, live). Every sub is
# ACKed code 200 even for a market that does not exist; the ack is not proof of a stream.
WS_SUBSCRIBE_SPACING = 0.02
# The server pings {"ping":"ping"} every 5 s (live: 5.00 s p50) and expects {"pong":"pong"}. No message for
# this long means a dead connection.
WS_MESSAGE_TIMEOUT = 30.0

# No book on subscribe for a market that doesn't change: after every (re)subscribe, each market the stream has
# not served SEED_DELAY seconds later gets a REST /v1/depth book, SEED_INTERVAL apart (P1's snapshot-first
# bootstrap, 2026-09-27).
SEED_DELAY = 1.5
SEED_INTERVAL = 0.1

# Freshness, by Hotcoin's own book (there is no sequence id): every FRESHNESS_INTERVAL s the market silent the
# longest (at least FRESHNESS_QUIET_SECONDS) is compared with REST /v1/depth, top FRESHNESS_LEVELS a side. A book
# that differs, gets no push within FRESHNESS_GRACE s, and still differs on a second REST read (so a level placed and
# pulled inside one push tick is not mistaken for a miss) missed an update: it takes REST's book, is re-subscribed and
# is probed again next. FRESHNESS_MISSES_TO_RECONNECT misses of the SAME market in a row (no push in between)
# reconnect the stream.
FRESHNESS_INTERVAL = 10.0
FRESHNESS_QUIET_SECONDS = 15.0
FRESHNESS_LEVELS = 5
FRESHNESS_GRACE = 3.0           # the longest push gap measured on an active market was 2.7 s (myserver, 2026-10-05)
FRESHNESS_MISSES_TO_RECONNECT = 2

# A book the tracker takes from _order_book_snapshot is emitted again this long after: on a runtime add the tracker
# replays any push it parked meanwhile as a DIFF over that snapshot, which would merge two whole books; the re-emitted
# SNAPSHOT replaces the merge.
SNAPSHOT_REEMIT_DELAY = 0.5

# Hotcoin's trading switch: /v1/common/symbols `state` (enable | disable). All 364 markets read `enable` on
# 2026-10-05, including GNR, a pre-listing whose book is empty until 2026-10-10. While a tracked market is not
# listed or not `enable`, its book is shown EMPTY (the XT rule, 2026-09-26); its symbol and trading rule stay.
TRADING_SWITCH_INTERVAL = 60

# Reconnect pacing (hotcoin_web_utils.HotcoinReconnectBackoff): a connection that failed, or lived less than
# WS_RETRY_RESET_SEC, waits 1, 2, 4 ... WS_RETRY_CAP s before the next attempt (XT's hot-loop lesson, 2026-09-30).
WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Private WebSocket ---------------------------------------------------------------------------------------
# {"signin": {accessKey, timestamp (ms), signature}} first; answer {"ch":"signin","code":200,"status":"ok"}, a bad
# key {"code":106,"msg":"登录失败","status":"error"} (live). Topic acks come for anything, signed in or not
# (live: market.trade.entrust.change was ACKed on an anonymous socket), so only the signin answer proves auth.
WS_SIGNIN_TIMEOUT = 10.0
WS_TOPIC_ORDERS = "market.trade.entrust.change"
WS_TOPIC_BALANCE = "market.trade.asset.balance"
CODE_WS_LOGIN_FAILED = 106

# --- Balance source (runbook §6.3) -----------------------------------------------------------------------------
# True = the WS balance push is authoritative between REST polls (Hummingbot's default). Set False if the fill
# test shows Hotcoin's asset push missing events (BitMart's stale-balance overbuy, 2026-07-24).
REAL_TIME_BALANCE_UPDATE = True
BALANCE_AUDIT_PUSHES = 5

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [HC-AUDIT], scoped to what the docs could not settle. Switch off once phase 6 has answered
# them (runbook §1.6). Money-guard firings are not audit lines: they always log as [HC-ALARM] WARNINGs.
LIVE_AUDIT_LOGGING = True

# --- Rate limits (per-endpoint "Rate Limit" lines of the docs) ---------------------------------------------------
RATE_LIMITS = [
    RateLimit(limit_id=SYMBOLS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TICKER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=DEPTH_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=TRADES_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=BALANCE_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=PLACE_ORDER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=ORDER_DETAIL_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=ORDER_LIST_PATH, limit=10, time_interval=1),
]
