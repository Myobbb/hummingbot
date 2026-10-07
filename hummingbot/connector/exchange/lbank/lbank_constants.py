from datetime import timedelta, timezone
from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-10-07 from myserver, AWS Tokyo; P1's probes of
# 2026-10-04/05 from brr_ws/1cc/4cc), the docs (offline mirror VS_code_projects/MDs/lbank-api/, English + zh_CN, which
# match), the four official SDKs and CCXT (MDs/lbank-api/sdk/). Where they disagree, the choice and its evidence are
# noted here. Wiki: trading/exchanges/lbank-api.

EXCHANGE_NAME = "lbank"
DEFAULT_DOMAIN = "com"

# Host: api.lbkex.com (Alibaba Hong Kong) for REST and WebSocket alike. The docs' api.lbank.info is Cloudflare in
# front of the same API (same answers, same codes, live 2026-10-07). From myserver: REST round trip p50 58 ms vs 72 ms
# (lbank.info also spiked to 300-490 ms); WS book age p50 29 ms on both, p99 435 vs 548 ms. P1 measured Cloudflare's
# WebSocket proxy stalling every socket from Germany (76.6 vs 0.9 episodes a market-hour) and runs api.lbkex.com since
# 2026-10-04. www.lbkex.net (the SDKs' and CCXT's) is never used: its anti-DDoS blocked an IP after one 120-socket test.
REST_URL = "https://api.lbkex.com"
WSS_URL = "wss://api.lbkex.com/ws/V2/"

# --- REST paths (POST unless noted; every private endpoint is a signed POST) -----------------------------------
SERVER_TIME_PATH = "/v2/timestamp.do"              # GET, data = server ms
PAIRS_PATH = "/v2/currencyPairs.do"                # GET, the API's list of tradable pairs (1,378 on 2026-10-07)
RULES_PATH = "/v2/accuracy.do"                     # GET, every pair's precisions and minimums, one call
DEPTH_PATH = "/v2/depth.do"                        # GET, symbol + size 1..200; lower-case symbols only (P1)
PRICE_PATH = "/v2/supplement/ticker/price.do"      # GET, no symbol = every pair's last price, one call (1,378 rows)
SYSTEM_STATUS_PATH = "/v2/supplement/system_status.do"   # POST, unsigned; data.status "1" normal, "0" maintenance
ACCOUNT_PATH = "/v2/supplement/user_info_account.do"     # balances[] {asset, free, locked}
FEE_RATE_PATH = "/v2/supplement/customer_trade_fee.do"   # the account's per-pair maker/taker rates
PLACE_ORDER_PATH = "/v2/supplement/create_order.do"
CANCEL_ORDER_PATH = "/v2/supplement/cancel_order.do"
# The order query: the docs moved it to /v2/spot/trade/orders_info.do (the supplement path is listed under "Abandoned
# endpoints ... offline soon"); the SDKs and CCXT still use the supplement path. Both answer (2026-10-07, a made-up key
# got 10005 from each, while a missing route answers the gateway's HTML 404). The documented current one is used.
ORDER_QUERY_PATH = "/v2/spot/trade/orders_info.do"   # by orderId or origClientOrderId
TRADE_HISTORY_PATH = "/v2/supplement/transaction_history.do"   # per-trade rows with the exact fee (audit only)
SUBSCRIBE_KEY_PATH = "/v2/subscribe/get_key.do"      # the private socket's subscribeKey, valid 60 min
REFRESH_KEY_PATH = "/v2/subscribe/refresh_key.do"    # +60 min from the call

# --- Responses -------------------------------------------------------------------------------------------------
# Every answer is HTTP 200, business errors included: {"result": "true"|"false" (a boolean on some endpoints),
# "error_code": <int>, "msg": ..., "ts": <ms>, "data": ...}. A missing route answers the gateway's HTML 404.
CODE_OK = 0
CODE_INTERNAL = 10000
CODE_TOO_FREQUENT = 10004
CODE_KEY_NOT_FOUND = 10005       # "Secret key non-existent" (live, a made-up key)
CODE_INVALID_SIGNATURE = 10007
CODE_PAIR_NOT_SUPPORTED = 10008
CODE_PERMISSION_DENIED = 10022   # "API Key permission denied, Invalid IP or permissions"
CODE_CANNOT_TRADE_PAIR = 10024   # "User cannot trade on this pair"
CODE_TIMESTAMP = 10600           # "Request timeout, please check the difference between timestamp and server time"
# Checked BEFORE the key (live 2026-10-07: a 90 s old timestamp with a made-up key answers 10600): such a request was
# refused unexecuted, so LbankExchange._api_request re-syncs the clock and repeats it once, an order placement too.
# Order not found: the docs list 10032 "Order number does not exist"; to be confirmed live on a non-existent order
# (lbank_private_probe.py with a read-only key; [LB-AUDIT] error-code lines). Only these codes fail an order.
ORDER_NOT_FOUND_CODES: tuple = (10032,)
# Cancel refusals that say the order is already final or going: the status read that follows settles it.
CANCEL_FINAL_CODES: tuple = (10025, 10026, 10027, 10037, 10104)
# Our own code for an answer that is ok with no usable data.
CODE_EMPTY_ANSWER = -1

# --- Signing (lbank_auth.LbankAuth) ----------------------------------------------------------------------------
SIGNATURE_METHOD = "HmacSHA256"
ECHOSTR_LENGTH = 35              # 30-40 letters and digits (docs); the SDKs send 35
# custom_id: no length is documented; the docs' examples are 36-character UUIDs. HBOT ids are alphanumeric.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- Orders ------------------------------------------------------------------------------------------------------
# type buy | sell = a limit order (price + amount); buy_maker / _ioc / _fok and market types exist but are not used.
TRADE_TYPES = {TradeType.BUY: "buy", TradeType.SELL: "sell"}

# status (REST order query and the cancel answer): -1 cancelled, 0 unfilled, 1 partially filled, 2 filled,
# 3 partially filled then cancelled, 4 cancel in progress (docs, English and Chinese alike). The order push's
# orderStatus uses the same numbers without 3 (docs). A value outside this map is derived from the amounts, never
# guessed (LbankExchange._order_state).
ORDER_STATE = {
    -1: OrderState.CANCELED,
    0: OrderState.OPEN,
    1: OrderState.PARTIALLY_FILLED,
    2: OrderState.FILLED,
    3: OrderState.CANCELED,
    4: OrderState.PENDING_CANCEL,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)

# A placement that got no answer is looked up by its client id (origClientOrderId) after these delays (s) before
# it may fail (LbankExchange._find_order_by_client_id).
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# A placement waits this long (s) for LBank's answer; then the lookup above decides. Orders are refused when they
# reach the matching engine more than 5 s after their timestamp (docs, `window`), so 10 s covers it.
PLACE_ORDER_TIMEOUT = 10.0

# An order push that can't be matched yet (no customerID echoed and the placement answer not back) is parked this
# long (s) for the placement answer to give the order its exchange id.
PENDING_PUSH_TTL_SECONDS = 30.0

# Every accepted order is expected to get a push (CCXT's 2024 sample: orderStatus 0 on creation). None this many
# seconds after the placement answer means the private stream is not delivering: orders and balances are polled over
# REST at once, with an [LB-ALARM] at most every ORDER_PUSH_ALARM_INTERVAL s.
ORDER_PUSH_EXPECTED_WITHIN = 3.0
ORDER_PUSH_ALARM_INTERVAL = 600.0

# A terminal update (FILLED, or CANCELED after fills) waits until the base asset's total balance shows the fills, at
# most this long (s); then balances are read over REST and the update goes anyway, with an [LB-ALARM]. Hotcoin pushed
# fills 0.4-3.7 s before the balance, and the hold-band reads the total on completion (it bought AEON twice,
# 2026-10-06). Whether LBank's assetUpdate comes before or after the orderUpdate is unknown: if it comes first, no
# terminal update ever waits.
FILL_BALANCE_WAIT_SECONDS = 5.0

# Fills: the order push (orderUpdate) carries the order's CUMULATIVE filled quantity (accAmt) plus the latest trade
# (txUuid, amount, volumePrice, price, role); the REST order query carries the cumulative quantity and value
# (executedQty, cummulativeQuoteQty). Neither carries a fee. Each new cumulative total becomes one fill of the
# difference (LbankExchange._fill_from_cumulative), so the push and the REST poll can never count a fill twice.
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
# A REST order query answer is reused for this long (s) by the status poll that follows the fills poll.
ORDER_DETAIL_CACHE_SECONDS = 2.0

# Fees: no fill carries one; each fill's fee = the account's rate for its pair (customer_trade_fee.do, maker or taker
# by the push's `role`, taker when unknown) x the received asset (the base on a buy, the quote on a sell: CCXT, and
# the docs' "the billing unit for buy orders is the transaction currency"). The first live fills are checked against
# LBank's own per-trade commission (transaction_history.do, [LB-AUDIT] fee-check). The rates are FRACTIONS: LIVE
# 2026-10-07 btc_usdt reads "0.001" = 0.1%, LBank's standard spot rate and CCXT's default; the docs' example "0.10" is
# not the live unit (as a percent, the live row would be 0.001%).
FEE_RATE_IS_PERCENT = False
FEE_CHECK_ORDERS = 10

# --- Public WebSocket ------------------------------------------------------------------------------------------
# `depth`: the market's whole top-N book, pushed on change on a ~600 ms tick, plus once right after the subscribe. No
# sequence id; TS = the push's server time (UTC+8), `s` = the publishing node. One market per message and per socket:
# LBank's gateway delivers only ~14 KB/s a socket and QUEUES the rest (P1 2026-10-04: 10 markets a socket put 14% of
# pushes > 3 s late, 1 a socket 0.34%). There is no other book channel (incrDepth, bookTicker answer nothing) and no
# cadence parameter (P1). Public trades are not subscribed: no strategy here reads them, and they would share each
# socket's queue (the tracker falls back to PRICE_PATH for a quiet book's last price).
# Depth 10, not 20 (arb_l walks up to 20): LBank's delays grow with the book's size. From myserver, the same 10
# markets, 5 min each (2026-10-07): depth 10 age p99 435 ms, worst 1.6 s, 0 pushes > 3 s; depth 20 p99 634 ms, worst
# 3.1 s, per-market p99 up to 1.5 s. P1 found the same from Germany (stall episodes 2.2 vs 5.6 a market-hour).
WS_DEPTH = 10
# Levels per side handed to Hummingbot's order book: all of them.
EMIT_DEPTH = WS_DEPTH
# Two publishing nodes per market, two different books: `s` = t-NN matches REST (P1: 91% exact top), m-NN is often
# stale (2%). Only t- books are used; the others are counted.
BOOK_NODE_PREFIX = "t-"
# A push whose own TS is more than this old when it arrives carries a stale book (a socket's backlog flushing after a
# stall): never shown; the market's book is shown EMPTY until a fresh push. P1's only freshness guard (no silence
# guard: silence can't tell a stall from a quiet push-on-change market). From myserver: age p50 29 ms, p99 435 ms,
# max 1.6 s, 0 of ~22k pushes past 3 s (2026-10-07, 10 markets, 5 min).
STALE_PUSH_SEC = 3.0
STALE_LOG_EVERY_SEC = 60.0
# The server's TS is in UTC+8.
SERVER_TZ = timezone(timedelta(hours=8))
# Keepalive: the client pings {"action":"ping","ping":id} every WS_PING_INTERVAL s and answers the server's pings
# (the server drops a client that leaves its ping unanswered for a minute). No message for WS_MESSAGE_TIMEOUT s (three
# pongs missed) = a dead socket: it is reconnected and its book shown empty meanwhile.
WS_PING_INTERVAL = 10.0
WS_MESSAGE_TIMEOUT = 35.0
# A socket's handshake (TCP + TLS to Hong Kong + the upgrade) gets this long; then the attempt counts as failed and
# backs off (myserver: 221 ms median).
WS_CONNECT_TIMEOUT = 20.0
# Sockets are opened this far apart: LBank's host throttles connection BURSTS per IP (P1).
WS_CONNECT_SPACING = 0.1
# A market the stream hasn't served this long after its (re)subscribe gets a REST /v2/depth.do book (the first push
# after a subscribe can come from an m- node, P1: 56 of 677; a quiet market then pushes only on change). The same
# happens this long after a late or crossed push emptied a book: a quiet market would otherwise stay empty until it
# changes.
SEED_DELAY = 3.0
# Freshness, by LBank's own book (there is no sequence id): every FRESHNESS_INTERVAL s the market silent the longest
# (at least FRESHNESS_QUIET_SECONDS since it was last known current) is compared with REST /v2/depth.do, top
# FRESHNESS_LEVELS a side. A book that differs, gets no push within FRESHNESS_GRACE s, and still differs on a second
# REST read (a level a bot placed and pulled inside one push tick is no miss) missed an update, or its socket is
# stalled or its subscription dropped: that market's socket is reconnected, and the new subscribe brings the current
# book. Hotcoin's probe (2026-10-05), one market per socket here.
FRESHNESS_INTERVAL = 10.0
FRESHNESS_QUIET_SECONDS = 15.0
FRESHNESS_LEVELS = 5
FRESHNESS_GRACE = 3.0
# A book the tracker takes from _order_book_snapshot is emitted again this long after: on a runtime add the tracker
# replays any push it parked meanwhile as a DIFF over that snapshot, which would merge two whole books; the
# re-emitted SNAPSHOT replaces the merge (Hotcoin's rule).
SNAPSHOT_REEMIT_DELAY = 0.5

# LBank's trading switch: the API has no per-market trading flag. A pair is tradable while it is listed by
# PAIRS_PATH (a delisted pair leaves it, P1); the whole venue is down while SYSTEM_STATUS_PATH says "0". While a
# tracked market is not listed, or the system is in maintenance, its book is shown EMPTY (the XT rule, 2026-09-26);
# its symbol and trading rule stay. Read every TRADING_SWITCH_INTERVAL s.
TRADING_SWITCH_INTERVAL = 60

# Reconnect pacing (lbank_web_utils.LbankReconnectBackoff). Every wait adds a random 0..WS_RETRY_JITTER s, so
# sockets that dropped together (a network blip) don't reconnect in one burst, nor in lockstep at each backoff step:
# LBank's host refuses connection BURSTS per IP (P1: a 677-connection burst refused whole). 50 sockets spread over
# 2.5 s = 20 a second, the rate P1's production restarts pass.
WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0
WS_RETRY_JITTER = 2.5

# --- Private WebSocket ---------------------------------------------------------------------------------------
# The same socket host. A subscribeKey (REST, signed) authorises `orderUpdate` (pair "all") and `assetUpdate`; it is
# valid 60 min from its creation or last refresh. Its age is tracked across connections (CCXT's pro lbank keeps an
# absolute expiry too): it is refreshed once SUBSCRIBE_KEY_REFRESH s old, on a connection or before the next one, and a
# key that could not be refreshed, or is SUBSCRIBE_KEY_MAX_AGE s old, is replaced by a new one.
SUBSCRIBE_KEY_REFRESH = 1800.0
SUBSCRIBE_KEY_MAX_AGE = 3000.0
WS_SUBSCRIBE_ORDERS = {"action": "subscribe", "subscribe": "orderUpdate", "pair": "all"}
WS_SUBSCRIBE_ASSETS = {"action": "subscribe", "subscribe": "assetUpdate"}

# --- Balance source (runbook §6.3) -----------------------------------------------------------------------------
# True = the WS asset push is authoritative between REST polls (Hummingbot's default). Set False if the fill test
# shows LBank's assetUpdate missing events (BitMart's stale-balance overbuy, 2026-07-24).
REAL_TIME_BALANCE_UPDATE = True
BALANCE_AUDIT_PUSHES = 5

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [LB-AUDIT], scoped to what the docs could not settle. Switch off once phase 6 has answered them
# (runbook §1.6). Money-guard firings are not audit lines: they always log as [LB-ALARM] WARNINGs.
LIVE_AUDIT_LOGGING = True

# --- Rate limits -------------------------------------------------------------------------------------------------
# Docs: "Create order and cancel order 500/10s, the other requests 200/10s", per API key (the Chinese page: 单个 API
# Key 维度限制). Public requests: 200/10s per IP (P1: 18/s clean, 30/s -> 20% refused with 10004).
ORDERS_LIMIT_ID = "lbank_orders"
PRIVATE_LIMIT_ID = "lbank_private"
PUBLIC_LIMIT_ID = "lbank_public"
_ORDERS = [LinkedLimitWeightPair(ORDERS_LIMIT_ID)]
_PRIVATE = [LinkedLimitWeightPair(PRIVATE_LIMIT_ID)]
_PUBLIC = [LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]
RATE_LIMITS = [
    RateLimit(limit_id=ORDERS_LIMIT_ID, limit=500, time_interval=10),
    RateLimit(limit_id=PRIVATE_LIMIT_ID, limit=200, time_interval=10),
    RateLimit(limit_id=PUBLIC_LIMIT_ID, limit=180, time_interval=10),
    RateLimit(limit_id=PLACE_ORDER_PATH, limit=500, time_interval=10, linked_limits=_ORDERS),
    RateLimit(limit_id=CANCEL_ORDER_PATH, limit=500, time_interval=10, linked_limits=_ORDERS),
    RateLimit(limit_id=ORDER_QUERY_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=ACCOUNT_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=FEE_RATE_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=TRADE_HISTORY_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=SUBSCRIBE_KEY_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=REFRESH_KEY_PATH, limit=200, time_interval=10, linked_limits=_PRIVATE),
    RateLimit(limit_id=SERVER_TIME_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
    RateLimit(limit_id=PAIRS_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
    RateLimit(limit_id=RULES_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
    RateLimit(limit_id=DEPTH_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
    RateLimit(limit_id=PRICE_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
    RateLimit(limit_id=SYSTEM_STATUS_PATH, limit=180, time_interval=10, linked_limits=_PUBLIC),
]
