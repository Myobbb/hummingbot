from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-11-08 from myserver with the account's key,
# hmb_local_tools/coinstore/), the docs (offline mirror VS_code_projects/MDs/coinstore-api/: English, plus the
# Chinese page cn/coinstore-api-cn.md, which alone lists the full ordState enum), and Coinstore's own Python SDK
# (MDs/coinstore-api/_sdk/coinstore.py). P1 runs Coinstore since 2026-10-05: wiki trading/exchanges/coinstore-api.
# Where they disagree, the choice and its evidence are noted here.

EXCHANGE_NAME = "coinstore"
DEFAULT_DOMAIN = "com"

REST_URL = "https://api.coinstore.com/api"
# One public socket host for books and trades, and the private stream (LOGIN, then !@order / !@account). Cloudflare;
# from myserver the Tokyo edge answers in ~25 ms and the upgrade takes ~30 ms (2026-11-08). TEXT JSON frames, no
# compression negotiated, no handshake extras.
WSS_URL = "wss://ws.coinstore.com/s/ws"

# clOrdId: "Unique customer-defined order ID" (docs, no length given); the docs' own examples are 32 characters.
# A v2 orderInfo by an unknown clOrdId answers 4003 "Order Not Found" (live), so a placement left unanswered can be
# looked up by it — but ONLY while the order is open: once it is done (filled or cancelled) its clOrdId reads "0" and the
# lookup by clOrdId answers 4003 (live 2026-11-08). The body's `timestamp` (ms) is kept as the order's own `timestamp`,
# exactly (13 of 13 orders, live): the connector sends a unique one per order and uses it as the order's second id.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"
# Docs: a regular account holds at most 50 active orders at a time (market-maker accounts are exempt). Not enforced
# here; a refusal for it is logged like any other.
MAX_ACTIVE_ORDERS = 50

# --- REST paths (under REST_URL, which already ends in /api) ----------------------------------------------------
SYMBOLS_PATH = "/v2/public/config/spot/symbols"   # POST {}: 376 markets (370 USDT), rules and fees, public
TICKERS_PATH = "/v1/market/tickers"               # every market's 24h ticker in ONE call (~120 KB), public
DEPTH_PATH = "/v1/market/depth/{symbol}"          # ?depth=5|10|20|50|100 (no depth = 5 levels; docs say 20), public
BALANCE_PATH = "/spot/accountList"                # POST {}: one row per currency and type (AVAILABLE / FROZEN)
PLACE_ORDER_PATH = "/trade/order/place"           # POST
CANCEL_ORDER_PATH = "/trade/order/cancel"         # POST {symbol, ordId}
ORDER_INFO_PATH = "/v2/trade/order/orderInfo"     # GET ordId= | clOrdId=
OPEN_ORDERS_PATH = "/v2/trade/order/active"       # GET [symbol=]
FILLS_PATH = "/trade/match/accountMatches"        # GET symbol= [ordId= pageNum= pageSize=]: per-fill rows
NETWORK_CHECK_PATH = "/v1/market/depth/BTCUSDT"   # the smallest public answer (depth=5, ~0.5 KB)

# --- Responses ------------------------------------------------------------------------------------------------
# Every answer is HTTP 200, business errors included: {"code": 0 | "0", "message"/"msg", "data"}. The code is the
# INTEGER 0 on the market and trading routes and the STRING "0" on the config / funding routes (live): compare as text.
CODE_OK = "0"
CODE_UNAUTHORIZED = "1401"        # IP outside the key's whitelist, a bad signature, an expired key (docs + live)
CODE_SIGNATURE_FAILED = "401"     # unknown key, or no key ("user-not-login")
CODE_ORDER_NOT_FOUND = "4003"     # "Order Not Found": v1 + v2 orderInfo, by ordId and by clOrdId (live 2026-11-08)
CODE_ORDER_NOT_FOUND_DOC = "3103" # "order-not-found" in the docs' error table; never seen live, accepted too
CODE_SYMBOL_NOT_FOUND = "3011"    # docs
CODE_DUPLICATE_ORDER = "3111"     # docs
CODE_INSUFFICIENT = "3113"        # docs
CODE_TOO_MANY = "429"             # the API's own rate limit: HTTP 200 + code 429 (live on the cancel and placement routes)
# Per-pair refusals of API trading (the placement probe's sweep, 2026-11-08; undocumented): BTC, ETH, SOL and XRP answer
# 3130 "This currency pair does not support API Key trading."; PRO answers 500 "Trading is currently not supported for
# this trading pair." while its market trades. 69 of P1's 70 Coinstore markets accept orders. No endpoint reports either
# (wiki coinstore-api §Placement probes), so P1 filters such pairs out at root and P2 trades only what it gets
# (runbook non-negotiable 3). 3130 is a definite refusal; 500 is a generic code, so a placement answering it is looked
# up before it fails (PLACE_UNKNOWN_CODES).
CODE_PAIR_NOT_API_TRADABLE = "3130"
CODE_SERVER_ERROR = "500"
CODE_SYSTEM_ERROR = "10001"       # e.g. the withdrawal history without `limit` (live)
ORDER_NOT_FOUND_CODES = (CODE_ORDER_NOT_FOUND, CODE_ORDER_NOT_FOUND_DOC)

# --- Rate limits (measured from myserver 2026-11-08; the docs' 300 / 3 s per IP and 120 / 3 s per user are not what
# binds) --------------------------------------------------------------------------------------------------------
# 1. Cloudflare's shared bucket, per IP: balances, fills, the open-order lists, both cancel routes and the placement
#    route share ONE budget of ~20 calls per ~10 s. Past it Cloudflare answers HTTP 429 with an HTML page
#    (Retry-After: 0); pushed further it answers Error 1015 (a JSON 429, retry_after 30) and shuts the IP out for 30 s
#    (the placement probe, 2026-11-08). 1.5/s on each of three routes together: half refused. The AM reads balances from
#    the same IP (myserver), so HMB keeps CF_POOL_LIMIT of it.
# 2. Coinstore's own per-route spacing on the trading routes: ONE cancel per second, account-wide (0.92 s apart: the
#    second refused with code 429; 1.02 s: both pass; a BTC cancel and an ETH cancel back to back: the second refused;
#    a cancel and a cancel-by-clOrdId back to back: both pass — per route). The placement route the same: two
#    placements 13 ms and 45 ms after a refused one answered 429 "too-many-visits" (the placement probe, 11:46 UTC).
# 3. Not in the bucket, no limit met at 10/s: the v1 + v2 orderInfo reads (the status poll's route); the deposit /
#    withdrawal histories, the currency route and the public depth at 3/s (36 of 36).
CF_POOL = "cf_pool"
CF_POOL_LIMIT = 14
CF_POOL_INTERVAL = 10.0
# The reads' share of the pool: balances, fill rows and open-order lists together take at most CF_READS_LIMIT of it,
# so placements and cancels always find room (with a deaf private socket the reads run 1/s per order behind).
CF_READS = "cf_reads"
CF_READS_LIMIT = 10
TRADE_ROUTE_SPACING = 1.05       # one call per route per second, plus margin
# Cloudflare's Error 1015 shuts the IP out for its `retry_after` (30 s live): every pool route then refuses at once,
# sending nothing, until it is over (a request sent meanwhile only extends nothing and spends nothing).
CF_BAN_DEFAULT_SECONDS = 30.0
CF_POOL_PATHS = frozenset({BALANCE_PATH, FILLS_PATH, OPEN_ORDERS_PATH, PLACE_ORDER_PATH, CANCEL_ORDER_PATH})
# Every request's own timeout (s); the placement has PLACE_ORDER_TIMEOUT. aiohttp's default is 300 s, which would hold
# the status poll (it reads orders one after another) for 5 min on one stalled read.
REQUEST_TIMEOUT = 10.0

RATE_LIMITS = [
    RateLimit(limit_id=CF_POOL, limit=CF_POOL_LIMIT, time_interval=CF_POOL_INTERVAL),
    RateLimit(limit_id=CF_READS, limit=CF_READS_LIMIT, time_interval=CF_POOL_INTERVAL),
    RateLimit(limit_id=BALANCE_PATH, limit=CF_POOL_LIMIT, time_interval=CF_POOL_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(CF_POOL), LinkedLimitWeightPair(CF_READS)]),
    RateLimit(limit_id=FILLS_PATH, limit=CF_POOL_LIMIT, time_interval=CF_POOL_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(CF_POOL), LinkedLimitWeightPair(CF_READS)]),
    RateLimit(limit_id=OPEN_ORDERS_PATH, limit=CF_POOL_LIMIT, time_interval=CF_POOL_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(CF_POOL), LinkedLimitWeightPair(CF_READS)]),
    RateLimit(limit_id=PLACE_ORDER_PATH, limit=1, time_interval=TRADE_ROUTE_SPACING,
              linked_limits=[LinkedLimitWeightPair(CF_POOL)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH, limit=1, time_interval=TRADE_ROUTE_SPACING,
              linked_limits=[LinkedLimitWeightPair(CF_POOL)]),
    RateLimit(limit_id=ORDER_INFO_PATH, limit=8, time_interval=1),
    RateLimit(limit_id=SYMBOLS_PATH, limit=1, time_interval=1),
    RateLimit(limit_id=TICKERS_PATH, limit=1, time_interval=1),
    RateLimit(limit_id=DEPTH_PATH, limit=5, time_interval=1),
    RateLimit(limit_id=NETWORK_CHECK_PATH, limit=1, time_interval=1),
]

# --- Orders ---------------------------------------------------------------------------------------------------
TRADE_TYPES = {TradeType.BUY: "BUY", TradeType.SELL: "SELL"}
ORDER_TYPE_LIMIT = "LIMIT"
TIME_IN_FORCE_GTC = "GTC"

# ordState / ordStatus, the Chinese docs' full enum (orderInfo): NOT_FOUND, REJECTED, SUBMITTING, SUBMITTED,
# PARTIAL_FILLED, REPLACING, REPLACED, CANCELING, CANCELED, EXPIRED, STOPPED, FILLED. A value outside this map is
# derived from the amounts and audit-logged, never guessed (CoinstoreExchange._order_state).
ORDER_STATE = {
    "SUBMITTING": OrderState.OPEN,          # accepted by the gateway, on its way to the book
    "SUBMITTED": OrderState.OPEN,
    "PARTIAL_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELING": OrderState.PENDING_CANCEL,
    "CANCELED": OrderState.CANCELED,
    "FILLED": OrderState.FILLED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.CANCELED,         # done, the rest dropped (an IOC's remainder)
    "STOPPED": OrderState.CANCELED,         # done by the venue; audit-logged when seen
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)
# The NUMERIC states of the `!@order` pushes (`ordState`) and the fill rows (`orderState`), live 2026-11-08: 11 accepted
# and open, 12 partially filled (fill rows), 31 cancelled, 50 filled. The REST status reads name the state instead
# (ORDER_STATE). A number outside this map is derived from the amounts and audit-logged, and the status read settles it.
ORDER_STATE_CODES = {
    11: OrderState.OPEN,
    12: OrderState.PARTIALLY_FILLED,
    31: OrderState.CANCELED,
    50: OrderState.FILLED,
}
OPEN_STATES = (OrderState.OPEN, OrderState.PARTIALLY_FILLED)

# Our own code for an answer that is code 0 with no row (never seen live).
CODE_EMPTY_ANSWER = "-1"

# An order that fills completely on arrival reads 4003 for a moment (live, twice: +0.02 s 4003, +0.3 s FILLED; a resting
# partial reads PARTIAL_FILLED at once). A not-found for an order younger than this (s) is no strike: the poll asks again.
ORDER_NOT_FOUND_MIN_AGE = 10.0

# A placement request waits this long (s) for Coinstore's answer; then the lookup decides.
PLACE_ORDER_TIMEOUT = 10.0
# A placement that got no answer is looked up after these delays (s) before it may fail: the order's id from a push, then
# the v2 orderInfo by clOrdId (free; finds the order while it is open), and at the last look, for an order already done
# (its clOrdId wiped), the market's newest fill rows (one read from the Cloudflare bucket) for an order of ours with the
# body's exact `timestamp`, side, price and quantity (CoinstoreExchange._find_done_order_by_send_time).
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# Codes the venue may answer for an order it then creates (CoinW's lesson: -3, then the order 0.45 s later). 10001
# "System Error" is Coinstore's catch-all and 500 a generic one; a placement answering either is looked up for longer.
PLACE_UNKNOWN_CODES = (CODE_SYSTEM_ERROR, CODE_SERVER_ERROR)
PLACEMENT_AMBIGUOUS_LOOKUP_DELAYS = (0.5, 1.5, 3.0)
# The done-order search: one page of the market's newest fill rows, rows matched since the send time minus this slack
# (s; matchTime is in whole seconds), at most this many candidate orders read by id.
FILL_SEARCH_PAGE_SIZE = 50
FILL_SEARCH_SLACK_S = 2
FILL_SEARCH_MAX_CANDIDATES = 5
# After a cancel is accepted, the order's status is read this long (s) later to settle it with its fills.
CANCEL_SETTLE_READ_DELAY = 0.3

# --- Fills ------------------------------------------------------------------------------------------------------
# GET /trade/match/accountMatches?symbol=&ordId=: one row per fill {tradeId, orderId, execQty, execAmt, fee,
# feeCurrencyId, baseCurrencyId, quoteCurrencyId, matchRole 1 TAKER / -1 MAKER, side 1 BUY / -1 SELL, matchTime (s)}.
# The rows come out of the Cloudflare bucket, so an order's rows are read only when the venue has reported more filled
# (cumQty) than the order has booked, and at most once per FILL_ROWS_READ_INTERVAL s per order.
FILLS_PAGE_SIZE = 50
FILLS_MAX_PAGES = 4
FILL_ROWS_READ_INTERVAL = 1.0
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
# A terminal order whose rows stay short of the venue's cumQty this long (s) books the rest from the cumulative totals
# as one fill with an estimated fee ([CS-ALARM] fill-from-cumulative): never forwarded short.
FILL_ROWS_WAIT_SECONDS = 15.0

# A terminal update (FILLED, or CANCELED after fills) waits until the base asset's total balance shows the fills, at most
# this long (s); then balances are read over REST and the update is reported anyway, with an [CS-ALARM] (Hotcoin's rule,
# the default for a venue whose balance-push timing is unknown before its first fill).
FILL_BALANCE_WAIT_SECONDS = 5.0

# No order push this many seconds after the placement answer: the private stream is not delivering. Live 2026-11-08 a
# socket acknowledged LOGIN and both channels, then pushed nothing for 154 orders (its subscribe answers named another
# server node, startEpochMill 1793689551968 vs 1793689552710 on the two that pushed). So REST is polled at once, the
# private stream is reconnected (at most every PRIVATE_STREAM_RECONNECT_MIN_INTERVAL s), and an [CS-ALARM] says so at
# most every ORDER_PUSH_ALARM_INTERVAL s.
ORDER_PUSH_EXPECTED_WITHIN = 3.0
ORDER_PUSH_ALARM_INTERVAL = 600.0
PRIVATE_STREAM_RECONNECT_MIN_INTERVAL = 60.0
# The first this many balance pushes are audit-logged next to the balance held before them (balance-push-vs-rest).
BALANCE_PUSH_AUDITS = 5

# --- Public WebSocket ------------------------------------------------------------------------------------------
# <sym>@depth@20: the top 20 levels a side as a FULL snapshot on every push, push on change, plus a snapshot right
# after each SUB (P1, 2026-10-05; myserver 2026-11-08). The five depth channels (5/10/20/50/100) publish the same
# books at the same moments: one push cycle of ~800 ms on active markets (the server's LIST says interval 500), and a
# re-push about every 60 s on a quiet market even with no change (DMTR: 0 top changes, a push every 60.0 s).
# The phase of that cycle belongs to the CONNECTION: two channels on one socket arrive together (lag p50 0, p90 0),
# the same channel on two sockets ~320 ms apart (p75). Pushes carry no server time and no per-market sequence; every
# frame carries the session's message number `S` (a gap = lost frames).
WS_DEPTH_CHANNEL = "{symbol}@depth@20"
WS_TRADE_CHANNEL = "{symbol}@trade"
EMIT_DEPTH = 20
# SUB takes many channels a message (50 tested by P1); a refused channel answers C 503
# "sub.channel_name.invalid[not found]" with the channel name as `echo`.
WS_SUBSCRIBE_BATCH = 50
WS_SUBSCRIBE_SPACING = 0.05
# The fork's WSConnection pings every WS_PING_INTERVAL s when idle (aiohttp heartbeat) and answers the server's PING
# frames (every 3 min; the server drops a link that hasn't PONGed within 10 min). Coinstore answers a client PING
# frame in 5-10 ms from myserver; JSON pings from a client are refused. The server itself sends {"T":"ping",
# "epochMillis":<ms>} about every 3 s on every socket, the private one included (live 2026-11-08), numbered in `S` like
# any frame; it needs no answer (P1 ignores them). Each link lives at most 24 h.
WS_PING_INTERVAL = 20.0
# A market's subscription is dead once it has pushed nothing for this long: a live one pushes at least every ~60 s.
# Checked every LIVENESS_INTERVAL s.
MARKET_SILENCE_SECONDS = 90.0
LIVENESS_INTERVAL = 5.0
# No frame at all for this long = a dead socket (with several markets the socket carries frames every second).
WS_MESSAGE_TIMEOUT = 75.0

# Freshness by Coinstore's own REST book (no sequence id): every FRESHNESS_INTERVAL s the market silent the longest
# (>= FRESHNESS_QUIET_SECONDS) is compared with REST depth, top FRESHNESS_LEVELS a side (Hotcoin's method). REST is
# not behind the stream: polled at 3/s it showed a new top before every one of 5 sockets in 146 of 322 cases (p50 31
# ms after the earliest), so a book REST disagrees with twice, with no push in between, missed an update.
FRESHNESS_INTERVAL = 10.0
FRESHNESS_QUIET_SECONDS = 15.0
FRESHNESS_LEVELS = 5
FRESHNESS_GRACE = 2.0           # beyond the ~0.8 s push cycle
FRESHNESS_MISSES_TO_RECONNECT = 2

# A book the tracker takes from _order_book_snapshot is emitted again this long after (a runtime add replays parked
# pushes as diffs over the snapshot; the re-emitted SNAPSHOT replaces the merge — Hotcoin's lesson).
SNAPSHOT_REEMIT_DELAY = 0.5

# Coinstore's trading switch: the symbol list's `openTrade` (true on all 376 markets 2026-11-08). While a tracked
# market is not listed or not open, its book is shown EMPTY (the XT rule, 2026-09-26); its symbol and rule stay.
TRADING_SWITCH_INTERVAL = 60

# Reconnect pacing: a connection that failed, or lived less than WS_RETRY_RESET_SEC, waits 1, 2, 4 ... WS_RETRY_CAP s.
WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Private WebSocket ------------------------------------------------------------------------------------------
# LOGIN (coinstore_auth.ws_login_payload) answers {"T":"req","C":200,"M":"login.success"}; then !@order (every order
# of the account) and !@account (every currency) answer C 200 "sub.channel.success" (echo {chName, startEpochMill,
# msgCount}) and LIST shows them as auth PRI. Before a login they answer C 503 "auth.session.needed". No other private
# name exists among match/fill/deal/execution/balance/asset (all C 503 sub.channel_name.invalid).
# The pushes (live 2026-11-08, the placement probes; both can arrive before the REST placement answer):
#   order   {"S","T":"order","channel":"<symbolId>@order","symbol":<symbolId>,"ordId":<int>,"clOrdId","ordState":<int>,
#            "side":1|-1,"ordPrice","ordQty","execQty","execAmt","remainingQty","matchQty","matchAmt","fee",
#            "actualFeeRate","timestamp":<the body's ms>,...}; a fill adds matchId, tradeId (= the REST fill row's),
#            role TAKER|MAKER, feeCurrencyId. matchQty / matchAmt are this fill; remainingQty reads "0" on an open order.
#   account {"S","T":"account","channel":"<currencyId>@account","type":"order.place"|"match.taker"|"match.cancel",
#            "currencyId","currency","available","frozen"}: absolute amounts, the hold included, with the fill.
# `S` numbers every frame of the connection, answers included: a gap = lost frames.
WS_LOGIN_TIMEOUT = 10.0
WS_ORDER_CHANNEL = "!@order"
WS_ACCOUNT_CHANNEL = "!@account"

# --- First-live-run audit logging ---------------------------------------------------------------------------------
# Every line tagged [CS-AUDIT], scoped to what the docs and probes could not settle (runbook §1.6). Money-guard firings
# are not audit lines: they always log as [CS-ALARM] WARNINGs.
LIVE_AUDIT_LOGGING = True
