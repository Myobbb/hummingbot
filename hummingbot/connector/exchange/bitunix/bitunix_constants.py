from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-10-07 from myserver with the asset_manager's key,
# asset_manager/test_and_reference/bitunix/probe_bitunix_phase0.py, and the raw book probe
# hmb_local_tools/bitunix/debug_bitunix_ws_cadence.py), then the docs (offline mirror VS_code_projects/MDs/bitunix-api-spot/,
# re-checked live in English and Chinese on 2026-10-07: unchanged but for error 10058). There is no official spot SDK
# (the GitHub one is futures-only) and CCXT has no Bitunix. Wiki: trading/exchanges/bitunix-api ("P2 + P3 onboarding").
#
# The one structural fact: Bitunix's spot API has NO push stream, public or private (its WS is signed
# request/response). Books come from the website's market socket (WEB tier, as P1 streams them); orders, fills and
# balances are polled over REST.

EXCHANGE_NAME = "bitunix"
DEFAULT_DOMAIN = "com"

REST_URL = "https://openapi.bitunix.com"
# The website's market socket (P1's book source since 2026-09-26). Plain JSON once the site's `transfer=pb` is left out.
WSS_URL = "wss://api.bitunix.com/ws-tide-batch/?from=trad"

# Client ids: undocumented, but place_order keeps a `clientId` and every order read (detail, pending, history) returns
# it (live 2026-10-07: a 32-char HBOT id kept). It is no key — no read or cancel by it; a cancel by clientId stalls
# the order ~60 s — so the connector sends it and looks orders up by it in the lists, and cancels by orderId only.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths --------------------------------------------------------------------------------------------------
PAIRS_PATH = "/api/spot/v1/common/coin_pair/list"     # markets + rules (~577 KB, every market in one call)
LAST_PRICE_PATH = "/api/spot/v1/market/last_price"    # ONE symbol per call: there is no bulk ticker (probed)
DEPTH_PATH = "/api/spot/v1/market/depth"              # 50 levels {price, volume}; any symbol case
ACCOUNT_PATH = "/api/spot/v1/user/account"            # signed GET: [{coin, balance, balanceLocked}]
PLACE_ORDER_PATH = "/api/spot/v1/order/place_order"   # signed POST {side, type, volume, price, symbol}
CANCEL_ORDER_PATH = "/api/spot/v1/order/cancel"       # signed POST {orderIdList: [{orderId, symbol}]}
ORDER_DETAIL_PATH = "/api/spot/v1/order/detail"       # signed GET ?orderId=
ORDER_DEALS_PATH = "/api/spot/v1/order/deal/list"     # signed POST {orderId, symbol}: the order's fills, each with an id
ORDER_PENDING_PATH = "/api/spot/v1/order/pending/list"   # signed POST {symbol} (required)
ORDER_HISTORY_PATH = "/api/spot/v1/order/history/page"   # signed POST {symbol (required), page, pageSize, startTime}
# Undocumented routes: none exist. 26 guessed public paths (bulk ticker, server time, recent trades, bbo …) and 6
# signed fee paths all answer the made-up path's 404 (2026-10-07). So: no time endpoint (the HTTP Date header is the
# clock), no fee endpoint (the fill's own fee is booked), no trade feed.
SERVER_TIME_PATH = LAST_PRICE_PATH
SERVER_TIME_PARAMS = {"symbol": "BTCUSDT"}

# --- Responses -----------------------------------------------------------------------------------------------
# Every answer we get is HTTP 200: {"code": "<string>", "msg", "data", "success"}; "0" is success. Codes seen live:
CODE_OK = "0"
CODE_PARAM_ERROR = "2"            # parameter error: an unknown symbol, a missing `symbol`, volume 0 …
CODE_VOLUME_PRECISION = "10058"   # docs (2026-10-07): "volume precision error"
CODE_KEY_NOT_FOUND = "100004"
CODE_SIGN_ERROR = "100005"        # "result.api.parameter.sign.illegal"
CODE_REQUEST_EXPIRED = "100008"   # "result.request.time.expired": refused before execution (live at -61 s)
CODE_NO_AUTHORITY = "110033"      # "OPEN_API_KEY_NO_AUTHORITY": the key lacks that permission
CODE_TOO_FAST = "110041"
# A refused placement carries no error code (live 2026-10-09): code "0" ("result.success"), and in data orderId null,
# placeStatus 0 and the reason in placeCode + placeMsg (Chinese). The placeCodes seen live:
PLACE_STATUS_REFUSED = "0"
PLACE_CODES = {"10034": "insufficient balance"}   # placeMsg "余额不足"
# Not found is SILENT on Bitunix (live): detail of an unknown order id answers code "0" with data null, deal/list [],
# and a cancel of an unknown id answers success. So a cancel's answer proves nothing, and "not found" is decided by the
# connector (BitunixExchange._resolve_missing_order), not by a code. These are our own codes for it.
CODE_ORDER_NOT_FOUND = "-404"     # null detail AND absent from the open orders AND from the recent history
CODE_EMPTY_ANSWER = "-1"          # code "0" with data that should have been there

# The timestamp window is asymmetric (live): -61 s is refused, +61 s still accepted. Signing (docs sign.md, live):
# sign = sha256hex(sha256hex(nonce + timestamp + apiKey + query + body) + secret); query = the GET parameters sorted by
# name, concatenated key+value with NO '=' or '&' (the REST page's "id=1uid=200" form is refused, 100005); body = the
# compact JSON sent. Headers: api-key, nonce (32 chars), timestamp (ms), sign.

# --- Orders --------------------------------------------------------------------------------------------------
SIDE = {TradeType.SELL: 1, TradeType.BUY: 2}
ORDER_TYPE_LIMIT = 1
# `volume` on a LIMIT order is the BASE quantity — settled live 2026-10-07 (P2 §0.1, bitunix_private_probe.py --trade
# on myserver): volume 271 DOGEUSDT @ 0.0444 read back volume = leftVolume = 271, amount = 12.0324 (the quote notional).
# The REST place_order page's "quote amount" is wrong; the batch and WS pages were right. The guard stays as a safety
# net: every order's first status read compares Bitunix's own quantity with ours, and a mismatch cancels the order at
# once ([BU-ALARM] volume-semantics).
LIMIT_VOLUME_IS_BASE = True
# A quantity mismatch beyond this fraction trips the guard above (precision rounding is far smaller).
VOLUME_MISMATCH_TOLERANCE = Decimal("0.01")

# status (every order page): 1 unfilled, 2 filled, 3 partially filled, 4 cancelled, 7 partially filled then
# cancelled. A value outside the map is derived from the amounts, never guessed (BitunixExchange._order_state).
ORDER_STATE = {
    1: OrderState.OPEN,
    2: OrderState.FILLED,
    3: OrderState.PARTIALLY_FILLED,
    4: OrderState.CANCELED,
    7: OrderState.CANCELED,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)

# A placement Bitunix did not confirm (no answer, or "0" without an order id) is looked up by its client id in the
# open orders and the recent history, after these delays (s). Not found, it is NOT failed: it stays pending and every
# status poll looks once more, until the not-found verdict below (and the tracker's 3 strikes) fails it. The poll never
# looks while a placement is awaited.
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
PLACE_ORDER_TIMEOUT = 10.0
# The look-back for that lookup and for a not-found check: history/page filters on creation time.
LOOKUP_WINDOW_MS = 5 * 60 * 1000

# A null detail is "not found" only for an order older than this (s) — a just-placed order may not read yet — and only
# once it is also absent from the open orders and the recent history.
NOT_FOUND_MIN_AGE = 10.0

# No private push: while any order is open the status poll runs every ORDER_POLL_INTERVAL (s), so a fill or a cancel
# is seen within about a second; otherwise the base's 10 s poll. One detail read per open order per poll (shared by the
# poll's two passes, fills then status; done orders are not read again), one deal/list read when the filled quantity
# grew, one balance read.
ORDER_POLL_INTERVAL = 1.0
# Server time comes from an HTTP Date header (1 s resolution, plenty for a 60 s window): re-read this often, and at
# once on a 100008.
TIME_SYNC_INTERVAL = 300.0

# Bitunix's fills arrive by polling, the balance by another read: a terminal update (FILLED, or CANCELED after fills)
# waits until the base asset's total balance shows the fills, at most this long (s); then balances are read over REST
# and the update goes anyway, with a [BU-ALARM] (Hotcoin's double-buy lesson, 2026-10-06).
FILL_BALANCE_WAIT_SECONDS = 5.0
# Bitunix says an order is done but its deal list hasn't shown every fill (or a guard refused a fill row): the order
# stays open in Hummingbot until it has (strict: a done order with fills missing gets hedged at the wrong size). Past
# this long (s), one [BU-ALARM] fills-missing per order.
FILLS_MISSING_ALARM_SECONDS = 30.0

# An empty account answers data null (live). While balances are held, null is believed only once it has held this long
# (s) over at least this many reads, with no order open; until then the last balances stay.
EMPTY_BALANCE_CONFIRM_SECONDS = 30.0
EMPTY_BALANCE_CONFIRM_READS = 3

# coin_pair/list (~577 KB) is read by the symbol map, the trading rules, the trading switch and runtime adds: a read
# younger than this (s) is shared (startup: one read); a runtime add accepts one younger than MARKET_LIST_FRESH_SECONDS
# (a burst of adds shares a read; a new listing is still seen).
MARKET_LIST_SHARE_SECONDS = 60.0
MARKET_LIST_FRESH_SECONDS = 5.0

# --- Public WebSocket (WEB tier) -------------------------------------------------------------------------------
# spot_<symbol lowercase>_depth_<precisions[0]>: the FULL 50-level book of a market at a fixed ~510 ms (1.83-1.98
# pushes/s), changed or not, arriving 6-7 ms after the frame's server ts (myserver, 2026-10-07). The sub reply carries
# every channel's current book at once. The stream LEADS REST (REST older by p50 832 ms), so REST is never a freshness
# reference. A coarser step merges levels, so only the finest is used; an unknown symbol or a step finer than the
# market's gets no data and no error.
WS_DEPTH_CHANNEL = "spot_{symbol}_depth_{step}"
# spot_simple_market_<symbol>: close + 24h amount ~1/s. Used for the last traded price (there is no trade channel).
WS_TICKER_CHANNEL = "spot_simple_market_{symbol}"
EMIT_DEPTH = 50
# The client pings; the server closes a socket that sends none for ~60 s (P1). The pong is a message too: a socket with
# no channel that streams (no pairs yet, closed markets) must still hear something within WS_MESSAGE_TIMEOUT, so the
# ping runs at under half of it (review 2026-10-07: at 15 s such a socket died every 10 s).
WS_PING_INTERVAL = 4.0
# Every subscribed market pushes about twice a second, so silence is death: a socket with no message for this long
# reconnects, and a tracked market with no push for this long is re-subscribed (STALE_MARKET_SECONDS).
WS_MESSAGE_TIMEOUT = 10.0
STALE_MARKET_SECONDS = 5.0
STALE_CHECK_INTERVAL = 2.0
# Channels per sub message (one comma-joined string; P1 streamed 549 on one socket).
WS_CHANNELS_PER_SUB = 100
WS_MAX_MSG_SIZE = 16 * 1024 * 1024   # a sub reply carries every channel's book at once (~4.5 KB each)

# Bitunix's trading switch: coin_pair/list `isOpen` (P1: 40/40 open books two-sided, 37/40 closed books empty — 3
# closed markets kept quoting). While a tracked market is not open, its book is shown EMPTY (the XT rule,
# 2026-09-26); its symbol and trading rule stay.
TRADING_SWITCH_INTERVAL = 120

WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Balance source --------------------------------------------------------------------------------------------
# No balance push exists: the REST poll is authoritative, and Hummingbot's in-flight snapshot keeps `available`
# right between polls (real_time_balance_update = False, as for BingX and BitMart).
REAL_TIME_BALANCE_UPDATE = False

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [BU-AUDIT], scoped to what the docs could not settle. Switch off once phase 6 has answered them.
# Money-guard firings are not audit lines: they always log as [BU-ALARM] WARNINGs.
LIVE_AUDIT_LOGGING = True

# --- Rate limits -------------------------------------------------------------------------------------------------
# Undocumented for trading (110041 "too fast" exists); wallet routes are 10/s per IP (docs). 40 sequential last_price
# calls took 2.5 s, all 200 (live). Conservative per-route limits under two pools: reads (the 1 s order poll, prices,
# the market list) never queue a placement or a cancel behind them (review 2026-10-07).
READS = "bitunix_reads"
TRADES = "bitunix_trades"
RATE_LIMITS = [
    RateLimit(limit_id=READS, limit=20, time_interval=1),
    RateLimit(limit_id=TRADES, limit=10, time_interval=1),
] + [
    RateLimit(limit_id=path, limit=10, time_interval=1, linked_limits=[LinkedLimitWeightPair(READS)])
    for path in (PAIRS_PATH, LAST_PRICE_PATH, DEPTH_PATH, ACCOUNT_PATH, ORDER_DETAIL_PATH, ORDER_DEALS_PATH,
                 ORDER_PENDING_PATH, ORDER_HISTORY_PATH)
] + [
    RateLimit(limit_id=path, limit=10, time_interval=1, linked_limits=[LinkedLimitWeightPair(TRADES)])
    for path in (PLACE_ORDER_PATH, CANCEL_ORDER_PATH)
]
