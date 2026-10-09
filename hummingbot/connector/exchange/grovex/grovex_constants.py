from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-10-09 from myserver through the brr_ws relay and
# from brr_ws: asset_manager/test_and_reference/grovex/probe_grovex_phase0.py; the order/fill shapes from
# hmb_local_tools/grovex/grovex_private_probe.py), then the docs (offline mirror VS_code_projects/MDs/grovex-api/: the
# official GroveXchange/grovexfile repo — api_doc_en.md, demo.txt — and the website's /api page). P1's tracker adapter
# (Tracker_cex_cex/ws_book_checker/exchanges/grovex.py) settled the socket at production scale. Wiki:
# trading/exchanges/grovex-api ("P2 / P3 onboarding").
#
# GroveX runs on the ChainUp platform: the "open/api" v1 REST (md5 signing, polling only: there is NO private push)
# and ChainUp's kline-api socket (gzip frames, a full book per push).

EXCHANGE_NAME = "grovex"
DEFAULT_DOMAIN = "io"

REST_URL = "https://openapi.grovex.io"
WSS_URL = "wss://ws.grovex.io/kline-api/ws"

# --- The relay -----------------------------------------------------------------------------------------------
# GroveX's Cloudflare edge answers every private route AND the book socket with a managed challenge (HTTP 403,
# Cf-Mitigated: challenge) for myserver's AWS address, on every published host and path (2026-10-09, wiki §Reachability).
# brr_ws passes, and GroveX's origin sits near Frankfurt, so every GroveX request and the socket go through
# grovex-relay on brr_ws (an HTTP CONNECT relay: myserver's address only, these two hosts only; TLS stays end to end).
# Measured from myserver: a warm request p50 288 ms (direct, if it were allowed: ~261 ms). Pavel's call, 2026-10-09.
# The relay is stopped and disabled since GroveX was scrapped (2026-10-09, see __init__.py). None = direct.
PROXY_URL = "http://5.83.147.196:18443"
# Cloudflare also refuses aiohttp's default User-Agent on the socket and some routes (P1); a browser UA passes.
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
# A connection through the relay costs ~1 s to open (TCP to brr_ws, CONNECT, TLS: 4 round trips of ~245 ms) and a
# request on an open one ~0.27 s, so connections are kept open: idle up to KEEPALIVE_SECONDS, and WARM_CONNECTIONS
# light public reads every WARM_INTERVAL keep that many ready. Placements and cancels have their own pool, never held
# by a slow read (user/account takes ~22 s, get_allticker 6-9 s).
KEEPALIVE_SECONDS = 120.0
WARM_INTERVAL = 20.0
WARM_TRADE_CONNECTIONS = 2
WARM_READ_CONNECTIONS = 2

# Client order ids: create_order documents none, so the connector places orders without one; a placement that got no
# answer is found by side, price, quantity and creation time (GrovexExchange._find_placed_order). The §0.1 probe tests
# two candidate client-id parameters.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths --------------------------------------------------------------------------------------------------
SYMBOLS_PATH = "/open/api/common/symbols"       # every market: price/amount precision (decimals), limit_volume_min
ALL_TICKER_PATH = "/open/api/get_allticker"     # every market's top, last, 24 h, isShow — 6-9 s server-side
TICKER_PATH = "/open/api/get_ticker"            # one market: buy/sell = the LIVE top, last
DEPTH_PATH = "/open/api/market_dept"            # NOT a book source (below); its `time` is the server's clock in ms
ACCOUNT_PATH = "/open/api/user/account"         # signed GET: coin_list[{coin, normal, locked}] — ~22 s per call
CREATE_ORDER_PATH = "/open/api/create_order"    # signed POST: side, type, volume, price, symbol -> {order_id}
CANCEL_ORDER_PATH = "/open/api/cancel_order"    # signed POST: order_id, symbol
ORDER_INFO_PATH = "/open/api/order_info"        # signed GET: order_id, symbol -> {order_info, trade_list}
OPEN_ORDERS_PATH = "/open/api/v2/new_order"     # signed GET: symbol (required) -> {count, resultList}
ALL_ORDERS_PATH = "/open/api/v2/all_order"      # signed GET: symbol (required), startDate/endDate <= 10 min apart
MY_TRADES_PATH = "/open/api/all_trade"          # signed GET: symbol (required), page, pageSize, sort=1 newest first
# There is no wallet route (21 signed names: Spring 404), no fee route and no time route (2026-10-09).
SERVER_TIME_PATH = DEPTH_PATH
SERVER_TIME_PARAMS = {"symbol": "btcusdt", "type": "step0"}
NETWORK_CHECK_PARAMS = {"symbol": "btcusdt"}

# ⚠️ market_dept is not GroveX's book for coins Binance lists: it serves Binance's book frozen 3-4 min back (P1,
# 2026-10-08: Binance's 1-s candle at lastUpdateId = the REST top on 6 of 6 coins; a REST bid above the live WS ask).
# get_ticker's buy/sell are the live top. So books come only from the socket, and REST never seeds or checks one.

# --- Responses -----------------------------------------------------------------------------------------------
# Every answer is HTTP 200 {"code": "<string>", "msg", "data", "message", "success"}; `msg` is null on errors, only the
# code says what happened. Codes seen live (2026-10-09):
CODE_OK = "0"
CODE_UNKNOWN_SYMBOL = "1"           # an unknown symbol (order_info, get_ticker)
CODE_OUT_OF_RANGE = "2"             # a v2/all_order window over 10 min
CODE_CANCEL_FAILED = "8"            # a cancel on an unknown symbol (docs: "Order cancellation failed")
CODE_CANCEL_UNKNOWN_ORDER = "22"    # a cancel of an unknown order id
CODE_PARAM_ERROR = "100004"         # a missing or illegal parameter
CODE_SIGN_ERROR = "100005"
CODE_ILLEGAL_IP = "100007"          # docs
CODE_REQUEST_EXPIRED = "100008"     # `time` more than ~60 s old: refused before execution
# Not found is SILENT: order_info of an unknown id answers code "0" with order_info and trade_list null. "Not found" is
# decided by the connector (_resolve_missing_order), never by a code. Our own codes for it:
CODE_ORDER_NOT_FOUND = "-404"
CODE_EMPTY_ANSWER = "-1"

# Signing (docs "Signature" + demo.txt, live 2026-10-09): sign = md5(the non-empty parameters sorted by name, each
# written key + value with no separator, + secret), LOWER-case hex (upper-case: 100005). api_key, time (ms) and sign
# are parameters: the query string on GETs, the form body on POSTs. Reads are GET only and writes POST only (405
# otherwise). `time` older than ~60 s is refused (100008: -61 s refused, -45 s accepted); no upper bound (+3,600 s
# accepted).

# --- Orders --------------------------------------------------------------------------------------------------
SIDE = {TradeType.BUY: "BUY", TradeType.SELL: "SELL"}
ORDER_TYPE_LIMIT = 1     # 2 = market (volume is a quote amount on a market BUY): never sent
# status (docs, v2/new_order): 0 INIT (not in the book yet), 1 NEW, 2 FILLED, 3 PART_FILLED, 4 CANCELED,
# 5 PENDING_CANCEL, 6 EXPIRED ("abnormal order"). An expired order is terminal: CANCELED with whatever filled. A value
# outside the map is derived from the amounts, never guessed (GrovexExchange._order_state).
ORDER_STATE = {
    0: OrderState.OPEN,
    1: OrderState.OPEN,
    2: OrderState.FILLED,
    3: OrderState.PARTIALLY_FILLED,
    4: OrderState.CANCELED,
    5: OrderState.PENDING_CANCEL,
    6: OrderState.CANCELED,
}
DONE_STATUSES = ("2", "4", "6")
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)
# A quantity mismatch beyond this fraction between GroveX's own order quantity and ours is an alarm (rounding is far
# smaller); also the tolerance of the placement lookup's size match.
VOLUME_MISMATCH_TOLERANCE = Decimal("0.01")

# A placement GroveX did not confirm (no answer, or "0" without an order id) is looked up by side, price, quantity and
# creation time in the open orders and the recent orders, after these delays (s). Not found, it is NOT failed: it stays
# pending and every status poll looks once more, until the not-found verdict below fails it. The poll never looks while
# a placement is awaited.
PLACEMENT_LOOKUP_DELAYS = (0.7, 2.0)
PLACE_ORDER_TIMEOUT = 10.0
# Every other request gets this timeout (s): a relay connection that hangs must not stall the sequential order poll for
# aiohttp's default 300 s (review 2026-10-09). user/account and get_allticker carry their own, longer ones.
DEFAULT_REQUEST_TIMEOUT = 10.0
# v2/all_order windows may span at most 10 min (code 2 beyond): the lookup reads the window from 5 s before the
# placement to 4 min after, never past the server's now. Its startDate/endDate are wall-clock strings in a zone the docs
# don't name: both UTC and UTC+8 (ChainUp's home zone) are read until the first live order says which
# (ALL_ORDER_ZONES_H); a zone GroveX refuses is skipped while another answers.
ALL_ORDER_ZONES_H = (0, 8)
ALL_ORDER_WINDOW_BEFORE_S = 5.0
ALL_ORDER_WINDOW_AFTER_S = 240.0
LIST_PAGE_SIZE = 100

# A null order_info is "not found" only for an order older than this (s) — a just-placed order may not read yet — and
# only once it is also absent from the open orders and the recent orders.
NOT_FOUND_MIN_AGE = 10.0

# No private push: while any order is open the status poll runs every ORDER_POLL_INTERVAL (s), one order_info read per
# open order (it carries the order's fills as well); otherwise the base's 10 s poll.
ORDER_POLL_INTERVAL = 1.0
# The server time comes from market_dept's `time` (ms): re-read this often, and at once on a 100008.
TIME_SYNC_INTERVAL = 300.0

# all_trade (the account's trades on a market, newest first) is read page by page back to the order's creation, at
# most this many pages of LIST_PAGE_SIZE.
MAX_TRADE_PAGES = 5

# GroveX says an order is done but its trades haven't all shown (or a guard refused one): the order stays open in
# Hummingbot until they have (a done order with fills missing gets hedged at the wrong size). Past this long (s), one
# [GX-ALARM] fills-missing per order.
FILLS_MISSING_ALARM_SECONDS = 30.0

# --- Balances: user/account takes ~22 s ------------------------------------------------------------------------
# (3 of 3 reads 22.0-23.8 s, 811 coins listed, zeros included.) So balances are read in their own background task,
# never inside the status poll (which would then run every ~23 s), one read at a time with BALANCE_READ_GAP s between
# reads. A read can't say when, inside its ~22 s, GroveX took the balances. Every fill booked goes into a journal with
# GroveX's own trade time (all_trade `ctime`) and moves the totals at once (base, quote, fee coin), so the hold-band sees
# a fill when it is booked. A read is applied to an asset (GrovexExchange._read_and_apply_balances) when:
#   - no fill on the asset has a trade time inside [sent - FILL_TIME_SLACK_MS, received + FILL_TIME_SLACK_MS] (the
#     read can't say whether it holds such a fill; fills after the window are added back on top of the read);
#   - no order of ours on the asset was open at the read or is open now — waived once the asset has gone
#     STALE_READ_SECONDS without a read, so a busy asset (USDT) still refreshes;
#   - and, for `available`, no placement or cancel on the asset in [sent - BALANCE_PRE_SLACK, received + POST_SLACK].
# The FIRST read is the baseline: applied to every asset (a restored order's fills from the downtime are in it), and the
# connector is not ready before it.
BALANCE_READ_GAP = 5.0
BALANCE_READ_TIMEOUT = 60.0
BALANCE_PRE_SLACK = 2.0
BALANCE_POST_SLACK = 3.0
# ... and while an order is open (it could fill unseen inside the window): its fill is booked within ~1-2 s.
BALANCE_POST_SLACK_BUSY = 6.0
FILL_TIME_SLACK_MS = 5000
STALE_READ_SECONDS = 180.0
# The journal keeps fills this long (s); older ones are below every asset's read cutoff.
FILL_JOURNAL_KEEP_SECONDS = 6 * 3600
# A busy asset whose running total and a read differ by more than this fraction is reported once per episode
# ([GX-ALARM] balance-drift): the read is not applied to it, the line only says the two disagree.
BALANCE_DRIFT_ALARM = Decimal("0.02")

# --- Public WebSocket ----------------------------------------------------------------------------------------
# market_<symbol>_depth_step0: the FULL book in every push (asks ascending, `buys` descending, 1 to ~60 levels a side),
# gzip binary frames. Pushed on change on a ~1 s server tick AND as a full push every ~11.1 s, changed or not (P1,
# 385 markets). `ts` = the push's send time floored to the second. step1/step2 merge levels: only step0 is used.
WS_DEPTH_CHANNEL = "market_{symbol}_depth_step0"
EMIT_DEPTH = 50
# The first push after a subscribe is the server's CACHED snapshot with its old ts (1.8-6.8 s from myserver through the
# relay, up to 16.6 s in P1); later pushes arrive p50 0.6 s, max 1.1 s behind their floored ts (myserver, 2026-10-09).
# A push more than STALE_PUSH_SECONDS behind is not shown, and the market's book is shown empty until a fresh one
# (the P1 rule): the market's first book is its next push, at most one ~11 s period later.
STALE_PUSH_SECONDS = 3.0
# Every subscribed market pushes at least every ~11.1 s, so silence is a dead stream, not a quiet book: a market silent
# for MARKET_SILENCE_SECONDS is shown empty and re-subscribed (at once x3, then at most every 60 s); a socket with
# markets subscribed and no frame for SOCKET_SILENCE_SECONDS is reconnected.
MARKET_SILENCE_SECONDS = 30.0
SOCKET_SILENCE_SECONDS = 35.0
STALE_CHECK_INTERVAL = 2.0
# Heartbeat: the server never pings, and a client JSON {"ping": ...} makes it CLOSE the socket (P1). It answers RFC 6455
# PING frames: aiohttp's heartbeat is the keepalive, and a missing PONG closes the socket.
WS_HEARTBEAT = 15.0
WS_MAX_MSG_SIZE = 4 * 1024 * 1024
# One channel per {"event": "sub"} message, no ACK; an unknown market is silently ignored (only silence shows it).
WS_SUB_DELAY = 0.02

# GroveX's trading switch: get_allticker `isShow` (the website's visibility; 5 USDT markets hidden on 2026-10-09). While
# a tracked market is hidden or no longer listed, its book is shown EMPTY (the XT rule, 2026-09-26); its symbol and
# trading rule stay. get_allticker also gives the last prices (`last`).
TRADING_SWITCH_INTERVAL = 120.0
ALL_TICKER_SHARE_SECONDS = 30.0

WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Balance source --------------------------------------------------------------------------------------------
REAL_TIME_BALANCE_UPDATE = False

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [GX-AUDIT], scoped to what the docs could not settle. Switch off once phase 6 has answered them.
# Money-guard firings are not audit lines: they always log as [GX-ALARM] WARNINGs.
LIVE_AUDIT_LOGGING = True

# --- Rate limits -------------------------------------------------------------------------------------------------
# Docs: 6 requests per 2 s per IP (public) and per user (private). Live through the relay (2026-10-09): 20 calls back to
# back at ~9/s, 200 signed calls at 4/s and sequential signed calls at ~3.7/s, all answered, no 429. Three pools, so a
# placement or a cancel never queues behind the 1 s order poll: reads 5/s, trades 5/s, public 5/s.
READS = "grovex_private_reads"
TRADES = "grovex_trades"
PUBLIC = "grovex_public"
RATE_LIMITS = [
    RateLimit(limit_id=READS, limit=5, time_interval=1),
    RateLimit(limit_id=TRADES, limit=5, time_interval=1),
    RateLimit(limit_id=PUBLIC, limit=5, time_interval=1),
] + [
    RateLimit(limit_id=path, limit=5, time_interval=1, linked_limits=[LinkedLimitWeightPair(READS)])
    for path in (ACCOUNT_PATH, ORDER_INFO_PATH, OPEN_ORDERS_PATH, ALL_ORDERS_PATH, MY_TRADES_PATH)
] + [
    RateLimit(limit_id=path, limit=5, time_interval=1, linked_limits=[LinkedLimitWeightPair(TRADES)])
    for path in (CREATE_ORDER_PATH, CANCEL_ORDER_PATH)
] + [
    RateLimit(limit_id=path, limit=5, time_interval=1, linked_limits=[LinkedLimitWeightPair(PUBLIC)])
    for path in (SYMBOLS_PATH, ALL_TICKER_PATH, TICKER_PATH, DEPTH_PATH)
]
# Paths whose requests use the trade pool's connections (GrovexConnectionsFactory).
TRADE_PATHS = (CREATE_ORDER_PATH, CANCEL_ORDER_PATH)
