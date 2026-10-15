from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth: the spot docs (https://www.coinw.com/api-doc/en/spot-trading, offline mirror at
# VS_code_projects/MDs/coinw-api/, identical to the live site on 2026-10-15), JKorf's CoinW.Net (the maintained SDK;
# CCXT has no CoinW), and the phase-0 probes on myserver (2026-10-15; wiki trading/exchanges/coinw-api,
# "P2 + P3 onboarding"). Where they disagree, the choice and its evidence are noted here.

EXCHANGE_NAME = "coinw"
DEFAULT_DOMAIN = "com"

REST_URL = "https://api.coinw.com"
WSS_URL = "wss://ws.futurescw.com"                 # the docs' "Method 2" socket: public AND private
# The handshake needs this Origin: without it the CDN answers 403 (live 2026-10-15, myserver; P1 2026-09-30).
WS_HEADERS = {"Origin": "https://www.coinw.com"}

# Hummingbot's client ids (HBOT + side + pair letters + a hex nonce) go out as `out_trade_no`: "Maximum length 50
# characters; only latin characters, numbers, hyphens and underscores" (Place an Order).
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST ----------------------------------------------------------------------------------------------------
PUBLIC_PATH = "/api/v1/public"                     # ?command=<public command>
PRIVATE_PATH = "/api/v1/private"                   # ?command=<private command>, POST, empty body, signed query

# public commands
CMD_SYMBOLS = "returnSymbol"                       # every market + its rules and `state` (1 normal / 2 disabled)
CMD_TICKER = "returnTicker"                        # every market's 24h row: `id` = the socket's pairCode, `last`,
#                                                    `isFrozen` (0 / 1); one call (~80 KB, ~150 ms from myserver)
CMD_ORDER_BOOK = "returnOrderBook"                 # size 5 / 20, no timestamp, no sequence: never a book source
# private commands (docs, per-command limits per user ID; a global 300/s per user and 100/s per IP on top)
CMD_PLACE = "doTrade"                              # 30 / 2 s
CMD_CANCEL = "cancelOrder"                         # 30 / 2 s, by orderNumber only
CMD_ORDER_STATUS = "returnOrderStatus"             # 30 / 2 s, orders <= 7 days old
CMD_ORDER_DETAIL = "returnOrderTrades"             # 30 / 2 s: the order's CUMULATIVE fill (success_total ...)
CMD_OPEN_ORDERS = "returnOpenOrders"               # 30 / 2 s, one market (currencyPair required); no client id
CMD_USER_TRADES = "getUserTrades"                  # 10 / 2 s: the account's fills, one row per fill, exact
#                                                    (tradeId, orderId, price, size, side, fee, time), <= 100 a page
CMD_ORDER_HISTORY = "returnUTradeHistory"          # 5 / 2 s: executed orders with `out_trade_no`, <= 30 days
CMD_BALANCES = "returnCompleteBalances"            # 5 / 2 s: {asset: {available, onOrders}}; held assets only
CMD_BATCH_HISTORY = "getBatchHistoryOrders"        # 1 / 2 s: CLOSED orders by orderNumber (<= 50, <= 3 months):
#                                                    dealSize, dealFunds, fee, status; -110 for an id it has no row
#                                                    for YET (live 2026-10-16: an open order, and one cancelled 3 s
#                                                    earlier; listed 3 min later). Its `status` is not returnOrderTrades'
#                                                    enum: that cancelled order with no fill read 2 here, 4 there.

# --- Responses (live 2026-10-15 from myserver; every answer HTTP 200) ---------------------------------------
# Success: {"code":"200","data":…,"msg":"SUCCESS","success":true,"failed":false} — `code` is the STRING "200".
CODE_OK = "200"
CODE_WRONG_SIGNATURE = "-105"                      # a wrong secret, or a parameter signed but not sent
CODE_PARAM_TYPE = "-104"                           # "Parameter type error,uid:…"
CODE_BUSINESS = "-200"                             # "业务异常": what returnOrderStatus / returnOrderTrades answer for
#                                                    an order id that isn't the account's (a made-up id), AND the
#                                                    docs' "system under update". Never a not-found on its own.
CODE_CANCEL_NOT_FOUND = "200012"                   # cancelOrder: "The order does not exist,uid:…"
CODE_BATCH_NOT_FOUND = "-110"                      # getBatchHistoryOrders: "订单不存在" (the order does not exist)
CODE_RATE_LIMIT = "29001"                          # "API access frequently"
CODE_METHOD = "405"                                # a GET to a POST command
# An unauthenticated private request (no api_key) answers HTTP 200 with an EMPTY body (live): no envelope at all.
CODE_EMPTY_ANSWER = "no-envelope"
# A placement answered -200 (also CoinW's maintenance answer) or with an empty body (proven only for an unauthenticated
# call) has NOT been refused: CoinW may have taken the order. It is looked up like an unanswered placement.
PLACE_UNKNOWN_CODES = (CODE_BUSINESS, CODE_EMPTY_ANSWER)
# Refusal codes worth naming in a WARNING (Error Code Guide; the order-specific ones are audit-logged as seen).
REFUSAL_CODES = {
    "-105": "wrong signature", "-104": "parameter type error", "-103": "user API error (parameters or signature)",
    "-200": "business exception (or maintenance)", "-3": "API access error", "29001": "rate limited",
    "6000": "API key error", "429": "gateway rate limit", "200012": "order does not exist",
}

# --- Orders --------------------------------------------------------------------------------------------------
TRADE_TYPES = {TradeType.BUY: "0", TradeType.SELL: "1"}
# returnOrderStatus / returnOrderTrades `status`: 1 uncompleted, 2 partially completed, 3 fully completed, 4 user
# canceled (docs; CoinW.Net OrderStatus). 4 with a fill is a partial-then-cancel.
ORDER_STATE = {
    1: OrderState.OPEN,
    2: OrderState.PARTIALLY_FILLED,
    3: OrderState.FILLED,
    4: OrderState.CANCELED,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)
# The order push (`order` channel): `type` RECEIVED (accepted) or DONE (finished) — CoinW.Net OrderEventType has
# only these two — and on DONE a `reason`: FILLED, CANCELLED or REJECTED (CoinW.Net OrderEventReason). A resting
# order's partial fills get NO push: the asset push, the status poll and DONE trigger the fills read (getUserTrades).
PUSH_RECEIVED = "RECEIVED"
PUSH_DONE = "DONE"
PUSH_REASON_STATE = {"FILLED": OrderState.FILLED, "CANCELLED": OrderState.CANCELED, "CANCELED": OrderState.CANCELED,
                     "REJECTED": OrderState.FAILED}

# --- Public WebSocket: the local book ------------------------------------------------------------------------
# One book sequence on three channels, each conflated server-side (myserver 2026-10-15, 11 markets x 300 s per run;
# hmb_local_tools/coinw/debug_coinw_book_freshness.py, debug_coinw_merged_book.py, debug_coinw_channel_content.py):
#   depth_sync      undocumented (the website's own feed): action "snapshot" (<= 100 levels, the full 64-bit seq), then
#                   "increment" frames {startSeq, endSeq, asks/bids [[px, qty, seq]]} carrying every level update; no
#                   gaps; its book equalled CoinW's own snapshot at the same seq on every comparison;
#   depth_snapshot  documented; every ~300 ms, changed or not, a 20-level frame then its <= 100-level twin with the
#                   same `seq` = the low 32 bits of the same sequence;
#   depth           documented deltas: NOT a book source — its batches carry the net change of their range and leave
#                   out deep levels' changes (stale and phantom levels in a book that took them).
# Behind the earliest of all three channels per change (p50 / p90 / p99 ms, 22,513 changes): depth_sync alone 10 / 200 /
# 355, depth_snapshot alone 141 / 275 / 346, the book from both (CoinwAPIOrderBookDataSource) 0 / 134 / 235, with 0 of
# 5,683 same-seq checks against CoinW's snapshot off (10 more on a thin book known only 20 deep: a shallow match).
# (`depth` alone 4 / 109 / 179: the ~25 ms it would add at p90 is not worth its stale and phantom levels.)
WS_SYNC_CHANNEL = "depth_sync"
WS_SNAPSHOT_CHANNEL = "depth_snapshot"
WS_TRADES_CHANNEL = "fills"                        # public trades: price, seq, side, size, symbol (pairCode), time
WS_BOOK_CHANNELS = (WS_SYNC_CHANNEL, WS_SNAPSHOT_CHANNEL)
# Levels per side handed to Hummingbot's order book, and kept locally (depth_snapshot's twin and depth_sync's snapshot
# carry up to 100; the website keeps 100).
BOOK_DEPTH = 20
BOOK_KEEP_LEVELS = 100
# A depth_sync increment that starts past the book (a gap: none seen in 4 runs) waits this long; a newer snapshot
# (~300 ms) carries the book past it meanwhile.
BOOK_GAP_WAIT_S = 1.0
# depth_snapshot pushes every ~300 ms for every market whether or not the book moved: a market with no frame of any
# book channel for this long has a dead stream (P1: XDC's depth died alone, 2026-09-30). Its book is shown EMPTY and it
# is re-subscribed (no more often than RESUBSCRIBE_MIN_INTERVAL).
MARKET_SILENCE_S = 3.0
RESUBSCRIBE_MIN_INTERVAL = 5.0
# A socket that delivered nothing for this long while markets are subscribed is half-open: reconnected.
SOCKET_SILENCE_S = 5.0
# The client pings ({"event":"ping"} -> {"event":"pong"}, ~165 ms RTT from myserver); the server does not require it.
WS_HEARTBEAT_INTERVAL = 10
WS_PONG_TIMEOUT = 10
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 60
# CoinW's per-market switches: returnSymbol `state` (1 normal, 2 disabled) and returnTicker `isFrozen` (1 = frozen).
# A market that is off is shown EMPTY while its symbol and trading rule stay (non-negotiable 3). Read with the ticker.
TRADING_SWITCH_INTERVAL = 30
# returnTicker (pairCodes, last prices, isFrozen) is shared by every reader within this many seconds.
TICKER_SHARE_SECONDS = 5.0

# Reconnect pacing (XT's lesson: the listen loops reconnect at once after a ConnectionError).
WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0

# --- Private WebSocket ---------------------------------------------------------------------------------------
# Login: {"event":"login","params":{"api_key":…,"passphrase":<the SECRET>}} -> {"data":{"result":true},"channel":"login"}
# (a wrong secret: result false, errorCode 1003 "ApiKey or passphrase is invalid"). Then {"event":"sub","params":
# {"biz":"exchange","type":"order"}} and "assets", each acked {"biz":"exchange","data":{"result":true},
# "channel":"subscribe","type":…}; an unknown type answers result false, errorCode 1007.
WS_ORDER_CHANNEL = "order"
WS_ASSETS_CHANNEL = "assets"
WS_LOGIN_TIMEOUT = 10

# --- Order safety (runbook §1.5) -----------------------------------------------------------------------------
# A placement that got no answer is looked up in the market's open orders (side, price, quantity, created after the
# send) after these delays; returnOpenOrders carries no client id.
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
# Every accepted order gets a RECEIVED push; none within this long means the private stream isn't delivering.
ORDER_PUSH_EXPECTED_WITHIN = 5.0
ORDER_PUSH_ALARM_INTERVAL = 300.0
# The status poll while any order is open: partial fills of a resting order are pushed by nothing.
OPEN_ORDER_POLL_INTERVAL = 2.0
# A terminal state whose fills CoinW's trade rows don't show yet is held back (the order stays open to HMB, the next
# read looks again) for at most this long. Then the rest of the order's fill is booked from CoinW's own cumulative
# totals (returnOrderTrades success_total / success_amount / fee, or getBatchHistoryOrders dealSize / dealFunds / fee)
# as ONE fill named <orderNumber>:<cumulative size>, with a [CW-ALARM] — never forwarded short. (The docs' "Only orders
# that have been completed will be returned" on getUserTrades leaves open whether a resting or cancelled order's fills
# are listed; the first live fills settle it.) Rows that turn up later find no room (capped at the order's size).
TERMINAL_HOLD_MAX_S = 30.0
FILL_AMOUNT_TOLERANCE = Decimal("1e-12")
# getUserTrades reads for one market are at least this far apart (10 requests / 2 s per user for the account).
USER_TRADES_MIN_INTERVAL = 0.25
USER_TRADES_PAGE = 100
# A full page means more rows: the next page is asked by time (endAt = the oldest row's time when the page runs newest
# first, startAt = the newest row's when it runs oldest first), de-duplicated by tradeId, up to this many pages.
USER_TRADES_MAX_PAGES = 5
# An order's fill rows are read from this long before its creation (this host's clock vs CoinW's fill times: the
# pushes' `time` is ~145 ms behind receipt on myserver, transit included).
FILL_ROWS_LOOKBACK_S = 10.0
# A not-found answer counts toward failing an order only once the order is this old. CoinW's two order histories
# (returnUTradeHistory, getBatchHistoryOrders) list a CLOSED order only after a lag: the placement probe's order
# (2026-10-16) was in neither 0.5–3 s after its cancel, and in both 3 min later. An order that closed at once (a taker
# fill) whose answer and push were both lost can be found by its client id only there, so nothing is "not found"
# before this long. (One observation; the first measured lag tightens it.)
NOT_FOUND_MIN_AGE_S = 180.0
# The lookup of an order by its client id (returnUTradeHistory: CLOSED orders, cancelled ones with no fill included,
# listed after the lag above; open orders never; 5 / 2 s) starts this long before the order was sent: a read with no
# startAt returns 30 days of the market's orders.
CLIENT_ID_LOOKBACK_S = 60.0
# A terminal update with fills (FILLED, or CANCELED after fills) is held until the base asset's total balance shows
# the order's own fills: a strategy reads balances on completion. CoinW's `assets` push comes only with trading (docs;
# the placement probe saw none for an order's hold, 2026-10-16), on its own schedule. Reported first, a stale total
# makes the hold-band buy twice (Hotcoin's AEON, 2026-10-06). The push is awaited this long, then balances are read
# over REST (up to BALANCE_MISSING_READS times: a sold-out coin leaves the answer); still short -> [CW-ALARM]
# fill-balance-late and the update goes anyway.
FILL_BALANCE_PUSH_WAIT_S = 1.0

# --- Request timeouts ----------------------------------------------------------------------------------------
# aiohttp's session default is 300 s: a hung doTrade would leave the order without an id (no cancel, no fill) that
# long, and a hung read stalls the sequential status poll. A placement with no answer in time goes to the lookup.
PLACE_ORDER_TIMEOUT = 10.0
REQUEST_TIMEOUT = 10.0

# --- Rate limiting (CoinW's error policy, "Precautions") ------------------------------------------------------
# 60 errors per 10 s per account (80 per IP) start a 5-minute warning phase that halves every limit; more errors bring
# 30-s bans of ALL calls, cancels included, and 10 bans in 8 h an 8-hour blacklist. A 29001 answer is always a
# [CW-ALARM], and for RATE_LIMITED_SLOW_S the connector slows its own reads: the open-order poll to
# RATE_LIMITED_POLL_INTERVAL and no fills read per balance push (the poll still books them).
RATE_LIMITED_SLOW_S = 300.0
RATE_LIMITED_POLL_INTERVAL = 10.0

# --- Balance source (runbook §6.3) ---------------------------------------------------------------------------
# The assets push carries `available` + `hold` per currency after each change (`ledger_id` orders them). WS-
# authoritative until the fill test shows otherwise; a REST answer never overwrites a newer push.
REAL_TIME_BALANCE_UPDATE = True
BALANCE_AUDIT_PUSHES = 5
# returnCompleteBalances lists held coins only (the empty account answered {}, 2026-10-15). An empty answer while the
# connector holds balances is believed only after this many in a row, over at least this long: one stray {} would zero
# every balance, and the hold-band would buy the "missing" positions back.
EMPTY_BALANCE_READS = 3
EMPTY_BALANCE_SECONDS = 60.0
# An asset missing from a non-empty answer is dropped (CoinW lists held coins only) once it has been missing from this
# many answers in a row: one partial answer would zero a held position, and the hold-band would buy it back.
BALANCE_MISSING_READS = 2

# --- First-live-run audit logging ----------------------------------------------------------------------------
# Every line is tagged [CW-AUDIT]; scoped to what the docs could not settle. Money-guard firings always log as
# [CW-ALARM] WARNINGs, whatever this says.
LIVE_AUDIT_LOGGING = True

# --- Rate limits ---------------------------------------------------------------------------------------------
# Per command (docs "Frequency Limit"), linked to the global per-user budget (300/s) and the per-IP one (100/s), each
# set to ~60% of CoinW's: the AM reads CoinW from the same host with the same account, and CoinW's error policy turns
# a burst of 29001s into bans. The throttler ids are the command names.
GLOBAL_USER_LIMIT_ID = "coinw-user"
GLOBAL_IP_LIMIT_ID = "coinw-ip"
_LINKED = [LinkedLimitWeightPair(GLOBAL_USER_LIMIT_ID), LinkedLimitWeightPair(GLOBAL_IP_LIMIT_ID)]
_LINKED_PUBLIC = [LinkedLimitWeightPair(GLOBAL_IP_LIMIT_ID)]
RATE_LIMITS = [
    RateLimit(limit_id=GLOBAL_USER_LIMIT_ID, limit=180, time_interval=1),
    RateLimit(limit_id=GLOBAL_IP_LIMIT_ID, limit=60, time_interval=1),
    RateLimit(limit_id=CMD_SYMBOLS, limit=40, time_interval=1, linked_limits=_LINKED_PUBLIC),
    RateLimit(limit_id=CMD_TICKER, limit=40, time_interval=1, linked_limits=_LINKED_PUBLIC),
    RateLimit(limit_id=CMD_ORDER_BOOK, limit=6, time_interval=1, linked_limits=_LINKED_PUBLIC),
    RateLimit(limit_id=CMD_PLACE, limit=18, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_CANCEL, limit=18, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_ORDER_STATUS, limit=18, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_ORDER_DETAIL, limit=18, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_OPEN_ORDERS, limit=18, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_USER_TRADES, limit=6, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_ORDER_HISTORY, limit=3, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_BALANCES, limit=3, time_interval=2, linked_limits=_LINKED),
    RateLimit(limit_id=CMD_BATCH_HISTORY, limit=1, time_interval=3, linked_limits=_LINKED),
]

# --- Names ---------------------------------------------------------------------------------------------------
# CoinW names a few coins with a qualifier, an old name or characters a Hummingbot pair can't carry (`FLUX-NEW`'s
# hyphen). Hummingbot uses the ticker the rest of the system (P1's groups, the arb lanes' `control create` lines, the
# AM) uses; this maps CoinW's base to it. The same 11 entries as P1 (exchanges/coinw_exchange.py TICKER_ALIASES), each
# proven there on price (2026-09-30). CoinW also lists a dead plain TRUMP_USDT (an ERC20 leftover: W/D closed, $0):
# the aliased market wins, the plain one is left unmapped with a warning, as in P1.
TICKER_ALIASES = {
    "TRUMP(OFFICIAL)": "TRUMP",
    "NEIRO(ETH)": "NEIRO",
    "FORM(NEW)": "FORM",
    "WCT(NEW)": "WCT",
    "FLUX-NEW": "FLUX",
    "$B": "B",
    "DOG(RUNE)": "DOG",
    "LATPLATON": "LAT",
    "MEMECOIN": "MEME",
    "INST": "FLUID",
    "BACKPACK": "BP",
}
