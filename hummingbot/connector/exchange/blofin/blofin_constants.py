from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-11-11 from myserver: the asset_manager's key with
# asset_manager/test_and_reference/blofin/probe_blofin_phase0.py, the raw book probes
# hmb_local_tools/blofin/debug_blofin_ws_cadence.py + debug_blofin_ws_paths.py, the private sockets
# debug_blofin_private_ws.py), then the docs (offline mirror VS_code_projects/MDs/blofin-api/, re-fetched 2026-11-11),
# then CCXT's blofin.ts route list. Wiki: trading/exchanges/blofin-api ("P2 + P3 onboarding").
#
# The structural facts:
#   - BloFin's docs and official SDK are FUTURES ONLY. Spot lives on an undocumented REST namespace
#     (/api/v1/spot/market/*, /api/v1/spot/trade/*), every call with instType=SPOT (152001 without it, GET and POST).
#     ⚠️ The unprefixed /api/v1/market/* and /api/v1/trade/* are PERPETUALS with the same instId ("BTC-USDT").
#   - No spot push for an API key: the API's private socket carries futures `orders` / `account` only (spot channel
#     names answer 60012), and the website's private spot socket wants a website session ("100017 user no login").
#     So orders, fills and balances are polled over REST (Bitunix's design).
#   - Spot books come only from the website's socket (the API socket is perpetuals only).

EXCHANGE_NAME = "blofin"
DEFAULT_DOMAIN = "com"

REST_URL = "https://openapi.blofin.com"
# The website's spot socket (WEB tier, P1's book source since 2026-09-30). Cloudflare (Tokyo edge from myserver: ping
# ~5 ms); no handshake header needed; text JSON.
WSS_URL = "wss://ws-public.blofin.com/ws/spot/public"
INST_TYPE = "SPOT"

# Client ids: clientOrderId, "a combination of case-sensitive alphanumerics, all numbers, or all letters of up to 32
# characters" (docs, futures). Spot order-detail and cancel-order both take it (live 2026-11-11, made-up ids). HMB's
# ids (HBOT + side + 2+2 symbol letters + hex nonce + instance id) are alphanumeric.
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths --------------------------------------------------------------------------------------------------
INSTRUMENTS_PATH = "/api/v1/spot/market/instruments"   # ?instType=SPOT: every market + rules + state (358 rows)
TICKERS_PATH = "/api/v1/spot/market/tickers"           # ?instType=SPOT: every market's last price, one call
BOOKS_PATH = "/api/v1/spot/market/books"               # ?instType=SPOT&instId=&size<=100 (bootstrap only)
BALANCES_PATH = "/api/v1/asset/balances"               # signed ?accountType=spot: every coin, zero rows included
PLACE_ORDER_PATH = "/api/v1/spot/trade/order"          # signed POST {instType, instId, side, orderType, price, size,
CANCEL_ORDER_PATH = "/api/v1/spot/trade/cancel-order"  #   clientOrderId}; cancel {instType, instId, orderId}
ORDER_DETAIL_PATH = "/api/v1/spot/trade/order-detail"  # signed GET ?instType&instId&orderId|clientOrderId
ORDERS_PENDING_PATH = "/api/v1/spot/trade/orders-pending"   # signed GET ?instType=SPOT[&instId][&limit<=100]
ORDERS_HISTORY_PATH = "/api/v1/spot/trade/orders-history"   # signed GET ?instType=SPOT&instId[&limit<=100]
FILLS_PATH = "/api/v1/spot/trade/fills-history"        # signed GET (documented under Tax) ?instType&instId&orderId
# There is no server-time route in the docs: the HTTP Date header of the smallest public answer is the clock (1 s
# resolution against a 60 s window, 152405).
SERVER_TIME_PATH = TICKERS_PATH
SERVER_TIME_PARAMS = {"instType": INST_TYPE, "instId": "BTC-USDT"}

# --- Responses -----------------------------------------------------------------------------------------------
# Every answer is HTTP 200 + {"code": "<string>", "msg", "data"}; "0" is success (live). Codes seen live 2026-11-11,
# in the order BloFin checks them: 152405 timestamp, 152401 unknown key, 152408 passphrase (a wrong secret gets it too
# while the passphrase is wrong), 152409 signature.
CODE_OK = "0"
CODE_PARAM_EMPTY = "152001"       # "Parameter instType cannot be empty." (the field is named)
CODE_PARAM_ERROR = "152002"
CODE_KEY_NOT_FOUND = "152401"
CODE_KEY_EXPIRED = "152402"
CODE_NOT_SUPPORTED = "152404"     # "This operation is not supported" (path or key permission)
CODE_TIMESTAMP_EXPIRED = "152405"  # "need to be within 60s": refused before anything runs (live, on a read)
CODE_IP_NOT_ALLOWED = "152406"
CODE_NONCE_REPEATED = "152407"
CODE_PASSPHRASE = "152408"
CODE_SIGNATURE = "152409"
CODE_CANCEL_NOT_OPEN = "106080"   # cancel: "filled, triggered, canceled or does not exist" (a made-up id, live)
CODE_RATE_LIMIT = "429"
AUTH_CODES = (CODE_KEY_NOT_FOUND, CODE_KEY_EXPIRED, CODE_NOT_SUPPORTED, CODE_IP_NOT_ALLOWED, CODE_PASSPHRASE,
              CODE_SIGNATURE)
# Not found is SILENT (live 2026-11-11): order-detail of an unknown orderId or clientOrderId answers code "0" with no
# `data` at all. "Not found" is decided by the connector, never by a code. These are our own codes for it.
# Only a placement BloFin never confirmed can be not found (BlofinExchange._adopt_placed_order): BloFin keeps every
# order it issued an id for readable, a cancelled one included (live), so for such an id an empty answer means BloFin's
# reads are empty for now, never that the order is gone (_resolve_missing_order: an alarm, no strike).
CODE_ORDER_NOT_FOUND = "-404"     # an unconfirmed placement: not found by its client id, and old enough
CODE_EMPTY_ANSWER = "-1"          # nothing to report now: not readable (yet), or the poll holds the order (the sentinel
#                                   _handle_update_error_for_active_order ignores; never a snapshot of the current state)

# --- Orders --------------------------------------------------------------------------------------------------
# Settled by Pavel's placement probe on myserver, 2026-11-11 18:14 UTC (hmb_local_tools/blofin/blofin_placement_probe.py,
# BTC-USDT): the body {instType, instId, side, orderType, price, size, clientOrderId} is taken; the answer is
# {"code":"0","data":[{"orderId":"6000023705371","clientOrderId":..,"code":"0","msg":""}]} in 18-47 ms; a refusal is
# top-level {"code":"1","msg":"All operations failed"} with the reason per order (106089 "Order minimum buy price is
# 14,886.29 USDT"). order-detail and orders-pending return the same row: state, filledSize, averagePrice, fee,
# filledAmount (quote), createTime, targetCurrency "base_currency" (`size` is the BASE quantity), feeCurrency. A
# cancel takes ~20 ms; a cancelled order stays readable (not deleted) and orders-history lists it ~1 s later. A
# 0.00001 BTC order (~0.41 USDT) was accepted: no minimum notional to speak of. The balance moved with the fill at the
# first read (0 ms): `balance` = available + frozen (the total).
SIDE = {TradeType.BUY: "buy", TradeType.SELL: "sell"}
ORDER_TYPE_LIMIT = "limit"
# The fee asset is named by ROLE (live): "base_currency" on the taker buy (0.1% charged in BTC), "quote_currency" on a
# sell's order row: the received asset, as XT.
FEE_CURRENCY_BASE = "base_currency"
FEE_CURRENCY_QUOTE = "quote_currency"
# Spot fill rows carry no maker/taker role: a fill within this many ms of its order's createTime matched on arrival
# (the probe's taker buy: 9 ms); a later fill rested first.
FILL_TAKER_WINDOW_MS = 100
# State strings (docs, futures; CCXT's map): live, partially_filled, filled, canceled, order_failed (+ effective for
# triggered algo orders). `partially_canceled` is listed by some BloFin pages; mapped too. A value outside the map is
# derived from the amounts, never guessed (BlofinExchange._order_state), and audit-logged once.
ORDER_STATE = {
    "live": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "cancelled": OrderState.CANCELED,
    "partially_canceled": OrderState.CANCELED,
    "order_failed": OrderState.FAILED,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)
# The spot buy band (order/price-range, live 2026-11-11, BTC-USDT at ~74,425): minPrice ~0.20x, maxPrice ~1.012x. A buy
# priced more than ~1.2% over the market is refused; the refusal is logged in full.

# A placement BloFin did not confirm (no answer, or "0" without an order id) is looked up by its client id
# (order-detail ?clientOrderId=) after these delays (s). Not found, it is NOT failed: it stays pending and every status
# poll looks once more, until the not-found verdict below (and the tracker's 3 strikes) fails it. The poll never
# looks while a placement is awaited.
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
PLACE_ORDER_TIMEOUT = 10.0
# Every other request has a timeout of its own (aiohttp's session default is 300 s: one hung read would stall the whole
# poll, and a hung cancel its status read).
REQUEST_TIMEOUT = 5.0
# A placement answered with one of these codes was refused before anything was placed: it fails at once. Any OTHER code
# proves no more than a timeout (runbook §1.5: CoinW's -3 internal error, and the order was created 0.45 s later) —
# BloFin's 500 and 103013 internal errors, a duplicate client id (102002 / 150003: the order may exist), a bare "1", a
# code never seen: the order is looked up by its client id first, like an unanswered placement.
#   106089 the spot price band (live); 150004 balance; 102014 / 102015 / 542 size limits; 102064 / 102065 price limits;
#   400 / 401 / 404 / 405 / 406 request errors; and every 152xxx: parameter and auth checks, made before the order is.
PLACE_REFUSED_CODES = frozenset({"106089", "150004", "102014", "102015", "542", "102064", "102065",
                                 "400", "401", "404", "405", "406"})
PLACE_REFUSED_PREFIXES = ("152",)
# A placement BloFin never confirmed and its client-id lookup can't find is "not found" only once older than this (s):
# a just-placed order may not read yet. The same age starts the order-unreadable alarm of an order BloFin issued an id
# for, which is then read again every UNREADABLE_RETRY_S (s) and never failed for it.
NOT_FOUND_MIN_AGE = 10.0
UNREADABLE_RETRY_S = 5.0

# No private push: while any order is open the status poll runs every ORDER_POLL_INTERVAL (s). One orders-pending
# read per poll covers every open order (BloFin's IP budget is 500/min and 1,500/5 min, shared with the asset_manager
# on myserver); order-detail only for an order the open list doesn't show; fills-history only when an order's filled
# quantity grew; one balance read.
ORDER_POLL_INTERVAL = 1.0
PENDING_PAGE_LIMIT = 100
TIME_SYNC_INTERVAL = 300.0

# Fills arrive by polling, the balance by another read: a terminal update (FILLED, or CANCELED after fills) waits for
# balances from a read SENT after the order's last fill was booked (BloFin credits a fill at once: the probe's balance
# read 80 ms after a taker buy's answer held it). One such read goes out at once; failing that the poll's count, up to
# this long (s); then the update goes anyway, with a [BF-ALARM] (Hotcoin's double-buy lesson, 2026-10-06).
FILL_BALANCE_WAIT_SECONDS = 5.0
# fills-history may trail the order row's filledSize (unproven for a resting order's partial fills: [BF-AUDIT]
# fills-behind-state settles it on the first one). An order whose last fills read left it short is read again after
# FILLS_RETRY_MIN_S, doubling to FILLS_RETRY_MAX_S (s) while nothing changes; a new filledSize or a terminal state reads
# at once. A terminal state whose fills stay short is held (the order stays open in Hummingbot) for TERMINAL_HOLD_MAX_S
# (s); then the rest is booked from the order row's own totals (filledSize, filledAmount, fee) as ONE fill with a
# [BF-ALARM] fill-from-cumulative (CoinW's rule, 2026-10-15): never forwarded short.
FILLS_RETRY_MIN_S = 1.0
FILLS_RETRY_MAX_S = 8.0
TERMINAL_HOLD_MAX_S = 30.0
# A quantity mismatch beyond this fraction trips the size guard (BloFin's own `size` vs ours: spot `size` is the base
# quantity, by the instruments' minSize/lotSize in base units; the first status read of every order checks it).
SIZE_MISMATCH_TOLERANCE = Decimal("0.01")

# The instrument list (~90 KB) is read by the symbol map, the trading rules, the trading switch and runtime adds: a
# read younger than this (s) is shared; a runtime add accepts one younger than MARKET_LIST_FRESH_SECONDS.
MARKET_LIST_SHARE_SECONDS = 60.0
MARKET_LIST_FRESH_SECONDS = 5.0

# --- Public WebSocket (the website's spot socket, WEB tier) ----------------------------------------------------------
# Measured on myserver 2026-11-11 (8 markets x 120 s, one socket, both channels at 100ms):
#   DEPTH     a SNAPSHOT (<= 400 levels a side) on subscribe, then UPDATE deltas chained by last_update_id = the previous
#             frame's update_id (0 gaps); a re-subscribe returns a fresh SNAPSHOT in ~7 ms that continues the chain.
#             It shows each new top FIRST: ahead of DEPTH-20 by p50 43-66 ms, p90 128-182 ms, and it showed every top
#             DEPTH-20 did (and a few short-lived ones DEPTH-20 never showed). The book is kept from it.
#   DEPTH-20  a 20-level snapshot at a FIXED cadence, changed or not (2/s at 500ms): the per-market liveness signal (a
#             market with no DEPTH-20 frame for MARKET_SILENCE_S has a dead stream) and the cross-check (it equalled
#             the DEPTH book at arrival on 99.9% of pushes).
#   TRADE     one trade per frame {id, price, quantity, amount, side, ts}; replays recent trades on subscribe.
# 5 sockets at once got identical DEPTH frames within ~1 ms of each other (p90 <= 2.3 ms): no slow path to avoid.
WS_DEPTH_CHANNEL = "DEPTH"
WS_DEPTH_SPEED = "100ms"
WS_CHECK_CHANNEL = "DEPTH-20"
WS_CHECK_SPEED = "500ms"
WS_TRADE_CHANNEL = "TRADE"
WS_TOPICS_PER_REQUEST = 20          # P1 streams 20 topics per SUBSCRIBE
BOOK_DEPTH = 20                     # levels handed to the tracker per side
BOOK_KEEP_LEVELS = 400              # what DEPTH's snapshot carries per side
# The client pings ({"op":"PING"} -> {"op":"PONG"}, ~5 ms); a socket with no subscription is closed at 60 s, a
# subscribed one stayed open 75 s without pings (live). No server pings seen. The pong is a message too: a socket
# with nothing that streams (no pairs left) must still hear something within SOCKET_SILENCE_S, so the ping runs at
# under half of it (Bitunix's review, 2026-10-07: at 15 s such a socket died every 10 s).
WS_PING_INTERVAL = 4.0
WS_PONG_TIMEOUT = 10.0
SOCKET_SILENCE_S = 10.0
MARKET_SILENCE_S = 5.0              # DEPTH-20 pushes every 0.5 s per market, changed or not
UNSYNCED_LIMIT_S = 5.0              # subscribed, no DEPTH snapshot yet: re-subscribe
# The cross-check: a DEPTH-20 frame that arrives after the local book has been quiet for XCHECK_QUIET_S must equal its
# top 20; XCHECK_MISSES in a row that don't = the book diverged: re-subscribe (a fresh snapshot in ~7 ms).
XCHECK_QUIET_S = 0.3
XCHECK_MISSES = 2
RESUBSCRIBE_MIN_INTERVAL = 2.0
# BloFin acks a subscription to any symbol, so a market that never streams (gone, or off) is repaired by the watchdog
# again and again: its limits above double with each repair that brought no snapshot, up to this (s); one WARNING per
# streak, one more when it reaches the cap. Markets BloFin has off are not repaired at all (shown empty).
DEAD_MARKET_RETRY_CAP_S = 300.0
WS_MAX_MSG_SIZE = 16 * 1024 * 1024
# BloFin's trading switch: the instrument `state` (live / suspend; 1 of 358 suspended, 2026-11-11). While a tracked
# market is not live its book is shown EMPTY (the XT rule, 2026-09-26); its symbol and trading rule stay.
TRADING_SWITCH_INTERVAL = 120

WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 30.0
WS_RETRY_RESET_SEC = 30.0
WS_RETRY_MAX_EXPONENT = 16          # bounded: 2 ** n on an unbounded counter overflows after ~1,025 failures (XT)

# --- Balance source --------------------------------------------------------------------------------------------
# No balance push exists for an API key: the REST poll is authoritative, and Hummingbot's in-flight snapshot keeps
# `available` right between polls (real_time_balance_update = False, as for Bitunix, BingX and BitMart).
REAL_TIME_BALANCE_UPDATE = False

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [BF-AUDIT], scoped to what the docs could not settle (the spot trade routes are undocumented).
# Switch off once phase 6 has answered them. Money-guard firings are not audit lines: they log as [BF-ALARM].
LIVE_AUDIT_LOGGING = True

# --- Rate limits -------------------------------------------------------------------------------------------------
# Docs: per IP 500 requests a minute (a breach suspends the IP for 5 min) and 1,500 per 5 minutes (1 hour); trading
# routes 30 per 10 s per user; HTTP 403 = a firewall ban. The IP is myserver's, shared with the asset_manager: both
# IP windows are kept well under the documented ones and every route counts toward both (a ban is worse than a
# queue); at the poll's ~2-3 reads a second with orders open the windows keep ample room for placements and cancels.
IP_MINUTE = "blofin_ip_minute"
IP_FIVE_MINUTES = "blofin_ip_five_minutes"
TRADES = "blofin_trades"
_IP = [LinkedLimitWeightPair(IP_MINUTE), LinkedLimitWeightPair(IP_FIVE_MINUTES)]
RATE_LIMITS = [
    RateLimit(limit_id=IP_MINUTE, limit=400, time_interval=60),
    RateLimit(limit_id=IP_FIVE_MINUTES, limit=1200, time_interval=300),
    RateLimit(limit_id=TRADES, limit=25, time_interval=10),
] + [
    RateLimit(limit_id=path, limit=10, time_interval=1, linked_limits=list(_IP))
    for path in (INSTRUMENTS_PATH, TICKERS_PATH, BOOKS_PATH, BALANCES_PATH, ORDER_DETAIL_PATH, ORDERS_PENDING_PATH,
                 ORDERS_HISTORY_PATH, FILLS_PATH)
] + [
    RateLimit(limit_id=path, limit=10, time_interval=1, linked_limits=list(_IP) + [LinkedLimitWeightPair(TRADES)])
    for path in (PLACE_ORDER_PATH, CANCEL_ORDER_PATH)
]
