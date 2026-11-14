from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Ground truth, in order of authority: live behaviour (probed 2026-11-13 from myserver: the asset_manager's key with
# asset_manager/test_and_reference/bexc/probe_bexc_phase0.py, Pavel's placement probe
# hmb_local_tools/bexc/bexc_placement_probe.py, the raw book probe hmb_local_tools/bexc/debug_bexc_ws_cadence.py; and
# P1's tracker on 4cc since 2026-11-11, Tracker_cex_cex/ws_book_checker/exchanges/bexc.py), then the docs (offline
# mirror VS_code_projects/MDs/bexc-api/, fetched 2026-11-13). Wiki: trading/exchanges/bexc-api ("P2 + P3 onboarding").
#
# The structural facts:
#   - REST only for the account: "an API key cannot authenticate a WebSocket connection". Orders, fills and balances
#     are polled (BloFin's design); the one socket is public.
#   - The origin sits near Frankfurt behind Cloudflare: every REST call from myserver (Tokyo) costs >= 0.3 s
#     (GETs 251-304 ms, placements 265-604 ms, cancels 276-328 ms, live 2026-11-13).
#   - A WAF in front of the API blocks an IP for hours (HTTP 403 RATE_002) on route probing: documented routes and
#     listed symbols only, never an absent symbol (a 404 counts toward the block).

EXCHANGE_NAME = "bexc"
DEFAULT_DOMAIN = "io"

REST_URL = "https://api.bexc.io/api/v1"
# The engine's public socket (the website's own; P1's book source since 2026-11-11). Cloudflare; no Origin,
# subprotocol or extension needed; no permessage-deflate is negotiated: each book frame is raw-deflate itself.
WSS_URL = "wss://engine-v3.bexc.io/ws?lean=1"
# A descriptive User-Agent on every request and the socket handshake (the probes' and P1's practice; Cloudflare lets it
# through).
USER_AGENT = "P2-hummingbot/1.0 (bexc spot connector)"

# Client ids: client_order_id, 1-36 characters of A-Za-z0-9_-:. (docs). HMB's 32-character ids were accepted live
# (HBOTBBTUS6602d9ffb68e5505792e525, 2026-11-13).
ORDER_ID_MAX_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"

# --- REST paths --------------------------------------------------------------------------------------------------
SERVER_TIME_PATH = "/time"              # {"server_time": ms}; not cached
MARKETS_PATH = "/markets"               # every market + rules + flags (~475 KB, 733 rows; 1.6-1.8 s from myserver)
TICKER_PATH = "/ticker"                 # every market's last_price, one call (cached 10 s by the server)
FEES_PATH = "/fees/me"                  # signed: the account's tier rates per market (effective_fees[])
BALANCES_PATH = "/wallet/balances"      # signed: total / available / locked; zero balances are omitted
PLACE_ORDER_PATH = "/order"             # signed POST; 201 {"order": {...}, "trades": n}
CANCEL_ORDER_PATH = "/order/cancel"     # signed POST {order_id | client_order_id}; synchronous answer
ORDER_PATH = "/order/{order_id}"        # signed GET: the CCXT view (open / closed / canceled / unknown)
ORDER_BY_CLIENT_ID_PATH = "/order"      # signed GET ?client_order_id=
ORDERS_PATH = "/orders"                 # signed GET ?symbol=ALL|SYM&status=open|filled|cancelled|closed&page&limit
MY_TRADES_PATH = "/trades/my"           # signed GET ?symbol=ALL|SYM&page&limit, newest first

# --- Orders (settled by Pavel's placement probe on myserver, 2026-11-13 19:42 UTC, BTC_USDT) ---------------------
SIDE = {TradeType.BUY: "buy", TradeType.SELL: "sell"}
ORDER_TYPE_LIMIT = "limit"
# The place and cancel answers and the /orders rows: open (alias active), filled, cancelled. The CCXT view of
# GET /order/{id}: open, closed (= filled), canceled, unknown. A value outside the map is derived from the amounts,
# never guessed.
ORDER_STATE = {
    "open": OrderState.OPEN,
    "active": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "closed": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
}
TERMINAL_STATES = (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)

# Errors: an HTTP status + a body `code` (docs; the ones marked live were seen 2026-11-13).
CODE_PRICE_OUT_OF_BAND = "PRICE_OUT_OF_BAND"            # 400, live: a limit price beyond ±price_band_ratio
CODE_INVALID_CANCEL_REQUEST = "INVALID_CANCEL_REQUEST"  # 400
CODE_SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND"              # 404
CODE_ORDER_NOT_FOUND = "ORDER_NOT_FOUND"                # 404
CODE_ORDER_NOT_OPEN = "ORDER_NOT_OPEN"                  # 409, live: a cancel of a finished order
CODE_DUPLICATE_CLIENT_ORDER_ID = "DUPLICATE_CLIENT_ORDER_ID"  # 409, live: carries the first order's `order_id`
CODE_ORDER_REPLACED_CANCEL_ONLY = "ORDER_REPLACED_CANCEL_ONLY"  # 409
CODE_ORDER_RATE_LIMIT = "ORDER_RATE_LIMIT"              # 429, with Retry-After
CODE_FEE_LOOKUP_UNAVAILABLE = "FEE_LOOKUP_UNAVAILABLE"  # 503, GET /fees/me only (docs)
CODE_WAF_BLOCK = "RATE_002"                             # 403 from the WAF: the IP is blocked (hours)
# Our own codes (never bexc's): the poll's verdicts on an order (BexcExchange).
CODE_PLACEMENT_NOT_FOUND = "-404"  # an unconfirmed placement: not found by its client id, and old enough (a strike)
CODE_EMPTY_ANSWER = "-1"           # nothing to report now: the sentinel _handle_update_error_for_active_order ignores
CODE_AUTH_HELD = "AUTH_HELD"       # a signed call not sent: bexc refused the key moments ago (AUTH_REFUSED_HOLD_S)
CODE_404_BUDGET = "CLIENT_ID_404_BUDGET"  # a client-id request not sent: CLIENT_ID_404_PER_MINUTE spent

# --- Private side: no push for an API key, REST polling (BloFin's design) -----------------------------------------
# The shapes, settled by Pavel's placement probe (myserver 2026-11-13 19:42 UTC, BTC_USDT; /tmp/bexc_probe_20261113_194211.log)
# and the asset_manager's read-only runs (2026-11-13/14):
#   POST /order          HTTP 201 {"order": {id (UUID), client_order_id, status, quantity, filled_quantity,
#                        avg_fill_price, created_at (ISO, ns), sequence, ...}, "trades": n}. A crossing order is
#                        "filled" in the answer itself (the probe's taker legs: trades 1, 529-604 ms; resting or refused
#                        265-307 ms). A refusal: an HTTP status + {"code", "error"} (400 PRICE_OUT_OF_BAND, live).
#   GET /order/{id}      the CCXT view, also GET /order?client_order_id=: {id, client_order_id, status open | closed
#                        (= filled) | canceled | unknown (an untriggered stop), amount, filled, remaining, average,
#                        timestamp (ms), fee: null}. Amounts came with 6 decimals on 11-13 and 18 on 11-14.
#                        An unknown id answers 404 ORDER_NOT_FOUND (docs); an order bexc issued stays readable, a
#                        cancelled one too (live: by both ids, and the 11-13 buy again on 11-14).
#   GET /orders          ?symbol=ALL&status=open&page&limit: {"orders": [...], "paging", "total"}. A row's order id is
#                        `order_id` (the UUID); its numeric `id` (7488811) is a history row number, never the order's.
#                        Rows: status open | filled | cancelled, quantity, filled_quantity, avg_price, execution_value
#                        (quote), fee, fee_asset ("USDT" even while open), created_at, source "engine", user_id.
#   POST /order/cancel   {order_id} or {client_order_id}: 200 + the flat order, status "cancelled" with its
#                        filled_quantity (synchronous, 276-328 ms, live by client id). A finished order: 409
#                        ORDER_NOT_OPEN. The docs name the client-id cancel "the right call when you never saw the
#                        engine id (for example after a timeout)".
#   GET /trades/my       ?symbol=S|ALL&page&limit, newest first: {"trades": [{trade_id (int), order_id, quantity, price,
#                        amount (quote), fee, fee_asset, is_maker, taker_side, executed_at (ISO), ...}]}. No client id
#                        and no order filter: joined on order_id. The fee: the QUOTE on both sides (USDT, 0.1%, live).
#   GET /wallet/balances {"balances": [{asset, total, available, locked, ...}], "source"}: zero balances omitted, a coin
#                        held once stays as a 0 row. A fill shows in the balance at the first read after the answer.
#   GET /fees/me         {"tier", "effective_fees": [{symbol, maker_fee, taker_fee, source}]}: fractions (0.001 = 0.1%);
#                        Bronze 0.1% / 0.1% on all 733 markets (live 11-13).
REAL_TIME_BALANCE_UPDATE = False   # no balance push: the REST poll is authoritative (BloFin, Bitunix, BingX)
TIME_IN_FORCE = "gtc"
# A placement answered with one of these HTTP statuses was refused before anything was placed: it fails at once. 400 is
# bexc's validation (PRICE_OUT_OF_BAND live; balance and size refusals undocumented, never provoked), 401 / 403 auth and
# the WAF, 404 SYMBOL_NOT_FOUND, 429 ORDER_RATE_LIMIT ("wait for Retry-After": refused). A 409 is refused too unless it
# is the duplicate: a conflict is a refusal by state (balance, market), and the docs' only 409 for a placement is
# DUPLICATE_CLIENT_ORDER_ID, which names the order that exists (live): adopted; a duplicate naming no order is looked
# up (review C, 2026-11-14: an undocumented 409 refusal, looked up, cost ~8 404s and left the order pending ~28 s).
# Any OTHER answer (5xx, a timeout, 2xx without an order id, a body that is not JSON) proves no more than a timeout:
# the order is looked up by its client id first (runbook §1.5).
PLACE_REFUSED_STATUSES = frozenset({400, 401, 403, 404, 405, 413, 415, 422, 429})
PLACEMENT_LOOKUP_DELAYS = (0.5, 1.5)
PLACE_ORDER_TIMEOUT = 10.0
# Every other request has a timeout of its own (aiohttp's session default is 300 s), for its headers and again for its
# body (review D, 2026-11-14). GETs take 251-304 ms from myserver, ~800 ms when Cloudflare opens a new connection.
REQUEST_TIMEOUT = 10.0
# A cancel of an order whose placement still awaits bexc's answer waits for the placement to settle (its id; or given
# up as unconfirmed: then the cancel names the client id) up to this long: the placement's own timeout, its lookups and
# their timeouts (review E, 2026-11-14: the base's 10 s wait for the id ran out first, and the cancel was dropped).
PLACEMENT_SETTLE_MAX_S = (PLACE_ORDER_TIMEOUT + sum(PLACEMENT_LOOKUP_DELAYS)
                          + len(PLACEMENT_LOOKUP_DELAYS) * REQUEST_TIMEOUT + 5.0)
# /markets is ~475 KB raw (20-23 KB compressed), streamed by the origin in 0.3 s to 50 s from myserver: 2026-11-14,
# 15 reads in an hour took 0.3-24.8 s through aiohttp and 42 and 50 s through curl (the body alone 41 and 49 s,
# after a first byte at ~1 s), compressed or not alike. The headers and the body each get this long (review D).
MARKETS_REQUEST_TIMEOUT = 90.0
# An unconfirmed placement is looked up by client id (GET /order?client_order_id=): by the placement after
# PLACEMENT_LOOKUP_DELAYS, then by the status poll at most every CLIENT_ID_LOOKUP_INTERVAL_S (the poll's open-order
# list finds a resting one for free). An unknown id answers 404, and 404s may count toward the WAF's block: the
# interval bounds them to a handful per lost placement. "Not found" only once older than NOT_FOUND_MIN_AGE (s); four
# such verdicts fail it (the tracker's limit of 3).
CLIENT_ID_LOOKUP_INTERVAL_S = 5.0
NOT_FOUND_MIN_AGE = 10.0
# The client-id requests (the lookups and the cancels by client id) are the ones that answer 404 by design: at most
# this many such 404s per process in any 60 s (review C, 2026-11-14: an incident where placements fail while reads
# work would cost ~8 a placement, and the WAF's block takes P2 and P3 off bexc for hours). Past it no client-id request
# goes out: a lookup counts as "could not tell" (no strike: it only delays a FAILED verdict), a cancel as not sent;
# one [BX-ALARM] client-id-404-budget per 10 minutes.
CLIENT_ID_404_PER_MINUTE = 20
# An order bexc issued an id for that GET /order/{id} answers 404 (bexc keeps every order readable: this "can't
# happen") is never failed for it: read again after UNREADABLE_RETRY_S, doubling to UNREADABLE_RETRY_CAP_S (404s and
# the WAF), with one [BX-ALARM] order-unreadable.
UNREADABLE_RETRY_S = 5.0
UNREADABLE_RETRY_CAP_S = 300.0
# While any order is open the status poll runs every ORDER_POLL_INTERVAL (s): ONE open-orders read (every market) for
# every open order, GET /order/{id} only for an order it doesn't list, /trades/my only when an order's filled quantity
# grew (one read per market per poll, shared), one balance read. Otherwise every SHORT_POLL_INTERVAL (10 s).
ORDER_POLL_INTERVAL = 1.0
# The docs' page size (no maximum is documented, and a refused one would be a deliberate refusal): more pages instead.
OPEN_ORDERS_PAGE_LIMIT = 25
OPEN_ORDERS_MAX_PAGES = 4
FILLS_PAGE_LIMIT = 25
FILLS_MAX_PAGES = 4
# /trades/my has no time filter: a market's fills are paged back to the oldest order needing them, sent at - this.
FILLS_FLOOR_MARGIN_MS = 60_000
TIME_SYNC_INTERVAL = 300.0         # bexc accepts ±10 s; /time is ms; the AM measured offsets of 3-7 ms
# A terminal update (FILLED, or CANCELED after fills) waits for balances from a read SENT after the order's last fill
# was booked (BloFin's rule: bexc credits a fill at once, the probe's first balance read after each answer held it).
# One such read goes out at once; failing that the poll's count, up to this long (s); then the update goes, with a
# [BX-ALARM] (Hotcoin's double-buy lesson, 2026-10-06).
FILL_BALANCE_WAIT_SECONDS = 5.0
# /trades/my may trail the order's filled quantity (unproven for a resting order's partial fills: [BX-AUDIT]
# fills-behind-state settles it on the first one): read again after FILLS_RETRY_MIN_S, doubling to FILLS_RETRY_MAX_S
# while nothing changes. A terminal state whose fills stay short is held for TERMINAL_HOLD_MAX_S (s); then the rest is
# booked from the order's own totals as ONE fill with a [BX-ALARM] fill-from-cumulative (CoinW's rule): never short.
FILLS_RETRY_MIN_S = 1.0
FILLS_RETRY_MAX_S = 8.0
TERMINAL_HOLD_MAX_S = 30.0
# The size guard: bexc's own quantity of the order vs ours (the base quantity, live), checked on its first read.
SIZE_MISMATCH_TOLERANCE = Decimal("0.01")
# A guard's cancel (the size guard, the orphan guard) that fails is sent again after this long (s) while bexc still
# lists the order (review G, 2026-11-14: one failed cancel left an orphan resting for good). 409 ORDER_NOT_OPEN = done.
GUARD_CANCEL_RETRY_S = 30.0
# The market list is read by the symbol map, the trading rules, the book's trading switch and runtime adds: a read
# younger than this (s) is shared.
MARKET_LIST_SHARE_SECONDS = 60.0
TICKER_CACHE_SECONDS = 5.0         # /ticker: every market in one call, cached 10 s by bexc
# A balance answer that leaves out a coin held (an EMPTY list leaves out all of them) answers a sale or an emptied
# account, and is also what a bad moment could send (a coin held once stays as a 0 row): it is believed only after
# EMPTY_BALANCES_TRUST_S and EMPTY_BALANCES_TRUST_READS such answers in a row (the AM's rule); until then the last
# balances stay (review B, 2026-11-14: a held coin missing from one answer was zeroed at once).
EMPTY_BALANCES_TRUST_S = 120.0
EMPTY_BALANCES_TRUST_READS = 3
# The WAF blocks myserver's IP for hours, P2's and P3's calls alike (decision 4, 2026-11-13): after a RATE_002, no bexc
# REST call from this process for WAF_BLOCK_HOLD_S. After a signed 401 (re-synced and re-signed once) or a 403, no
# signed call for AUTH_REFUSED_HOLD_S: a revoked key then costs one refused call a minute, not one a second.
WAF_BLOCK_HOLD_S = 900.0
AUTH_REFUSED_HOLD_S = 60.0
# A per-market rate above this is taken as a unit error (LBank: "0.10" read as a percent) and not used.
FEE_RATE_SANITY_MAX = Decimal("0.01")

# --- Public WebSocket --------------------------------------------------------------------------------------------
# Measured on myserver 2026-11-13 (debug_bexc_ws_cadence.py, 2 runs x 180 s, 7-8 markets on one socket):
#   book_update  BINARY raw-deflate JSON (zlib wbits -15; a small one, e.g. an empty book, can come as TEXT): the
#                market's FULL book, <= 100 levels a side, best first, no zero-quantity levels, levels
#                {"price": str, "quantity": str, "order_count": int}. Pushed on change: 3.6-6.5 a second on majors,
#                2.2-2.6 on thin markets, none at all on PEPE in 180 s: not a quiet market but a frozen stream (it
#                traded all along; FROZEN_AFTER_TRADE_S below); intervals
#                p50 16-41 ms (bursts), p90 ~0.9-1.5 s. 74-79% of pushes leave the top 20 unchanged. Frames 2.2 KB
#                deflated / 11.5 KB of JSON, decoded in p50 126 us, p99 ~300 us.
#                The first push after a subscribe is the snapshot (flagged "snapshot"), 246-282 ms after it.
#                update_id +1 per push per market (0 gaps in steady state), equal to REST /orderbook's
#                last_update_id for the same book (67/67 top-20 books equal); it starts at 0 when a market had no
#                subscriber. A re-subscribe brings a new snapshot in 251-263 ms and the update_id continues.
#                REST /orderbook is ~17-19 s stale (p50 17.4 s): never a book source here, not even a bootstrap.
#   trade_update TEXT {"type","symbol","trade_id" (a UUID),"price","quantity","taker_side","timestamp" (ISO, ns, Z)},
#                p50 145 ms after the trade.
#   candle_update TEXT, ~5 a second on 8 markets: never parsed.
#   Three sockets carried identical content; per-socket lag 8-16 ms, following the ping RTT (242-261 ms): no slow path
#   to avoid. The server PINGs every ~36 s; an idle socket with no subscription stayed open 180 s.
# Subscribe: {"action":"subscribe","symbol":"BTC_USDT"}, one market a message, no ACK; {"action":"unsubscribe",...}
# ends it. The site's own client sends at most 8 messages a second.
# Per-IP limits (P1, measured from 4cc 2026-11-11): at most 30 subscriptions a SOCKET (the 31st answers TEXT
# {"type":"error",...,"Maximum subscription limit (30) reached"}, naming no symbol, and never streams), at most 10
# sockets an IP (the 11th handshake answers HTTP 429), and the gateway now and then drops every socket at once; a
# reconnect can then meet 429s for 1-3 minutes. myserver's IP carries only this connector's bexc sockets.
WS_ACTION_SUBSCRIBE = "subscribe"
WS_ACTION_UNSUBSCRIBE = "unsubscribe"
WS_MAX_MARKETS_PER_SOCKET = 28      # under the server's 30, as P1
WS_MAX_SOCKETS = 4                  # 112 markets; the IP holds 10
WS_SUBSCRIBE_SPACING = 0.2          # s between two messages on one socket (P1's pace; the site sends <= 8 a second)
EMIT_DEPTH = 20                     # levels a side handed to the tracker
# Liveness. bexc pushes a book only on change, with no server time, so a quiet market can't be told from a dead
# stream by waiting; asking can: a (re)subscribe is answered by the market's snapshot within ~0.3 s.
#   - A served market with no push for MARKET_SILENCE_S is re-subscribed (P1's silence guard, Pavel 2026-11-11: 30 s).
#     Its book stays shown meanwhile; the snapshot that answers confirms it and is emitted again, which also keeps the
#     orchestrator's 120-s stale-book check from repairing a quiet but current book (quiet != dropped).
#   - A subscribe not answered within SUBSCRIBE_ANSWER_S: the market's stream is not serving (P1 saw bexc leave some
#     streams silent after a reconnect, and a re-subscribe bring them back). Its book is shown empty and it is
#     subscribed again, the wait doubling with each unanswered try up to RESUBSCRIBE_RETRY_CAP_S.
#   - A socket that carries markets and receives no frame at all for SOCKET_SILENCE_S is reconnected (a zombie: with
#     quiet markets re-subscribed every MARKET_SILENCE_S, a live socket always hears something sooner).
#   - A dead TCP path: aiohttp pings after WS_HEARTBEAT_S of silence and closes the socket when no PONG comes within
#     half of it (P1's 20 s).
#   - A frozen stream (measured on myserver 2026-11-13): bexc can stop pushing a market's book for minutes while the
#     market trades. PEPE_USDT and LTC_USDT: one update_id for 150-180 s while 86 and 76 trades came on the same
#     subscription; a fresh snapshot every ~10 s on a second socket showed another book under that same update_id each
#     time (15/15), and 14 of PEPE's listed one price twice (individual orders). Six live markets meanwhile: every
#     snapshot equal to the last push (13-14/14). On 9 live markets every trade was followed by a push with a new
#     update_id within 1.25 s (745 trades). So a trade with no such push within FROZEN_AFTER_TRADE_S marks the stream
#     frozen, as does a subscribe answer under the held update_id with another book, or a frame listing a price twice:
#     the book is shown empty until a push with another update_id.
FROZEN_AFTER_TRADE_S = 3.0
MARKET_SILENCE_S = 30.0
SUBSCRIBE_ANSWER_S = 10.0
RESUBSCRIBE_RETRY_CAP_S = 300.0
SOCKET_SILENCE_S = 90.0
WS_HEARTBEAT_S = 20.0
LIVENESS_INTERVAL = 5.0
WS_MAX_MSG_SIZE = 4 * 1024 * 1024
# The tracker asks for a market's book right after subscribing it (startup, a runtime add): a market subscribed on a
# connected socket gets up to this long (s) for its snapshot, else the book starts empty and the stream fills it.
SNAPSHOT_REQUEST_WAIT_S = 10.0
# A runtime add parks the pushes that land while its snapshot is built and replays them over it as diffs: the same
# book, emitted again after this delay (s), replaces any merge (Coinstore's rule).
SNAPSHOT_REEMIT_DELAY = 0.5
# bexc's trading switch: /markets `status` ("trading" / "halted") and `is_active`. While a tracked market is not both,
# its book is shown EMPTY (the XT rule, 2026-09-26); its symbol and trading rule stay. Read from the shared market
# list (one ~475 KB read serves the switch, the symbol map and the trading rules).
TRADING_SWITCH_INTERVAL = 120.0
# Counters (update_id gaps and restarts, silence re-subscribes, unanswered subscribes, crossed pushes, freezes,
# undecodable frames) are logged
# at most once per STATS_INTERVAL s, and only when one moved.
STATS_INTERVAL = 60.0

WS_RETRY_BASE = 1.0
WS_RETRY_CAP = 60.0                 # bexc's 429 chains last 1-3 min: no point retrying faster than once a minute
WS_RETRY_RESET_SEC = 30.0
WS_RETRY_MAX_EXPONENT = 16          # bounded: 2 ** n on an unbounded counter overflows after ~1,025 failures (XT)

# --- First-live-run audit logging --------------------------------------------------------------------------------
# Every line tagged [BX-AUDIT], scoped to what the probes could not settle. Switch off once phase 6 has answered them.
# Money-guard firings are not audit lines: they log as [BX-ALARM].
LIVE_AUDIT_LOGGING = True

# --- Rate limits -------------------------------------------------------------------------------------------------
# Docs: 10 requests a second per key, across every route; 600 orders a minute per account (cancels are free). The WAF
# blocks an IP for hours, so the windows stay under the documented ones (a queue is better than a block), and the
# public routes count toward the same per-second budget.
REQUESTS_SECOND = "bexc_requests_second"
ORDERS_MINUTE = "bexc_orders_minute"
_ALL = [LinkedLimitWeightPair(REQUESTS_SECOND)]
RATE_LIMITS = [
    RateLimit(limit_id=REQUESTS_SECOND, limit=8, time_interval=1),
    RateLimit(limit_id=ORDERS_MINUTE, limit=500, time_interval=60),
] + [
    RateLimit(limit_id=path, limit=8, time_interval=1, linked_limits=list(_ALL))
    for path in (SERVER_TIME_PATH, MARKETS_PATH, TICKER_PATH, FEES_PATH, BALANCES_PATH, CANCEL_ORDER_PATH, ORDER_PATH,
                 ORDERS_PATH, MY_TRADES_PATH)
] + [
    # POST /order counts as orders-minute traffic. GET /order?client_order_id= shares its path string but is throttled
    # as a read, under ORDER_PATH's limit id (_find_placed_order), outside the placements' 500 a minute.
    RateLimit(limit_id=PLACE_ORDER_PATH, limit=8, time_interval=1,
              linked_limits=list(_ALL) + [LinkedLimitWeightPair(ORDERS_MINUTE)]),
]
