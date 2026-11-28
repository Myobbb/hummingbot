from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# CoinUp spot (ChainUp white-label), P2 build 2026-11-27. Ground truth, in order of authority: the live probes run from
# myserver on 2026-11-27 (hmb_local_tools/coinup/README.md), P1's live adapter
# (Tracker_cex_cex/ws_book_checker/exchanges/coinup.py + exchanges/coinup_exchange.py, LIVE since 2026-11-27), the docs
# mirror (VS_code_projects/MDs/coinup-api/), the wiki page trading/exchanges/coinup-api.
#
# ENGINE: ChainUp. Two REST surfaces:
#   - v1 `/open/api/*` (undocumented): the two BULK reads a cycle, UNSIGNED (the same routes GroveX used, built there
#     with md5 signing; CoinUp's bulk reads need none — probed 2026-11-27).
#   - v2 `/sapi/v1/*` (documented): public market data + the SIGNED trade/account routes (HMAC-SHA256, headers).
# No wallet/currency route exists in the documented API (no funding route at all): P3 is TRACK-ONLY (see README).
# NO PRIVATE PUSH: the only socket is market data (`kline-api`); orders, fills and balances are polled over REST.
#
# ACCESS: every host sits behind Cloudflare's managed challenge. Plain curl and most TLS fingerprints get HTTP 403
# ("Just a moment..."); from myserver `safari17_0` passed every route on the first try, while `chrome` and `chrome131`
# were challenged on every one (the passing profile is IP-dependent: 4cc chrome, the Mac chrome131/safari17_0). A burst
# of public calls put the whole IP behind the challenge for minutes (2026-11-27, probe 3): the transport must pace
# itself and retry across profiles with backoff. This is the reason the connector speaks through curl_cffi, not aiohttp.

EXCHANGE_NAME = "coinup"
DEFAULT_DOMAIN = "io"

REST_URL = "https://openapi.coinup.io"
WSS_URL = "wss://ws.marketketac.com/kline-api/ws"   # the site's own wsUrl; ws.coinup.io answers Cloudflare's 403
SITE_URL = "https://www.coinup.io"

# A browser User-Agent is refused if it is a stub: the site backend answered only with a full browser UA + Origin, and
# P1 measured a stubbed UA against a browser TLS fingerprint being challenged on 5 of 5 calls.
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
ORIGIN = "https://www.coinup.io"

# The curl_cffi profiles tried in order, with the pause before each retry. safari17_0 is the one that passed from
# myserver; chrome/chrome131 passed from 4cc/the Mac. A challenge is HTTP 403, a `Cf-Mitigated: challenge` header, or the
# "Just a moment..." page. Retries are part of the client, not an exception path.
IMPERSONATE_PROFILES = ("safari17_0", "chrome", "chrome131")
CHALLENGE_BACKOFF_SECONDS = (0.6, 1.5, 3.0, 6.0)
CHALLENGE_TRIES = len(CHALLENGE_BACKOFF_SECONDS)
# A floor between requests on the Cloudflare-fronted host, so the connector never bursts it. The site's own backend
# tolerated the same burst that put the API behind the challenge; the REST connection serialises through one lock and
# sleeps this long between calls.
REQUEST_FLOOR_SECONDS = 0.35

# --- REST paths ---------------------------------------------------------------------------------------------------
# v1, bulk, unsigned. One call each a cycle: the market list and every top + 24 h turnover.
SYMBOLS_PATH = "/open/api/common/symbols"        # 318 rows: symbol (wire, lowercase), base_coin, count_coin, precisions,
                                                 # limit_volume_min; no per-market tradability flag
ALL_TICKER_PATH = "/open/api/get_allticker"      # 318 rows, one call: isShow (the site's live flag), last/buy/sell,
                                                 # high/low/vol, amount = 24 h QUOTE turnover (amount / vol = last)
# v2, documented.
PING_PATH = "/sapi/v1/ping"
TIME_PATH = "/sapi/v1/time"                      # {"timezone", "serverTime" ms}
SAPI_SYMBOLS_PATH = "/sapi/v1/symbols"           # same 318 markets, per-market precisions + min sizes (documented)
DEPTH_PATH = "/sapi/v1/depth"                    # ?symbol=&limit=; equals the socket's book (one tick off at most)
TICKER_PATH = "/sapi/v1/ticker"                  # ?symbol= : last/buy/sell + amount (quote) + vol (base)
TRADES_PATH = "/sapi/v1/trades"
KLINES_PATH = "/sapi/v1/klines"
ACCOUNT_PATH = "/sapi/v1/account"                # SIGNED GET: {balances: [{asset, free, locked}]} — includes zero rows
CREATE_ORDER_PATH = "/sapi/v1/order"             # SIGNED POST (place) / SIGNED GET (query by orderId + symbol)
ORDER_TEST_PATH = "/sapi/v1/order/test"          # SIGNED POST: validates, never reaches the matching engine
CANCEL_ORDER_PATH = "/sapi/v1/cancel"            # SIGNED POST: {orderId, symbol} -> status PENDING_CANCEL (no fill qty)
OPEN_ORDERS_PATH = "/sapi/v1/openOrders"         # SIGNED GET: symbol (required), limit -> rows
MY_TRADES_PATH = "/sapi/v1/myTrades"             # SIGNED GET: symbol (required), limit -> per-fill rows with fee/feeCoin
# The site backend (the venue's own front end; not the API): networks, per-network W/D flags, withdraw fees, contracts.
# Public, no signature, a full browser UA + Origin required (probed 2026-11-27 from myserver: 200, 851 coins / 307 rows).
SITE_PUBLIC_INFO_PATH = "/fe-ex-api/common/public_info_v5"
SITE_WD_FEE_PATH = "/fe-ex-api/membership/get_coin_withdraw_fee_list"

# Signed GETs sign ts + METHOD + requestPath + ("?" + queryString) [+ body]; a path-only signature answers -1022
# (P1's live lesson). POSTs sign the exact JSON body sent. Headers: X-CH-APIKEY, X-CH-SIGN (hex, lower), X-CH-TS (ms).
SIGN_HEADER_KEY = "X-CH-APIKEY"
SIGN_HEADER_SIGN = "X-CH-SIGN"
SIGN_HEADER_TS = "X-CH-TS"

# --- ids and states ------------------------------------------------------------------------------------------------
ORDER_ID_MAX_LEN = 32            # newClientOrderId: documented, returned as-is; the venue does not state a max
HBOT_ORDER_ID_PREFIX = "HBOT"    # the AM's Hummingbot filter reads this via exchange_patterns["COINUP"]

# The first live run's audit switch (P2 runbook §1.6): ON until phase 6 settles the order/fill/balance unknowns; the
# money-guard alarms are NOT audit lines and stay on. Off once the unknowns settle.
LIVE_AUDIT_LOGGING = True

CODE_OK = "0"
# Business errors ride in HTTP 200 {"code": "...", "msg", "data"}; a NOT-FOUND order query answers an EMPTY OBJECT
# (probed 2026-11-27: GET /sapi/v1/order?orderId=99999999999999 -> {}), and openOrders without a symbol answers
# {"code": "-1121", "msg": "Invalid contract"}.
CODE_PARAM_ERROR = "-1121"       # missing symbol on openOrders (live)
CODE_SIGN_ERROR = "-1022"        # signature must include the query string

# The documented text states; case and the "NEW_" spelling of openOrders are handled in the map. Unknown values are left
# unmapped (None) on purpose — the phase-3 audit logs each raw string once instead of guessing.
ORDER_STATE_MAP: Dict[str, Optional[OrderState]] = {
    "new": OrderState.OPEN,
    "new_": OrderState.OPEN,
    "new order": OrderState.OPEN,
    "open": OrderState.OPEN,
    "partially filled": OrderState.PARTIALLY_FILLED,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
    "partially filled/cancelled": OrderState.CANCELED,
    "partially filled/canceled": OrderState.CANCELED,
    "partially_filled_cancelled": OrderState.CANCELED,
    "pending_cancel": OrderState.PENDING_CANCEL,
    "to be cancelled": OrderState.PENDING_CANCEL,
}

# The cancel answer carries only PENDING_CANCEL (no filled amount): the order query settles it.
IS_CANCEL_REQUEST_IN_EXCHANGE_SYNCHRONOUS = False

# --- the book socket ----------------------------------------------------------------------------------------------
# market_<wire>_depth_step0: a FULL snapshot on every push (asks ascending, `buys` = bids descending), up to 45 levels
# a side, on a fixed ~0.75 s server tick (1.34-1.38 pushes/s per market whether or not it changed — P1's measurement),
# gzip-compressed binary frames. The server sends {"ping": ms} every ~10 s and expects {"pong": ms}; a subscribe gets
# no ACK and an unknown market is silently ignored. Wire = <base_coin>.lower() + "usdt" (verified on all 317 USDT
# markets by P1), with one USDC-quoted exception.
WS_DEPTH_CHANNEL = "market_{symbol}_depth_step0"
WS_CHANNEL_PREFIX = "market_"
WS_CHANNEL_SUFFIX = "_depth_step0"
WIRE_OVERRIDES = {"XAUT": "xautusdc"}
EMIT_DEPTH = 20                  # the engine keeps 20 of the up-to-45 levels
STALE_PUSH_SECONDS = 3.0         # a push whose `ts` is older than this is never shown
MARKET_SILENCE_SECONDS = 20.0    # every market pushes ~1.35/s: silence this long is a dead stream
SILENT_RESUB_SECONDS = 60.0      # a silent market is unsub+sub'd at most this often
SOCKET_SILENCE_SECONDS = 35.0    # no frame at all for this long: reconnect (the server pings every ~10 s)
WS_HEARTBEAT_SECONDS = 15        # the client-side keepalive (RFC 6455 PING; the server answers)
WS_SUB_DELAY_SECONDS = 0.05
WS_SUB_DELAY = WS_SUB_DELAY_SECONDS            # the data source's name (grovex's, kept)
STALE_CHECK_INTERVAL = 2.0                     # the silence/staleness loop's cadence
TRADING_SWITCH_INTERVAL = 120.0                # get_allticker `isShow`, read for the empty-book switch (one Cloudflare call)
ALL_TICKER_SHARE_SECONDS = 30.0                # the exchange's shared ticker cache the switch read reuses
WS_MAX_MSG_SIZE = 512 * 1024     # a 45-level gzip frame is a few KB; room to spare
WS_RETRY_BASE_SECONDS = 1.0
WS_RETRY_CAP_SECONDS = 30.0
WS_RETRY_RESET_SECONDS = 60.0

# --- rates (documented, per 2 s; the Cloudflare floor paces the transport below them) ------------------------------
RATE_LIMITS = [
    RateLimit(limit_id="coinup_place", limit=100, time_interval=2.0),
    RateLimit(limit_id="coinup_cancel", limit=100, time_interval=2.0),
    RateLimit(limit_id="coinup_query", limit=20, time_interval=2.0),
    RateLimit(limit_id="coinup_public", limit=20, time_interval=2.0),
    RateLimit(limit_id="coinup_bulk", limit=5, time_interval=2.0),
]
# Every public read rides the public pool; the two v1 bulk reads (server-side slow under load) their own.
PUBLIC_LIMIT_IDS = {"coinup_public": 1}

# --- polling (no private push: the REST poll is the only order/fill/balance source) -------------------------------
# A Cloudflare-fronted host: poll as slowly as safety allows. While an order is open the status poll runs every 2 s
# (the tick sets the notifier every 10 ms, so this is the connector's own throttle); idle, every 10 s; balances every
# 30 s (a fill shows in /sapi/v1/account at once — probe-verified zero rows, not yet a fill; phase 6 re-checks).
ORDER_POLL_INTERVAL = 2.0
SHORT_POLL_INTERVAL = 10.0
BALANCE_POLL_INTERVAL = 30.0
TRADES_LIMIT = 200          # myTrades per symbol per poll (light); a full page is re-read at the documented max
TRADES_LIMIT_MAX = 1000     # myTrades' documented maximum, used only when the 200-row page comes back full
OPEN_ORDERS_LIMIT = 100     # openOrders' page for the unanswered-placement lookup (rows carry NO clientOrderId)
PLACEMENT_LOOKUP_WINDOW_MS = 20000   # a content match must be created within this of our send
REAL_TIME_BALANCE_UPDATE = False