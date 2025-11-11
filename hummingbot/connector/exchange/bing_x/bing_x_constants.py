from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "main"

HBOT_ORDER_ID_PREFIX = "BING_X_"
MAX_ORDER_ID_LEN = 40

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_IOC = "IOC"
TIME_IN_FORCE_POC = "POC"

REST_URLS = {"main": "https://open-api.bingx.com"}

WSS_PUBLIC_URL = {"main": "wss://open-api-ws.bingx.com/market"}

WSS_PRIVATE_URL = {"main": "wss://open-api-ws.bingx.com/market"}

# Websocket event types
# Use explicit queue keys aligned with tracker defaults
DIFF_EVENT_TYPE = "order_book_diff"
TRADE_EVENT_TYPE = "trade"
SNAPSHOT_EVENT_TYPE = "order_book_snapshot"

# Public API endpoints
LAST_TRADED_PRICE_PATH = "/openApi/spot/v1/ticker/24hr"
EXCHANGE_INFO_PATH_URL = "/openApi/spot/v1/common/symbols"
SNAPSHOT_PATH_URL = "/openApi/spot/v1/market/depth"
SERVER_TIME_PATH_URL = "/openApi/swap/v2/server/time"

# Private API endpoints
USER_STREAM_PATH_URL = "/openApi/user/auth/userDataStream"
ACCOUNTS_PATH_URL = "/openApi/spot/v1/account/balance"
MY_TRADES_PATH_URL = "/openApi/spot/v1/trade/query"
ORDER_PATH_URL = "/openApi/spot/v1/trade/order"
CANCEL_ORDER_PATH_URL = "/openApi/spot/v1/trade/cancel"
# Order details (per-order status) endpoint
ORDER_INFO_PATH_URL = "/openApi/spot/v1/trade/orderInfo"

WS_HEARTBEAT_TIME_INTERVAL = 5

# Orderbook management
ONE_HOUR = 60 * 60  # Periodic snapshot interval (seconds)
SNAPSHOT_DEPTH_LIMIT = 1000  # Maximum depth levels per REST snapshot
DIFF_BATCH_SIZE = 100  # Max diffs to process per iteration (legacy for REST fallback)

# BingX-specific WebSocket stream identifiers
BINGX_DEPTH_LEVEL = "100"          # Depth level for market depth subscription
BINGX_DEPTH_STREAM_SUFFIX = "@depth100"  # Market depth stream (300ms updates, full snapshots)
BINGX_TRADE_STREAM_SUFFIX = "@trade"

# Order States
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "CANCELED": OrderState.CANCELED,
    "FAILED": OrderState.FAILED,
}

# Rate Limit Type
REQUEST_GET = "GET"
REQUEST_GET_BURST = "GET_BURST"
REQUEST_GET_MIXED = "GET_MIXED"
REQUEST_POST = "POST"
REQUEST_POST_BURST = "POST_BURST"
REQUEST_POST_MIXED = "POST_MIXED"

# Rate Limit Max request

MAX_REQUEST_GET = 6000
MAX_REQUEST_GET_BURST = 70
MAX_REQUEST_GET_MIXED = 400
MAX_REQUEST_POST = 2400
MAX_REQUEST_POST_BURST = 50
MAX_REQUEST_POST_MIXED = 270

# Rate Limit time intervals
TWO_MINUTES = 120
ONE_SECOND = 1
SIX_SECONDS = 6
ONE_DAY = 86400

RATE_LIMITS = {
    # General
    RateLimit(limit_id=REQUEST_GET, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES),
    RateLimit(limit_id=REQUEST_GET_BURST, limit=MAX_REQUEST_GET_BURST, time_interval=ONE_SECOND),
    RateLimit(limit_id=REQUEST_GET_MIXED, limit=MAX_REQUEST_GET_MIXED, time_interval=SIX_SECONDS),
    RateLimit(limit_id=REQUEST_POST, limit=MAX_REQUEST_POST, time_interval=TWO_MINUTES),
    RateLimit(limit_id=REQUEST_POST_BURST, limit=MAX_REQUEST_POST_BURST, time_interval=ONE_SECOND),
    RateLimit(limit_id=REQUEST_POST_MIXED, limit=MAX_REQUEST_POST_MIXED, time_interval=SIX_SECONDS),
    # Linked limits
    RateLimit(limit_id=LAST_TRADED_PRICE_PATH, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    # Per docs: user data stream endpoints limited to ~2/s by UID/IP
    RateLimit(limit_id=USER_STREAM_PATH_URL, limit=2, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST_GET, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=ORDER_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),
    RateLimit(limit_id=ORDER_INFO_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),

    # Balance endpoint: 2/s per UID (per BingX API docs)
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=1, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),

}


EXCHANGE_NAME = "bing_x"
HBOT_BROKER_ID = "hummingbot"
HBOT_ORDER_ID = "t-HBOT"

REST_URL = "https://open-api.bingx.com/openApi"
SOURCE_KEY = 'Hummingbot'

ENABLE_ANTI_SPOOFING = False  
SPOOFING_MIN_CONFIRMATION_TIME_MS = 601