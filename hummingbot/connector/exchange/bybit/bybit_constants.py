from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "bybit_main"

HBOT_ORDER_ID_PREFIX = "BYBIT-"
MAX_ORDER_ID_LEN = 32
HBOT_BROKER_ID = "Hummingbot"

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"
# Base URL
REST_URLS = {"bybit_main": "https://api.bybit.com",
             "bybit_testnet": "https://api-testnet.bybit.com"}

WSS_V5_PUBLIC_URL = {"bybit_main": "wss://stream.bybit.com/v5/public/spot",
                     "bybit_testnet": "wss://stream-testnet.bybit.com/v5/public/spot"}

WSS_PRIVATE_URL = {"bybit_main": "wss://stream.bybit.com/v5/private",
                   "bybit_testnet": "wss://stream-testnet.bybit.com/v5/private"}

# Websocket event types
ORDERBOOK_DIFF_EVENT_TYPE = "delta"
DIFF_EVENT_TYPE = "diffDepth"
TRADE_EVENT_TYPE = "trade"
SNAPSHOT_EVENT_TYPE = "depth"

# Public API endpoints
LAST_TRADED_PRICE_PATH = "/v5/market/tickers"
EXCHANGE_INFO_PATH_URL = "/v5/market/instruments-info"
SNAPSHOT_PATH_URL = "/v5/market/orderbook"
SERVER_TIME_PATH_URL = "/v5/market/time"

# Private API endpointsç
ACCOUNT_INFO_PATH_URL = "/v5/account/info"
BALANCE_PATH_URL = "/v5/account/wallet-balance"
ORDER_PLACE_PATH_URL = "/v5/order/create"
ORDER_CANCEL_PATH_URL = "/v5/order/cancel"

# Order States
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}

WS_HEARTBEAT_TIME_INTERVAL = 30

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
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_GET, 1), LinkedLimitWeightPair(REQUEST_GET_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_GET_MIXED, 1)]),
    RateLimit(limit_id=ORDER_PLACE_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),
    RateLimit(limit_id=ACCOUNT_INFO_PATH_URL, limit=MAX_REQUEST_GET, time_interval=TWO_MINUTES,
              linked_limits=[LinkedLimitWeightPair(REQUEST_POST, 1), LinkedLimitWeightPair(REQUEST_POST_BURST, 1),
                             LinkedLimitWeightPair(REQUEST_POST_MIXED, 1)]),
}
