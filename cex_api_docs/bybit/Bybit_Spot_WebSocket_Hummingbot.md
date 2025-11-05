# Bybit Spot WebSocket (Hummingbot Usage)

## Scope
This document summarizes Bybit V5 API behavior relevant to Hummingbot’s Bybit Spot connector, with emphasis on WebSocket connection liveness (heartbeats), authentication, and the specific topics Hummingbot subscribes to. It also lists the REST endpoints used by the connector. Content reflects Bybit’s official V5 documentation and Hummingbot’s current implementation (spot only).

## WebSocket Endpoints
- Public (Spot):
  - `wss://stream.bybit.com/v5/public/spot`
  - `wss://stream-testnet.bybit.com/v5/public/spot`
- Private:
  - `wss://stream.bybit.com/v5/private`
  - `wss://stream-testnet.bybit.com/v5/private`

## REST Base URLs
- Mainnet: `https://api.bybit.com`
- Testnet: `https://api-testnet.bybit.com`

## Data format
- WebSocket messages are plain JSON.
- Public order book topics send event `type` values of `snapshot` and `delta`.
- Frames contain exchange timestamps `ts` (ms) and sometimes engine timestamps `cts` (ms).

## Heartbeats (liveness)
### Public WebSocket
- Client ping: Hummingbot sends an application-level ping every 20s: `{ "op": "ping", "req_id": "<ms-timestamp>" }`.
- Server response: Bybit ack may arrive as `{ "op": "pong", "req_id": "..." }` or `{ "op": "ping", "ret_msg": "pong" }` depending on channel behavior. Hummingbot treats both as valid acks.
- Server-initiated ping: If server sends `{ "op": "ping", ... }`, client replies with `{ "op": "pong", "req_id": "<echo ts|req_id>" }`.
- Watchdog: If no frames (including pong/acks) for > ~2.5 heartbeats, Hummingbot reconnects the public WS. If data is idle but acks still arrive, Hummingbot resnapshots/resubscribes topics without reconnecting.

### Private WebSocket
- Client ping: Hummingbot sends `{ "op": "ping", "req_id": "<ms-timestamp>" }` every ~20s.
- Server-initiated ping: Hummingbot replies `{ "op": "pong", "req_id": "..." }`.
- Watchdog: If no frames for > ~2 heartbeats, Hummingbot reconnects the private WS.

References: `WebSocket Stream` (see Public/Private sections in Bybit V5 docs) [`https://bybit-exchange.github.io/docs/v5/intro`]

## Authentication (private WS)
- Send immediately after connecting to the private endpoint.
- Message format:
```json
{ "op": "auth", "args": [ "<api_key>", <timestamp_ms>, "<signature>" ] }
```
- Signature (V5): HMAC SHA256 with your API secret over the string `GET/realtime` concatenated with the same `<timestamp_ms>`, hex-encoded.
- Hummingbot uses a short-lived future timestamp ("expires") in milliseconds and the same signing string, i.e. `args = [apiKey, expiresMs, sign("GET/realtime" + expiresMs)]`.

References: `Guide – Parameters for authenticated endpoints` [`https://bybit-exchange.github.io/docs/v5/intro`]

## Subscription message shapes
- Subscribe:
```json
{ "op": "subscribe", "args": [ "<topic>" ] }
```
- Unsubscribe:
```json
{ "op": "unsubscribe", "args": [ "<topic>" ] }
```
- Acknowledgements may include `{ "op": "subscribe", "success": true, "args": ["<topic>"] }` or an error with `success: false` and `ret_msg`.

## Topics used by Hummingbot
### Public market (spot)
- Order book snapshot and incremental deltas (depth 50):
  - Topic: `orderbook.50.{symbol}` (e.g., `orderbook.50.BTCUSDT`)
  - Event types:
    - `snapshot`: full L50 order book snapshot
    - `delta`: incremental updates; messages include sequence fields such as `u`.
- Trades (optional):
  - Topic: `publicTrade.{symbol}`
  - Note: Hummingbot’s current Bybit Spot connector disables public trades stream by default.

### Private account and order
- Orders: `order`
- Executions (fills): `execution`
- Wallet/balances: `wallet`

References (WebSocket):
- Public Orderbook (V5): `https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook`
- Public Trades (V5): `https://bybit-exchange.github.io/docs/v5/websocket/public/trade`
- Private Orders: `https://bybit-exchange.github.io/docs/v5/websocket/private/order`
- Private Executions: `https://bybit-exchange.github.io/docs/v5/websocket/private/execution`
- Private Wallet: `https://bybit-exchange.github.io/docs/v5/websocket/private/wallet`

## Sequencing and staleness (order book)
- Public orderbook frames include sequence fields (e.g., `u`). Hummingbot drops stale/duplicate deltas (`u` not strictly increasing per symbol).
- Snapshots are used to initialize per-symbol state; deltas apply thereafter.
- If a symbol shows no orderbook data for a while, Hummingbot attempts topic re-subscribe and, if needed, forces a fresh REST snapshot or reconnects the WS.

## REST endpoints used by Hummingbot (Spot)
All REST calls use `category=spot` when required.

- Exchange info (instrument metadata and rules)
  - Path: `/v5/market/instruments-info` (GET)
  - Params: `category=spot`
  - Used for: trading pair map, tick size, lot size, min notional, precisions
  - Doc: `https://bybit-exchange.github.io/docs/v5/market/instruments-info`

- Tickers (last price)
  - Path: `/v5/market/tickers` (GET)
  - Params: `category=spot`, `symbol=<symbol>`
  - Used for: last traded price
  - Doc: `https://bybit-exchange.github.io/docs/v5/market/tickers`

- Order book snapshot (REST backup / hourly refresh)
  - Path: `/v5/market/orderbook` (GET)
  - Params: `category=spot`, `symbol=<symbol>`, `limit=1..200` (Hummingbot caps to 200; WS uses L50)
  - Doc: `https://bybit-exchange.github.io/docs/v5/market/orderbook`

- Server time (time sync)
  - Path: `/v5/market/time` (GET)
  - Used for: time synchronization; Hummingbot prefers `timeNano` for higher resolution
  - Doc: `https://bybit-exchange.github.io/docs/v5/market/time`

- Account info
  - Path: `/v5/account/info` (GET, auth)
  - Used for: determining account/unified status (e.g., `unifiedMarginStatus`)
  - Doc: `https://bybit-exchange.github.io/docs/v5/account/info`

- Wallet balance
  - Path: `/v5/account/wallet-balance` (GET, auth)
  - Params: `accountType=SPOT|UNIFIED|...`
  - Used for: balances and available funds
  - Doc: `https://bybit-exchange.github.io/docs/v5/account/wallet-balance`

- Place order
  - Path: `/v5/order/create` (POST, auth)
  - Required fields (spot usage): `category=spot`, `symbol`, `side=Buy|Sell`, `orderType=Limit|Market`, `qty`, `orderLinkId`
  - Additional: `price` (for Limit), `timeInForce=GTC` (for Limit), `marketUnit=baseCoin` (for market qty unit)
  - Doc: `https://bybit-exchange.github.io/docs/v5/order/create`

- Cancel order
  - Path: `/v5/order/cancel` (POST, auth)
  - Identify via `orderId` or `orderLinkId`, plus `category=spot`, `symbol`
  - Doc: `https://bybit-exchange.github.io/docs/v5/order/cancel`

- Get open orders (real-time)
  - Path: `/v5/order/realtime` (GET, auth)
  - Params: `category=spot`, `symbol`, optionally `orderId` or `orderLinkId`
  - Used for: polling order status
  - Doc: `https://bybit-exchange.github.io/docs/v5/order/realtime`

- Trade history (executions)
  - Path: `/v5/execution/list` (GET, auth)
  - Params: `category=spot`, `symbol`, optional filters like `orderId|orderLinkId`, `execType=Trade`
  - Used for: reconstructing fills for an order
  - Doc: `https://bybit-exchange.github.io/docs/v5/execution/list`

- Account fee rates
  - Path: `/v5/account/fee-rate` (GET, auth)
  - Params: `category=spot`, optional `symbol`
  - Used for: maker/taker fee rates per trading pair
  - Doc: `https://bybit-exchange.github.io/docs/v5/account/fee-rate`

## Rate limits
- IP rate limit: no more than 600 requests in any rolling 5-second window.
- Default per-endpoint limit: ~20 requests/second (varies by role/tier; connector uses shared throttling).
- WebSocket message and connection limits apply as per Bybit’s docs.

Doc: `https://bybit-exchange.github.io/docs/v5/rate-limit`

## Enums and states
- Order status mapping used by Hummingbot aligns with Bybit’s `orderStatus` enum (`New`, `PartiallyFilled`, `Filled`, `Cancelled`, `PartiallyFilledCanceled`, `Rejected`).

Doc: `https://bybit-exchange.github.io/docs/v5/enum#orderstatus`

## Notes specific to Hummingbot implementation
- Category is always `spot` for all relevant REST/WS operations.
- Public orderbook depth used on WS is L50 (`orderbook.50.{symbol}`); REST snapshots cap at `limit<=200`.
- Private WS connection URL includes an optional `?max_active_time=5m` query to hint session longevity; not required by Bybit.
- Hummingbot proactively resubscribes/resnapshots on per-symbol staleness, and reconnects WS if heartbeats are missed.

## References
- Bybit V5 Introduction: `https://bybit-exchange.github.io/docs/v5/intro`
- Market Endpoints: `https://bybit-exchange.github.io/docs/v5/market/instruments-info`, `https://bybit-exchange.github.io/docs/v5/market/tickers`, `https://bybit-exchange.github.io/docs/v5/market/orderbook`, `https://bybit-exchange.github.io/docs/v5/market/time`
- Account/Order Endpoints: `https://bybit-exchange.github.io/docs/v5/account/info`, `https://bybit-exchange.github.io/docs/v5/account/wallet-balance`, `https://bybit-exchange.github.io/docs/v5/account/fee-rate`, `https://bybit-exchange.github.io/docs/v5/order/create`, `https://bybit-exchange.github.io/docs/v5/order/cancel`, `https://bybit-exchange.github.io/docs/v5/order/realtime`, `https://bybit-exchange.github.io/docs/v5/execution/list`
- WebSocket Streams: `https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook`, `https://bybit-exchange.github.io/docs/v5/websocket/public/trade`, `https://bybit-exchange.github.io/docs/v5/websocket/private/order`, `https://bybit-exchange.github.io/docs/v5/websocket/private/execution`, `https://bybit-exchange.github.io/docs/v5/websocket/private/wallet`
- Rate Limit and Enums: `https://bybit-exchange.github.io/docs/v5/rate-limit`, `https://bybit-exchange.github.io/docs/v5/enum#orderstatus`

