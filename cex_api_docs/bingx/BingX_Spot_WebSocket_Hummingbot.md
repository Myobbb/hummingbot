## BingX Spot WebSocket (Hummingbot Usage)

### Scope
This document summarizes BingX Spot API behavior relevant to Hummingbot’s BingX Spot connector, with emphasis on WebSocket connection liveness (heartbeats), authentication/listen-key flow, subscription topics, and the REST endpoints used by the connector. Content reflects BingX’s public Spot API and Hummingbot’s current implementation (spot only).

Source: BingX Spot API docs (see Changelog for links/navigation) [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### WebSocket Endpoints
- **Public (Spot market and private with listen key)**:
  - `wss://open-api-ws.bingx.com/market`
- **Private User Stream (Spot)**:
  - Same endpoint, include query param: `wss://open-api-ws.bingx.com/market?listenKey=<key>`

Notes:
- Hummingbot disables protocol-level ping frames and relies on JSON-level heartbeats.
- WS frames may be gzip-compressed. Hummingbot sends `Accept-Encoding: gzip` and transparently decompresses.

### Connection limits
- Max ~200 topics per WebSocket connection (Spot). If you need more, split subscriptions across multiple connections.
- Reference: Connection Limits (Spot WS) [`https://bingx-api.github.io/docs/#/en-us/spot/socket/#Connection%20Limits`]

### REST Base URLs
- **Mainnet**: `https://open-api.bingx.com`

### Data format
- WebSocket messages are JSON (sometimes compressed with gzip). Hummingbot robustly handles bytes/string frames and falls back to plain JSON parsing if not gzip.
- Public order book “depth100” stream emits full snapshots every ~300ms; no incremental delta stream is currently consumed by the connector.

### Heartbeats (liveness)
- **Server-initiated ping**: BingX sends heartbeats approximately every 5 seconds.
  - Common forms observed in docs/examples:
    - JSON: `{ "ping": <timestamp_ms>, "time": <timestamp_ms> }`
    - Text: a frame containing the word `ping`
  - Client reply:
    - Hummingbot replies with JSON: `{ "pong": <echo ping>, "time": <echo time if provided> }`.
    - Per docs examples, servers may also accept a text `"Pong"` reply.
- **Client ping**: Not required; Hummingbot does not proactively send JSON pings for BingX. It relies on server pings and general frame activity to assess liveness.
- **Watchdog**:
  - Public WS: If no frames (including pings) for multiple heartbeats, Hummingbot disconnects and reconnects.
  - Private WS: Same policy; additionally, any error sending a required pong triggers reconnection.

Reference: Heartbeats (Spot WS) [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Authentication (Private WS Listen Key)
- Private WS uses a listen key instead of WS-op authentication.
- Create listen key (REST, signed): `POST /openApi/user/auth/userDataStream`
- Keep-alive (REST, signed): `PUT /openApi/user/auth/userDataStream?listenKey=<key>`
- Delete listen key (REST, signed): `DELETE /openApi/user/auth/userDataStream?listenKey=<key>`
- Connect WS with `?listenKey=<key>` on the same market WS endpoint to receive private updates.
- Hummingbot manages listen key lifecycle in the background: create, periodically renew, and rotate on 404/not found.
  - Implementation detail: Hummingbot renews approximately every 25 minutes to avoid expiry-related 404s.

Reference: User Data Stream / Listen Key [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Subscription message shapes
- **Subscribe**:
```json
{ "id": "<client-id>", "reqType": "sub", "dataType": "<topic>" }
```
- **Unsubscribe**:
```json
{ "id": "<client-id>", "reqType": "unsub", "dataType": "<topic>" }
```
- **Acknowledgements**: Typical success payloads include `{ "msg": "SUCCESS" }`. Error payloads include non-success `msg` or error codes/messages.

Reference: Spot WebSocket subscription format [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Topics used by Hummingbot
- **Public market (spot)**
  - Order book full snapshots (depth 100):
    - Topic: `<symbol>@depth100` (e.g., `BTC-USDT@depth100`)
    - Behavior: Full snapshots only, ~300ms cadence (no incremental diffs consumed by connector)
  - Trades (optional):
    - Topic: `<symbol>@trade`

- **Private account and order**
  - Orders/executions: `spot.executionReport`
  - Wallet/balances: `ACCOUNT_UPDATE`

Reference: Spot topics and payloads [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Sequencing and staleness (order book)
- The connector consumes full snapshots from the `@depth100` topic; no incremental delta sequence is applied.
- If snapshots stop arriving (staleness) or the WS is idle, Hummingbot re-subscribes or refreshes order book state via REST snapshot.

### REST endpoints used by Hummingbot (Spot)
- **Exchange info (symbols and trading rules)**
  - Path: `/openApi/spot/v1/common/symbols` (GET)
  - Used for: trading pair list, tick size, step size, min/max notional, etc.
- **Tickers (24h)**
  - Path: `/openApi/spot/v1/ticker/24hr` (GET)
  - Params: `symbol=<symbol>`
  - Used for: last traded price
- **Order book snapshot**
  - Path: `/openApi/spot/v1/market/depth` (GET)
  - Params: `symbol=<symbol>`, `limit=1..1000` (connector uses up to 1000)
- **Server time**
  - Path: `/openApi/swap/v2/server/time` (GET)
  - Used for: time synchronization
- **Account balances**
  - Path: `/openApi/spot/v1/account/balance` (GET, signed)
  - Used for: balances and available funds
- **Create listen key**
  - Path: `/openApi/user/auth/userDataStream` (POST, signed)
- **Keep-alive listen key**
  - Path: `/openApi/user/auth/userDataStream` (PUT, signed; param `listenKey`)
- **Delete listen key**
  - Path: `/openApi/user/auth/userDataStream` (DELETE, signed; param `listenKey`)
- **Place order**
  - Path: `/openApi/spot/v1/trade/order` (POST, signed)
  - Fields (spot): `symbol`, `side=BUY|SELL`, `type=LIMIT|MARKET`, `quantity`, `price` (for LIMIT), `newClientOrderId`
- **Cancel order**
  - Path: `/openApi/spot/v1/trade/cancel` (POST, signed)
  - Identify via `orderId` or `clientOrderId`, plus `symbol`
- **Trade query (executions/fills)**
  - Path: `/openApi/spot/v1/trade/query` (GET, signed) — trade fill details

Reference: Spot REST endpoints [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Rate limits
- The connector applies shared throttling according to BingX’s documented categories and endpoint limits.
- Balances endpoint is treated conservatively (up to ~2/s per UID as observed/docs guidance).

Reference: Spot rate limit guidance [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Enums and states
- Order status mapping aligns to Hummingbot internal states:
  - `PENDING`, `NEW`, `PARTIALLY_FILLED`, `FILLED`, `PENDING_CANCEL`, `CANCELED`, `FAILED`.

Reference: Order status and execution report fields [`https://bingx-api.github.io/docs/#/en-us/spot/changelog`]

### Notes specific to Hummingbot implementation
- Public market depth uses `@depth100` snapshots on WS; REST snapshots are used as backup.
- Private WS uses listen key; Hummingbot manages creation and renewal and reconnects when keys change/expire.
- Protocol-level ping is disabled; JSON heartbeats are used with server pings and client pong echoes.
- WS frames are decompressed when gzip-compressed.

### References
- BingX Spot API Changelog and navigation to Spot endpoints: `https://bingx-api.github.io/docs/#/en-us/spot/changelog`

