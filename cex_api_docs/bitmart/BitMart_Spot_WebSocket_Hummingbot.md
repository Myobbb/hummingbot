## BitMart Spot WebSocket (Hummingbot Usage)

### Scope
This document summarizes BitMart Spot API behavior relevant to Hummingbot’s BitMart Spot connector, with emphasis on WebSocket connection liveness (heartbeats/keepalive), authentication, subscription topics, and limits. Content reflects BitMart’s public Spot API and Hummingbot’s current implementation.

Source: BitMart Spot API docs: [API Basic Information](https://developer-pro.bitmart.com/en/spot/#api-basic-information), WebSocket Subscription sections (Overview, Stay Connected and Limit, Subscribe/Unsubscribe, Public/Private channels), Error Codes, and Change Log updates on ping text. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### WebSocket Endpoints
- **Public (Spot market)**: `wss://ws-manager-compress.bitmart.com/api?protocol=1.1`
- **Private (User)**: `wss://ws-manager-compress.bitmart.com/user?protocol=1.1`

Notes:
- BitMart uses compressed WebSocket channels. See Data Compression.
- Protocol negotiation via `?protocol=1.1` is supported and used by Hummingbot.

### Data format and compression
- Messages are JSON; BitMart can return text or binary frames. When binary is returned, it is compressed and the client must decompress.
- Hummingbot auto-decompresses gzip, raw DEFLATE, and zlib before JSON parsing.
- Public messages typically contain fields like `table` and `data`; private streams also follow the `table` + `data` array pattern.

### Connection and subscription limits
Per BitMart docs (Stay Connected and Limit):
- **Connections per IP**:
  - Public WS: up to 20 per IP
  - Private WS: up to 10 per IP
- **Topics per connection**:
  - Public WS: up to ~115 topics/connection
  - Private WS: up to ~100 topics/connection
- **Message rate limits**:
  - Connect: max 30 connection requests per minute
  - Subscribe: max 100 messages per 10 seconds (includes PING text and JSON subscribe/unsubscribe)
  - One subscribe message may include up to 20 topics

Reference: Stay Connected and Limit (WebSocket) [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Keepalive (liveness) — ping text requirement
- If no data is received after connecting, the link is automatically disconnected after ~20 seconds.
- Clients should proactively send a TEXT ping to keep the connection alive:
  - Send a text frame containing `"ping"` if no message has been received within N seconds, with N < 20.
  - Expect the server to respond with a text `"pong"`. If no `"pong"` arrives in time, treat as a connection failure and reconnect.
- Ping frames are not supported. Use text `"ping"` and expect text `"pong"`.

Recommendation for Hummingbot:
- Maintain an inactivity timer per WS connection; on idle for e.g. 15–18s, send `"ping"` (text).
- On receiving `"pong"`, reset the inactivity timer.
- Do not rely solely on protocol-level ping frames.

Reference: Change Log (KeepAlive ping text) and WebSocket “Stay Connected and Limit”. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Authentication (Private WS login)
- Login message:
```json
{ "op": "login", "args": [ "<apiKey>", "<timestamp_ms>", "<sign>" ] }
```
- `timestamp_ms`: current server-synchronized timestamp in milliseconds.
- `sign`: HMAC-SHA256 hex of the string `"<timestamp_ms>#<memo>#bitmart.WebSocket"` using your secret key.
- Upon success, server replies an event acknowledging login (e.g., `{"event": "login", ...}`); otherwise error code/message.

Reference: Private Login (WebSocket) and Signature docs. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Subscribe / Unsubscribe format
- Subscribe:
```json
{ "op": "subscribe", "args": [ "<topic-1>", "<topic-2>", "..." ] }
```
- Unsubscribe:
```json
{ "op": "unsubscribe", "args": [ "<topic-1>", "<topic-2>", "..." ] }
```
- Acknowledge messages typically include an `event` field (e.g., `"subscribe"`) and/or error codes when invalid.
- Respect the topic-per-message and messages-per-window limits mentioned above.

Reference: Subscribe/Unsubscribe (WebSocket). [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Public topics used by Hummingbot
- **Trades**:
  - Topic: `spot/trade:<symbol>` (e.g., `spot/trade:BTC_USDT`)
  - Used for real-time trade ticks
- **Order Book (full snapshots)**:
  - Topic: `spot/depth50:<symbol>` (full depth snapshots, top 50)
  - Hummingbot consumes full snapshots only; no incremental diffs

Other public channels (e.g., Ticker, KLine, Depth-Increase) are available per docs but not currently consumed by the connector.

Reference: Public WebSocket channels (Ticker, KLine, Depth, Trade). [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Private topics used by Hummingbot
- **Order Progress**:
  - Topic: `spot/user/order:<symbol>`
  - Used for order state transitions, fills, and cancellations
- **Balance Change**:
  - Topic: `spot/user/balance:BALANCE_UPDATE`
  - Balance snapshots/changes by currency

Reference: Private Order Progress, Private Balance Change. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Error handling (WebSocket)
BitMart returns structured error codes for WebSocket operations, including:
- Format or parameter errors: 90001–90009
- Topic/connection limits: 90005–90007, 94001–94002
- Auth errors: 91001–91011, 91021–91023
- Symbol errors: 92001
- Internal errors: 95000

Clients should close/reconnect and/or backoff on rate/limit errors and correct invalid parameters.

Reference: WebSocket Error Code. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### REST base URLs and selected endpoints (context)
- **REST Base**: `https://api-cloud.bitmart.com`
- Commonly used (Spot):
  - Exchange info / symbols: `GET /spot/v1/symbols/details`
  - Ticker: `GET /spot/quotation/v3/ticker`
  - Order book snapshot: `GET /spot/quotation/v3/books`
  - Account wallet (balances): `GET /spot/v1/wallet` (KEYED)
  - Orders: `POST /spot/v2/submit_order`, `POST /spot/v3/cancel_order`
  - Order detail / trades: `POST /spot/v4/query/order`, `POST /spot/v4/query/order-trades`
  - Server time: `GET /system/time`

Reference: REST API sections (Basic Information, Spot/Margin Trading, Funding Account). [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Rate limits (WS-related excerpt)
Hummingbot applies throttling consistent with BitMart documented limits:
- Connect: up to 30 per 60s
- Subscribe: up to 100 per 10s
These align to the “Stay Connected and Limit” guidance.

Reference: Rate Limit & Stay Connected and Limit. [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### Notes specific to Hummingbot implementation
- Public order book uses full-snapshot channel `spot/depth50` only.
- All WS frames are decompressed if gzip/deflate/zlib.
- Current connector relies on library-level ping/pong timeouts; BitMart recommends text `"ping"` for keepalive, and will deprecate protocol ping frames in the future. See Compliance section below.

### Compliance check (current connector vs. BitMart guidance)
- Endpoints and topics:
  - Uses official WS endpoints with `?protocol=1.1` (public and private) — compliant.
  - Subscribes to `spot/trade:<symbol>` and `spot/depth50:<symbol>` — compliant with public topics used.
  - Private login payload (`op=login`, `args=[apiKey, timestamp, sign]`) — compliant.
  - Subscribes to private `spot/user/order:<symbol>` and `spot/user/balance:BALANCE_UPDATE` — compliant.
- Compression:
  - Gzip/deflate/zlib decompression supported — compliant.
- Limits & throttling:
  - Connector enforces ~30 connect/min and 100 subs/10s — compliant.
- Topic-per-message:
  - GAP: Connector batches all symbols into a single `subscribe` message per channel. If tracking > 20 symbols, this exceeds BitMart’s “max 20 topics per subscribe message”. Recommendation: chunk `args` into batches of ≤ 20 topics per subscribe call.
- Keepalive:
  - GAP: Connector does not proactively send WS text `"ping"` on idle; it relies on protocol ping/pong/timeout from the underlying WS layer. Per BitMart’s update, text ping should be used and ping frames will be unsupported in the future.
  - Recommendation: Add an inactivity watchdog per connection (idle threshold < 20s) to send text `"ping"` and expect `"pong"`, with reconnect on missing `"pong"`.

Reference: API Basic Information, WebSocket sections, Change Log (KeepAlive ping text). [`https://developer-pro.bitmart.com/en/spot/#api-basic-information`]

### References
- BitMart Spot API Basic Information and WebSocket Subscription: `https://developer-pro.bitmart.com/en/spot/#api-basic-information`


