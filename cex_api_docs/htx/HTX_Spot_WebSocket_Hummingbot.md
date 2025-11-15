# HTX Spot WebSocket (Hummingbot Usage)

## Scope
This document summarizes the official HTX Spot WebSocket behavior relevant to Hummingbot’s connector, with emphasis on connection liveness (heartbeats), authentication, and the specific topics Hummingbot subscribes to. Content below reflects the official HTX documentation.

## Endpoints
- Public market feed (except MBP incremental):
  - `wss://api.huobi.pro/ws`
  - `wss://api-aws.huobi.pro/ws`
- MBP incremental feed:
  - `wss://api.huobi.pro/feed`
  - `wss://api-aws.huobi.pro/feed`
- Private account and order:
  - `wss://api.huobi.pro/ws/v2`
  - `wss://api-aws.huobi.pro/ws/v2`

## Data format
- Public market WebSocket responses are GZIP-compressed.
- Private account and order WebSocket responses are not compressed (plain JSON).

## Heartbeats (liveness)
### Public WebSocket (market)
- Server heartbeat: every 5 seconds
- Ping example: `{ "ping": 1492420473027 }`
- Client must reply with matching pong: `{ "pong": 1492420473027 }`
- If two heartbeats are missed (no matching pong received), the server disconnects the client before the next ping.

### Private WebSocket (account and order)
- Server heartbeat: every 20 seconds
- Ping example: `{ "action": "ping", "data": { "ts": 1575537778295 } }`
- Client must reply with matching pong: `{ "action": "pong", "data": { "ts": 1575537778295 } }`
- Valid `action` values: `sub`, `unsub`, `req`, `ping`, `pong`, `push`.

## Authentication (private ws/v2)
- Send immediately after connecting.
- Request format:
```json
{
  "action": "req",
  "ch": "auth",
  "params": {
    "authType": "api",
    "accessKey": "<access-key>",
    "signatureMethod": "HmacSHA256",
    "signatureVersion": "2.1",
    "timestamp": "YYYY-MM-DDTHH:mm:ss",
    "signature": "<base64-signature>"
  }
}
```
- Signature construction (differences vs REST):
  - Method: `GET`
  - Path: `/ws/v2`
  - Host: `api.huobi.pro` (or `api-aws.huobi.pro` if using AWS endpoint)
  - Parameters used in signature: `accessKey`, `signatureMethod`, `signatureVersion`, `timestamp`
  - `signatureVersion`: `2.1`

## Rate limits (private ws/v2)
- 50 valid requests per second per connection (excludes ping/pong)
- 10 concurrent connections per API key
- 100 requests per second per IP

## Subscription message shapes
- Subscribe: `{ "sub": "<topic>", "id": "<client-id>" }` or (private) `{ "action": "sub", "ch": "<topic>" }`
- Unsubscribe: `{ "unsub": "<topic>", "id": "<client-id>" }` or (private) `{ "action": "unsub", "ch": "<topic>" }`
- Pull-style request (public): `{ "req": "<topic>", "id": "<client-id>" }` (100 ms limit per connection)

## Topics used by Hummingbot
### Public market
- Order book snapshot/refresh level:
  - `market.{symbol}.depth.step0`
- Trade detail (tick-by-tick):
  - `market.{symbol}.trade.detail`

### Private account and order
- Account updates:
  - `accounts.update#${mode}`
  - Modes:
    - `0` or unspecified: update when account balance changes
    - `1`: update when balance OR available changes
    - `2`: update both balance AND available together whenever either changes
- Order updates:
  - `orders#${symbol}` (supports wildcard `*`)
  - Event types include: `creation`, `trade`, `cancellation`, `trigger`, `deletion` (fields vary by event type)
- Trade details and post-clearing cancellation:
  - `trade.clearing#${symbol}#${mode}`
  - `mode`: `0` (trade only), `1` (trade + cancellation); default `0`

## Notes on sequencing (from official docs)
- `trade.clearing` is tick-by-tick; for IOC-like orders, a cancellation update may arrive before the related trade update.
- For strict order sequencing, subscribe to `orders#${symbol}`.

## References
- HTX API Docs (Spot > Websocket Market Data > Introduction): `https://www.htx.com/en-us/opend/newApiPages/`
- HTX API Docs (Spot > Websocket Market Data > Trade Detail): `https://www.htx.com/en-us/opend/newApiPages/`
- HTX API Docs (Spot > Websocket Account and Order > Introduction): `https://www.htx.com/en-us/opend/newApiPages/`
- HTX API Docs (Spot > Websocket Account and Order > Subscribe Account Change): `https://www.htx.com/en-us/opend/newApiPages/`
- HTX API Docs (Spot > Websocket Account and Order > Subscribe Order Updates): `https://www.htx.com/en-us/opend/newApiPages/`
- HTX API Docs (Spot > Websocket Account and Order > Subscribe Trade Details & Order Cancellation post Clearing): `https://www.htx.com/en-us/opend/newApiPages/`

