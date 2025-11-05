# HTX Spot WebSocket API Documentation

## Overview

This document covers the HTX (formerly Huobi) Spot WebSocket API, focusing on authentication, connection management, and key data subscriptions for spot trading.

## Base Information

### WebSocket Endpoints

#### Public Market Data WebSocket
- **Primary:** `wss://api.huobi.pro/ws`
- **AWS Optimized:** `wss://api-aws.huobi.pro/ws`
- **Purpose:** Market data (excluding MBP incremental)
- **Authentication:** Not required
- **Data Format:** GZIP compressed (must be decompressed)

#### MBP Incremental Feed
- **Primary:** `wss://api.huobi.pro/feed`
- **AWS Optimized:** `wss://api-aws.huobi.pro/feed`
- **Purpose:** Market By Price incremental updates
- **Authentication:** Not required
- **Data Format:** GZIP compressed

#### Private Account and Order WebSocket
- **Primary:** `wss://api.huobi.pro/ws/v2`
- **AWS Optimized:** `wss://api-aws.huobi.pro/ws/v2`
- **Purpose:** Account updates, order updates, private data
- **Authentication:** Required
- **Data Format:** NOT compressed (plain JSON)

**Note:** AWS-optimized endpoints (`api-aws.huobi.pro`) provide lower latency for clients hosted on AWS infrastructure.

---

## Public WebSocket (Market Data)

### Connection Setup

1. Connect to `wss://api.huobi.pro/ws`
2. All received data is GZIP compressed and must be decompressed
3. No authentication required for public channels

### Heartbeat Mechanism (Public)

**Server sends ping every 5 seconds:**
```json
{
  "ping": 1492420473027
}
```

**Client must respond with pong:**
```json
{
  "pong": 1492420473027
}
```

**Important:** 
- The pong message must contain the same integer value as the ping
- If server sends two consecutive pings without receiving at least one pong response, the connection will be disconnected before the next ping is sent
- Keep-alive interval: 5 seconds

### Subscription Format

**Subscribe Request:**
```json
{
  "sub": "topic_to_subscribe",
  "id": "client_generated_id"
}
```

**Subscribe Response:**
```json
{
  "id": "client_generated_id",
  "status": "ok",
  "subbed": "topic_to_subscribe",
  "ts": 1489474081631
}
```

**Unsubscribe Request:**
```json
{
  "unsub": "topic_to_unsubscribe",
  "id": "client_generated_id"
}
```

**Unsubscribe Response:**
```json
{
  "id": "client_generated_id",
  "status": "ok",
  "unsubbed": "topic_to_unsubscribe",
  "ts": 1494326028889
}
```

### Pull-Style Data Request

You can also request data in pull style (one-time request):

**Request:**
```json
{
  "req": "topic_to_request",
  "id": "client_generated_id"
}
```

**Rate Limit:** 100ms per connection for pull-style requests

---

## Market Depth (Orderbook)

### Topic Format
```
market.$symbol.depth.$type
```

### Subscription Parameters

| Parameter | Type | Required | Description | Values |
|-----------|------|----------|-------------|--------|
| symbol | string | Yes | Trading pair | btcusdt, ethusdt, etc. |
| type | string | Yes | Aggregation level | step0, step1, step2, step3, step4, step5 |

**Aggregation Levels:**
- `step0`: No aggregation
- `step1`: Aggregation level = precision × 10
- `step2`: Aggregation level = precision × 100
- `step3`: Aggregation level = precision × 1000
- `step4`: Aggregation level = precision × 10000
- `step5`: Aggregation level = precision × 100000

### Subscription Example

**Single symbol:**
```json
{
  "sub": "market.btcusdt.depth.step0",
  "id": "id1"
}
```

**Multiple symbols:**
```json
{
  "sub": [
    "market.btcusdt.depth.step0",
    "market.ethusdt.depth.step0",
    "market.trxusdt.depth.step0"
  ],
  "id": "id1"
}
```

### Success Response
```json
{
  "id": "id1",
  "status": "ok",
  "subbed": "market.btcusdt.depth.step0",
  "ts": 1489474081631
}
```

### Data Update Format

```json
{
  "ch": "market.btcusdt.depth.step0",
  "ts": 1630983549503,
  "tick": {
    "bids": [
      [52690.69, 0.36281],
      [52690.68, 0.2]
    ],
    "asks": [
      [52690.7, 0.372591],
      [52691.26, 0.13]
    ],
    "version": 136998124622,
    "ts": 1630983549500
  }
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| ch | string | Channel name: `market.$symbol.depth.$type` |
| ts | long | Response generation time (milliseconds) |
| tick.bids | array | All current bids as `[price, size]` pairs |
| tick.asks | array | All current asks as `[price, size]` pairs |
| tick.version | integer | Internal data version |
| tick.ts | long | UNIX timestamp (milliseconds, Singapore time) |

**Update Frequency:** Every 1 second (snapshot mode)

---

## Trade Detail

### Topic Format
```
market.$symbol.trade.detail
```

### Subscription Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| symbol | string | Yes | Trading pair (e.g., btcusdt, ethusdt) |

### Subscription Example

**Single symbol:**
```json
{
  "sub": "market.btcusdt.trade.detail",
  "id": "id1"
}
```

**Multiple symbols:**
```json
{
  "sub": [
    "market.btcusdt.trade.detail",
    "market.ethusdt.trade.detail",
    "market.htxusdt.trade.detail"
  ],
  "id": "id1"
}
```

### Success Response
```json
{
  "id": "id1",
  "status": "ok",
  "subbed": "market.btcusdt.trade.detail",
  "ts": 1489474081631
}
```

### Data Update Format

```json
{
  "ch": "market.btcusdt.trade.detail",
  "ts": 1630994963175,
  "tick": {
    "id": 137005445109,
    "ts": 1630994963173,
    "data": [
      {
        "id": 1.3700544510935929e+26,
        "ts": 1630994963173,
        "tradeId": 102523573486,
        "amount": 0.006754,
        "price": 52648.62,
        "direction": "buy"
      }
    ]
  }
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| ch | string | Channel name: `market.$symbol.trade.detail` |
| ts | long | Response generation time (milliseconds) |
| tick.id | long | Global transaction ID |
| tick.ts | long | Latest creation time |
| tick.data | array | Array of trade objects |
| data[].id | integer | Unique trade ID (deprecated, use tradeId) |
| data[].tradeId | long | Unique trade ID (recommended) |
| data[].amount | float | Trade volume (base currency) |
| data[].price | float | Trade price (quote currency) |
| data[].ts | long | Trade timestamp (milliseconds) |
| data[].direction | string | Taker direction: `buy` or `sell` |

**Update Mode:** Tick-by-tick (real-time)

**Best Practice:** Use `tradeId` for deduplication instead of the deprecated `id` field.

---

## Private WebSocket (Account and Order)

### Connection Setup

1. Connect to `wss://api.huobi.pro/ws/v2`
2. Data is **NOT** GZIP compressed (plain JSON)
3. Authentication is **required** before subscribing to private channels
4. Must authenticate immediately after connection

### Data Format (Private)

- Return data for Account and Order WebSocket is not GZIP-compressed (plain JSON)

### Heartbeat Mechanism (Private)

**Server sends ping every 20 seconds:**
```json
{
  "action": "ping",
  "data": {
    "ts": 1575537778295
  }
}
```

**Client must respond with pong:**
```json
{
  "action": "pong",
  "data": {
    "ts": 1575537778295
  }
}
```

**Important:**
- Keep-alive interval: 20 seconds
- Must respond with the same timestamp value from ping message
- Different format from public WebSocket (uses "action" field)

### Valid Action Values

| Action | Description |
|--------|-------------|
| sub | Subscribe to a topic |
| unsub | Unsubscribe from a topic |
| req | Request data (authentication) |
| ping | Heartbeat from server |
| pong | Heartbeat response from client |
| push | Data push from server to client |

### Rate Limits

- **Per connection:** 50 valid requests per second (excluding ping/pong)
- **Per API Key:** Maximum 10 concurrent connections
- **Per IP:** 100 requests per second
- Exceeding limits returns "too many requests" or "too many connections" error

---

## Authentication Flow

### Step 1: Generate Signature

Authentication uses signature version 2.1 with the following parameters:

**Required Parameters:**
- `accessKey`: Your API access key
- `signatureMethod`: `HmacSHA256` or `Ed25519`
- `signatureVersion`: `2.1`
- `timestamp`: UTC time in format `YYYY-MM-DDTHH:mm:ss` (e.g., `2019-09-01T18:16:16`)

**Pre-signature String Format:**
```
GET\n
api.huobi.pro\n
/ws/v2\n
accessKey=<your-access-key>&signatureMethod=HmacSHA256&signatureVersion=2.1&timestamp=2019-12-05T11%3A53%3A03
```

**Signature Steps:**

1. **Method:** `GET`
2. **Host:** `api.huobi.pro` (lowercase)
3. **Path:** `/ws/v2`
4. **Parameters:** URL-encoded and sorted by ASCII order
   - Colon `:` → `%3A`
   - Space → `%20`
   - All hex characters must be uppercase

5. **Concatenate** with `&` separator:
```
accessKey=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx&signatureMethod=HmacSHA256&signatureVersion=2.1&timestamp=2019-12-05T11%3A53%3A03
```

6. **Sign** using HmacSHA256 with your Secret Key
7. **Encode** signature with Base64

**Example pre-signed text:**
```
GET
api.huobi.pro
/ws/v2
accessKey=0664b695-rfhfg2mkl3-abbf6c5d-49810&signatureMethod=HmacSHA256&signatureVersion=2.1&timestamp=2019-12-05T11%3A53%3A03
```

**Important Notes:**
- Timestamp is valid for 5 minutes
- JSON payload does NOT require URL encoding
- Only the query parameters need URL encoding

### Step 2: Send Authentication Request

**Request Format:**
```json
{
  "action": "req",
  "ch": "auth",
  "params": {
    "authType": "api",
    "accessKey": "e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx",
    "signatureMethod": "HmacSHA256",
    "signatureVersion": "2.1",
    "timestamp": "2019-09-01T18:16:16",
    "signature": "4F65x5A2bLyMWVQj3Aqp+B4w+ivaA7n5Oi2SuYtCJ9o="
  }
}
```

**Request Fields:**

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| action | Yes | string | Must be `req` |
| ch | Yes | string | Must be `auth` |
| params.authType | Yes | string | Must be `api` (not part of signature) |
| params.accessKey | Yes | string | Your API access key |
| params.signatureMethod | Yes | string | `HmacSHA256` or `Ed25519` |
| params.signatureVersion | Yes | string | Must be `2.1` |
| params.timestamp | Yes | string | UTC timestamp `YYYY-MM-DDTHH:mm:ss` |
| params.signature | Yes | string | Base64-encoded signature |

### Step 3: Receive Authentication Response

**Success Response:**
```json
{
  "action": "req",
  "code": 200,
  "ch": "auth",
  "data": {}
}
```

**Error Response:**
```json
{
  "action": "req",
  "code": <error_code>,
  "ch": "auth",
  "message": "<error_message>"
}
```

After successful authentication, you can subscribe to private channels.

---

## Account Updates Channel

### Topic Format
```
accounts.update#${mode}
```

### Subscription Parameters

| Parameter | Type | Required | Description | Default |
|-----------|------|----------|-------------|---------|
| mode | integer | No | Trigger mode | 0 |

**Mode Options:**

| Mode | Topic | Behavior |
|------|-------|----------|
| 0 (or unspecified) | `accounts.update` or `accounts.update#0` | Update only when account balance changes |
| 1 | `accounts.update#1` | Update when either account balance OR available balance changes |
| 2 | `accounts.update#2` | Update both account balance AND available balance together whenever either changes |

### Subscription Example

**Mode 0 (default):**
```json
{
  "action": "sub",
  "ch": "accounts.update"
}
```

**Mode 1:**
```json
{
  "action": "sub",
  "ch": "accounts.update#1"
}
```

**Mode 2:**
```json
{
  "action": "sub",
  "ch": "accounts.update#2"
}
```

### Success Response
```json
{
  "action": "sub",
  "code": 200,
  "ch": "accounts.update#0",
  "data": {}
}
```

### Initial Snapshot

**Important:** Upon subscription, the server first sends the current static values of individual accounts. In these initial messages:
- `changeType` field is `null`
- `changeTime` field is `null`

This is followed by real-time account change updates.

### Data Update Format

**Mode 0 Example (balance change only):**
```json
{
  "action": "push",
  "ch": "accounts.update#0",
  "data": {
    "currency": "btc",
    "accountId": 123456,
    "balance": "23.111",
    "changeType": "transfer",
    "accountType": "trade",
    "seqNum": "86872993928",
    "changeTime": 1568601800000
  }
}
```

**Mode 1 Examples (balance OR available):**

*Available balance change:*
```json
{
  "action": "push",
  "ch": "accounts.update#1",
  "data": {
    "currency": "btc",
    "accountId": 33385,
    "available": "2028.699426619837209087",
    "changeType": "order.match",
    "accountType": "trade",
    "seqNum": "86872993928",
    "changeTime": 1574393385167
  }
}
```

*Balance change:*
```json
{
  "action": "push",
  "ch": "accounts.update#1",
  "data": {
    "currency": "btc",
    "accountId": 33385,
    "balance": "2065.100267619837209301",
    "changeType": "order.match",
    "accountType": "trade",
    "seqNum": "86872993928",
    "changeTime": 1574393385122
  }
}
```

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| action | string | Always `push` for updates |
| ch | string | Channel name with mode |
| data.currency | string | Currency code |
| data.accountId | long | Account ID |
| data.balance | string | Account balance (only present when balance changes) |
| data.available | string | Available balance (only present when available changes) |
| data.changeType | string | Change type (see below) |
| data.accountType | string | Account type: `trade`, `loan`, `interest` |
| data.changeTime | long | Change timestamp (milliseconds) |
| data.seqNum | long | Serial number of account change |

**Change Types:**
- `order.place`
- `order.match`
- `order.refund`
- `order.cancel`
- `order.fee-refund`
- `margin.transfer`
- `margin.loan`
- `margin.interest`
- `margin.repay`
- `deposit`
- `withdraw`
- `other`

**Note:** Maker rebates may be paid in batch mode for multiple trades.

### Unsubscribe Example
```json
{
  "action": "unsub",
  "ch": "accounts.update"
}
```

### Error Handling

**System Exception on First Push:**
```json
{
  "action": "sub",
  "code": 500,
  "ch": "accounts.update#2",
  "message": "系统异常:"
}
```

If you receive this error message on the first push, account update information will not be pushed anymore. You must re-subscribe to the accounts update topic.

---

## Orders Updates Channel

### Topic Format
```
orders#${symbol}
```

### Notes
- `${symbol}` may be a specific symbol (e.g., `btcusdt`) or the wildcard `*` to subscribe to all symbols
- Signature verification: No
- Interface permission: Read

### Subscription Example
```json
{
  "action": "sub",
  "ch": "orders#*"
}
```

### Event Types
- `creation` (order submitted)
- `trade` (order matched)
- `cancellation` (order canceled)
- `trigger` (conditional order trigger failure)
- `deletion` (conditional order canceled before trigger)

Fields vary by event type as per official docs.

---

## Trade Details and Cancellation After Clearing

### Topic Format
```
trade.clearing#${symbol}#${mode}
```

### Parameters
- `symbol`: Trading symbol (wildcard `*` allowed)
- `mode`: 0 – trade events only; 1 – trade and cancellation events; default 0

### Subscription Example
```json
{
  "action": "sub",
  "ch": "trade.clearing#btcusdt#0"
}
```

### Notes
- Updates are tick-by-tick for trades
- Cancellation updates may precede trade updates for IOC-like orders

---

## Subscription Rules

### Multiple Subscriptions

**Allowed:**
- Subscribe to multiple symbols in a single request
- Unsubscribe from the same set of symbols subscribed together

**Not Allowed:**
- Unsubscribe from symbols that were not part of the original subscription group

### Examples

✅ **Valid:**
```json
// Subscribe
{
  "sub": ["market.btcusdt.trade.detail", "market.ethusdt.trade.detail"]
}

// Unsubscribe (same set)
{
  "unsub": ["market.btcusdt.trade.detail", "market.ethusdt.trade.detail"]
}
```

❌ **Invalid:**
```json
// Subscribe to A, B, C
{
  "sub": ["market.btcusdt.trade.detail", "market.ethusdt.trade.detail", "market.htxusdt.trade.detail"]
}

// Unsubscribe to D (not in original subscription)
{
  "unsub": ["market.bnbusdt.trade.detail"]
}
```

---

## Best Practices

### Connection Management

1. **Use AWS endpoints** (`api-aws.huobi.pro`) if your server is on AWS for lower latency
2. **Implement automatic reconnection** logic for connection failures
3. **Monitor ping/pong** and reconnect if heartbeat fails
4. **Don't subscribe to too many topics** in a single connection to avoid network latency

### Public WebSocket

1. **Decompress all data** using GZIP before parsing
2. **Respond to pings** within 5 seconds to maintain connection
3. **Use `version` field** for orderbook deduplication
4. **Use `tradeId`** (not deprecated `id`) for trade deduplication
5. **Cache data locally** for better performance

### Private WebSocket

1. **Authenticate immediately** after connection establishment
2. **Monitor authentication response** before subscribing to channels
3. **Handle initial snapshot** separately from real-time updates
4. **Use `seqNum`** to track account change sequence
5. **Subscribe to both** `orders.$symbol` and `accounts.update#${mode}` for complete account state
6. **Respect rate limits:** 50 requests/second per connection, 10 connections per API key
7. **Re-subscribe on system exception** if first push returns error

### Data Integrity

1. **For orderbook:** Use `version` field to discard out-of-order updates
2. **For trades:** Use `tradeId` for deduplication
3. **For account updates:** Use `seqNum` to track sequential changes
4. **Timestamp validation:** Ensure your system clock is synchronized (NTP)
5. **Signature validity:** Remember signatures expire after 5 minutes

---

## Error Codes

Common error scenarios and their solutions:

| Scenario | Error | Solution |
|----------|-------|----------|
| Too many requests | `too many requests` | Reduce request rate, respect rate limits |
| Too many connections | `too many connections` | Close unused connections (max 10 per API key) |
| Invalid signature | Authentication failed | Verify signature generation, check timestamp validity |
| System exception | `code: 500` | Re-subscribe to the channel |
| Connection timeout | No ping received | Implement reconnection logic |

---

## Quick Reference

### Public Market Data (wss://api.huobi.pro/ws)

| Feature | Details |
|---------|---------|
| Authentication | Not required |
| Data Format | GZIP compressed |
| Heartbeat | Every 5 seconds (ping/pong) |
| Main Channels | market.{symbol}.depth.{type}, market.{symbol}.trade.detail |

### Private Account & Order (wss://api.huobi.pro/ws/v2)

| Feature | Details |
|---------|---------|
| Authentication | Required (signature v2.1) |
| Data Format | Plain JSON (not compressed) |
| Heartbeat | Every 20 seconds (action: ping/pong) |
| Rate Limit | 50 req/s per connection, 10 connections per API key |
| Main Channels | accounts.update#{mode} |

### Signature Parameters

| Field | Value |
|-------|-------|
| Method | GET |
| Path | /ws/v2 |
| Host | api.huobi.pro |
| Signature Version | 2.1 |
| Signature Method | HmacSHA256 or Ed25519 |
| Timestamp Format | YYYY-MM-DDTHH:mm:ss (UTC) |
| Validity | 5 minutes |

---

## Additional Resources

- Official API Documentation: https://www.htx.com/en-in/opend/newApiPages/
- API Key Management: https://www.htx.com/apikey/
- Support: https://www.htx.com/support/en-in/

---

**Document Version:** 1.0  
**Last Updated:** November 2025  
**Based on:** HTX Official API Documentation
