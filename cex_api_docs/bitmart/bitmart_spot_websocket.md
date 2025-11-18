# BitMart Spot WebSocket API Documentation

> **Source:** Official BitMart API Documentation  
> **URL:** https://developer-pro.bitmart.com/en/spot/#websocket-subscription  
> **Last Updated:** Based on documentation as of 2025

---

## Table of Contents

- [Overview](#overview)
  - [Server URL](#server-url)
- [Format](#format)
  - [Message Format](#message-format)
  - [Successful Response Format](#successful-response-format)
  - [Failed Response Format](#failed-response-format)
- [Connection Management](#connection-management)
  - [Stay Connected](#stay-connected)
  - [Connection Limits](#connection-limits)
  - [Lifeless Connection](#lifeless-connection)
- [Data Compression](#data-compression)
- [Subscribe & Unsubscribe](#subscribe--unsubscribe)
- [Public Channels](#public-channels)
  - [Ticker Channel](#public-ticker-channel)
  - [KLine Channel](#public-kline-channel)
  - [Depth-All Channel](#public-depth-all-channel)
  - [Depth-Increase Channel](#public-depth-increase-channel)
  - [Trade Channel](#public-trade-channel)
- [Private Channels](#private-channels)
  - [Login](#private-login)
  - [Order Progress](#private-order-progress)
  - [Balance Change](#private-balance-change)
- [Error Codes](#error-codes)
- [Code Examples](#code-examples)
  - [Python Examples](#python-examples)
  - [Other Languages](#other-languages)

---

## Overview

### Server URL

- **Public Channel:** `wss://ws-manager-compress.bitmart.com/api?protocol=1.1`
- **Private Channel:** `wss://ws-manager-compress.bitmart.com/user?protocol=1.1`

> **Important Notice (2024-10-01):** The KeepAlive mechanism of spot websocket now supports clients sending ping frames and ping text to maintain the connection. In the future we will only support ping text, not ping frames.

---

## Format

### Message Format

The message format sent by the client to the BitMart server:

```json
{"op":"<operation>", "args":["<topic1>","<topic2>"]}
```

**Parameters:**
- `operation` — Request action. Values:
  - `subscribe` — Subscribe channel
  - `unsubscribe` — Unsubscribe channel
  - `login` — Account login
- `args` — Request parameter. Value: channel array or parameters required for login
- `topic` — Channel topic, composed of `<channel>:<filter>`
  - `channel` is composed of `business/name`
  - `filter` filters data, refer to each channel description for details

**Examples:**

```json
// Example 1: Subscribe to ticker
{"op": "subscribe", "args": ["spot/ticker:BTC_USDT"]}

// Example 2: Login request
{"op": "login", "args": ["80618e45710812162b04892c7ee5ead4a3cc3e56", "1589267764859", "3ceeb7e1b8cb165a975e28a2e2dfaca4d30b358873c0351c1a071d8c83314556"]}
```

### Successful Response Format

If the returned field **does not contain** `errorCode`, it indicates success.

```json
// When op=login:
{"event":"<operation>"}

// When op=unsubscribe:
{"event":"<operation>","topic":"<topic>"}

// When op=subscribe:
{"table":"<topic1>","data":[{"<value1>","<value2>"}]}
{"table":"<topic2>","data":[{"<value1>","<value2>"}]}
```

**Examples:**
- `{"event":"login"}` — Successful login
- `{"topic":"spot/ticker:BTC_USDT","event":"subscribe"}` — Successful subscription
- `{"event":"unsubscribe","topic":"spot/ticker:BTC_USDT"}` — Successful unsubscription
- `{"table":"spot/ticker:BTC_USDT","data":[{...}]}` — Data push

### Failed Response Format

If the returned field **contains** `errorCode`, it indicates failure.

```json
{"event":"<operation>","errorMessage":"<error_message>","errorCode":"<error_code>"}
```

**Examples:**
- `{"event":"login","errorCode":"91002","errorMessage":"API KEY not found"}` — Login failed
- `{"event":"subscribe","errorCode":"90004","errorMessage":"Invalid channel param"}` — Invalid channel

---

## Connection Management

> **Note:** If there is a network problem, the connection will be automatically disconnected. Please set up a reconnection mechanism.

### Stay Connected

WebSocket uses the **Ping/Pong** mechanism to maintain the connection.

**Implementation:**
1. After each message is received, set a timer for N seconds (N < 20)
2. If timer triggers (no new message within N seconds), send text **"ping"** (Ping frames are NOT supported)
3. Expect text **"pong"** as response. If not received within N seconds, issue an error or reconnect
4. The server does not actively disconnect when message interaction is continuous

**Ping text example (Java pseudocode):**
```java
ws.send(new TextWebSocketFrame("ping"));
```

> **Important:** If no data is returned after connecting, the link will be **automatically disconnected after 20s**.

### Connection Limits

#### Public Channel Limits

- **Connections:** Max 20 connections per IP
- **Channels:** Max 115 channels per connection
- **Rate Limits:**
  - **Connection initiation:** Max 30 requests within 1 minute
  - **Once connected:** Max 100 subscription messages within 10 seconds (includes PING text, JSON messages)
  - **Single subscription:** Max 20 message arrays

#### Private Channel Limits

- **Connections:** Max 10 connections per IP
- **Channels:** Max 100 channels per connection
- **Rate Limits:**
  - **Connection initiation:** Max 30 requests within 1 minute
  - **Once connected:** Max 100 subscription messages within 10 seconds
  - **Single subscription:** Max 20 message arrays

> **Warning:** If you exceed limits, the connection will be disconnected. IPs repeatedly disconnected will be **blocked**.

#### How to Subscribe to More Than 1000 Public Channels?

Create **20 connectors** and **1 receiving function**. Each connector subscribes to **100 channels** → you can subscribe to **2000 channels**.

**Important Notes:**
- Fewer channels → faster response. Subscribe **only** to what you need
- Private and Public channel limits are calculated **separately** (10 private + 20 public connections allowed simultaneously)

### Lifeless Connection

Connections that **do not send task subscription data within 5 minutes** are considered lifeless and the server will close the connection.

---

## Data Compression

When market data is returned **after subscription**, the remote service **may compress** the data.

**Two return formats:**
- **Binary format:** Data is compressed, client needs to decompress
- **Text format:** Data is not compressed

### Compression Method

**Algorithm:** zlib (DEFLATE)  
**Official link:** http://zlib.net/

### Decompression Examples

#### Python
```python
import zlib

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated.decode('UTF-8')
```

#### Node.js
```javascript
const zlib = require('zlib');
zlib.inflateRawSync(data);
```

#### Golang
```go
import (
    "bytes"
    "compress/flate"
    "io/ioutil"
)

func zipDecode(in []byte) ([]byte, error) {
    reader := flate.NewReader(bytes.NewReader(in))
    defer reader.Close()
    return ioutil.ReadAll(reader)
}
// Usage: string(zipDecode(data))
```

#### PHP
```php
// https://php.net/manual/en/function.gzinflate.php
gzinflate($data);
```

#### Java
```java
import java.util.zip.*;

public class StringCompressUtil {
    private static String uncompress(ByteBuf buf) {
        try {
            byte[] temp = new byte[buf.readableBytes()];
            ByteBufInputStream bis = new ByteBufInputStream(buf);
            bis.read(temp);
            bis.close();
            Inflater decompresser = new Inflater(true);
            decompresser.setInput(temp, 0, temp.length);
            StringBuilder sb = new StringBuilder();
            byte[] result = new byte[1024];
            while (!decompresser.finished()) {
                int resultLength = decompresser.inflate(result);
                sb.append(new String(result, 0, resultLength, "UTF-8"));
            }
            decompresser.end();
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
```

---

## Subscribe & Unsubscribe

Users can subscribe to **one or more** channels. The total length of multiple channels **cannot exceed 4096 bytes**.

### Subscribe

**Request:**
```json
{"op": "subscribe", "args": ["<topic>"]}
```

**Example — Single channel:**
```json
{"op": "subscribe", "args": ["spot/ticker:BTC_USDT"]}
```

**Example — Multiple channels:**
```json
{"op": "subscribe", "args": ["spot/ticker:BTC_USDT", "spot/depth5:BTC_USDT"]}
```

**Success Response:**
```json
{"event": "subscribe", "topic": "spot/ticker:BTC_USDT"}
```

**Data Push:**
```json
{"table":"spot/ticker:BTC_USDT","data":[{...}]}
```

### Unsubscribe

**Request:**
```json
{"op": "unsubscribe", "args": ["<topic>"]}
```

**Example:**
```json
{"op": "unsubscribe", "args": ["spot/ticker:BTC_USDT", "spot/ticker:ETH_USDT"]}
```

**Success Response:**
```json
{"event":"unsubscribe","topic":"spot/ticker:BTC_USDT"}
{"event":"unsubscribe","topic":"spot/ticker:ETH_USDT"}
```

---

## Public Channels

### 【Public】Ticker Channel

Get the latest price, bid price, ask price and 24-hour trading volume.

**Pushing Rules:**
1. No user login required
2. After subscribing, current data returned directly, then pushes on change
3. Push frequency: Fastest is 500ms once

**Subscribe:**
```json
{"op": "subscribe", "args": ["spot/ticker:BTC_USDT"]}
```

**Success Response:**
```json
{"event": "subscribe", "topic": "spot/ticker:BTC_USDT"}
```

**Push Data:**
```json
{
  "data": [
    {
      "ask_px": "36000",
      "ask_sz": "1.021",
      "base_volume_24h": "2.02000",
      "bid_px": "35000",
      "bid_sz": "11",
      "fluctuation": "-0.0001",
      "high_24h": "35003.04",
      "last_price": "35000.00",
      "low_24h": "32891.01",
      "ms_t": 1709024305000,
      "open_24h": "35003.50",
      "quote_volume_24h": "19991.4832",
      "symbol": "BTC_USDT",
      "url": "https://www.bitmart.com/trade/en-US?symbol=BTC_USDT"
    }
  ],
  "table": "spot/ticker"
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Trading pair, e.g., `BTC_USDT` |
| last_price | String | Latest price |
| quote_volume_24h | String | Trading volume in quote currency in 24hr |
| base_volume_24h | String | Trading volume in base currency in 24hr |
| high_24h | String | Highest price in 24hr |
| low_24h | String | Lowest price in 24hr |
| open_24h | String | Open price before 24hr |
| ms_t | Long | Latest transaction time (milliseconds) |
| fluctuation | String | Price change in 24hr |
| bid_px | String | Best bid price |
| bid_sz | String | Best bid quantity |
| ask_px | String | Best ask price |
| ask_sz | String | Best ask quantity |

### 【Public】KLine Channel

Get the spot K-line data.

**Pushing Rules:**
1. No user login required
2. After subscribing, current data returned directly, then pushes on change
3. Push frequency: Fastest is 500ms once

**Available Intervals:**

| Channel Name | Description |
|-------------|-------------|
| spot/kline1m | 1-minute KLine |
| spot/kline3m | 3-minute KLine |
| spot/kline5m | 5-minute KLine |
| spot/kline15m | 15-minute KLine |
| spot/kline30m | 30-minute KLine |
| spot/kline45m | 45-minute KLine |
| spot/kline1H | 1-hour KLine |
| spot/kline2H | 2-hour KLine |
| spot/kline4H | 4-hour KLine |
| spot/kline1D | 1-day KLine |
| spot/kline1W | 1-week KLine |
| spot/kline1M | 1-month KLine |

**Subscribe:**
```json
{"op": "subscribe", "args": ["spot/kline1m:BTC_USDT"]}
```

**Success Response:**
```json
{"topic": "spot/kline1m:BTC_USDT", "event": "subscribe"}
```

**Push Data:**
```json
{
  "data": [
    {
      "candle": [
        1709025360,
        "162.01",
        "162.02",
        "162.03",
        "162.04",
        "336.452694"
      ],
      "symbol": "BTC_USDT"
    }
  ],
  "table": "spot/kline1m"
}
```

**Candle Array Format:**
`[timestamp, open, high, low, close, volume]`
- Timestamp (in seconds)
- Opening price
- Highest price
- Lowest price
- Closing price
- Trading volume

### 【Public】Depth-All Channel

Return depth data, each push is the full data.

**Pushing Rules:**
1. No user login required
2. After subscribing, current data returned directly, then pushes on change
3. Push frequency: Fastest is 500ms once

**Available Depth Levels:**

| Channel Name | Description |
|-------------|-------------|
| spot/depth5 | 5 Level Depth |
| spot/depth20 | 20 Level Depth |
| spot/depth50 | 50 Level Depth |
| spot/depth100 | 100 Level Depth |

**Subscribe:**
```json
{"op": "subscribe", "args": ["spot/depth5:BTC_USDT"]}
```

**Success Response:**
```json
{"topic": "spot/depth5:BTC_USDT", "event": "subscribe"}
```

**Push Data:**
```json
{
  "table": "spot/depth5",
  "data": [
    {
      "asks": [
        ["161.96", "7.37567"],
        ["161.97", "5.23456"]
      ],
      "bids": [
        ["161.94", "4.552355"],
        ["161.93", "3.12345"]
      ],
      "symbol": "ETH_USDT",
      "ms_t": 1542337219120
    }
  ]
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Trading pair |
| asks | List | Ask depth [price, quantity] |
| bids | List | Bid depth [price, quantity] |
| ms_t | Long | Timestamp (milliseconds) |

### 【Public】Depth-Increase Channel

Return incremental depth data, supports creating a local full depth cache.

**Pushing Rules:**
1. No user login required
2. After subscribing, current data returned directly, then pushes incremental changes
3. Push frequency: Fastest is 100ms once

**Subscribe for incremental updates:**
```json
{"op": "subscribe", "args": ["spot/depth/increase100:BTC_USDT"]}
```

**Request full snapshot:**
```json
{"op": "request", "args": ["spot/depth/increase100:BTC_USDT"]}
```

**Full Snapshot Response:**
```json
{
  "data": [{
    "asks": [
      ["23200", "0.69959"],
      ["28000.00", "0.20000"]
    ],
    "bids": [
      ["23105", "1.80114"]
    ],
    "ms_t": 1698292343610,
    "symbol": "BTC_USDT",
    "type": "snapshot",
    "version": 4
  }],
  "table": "spot/depth/increase100"
}
```

**Incremental Update:**
```json
{
  "data": [{
    "asks": [
      ["23200", "0.59959"]
    ],
    "bids": [],
    "ms_t": 1698292358292,
    "symbol": "BTC_USDT",
    "type": "update",
    "version": 5
  }],
  "table": "spot/depth/increase100"
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Trading pair |
| asks | List | Ask depth changes |
| bids | List | Bid depth changes |
| ms_t | Long | Timestamp (milliseconds) |
| version | Long | Data version |
| type | String | `snapshot` or `update` |

#### How to Maintain Local OrderBook:

1. Subscribe: `{"op": "subscribe", "args": ["spot/depth/increase100:<symbol>"]}`
2. Receive two message types: `type=snapshot` (full) and `type=update` (incremental)
3. On `snapshot`: Update local cache completely
4. On `update`:
   - If `new version <= local version`: Discard
   - If `new version == local version + 1`: Update local cache
   - If `new version > local version + 1`: Request new snapshot
5. Update rules:
   - **New price:** Add to cache
   - **Existing price with quantity > 0:** Update quantity
   - **Existing price with quantity = 0:** Remove from cache
6. Request snapshot if version gap detected: `{"op": "request", "args": ["spot/depth/increase100:<symbol>"]}`

**Abnormal Situations:**
- Empty messages (`'asks': [], 'bids': []`) indicate connection is normal
- Price tiers outside snapshot limit won't appear in incremental updates

### 【Public】Trade Channel

Get the latest real-time transaction data.

**Pushing Rules:**
1. No user login required
2. After successful subscription, incremental trade messages pushed (Taker trades)
3. Push frequency: Push on change

**Subscribe:**
```json
{"op": "subscribe", "args": ["spot/trade:BTC_USDT"]}
```

**Success Response:**
```json
{"event": "subscribe", "topic": "spot/trade:BTC_USDT"}
```

**Push Data:**
```json
{
  "table": "spot/trade",
  "data": [
    {
      "symbol": "ETH_USDT",
      "price": "162.12",
      "side": "buy",
      "size": "11.085",
      "s_t": 1542337219,
      "ms_t": 1542337219120
    }
  ]
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Trading pair |
| side | String | Taker side (`buy` or `sell`) |
| price | String | Trade price |
| size | String | Trade quantity |
| s_t | Long | Timestamp (seconds) - deprecated, use ms_t |
| ms_t | Long | Timestamp (milliseconds) |

---

## Private Channels

### 【Private】Login

Authentication is required before subscribing to private channels.

**Request Format:**
```json
{"op":"login","args":["<API_KEY>", "<timestamp>", "<sign>"]}
```

**Parameters:**
- `API_KEY`: Your API key
- `timestamp`: Current timestamp in milliseconds (expires after 60 seconds)
- `sign`: Signature = HmacSHA256(timestamp + "#" + api_memo + "#" + "bitmart.WebSocket", secret)

**Example:**

Given:
- timestamp = `1589267764859`
- API_KEY = `80618e45710812162b04892c7ee5ead4a3cc3e56`
- API_SECRET = `6c6c98544461bbe71db2bca4c6d7fd0021e0ba9efc215f9c6ad41852df9d9df9`
- API_MEMO = `test001`

**JavaScript signature:**
```javascript
sign = CryptoJS.HmacSHA256(
    "1589267764859#test001#bitmart.WebSocket",
    "6c6c98544461bbe71db2bca4c6d7fd0021e0ba9efc215f9c6ad41852df9d9df9"
)
// Result: 3ceeb7e1b8cb165a975e28a2e2dfaca4d30b358873c0351c1a071d8c83314556
```

**Shell signature:**
```bash
echo -n '1589267764859#test001#bitmart.WebSocket' | openssl dgst -sha256 -hmac "6c6c98544461bbe71db2bca4c6d7fd0021e0ba9efc215f9c6ad41852df9d9df9"
# Result: 3ceeb7e1b8cb165a975e28a2e2dfaca4d30b358873c0351c1a071d8c83314556
```

**Login Request:**
```json
{
  "op": "login",
  "args": [
    "80618e45710812162b04892c7ee5ead4a3cc3e56",
    "1589267764859",
    "3ceeb7e1b8cb165a975e28a2e2dfaca4d30b358873c0351c1a071d8c83314556"
  ]
}
```

**Success Response:**
```json
{"event":"login"}
```

> **Note:** If login fails, the connection will be automatically disconnected.

### 【Private】Order Progress

Subscribe to order execution progress for single or all trading pairs.

**Pushing Rules:**
1. User login required
2. Pushes qualified orders: Successfully placed, Partially filled, Fully filled, Canceled
3. Push frequency: Push on change

**Subscribe — Single Symbol:**
```json
{"op": "subscribe", "args": ["spot/user/order:BTC_USDT"]}
```

**Subscribe — All Symbols:**
```json
{"op": "subscribe", "args": ["spot/user/orders:ALL_SYMBOLS"]}
```

> **Note:** Channel names differ for single (`order`) vs all (`orders`) trading pairs.

**Success Response — Single:**
```json
{"event": "subscribe", "topic": "spot/user/order:BTC_USDT"}
```

**Success Response — All:**
```json
{"event": "subscribe", "topic": "spot/user/orders:ALL_SYMBOLS"}
```

**Push Data:**
```json
{
  "data": [
    {
      "symbol": "BTC_USDT",
      "side": "buy",
      "type": "market",
      "notional": "",
      "size": "1.0000000000",
      "ms_t": "1609926028000",
      "price": "46100.0000000000",
      "filled_notional": "46100.0000000000",
      "filled_size": "1.0000000000",
      "margin_trading": "0",
      "state": "4",
      "order_id": "2147857398",
      "order_type": "0",
      "last_fill_time": "1609926039226",
      "last_fill_price": "46100.00000",
      "last_fill_count": "1.00000",
      "exec_type": "M",
      "detail_id": "256348632",
      "client_order_id": "order4872191",
      "create_time": "1609926028000",
      "update_time": "1609926044000",
      "order_mode": "spot",
      "entrust_type": "NORMAL",
      "order_state": "partially_filled",
      "dealFee": "10.00",
      "deal_fee_coin_name": "BMX"
    }
  ],
  "table": "spot/user/order"
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| symbol | String | Trading pair |
| order_id | String | Order ID |
| price | String | Order price |
| size | String | Order quantity |
| notional | String | Purchase amount (market buy), else empty |
| side | String | `buy` or `sell` |
| type | String | `limit` or `market` |
| ms_t | String | Order create timestamp (ms) |
| filled_size | String | Filled size (base currency) |
| filled_notional | String | Filled notional (quote currency) |
| margin_trading | String | `0`=Spot (deprecated, use `order_mode`) |
| order_type | String | Order type (deprecated, use `entrust_type`)<br>`0`=Regular, `1`=Post only, `2`=FOK, `3`=IOC |
| state | String | Order state (deprecated, use `order_state`)<br>`4`=Pending, `5`=Partially filled, `6`=Fully filled, `8`=Canceled, `12`=Partially canceled |
| last_fill_price | String | Latest fill price (`0` if none) |
| last_fill_count | String | Latest fill quantity (`0` if none) |
| last_fill_time | String | Latest fill time (ms, `0` if none) |
| exec_type | String | `M`=Maker, `T`=Taker |
| detail_id | String | Trade ID |
| client_order_id | String | Client-defined OrderId |
| create_time | String | Create timestamp (ms) |
| update_time | String | Update timestamp (ms) |
| order_mode | String | `spot` or `iso_margin` |
| entrust_type | String | `NORMAL`, `LIMIT_MAKER`, `IOC` |
| order_state | String | `new`, `partially_filled`, `filled`, `canceled`, `partially_canceled` |
| dealFee | String | Fee amount |
| deal_fee_coin_name | String | Fee currency |

> **Notice:** This data is displayed after decompression. See [Data Compression](#data-compression).

### 【Private】Balance Change

Balance change push for all currencies.

**Pushing Rules:**
1. User login required
2. Pushes qualified balance changes: recharge, withdrawal, transfer, transaction, BMX fee deduction
3. Push frequency: Push on change

**Subscribe:**
```json
{"op": "subscribe", "args": ["spot/user/balance:BALANCE_UPDATE"]}
```

**Success Response:**
```json
{"event": "subscribe", "topic": "spot/user/balance:BALANCE_UPDATE"}
```

**Push Data:**
```json
{
  "data": [
    {
      "event_type": "TRANSACTION_COMPLETED",
      "event_time": "1693364237000",
      "balance_details": [
        {
          "ccy": "BTC",
          "av_bal": "123.22",
          "fz_bal": "12.56"
        }
      ]
    }
  ],
  "table": "spot/user/balance"
}
```

**Field Description:**

| Field | Type | Description |
|-------|------|-------------|
| event_type | String | Change type:<br>`TRANSACTION_COMPLETED`=Trade<br>`ACCOUNT_RECHARGE`=Recharge<br>`ACCOUNT_WITHDRAWAL`=Withdraw<br>`ACCOUNT_TRANSFER`=Transfer<br>`BMX_DEDUCTION`=BMX fee deduction |
| event_time | String | Timestamp (ms) |
| balance_details | Array | Balance change details |
| > ccy | String | Currency |
| > av_bal | String | Available balance after change |
| > fz_bal | String | Frozen balance after change |

> **Notice:** This data is displayed after decompression. See [Data Compression](#data-compression).

---

## Error Codes

### WebSocket Error Codes

**Error Response Format:**
```json
{"event":"<operation>", "errorMessage":"<message>", "errorCode":"<code>"}
```

**Error Code List:**

| Error Message | Error Code | Description |
|--------------|------------|-------------|
| Invalid message format | 90001 | Message format is incorrect |
| Invalid op param | 90002 | Invalid operation parameter |
| Invalid args param | 90003 | Invalid arguments |
| Invalid channel param | 90004 | Channel doesn't exist |
| Topic quantity in single subscription exceeds limit | 90005 | Too many topics in one subscription |
| Subscribed total topic quantity exceeds limit | 90006 | Total subscription limit exceeded |
| Subscribed message frequency exceeds limit | 90007 | Rate limit exceeded |
| Duplicate subscription | 90008 | Already subscribed |
| Invalid subscription | 90009 | Subscription not valid |
| API KEY is empty | 91001 | Missing API key |
| API KEY not found | 91002 | API key doesn't exist |
| API KEY has frozen | 91003 | API key is frozen |
| API KEY over expire time | 91004 | API key expired |
| Already logged in | 91005 | Already authenticated |
| User not logged in | 91006 | Login required |
| Param sign is empty | 91010 | Missing signature |
| Param sign is wrong | 91011 | Invalid signature |
| Param timestamp is empty | 91021 | Missing timestamp |
| Param timestamp range | 91022 | Timestamp outside valid range |
| Param timestamp invalid format | 91023 | Invalid timestamp format |
| Invalid symbol param | 92001 | Invalid trading pair |
| Frequently reestablishing connections | 94001 | Too many reconnections |
| Connection limit exceeded | 94002 | Too many connections from IP |
| Internal system error | 95000 | Server error |

---

## Code Examples

### Python Examples

#### Complete WebSocket Client Example

```python
import json
import time
import threading
import hmac
import hashlib
import zlib
from websocket import WebSocketApp

class BitmartWebSocket:
    PUBLIC_WS = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
    PRIVATE_WS = "wss://ws-manager-compress.bitmart.com/user?protocol=1.1"
    
    def __init__(self, api_key=None, api_secret=None, api_memo=None, is_private=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_memo = api_memo
        self.ws_url = self.PRIVATE_WS if is_private else self.PUBLIC_WS
        self.ws = None
        
    def inflate(self, data):
        """Decompress binary data"""
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('UTF-8')
    
    def sign_message(self, timestamp):
        """Create signature for login"""
        message = f"{timestamp}#{self.api_memo}#bitmart.WebSocket"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def on_message(self, ws, message):
        """Handle incoming messages"""
        if isinstance(message, (bytes, bytearray)):
            # Binary message - decompress
            message = self.inflate(message)
            
        print(f"Received: {message}")
        
        # Parse JSON
        try:
            data = json.loads(message)
            
            # Handle login success
            if data.get('event') == 'login':
                print("Login successful!")
                self.after_login(ws)
                
        except json.JSONDecodeError:
            if message == 'pong':
                print("Received pong")
    
    def on_error(self, ws, error):
        print(f"Error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print("### Connection closed ###")
    
    def on_open(self, ws):
        print("### Connection opened ###")
        
        if self.api_key:
            # Private channel - login first
            timestamp = int(time.time() * 1000)
            sign = self.sign_message(timestamp)
            login_msg = {
                "op": "login",
                "args": [self.api_key, str(timestamp), sign]
            }
            ws.send(json.dumps(login_msg))
            print(f"Login sent: {login_msg}")
        else:
            # Public channel - subscribe directly
            self.subscribe_public(ws)
        
        # Start ping thread
        self.start_ping_thread(ws)
    
    def after_login(self, ws):
        """Subscribe to private channels after login"""
        # Subscribe to order updates for all symbols
        sub_msg = {"op": "subscribe", "args": ["spot/user/orders:ALL_SYMBOLS"]}
        ws.send(json.dumps(sub_msg))
        print(f"Subscribed: {sub_msg}")
        
        # Subscribe to balance changes
        sub_msg = {"op": "subscribe", "args": ["spot/user/balance:BALANCE_UPDATE"]}
        ws.send(json.dumps(sub_msg))
        print(f"Subscribed: {sub_msg}")
    
    def subscribe_public(self, ws):
        """Subscribe to public channels"""
        channels = [
            "spot/ticker:BTC_USDT",
            "spot/kline1m:BTC_USDT",
            "spot/depth5:BTC_USDT",
            "spot/trade:BTC_USDT"
        ]
        
        sub_msg = {"op": "subscribe", "args": channels}
        ws.send(json.dumps(sub_msg))
        print(f"Subscribed: {sub_msg}")
    
    def start_ping_thread(self, ws):
        """Start ping thread to keep connection alive"""
        def ping_loop():
            while True:
                time.sleep(15)  # N < 20 seconds
                try:
                    ws.send("ping")  # Send text ping
                    print("Sent: ping")
                except Exception as e:
                    print(f"Ping error: {e}")
                    break
        
        ping_thread = threading.Thread(target=ping_loop, daemon=True)
        ping_thread.start()
    
    def run(self):
        """Start WebSocket connection"""
        self.ws = WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.run_forever()


# Example 1: Public Channel
if __name__ == "__main__":
    # Public WebSocket
    public_ws = BitmartWebSocket()
    # public_ws.run()
    
    # Private WebSocket
    API_KEY = "your_api_key"
    API_SECRET = "your_api_secret"
    API_MEMO = "your_api_memo"
    
    private_ws = BitmartWebSocket(
        api_key=API_KEY,
        api_secret=API_SECRET,
        api_memo=API_MEMO,
        is_private=True
    )
    # private_ws.run()
```

#### Incremental Depth Management Example

```python
import json
import zlib
from websocket import WebSocketApp

class DepthManager:
    def __init__(self, symbol="BTC_USDT"):
        self.symbol = symbol
        self.local_depth = {
            'asks': {},  # price -> quantity
            'bids': {},  # price -> quantity
            'version': 0
        }
        self.ws_url = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
        
    def inflate(self, data):
        """Decompress binary data"""
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('UTF-8')
    
    def update_depth(self, data):
        """Update local depth based on message type"""
        msg_type = data.get('type')
        version = data.get('version', 0)
        
        if msg_type == 'snapshot':
            # Full snapshot - replace everything
            self.local_depth = {
                'asks': {item[0]: item[1] for item in data.get('asks', [])},
                'bids': {item[0]: item[1] for item in data.get('bids', [])},
                'version': version
            }
            print(f"Depth snapshot loaded, version: {version}")
            
        elif msg_type == 'update':
            # Check version
            if version <= self.local_depth['version']:
                # Discard old version
                return
            elif version == self.local_depth['version'] + 1:
                # Apply updates
                for ask in data.get('asks', []):
                    price, quantity = ask[0], ask[1]
                    if float(quantity) == 0:
                        # Remove from order book
                        self.local_depth['asks'].pop(price, None)
                    else:
                        # Update or add
                        self.local_depth['asks'][price] = quantity
                
                for bid in data.get('bids', []):
                    price, quantity = bid[0], bid[1]
                    if float(quantity) == 0:
                        # Remove from order book
                        self.local_depth['bids'].pop(price, None)
                    else:
                        # Update or add
                        self.local_depth['bids'][price] = quantity
                
                self.local_depth['version'] = version
                print(f"Depth updated to version: {version}")
                
            else:
                # Version gap - request new snapshot
                print(f"Version gap detected: local={self.local_depth['version']}, new={version}")
                self.request_snapshot()
    
    def request_snapshot(self):
        """Request full depth snapshot"""
        request = {
            "op": "request",
            "args": [f"spot/depth/increase100:{self.symbol}"]
        }
        self.ws.send(json.dumps(request))
        print("Requested new snapshot")
    
    def get_top_levels(self, n=5):
        """Get top N price levels"""
        sorted_asks = sorted(self.local_depth['asks'].items(), key=lambda x: float(x[0]))
        sorted_bids = sorted(self.local_depth['bids'].items(), key=lambda x: float(x[0]), reverse=True)
        
        return {
            'asks': sorted_asks[:n],
            'bids': sorted_bids[:n],
            'version': self.local_depth['version']
        }
    
    def on_message(self, ws, message):
        if isinstance(message, (bytes, bytearray)):
            message = self.inflate(message)
        
        try:
            data = json.loads(message)
            
            if data.get('table') == 'spot/depth/increase100':
                for item in data.get('data', []):
                    if item.get('symbol') == self.symbol:
                        self.update_depth(item)
                        
                        # Print top 5 levels
                        top_levels = self.get_top_levels(5)
                        print(f"Top 5 asks: {top_levels['asks']}")
                        print(f"Top 5 bids: {top_levels['bids']}")
                        
        except json.JSONDecodeError:
            pass
    
    def on_open(self, ws):
        self.ws = ws
        # Subscribe to incremental depth
        subscribe = {
            "op": "subscribe",
            "args": [f"spot/depth/increase100:{self.symbol}"]
        }
        ws.send(json.dumps(subscribe))
        print(f"Subscribed to incremental depth for {self.symbol}")
        
        # Keep alive
        def ping_loop():
            import time
            import threading
            while True:
                time.sleep(15)
                try:
                    ws.send("ping")
                except:
                    break
        
        threading.Thread(target=ping_loop, daemon=True).start()
    
    def run(self):
        self.ws = WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message
        )
        self.ws.run_forever()


# Usage
if __name__ == "__main__":
    manager = DepthManager("BTC_USDT")
    manager.run()
```

### Other Languages

#### JavaScript/Node.js Example

```javascript
const WebSocket = require('ws');
const crypto = require('crypto');

class BitmartWebSocket {
    constructor(apiKey = null, apiSecret = null, apiMemo = null, isPrivate = false) {
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
        this.apiMemo = apiMemo;
        this.wsUrl = isPrivate 
            ? 'wss://ws-manager-compress.bitmart.com/user?protocol=1.1'
            : 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1';
    }

    sign(timestamp) {
        const message = `${timestamp}#${this.apiMemo}#bitmart.WebSocket`;
        return crypto
            .createHmac('sha256', this.apiSecret)
            .update(message)
            .digest('hex');
    }

    connect() {
        this.ws = new WebSocket(this.wsUrl);

        this.ws.on('open', () => {
            console.log('Connected to BitMart WebSocket');
            
            if (this.apiKey) {
                // Login for private channels
                const timestamp = Date.now();
                const sign = this.sign(timestamp);
                this.ws.send(JSON.stringify({
                    op: 'login',
                    args: [this.apiKey, timestamp.toString(), sign]
                }));
            } else {
                // Subscribe to public channels
                this.ws.send(JSON.stringify({
                    op: 'subscribe',
                    args: ['spot/ticker:BTC_USDT', 'spot/depth5:BTC_USDT']
                }));
            }

            // Keep alive
            setInterval(() => {
                this.ws.send('ping');
            }, 15000);
        });

        this.ws.on('message', (data) => {
            // Handle both text and binary messages
            const message = data.toString();
            
            if (message === 'pong') {
                console.log('Received pong');
                return;
            }

            try {
                const json = JSON.parse(message);
                console.log('Received:', json);
            } catch (e) {
                // Binary message - needs decompression
                const zlib = require('zlib');
                const inflated = zlib.inflateRawSync(data);
                console.log('Decompressed:', inflated.toString());
            }
        });

        this.ws.on('error', (error) => {
            console.error('WebSocket error:', error);
        });

        this.ws.on('close', () => {
            console.log('WebSocket connection closed');
        });
    }
}

// Usage
const client = new BitmartWebSocket();
client.connect();
```

#### Go Example

```go
package main

import (
    "bytes"
    "compress/flate"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "time"

    "github.com/gorilla/websocket"
)

const (
    PublicWS  = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
    PrivateWS = "wss://ws-manager-compress.bitmart.com/user?protocol=1.1"
)

type BitmartWS struct {
    conn      *websocket.Conn
    apiKey    string
    apiSecret string
    apiMemo   string
}

func (b *BitmartWS) sign(timestamp int64) string {
    message := fmt.Sprintf("%d#%s#bitmart.WebSocket", timestamp, b.apiMemo)
    h := hmac.New(sha256.New, []byte(b.apiSecret))
    h.Write([]byte(message))
    return hex.EncodeToString(h.Sum(nil))
}

func decompress(data []byte) ([]byte, error) {
    reader := flate.NewReader(bytes.NewReader(data))
    defer reader.Close()
    return ioutil.ReadAll(reader)
}

func (b *BitmartWS) Connect(isPrivate bool) error {
    url := PublicWS
    if isPrivate {
        url = PrivateWS
    }

    conn, _, err := websocket.DefaultDialer.Dial(url, nil)
    if err != nil {
        return err
    }
    b.conn = conn

    // Handle authentication for private channels
    if isPrivate && b.apiKey != "" {
        timestamp := time.Now().UnixNano() / int64(time.Millisecond)
        sign := b.sign(timestamp)
        
        loginMsg := map[string]interface{}{
            "op": "login",
            "args": []string{
                b.apiKey,
                fmt.Sprintf("%d", timestamp),
                sign,
            },
        }
        
        if err := conn.WriteJSON(loginMsg); err != nil {
            return err
        }
    }

    // Subscribe to channels
    subMsg := map[string]interface{}{
        "op": "subscribe",
        "args": []string{
            "spot/ticker:BTC_USDT",
            "spot/depth5:BTC_USDT",
        },
    }
    
    if err := conn.WriteJSON(subMsg); err != nil {
        return err
    }

    // Keep alive
    go func() {
        ticker := time.NewTicker(15 * time.Second)
        defer ticker.Stop()
        
        for range ticker.C {
            if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
                log.Println("Ping error:", err)
                return
            }
        }
    }()

    return nil
}

func (b *BitmartWS) Listen() {
    for {
        messageType, message, err := b.conn.ReadMessage()
        if err != nil {
            log.Println("Read error:", err)
            return
        }

        if messageType == websocket.BinaryMessage {
            // Decompress binary message
            decompressed, err := decompress(message)
            if err != nil {
                log.Println("Decompress error:", err)
                continue
            }
            message = decompressed
        }

        fmt.Printf("Received: %s\n", string(message))
    }
}

func main() {
    // Public WebSocket
    ws := &BitmartWS{}
    if err := ws.Connect(false); err != nil {
        log.Fatal(err)
    }
    ws.Listen()
}
```

---

## Quick Reference

### Connection Checklist

- [ ] WebSocket URL correct (Public vs Private)
- [ ] Ping mechanism implemented (text "ping", not frames)
- [ ] Ping interval < 20 seconds
- [ ] Decompression for binary messages
- [ ] Reconnection mechanism on disconnect
- [ ] Rate limits observed

### Public Channels Summary

| Channel | Format | Description |
|---------|--------|-------------|
| Ticker | `spot/ticker:<symbol>` | Price & volume data |
| KLine | `spot/kline{interval}:<symbol>` | Candlestick data |
| Depth | `spot/depth{levels}:<symbol>` | Order book snapshot |
| Depth Incremental | `spot/depth/increase100:<symbol>` | Order book updates |
| Trade | `spot/trade:<symbol>` | Recent trades |

### Private Channels Summary

| Channel | Format | Description |
|---------|--------|-------------|
| Order Progress (Single) | `spot/user/order:<symbol>` | Single pair orders |
| Order Progress (All) | `spot/user/orders:ALL_SYMBOLS` | All pairs orders |
| Balance | `spot/user/balance:BALANCE_UPDATE` | Balance changes |

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| Connection drops after 20s | Implement ping/pong with text "ping" |
| Binary data received | Use zlib decompression |
| Login fails | Check timestamp (ms), memo, and signature |
| Version gap in depth | Request new snapshot |
| Rate limit exceeded | Reduce subscription frequency |
| Lifeless connection | Send subscription within 5 minutes |

---

## Additional Resources

- **Official Documentation:** https://developer-pro.bitmart.com/en/spot/
- **SDK Libraries:** Available for Python, JavaScript, Go, Java, PHP
- **Support:** BitMart API Club (Telegram)

> **Disclaimer:** Always test in a development environment first. BitMart may update their API without notice. Check the official documentation for the latest information.
