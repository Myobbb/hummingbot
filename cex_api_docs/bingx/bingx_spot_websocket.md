# BingX Spot WebSocket — Agent-Focused Cheat Sheet
_Last updated: 2025-11-11_

This guide condenses the official docs for fast, reliable integration by bots/agents. It removes site chrome, fluff, and duplicate samples while keeping request/response shapes exact.

---

## 0) Endpoints & Connection Essentials

- **Market WS URL:** `wss://open-api-ws.bingx.com/market`  ⟶ use this for market and account streams (the latter with `listenKey`). fileciteturn4file18
- **Compression:** All server messages are **GZIP**-compressed; decompress before parsing. fileciteturn4file18
- **Heartbeat:** Server sends a `ping` roughly every 5s; client should reply with a `pong`.  
  Example payloads from docs:  
  `{{"ping":"2177...","time":"2022-06-07T16:27:36.323+0800"}}` → reply `{{"pong":"2177...","time":"2022-06-07T16:27:36.323+0800"}}`. fileciteturn4file18
- **Connection limits:** Max **200 topics per WebSocket** (error `100416`), and max **60 sockets per IP** (error `100419`). fileciteturn4file18
- **Symbols:** Must be **UPPERCASE** with a **hyphen** separator, e.g., `BTC-USDT`. fileciteturn4file18

**Subscribe / Unsubscribe envelopes** (ack returns same `id`):
```json
{ "id": "id1", "reqType": "sub",  "dataType": "DATA_TO_SUB" }
{ "id": "id1", "reqType": "unsub","dataType": "DATA_TO_UNSUB" }
```
Ack on success: `{{ "id": "id1", "code": 0, "msg": "" }}`. fileciteturn4file18

**Common error codes** (subset):  
`0: SUCCESS`, `100204: SEARCH_NO_CONTENT`, `100205: REPEAT_REQUEST`, `100400: ILLEGAL_ARGUMENT`, `100401: AUTHENTICATION_FAIL`, `100403: AUTHORIZATION_FAIL`, `100410: FREQUENCY_LIMIT`, `100500: INTERNAL_SERVER_ERROR`, `100503: SERVER_BUSY`. fileciteturn4file18

---

## 1) Listen Key (Account Streams)

- **Purpose:** Authenticate account WebSocket streams by appending `?listenKey=...` to the Market WS URL. fileciteturn4file1
- **Validity:** Listen key is **valid for 1 hour**; extending resets it to **60 minutes** (docs recommend sending a ping every 30 minutes). fileciteturn4file2
- **Rate limits:** By UID **2/s**; by IP (group) **2/s** for the listen key endpoints. fileciteturn4file17

### Endpoints
- **Generate:** `POST /openApi/user/auth/userDataStream` → `{{"listenKey":"..."}}`. fileciteturn4file17
- **Extend:** `PUT /openApi/user/auth/userDataStream` (param `listenKey`). fileciteturn4file16
- **Delete:** `DELETE /openApi/user/auth/userDataStream` (param `listenKey`). fileciteturn4file13

**Headers:** `X-BX-APIKEY` (API key). ● Content-Type: JSON. fileciteturn4file17

#### Minimal Python — generate/extend/delete listenKey (doc-aligned)
```python
import time, requests, hmac
from hashlib import sha256

APIURL = "https://open-api.bingx.com"
APIKEY = "<your_api_key>"
SECRETKEY = "<your_api_secret>"

def sign(qs: str) -> str:
    return hmac.new(SECRETKEY.encode(), qs.encode(), sha256).hexdigest()

def ts_params(extra: dict=None) -> str:
    p = {} if not extra else dict(extra)
    p["timestamp"] = int(time.time() * 1000)
    return "&".join([f"{k}={p[k]}" for k in sorted(p)])

def call(method: str, path: str, params: dict=None):
    qs = ts_params(params)
    url = f"{APIURL}{path}?{qs}&signature={sign(qs)}"
    return requests.request(method, url, headers={"X-BX-APIKEY": APIKEY}).json()

# Generate
lk = call("POST", "/openApi/user/auth/userDataStream")["listenKey"]  # -> str
# Extend (reset to 60m)
call("PUT", "/openApi/user/auth/userDataStream", {"listenKey": lk})
# Delete
call("DELETE", "/openApi/user/auth/userDataStream", {"listenKey": lk})
```
(Same structure as doc samples, trimmed to essentials.) fileciteturn4file17

---

## 2) Market Data Streams (dataType)

Each subscription uses the generic envelope above with `dataType` set per stream. fileciteturn4file7

| Use case | dataType (example) |
|---|---|
| **Trades** | `<symbol>@trade` e.g., `BTC-USDT@trade` fileciteturn4file0 |
| **Klines** | `<symbol>@kline_<interval>` e.g., `BTC-USDT@kline_1min` fileciteturn4file7 |
| **Market depth (limited)** | `<symbol>@depth<level>` e.g., `BTC-USDT@depth50` • Pushed every **300ms** (default level 20). fileciteturn4file7 |
| **24h ticker** | `<symbol>@ticker` e.g., `BTC-USDT@ticker` • Pushed every **1000ms** (1 second). fileciteturn4file14 |
| **Last price** | `<symbol>@lastPrice` e.g., `BTC-USDT@lastPrice` fileciteturn4file4 |
| **Best bid/ask** | `<symbol>@bookTicker` e.g., `BTC-USDT@bookTicker` fileciteturn4file7 |
| **Incremental/full depth** | `<symbol>@incrDepth` e.g., `BTC-USDT@incrDepth` • **1000 levels**, pushed every **500ms**. See notes below. fileciteturn4file19 |

#### Minimal Python — one reusable WS client (set `CHANNEL` accordingly)
```python
import json, websocket, gzip, io

URL = "wss://open-api-ws.bingx.com/market"
CHANNEL = {"id":"<your-uuid>","reqType":"sub","dataType":"BTC-USDT@ticker"}  # set dataType

def on_open(ws):
    ws.send(json.dumps(CHANNEL))

def on_message(ws, message):
    data = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb').read().decode('utf-8')
    # Important from docs: respond if you receive a ping
    if "ping" in data:
        ws.send("Pong")
    print(data)

ws = websocket.WebSocketApp(URL, on_open=on_open, on_message=on_message)
ws.run_forever()
```
(Directly aligned with the doc Python examples, de-duplicated.) fileciteturn4file14

### Kline Intervals
Available `<interval>` values for kline streams:
- **Minutes:** `1min`, `3min`, `5min`, `15min`, `30min`, `60min`
- **Hours:** `2hour`, `4hour`, `6hour`, `8hour`, `12hour`
- **Days:** `1day`, `3day`
- **Week:** `1week`
- **Month:** `1mon`

### Depth Levels
Available `<level>` values for depth streams:
- `5` (level5), `10` (level10), `20` (level20), `50` (level50), `100` (level100)
- Default level is **20** if not specified
- Example: `BTC-USDT@depth5`, `BTC-USDT@depth100`

### Incremental Depth (`@incrDepth`) — Important Implementation Notes

The `<symbol>@incrDepth` stream pushes **incremental depth of 1000 levels every 500ms**. Critical for maintaining local order book:

1. **First message:** Full depth snapshot with `action: "all"` and `lastUpdateId` for continuity tracking
2. **Subsequent messages:** Incremental updates with `action: "update"`. Each update's `lastUpdateId` should equal previous + 1
3. **Handling discontinuity:** If `lastUpdateId` is not continuous (rare), either:
   - Reconnect, OR
   - Cache last 3 incremental depths and merge by finding continuous `lastUpdateId` (network/threading may cause out-of-order delivery)
4. **Merging incremental updates into local book:**
   - **Add:** If price level doesn't exist, add it
   - **Delete:** If quantity = 0, remove the price level
   - **Update:** If quantity ≠ 0 and exists, replace quantity
   - After each merge, update your cached `lastUpdateId`

Use thread-safe data structures (e.g., `TreeMap` or sorted map) as push frequency may increase.

---

## 3) Account Data Streams (requires listenKey)

**WS URL:** `wss://open-api-ws.bingx.com/market?listenKey=YOUR_LISTEN_KEY` fileciteturn4file9

| Use case | dataType | Subscribe example |
|---|---|---|
| **Order updates** | `spot.executionReport` | `{{"id":"id1","reqType":"sub","dataType":"spot.executionReport"}}` fileciteturn4file9 |
| **Account balance/events** | `ACCOUNT_UPDATE` | `{{"id":"id1","reqType":"sub","dataType":"ACCOUNT_UPDATE"}}` fileciteturn4file15 |

**Event reason (`m`) values** for `ACCOUNT_UPDATE` include: `INIT`, `DEPOSIT`, `WITHDRAW`, `ORDER`, `FUNDING_FEE`, `WITHDRAW_REJECT`, `ADJUSTMENT`, `INSURANCE_CLEAR`, `ADMIN_DEPOSIT`, `ADMIN_WITHDRAW`, `MARGIN_TRANSFER`, `MARGIN_TYPE_CHANGE`, `ASSET_TRANSFER`, `OPTIONS_PREMIUM_FEE`, `OPTIONS_SETTLE_PROFIT`, `AUTO_EXCHANGE`. fileciteturn4file15

You must **refresh the listen key** to avoid interruption (valid 1 hour). fileciteturn4file9

---

## 4) Practical notes

- **Topic cap per connection:** 200 (see also BingX support notice confirming this cap).  
- **IP cap:** 60 WebSocket connections per IP. fileciteturn4file18

---

## Appendix: Quick JSON examples

**Trade stream subscribe**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@trade"}
```
**Kline subscribe**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@kline_1min"}
```
**Depth (limited)**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@depth50"}
```
**24h Ticker**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@ticker"}
```
**Last Price**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@lastPrice"}
```
**Best Bid/Ask**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@bookTicker"}
```
**Incremental/Full Depth**
```json
{"id":"id1","reqType":"sub","dataType":"BTC-USDT@incrDepth"}
```
(See stream rows above for details.) fileciteturn4file7

---

### References
- Official docs: Socket → Introduction, Listen Key, Market Data, Account Data (same site sections as above). fileciteturn4file18 fileciteturn4file1 fileciteturn4file7 fileciteturn4file9
