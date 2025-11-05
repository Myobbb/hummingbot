# HTX Spot API Documentation

## Overview

This document contains essential information for integrating with the HTX (formerly Huobi) Spot API.

---

## Access URLs

HTX provides multiple endpoints for REST and WebSocket connections. Choose based on your server location:

### REST API
- **Primary**: `https://api.huobi.pro`
- **AWS Optimized**: `https://api-aws.huobi.pro`

### WebSocket Endpoints

#### Market Data (except MBP incremental)
- **Primary**: `wss://api.huobi.pro/ws`
- **AWS Optimized**: `wss://api-aws.huobi.pro/ws`

#### Market Data (MBP incremental only)
- **Primary**: `wss://api.huobi.pro/feed`
- **AWS Optimized**: `wss://api-aws.huobi.pro/feed`

#### Account and Order Data
- **Primary**: `wss://api.huobi.pro/ws/v2`
- **AWS Optimized**: `wss://api-aws.huobi.pro/ws/v2`

### Connection Recommendations
- Compare network latency between `api.huobi.pro` and `api-aws.huobi.pro` to choose the optimal endpoint
- The domain `api-aws.huobi.pro` is optimized for AWS clients and typically offers lower latency
- Initiate API calls with non-China IP addresses
- **Not recommended**: Using proxy to access HTX API (introduces high latency and instability)
- **Recommended**: Access HTX API from AWS Japan for better stability
- Servers in mainland China may experience instability

---

## Authentication

All private API requests must be signed using your API Key. HTX supports two signature methods:

### Components of a Valid Request

1. **API Path**: e.g., `api.huobi.pro/v1/order/orders`
2. **API Access Key**: The 'Access Key' from your API Key
3. **Signature Method**: 
   - `Ed25519` (elliptic curve digital signature algorithm)
   - `HmacSHA256` (hash-based protocol)
4. **Signature Version**: `2`
5. **Timestamp**: UTC time when request is sent (format: `2017-05-11T16:22:06`)
   - Valid for 5 minutes from generation
6. **Parameters**:
   - **GET requests**: All parameters must be signed
   - **POST requests**: Parameters don't need to be signed and should be in request body
7. **Signature**: Ensures request validity and prevents tampering

---

## Signature Method 1: Ed25519

Ed25519 is a high-performance digital signature algorithm providing fast signature verification and generation with high security.

### Signing Process

#### Example Request
```
https://api.huobi.pro/v1/order/orders?
AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx
&SignatureMethod=Ed25519
&SignatureVersion=2
&Timestamp=2017-05-11T15:19:30
&order-id=1234567890
```

### Step-by-Step Signing

**Step 1**: Request Method + Line Break
```
GET\n
```
*Note: WebSocket uses GET*

**Step 2**: Lowercase Host + Line Break
```
api.huobi.pro\n
```

**Step 3**: Path + Line Break

For REST endpoint:
```
/v1/order/orders\n
```

For WebSocket v2:
```
/ws/v2
```

**Step 4**: URL Encode and Sort Parameters by ASCII

Original parameters:
```
AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx
order-id=1234567890
SignatureMethod=Ed25519
SignatureVersion=2
Timestamp=2017-05-11T15%3A19%3A30
```

Encoding rules:
- Use UTF-8 encoding
- Hexadecimal must be uppercase
- `:` becomes `%3A`
- Space becomes `%20`
- Timestamp format: `YYYY-MM-DDThh:mm:ss` then URL encoded

Sorted parameters:
```
AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx
SignatureMethod=Ed25519
SignatureVersion=2
Timestamp=2017-05-11T15%3A19%3A30
order-id=1234567890
```

**Step 5**: Concatenate Parameters with "&"
```
AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx&SignatureMethod=Ed25519&SignatureVersion=2&Timestamp=2017-05-11T15%3A19%3A30&order-id=1234567890
```

**Step 6**: Assemble Pre-Signed Text
```
GET\n
api.huobi.pro\n
/v1/order/orders\n
AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx&SignatureMethod=Ed25519&SignatureVersion=2&Timestamp=2017-05-11T15%3A19%3A30&order-id=1234567890
```

**Step 7**: Generate Signature
- Use the pre-signed text and Ed25519 private key to generate signature
- Encode the signature with Base64

Example result:
```
4F65x5A2bLyMWVQj3Aqp+B4w+ivaA7n5Oi2SuYtCJ9o=
```

**Step 8**: Add Signature to Request

**For REST Interface**:
1. Put all parameters in URL
2. URL encode the signature
3. Append as `Signature` parameter

Final URL:
```
https://api.huobi.pro/v1/order/orders?AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx&order-id=1234567890&SignatureMethod=Ed25519&SignatureVersion=2&Timestamp=2017-05-11T15%3A19%3A30&Signature=4F65x5A2bLyMWVQj3Aqp%2BB4w%2BivaA7n5Oi2SuYtCJ9o%3D
```

**For WebSocket Interface**:
1. Fill values in JSON format
2. Values in JSON don't require URL encoding

Example:
```json
{
  "action": "req",
  "ch": "auth",
  "params": {
    "authType": "api",
    "accessKey": "e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx",
    "signatureMethod": "Ed25519",
    "signatureVersion": "2.1",
    "timestamp": "2019-09-01T18:16:16",
    "signature": "4F65x5A2bLyMWVQj3Aqp+B4w+ivaA7n5Oi2SuYtCJ9o="
  }
}
```

---

## Signature Method 2: HmacSHA256

### Signing Process

The process is identical to Ed25519, with the following differences:

**Step 1-6**: Same as Ed25519

**Step 7**: Generate Signature Using HmacSHA256
- Use pre-signed text from Step 6
- Generate hash code using HmacSHA256 with your API Secret Key
- Encode hash with Base64

Example result:
```
4F65x5A2bLyMWVQj3Aqp+B4w+ivaA7n5Oi2SuYtCJ9o=
```

**Step 8**: Same as Ed25519

**For REST Interface** - Final URL:
```
https://api.huobi.pro/v1/order/orders?AccessKeyId=e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx&order-id=1234567890&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp=2017-05-11T15%3A19%3A30&Signature=4F65x5A2bLyMWVQj3Aqp%2BB4w%2BivaA7n5Oi2SuYtCJ9o%3D
```

**For WebSocket Interface** - Example:
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

---

## API Key Permissions

Each API Key can have up to three permissions:
- **Read**: Query data (orders, trades, account information)
- **Trade**: Place orders, cancel orders, transfer funds
- **Withdraw**: Create and cancel withdrawal orders

---

## Best Practices

### Security

- **Strongly recommended**: Bind your API Key to your IP address
  - Ensures API Key can only be used from your machine
  - API Keys expire after 90 days if not IP-bound
- **Never share**: Don't share your API Key with anyone or third-party software
  - Risk of personal information and asset theft
- **Immediate action**: If API Key is exposed, delete it immediately and create a new one

### General API Access

- **Avoid**: Temporary domains or proxies (causes instability)
- **Recommended**: Use AWS Japan for API access (lower latency)
- **Preferred endpoint**: Use `api-aws.huobi.pro` for AWS-based servers

### Rate Limiting

- New rate limit rules apply only to endpoints with separately marked rate limit values
- Read HTTP headers to manage rate limits dynamically:
  - `X-HB-RateLimit-Requests-Remain`: Remaining request count
  - `X-HB-RateLimit-Requests-Expire`: Expiration time for current rate limit window
- Overall access rate from all API Keys under same UID to single endpoint must not exceed the limit

### Market Data

- **Recommended**: Use WebSocket for market data updates
  - Lower latency
  - No rate limits
  - Real-time updates
- **Avoid**: Subscribing to too many topics in a single WebSocket connection
  - May cause network latency and disconnection

### Latest Trade Data

- **Subscribe to**: `market.$symbol.trade.detail` WebSocket topic
  - `price` field represents latest price
  - Lower latency than REST
- **Use**: `tradeId` field for de-duplication

### Depth/Orderbook Data

Choose the appropriate topic based on your needs:

- **Best Bid/Offer only**: Subscribe to `market.$symbol.bbo`
- **Multiple levels (normal latency)**: Subscribe to `market.$symbol.depth.$type`
- **Multiple levels (low latency)**: Subscribe to `market.$symbol.mbp.$level`
- **De-duplication**:
  - Use `version` field for REST `/market/depth` and WebSocket `market.$symbol.depth.$type`
  - Use `seqNum` field for WebSocket `market.$symbol.mbp.$levels`

### Order Management

#### Placing Orders (`/v1/order/orders/place`)

- **Validate before submission**: Check symbol reference (`/v1/common/symbols`) for amount and value limits
- **Use unique client-order-id**: 
  - Useful for tracking order status if order ID response fails
  - Can match with WebSocket order notifications
  - Query order details using `/v1/order/orders/getClientOrder`
  - **Note**: Uniqueness is not enforced server-side; manage it client-side

#### Searching Historical Orders (`/v1/order/orders`)

- **Use timestamps**: Specify `start-time` and `end-time` (13-digit millisecond timestamps)
- **Query window**: Maximum 48 hours (2 days)
- **Tip**: More precise time ranges = better performance
- Can query across multiple iterations

#### Order Updates

- **Recommended**: Subscribe to WebSocket `orders.$symbol`
  - Lower latency
  - More accurate sequencing

### Account Management

#### Asset Updates

- **Subscribe to both topics**:
  1. `orders.$symbol`: Order status updates (arrives first)
  2. `accounts.update#${mode}`: Final asset balance confirmation
- **Deprecated**: `accounts` topic (use `accounts.update#${mode}` instead)

---

## Important Notes

- All timestamps are in **UTC**
- Timestamp values are valid for **5 minutes** from generation
- Parameter values must be **UTF-8 encoded**
- URL encoding hexadecimal characters must be **UPPERCASE**
- Private API requests may be tampered with during transmission; signing is mandatory
- Each API Key has permission properties; verify proper permissions before use

---

## References

- Official API Documentation: https://www.htx.com/en-in/opend/newApiPages/
