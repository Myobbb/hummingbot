import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlsplit

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest

# The SIGNED paths (v2). Everything else is public and must not carry the key: the v1 bulk reads are unsigned, and so are
# the v2 market routes. `/sapi/v1/order` is both a signed POST (place) and a signed GET (query) — the path is signed
# either way.
SIGNED_PATHS = (
    "/sapi/v1/account",
    "/sapi/v1/order",
    "/sapi/v1/order/test",
    "/sapi/v1/cancel",
    "/sapi/v1/openOrders",
    "/sapi/v1/myTrades",
)


class CoinupAuth(AuthBase):
    """CoinUp's signed-request auth (ChainUp v2, verified live 2026-11-27):

        X-CH-SIGN = HMAC-SHA256 hex of ts + METHOD + requestPath + ("?" + queryString) + body_string
        X-CH-TS   = the same ts in milliseconds; recvWindow defaults to 5000
        X-CH-APIKEY = the key

    A signed GET MUST sign the query string, not only the path (P1's live lesson: a path-only signature answers
    HTTP 200 {"code": "-1022", "msg": "Invalid signature"}). The signing string therefore uses exactly the query the
    connection will send (the same urlencode over the same dict) and exactly the JSON body string placed on the wire.
    """

    def __init__(self, api_key: str, secret_key: str) -> None:
        self._api_key = api_key
        self._secret_key = secret_key.encode("utf-8")

    def _sign(self, ts: str, method: str, path: str, query: str, body: str) -> str:
        payload = ts + method + path + query + body
        return hmac.new(self._secret_key, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def _query(params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return ""
        return "?" + urlencode({str(k): str(v) for k, v in params.items() if v is not None})

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        path = urlsplit(request.url).path
        if path not in SIGNED_PATHS:
            return request
        ts = str(int(time.time() * 1e3))
        headers: Dict[str, str] = dict(request.headers or {})
        if request.method == RESTMethod.GET:
            query = self._query(request.params)
            signature = self._sign(ts, "GET", path, query, "")
        else:
            body = request.data
            if isinstance(body, (bytes, bytearray)):
                body = body.decode("utf-8")
            if isinstance(body, dict):
                body = json.dumps(body, separators=(",", ":"), sort_keys=False)
                request.data = body
            body = body or ""
            headers["Content-Type"] = "application/json"
            signature = self._sign(ts, "POST", path, "", body)
        headers["X-CH-APIKEY"] = self._api_key
        headers["X-CH-SIGN"] = signature
        headers["X-CH-TS"] = ts
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The only socket is market data: nothing to sign.
        return request