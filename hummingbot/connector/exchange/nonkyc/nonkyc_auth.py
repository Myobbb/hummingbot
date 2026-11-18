import hashlib
import hmac
import json
import urllib.parse
from typing import Any, Dict, Optional

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

_BODY_METHODS = ("POST", "PUT", "PATCH")


class NonkycAuth(AuthBase):
    """
    NonKYC request signing (OpenAPI 1.3.0, "Authentication"; live-probed 2026-11-17 from myserver):

        message   = apiKey + requestUrl + body + nonce
        requestUrl= the FULL url, query string included, exactly as sent
        body      = the compact JSON string on POST / PUT / PATCH, "" on GET
        nonce     = unix milliseconds, as a string
        signature = lowercase hex HMAC-SHA256(secret, message)

    Headers: X-API-KEY, X-API-NONCE, X-API-SIGN (and Content-Type: application/json on a body).
    The query being signed is not a doc detail: a GET whose params were signed without them answers HTTP 401
    "Not Authorized" (probed 2026-11-17), so this class builds the query itself and the URL carries it -- the
    assistant must add none of its own.

    A repeated nonce is refused (WS docs code 10001), so each request gets its own, strictly increasing within the
    process: two identical requests can never sign alike.

    The socket's login signs the NONCE ALONE (HS256), which `ws_login_params` builds.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider
        self._last_nonce = 0

    @staticmethod
    def compact_body(data: Any) -> str:
        if data is None or data == "":
            return ""
        payload = json.loads(data) if isinstance(data, str) else data
        return json.dumps(payload, separators=(",", ":"))

    @staticmethod
    def query_string(params: Optional[Dict[str, Any]]) -> str:
        return urllib.parse.urlencode({str(k): str(v) for k, v in (params or {}).items() if v is not None})

    def _nonce(self) -> str:
        nonce = max(int(self._time_provider.time() * 1e3), self._last_nonce + 1)
        self._last_nonce = nonce
        return str(nonce)

    def sign(self, message: str) -> str:
        return hmac.new(self._secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    def auth_headers(self, url: str, body: str, nonce: Optional[str] = None) -> Dict[str, str]:
        nonce = nonce or self._nonce()
        return {
            "X-API-KEY": self._api_key,
            "X-API-NONCE": nonce,
            "X-API-SIGN": self.sign(f"{self._api_key}{url}{body}{nonce}"),
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        parsed = urllib.parse.urlsplit(request.url)
        query = self.query_string(request.params)
        if parsed.query:
            query = f"{parsed.query}&{query}" if query else parsed.query
        url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
        request.url = url
        request.params = None
        method = str(request.method.value).upper()
        body = self.compact_body(request.data) if method in _BODY_METHODS else ""
        request.data = body or None
        headers: Dict[str, str] = dict(request.headers or {})
        if method in _BODY_METHODS:
            headers["Content-Type"] = "application/json"
        headers.update(self.auth_headers(url, body))
        request.headers = headers
        return request

    def ws_login_params(self) -> Dict[str, str]:
        """The private socket's `login` params (WS API 1.1.0): HS256 signs the nonce alone, not a URL."""
        nonce = self._nonce()
        return {
            "algo": "HS256",
            "pKey": self._api_key,
            "nonce": nonce,
            "signature": self.sign(nonce),
        }

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The socket authenticates with an explicit `login` frame, built by the user stream data source.
        return request
