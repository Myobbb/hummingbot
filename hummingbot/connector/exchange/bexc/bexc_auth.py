import hashlib
import hmac
import json
import urllib.parse
from typing import Any, Dict, Optional

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

_BODY_METHODS = ("POST", "PUT", "PATCH")


class BexcAuth(AuthBase):
    """
    bexc request signing (docs "Authentication"; GET proven from myserver by the phase-0 probe, POST by Pavel's
    placement probe, 2026-11-13):

        message   = timestamp (ms) + METHOD + path + bodyHash
        path      = /api/v1/... with "?" + the query string exactly as sent
        bodyHash  = sha256_hex(body) on POST / PUT / PATCH, "" on GET / DELETE
        signature = lowercase hex HMAC-SHA256(secret, message)

    Headers: X-BEXC-API-KEY, X-BEXC-TIMESTAMP, X-BEXC-SIGNATURE, and on POST / PUT / PATCH X-BEXC-BODY-HASH (exactly 64
    hex, else 400). The timestamp must be within ±10 s of the server's, and a signature seen in the last 30 s is
    refused as a replay: each request gets its own timestamp, strictly increasing within the process, so two identical
    requests can never sign alike. The body is serialized compactly here and that exact string is hashed and sent
    (Hummingbot's REST assistant would add spaces); the query string is built here too and the URL carries it, so the
    signed query is the sent one.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider
        self._last_timestamp = 0

    @staticmethod
    def compact_body(data: Any) -> str:
        if data is None or data == "":
            return ""
        payload = json.loads(data) if isinstance(data, str) else data
        return json.dumps(payload, separators=(",", ":"))

    @staticmethod
    def query_string(params: Optional[Dict[str, Any]]) -> str:
        return urllib.parse.urlencode({str(k): str(v) for k, v in (params or {}).items() if v is not None})

    def _timestamp(self) -> str:
        timestamp = max(int(self._time_provider.time() * 1e3), self._last_timestamp + 1)
        self._last_timestamp = timestamp
        return str(timestamp)

    def sign(self, message: str) -> str:
        return hmac.new(self._secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    def auth_headers(self, method: str, request_path: str, body: str) -> Dict[str, str]:
        method = method.upper()
        timestamp = self._timestamp()
        body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest() if method in _BODY_METHODS else ""
        headers = {
            "X-BEXC-API-KEY": self._api_key,
            "X-BEXC-TIMESTAMP": timestamp,
            "X-BEXC-SIGNATURE": self.sign(f"{timestamp}{method}{request_path}{body_hash}"),
        }
        if method in _BODY_METHODS:
            headers["X-BEXC-BODY-HASH"] = body_hash
        return headers

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        parsed = urllib.parse.urlsplit(request.url)
        query = self.query_string(request.params)
        if parsed.query:
            query = f"{parsed.query}&{query}" if query else parsed.query
        request_path = parsed.path + (f"?{query}" if query else "")
        # The URL carries the exact query that was signed; the assistant must add none of its own.
        request.url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
        request.params = None
        method = str(request.method.value).upper()
        body = self.compact_body(request.data) if method in _BODY_METHODS else ""
        request.data = body or None
        headers: Dict[str, str] = dict(request.headers or {})
        if method in _BODY_METHODS:
            headers["Content-Type"] = "application/json"
        headers.update(self.auth_headers(method, request_path, body))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The only socket is the public engine socket: an API key can't authenticate one (docs).
        return request
