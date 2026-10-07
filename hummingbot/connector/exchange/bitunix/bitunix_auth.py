import hashlib
import json
import uuid
from typing import Any, Dict, Optional

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BitunixAuth(AuthBase):
    """
    Bitunix request signing (docs sign.md; verified live 2026-10-07 from myserver):

        digest = sha256hex(nonce + timestamp + apiKey + query + body)
        sign   = sha256hex(digest + secretKey)

    query = the GET parameters sorted by name, each written as key + value with NO '=' and no separator (the WS rule
    "symbolBTC"; the REST page's own example "id=1uid=200" is refused with 100005, live). body = the exact JSON sent,
    compact (no spaces). Headers: api-key, nonce (32 random characters), timestamp (ms), sign.

    Hummingbot's REST assistant serializes `data` with json.dumps' default separators, which put spaces in the body;
    the body is re-serialized compactly here and that exact string is both signed and sent.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    @staticmethod
    def signing_query(params: Optional[Dict[str, Any]]) -> str:
        return "".join(f"{key}{params[key]}" for key in sorted(params or {}))

    @staticmethod
    def compact_body(data: Any) -> str:
        if data is None or data == "":
            return ""
        payload = json.loads(data) if isinstance(data, str) else data
        return json.dumps(payload, separators=(",", ":"))

    def sign(self, nonce: str, timestamp: str, query: str, body: str) -> str:
        digest = hashlib.sha256((nonce + timestamp + self._api_key + query + body).encode("utf-8")).hexdigest()
        return hashlib.sha256((digest + self._secret_key).encode("utf-8")).hexdigest()

    def auth_headers(self, params: Optional[Dict[str, Any]], body: str) -> Dict[str, str]:
        nonce = uuid.uuid4().hex
        timestamp = str(int(self._time_provider.time() * 1e3))
        return {
            "api-key": self._api_key,
            "nonce": nonce,
            "timestamp": timestamp,
            "sign": self.sign(nonce, timestamp, self.signing_query(params), body),
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        params = {str(key): str(value) for key, value in (request.params or {}).items() if value is not None}
        request.params = params or None
        body = self.compact_body(request.data)
        request.data = body or None
        headers: Dict[str, str] = dict(request.headers or {})
        headers["Content-Type"] = "application/json"
        headers.update(self.auth_headers(params, body))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The only socket used is the website's public market socket: nothing to sign.
        return request
