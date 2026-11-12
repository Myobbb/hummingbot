import base64
import hashlib
import hmac
import json
import urllib.parse
import uuid
from typing import Any, Dict, Optional

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BlofinAuth(AuthBase):
    """
    BloFin request signing (docs "REST Authentication", the same scheme the P1 tracker and the asset_manager probes
    use; verified live from myserver 2026-11-11):

        prehash = requestPath (with the query string, exactly as sent) + METHOD + timestamp (ms) + nonce + body
        sign    = base64( hex( HMAC-SHA256(secret, prehash) ) )      -- base64 of the HEX STRING, not of the digest

    Headers: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP, ACCESS-NONCE (unique within the server's window: a UUID),
    ACCESS-PASSPHRASE. body = the exact JSON sent ("" for GET). Hummingbot's REST assistant serializes `data` with
    json.dumps' default separators (spaces), and the docs' troubleshooting says the signed body must carry none: the
    body is re-serialized compactly here and that exact string is both signed and sent. The query string is built here
    too (the assistant's own encoding could differ from the signed one) and the URL carries it.
    """

    def __init__(self, api_key: str, secret_key: str, passphrase: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._passphrase = passphrase
        self._time_provider = time_provider

    @staticmethod
    def compact_body(data: Any) -> str:
        if data is None or data == "":
            return ""
        payload = json.loads(data) if isinstance(data, str) else data
        return json.dumps(payload, separators=(",", ":"))

    @staticmethod
    def query_string(params: Optional[Dict[str, Any]]) -> str:
        return urllib.parse.urlencode({str(k): str(v) for k, v in (params or {}).items() if v is not None})

    def sign(self, prehash: str) -> str:
        digest = hmac.new(self._secret_key.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
        return base64.b64encode(digest.encode("utf-8")).decode("utf-8")

    def auth_headers(self, method: str, request_path: str, body: str) -> Dict[str, str]:
        timestamp = str(int(self._time_provider.time() * 1e3))
        nonce = str(uuid.uuid4())
        return {
            "ACCESS-KEY": self._api_key,
            "ACCESS-SIGN": self.sign(f"{request_path}{method.upper()}{timestamp}{nonce}{body}"),
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-NONCE": nonce,
            "ACCESS-PASSPHRASE": self._passphrase,
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        parsed = urllib.parse.urlsplit(request.url)
        query = self.query_string(request.params)
        if parsed.query:
            query = f"{parsed.query}&{query}" if query else parsed.query
        request_path = parsed.path + (f"?{query}" if query else "")
        # The URL carries the exact query that was signed; the assistant must add none of its own.
        request.url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
        request.params = None
        body = self.compact_body(request.data)
        request.data = body or None
        headers: Dict[str, str] = dict(request.headers or {})
        headers["Content-Type"] = "application/json"
        headers.update(self.auth_headers(str(request.method.value), request_path, body))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The only socket used is the website's public spot socket: nothing to sign.
        return request
