import hashlib
import hmac
import math
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class CoinstoreAuth(AuthBase):
    """
    Coinstore request signing (docs "Signature Authentication"; Coinstore's own Python SDK, mirrored in
    VS_code_projects/MDs/coinstore-api/_sdk/coinstore.py):

        key       = hex(HMAC-SHA256(secret, str(floor(X-CS-EXPIRES / 30000))))
        X-CS-SIGN = hex(HMAC-SHA256(key, payload))
        payload   = the query string exactly as sent (GET), or the JSON body exactly as sent (POST); not sorted

    Headers X-CS-APIKEY, X-CS-EXPIRES (ms) and X-CS-SIGN, with Content-Type application/json. A GET's parameters are
    written into the URL here, in their given order, and the very same string is signed, so the bytes signed and the
    bytes sent cannot differ (aiohttp would otherwise re-encode `params`). A POST body is the string RESTAssistant
    serialised before auth runs: that string is signed and sent as is.

    Live 2026-11-08 (Pavel's key, from myserver): every signed route answered code 0; an X-CS-EXPIRES 300 s behind and
    120 s ahead of the server clock was still accepted, so the local (NTP) clock is enough. A bad signature, an unknown
    key and an IP outside the key's whitelist all answer 1401 "Unauthorized" (or 401 "signature-failed"), never a
    clock error.

    The private socket logs in with {"op": "LOGIN", "channel": ["trade"], "auth": {"token": <key>, "type": "apikey",
    "expires": "<ms>", "signature": <the same signature over the expires string itself>}, "id": n}: undocumented
    (the docs only name the LOGIN op), found live 2026-11-08 — the server answers C 200 "login.success"; an empty
    payload signed instead answers C 503 "auth.session.needed".
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    def expires_ms(self) -> int:
        return int(self._time_provider.time() * 1e3)

    def signature(self, payload: str, expires_ms: int) -> str:
        key = hmac.new(self._secret_key.encode("utf-8"), str(math.floor(expires_ms / 30000)).encode("utf-8"),
                       hashlib.sha256).hexdigest()
        return hmac.new(key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def query_string(params: Optional[Mapping[str, Any]]) -> str:
        """k=v joined with '&', in the given order (Coinstore signs the string as sent, unsorted). Every value we send
        (symbols, coin codes, ids, numbers) is plain alphanumeric, so the quoting is a no-op kept for safety."""
        return "&".join(f"{k}={quote(str(v), safe='-_.~')}" for k, v in (params or {}).items() if v is not None)

    def signed_headers(self, payload: str) -> Dict[str, str]:
        expires = self.expires_ms()
        return {
            "X-CS-APIKEY": self._api_key,
            "X-CS-EXPIRES": str(expires),
            "X-CS-SIGN": self.signature(payload, expires),
            "Content-Type": "application/json",
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if request.method is RESTMethod.GET:
            query = self.query_string(request.params)
            if query:
                request.url = f"{request.url}{'&' if '?' in request.url else '?'}{query}"
            request.params = None
            payload = query
        else:
            payload = request.data if isinstance(request.data, str) else ""
        headers: Dict[str, str] = dict(request.headers or {})
        headers.update(self.signed_headers(payload))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private socket is authorised by the LOGIN frame (ws_login_payload), not per request.
        return request

    def ws_login_payload(self, request_id: int) -> Dict[str, Any]:
        expires = self.expires_ms()
        return {
            "op": "LOGIN",
            "channel": ["trade"],
            "auth": {"token": self._api_key, "type": "apikey", "expires": str(expires),
                     "signature": self.signature(str(expires), expires)},
            "id": request_id,
        }
