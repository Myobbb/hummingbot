import base64
import hashlib
import hmac
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit

from hummingbot.connector.exchange.poloniex import poloniex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class PoloniexAuth(AuthBase):
    """
    Poloniex request signing, as the official SDK signs (polo-sdk-python polosdk/spot/rest/request.py
    _get_sig_header; the docs' worked example drops signTimestamp from its own final string, the SDK does not):

        no body:   "METHOD\\n<path>\\n" + "&".join(sorted k=v, signTimestamp included, values URI-encoded)
        a body:    "METHOD\\n<path>\\nrequestBody=<the exact body sent>&signTimestamp=<ms>"
        signature = base64(HMAC-SHA256(secret, payload)); headers key / signature / signTimestamp

    The body: RESTAssistant serialises `data` with json.dumps before auth runs, and that very string is both signed
    and sent, so they cannot differ. Proven 2026-10-09 against the SDK's signer (S5) and live with the AM's key
    (GETs, a cid: path, a DELETE with a body).
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    @staticmethod
    def _encode(value: Any) -> str:
        return quote("" if value is None else str(value), safe=CONSTANTS.SIGN_SAFE_CHARS)

    def _signature(self, payload: str) -> str:
        return base64.b64encode(
            hmac.new(self._secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()).decode()

    def timestamp_ms(self) -> int:
        return int(self._time_provider.time() * 1e3)

    def payload(self, method: str, path: str, params: Optional[Dict[str, Any]], body: str, timestamp: str) -> str:
        if body:
            signed = f"requestBody={body}&signTimestamp={timestamp}"
        else:
            pairs = {str(k): v for k, v in (params or {}).items() if v is not None}
            pairs["signTimestamp"] = timestamp
            signed = "&".join(f"{k}={self._encode(pairs[k])}" for k in sorted(pairs))
        return f"{method.upper()}\n{path}\n{signed}"

    def signed_headers(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                       body: str = "") -> Dict[str, str]:
        timestamp = str(self.timestamp_ms())
        return {
            "key": self._api_key,
            "signTimestamp": timestamp,
            "signature": self._signature(self.payload(method, path, params, body, timestamp)),
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        body = request.data if isinstance(request.data, str) else ""
        headers: Dict[str, str] = dict(request.headers or {})
        headers.update(self.signed_headers(method=request.method.value, path=urlsplit(request.url).path,
                                           params=request.params, body=body))
        headers["Content-Type"] = "application/json"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private stream is authorised by an `auth` event on the socket (ws_auth_payload), not per request.
        return request

    def ws_auth_payload(self) -> Dict[str, Any]:
        """{"event":"subscribe","channel":["auth"],"params":{key, signTimestamp, signature}}, signed
        "GET\\n/ws\\nsignTimestamp=<ms>" (Spot WebSocket API/Authentication; the SDK's ClientAuthenticated)."""
        timestamp = self.timestamp_ms()
        return {
            "event": "subscribe",
            "channel": [CONSTANTS.WS_AUTH_CHANNEL],
            "params": {
                "key": self._api_key,
                "signTimestamp": timestamp,
                "signature": self._signature(f"GET\n/ws\nsignTimestamp={timestamp}"),
            },
        }
