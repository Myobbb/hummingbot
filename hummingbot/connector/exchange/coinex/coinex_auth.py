import hashlib
import hmac
import json
from typing import Any, Dict
from urllib.parse import urlencode, urlsplit

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class CoinexAuth(AuthBase):
    """
    CoinEx v2 authentication (https://docs.coinex.com/api/v2/authorization).

    Two different signing payloads, which is the single easiest thing to get wrong:

      REST : sign  method + request_path(+query_string) + body + timestamp
      WS   : sign  timestamp   -- and nothing else

    Both use HMAC-SHA256 keyed with the secret, rendered as LOWERCASE hex (64 chars).

    `request_path` must include the `/v2` prefix. The docs' own worked example signs
    "/v2/spot/pending-order?market=BTCUSDT&...", so the path is taken from the real request URL
    rather than from the throttler limit id (which is only the bare "/spot/..." suffix here).
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    def _sign(self, prepared_str: str) -> str:
        return hmac.new(
            bytes(self._secret_key, "latin-1"),
            msg=bytes(prepared_str, "latin-1"),
            digestmod=hashlib.sha256,
        ).hexdigest().lower()

    @staticmethod
    def _body_to_string(data: Any) -> str:
        """
        Render the request body exactly as it will go on the wire.

        The signature has to match the transmitted bytes, so a dict is serialised with the same
        separators the connector uses, and an already-serialised string is passed through untouched.
        """
        if data is None:
            return ""
        if isinstance(data, (bytes, bytearray)):
            return data.decode("utf-8")
        if isinstance(data, str):
            return data
        return json.dumps(data, separators=(",", ":"))

    def _request_path(self, request: RESTRequest) -> str:
        split = urlsplit(request.url)
        path = split.path
        # A GET's query string is part of the signed payload. request.params is appended by the
        # transport after auth runs, so it is folded in here; a query already embedded in the URL
        # is preserved as-is.
        query = split.query
        if request.method is RESTMethod.GET and request.params:
            encoded = urlencode({str(k): v for k, v in request.params.items()})
            query = f"{query}&{encoded}" if query else encoded
        return f"{path}?{query}" if query else path

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = str(int(self._time_provider.time() * 1e3))
        prepared_str = (
            request.method.value.upper()
            + self._request_path(request)
            + self._body_to_string(request.data)
            + timestamp
        )
        headers: Dict[str, str] = dict(request.headers or {})
        headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-COINEX-KEY": self._api_key,
            "X-COINEX-SIGN": self._sign(prepared_str),
            "X-COINEX-TIMESTAMP": timestamp,
        })
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # CoinEx authenticates the socket with an explicit server.sign message rather than by
        # signing each frame; see get_ws_auth_payload.
        return request

    def get_ws_auth_payload(self) -> Dict[str, Any]:
        """Build the `server.sign` params. The signed string is the timestamp alone."""
        timestamp = int(self._time_provider.time() * 1e3)
        return {
            "access_id": self._api_key,
            "signed_str": self._sign(str(timestamp)),
            "timestamp": timestamp,
        }
