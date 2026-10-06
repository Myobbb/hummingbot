import base64
import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit

from hummingbot.connector.exchange.hotcoin import hotcoin_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class HotcoinAuth(AuthBase):
    """
    Hotcoin request signing (Spot "Signature Algorithm", Huobi's signature v2):

        payload   = METHOD \\n api.hotcoinfin.com \\n path \\n query
        query     = every parameter except Signature, sorted by name in code-point order (AccessKeyId ... Timestamp
                    before the lowercase names), each value percent-encoded as UTF-8 with uppercase hex and nothing
                    left bare but A-Za-z0-9-_.~ (':' -> %3A), joined as k=v with '&'
        Signature = base64(HMAC-SHA256(secret, payload))

    The four auth parameters (AccessKeyId, SignatureMethod=HmacSHA256, SignatureVersion=2, Timestamp = ISO-8601 UTC
    with milliseconds) ride in the query string of every signed request, GET or POST. So do a POST's own parameters
    (/v1/order/place, /v1/order/cancel), as the SDK sends them (POST_SIGN_URL): live 2026-10-05, the server does not
    read AccessKeyId from a JSON body. The params handed to aiohttp are exactly the ones signed; aiohttp's own
    encoding of them decodes to the same values on the server.

    The docs' worked example does not reproduce (its signature matches none of GET/POST x tradeAmount 0.1/0.01), so
    the proof is equality with the official SDK's create_signature (S3) and, live, a signed call answered code 200.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    @staticmethod
    def _encode(value: Any) -> str:
        return quote(str(value), safe="")

    def _sign(self, payload: str) -> str:
        digest = hmac.new(self._secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def _timestamp(self) -> str:
        now = datetime.fromtimestamp(self._time_provider.time(), timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    def signed_params(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        # A Signature already present (a request signed once) is never part of what is signed.
        signed = {str(key): str(value) for key, value in (params or {}).items()
                  if value is not None and key != "Signature"}
        signed.update({
            "AccessKeyId": self._api_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": self._timestamp(),
        })
        query = "&".join(f"{key}={self._encode(signed[key])}" for key in sorted(signed))
        signed["Signature"] = self._sign("\n".join((method.upper(), CONSTANTS.REST_SIGN_HOST, path, query)))
        return signed

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        request.params = self.signed_params(request.method.value, urlsplit(request.url).path, request.params)
        headers: Dict[str, str] = dict(request.headers or {})
        # The SDK's header for signed requests whose parameters are all in the URL; there is no body.
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        request.headers = headers
        return request

    def ws_signin_message(self) -> Dict[str, Any]:
        """The private stream's login frame. Signed like REST with method POST, host wss.hotcoinfin.com and the
        path `signin` (no slash), over accessKey and a millisecond timestamp (docs "Websocket Login", SDK
        build_websocket_signin)."""
        timestamp = str(int(self._time_provider.time() * 1e3))
        payload = "\n".join(("POST", CONSTANTS.WS_SIGNIN_HOST, CONSTANTS.WS_SIGNIN_PATH,
                             f"accessKey={self._api_key}&timestamp={timestamp}"))
        return {"signin": {"accessKey": self._api_key, "timestamp": timestamp, "signature": self._sign(payload)}}

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private stream is authorised by the signin frame (ws_signin_message), not per request.
        return request
