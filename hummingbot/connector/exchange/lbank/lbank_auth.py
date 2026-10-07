import hashlib
import hmac
import json
import random
import string
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LbankAuth(AuthBase):
    """
    LBank request signing (docs "Interaction Introduction > Authentication", the four official SDKs, CCXT's sign()):

        signed string = every parameter of the request except `sign`, plus api_key, echostr, signature_method and
                        timestamp (ms), sorted by name in code-point order and joined RAW (no percent-encoding) as k=v&...
        prepared      = MD5(signed string), upper-case hex
        sign          = HMAC-SHA256(secret, prepared), lower-case hex

    Every private endpoint is a POST with a form-urlencoded body: the request's parameters + api_key + sign.
    timestamp, signature_method and echostr travel as HEADERS, carrying the values that were signed (the Chinese docs:
    the three "需要和header中的保持一致", must match the headers). That is CCXT's live-tested form; the Python SDK's JSON
    body is not used. The connector hands every value over as a plain string, so what is signed is what is sent.

    Only HmacSHA256 keys are supported. LBank also issues RSA keys (the secret is then a private key); those would
    need the RSA signer and are not used.

    The timestamp is checked BEFORE the key (myserver 2026-10-07: a timestamp 90 s old with a made-up key answers
    10600, one 3 s ahead answers the key's 10005), so a request refused for its timestamp was not executed.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    @staticmethod
    def _echostr() -> str:
        # 30 to 40 letters and digits (docs; 10031 otherwise). The SDKs send 35.
        return "".join(random.choices(string.ascii_letters + string.digits, k=CONSTANTS.ECHOSTR_LENGTH))

    def sign(self, params: Dict[str, str]) -> str:
        """`params` is everything that is signed: the request's parameters, api_key and the three auth values."""
        raw = "&".join(f"{key}={params[key]}" for key in sorted(params))
        prepared = hashlib.md5(raw.encode("utf-8")).hexdigest().upper()
        return hmac.new(self._secret_key.encode("utf-8"), prepared.encode("utf-8"), hashlib.sha256).hexdigest()

    def signed_body(self, params: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, str], Dict[str, str]]:
        """(form body, auth headers) for one request. A `sign` already present (a request signed once) is dropped and
        signed again with a fresh timestamp."""
        body = {str(key): str(value) for key, value in (params or {}).items()
                if value is not None and key not in ("sign", "api_key")}
        body["api_key"] = self._api_key
        headers = {
            "timestamp": str(int(self._time_provider.time() * 1e3)),
            "signature_method": CONSTANTS.SIGNATURE_METHOD,
            "echostr": self._echostr(),
        }
        body["sign"] = self.sign(dict(body, **headers))
        return body, headers

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # The REST assistant JSON-encodes `data`; LBank reads a form body.
        params = request.data
        if isinstance(params, (str, bytes)):
            params = json.loads(params) if params else {}
        body, auth_headers = self.signed_body(params)
        request.data = urlencode(body)
        headers: Dict[str, str] = dict(request.headers or {})
        headers.update(auth_headers)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private socket is authorised by a subscribeKey taken over signed REST, not per message.
        return request
