import hashlib
import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class GrovexAuth(AuthBase):
    """
    GroveX request signing (ChainUp "open/api" v1: docs "Signature" and demo.txt; verified live 2026-10-09):

        signed string = every NON-EMPTY parameter of the request except `sign`, api_key and time (ms) included, sorted
                        by name in code-point order, each written key + value with NO separator
        sign          = md5(signed string + secret), LOWER-case hex (upper-case is refused: 100005)

    api_key, time and sign travel as PARAMETERS, never headers: in the query string of a GET (every read; a GET's form
    body is ignored -> 100004), in the form body of a POST (every write; GroveX also reads a POST's query, live). The
    REST assistant JSON-encodes a POST's `data`: it is turned back into a dict and sent form-encoded, exactly the values
    signed. An empty value is neither signed nor sent.

    The timestamp: older than ~60 s is refused (100008) before anything executes; there is no upper bound (live).
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    def sign(self, params: Dict[str, str]) -> str:
        raw = "".join(f"{key}{params[key]}" for key in sorted(params))
        return hashlib.md5((raw + self._secret_key).encode("utf-8")).hexdigest()

    def signed_params(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """The request's parameters + api_key + time + sign. A request signed once (a sign already present) is signed
        again with a fresh time."""
        signed = {str(key): str(value) for key, value in (params or {}).items()
                  if value is not None and str(value) != "" and key not in ("sign", "api_key", "time")}
        signed["api_key"] = self._api_key
        signed["time"] = str(int(self._time_provider.time() * 1e3))
        signed["sign"] = self.sign(signed)
        return signed

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers: Dict[str, str] = dict(request.headers or {})
        if request.method == RESTMethod.GET:
            request.params = self.signed_params(request.params)
            request.data = None
        else:
            body = request.data
            if isinstance(body, (bytes, bytearray)):
                body = body.decode("utf-8")
            if isinstance(body, str):
                body = json.loads(body) if body else {}
            merged: Dict[str, Any] = dict(request.params or {})
            merged.update(body or {})
            request.params = None
            request.data = urlencode(self.signed_params(merged))
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The only socket is the public market socket: nothing to sign.
        return request
