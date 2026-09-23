import hashlib
import hmac
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class XtAuth(AuthBase):
    """
    XT v4 request signing (Access Description/SignatureGeneration).

        X = the four validate-* headers, sorted by name, joined as k=v with '&'
        Y = '#' + '#'.join(non-empty parts of: METHOD, path, query, body)
        validate-signature = hex(HMAC-SHA256(secret, X + Y))

    query  is every parameter sorted by key and joined as k=v with '&'. The request's params are
           re-ordered to that same order here, so what goes on the wire is what was signed.
    body   is the raw JSON string exactly as sent. RESTAssistant serialises `data` with json.dumps
           before auth runs, and that string is both signed and transmitted, so they cannot differ
           (the SDK signs json.dumps output the same way).

    Neither of the docs' two worked examples reproduces with the published demo secret (they mix
    keys across examples), so the proof is live: a signed GET /v4/balances returned rc=0 with a
    read-only key on 2026-09-23, and a zeroed signature returned AUTH_103.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._time_provider = time_provider

    @staticmethod
    def _query_string(params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return ""
        return "&".join(f"{key}={params[key]}" for key in sorted(params))

    def _signature(self, payload: str) -> str:
        return hmac.new(self._secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def signed_headers(self, method: str, path: str, query: str = "", body: str = "") -> Dict[str, str]:
        prefix = CONSTANTS.AUTH_HEADER_PREFIX
        headers = {
            f"{prefix}algorithms": CONSTANTS.AUTH_ALGORITHM,
            f"{prefix}appkey": self._api_key,
            f"{prefix}recvwindow": CONSTANTS.AUTH_RECV_WINDOW_MS,
            f"{prefix}timestamp": str(int(self._time_provider.time() * 1e3)),
        }
        x = "&".join(f"{name}={headers[name]}" for name in sorted(headers))
        y = "#" + "#".join(part for part in (method.upper(), path, query, body) if part)
        headers[f"{prefix}signature"] = self._signature(x + y)
        return headers

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if request.params:
            request.params = {str(key): request.params[key] for key in sorted(request.params)}
        body = request.data if isinstance(request.data, str) else ""
        headers: Dict[str, str] = dict(request.headers or {})
        headers.update(self.signed_headers(
            method=request.method.value,
            path=urlsplit(request.url).path,
            query=self._query_string(request.params),
            body=body,
        ))
        # The docs ask for application/json on every call; RESTAssistant defaults GETs to form type.
        headers["Content-Type"] = "application/json"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private stream is authorised by the listenKey carried inside each subscribe message.
        return request
