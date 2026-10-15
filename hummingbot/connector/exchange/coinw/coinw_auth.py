import hashlib
from typing import Any, Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoinwAuth(AuthBase):
    """
    CoinW spot request signing (docs, Common > Authentication & Code Snippet > Spot > RESTful Private Interface; the
    official Python and Java snippets agree):

        every request parameter + api_key, sorted by key, joined "k=v&" (raw values), then "secret_key=<secret>"
        sign = MD5(that string), upper-case hex

    The request is POST /api/v1/private?command=<command>&sign=<sign>&<the parameters>&api_key=<key> with an empty
    body. `command` names the route and is never signed (the docs keep it in the path, the query is identical either
    way). The signature carries no timestamp or nonce, so a signed URL is valid forever: nothing here or in the
    connector ever logs a URL or a query (CoinW's api_key travels in it too). Verified live 2026-10-15 from myserver
    with the AM's key (returnBalances, returnOpenOrders, returnOrderStatus, cancelOrder): a wrong secret, or a
    parameter signed but not sent, answers code "-105" "wrong signature".

    The private socket is not signed: its login sends the api key and the SECRET itself as the docs' "passphrase"
    ({"event":"login","params":{"api_key":…,"passphrase":<secret>}}, live 2026-10-15: {"data":{"result":true},
    "channel":"login"}). ws_login_payload() builds it; it is sent once per connection and never logged.
    """

    def __init__(self, api_key: str, secret_key: str) -> None:
        self._api_key = api_key
        self._secret_key = secret_key

    def sign(self, params: Dict[str, str]) -> str:
        payload = "".join(f"{key}={params[key]}&" for key in sorted(params)) + "secret_key=" + self._secret_key
        return hashlib.md5(payload.encode("utf-8")).hexdigest().upper()

    def signed_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        """The query to send: `command` first (unsigned), the parameters, api_key, sign."""
        values = {str(key): str(value) for key, value in (params or {}).items() if value is not None}
        command = values.pop("command", None)
        values["api_key"] = self._api_key
        signature = self.sign(values)
        query = {"command": command} if command is not None else {}
        query.update(values)
        query["sign"] = signature
        return query

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        request.params = self.signed_params(dict(request.params or {}))
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The private stream is authorised by the login event on the socket (ws_login_payload), not per request.
        return request

    def ws_login_payload(self) -> Dict[str, Any]:
        return {"event": "login", "params": {"api_key": self._api_key, "passphrase": self._secret_key}}
