import hashlib
import hmac
import json
from collections import OrderedDict
from typing import Any, Dict
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class MexcAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        
        NOTE: For Unicode trading pairs (e.g., Chinese characters), we must handle encoding 
        carefully. yarl (used by aiohttp) does NOT percent-encode Unicode chars the same way
        as Python's urlencode. To ensure signature matches, we append the pre-encoded query 
        string directly to the URL.
        """
        if request.method == RESTMethod.POST:
            if request.data is not None:
                # POST with data (e.g., orders)
                params = json.loads(request.data)
            else:
                # POST without data (e.g., userDataStream)
                params = request.params or {}
            authenticated_params = self.add_auth_to_params(params=params)
            # Build pre-encoded query string and append to URL
            encoded_query = urlencode(authenticated_params)
            separator = "&" if "?" in request.url else "?"
            request.url = f"{request.url}{separator}{encoded_query}"
            request.params = None  # Clear params so yarl doesn't re-encode
            request.data = None    # Clear body
        else:
            # GET/DELETE: also use pre-encoded URL
            existing_params = request.params or {}
            authenticated_params = self.add_auth_to_params(params=existing_params)
            encoded_query = urlencode(authenticated_params)
            separator = "&" if "?" in request.url else "?"
            request.url = f"{request.url}{separator}{encoded_query}"
            request.params = None  # Clear params so yarl doesn't re-encode

        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication())
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Mexc does not use this
        functionality
        """
        return request  # pass-through

    def add_auth_to_params(self,
                           params: Dict[str, Any]):
        timestamp = int(self.time_provider.time() * 1e3)

        request_params = OrderedDict(params or {})
        request_params["timestamp"] = timestamp

        signature = self._generate_signature(params=request_params)
        request_params["signature"] = signature

        return request_params

    def header_for_authentication(self) -> Dict[str, str]:
        return {"X-MEXC-APIKEY": self.api_key}

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        encoded_params_str = urlencode(params)
        digest = hmac.new(self.secret_key.encode("utf8"), encoded_params_str.encode("utf8"), hashlib.sha256).hexdigest()
        return digest
