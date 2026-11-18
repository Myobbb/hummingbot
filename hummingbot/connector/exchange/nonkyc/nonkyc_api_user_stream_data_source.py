import asyncio
import time
from typing import TYPE_CHECKING, Any, Optional

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS, nonkyc_web_utils as web_utils
from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange


class NonkycAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    NonKYC private stream (wss://ws.nonkyc.io, WS API 1.1.0 "Socket Session Authentication"):

        login              {algo: HS256, pKey, nonce, signature = HMAC-SHA256(secret, nonce)}
        subscribeReports   -> an `activeOrders` frame (a snapshot of the open orders), then `report` frames
        subscribeBalances  -> a `currentBalances` frame (every non-zero asset), then `balanceUpdate` frames

    The login's answer is awaited before anything is subscribed: a subscribe before the login would be refused, and
    a refused login must raise into the reconnect backoff rather than leave a silent socket. A nonce may be used once
    per key (code 10001), so each attempt signs a fresh one.

    No application-level ping: the venue sends a WS PING every ~60 s and aiohttp answers it (autoping), while its own
    heartbeat closes a socket whose PONG stops arriving -- the liveness signal for a stream whose quiet periods are
    normal.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: NonkycAuth,
        connector: "NonkycExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._backoff = web_utils.NonkycReconnectBackoff()
        self._next_request_id = 0

    def _request_id(self) -> int:
        self._next_request_id += 1
        return self._next_request_id

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _authenticate(self, ws: WSAssistant) -> None:
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_LOGIN,
            "params": self._auth.ws_login_params(),
            "id": self._request_id(),
        }))
        deadline = time.monotonic() + CONSTANTS.WS_AUTH_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConnectionError(f"NonKYC private login: no answer within {CONSTANTS.WS_AUTH_TIMEOUT} s")
            try:
                response = await asyncio.wait_for(ws.receive(), timeout=remaining)
            except asyncio.TimeoutError:
                raise ConnectionError(f"NonKYC private login: no answer within {CONSTANTS.WS_AUTH_TIMEOUT} s") from None
            data: Any = response.data if response is not None else None
            if not isinstance(data, dict):
                continue
            error = data.get("error")
            if isinstance(error, dict):
                raise ConnectionError(f"NonKYC private login refused: {error}")
            if data.get("result") is True:
                return
            # Anything else on this socket before the login's answer is a push we cannot use yet: ignored.

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            await self._authenticate(websocket_assistant)
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_SUBSCRIBE_REPORTS, "params": {}, "id": self._request_id()}))
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_SUBSCRIBE_BALANCES, "params": {}, "id": self._request_id()}))
            self.logger().log(self._subscribed_log_level(),
                              "Authenticated and subscribed to the NonKYC private order, trade and balance channels.")
            self._backoff.connected()
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to the NonKYC private channels...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            data: Any = ws_response.data
            if not isinstance(data, dict):
                continue
            method = data.get("method")
            if method in (CONSTANTS.WS_FRAME_REPORT, CONSTANTS.WS_FRAME_ACTIVE_ORDERS,
                          CONSTANTS.WS_FRAME_CURRENT_BALANCES, CONSTANTS.WS_FRAME_BALANCE_UPDATE):
                queue.put_nowait(data)
                continue
            error = data.get("error")
            if isinstance(error, dict):
                # A refused subscribe after a good login: the private channels are not delivering. Reconnect.
                raise ConnectionError(f"NonKYC private stream error ({error}); reconnecting")
            if data.get("result") is True or "result" in data or "id" in data:
                continue  # an ack
            self.logger().debug(f"Unrecognised NonKYC private WS message: {data}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        await self._backoff.wait()  # the listen loop reconnects right after this
