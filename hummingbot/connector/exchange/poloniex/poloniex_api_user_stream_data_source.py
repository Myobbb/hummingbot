import asyncio
import time
from typing import TYPE_CHECKING, Any, Optional

from hummingbot.connector.exchange.poloniex import poloniex_constants as CONSTANTS, poloniex_web_utils as web_utils
from hummingbot.connector.exchange.poloniex.poloniex_auth import PoloniexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.poloniex.poloniex_exchange import PoloniexExchange


class PoloniexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Poloniex private stream (wss://ws.poloniex.com/ws/private): channels `orders` and `balances`.

    Authorisation is an `auth` event on the socket (PoloniexAuth.ws_auth_payload). Its answer is awaited before
    anything is subscribed: subscribes sent with the auth event were answered "user must be authenticated!" (live
    2026-10-09; the ack came 5 ms later). A refused auth, or a subscribe error, raises into the reconnect backoff:
    without the private channels, fills and order updates would silently stop.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: PoloniexAuth,
        connector: "PoloniexExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ping_task: Optional[asyncio.Task] = None
        self._last_pong: float = 0.0
        self._backoff = web_utils.PoloniexReconnectBackoff()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_PRIVATE_URL)

    async def _authenticate(self, ws: WSAssistant) -> None:
        await ws.send(WSJSONRequest(payload=self._auth.ws_auth_payload()))
        deadline = time.monotonic() + CONSTANTS.WS_AUTH_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConnectionError(f"Poloniex private auth: no answer within {CONSTANTS.WS_AUTH_TIMEOUT} s")
            try:
                response = await asyncio.wait_for(ws.receive(), timeout=remaining)
            except asyncio.TimeoutError:
                raise ConnectionError(f"Poloniex private auth: no answer within {CONSTANTS.WS_AUTH_TIMEOUT} s") from None
            data: Any = response.data if response is not None else None
            if not isinstance(data, dict):
                continue
            if data.get("channel") == CONSTANTS.WS_AUTH_CHANNEL:
                if (data.get("data") or {}).get("success") is True:
                    return
                raise ConnectionError(f"Poloniex private auth refused: {data}")
            if str(data.get("event") or "").lower() == "error":
                raise ConnectionError(f"Poloniex private auth refused: {data}")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            await self._authenticate(websocket_assistant)
            await websocket_assistant.send(WSJSONRequest(payload={
                "event": "subscribe", "channel": [CONSTANTS.WS_ORDERS_CHANNEL], "symbols": ["all"]}))
            await websocket_assistant.send(WSJSONRequest(payload={
                "event": "subscribe", "channel": [CONSTANTS.WS_BALANCES_CHANNEL]}))
            self.logger().info("Authenticated and subscribed to the Poloniex private orders and balances channels.")
            self._backoff.connected()
            self._last_pong = time.monotonic()
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(websocket_assistant))
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to the Poloniex private channels...")
            raise

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """{"event":"ping"} every WS_HEARTBEAT_INTERVAL; no pong within WS_PONG_TIMEOUT ends the connection."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                sent = time.monotonic()
                await ws.send(WSJSONRequest(payload={"event": "ping"}))
                await asyncio.sleep(CONSTANTS.WS_PONG_TIMEOUT)
                if self._last_pong < sent:
                    self.logger().warning(f"Poloniex private WS: no pong within {CONSTANTS.WS_PONG_TIMEOUT} s; "
                                          f"reconnecting.")
                    await ws.disconnect()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Poloniex private WS keepalive stopped: {e}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            data: Any = ws_response.data
            if not isinstance(data, dict):
                continue
            event = str(data.get("event") or "").lower()
            if event == "pong":
                self._last_pong = time.monotonic()
            elif event == "error":
                # A subscribe refused after a good auth: the private channels are not delivering. Reconnect.
                raise ConnectionError(f"Poloniex private stream error ({data}); reconnecting")
            elif event:
                continue  # subscribe acks
            elif data.get("channel") in (CONSTANTS.WS_ORDERS_CHANNEL, CONSTANTS.WS_BALANCES_CHANNEL):
                queue.put_nowait(data)
            else:
                self.logger().debug(f"Unrecognised Poloniex private WS message: {data}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        await self._backoff.wait()  # the listen loop reconnects right after this
