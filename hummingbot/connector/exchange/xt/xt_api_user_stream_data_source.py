import asyncio
import time
from typing import TYPE_CHECKING, Any, Optional

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_web_utils as web_utils
from hummingbot.connector.exchange.xt.xt_auth import XtAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.xt.xt_exchange import XtExchange


class XtAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    XT private stream (wss://stream.xt.com/private): topics `balance`, `order`, `trade`.

    The stream is authorised by a listenKey from POST /v4/ws-token (1 call / 10 s / apikey, valid
    2 days, and every call resets the validity). It rides inside each subscribe message rather than
    in a login frame. Live 2026-09-23: the ack is {"id","code":0,"msg":"SUCCESS","method"} and a bad
    key answers {"code":1,"msg":"token error"}.

    A fresh key is fetched for every connection, and re-requested every WS_TOKEN_REFRESH_SECONDS while
    the socket stays up, so the 2-day expiry is never reached.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: XtAuth,
        connector: "XtExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._listen_key: Optional[str] = None
        self._listen_key_ts: float = 0.0
        self._ping_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0

    def _request_id(self) -> str:
        self._next_request_id += 1
        return f"u{self._next_request_id}"

    async def _get_listen_key(self) -> str:
        response = await self._connector._api_post(
            path_url=CONSTANTS.WS_TOKEN_PATH,
            is_auth_required=True,
            limit_id=CONSTANTS.WS_TOKEN_PATH,
        )
        if web_utils.is_error_response(response):
            raise IOError(f"XT ws-token request failed: {web_utils.error_code(response)} ({response})")
        token = (response.get("result") or {}).get("accessToken")
        if not token:
            raise IOError("XT ws-token response carried no accessToken")
        self._listen_key = token
        self._listen_key_ts = time.time()
        return token

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._get_listen_key()
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_PRIVATE_URL)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            # The ack is checked in _process_websocket_messages rather than awaited here, so a push
            # that happens to land before it is never consumed and lost.
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_SUBSCRIBE,
                "params": [CONSTANTS.WS_PRIVATE_TOPIC_BALANCE,
                           CONSTANTS.WS_PRIVATE_TOPIC_ORDER,
                           CONSTANTS.WS_PRIVATE_TOPIC_TRADE],
                "listenKey": self._listen_key,
                "id": self._request_id(),
            }))
            self.logger().info("Subscribed to XT private balance, order and trade channels.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(websocket_assistant))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to XT private channels...")
            raise

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        # XT's keepalive is the text "ping" (answered by a text "pong", which refreshes last_recv_time).
        await websocket_assistant.send(WSPlainTextRequest(payload="ping"))

    async def _ping_loop(self, ws: WSAssistant) -> None:
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                await self._send_ping(ws)
                if time.time() - self._listen_key_ts > CONSTANTS.WS_TOKEN_REFRESH_SECONDS:
                    try:
                        await self._get_listen_key()   # resets the key's validity; the socket stays up
                    except Exception as e:
                        self.logger().warning(f"XT listenKey refresh failed ({e}); will retry next cycle.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"XT private WS keepalive stopped: {e}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            data: Any = ws_response.data
            if not isinstance(data, dict):
                continue  # the text "pong" reply
            topic = data.get("topic")
            if topic in (CONSTANTS.WS_PRIVATE_TOPIC_BALANCE,
                         CONSTANTS.WS_PRIVATE_TOPIC_ORDER,
                         CONSTANTS.WS_PRIVATE_TOPIC_TRADE):
                queue.put_nowait(data)
            elif "code" in data and topic is None:
                # Subscribe ack. Any failure is fatal for this connection: without the private topics
                # fills and order updates would silently stop. A fresh listenKey is used next time.
                if data.get("code") != 0:
                    self._listen_key = None
                    raise ConnectionError(f"XT private subscribe rejected ({data}); reconnecting with a new listenKey")
            else:
                self.logger().debug(f"Unrecognised XT private WS message: {data}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
