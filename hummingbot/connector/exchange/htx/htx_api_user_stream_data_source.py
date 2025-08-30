import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

import hummingbot.connector.exchange.htx.htx_constants as CONSTANTS
from hummingbot.connector.exchange.htx.htx_auth import HtxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.htx.htx_exchange import HtxExchange


class HtxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, htx_auth: HtxAuth,
                 trading_pairs: List[str],
                 connector: 'HtxExchange',
                 api_factory: Optional[WebAssistantsFactory]):
        self._auth: HtxAuth = htx_auth
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs = trading_pairs
        self._ping_task: Optional[asyncio.Task] = None
        super().__init__()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # Prefer AWS private WS endpoint. Disable underlying WS heartbeat to avoid double keepalive with JSON ping.
        await ws.connect(ws_url=CONSTANTS.WS_PRIVATE_URL, ping_timeout=None)
        return ws

    async def _authenticate_client(self, ws: WSAssistant):
        """
        Sends an Authentication request to Htx's WebSocket API Server
        """
        try:
            ws_request: WSJSONRequest = WSJSONRequest(
                {
                    "action": "req",
                    "ch": "auth",
                    "params": {},
                }
            )
            auth_params = self._auth.generate_auth_params_for_WS(ws_request)
            ws_request.payload['params'] = auth_params
            await ws.send(ws_request)
            resp: WSResponse = await ws.receive()
            auth_response = resp.data
            if auth_response.get("code", 0) != 200:
                raise ValueError(f"User Stream Authentication Fail! {auth_response}")
            self.logger().info("Successfully authenticated to user stream...")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error occurred authenticating websocket connection... Error: {str(e)}", exc_info=True)
            raise

    async def _subscribe_topic(self, topic: str, websocket_assistant: WSAssistant):
        """
        Specifies which event channel to subscribe to

        :param topic: the event type to subscribe to

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            subscribe_request: WSJSONRequest = WSJSONRequest({"action": "sub", "ch": topic})
            await websocket_assistant.send(subscribe_request)
            self.logger().info(f"Subscribed to {topic}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(f"Cannot subscribe to user stream topic: {topic}")
            raise

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order events, balance events and account events

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            await self._authenticate_client(websocket_assistant)
            await self._subscribe_topic(CONSTANTS.HTX_ACCOUNT_UPDATE_TOPIC, websocket_assistant)
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                await self._subscribe_topic(CONSTANTS.HTX_TRADE_DETAILS_TOPIC.format(exchange_symbol),
                                            websocket_assistant)
                await self._subscribe_topic(CONSTANTS.HTX_ORDER_UPDATE_TOPIC.format(exchange_symbol),
                                            websocket_assistant)
            # Start JSON-level ping keepalive
            if self._ping_task is None or self._ping_task.done():
                self._ping_task = asyncio.create_task(self._json_ping_loop(websocket_assistant))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to private user streams...", exc_info=True)
            raise

    async def _json_ping_loop(self, websocket_assistant: WSAssistant):
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                try:
                    ts = int(time.time() * 1000)
                    ping_req: WSJSONRequest = WSJSONRequest(payload={"action": "ping", "data": {"ts": ts}})
                    await websocket_assistant.send(request=ping_req)
                except Exception:
                    break
        except asyncio.CancelledError:
            return

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            action = data.get("action")
            if action == "ping":
                payload_data = data.get("data")
                if isinstance(payload_data, dict) and "ts" in payload_data:
                    pong_payload = {"action": "pong", "data": {"ts": payload_data.get("ts")}}
                else:
                    pong_payload = {"action": "pong"}
                await websocket_assistant.send(request=WSJSONRequest(payload=pong_payload))
            elif action == "pong":
                # No-op
                continue
            elif action == "sub":
                if data.get("code") != 200:
                    raise ValueError(f"Error subscribing to topic: {data.get('ch')} ({data})")
            else:
                queue.put_nowait(data)
