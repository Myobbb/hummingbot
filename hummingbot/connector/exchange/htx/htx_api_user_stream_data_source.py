import asyncio
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
        self._last_ws_message_sent_timestamp = 0
        self._ping_interval = CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        # Liveness tracking
        self._last_pong: float = 0.0
        self._last_frame_ts: float = 0.0
        self._last_inactivity_warn_ts: float = 0.0
        super().__init__()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # Prefer AWS private WS endpoint. Disable protocol heartbeat (use JSON ping/pong). Throttle and tune sizes.
        throttler = getattr(self._api_factory, "throttler", None)
        if throttler is not None:
            async with throttler.execute_task(CONSTANTS.WS_CONNECTION_LIMIT_ID):
                await ws.connect(
                    ws_url=CONSTANTS.WS_PRIVATE_URL,
                    ping_timeout=None,
                    message_timeout=45,
                    ws_headers={"Accept-Encoding": "gzip"},
                    max_msg_size=16 * 1024 * 1024,
                )
        else:
            await ws.connect(
                ws_url=CONSTANTS.WS_PRIVATE_URL,
                ping_timeout=None,
                message_timeout=45,
                ws_headers={"Accept-Encoding": "gzip"},
                max_msg_size=16 * 1024 * 1024,
            )
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
            throttler = getattr(self._api_factory, "throttler", None)
            if throttler is not None:
                async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                    await ws.send(ws_request)
            else:
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
            throttler = getattr(self._api_factory, "throttler", None)
            if throttler is not None:
                async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                    await websocket_assistant.send(subscribe_request)
            else:
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
            # Prefer precise accountId for balances when available
            account_topic = CONSTANTS.HTX_ACCOUNT_UPDATE_TOPIC
            try:
                account_id = getattr(self._connector, "_account_id", "")
                # Only trust account id provided by config/env. If not explicitly set, use default #2.
                if account_id and getattr(self._connector, "_account_id_from_config", False):
                    account_topic = f"accounts.update#{account_id}"
            except Exception:
                account_topic = CONSTANTS.HTX_ACCOUNT_UPDATE_TOPIC
            await self._subscribe_topic(account_topic, websocket_assistant)
            await asyncio.sleep(0.2)
            await self._subscribe_topic("trade.clearing#*", websocket_assistant)
            await asyncio.sleep(0.2)
            await self._subscribe_topic("orders#*", websocket_assistant)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to private user streams...", exc_info=True)
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            try:
                seconds_until_next_ping = max(1.0, self._ping_interval - (self._time() - self._last_ws_message_sent_timestamp))
                ws_response: WSResponse = await asyncio.wait_for(websocket_assistant.receive(), timeout=seconds_until_next_ping)
                if ws_response is None:
                    # Connection was closed; let outer loop reconnect
                    break
                data = ws_response.data
                # Update last-frame timestamp on any received frame
                self._last_frame_ts = self._time()
                action = data.get("action")
                if action == "ping":
                    payload_data = data.get("data")
                    if isinstance(payload_data, dict) and "ts" in payload_data:
                        pong_payload = {"action": "pong", "data": {"ts": payload_data.get("ts")}}
                    else:
                        pong_payload = {"action": "pong"}
                    throttler = getattr(self._api_factory, "throttler", None)
                    if throttler is not None:
                        async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                            await websocket_assistant.send(request=WSJSONRequest(payload=pong_payload))
                    else:
                        await websocket_assistant.send(request=WSJSONRequest(payload=pong_payload))
                elif action == "pong":
                    # Record last pong time
                    self._last_pong = self._time()
                    continue
                elif action == "sub":
                    if data.get("code") != 200:
                        ch = data.get("ch", "")
                        msg = data.get("message", "")
                        # If account-specific balances topic is invalid, fallback to default #2 once
                        try:
                            if ch.startswith("accounts.update#"):
                                fallback = CONSTANTS.HTX_ACCOUNT_UPDATE_TOPIC
                                self.logger().warning(
                                    f"Balances topic rejected: {ch} (code={data.get('code')}, msg={msg}); "
                                    f"falling back to {fallback}"
                                )
                                await self._subscribe_topic(fallback, websocket_assistant)
                                continue
                        except Exception:
                            pass
                        raise ValueError(f"Error subscribing to topic: {data.get('ch')} ({data})")
                else:
                    queue.put_nowait(data)
            except asyncio.TimeoutError:
                # Send JSON ping proactively
                ping_payload = {"action": "ping", "data": {"ts": int(self._time() * 1000)}}
                throttler = getattr(self._api_factory, "throttler", None)
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await websocket_assistant.send(request=WSJSONRequest(payload=ping_payload))
                else:
                    await websocket_assistant.send(request=WSJSONRequest(payload=ping_payload))
                self._last_ws_message_sent_timestamp = self._time()
                now = self._time()
                # Health checks: pong freshness and inactivity
                try:
                    if self._last_pong and (now - self._last_pong) > 45:
                        await websocket_assistant.disconnect()
                        break
                except Exception:
                    pass
                try:
                    if self._last_frame_ts and (now - self._last_frame_ts) > 120:
                        if (now - self._last_inactivity_warn_ts) > 60:
                            self._last_inactivity_warn_ts = now
                            self.logger().warning("No private WS frames received recently", extra={"idle_seconds": int(now - self._last_frame_ts)})
                        if (now - self._last_frame_ts) > 180:
                            await websocket_assistant.disconnect()
                            break
                except Exception:
                    pass
                # Fallback health check based on underlying connection recv time
                if (now - websocket_assistant.last_recv_time) > 45:
                    await websocket_assistant.disconnect()
                    break
            except ConnectionError:
                # Underlying WS closed (e.g., normal or transient closure). Exit cleanly and let outer loop reconnect.
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                # Any other error - break to trigger reconnect by caller
                raise

    async def _send_ping(self, websocket_assistant: WSAssistant):
        # Override to use JSON ping instead of protocol-level ping frames
        ping_payload = {"action": "ping", "data": {"ts": int(self._time() * 1000)}}
        throttler = getattr(self._api_factory, "throttler", None)
        if throttler is not None:
            async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                await websocket_assistant.send(request=WSJSONRequest(payload=ping_payload))
        else:
            await websocket_assistant.send(request=WSJSONRequest(payload=ping_payload))
        self._last_ws_message_sent_timestamp = self._time()
