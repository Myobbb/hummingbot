import asyncio
import logging
import re
import time
from typing import Optional, Dict, Any

import hummingbot.connector.exchange.bybit.bybit_constants as CONSTANTS
import hummingbot.connector.exchange.bybit.bybit_web_utils as web_utils
from hummingbot.connector.exchange.bybit.bybit_auth import BybitAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BybitAPIUserStreamDataSource(UserStreamTrackerDataSource):

    HEARTBEAT_TIME_INTERVAL = 30.0

    _bausds_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BybitAuth,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__()
        self._auth: BybitAuth = auth
        self._time_synchronizer = time_synchronizer
        self._last_recv_time: float = 0
        self._domain = domain
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_ws_message_sent_timestamp = 0
        # Reconnect/error handling state
        self._reconnect_attempts: int = 0
        self._pending_reconnect_notice: Optional[Dict[str, Any]] = None
        self._suppress_reconnect_logs: bool = False

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message
        :return: the timestamp of the last received message in seconds
        """
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue
        :param output: the queue to use to store the received messages
        """
        while True:
            try:
                ws: Optional[WSAssistant] = None
                ws = await self._connected_websocket_assistant(self._domain)
                await self._subscribe_channels(ws)
                # Reset attempts on successful connection + subscribe
                self._reconnect_attempts = 0
                self._suppress_reconnect_logs = False
                self._last_ws_message_sent_timestamp = self._time()
                while True:
                    try:
                        seconds_until_next_ping = (
                            CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL -
                            (self._time() - self._last_ws_message_sent_timestamp)
                        )
                        await asyncio.wait_for(
                            self._process_ws_messages(ws=ws, output=output), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        await self._ping_server(ws)
                        # Watchdog: if no frames received in > 2 heartbeats, force reconnect
                        if ws.last_recv_time and (self._time() - ws.last_recv_time) > (2.5 * CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL):
                            raise ConnectionError("Bybit private WS inactive for too long; reconnecting.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._reconnect_attempts += 1
                if self._is_transient_ws_close_exception(e) or self._is_pong_timeout_exception(e) or self._is_inactivity_reconnect_exception(e):
                    code = self._extract_close_code(e)
                    # Defer noisy logs; emit concise INFO after resubscribe
                    self._pending_reconnect_notice = {"code": code or "unknown", "t0": time.time()}
                    self._suppress_reconnect_logs = True
                    try:
                        if self._is_pong_timeout_exception(e):
                            self.logger().warning("Bybit private WS PONG not received within expected time. Reconnecting...")
                        elif self._is_inactivity_reconnect_exception(e):
                            self.logger().warning("Bybit private WS inactive; reconnecting...")
                        else:
                            self.logger().warning(
                                f"Bybit private WS transient close (code={code or 'unknown'}). Reconnecting...")
                    except Exception:
                        pass
                    backoff = self._backoff_seconds(transient=True)
                else:
                    self.logger().error(
                        "Unexpected error while listening to user stream. Will retry.",
                        exc_info=True,
                    )
                    backoff = self._backoff_seconds(transient=False)
            finally:
                # Make sure no background task is leaked.
                try:
                    if ws is not None:
                        await ws.disconnect()
                except Exception:
                    pass
                try:
                    await self._sleep(backoff if 'backoff' in locals() else 1.0)
                except Exception:
                    await self._sleep(1.0)

    async def _ping_server(self, ws: WSAssistant):
        ping_time = self._time()
        payload = {
            "op": "ping",
            # Per Bybit docs, include req_id (optional). We use ms timestamp as string.
            "req_id": str(int(ping_time * 1e3))
        }
        ping_request = WSJSONRequest(payload=payload)
        await ws.send(request=ping_request)
        self._last_ws_message_sent_timestamp = ping_time

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            payload = {
                "op": "subscribe",
                "args": [f"{CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME}"],
            }
            subscribe_orders_request = WSJSONRequest(payload)
            payload = {
                "op": "subscribe",
                "args": [f"{CONSTANTS.WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME}"],
            }
            subscribe_executions_request = WSJSONRequest(payload)
            payload = {
                "op": "subscribe",
                "args": [f"{CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME}"],
            }
            subscribe_wallet_request = WSJSONRequest(payload)

            await ws.send(subscribe_orders_request)
            await ws.send(subscribe_executions_request)
            await ws.send(subscribe_wallet_request)
            if self._pending_reconnect_notice is not None:
                try:
                    elapsed = max(0.0, self._time() - self._pending_reconnect_notice.get("t0", self._time()))
                    code = self._pending_reconnect_notice.get("code", "unknown")
                    self.logger().info(
                        f"Bybit private WS reconnected after transient close (code={code}) in {elapsed:.1f}s; "
                        f"subscribed to private channels"
                    )
                finally:
                    self._pending_reconnect_notice = None
                    self._suppress_reconnect_logs = False
            else:
                self.logger().info("Subscribed to private orders, executions and wallet channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private channels...",
                exc_info=True
            )
            raise

    async def _authenticate_connection(self, ws: WSAssistant):
        """
        Sends the authentication message.
        :param ws: the websocket assistant used to connect to the exchange
        """
        request: WSJSONRequest = WSJSONRequest(
            payload=self._auth.generate_ws_auth_message()
        )
        await ws.send(request)

    async def _process_ws_messages(self, ws: WSAssistant, output: asyncio.Queue):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if "op" in data:
                if data.get("op") == "auth":
                    await self._process_ws_auth_msg(data)
                elif data.get("op") == "subscribe":
                    if data.get("success") is False:
                        # Treat subscribe failure as hard failure to trigger reconnect
                        self.logger().error(f"Private subscribe failed: {data}")
                        raise ConnectionError(f"Subscribe failed: {data}")
                    else:
                        try:
                            acked = data.get("args") or data.get("topic") or "<unknown>"
                            if not self._suppress_reconnect_logs:
                                self.logger().info(f"Bybit private subscribe acknowledged: {acked}")
                        except Exception:
                            pass
                elif data.get("op") == "ping":
                    # Respond to server-initiated pings to keep the connection healthy
                    try:
                        # Private WS expects pong with args array per Bybit docs
                        pong_arg = None
                        if "ts" in data:
                            pong_arg = str(data["ts"])
                        elif "req_id" in data:
                            pong_arg = str(data["req_id"])
                        else:
                            pong_arg = str(int(self._time() * 1000))
                        pong_payload = {"op": "pong", "args": [pong_arg]}
                        await ws.send(WSJSONRequest(pong_payload))
                        self._last_ws_message_sent_timestamp = self._time()
                    except Exception:
                        pass
                elif data.get("op") == "pong":
                    # Ack received; nothing else required
                    pass
                continue
            topic = data.get("topic")
            channel = ""
            if topic == CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME:
                channel = CONSTANTS.PRIVATE_ORDER_CHANNEL
            elif topic == CONSTANTS.WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME:
                channel = CONSTANTS.PRIVATE_TRADE_CHANNEL
            elif topic == CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME:
                channel = CONSTANTS.PRIVATE_WALLET_CHANNEL
            else:
                output.put_nowait(data)
            if channel:
                data["channel"] = channel
                output.put_nowait(data)

    def _extract_close_code(self, exc: Exception) -> Optional[str]:
        try:
            text = str(exc)
        except Exception:
            return None
        match = re.search(r"Close code\s*=\s*(\d+)", text)
        return match.group(1) if match else None

    def _is_transient_ws_close_exception(self, exc: Exception) -> bool:
        code = self._extract_close_code(exc)
        # Treat common network/service resets as transient
        return code in {"1000", "1001", "1005", "1006", "1012", "1013"}

    def _is_pong_timeout_exception(self, exc: Exception) -> bool:
        try:
            text = str(exc)
        except Exception:
            return False
        # Raised by aiohttp heartbeat when protocol-level PONG is not received
        return "No PONG received" in text

    def _is_inactivity_reconnect_exception(self, exc: Exception) -> bool:
        try:
            text = str(exc)
        except Exception:
            return False
        return "inactive for too long" in text

    def _backoff_seconds(self, transient: bool) -> float:
        if transient:
            return 1.0
        # Exponential backoff for non-transient issues, capped at 30s
        exponent = min(self._reconnect_attempts, 5)
        return float(min(30, 2 ** max(1, exponent)))

    async def _process_ws_auth_msg(self, data: dict):
        if not data.get("success"):
            raise IOError(f"Private channel authentication failed - {data['ret_msg']}")
        else:
            self.logger().info("Private channel authentication success.")

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> WSAssistant:
        ws: WSAssistant = await self._get_ws_assistant()
        ws_url = f"{CONSTANTS.WSS_PRIVATE_URL[domain]}?max_active_time=5m"
        await ws.connect(
            ws_url=ws_url,
            # Disable protocol-level heartbeat; Bybit uses JSON ping/pong acks
            ping_timeout=None
        )
        await self._authenticate_connection(ws)
        return ws

    def _get_server_timestamp(self):
        return web_utils.get_current_server_time()

    def _time(self):
        return time.time()
