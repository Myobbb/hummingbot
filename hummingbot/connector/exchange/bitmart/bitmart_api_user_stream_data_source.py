import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitmart import bitmart_constants as CONSTANTS, bitmart_utils as utils
from hummingbot.connector.exchange.bitmart.bitmart_auth import BitmartAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitmart.bitmart_exchange import BitmartExchange


class BitmartAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None
    _PING_INTERVAL_SECONDS: float = 15.0  # < 20s per BitMart docs
    _FORCE_RECONNECT_IDLE_SECONDS: float = 30.0  # Increased margin beyond BitMart's 20s threshold
    _DATA_STALENESS_SECONDS: float = 60.0  # Watchdog for actual user data (not just pongs)

    def __init__(
        self,
        auth: BitmartAuth,
        trading_pairs: List[str],
        connector: 'BitmartExchange',
        api_factory: WebAssistantsFactory
    ):
        super().__init__()
        self._auth: BitmartAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._keepalive_task: Optional[asyncio.Task] = None
        self._data_watchdog_task: Optional[asyncio.Task] = None
        self._reconnect_attempts: int = 0
        self._last_ping_sent_time: float = 0.0
        self._last_data_received_time: float = 0.0  # Track actual user data, not just pongs

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """

        ws: WSAssistant = await self._get_ws_assistant()
        # Disable protocol-level ping frames; BitMart requires text 'ping'
        await ws.connect(
            ws_url=CONSTANTS.WSS_PRIVATE_URL,
            ping_timeout=None,
            message_timeout=60,
            ws_headers={"Accept-Encoding": "gzip"},  #not really needed, but it's here for completeness
        )

        payload = {
            "op": "login",
            "args": self._auth.websocket_login_parameters()
        }

        login_request: WSJSONRequest = WSJSONRequest(payload=payload)

        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
            await ws.send(login_request)

        response: WSResponse = await ws.receive()
        message = response.data
        if "errorCode" in message or "error_code" in message or message.get("event") != "login":
            self.logger().error("Error authenticating the private websocket connection")
            raise IOError(f"Private websocket connection authentication failed ({message})")

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            # Subscribe to private order progress for ALL symbols (simplifies when tracking many pairs)
            order_topics = [f"{CONSTANTS.PRIVATE_ORDER_PROGRESS_ALL_CHANNEL_NAME}:ALL_SYMBOLS"]
            balance_topic = [CONSTANTS.PRIVATE_BALANCE_CHANNEL_NAME + ":BALANCE_UPDATE"]

            async def send_chunked(topics: List[str]):
                CHUNK_SIZE = 20
                for i in range(0, len(topics), CHUNK_SIZE):
                    chunk = topics[i:i + CHUNK_SIZE]
                    payload = {"op": "subscribe", "args": chunk}
                    subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)
                    async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                        await websocket_assistant.send(subscribe_request)

            await send_chunked(order_topics)
            # Send balance subscription separately
            await send_chunked(balance_topic)
            self.logger().info("Subscribed to private orders (ALL_SYMBOLS) and balance channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        # Start keepalive task
        self._keepalive_task = asyncio.create_task(self._keepalive_ping_loop(websocket_assistant))
        # Start data watchdog task
        self._data_watchdog_task = asyncio.create_task(self._data_watchdog_loop(websocket_assistant))
        try:
            async for ws_response in websocket_assistant.iter_messages():
                data: Dict[str, Any] = ws_response.data
                decompressed_data = utils.decompress_ws_message(data)
                try:
                    if type(decompressed_data) == str:
                        # Ignore raw 'pong' frames
                        if decompressed_data.strip().lower() == "pong":
                            continue
                        json_data = json.loads(decompressed_data)
                    else:
                        json_data = decompressed_data
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Ignore unparsable frames (e.g., plain text pong)
                    continue

                if isinstance(json_data, dict) and ("errorCode" in json_data or "errorMessage" in json_data):
                    # Escalate to reconnect
                    self.logger().error(f"BitMart private WS error: {json_data}")
                    raise ConnectionError(f"BitMart private WS error: {json_data}")

                await self._process_event_message(event_message=json_data, queue=queue)
        finally:
            if self._keepalive_task is not None and not self._keepalive_task.done():
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except Exception:
                    pass
            self._keepalive_task = None
            if self._data_watchdog_task is not None and not self._data_watchdog_task.done():
                self._data_watchdog_task.cancel()
                try:
                    await self._data_watchdog_task
                except Exception:
                    pass
            self._data_watchdog_task = None

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if len(event_message) > 0 and "table" in event_message and "data" in event_message:
            # Update timestamp when actual user data is received
            self._last_data_received_time = time.time()
            queue.put_nowait(event_message)

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _keepalive_ping_loop(self, ws: WSAssistant):
        try:
            while True:
                await asyncio.sleep(1.0)
                last_recv = getattr(ws, "last_recv_time", 0) or 0
                now = time.time()
                # Only send at most one ping per interval when idle
                if (now - last_recv) >= self._PING_INTERVAL_SECONDS and (now - self._last_ping_sent_time) >= self._PING_INTERVAL_SECONDS:
                    try:
                        await ws.send(WSPlainTextRequest(payload="ping"))
                        self._last_ping_sent_time = now
                        self.logger().debug("BitMart private WS: sent ping")
                    except Exception:
                        # Force reconnect
                        raise
                # Force reconnect on prolonged idle (no messages at all, including pongs)
                if (now - last_recv) >= self._FORCE_RECONNECT_IDLE_SECONDS:
                    self.logger().warning("BitMart private WS: no messages for 30s, forcing reconnect")
                    raise ConnectionError("BitMart private WS idle exceeded threshold; forcing reconnect")
        except asyncio.CancelledError:
            raise
        except Exception:
            raise

    async def _data_watchdog_loop(self, ws: WSAssistant):
        """
        Detect when actual user data (orders/balance) stops flowing even though connection appears alive.
        This can happen if BitMart's server stops sending data without closing the connection.
        """
        try:
            while True:
                await asyncio.sleep(10.0)
                now = time.time()
                # Initialize on first check
                if self._last_data_received_time == 0:
                    self._last_data_received_time = now
                    continue
                
                # Check if we've received actual user data recently
                time_since_data = now - self._last_data_received_time
                if time_since_data >= self._DATA_STALENESS_SECONDS:
                    self.logger().warning(
                        f"BitMart private WS: no user data for {time_since_data:.0f}s "
                        f"(threshold: {self._DATA_STALENESS_SECONDS}s), forcing reconnect"
                    )
                    raise ConnectionError("BitMart private WS data stream stale; forcing reconnect")
        except asyncio.CancelledError:
            raise
        except Exception:
            raise

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Override to add graceful transient reconnect/backoff and text-ping keepalive.
        """
        while True:
            reconnect_delay = 1.0
            try:
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(websocket_assistant=self._ws_assistant)
                # Initialize data timestamp on new connection
                self._last_data_received_time = time.time()
                # initial text ping to mark activity
                try:
                    await self._ws_assistant.send(WSPlainTextRequest(payload="ping"))
                except Exception:
                    pass
                await self._process_websocket_messages(websocket_assistant=self._ws_assistant, queue=output)
            except asyncio.CancelledError:
                raise
            except (ConnectionError, Exception) as e:
                text = str(e)
                code = None
                try:
                    if "Close code" in text:
                        code = text.split("Close code =")[1].split()[0]
                except Exception:
                    pass
                is_transient = any(tok in text for tok in ["1000", "1001", "1005", "1006", "1012", "1013"])
                if is_transient:
                    self.logger().warning(f"BitMart private WS transient close ({code or 'unknown'}). Reconnecting...")
                    reconnect_delay = 1.0
                else:
                    self.logger().error("BitMart private WS error; reconnecting...", exc_info=True)
                    self._reconnect_attempts += 1
                    exponent = min(self._reconnect_attempts, 5)
                    reconnect_delay = float(min(30, 2 ** max(1, exponent)))
            finally:
                await self._sleep(reconnect_delay)
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                self._ws_assistant = None
