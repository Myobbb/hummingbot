import asyncio
import logging
import time
import re
from typing import Optional

import hummingbot.connector.exchange.bing_x.bing_x_constants as CONSTANTS
import hummingbot.connector.exchange.bing_x.bing_x_utils as utils
import hummingbot.connector.exchange.bing_x.bing_x_web_utils as web_utils
from hummingbot.connector.exchange.bing_x.bing_x_auth import BingXAuth
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BingXAPIUserStreamDataSource(UserStreamTrackerDataSource):

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800

    _bausds_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BingXAuth,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None):
        super().__init__()
        self._auth: BingXAuth = auth
        self._last_recv_time: float = 0
        self._domain = domain
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            domain=self._domain,
            auth=self._auth)
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_ws_message_sent_timestamp = 0

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0
        self._current_listen_key = None
        self._connected_listen_key = None  # Track which key the WS is connected with
        self._manage_listen_key_task = None
        self._listen_key_lock: asyncio.Lock = asyncio.Lock()  # Prevent concurrent key creation
        self._reconnect_attempts: int = 0

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
        ws = None
        while True:
            reconnect_delay = 1.0
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                # Store which key we're connected with
                self._connected_listen_key = self._current_listen_key
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()

                # Simply process messages continuously - no timeout logic needed
                # BingX server sends pings every 5 seconds, we respond in _process_ws_messages
                await self._process_ws_messages(ws=ws, output=output)

            except asyncio.CancelledError:
                raise
            except (ConnectionError, Exception) as e:
                self._reconnect_attempts += 1
                code = self._extract_close_code(e)
                if self._is_transient_ws_close_exception(e):
                    self.logger().warning(f"BingX private WS transient close (code={code or 'unknown'}). Reconnecting...")
                    reconnect_delay = self._backoff_seconds(transient=True)
                else:
                    self.logger().exception("Unexpected error while listening to user stream. Reconnecting...")
                    reconnect_delay = self._backoff_seconds(transient=False)
            finally:
                # Make sure no background task is leaked.
                self._connected_listen_key = None
                ws and await ws.disconnect()
                await self._sleep(reconnect_delay)

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_payload = {
                "id": "usertrade",
                "reqType": "sub",
                "dataType": "spot.executionReport"
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

            balance_payload = {
                "id": "userbalance",
                "reqType": "sub",
                "dataType": "ACCOUNT_UPDATE"
            }
            subscribe_balance_request: WSJSONRequest = WSJSONRequest(payload=balance_payload)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_balance_request)

            self.logger().info("Subscribed to private channel...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    # async def _authenticate_connection(self, ws: WSAssistant):
    #     """
    #     Sends the authentication message.
    #     :param ws: the websocket assistant used to connect to the exchange
    #     """
    #     auth_message: WSJSONRequest = WSJSONRequest(payload=self._auth.generate_ws_authentication_message())
    #     await ws.send(auth_message)

    async def _process_ws_messages(self, ws: WSAssistant, output: asyncio.Queue):
        self._last_recv_time = self._time()
        async for ws_response in ws.iter_messages():
            # CRITICAL: Check if listen key changed - if so, reconnect immediately
            if (self._connected_listen_key is not None and
                self._current_listen_key is not None and
                self._connected_listen_key != self._current_listen_key):
                self.logger().warning(
                    f"Listen key changed from {self._connected_listen_key[:8]}... to "
                    f"{self._current_listen_key[:8]}... - reconnecting with new key"
                )
                raise ConnectionError("Listen key changed - reconnection required")

            data = utils.decompress_ws_message(ws_response.data)

            # Respond to server heartbeat ping per BingX spec (JSON or raw text frames)
            if isinstance(data, dict) and "ping" in data:
                try:
                    pong_payload = {"pong": data.get("ping")}
                    if data.get("time") is not None:
                        pong_payload["time"] = data.get("time")
                    await ws.send(request=WSJSONRequest(payload=pong_payload))
                    self._last_ws_message_sent_timestamp = self._time()
                except Exception as e:
                    self.logger().error(f"Failed to send pong: {e}")
                    raise
                continue

            # Fallback: detect raw text ping frames
            try:
                raw = None
                if isinstance(ws_response.data, bytes):
                    try:
                        raw = ws_response.data.decode("utf-8", errors="ignore")
                    except Exception:
                        raw = None
                elif isinstance(ws_response.data, str):
                    raw = ws_response.data
                if raw and ("ping" in raw.lower()):
                    try:
                        # Per docs examples, reply with plain text Pong for raw ping frames
                        await ws.send(request=WSPlainTextRequest(payload="Pong"))
                        self._last_ws_message_sent_timestamp = self._time()
                        continue
                    except Exception as e:
                        self.logger().error(f"Failed to send Pong (raw): {e}")
                        raise
            except Exception:
                pass

            # Process actual data events
            if data.get("e") == "ACCOUNT_UPDATE":
                # Ignore funding/non-spot reasons per requirement
                reason = str(data.get("a", {}).get("m", "")).upper()
                if reason in ("INIT", "FUNDING_FEE"):
                    continue
                output.put_nowait(data)
            elif data.get("dataType") == "spot.executionReport":
                output.put_nowait(data)

    def _extract_close_code(self, exc: Exception) -> Optional[str]:
        try:
            text = str(exc)
        except Exception:
            return None
        match = re.search(r"Close code\s*=\s*(\d+)", text)
        if match:
            return match.group(1)
        if "1006" in text:
            return "1006"
        return None

    def _is_transient_ws_close_exception(self, exc: Exception) -> bool:
        code = self._extract_close_code(exc)
        return code in {"1000", "1001", "1005", "1006", "1012", "1013"}

    def _backoff_seconds(self, transient: bool) -> float:
        if transient:
            return 1.0
        exponent = min(self._reconnect_attempts, 5)
        return float(min(30, 2 ** max(1, exponent)))

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    def _time(self):
        return time.time()

    async def _ensure_listen_key_task_running(self):
        """
        Ensures the listen key management task is running.

        Creates a new task if none exists or if the previous task has completed.
        This method is idempotent and safe to call multiple times.
        """
        # If task is already running, do nothing
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            return

        # Cancel old task if it exists and is done (failed)
        if self._manage_listen_key_task is not None:
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass  # Ignore any exception from the failed task

        # Create new task
        self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _get_listen_key(self):
        try:
            # Signed request per docs: include timestamp and signature
            data = await web_utils.api_request(
                path=CONSTANTS.USER_STREAM_PATH_URL,
                api_factory=self._api_factory,
                throttler=self._throttler,
                time_synchronizer=None,
                domain=self._domain,
                method=RESTMethod.POST,
                is_auth_required=True,
                headers=self._auth.header_for_authentication(),
                limit_id=CONSTANTS.USER_STREAM_PATH_URL,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            raise IOError(f"Error fetching user stream listen key. Error: {exception}")

        return data["listenKey"]

    async def _ping_listen_key(self) -> bool:
        """
        Extends the validity period of the current listen key.

        NOTE: This method should only be called from within _listen_key_lock to prevent race conditions.
        If the key is not found (404), it will clear the current key to trigger a new key creation.

        :return: True if renewal was successful, False otherwise
        """
        try:
            # BingX returns 200 (with body) or 204 (no content) on success
            # Use return_err=False to let api_request handle 204 properly
            await web_utils.api_request(
                path=CONSTANTS.USER_STREAM_PATH_URL,
                api_factory=self._api_factory,
                throttler=self._throttler,
                time_synchronizer=None,
                domain=self._domain,
                params={"listenKey": self._current_listen_key},
                method=RESTMethod.PUT,
                is_auth_required=True,
                return_err=False,  # Let it raise on error
                limit_id=CONSTANTS.USER_STREAM_PATH_URL,
                headers=self._auth.header_for_authentication(),
            )
            self.logger().debug(f"Successfully renewed listen key {self._current_listen_key}")
            return True

        except asyncio.CancelledError:
            raise
        except Exception as exception:
            # 404 means key not found - need new key
            # Clear the key here (safe because caller holds _listen_key_lock)
            if "404" in str(exception):
                self.logger().warning(f"Listen key {self._current_listen_key} not found (404). Will create new key.")
                # Clear current key to force new key creation
                # This is safe because this method is called within _listen_key_lock
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
            else:
                self.logger().warning(f"Failed to refresh listen key: {exception}")
            return False

    async def _manage_listen_key_task_loop(self):
        """
        Background task that manages the listen key lifecycle.

        Uses a lock to prevent multiple concurrent tasks from creating duplicate listen keys.
        This ensures only one API call is made when the key needs to be created or renewed.
        """
        try:
            while True:
                now = int(time.time())

                # Create new listen key if needed - use lock to prevent concurrent creation
                if self._current_listen_key is None:
                    async with self._listen_key_lock:
                        # Double-check after acquiring lock - another task might have created it
                        if self._current_listen_key is None:
                            self._current_listen_key = await self._get_listen_key()
                            self.logger().info(f"Successfully obtained listen key {self._current_listen_key}")
                            self._listen_key_initialized_event.set()
                            self._last_listen_key_ping_ts = int(time.time())

                # Renew listen key periodically
                if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    # Use lock to ensure only one renewal happens at a time
                    async with self._listen_key_lock:
                        # Check again after acquiring lock
                        if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                            success: bool = await self._ping_listen_key()
                            if not success:
                                # _ping_listen_key already cleared the key if it was 404
                                # Just log and continue - next iteration will create new key
                                self.logger().error("Error occurred renewing listen key, will get a new one...")
                                await self._sleep(5)  # Brief delay before retry
                                continue
                            else:
                                self.logger().debug(f"Refreshed listen key {self._current_listen_key}.")
                                self._last_listen_key_ping_ts = int(time.time())
                else:
                    # Sleep shorter to ensure timely renewal regardless of drift
                    next_renewal = self._last_listen_key_ping_ts + self.LISTEN_KEY_KEEP_ALIVE_INTERVAL
                    sleep_duration = max(5, next_renewal - int(time.time()))
                    await self._sleep(min(sleep_duration, 300))  # Cap at 5 minutes
        except asyncio.CancelledError:
            self.logger().info("Listen key management task cancelled")
            raise
        except Exception as e:
            self.logger().error(f"Unexpected error in listen key management: {e}", exc_info=True)
            raise
        finally:
            # Only clear state on task termination (cancellation or error)
            async with self._listen_key_lock:
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()


    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.

        This method ensures the listen key management task is running before connecting.
        The connection process follows these steps:
        1. Ensures the listen key management task is running (creates if needed)
        2. Waits for a valid listen key to be obtained
        3. Establishes websocket connection with the listen key
        """
        # Ensure only one listen key management task is running
        await self._ensure_listen_key_task_running()
        await self._listen_key_initialized_event.wait()

        ws: WSAssistant = await self._get_ws_assistant()
        # Remove the stray line that does nothing
        url = f"{CONSTANTS.WSS_PRIVATE_URL[self._domain]}?listenKey={self._current_listen_key}"
        # Disable protocol heartbeat; rely on JSON ping/pong; allow larger frames; request gzip
        await ws.connect(
            ws_url=url,
            ping_timeout=None,
            message_timeout=60,
            ws_headers={"Accept-Encoding": "gzip"},
            max_msg_size=16 * 1024 * 1024,
        )
        return ws
    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        """
        Handles websocket disconnection by cleaning up resources.

        This method is called when the websocket connection is interrupted.
        It ensures proper cleanup by:
        1. Cancelling the listen key management task
        2. Disconnecting the websocket assistant if it exists
        3. Clearing the current listen key to force renewal on reconnection
        4. Resetting the initialization event to block new connections until ready
        """
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)

        # Cancel listen key management task if it exists
        if self._manage_listen_key_task and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass  # Ignore any exception from the task
            self._manage_listen_key_task = None

        # Clear listen key state - use lock to prevent race conditions
        async with self._listen_key_lock:
            self._current_listen_key = None
            self._listen_key_initialized_event.clear()

        await self._sleep(5)