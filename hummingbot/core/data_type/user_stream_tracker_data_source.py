import asyncio
import logging
import time
from abc import ABCMeta
from typing import Any, Dict, Optional

from aiohttp import ClientError, WSServerHandshakeError, ServerDisconnectedError

from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

# HTTP status codes considered transient (server-side issues that typically resolve on retry)
TRANSIENT_HTTP_STATUS_CODES = {502, 503, 504, 520, 521, 522, 523, 524}


class UserStreamTrackerDataSource(metaclass=ABCMeta):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self):
        self._ws_assistant: Optional[WSAssistant] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message

        :return: the timestamp of the last received message in seconds
        """
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    def _is_transient_connection_error(self, exc: Exception) -> bool:
        """
        Check if an exception represents a transient connection error that should be
        logged as a warning rather than an error with full traceback.

        :param exc: The exception to check
        :return: True if the error is transient and expected during network issues
        """
        # WSServerHandshakeError with transient HTTP status codes (502, 503, 504, etc.)
        if isinstance(exc, WSServerHandshakeError):
            return exc.status in TRANSIENT_HTTP_STATUS_CODES

        # Server disconnection errors are transient
        if isinstance(exc, ServerDisconnectedError):
            return True

        # Check for common transient error patterns in the exception message
        exc_str = str(exc).lower()
        transient_patterns = [
            "connection reset",
            "connection refused",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        ]
        return any(pattern in exc_str for pattern in transient_patterns)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue

        :param output: the queue to use to store the received messages
        """
        while True:
            try:
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(websocket_assistant=self._ws_assistant)
                await self._send_ping(websocket_assistant=self._ws_assistant)  # to update last_recv_timestamp
                await self._process_websocket_messages(websocket_assistant=self._ws_assistant, queue=output)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except ClientError as client_error:
                # Handle aiohttp client errors (WSServerHandshakeError, ServerDisconnectedError, etc.)
                if self._is_transient_connection_error(client_error):
                    self.logger().warning(f"Transient connection error, reconnecting... ({type(client_error).__name__}: {client_error})")
                else:
                    self.logger().exception("Client error while listening to user stream. Retrying after 5 seconds...")
                    await self._sleep(1.0)
            except Exception as e:
                # Check if the general exception has transient characteristics
                if self._is_transient_connection_error(e):
                    self.logger().warning(f"Transient connection error, reconnecting... ({type(e).__name__}: {e})")
                else:
                    self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                    await self._sleep(1.0)
            finally:
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                self._ws_assistant = None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange

        :return: an instance of WSAssistant connected to the exchange
        """
        raise NotImplementedError

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        raise NotImplementedError

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            await self._process_event_message(event_message=data, queue=queue)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if len(event_message) > 0:
            queue.put_nowait(event_message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        websocket_assistant and await websocket_assistant.disconnect()

    async def stop(self):
        """
        Stop the user stream data source and clean up any running tasks.
        This method should be overridden by subclasses to handle specific cleanup logic.
        """
        # Cancel listen key task if it exists (for exchanges that use listen keys)
        if hasattr(self, '_manage_listen_key_task') and self._manage_listen_key_task is not None:
            if not self._manage_listen_key_task.done():
                self._manage_listen_key_task.cancel()
                try:
                    await self._manage_listen_key_task
                except asyncio.CancelledError:
                    pass
            self._manage_listen_key_task = None

        # Clear listen key state if it exists
        if hasattr(self, '_current_listen_key'):
            self._current_listen_key = None
        if hasattr(self, '_listen_key_initialized_event'):
            self._listen_key_initialized_event.clear()

        # Disconnect websocket if connected
        if self._ws_assistant:
            await self._ws_assistant.disconnect()
            self._ws_assistant = None

    async def _send_ping(self, websocket_assistant: WSAssistant):
        await websocket_assistant.ping()

    async def _sleep(self, delay: float):
        """
        Function added only to facilitate patching the sleep in unit tests without affecting the asyncio module

        :param delay: number of seconds to sleep
        """
        await asyncio.sleep(delay)

    def _time(self) -> float:
        return time.time()
