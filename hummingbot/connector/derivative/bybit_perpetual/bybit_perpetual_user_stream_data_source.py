import asyncio
import logging
import time
from typing import Optional

from hummingbot.connector.derivative.bybit_perpetual import (
    bybit_perpetual_constants as CONSTANTS,
    bybit_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.bybit_perpetual.bybit_perpetual_auth import BybitPerpetualAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BybitPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BybitPerpetualAuth,
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__()
        self._auth = auth
        self._domain = domain
        self._throttler = throttler
        self._time_synchronizer = time_synchronizer
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth
        )
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_ws_message_sent_timestamp = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
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

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue
        :param output: the queue to use to store the received messages
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._get_ws_assistant()
                await ws.connect(ws_url=CONSTANTS.WSS_PRIVATE_URL[self._domain])
                await self._authenticate_connection(ws)
                self._last_ws_message_sent_timestamp = self._time()
                while True:
                    try:
                        seconds_until_next_ping = (CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL -
                                                   (self._time() - self._last_ws_message_sent_timestamp))
                        await asyncio.wait_for(
                            self._process_ws_messages(ws=ws, output=output), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        await self._ping_server(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
            finally:
                # Make sure no background task is leaked.
                ws and await ws.disconnect()
                await self._sleep(5)

    async def _ping_server(self, ws: WSAssistant):
        ping_time = self._time()
        payload = {
            "op": "ping",
            "args": int(ping_time * 1e3)
        }
        ping_request = WSJSONRequest(payload=payload)
        await ws.send(request=ping_request)
        self._last_ws_message_sent_timestamp = ping_time

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
                    await self._process_ws_auth_msg(data, output)
                elif data.get("op") == "pong":
                    await self._process_ws_pingpong_msg(data, output)

    async def _process_ws_auth_msg(self, data: dict, output: asyncio.Queue):
        # {
        #     "success": true,
        #     "ret_msg": "",
        #     "op": "auth",
        #     "conn_id": "XXX"
        # }
        if data.get("success") is False:
            raise IOError("Private channel authentication failed.")
        else:
            self.logger().info("Private channel authentication success.")

    async def _process_ws_pingpong_msg(self, data: dict, output: asyncio.Queue):
        # {
        #     "success": true,
        #     "ret_msg": "pong",
        #     "conn_id": "XXXX",
        #     "op": "ping"
        # }
        pass

    # async def _subscribe_to_channels(self, ws: WSAssistant, url: str):
    #     try:
    #         payload = {
    #             "op": "subscribe",
    #             "args": [f"{CONSTANTS.WS_SUBSCRIPTION_POSITIONS_ENDPOINT_NAME}"],
    #         }
    #         subscribe_positions_request = WSJSONRequest(payload)
    #         payload = {
    #             "op": "subscribe",
    #             "args": [f"{CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME}"],
    #         }
    #         subscribe_orders_request = WSJSONRequest(payload)
    #         payload = {
    #             "op": "subscribe",
    #             "args": [f"{CONSTANTS.WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME}"],
    #         }
    #         subscribe_executions_request = WSJSONRequest(payload)
    #         payload = {
    #             "op": "subscribe",
    #             "args": [f"{CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME}"],
    #         }
    #         subscribe_wallet_request = WSJSONRequest(payload)

    #         await ws.send(subscribe_positions_request)
    #         await ws.send(subscribe_orders_request)
    #         await ws.send(subscribe_executions_request)
    #         await ws.send(subscribe_wallet_request)

    #         self.logger().info(
    #             f"Subscribed to private account and orders channels {url}..."
    #         )
    #     except asyncio.CancelledError:
    #         raise
    #     except Exception:
    #         self.logger().exception(
    #             f"Unexpected error occurred subscribing to order book trading and delta streams {url}..."
    #         )
    #         raise

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        ByBit does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        pass

    async def _connected_websocket_assistant(self, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTIONS_RATE_LIMIT):
            await ws.connect(
                ws_url=CONSTANTS.WSS_PRIVATE_URL[domain],
                ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
            )
        return ws

    def _time(self):
        return int(time.time())

    def _get_server_timestamp(self):
        return web_utils.get_current_server_time()
