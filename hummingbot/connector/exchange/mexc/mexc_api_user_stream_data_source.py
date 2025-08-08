import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS, mexc_web_utils as web_utils
from hummingbot.connector.exchange.mexc.mexc_auth import MexcAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger
from . import pb_decode

if TYPE_CHECKING:
    from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange


class MexcAPIUserStreamDataSource(UserStreamTrackerDataSource):
    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: MexcAuth,
                 trading_pairs: List[str],
                 connector: 'MexcExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: MexcAuth = auth
        self._current_listen_key = None
        self._domain = domain
        self._api_factory = api_factory

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0
        self._manage_listen_key_task: Optional[asyncio.Task] = None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())
        await self._listen_key_initialized_event.wait()

        ws: WSAssistant = await self._get_ws_assistant()
        url = f"{CONSTANTS.WSS_URL.format(self._domain)}?listenKey={self._current_listen_key}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order events and balance events.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:

            orders_change_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_ORDERS_ENDPOINT_NAME_PB],
                "id": 1
            }
            subscribe_order_change_request: WSJSONRequest = WSJSONRequest(payload=orders_change_payload)

            trades_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_TRADES_ENDPOINT_NAME_PB],
                "id": 2
            }
            subscribe_trades_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

            balance_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_BALANCE_ENDPOINT_NAME_PB],
                "id": 3
            }
            subscribe_balance_request: WSJSONRequest = WSJSONRequest(payload=balance_payload)

            await websocket_assistant.send(subscribe_order_change_request)
            await websocket_assistant.send(subscribe_trades_request)
            await websocket_assistant.send(subscribe_balance_request)

            self.logger().info("Subscribed to private order changes and balance updates channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _get_listen_key(self):
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.MEXC_USER_STREAM_PATH_URL, domain=self._domain),
                method=RESTMethod.POST,
                throttler_limit_id=CONSTANTS.MEXC_USER_STREAM_PATH_URL,
                is_auth_required=True
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            raise IOError(f"Error fetching user stream listen key. Error: {exception}")

        return data["listenKey"]

    async def _ping_listen_key(self) -> bool:
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.MEXC_USER_STREAM_PATH_URL, domain=self._domain),
                params={"listenKey": self._current_listen_key},
                method=RESTMethod.PUT,
                return_err=True,
                throttler_limit_id=CONSTANTS.MEXC_USER_STREAM_PATH_URL,
                is_auth_required=True
            )

            if "code" in data:
                self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {data}")
                return False

        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {exception}")
            return False

        return True

    async def _manage_listen_key_task_loop(self):
        try:
            while True:
                now = int(time.time())
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self.logger().info(f"Successfully obtained listen key {self._current_listen_key}")
                    self._listen_key_initialized_event.set()
                    self._last_listen_key_ping_ts = int(time.time())

                elapsed = now - self._last_listen_key_ping_ts
                if elapsed >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success: bool = await self._ping_listen_key()
                    if not success:
                        # Do not stop the loop; retry soon to avoid key expiration
                        self.logger().warning("Failed to refresh MEXC listen key; will retry in 60s")
                        await self._sleep(60)
                        continue
                    self.logger().info(f"Refreshed listen key {self._current_listen_key}.")
                    self._last_listen_key_ping_ts = int(time.time())
                else:
                    # Sleep only the remaining time until next keep-alive
                    remaining = max(1, self.LISTEN_KEY_KEEP_ALIVE_INTERVAL - elapsed)
                    await self._sleep(remaining)
        finally:
            self._current_listen_key = None
            self._listen_key_initialized_event.clear()

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _send_ping(self, websocket_assistant: WSAssistant):
        payload = {
            "method": "PING",
        }
        ping_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await websocket_assistant.send(ping_request)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        self._manage_listen_key_task and self._manage_listen_key_task.cancel()
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
        await self._sleep(5)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """Receive messages and normalize PB frames to legacy JSON-like structure.

        This preserves downstream handling without requiring protobuf classes.
        """
        while True:
            try:
                raw = await asyncio.wait_for(websocket_assistant.receive(), timeout=CONSTANTS.WS_CONNECTION_TIME_INTERVAL)

                # Unwrap WSResponse to its data payload if present
                content = getattr(raw, 'data', raw)

                # Handle bytes: PB frames
                if isinstance(content, (bytes, bytearray)):
                    try:
                        text = content.decode('utf-8', errors='ignore')
                    except Exception:
                        text = ""

                    channel = None
                    if 'spot@private.account.v3.api.pb' in text:
                        channel = CONSTANTS.USER_BALANCE_ENDPOINT_NAME
                    elif 'spot@private.orders.v3.api.pb' in text:
                        channel = CONSTANTS.USER_ORDERS_ENDPOINT_NAME
                    elif 'spot@private.deals.v3.api.pb' in text:
                        channel = CONSTANTS.USER_TRADES_ENDPOINT_NAME

                    if channel is not None:
                        # If we have protobuf decoders, use them to map to legacy JSON 'd'
                        decoded_d = None
                        if channel == CONSTANTS.USER_BALANCE_ENDPOINT_NAME:
                            decoded_d = pb_decode.parse_account_pb(content)
                        elif channel == CONSTANTS.USER_ORDERS_ENDPOINT_NAME:
                            decoded_d = pb_decode.parse_orders_pb(content)
                        elif channel == CONSTANTS.USER_TRADES_ENDPOINT_NAME:
                            decoded_d = pb_decode.parse_deals_pb(content)

                        if decoded_d is not None:
                            await queue.put({"c": channel, "d": decoded_d})
                            continue

                        # Fallback when PB decode not available: REST-backed balance snapshot for account channel
                        if channel == CONSTANTS.USER_BALANCE_ENDPOINT_NAME:
                            try:
                                rest = await self._api_factory.get_rest_assistant()
                                account_info = await rest.execute_request(
                                    url=web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNTS_PATH_URL, domain=self._domain),
                                    method=RESTMethod.GET,
                                    is_auth_required=True,
                                    headers={"Content-Type": "application/json"},
                                )
                                for bal in account_info.get("balances", []):
                                    event = {
                                        "c": CONSTANTS.USER_BALANCE_ENDPOINT_NAME,
                                        "d": {
                                            "a": bal.get("asset"),
                                            "f": bal.get("free", "0"),
                                            "l": bal.get("locked", "0"),
                                        },
                                    }
                                    await queue.put(event)
                            except Exception:
                                # Do not enqueue empty balance events; skip to avoid KeyErrors downstream
                                pass
                            continue

                        # Orders/deals fallback not implemented here (to avoid malformed events)
                        continue

                    # Unknown PB frame: ignore to avoid breaking downstream
                    continue

                # If it's str JSON, parse to dict
                if isinstance(content, str):
                    try:
                        import json
                        parsed = json.loads(content)
                        # Ignore subscription acks or malformed events
                        if not isinstance(parsed, dict):
                            continue
                        if 'code' in parsed:
                            continue
                        if ('c' not in parsed and 'channel' not in parsed) or 'd' not in parsed:
                            continue
                        await queue.put(parsed)
                        continue
                    except Exception:
                        # Not JSON; ignore
                        continue

                # If it's already a dict, forward
                if isinstance(content, dict):
                    await queue.put(content)
                    continue

                # Otherwise, ignore unknown payload types
                continue

            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"method": "PING"})
                await websocket_assistant.send(ping_request)
