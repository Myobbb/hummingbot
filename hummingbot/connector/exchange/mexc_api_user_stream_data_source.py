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
from hummingbot.connector.exchange.mexc.pb import PushDataV3ApiWrapper_pb2 as PBPush

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
        # Try new endpoint first, then fallback
        url_new = f"{CONSTANTS.WSS_API_URL.format(self._domain)}?listenKey={self._current_listen_key}"
        url_legacy = f"{CONSTANTS.WSS_URL.format(self._domain)}?listenKey={self._current_listen_key}"
        try:
            await ws.connect(ws_url=url_new, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        except Exception:
            await ws.connect(ws_url=url_legacy, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order events and balance events.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:

            orders_change_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_ORDERS_ENDPOINT_NAME],
                "id": 1
            }
            subscribe_order_change_request: WSJSONRequest = WSJSONRequest(payload=orders_change_payload)

            trades_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_TRADES_ENDPOINT_NAME],
                "id": 2
            }
            subscribe_trades_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

            balance_payload = {
                "method": "SUBSCRIPTION",
                "params": [CONSTANTS.USER_BALANCE_ENDPOINT_NAME],
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
        payload = {"method": "ping"}
        ping_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await websocket_assistant.send(ping_request)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        self._manage_listen_key_task and self._manage_listen_key_task.cancel()
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
        await self._sleep(5)

    def _decode_private_pb_payload(self, payload: bytes):
        try:
            wrapper = PBPush.PushDataV3ApiWrapper()
            wrapper.ParseFromString(payload)
            channel = wrapper.channel
            symbol = getattr(wrapper, 'symbol', '') or ''
            # Orders
            if wrapper.HasField('privateOrders'):
                body = wrapper.privateOrders
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'createTime', 0) or 0),
                    "d": {
                        "i": str(getattr(body, 'id', '')),
                        "c": str(getattr(body, 'clientId', '')),
                        "p": str(getattr(body, 'price', '')),
                        # Original requested quantities and amounts
                        "v": str(getattr(body, 'quantity', '')),
                        "V": str(getattr(body, 'quantity', '')),
                        "a": str(getattr(body, 'amount', '')),
                        "A": str(getattr(body, 'amount', '')),
                        # Cumulative filled quantities/amounts
                        "cv": str(getattr(body, 'cumulativeQuantity', '')),
                        "ca": str(getattr(body, 'cumulativeAmount', '')),
                        # Average price
                        "ap": str(getattr(body, 'avgPrice', '')),
                        # Order attributes
                        "ot": int(getattr(body, 'orderType', 0)),
                        "tt": int(getattr(body, 'tradeType', 0)),
                        # Duplicate under 'S' to match our current connector mapping
                        "S": int(getattr(body, 'tradeType', 0)),
                        "m": bool(getattr(body, 'isMaker', False)),
                        # Status (numeric, aligns with our ORDER_STATUS_MAP)
                        "s": int(getattr(body, 'status', 0)),
                        # Order creation time for downstream timestamping
                        "O": int(getattr(body, 'createTime', 0) or 0),
                    }
                }
                return event
            # Deals
            if wrapper.HasField('privateDeals'):
                body = wrapper.privateDeals
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'time', 0) or 0),
                    "d": {
                        # Trade id used as our execution id (orderId uses separate mapping)
                        "t": str(getattr(body, 'tradeId', '')),
                        "p": str(getattr(body, 'price', '')),
                        "v": str(getattr(body, 'quantity', '')),
                        "a": str(getattr(body, 'amount', '')),
                        "S": int(getattr(body, 'tradeType', 0)),
                        # order id and client order id
                        "i": str(getattr(body, 'orderId', '')),
                        "c": str(getattr(body, 'clientOrderId', '')),
                        "n": str(getattr(body, 'feeAmount', '')),
                        "N": str(getattr(body, 'feeCurrency', '')),
                        # Trade timestamp
                        "T": int(getattr(body, 'time', 0) or 0),
                    }
                }
                return event
            # Account (balances)
            if wrapper.HasField('privateAccount'):
                body = wrapper.privateAccount
                # Map to what _process_balance_message_ws expects: keys "a" (asset), "f" (free), "l" (frozen)
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'time', 0) or 0),
                    "d": {
                        "a": str(getattr(body, 'vcoinName', '')),
                        "f": str(getattr(body, 'balanceAmount', '')),
                        "l": str(getattr(body, 'frozenAmount', '')),
                    }
                }
                return event
        except Exception:
            self.logger().debug("Failed to decode private PB payload", exc_info=True)
        return {}

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            try:
                # Receive a raw message from WS
                ws_msg = await asyncio.wait_for(
                    websocket_assistant.receive(),
                    timeout=CONSTANTS.WS_CONNECTION_TIME_INTERVAL
                )
                data = ws_msg.data
                # Convert protobuf bytes (if any) to the existing dict structure, else forward as-is
                if isinstance(data, (bytes, bytearray)):
                    event_dict = self._decode_private_pb_payload(data)
                    if event_dict:
                        await queue.put(event_dict)
                        continue
                # Normalize JSON dicts to include expected fields (e.g., ensure 'T' exists for trade fills)
                if isinstance(data, dict):
                    try:
                        channel = data.get('c') or data.get('channel') or ''
                        payload = data.get('d') or data.get('data') or {}
                        if isinstance(payload, dict) and ('private.deals' in str(channel)):
                            if 'T' not in payload:
                                # Prefer explicit 'time' if present
                                if 'time' in payload:
                                    try:
                                        payload['T'] = int(payload.get('time') or 0)
                                    except Exception:
                                        payload['T'] = 0
                                # Fallback: if lowercase 't' looks numeric, mirror it
                                elif 't' in payload and str(payload.get('t', '')).isdigit():
                                    try:
                                        payload['T'] = int(payload.get('t') or 0)
                                    except Exception:
                                        payload['T'] = 0
                                # Leave as-is otherwise; consumer may handle missing timestamp
                            # Write back normalized payload
                            if 'd' in data:
                                data['d'] = payload
                            elif 'data' in data:
                                data['data'] = payload
                    except Exception:
                        # Non-fatal; forward raw data
                        pass
                await queue.put(data)
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"method": "ping"})
                await websocket_assistant.send(ping_request)
