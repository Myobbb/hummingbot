import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS, mexc_web_utils as web_utils
from hummingbot.connector.exchange.mexc.pb import PushDataV3ApiWrapper_pb2 as PBPush
from hummingbot.connector.exchange.mexc.mexc_order_book import MexcOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange


class MexcAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'MexcExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "1000"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
            headers={"Content-Type": "application/json"}
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_params = []
            depth_params = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # Prefer protobuf channels with interval hints
                trade_params.append(f"spot@public.aggre.deals.v3.api.pb@10ms@{symbol}")
                depth_params.append(f"spot@public.aggre.depth.v3.api.pb@100ms@{symbol}")
            payload = {
                "method": "SUBSCRIPTION",
                "params": trade_params,
                "id": 1
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

            payload = {
                "method": "SUBSCRIPTION",
                "params": depth_params,
                "id": 2
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # Try new endpoint first, then fallback to legacy
        try:
            await ws.connect(ws_url=CONSTANTS.WSS_API_URL.format(self._domain),
                             ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        except Exception:
            await ws.connect(ws_url=CONSTANTS.WSS_URL.format(self._domain),
                             ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = MexcOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Convert PB bytes into JSON-like dict if needed
        if isinstance(raw_message, (bytes, bytearray)):
            raw_message = self._decode_pb_envelope_to_event_dict(raw_message)
        if isinstance(raw_message, dict) and "code" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message.get("s", ""))
            for single_msg in raw_message.get('d', {}).get('deals', []):
                trade_message = MexcOrderBook.trade_message_from_exchange(
                    single_msg, timestamp=raw_message.get('t'), metadata={"trading_pair": trading_pair})
                message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Convert PB bytes into JSON-like dict if needed
        if isinstance(raw_message, (bytes, bytearray)):
            raw_message = self._decode_pb_envelope_to_event_dict(raw_message)
        if isinstance(raw_message, dict) and "code" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message.get("s", ""))
            order_book_message: OrderBookMessage = MexcOrderBook.diff_message_from_exchange(
                raw_message, raw_message.get('t'), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        # Accept both JSON dict and PB bytes
        parsed = event_message
        if isinstance(event_message, (bytes, bytearray)):
            parsed = self._decode_pb_envelope_to_event_dict(event_message)
        channel = ""
        if isinstance(parsed, dict) and "code" not in parsed:
            event_type = parsed.get("c", "")
            if (CONSTANTS.DIFF_EVENT_TYPE in event_type) or ("aggre.depth" in event_type) or ("depth" in event_type):
                channel = self._diff_messages_queue_key
            else:
                channel = self._trade_messages_queue_key
        return channel

    def _decode_pb_envelope_to_event_dict(self, payload: bytes) -> Dict[str, Any]:
        """Decode a PB wrapper payload into the JSON-like event dict our parsers expect."""
        try:
            wrapper = PBPush.PushDataV3ApiWrapper()
            wrapper.ParseFromString(payload)
            channel = wrapper.channel
            symbol = getattr(wrapper, 'symbol', '') or ''
            # choose createTime if available, else sendTime, else now
            create_time = getattr(wrapper, 'createTime', 0) or 0
            send_time = getattr(wrapper, 'sendTime', 0) or 0
            ts = int(create_time or send_time or time.time() * 1000)

            # Depth (aggregated)
            if wrapper.HasField('publicAggreDepths'):
                body = wrapper.publicAggreDepths
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {
                        "r": getattr(body, 'toVersion', ''),
                        "bids": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'bids', [])],
                        "asks": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'asks', [])],
                    },
                }
                return event

            # Depth (increase)
            if wrapper.HasField('publicIncreaseDepths'):
                body = wrapper.publicIncreaseDepths
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {
                        "r": getattr(body, 'version', ''),
                        "bids": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'bids', [])],
                        "asks": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'asks', [])],
                    },
                }
                return event

            # Trades (aggregated)
            if wrapper.HasField('publicAggreDeals'):
                body = wrapper.publicAggreDeals
                deals = [{
                    "p": it.price,
                    "v": it.quantity,
                    "S": int(getattr(it, 'tradeType', 0)),
                    "t": int(getattr(it, 'time', ts)),
                } for it in getattr(body, 'deals', [])]
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {"deals": deals},
                }
                return event

            # Trades (plain)
            if wrapper.HasField('publicDeals'):
                body = wrapper.publicDeals
                deals = [{
                    "p": it.price,
                    "v": it.quantity,
                    "S": int(getattr(it, 'tradeType', 0)),
                    "t": int(getattr(it, 'time', ts)),
                } for it in getattr(body, 'deals', [])]
                event = {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {"deals": deals},
                }
                return event
        except Exception:
            # If decoding fails, return empty dict so caller ignores
            self.logger().debug("Failed to decode PB payload", exc_info=True)
        return {}
