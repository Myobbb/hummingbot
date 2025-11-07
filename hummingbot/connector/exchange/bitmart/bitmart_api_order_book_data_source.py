import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitmart import (
    bitmart_constants as CONSTANTS,
    bitmart_utils as utils,
    bitmart_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitmart.bitmart_exchange import BitmartExchange


class BitmartAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None
    _PING_INTERVAL_SECONDS: float = 15.0  # < 20s per BitMart docs
    _FORCE_RECONNECT_IDLE_SECONDS: float = 20.0

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BitmartExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector: BitmartExchange = connector
        self._api_factory = api_factory
        self._keepalive_task: Optional[asyncio.Task] = None
        self._reconnect_attempts: int = 0
        self._last_ping_sent_time: float = 0.0

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Bitmart only sends full snapshots, they never send diff messages. That is why this method is overwritten to
        do nothing.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Bitmart sends always full snapshots through the depth channel. That is why they are processed here.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                snapshot_event = await message_queue.get()
                await self._parse_order_book_snapshot_message(raw_message=snapshot_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response["data"]
        snapshot_timestamp: float = int(snapshot_data["ts"]) * 1e-3
        update_id: int = int(snapshot_data["ts"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(bid[0], bid[1]) for bid in snapshot_data["bids"]],
            "asks": [(ask[0], ask[1]) for ask in snapshot_data["asks"]],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "size": 200
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.GET_ORDER_BOOK_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.GET_ORDER_BOOK_PATH_URL,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_updates = raw_message["data"]

        for trade_data in trade_updates:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["symbol"])
            message_content = {
                "trade_id": int(trade_data["s_t"]),
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                    TradeType.SELL.value),
                "amount": trade_data["size"],
                "price": trade_data["price"]
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=int(trade_data["s_t"]))

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Bitmart never sends diff messages. This method will never be called
        pass

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_updates: Dict[str, Any] = raw_message["data"]

        for diff_data in diff_updates:
            timestamp: float = int(diff_data["ms_t"]) * 1e-3
            update_id: int = int(diff_data["ms_t"])
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                symbol=diff_data["symbol"])

            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [(bid[0], bid[1]) for bid in diff_data["bids"]],
                "asks": [(ask[0], ask[1]) for ask in diff_data["asks"]],
            }
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                order_book_message_content,
                timestamp)

            message_queue.put_nowait(diff_message)

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                       for trading_pair in self._trading_pairs]

            # BitMart allows up to 20 topics per subscription message
            trade_topics = [f"{CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME}:{symbol}" for symbol in symbols]
            depth_topics = [f"{CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME}:{symbol}" for symbol in symbols]

            async def send_chunked(topics: List[str]):
                CHUNK_SIZE = 20
                for i in range(0, len(topics), CHUNK_SIZE):
                    chunk = topics[i:i + CHUNK_SIZE]
                    payload = {"op": "subscribe", "args": chunk}
                    req = WSJSONRequest(payload=payload)
                    async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                        await ws.send(req)

            await send_chunked(trade_topics)
            await send_chunked(depth_topics)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        # Start keepalive task
        self._keepalive_task = asyncio.create_task(self._keepalive_ping_loop(websocket_assistant))
        try:
            async for ws_response in websocket_assistant.iter_messages():
                data: Dict[str, Any] = ws_response.data
                decompressed_data = utils.decompress_ws_message(data)
                try:
                    if isinstance(decompressed_data, str):
                        # Gracefully ignore plain-text 'pong'
                        if decompressed_data.strip().lower() == "pong":
                            continue
                        json_data = json.loads(decompressed_data)
                    else:
                        json_data = decompressed_data
                except Exception:
                    # Ignore unparsable frames (e.g., raw pongs)
                    continue

                # Handle exchange error messages gracefully
                if isinstance(json_data, dict) and ("errorCode" in json_data or "errorMessage" in json_data):
                    # Convert to ConnectionError to trigger reconnect with backoff at higher level
                    raise ConnectionError(f"BitMart WS error: {json_data}")

                channel: str = self._channel_originating_message(event_message=json_data)
                if channel in [self._diff_messages_queue_key, self._trade_messages_queue_key]:
                    self._message_queue[channel].put_nowait(json_data)
        finally:
            if self._keepalive_task is not None and not self._keepalive_task.done():
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except Exception:
                    pass
            self._keepalive_task = None

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "data" in event_message:
            event_channel = event_message["table"]
            if event_channel == CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME:
                channel = self._trade_messages_queue_key
            if event_channel == CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME:
                channel = self._diff_messages_queue_key

        return channel

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECT):
            # Disable protocol ping frames; use text 'ping' keepalive per BitMart docs
            await ws.connect(
                ws_url=CONSTANTS.WSS_PUBLIC_URL,
                ping_timeout=None,
                message_timeout=60,
                ws_headers={"Accept-Encoding": "gzip"},
            )
        return ws

    async def _keepalive_ping_loop(self, ws: WSAssistant):
        try:
            while True:
                await asyncio.sleep(1.0)
                # If idle for >= interval, send text 'ping'
                last_recv = getattr(ws, "last_recv_time", 0) or 0
                now = time.time()
                # Send at most one ping per interval and only when idle
                if (now - last_recv) >= self._PING_INTERVAL_SECONDS and (now - self._last_ping_sent_time) >= self._PING_INTERVAL_SECONDS:
                    try:
                        await ws.send(WSPlainTextRequest(payload="ping"))
                        self._last_ping_sent_time = now
                    except Exception:
                        # Force reconnect by raising
                        raise
                # If still no messages beyond max idle threshold, force reconnect
                if (now - last_recv) >= self._FORCE_RECONNECT_IDLE_SECONDS:
                    raise ConnectionError("BitMart WS idle exceeded threshold; forcing reconnect")

        except asyncio.CancelledError:
            raise
        except Exception:
            # Bubble up to reconnect loop
            raise

    async def listen_for_subscriptions(self):
        """
        Override to add graceful transient reconnect/backoff and keepalive-driven reconnects.
        """
        ws: Optional[WSAssistant] = None
        while True:
            reconnect_delay = 1.0
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except (ConnectionError, Exception) as e:
                # Inspect close codes to decide transient backoff
                text = str(e)
                code = None
                try:
                    if "Close code" in text:
                        code = text.split("Close code =")[1].split()[0]
                except Exception:
                    pass
                is_transient = any(tok in text for tok in ["1000", "1001", "1005", "1006", "1012", "1013"])
                if is_transient:
                    self.logger().warning(f"BitMart public WS transient close ({code or 'unknown'}). Reconnecting...")
                    reconnect_delay = 1.0
                else:
                    self.logger().error("BitMart public WS error; reconnecting...", exc_info=True)
                    self._reconnect_attempts += 1
                    exponent = min(self._reconnect_attempts, 5)
                    reconnect_delay = float(min(30, 2 ** max(1, exponent)))
            finally:
                await self._sleep(reconnect_delay)
                ws and await ws.disconnect()