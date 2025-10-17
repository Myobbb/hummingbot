import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.exchange.bybit.bybit_constants as CONSTANTS
from hummingbot.connector.exchange.bybit import bybit_web_utils as web_utils
from hummingbot.connector.exchange.bybit.bybit_order_book import BybitOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bybit.bybit_exchange import BybitExchange


class BybitAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BybitExchange',
                 api_factory: Optional[WebAssistantsFactory] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._time_synchronizer = time_synchronizer
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
        )
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._last_ws_message_sent_timestamp = 0
        self._category = "spot"
        self._depth = CONSTANTS.SPOT_ORDER_BOOK_DEPTH
        # Track last per-pair update time (diff or snapshot) to detect staleness
        self._pair_last_update_time: Dict[str, float] = {}
        # Minimum inactivity before a resnapshot is attempted (seconds)
        self._per_pair_stale_threshold: float = 180.0
        # Cooldown between topic resubscriptions per pair (seconds)
        self._pair_last_resubscribe_time: Dict[str, float] = {}
        self._per_pair_resubscribe_cooldown: float = 120.0

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
            "category": self._category,
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "1000"
        }
        data = await self._connector._api_request(
            path_url=CONSTANTS.SNAPSHOT_PATH_URL,
            method=RESTMethod.GET,
            params=params
        )
        return data['result']

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = float(snapshot["ts"]) * 1e-3
        snapshot_msg: OrderBookMessage = BybitOrderBook.snapshot_message_from_exchange_rest(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message["data"]
        for trade in data:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade["s"])
            trade_message: OrderBookMessage = BybitOrderBook.trade_message_from_exchange(
                trade,
                {"trading_pair": trading_pair}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
            symbol=raw_message["data"]["s"]
        )
        order_book_message: OrderBookMessage = BybitOrderBook.diff_message_from_exchange(
            raw_message['data'],
            raw_message["ts"] * 1e-3,
            {"trading_pair": trading_pair}
        )
        message_queue.put_nowait(order_book_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        while True:
            try:
                await asyncio.wait_for(self._process_ob_snapshot(snapshot_queue=output), timeout=CONSTANTS.ONE_HOUR)
            except asyncio.TimeoutError:
                await self._take_full_order_book_snapshot(trading_pairs=self._trading_pairs, snapshot_queue=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._take_full_order_book_snapshot(trading_pairs=self._trading_pairs, snapshot_queue=output)
                await self._sleep(5.0)

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=CONSTANTS.WSS_PUBLIC_URL[self._domain],
                    ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
                )
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()

                while True:
                    try:
                        seconds_until_next_ping = (CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL - (
                            self._time() - self._last_ws_message_sent_timestamp))
                        await asyncio.wait_for(self._process_ws_messages(ws=ws), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        ping_time = self._time()
                        payload = {
                            "op": "ping"
                        }
                        ping_request = WSJSONRequest(payload=payload)
                        await ws.send(request=ping_request)
                        self._last_ws_message_sent_timestamp = ping_time
                        # Watchdog: if no frames received in > 2 heartbeats, force reconnect
                        if ws.last_recv_time and (self._time() - ws.last_recv_time) > (2 * CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL):
                            raise ConnectionError("Bybit public WS inactive for too long; reconnecting.")
                        # Per-pair staleness check: resnapshot stale pairs without tearing down connection
                        await self._resnapshot_stale_pairs_if_any(
                            ws=ws,
                            snapshot_queue=self._message_queue[self._snapshot_messages_queue_key]
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 1 second...",
                    exc_info=True,
                )
                await self._sleep(1.0)
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_topic = self._get_trade_topic_from_symbol(symbol)
                trade_payload = {
                    "op": "subscribe",
                    "args": [trade_topic]
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

                orderbook_topic = self._get_ob_topic_from_symbol(symbol, self._depth)
                orderbook_payload = {
                    "op": "subscribe",
                    "args": [orderbook_topic]
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=orderbook_payload)

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

    async def _process_ws_messages(self, ws: WSAssistant):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if data.get("op") == "subscribe":
                if data.get("success") is False:
                    self.logger().error(
                        "Unexpected error occurred subscribing to order book trading and delta streams...",
                        exc_info=True
                    )
                continue
            event_type = data.get("type")
            topic = data.get("topic")
            if event_type == CONSTANTS.TRADE_EVENT_TYPE and "publicTrade" in topic:
                channel = self._trade_messages_queue_key
            elif event_type == CONSTANTS.ORDERBOOK_SNAPSHOT_EVENT_TYPE and "orderbook" in topic:
                channel = self._snapshot_messages_queue_key
            elif event_type == CONSTANTS.ORDERBOOK_DIFF_EVENT_TYPE and "orderbook" in topic:
                channel = self._diff_messages_queue_key
            else:
                channel = None
            if channel:
                # Update per-pair timestamp for orderbook topics to track freshness
                try:
                    if "orderbook" in topic:
                        symbol = data["data"].get("s")
                        if symbol:
                            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                            self._pair_last_update_time[trading_pair] = self._time()
                except Exception:
                    # Do not block on telemetry
                    pass
                self._message_queue[channel].put_nowait(data)

    async def _resnapshot_stale_pairs_if_any(self, ws: WSAssistant, snapshot_queue: asyncio.Queue):
        now = self._time()
        for trading_pair in list(self._trading_pairs):
            last_ts = self._pair_last_update_time.get(trading_pair)
            if last_ts is None:
                # Initialize on first run to avoid immediate resnapshot
                self._pair_last_update_time[trading_pair] = now
                continue
            if (now - last_ts) >= self._per_pair_stale_threshold:
                # Always attempt topic re-subscribe first (favor WS stream continuity)
                try:
                    exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                    last_re_sub = self._pair_last_resubscribe_time.get(trading_pair, 0)
                    if (now - last_re_sub) >= self._per_pair_resubscribe_cooldown:
                        topic = f"orderbook.{self._depth}.{exchange_symbol}"
                        try:
                            await ws.send(WSJSONRequest({"op": "unsubscribe", "args": [topic]}))
                        except Exception:
                            pass
                        try:
                            await ws.send(WSJSONRequest({"op": "subscribe", "args": [topic]}))
                            self._pair_last_resubscribe_time[trading_pair] = now
                            self.logger().info(f"Re-subscribed Bybit topic for {trading_pair} ({topic}) after staleness.")
                        except Exception:
                            self.logger().warning(f"Failed to re-subscribe topic for {trading_pair}", exc_info=True)
                except Exception:
                    self.logger().warning(f"Failed during re-subscribe attempt for {trading_pair}", exc_info=True)

                # If still severely stale, inject a REST snapshot to heal book state
                if (now - last_ts) >= (2 * self._per_pair_stale_threshold):
                    try:
                        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair=trading_pair)
                        snapshot_timestamp: float = float(snapshot["ts"]) * 1e-3
                        snapshot_msg: OrderBookMessage = BybitOrderBook.snapshot_message_from_exchange_rest(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                        snapshot_queue.put_nowait({
                            "type": CONSTANTS.ORDERBOOK_SNAPSHOT_EVENT_TYPE,
                            "topic": f"orderbook.{self._depth}.{exchange_symbol}",
                            "data": {
                                "s": exchange_symbol,
                                "b": snapshot_msg.bids,
                                "a": snapshot_msg.asks,
                                "u": snapshot_msg.update_id,
                            },
                            "ts": int(snapshot_timestamp * 1e3),
                        })
                        self._pair_last_update_time[trading_pair] = now
                        self.logger().warning(f"Resnapshotted stale orderbook for {trading_pair} after {(now - last_ts):.0f}s inactivity.")
                    except Exception:
                        self.logger().warning(f"Failed to resnapshot stale orderbook for {trading_pair}", exc_info=True)

                # Escalate to full WS reconnect if extreme staleness persists
                if (now - last_ts) >= (3 * self._per_pair_stale_threshold):
                    raise ConnectionError(f"Persistent staleness for {trading_pair} ({int(now - last_ts)}s); reconnecting WS.")

    async def _process_ob_snapshot(self, snapshot_queue: asyncio.Queue):
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                json_msg = await message_queue.get()
                data = json_msg["data"]
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                    symbol=data["s"])
                order_book_message: OrderBookMessage = BybitOrderBook.snapshot_message_from_exchange_websocket(
                    data, json_msg["ts"], {"trading_pair": trading_pair})
                snapshot_queue.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error when processing public order book updates from exchange")
                raise

    async def _take_full_order_book_snapshot(self, trading_pairs: List[str], snapshot_queue: asyncio.Queue):
        for trading_pair in trading_pairs:
            try:
                snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair=trading_pair)
                snapshot_timestamp: float = float(snapshot["ts"]) * 1e-3
                snapshot_msg: OrderBookMessage = BybitOrderBook.snapshot_message_from_exchange_rest(
                    snapshot,
                    snapshot_timestamp,
                    metadata={"trading_pair": trading_pair}
                )
                snapshot_queue.put_nowait(snapshot_msg)
                self.logger().debug(f"Saved order book snapshot for {trading_pair}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(f"Unexpected error fetching order book snapshot for {trading_pair}.",
                                    exc_info=True)
                await self._sleep(5.0)

    def _time(self):
        return time.time()

    def _get_trade_topic_from_symbol(self, symbol: str) -> str:
        return f"publicTrade.{symbol}"

    def _get_ob_topic_from_symbol(self, symbol: str, depth: int) -> str:
        return f"orderbook.{depth}.{symbol}"
