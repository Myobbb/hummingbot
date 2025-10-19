import asyncio
import time
import os
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
                 time_synchronizer: Optional[TimeSynchronizer] = None,
                 max_queue_size: Optional[int] = None):
                 
        super().__init__(trading_pairs)
        self._symbol_drop_count: Dict[str, int] = {}
        self._connector = connector
        self._domain = domain
        self._time_synchronizer = time_synchronizer
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
        )
        # Bounded queues to prevent unbounded growth under high load (configurable/scaled)
        env_max = os.getenv("HUMMINGBOT_BYBIT_WS_QUEUE_MAXSIZE")
        env_buffer_seconds = os.getenv("HUMMINGBOT_BYBIT_WS_QUEUE_BUFFER_SECONDS")
        try:
            computed_updates_per_second = 50  # Bybit L50 can burst ~50 updates/sec
            buffer_seconds = int(env_buffer_seconds) if env_buffer_seconds is not None else 60
            pairs_factor = max(1, min(len(self._trading_pairs), 5))
            computed_size = computed_updates_per_second * buffer_seconds * pairs_factor
        except Exception:
            computed_size = 1000
        try:
            override_size = int(max_queue_size) if max_queue_size is not None else (int(env_max) if env_max is not None else None)
        except Exception:
            override_size = None
        # Clamp to sane bounds
        self._max_queue_size: int = max(1000, min(50000, override_size if override_size is not None else computed_size))
        self._message_queue: Dict[str, asyncio.Queue] = {
            self._trade_messages_queue_key: asyncio.Queue(maxsize=self._max_queue_size),
            self._diff_messages_queue_key: asyncio.Queue(maxsize=self._max_queue_size),
            self._snapshot_messages_queue_key: asyncio.Queue(maxsize=self._max_queue_size),
        }
        self._last_ws_message_sent_timestamp = 0
        self._category = "spot"
        self._depth = CONSTANTS.SPOT_ORDER_BOOK_DEPTH
        # Track last per-symbol update time (diff or snapshot) to detect staleness (Bybit symbol-specific)
        self._symbol_last_update_time: Dict[str, float] = {}
        # Minimum inactivity before a resnapshot is attempted (seconds)
        # Tighten threshold to recover faster from silent stream stalls
        self._per_pair_stale_threshold: float = 60.0
        # Cooldown between topic resubscriptions per symbol (seconds)
        self._symbol_last_resubscribe_time: Dict[str, float] = {}
        self._per_pair_resubscribe_cooldown: float = 45.0
        # Proactive reconnect window to avoid aged connections (seconds)
        self._max_connection_age_seconds: float = 60.0 * 60.0 * 1.0  # 1 hours
        self._conn_start_time: Optional[float] = None
        # Track last ping req_id to correlate pong acks
        self._last_ping_req_id: Optional[str] = None
        # Cache for symbol -> trading pair to avoid blocking lookups in hot path
        self._symbol_to_pair_cache: Dict[str, str] = {}
        # Cache for trading pair -> symbol (Bybit-specific)
        self._pair_to_symbol_cache: Dict[str, str] = {}
        # Track repeated subscription failures to avoid reconnect loops
        self._subscription_failure_count: Dict[str, int] = defaultdict(int)
        self._max_subscription_failures: int = 5
        # Throttled per-symbol warning timestamps to avoid log spam
        self._symbol_last_warn_time: Dict[str, float] = {}

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
        data = raw_message.get("data", [])
        # Fast path: prefer enriched trading_pair, else cached symbol mapping, fallback to async lookup once
        trading_pair = raw_message.get("trading_pair")
        if trading_pair is None and data:
            symbol = data[0].get("s")
            if symbol:
                trading_pair = self._symbol_to_pair_cache.get(symbol)
                if trading_pair is None:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                    # Cache for subsequent frames
                    try:
                        self._symbol_to_pair_cache[symbol] = trading_pair
                    except Exception:
                        pass
        if trading_pair is None:
            return
        for trade in data:
            trade_message: OrderBookMessage = BybitOrderBook.trade_message_from_exchange(
                trade,
                {"trading_pair": trading_pair}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = raw_message.get("trading_pair")
        if trading_pair is None:
            symbol = raw_message.get("data", {}).get("s")
            if symbol:
                trading_pair = self._symbol_to_pair_cache.get(symbol)
                if trading_pair is None:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                    try:
                        self._symbol_to_pair_cache[symbol] = trading_pair
                    except Exception:
                        pass
        if trading_pair is None:
            return
        order_book_message: OrderBookMessage = BybitOrderBook.diff_message_from_exchange(
            raw_message['data'],
            raw_message["ts"] * 1e-3,
            {"trading_pair": trading_pair}
        )
        message_queue.put_nowait(order_book_message)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        High-throughput diff consumer with batching to reduce per-message await overhead.
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                # Collect batch WITHOUT awaiting to drain queue faster
                batch = [await message_queue.get()]  # Get first, blocking
                max_batch = 500
                
                # Drain remaining without awaiting
                while len(batch) < max_batch:
                    try:
                        batch.append(message_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                
                # Process batch - queue is now drained
                for diff_event in batch:
                    await self._parse_order_book_diff_message(raw_message=diff_event, message_queue=output)
                    
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing Bybit order book diffs")

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
                # Wait briefly to allow subscribe acks without consuming messages
                await self._wait_for_initial_subscribe_acks(ws, timeout=5.0)
                self._last_ws_message_sent_timestamp = self._time()
                self._conn_start_time = self._time()

                while True:
                    try:
                        seconds_until_next_ping = (CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL - (
                            self._time() - self._last_ws_message_sent_timestamp))
                        await asyncio.wait_for(self._process_ws_messages(ws=ws), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        ping_time = self._time()
                        ping_req_id = str(int(ping_time * 1e3))
                        payload = {
                            "req_id": ping_req_id,
                            "op": "ping"
                        }
                        ping_request = WSJSONRequest(payload=payload)
                        await ws.send(request=ping_request)
                        self._last_ws_message_sent_timestamp = ping_time
                        self._last_ping_req_id = ping_req_id
                        # Watchdog: if no frames received in > 2.5 heartbeats, force reconnect
                        if ws.last_recv_time and (self._time() - ws.last_recv_time) > (2.5 * CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL):
                            raise ConnectionError("Bybit public WS inactive for too long; reconnecting.")
                        # Proactive reconnect if connection age exceeds threshold
                        if self._conn_start_time is not None and (self._time() - self._conn_start_time) > self._max_connection_age_seconds:
                            raise ConnectionError("Bybit public WS reached max connection age; reconnecting proactively.")
                        # Per-pair staleness check: resnapshot stale pairs without tearing down connection
                        await self._resnapshot_stale_pairs_if_any(
                            ws=ws,
                            snapshot_queue=self._message_queue[self._snapshot_messages_queue_key]
                        )
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                # Expected reconnect scenarios (inactive socket, proactive age-based reconnect, etc.)
                try:
                    self.logger().warning(f"Bybit public WS reconnecting: {connection_exception}")
                except Exception:
                    pass
                await self._sleep(1.0)
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
                # Pre-seed symbol -> trading pair cache to avoid per-message async lookups
                try:
                    self._symbol_to_pair_cache[symbol] = trading_pair
                    self._pair_to_symbol_cache[trading_pair] = symbol
                except Exception:
                    pass
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
            self.logger().info(f"Subscribed to public order book and trade channels for {len(self._trading_pairs)} pairs")
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
            # Handle ping/pong acks from server (public channels may respond with op: "ping" + ret_msg: "pong")
            if data.get("op") in ("ping", "pong"):
                # Treat ack frames first (public spot acks: op=="ping" with ret_msg=="pong"; some channels: op=="pong")
                if data.get("ret_msg") == "pong" or data.get("op") == "pong":
                    received_req_id = data.get("req_id")
                    if received_req_id is not None and self._last_ping_req_id is not None and received_req_id == self._last_ping_req_id:
                        try:
                            self.logger().debug(f"Bybit pong ack for ping {received_req_id}")
                        except Exception:
                            pass
                    continue  # ack handled
                # Server-initiated ping without ack fields: reply with pong
                if data.get("op") == "ping":
                    try:
                        pong_payload = {"op": "pong"}
                        if "req_id" in data:
                            pong_payload["req_id"] = str(data["req_id"])  # echo if present
                        elif "ts" in data:
                            pong_payload["req_id"] = str(data["ts"])  # some variants use ts
                        await ws.send(WSJSONRequest(pong_payload))
                    except Exception:
                        pass
                    continue
                continue
            if data.get("op") == "subscribe":
                if data.get("success") is False:
                    failed_args = data.get("args") or (
                        data.get("data", {}).get("failTopics", []) if isinstance(data.get("data"), dict) else []
                    )
                    self.logger().error(f"Subscription failed for {failed_args}: {data.get('ret_msg')}")
                    # Track failures per topic and avoid tight reconnect loops
                    for arg in failed_args:
                        topic_str = str(arg)
                        self._subscription_failure_count[topic_str] += 1
                        self.logger().warning(
                            f"Bybit subscribe failure #{self._subscription_failure_count[topic_str]} for topic '{topic_str}': {data.get('ret_msg')}"
                        )
                        # If we've hit max failures, drop only the affected pair/topic; otherwise retry subscribing with backoff
                        if self._subscription_failure_count[topic_str] >= self._max_subscription_failures:
                            try:
                                symbol = self._parse_symbol_from_topic(topic_str)
                                trading_pair = None
                                if symbol:
                                    trading_pair = self._symbol_to_pair_cache.get(symbol)
                                    if trading_pair is None:
                                        try:
                                            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                                        except Exception:
                                            trading_pair = None
                                if trading_pair and trading_pair in self._trading_pairs:
                                    self._trading_pairs.remove(trading_pair)
                                    self.logger().error(f"Removing {trading_pair} from tracking after repeated subscription failures for {topic_str}")
                                else:
                                    self.logger().error(f"Max subscription failures reached for {topic_str}, not reconnecting entire WS")
                            except Exception:
                                self.logger().error(f"Error handling max failure for topic {topic_str}", exc_info=True)
                        else:
                            # Exponential backoff capped to 30s
                            delay = min(30.0, 2.0 * float(self._subscription_failure_count[topic_str]))
                            try:
                                asyncio.create_task(self._retry_subscribe_topic(ws, topic_str, delay))
                            except Exception:
                                self.logger().warning(f"Failed to schedule retry subscribe for topic {topic_str}")
                else:
                    # Successful subscribe ack (rarely carries args). Log at info for visibility.
                    try:
                        acked = data.get("args") or data.get("topic") or "<unknown>"
                        self.logger().info(f"Bybit subscribe acknowledged: {acked}")
                    except Exception:
                        pass
                continue
            event_type = data.get("type")
            topic = data.get("topic")

            if event_type == CONSTANTS.TRADE_EVENT_TYPE and topic and "publicTrade" in topic:
                channel = self._trade_messages_queue_key
            elif event_type == CONSTANTS.ORDERBOOK_SNAPSHOT_EVENT_TYPE and topic and "orderbook" in topic:
                channel = self._snapshot_messages_queue_key
            elif event_type == CONSTANTS.ORDERBOOK_DIFF_EVENT_TYPE and topic and "orderbook" in topic:
                channel = self._diff_messages_queue_key
            else:
                channel = None
           # Enrich message with trading_pair to avoid async lookups in parsers (NO staleness update yet)
            try:
                if topic and "orderbook" in topic:
                    symbol = data.get("data", {}).get("s")
                    if symbol:
                        # Only enrich if in cache - don't block WS loop with async lookup
                        trading_pair = self._symbol_to_pair_cache.get(symbol)
                        if trading_pair:
                            try:
                                data["trading_pair"] = trading_pair
                            except Exception:
                                pass
                        # If not in cache, parser will do slow lookup (acceptable - keeps WS flowing)
                        
                elif topic and "publicTrade" in topic:
                    trades = data.get("data", []) if isinstance(data, dict) else []
                    if trades:
                        ts = trades[0]
                        symbol = ts.get("s") if isinstance(ts, dict) else None
                        if symbol:
                            tp = self._symbol_to_pair_cache.get(symbol)
                            if tp is not None:
                                try:
                                    data["trading_pair"] = tp
                                except Exception:
                                    pass
            except Exception as e:
                self.logger().warning(f"Failed to enrich message for topic={topic}: {e}")

                
                # Move staleness update AFTER put_nowait succeeds
            if channel:
                try:
                    self._message_queue[channel].put_nowait(data)
                    
                    # NOW update staleness after we know message was queued
                    if topic and "orderbook" in topic:
                        symbol = data["data"].get("s")
                        if symbol:
                            self._symbol_last_update_time[symbol] = self._time()
                            self._symbol_drop_count[symbol] = 0 
                            
                except asyncio.QueueFull:
                    self.logger().warning(f"Message queue '{channel}' full; dropping {topic}")
                    
                    if topic and "orderbook" in topic:
                        symbol = data.get("data", {}).get("s")
                        if symbol:
                            # Force immediate staleness
                            self._symbol_last_update_time[symbol] = self._time() - self._per_pair_stale_threshold - 1
                            
                            # Track consecutive drops for this symbol
                            if not hasattr(self, '_symbol_drop_count'):
                                self._symbol_drop_count = {}
                            self._symbol_drop_count[symbol] = self._symbol_drop_count.get(symbol, 0) + 1
                            
                            # Emergency: if 50+ consecutive drops, force reconnect immediately
                            if self._symbol_drop_count.get(symbol, 0) >= 50:
                                raise ConnectionError(f"Queue overrun for {symbol} - {self._symbol_drop_count[symbol]} drops; reconnecting")
         

    async def _wait_for_initial_subscribe_acks(self, ws: WSAssistant, timeout: float = 5.0):
        """
        Brief sleep to allow subscription acknowledgements to arrive without consuming messages
        from the main iterator/receive pipeline.
        """
        try:
            await asyncio.sleep(min(max(0.0, timeout), 2.0))
        except Exception:
            pass

    async def _resnapshot_stale_pairs_if_any(self, ws: WSAssistant, snapshot_queue: asyncio.Queue):
        now = self._time()
        for trading_pair in list(self._trading_pairs):
            # Use Bybit symbol to detect staleness so other exchanges do not mask inactivity here
            symbol = self._pair_to_symbol_cache.get(trading_pair)
            if symbol is None:
                try:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                    self._pair_to_symbol_cache[trading_pair] = symbol
                except Exception:
                    symbol = None
            last_ts = self._symbol_last_update_time.get(symbol) if symbol else None
            if last_ts is None:
                # Initialize on first run to avoid immediate resnapshot
                if symbol:
                    self._symbol_last_update_time[symbol] = now
                continue
            # Throttled warning if pair has had no orderbook updates for a while
            idle = now - last_ts
            if idle >= 30.0:  # warn after 30s of silence
                last_warn = self._symbol_last_warn_time.get(symbol or trading_pair, 0)
                if (now - last_warn) >= 30.0:
                    try:
                        self.logger().warning(f"Bybit orderbook idle {idle:.0f}s for {trading_pair}")
                    except Exception:
                        pass
                    self._symbol_last_warn_time[symbol or trading_pair] = now
            if (now - last_ts) >= self._per_pair_stale_threshold:
                # Always attempt topic re-subscribe first (favor WS stream continuity)
                try:
                    exchange_symbol = symbol or await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                    last_re_sub = self._symbol_last_resubscribe_time.get(exchange_symbol, 0)
                    if (now - last_re_sub) >= self._per_pair_resubscribe_cooldown:
                        topic = f"orderbook.{self._depth}.{exchange_symbol}"
                        try:
                            await ws.send(WSJSONRequest({"op": "unsubscribe", "args": [topic]}))
                        except Exception:
                            pass
                        try:
                            await ws.send(WSJSONRequest({"op": "subscribe", "args": [topic]}))
                            self._symbol_last_resubscribe_time[exchange_symbol] = now
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
                        exchange_symbol = symbol or await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                        snapshot_queue.put_nowait({
                            "type": CONSTANTS.ORDERBOOK_SNAPSHOT_EVENT_TYPE,
                            "topic": f"orderbook.{self._depth}.{exchange_symbol}",
                            "data": {
                                "s": exchange_symbol,
                                "b": snapshot.get("b"),
                                "a": snapshot.get("a"),
                                "u": snapshot.get("u"),
                            },
                            "ts": int(snapshot_timestamp * 1e3),
                        })
                        if symbol:
                            self._symbol_last_update_time[symbol] = now
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
                trading_pair = json_msg.get("trading_pair")
                if trading_pair is None:
                    # Fallback lookup (infrequent path)
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

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        High-throughput trade consumer with batching.
        """
        message_queue = self._message_queue[self._trade_messages_queue_key]
        while True:
            try:
                # Collect batch WITHOUT awaiting to drain queue faster
                batch = [await message_queue.get()]  # Get first, blocking
                max_batch = 500
                
                # Drain remaining without awaiting
                while len(batch) < max_batch:
                    try:
                        batch.append(message_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                
                # Process batch - queue is now drained
                for trade_event in batch:
                    await self._parse_trade_message(raw_message=trade_event, message_queue=output)
                    
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing Bybit public trade updates")

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

    def _parse_symbol_from_topic(self, topic: str) -> Optional[str]:
        try:
            parts = str(topic).split(".")
            return parts[-1] if parts else None
        except Exception:
            return None

    async def _retry_subscribe_topic(self, ws: WSAssistant, topic: str, delay: float):
        try:
            await self._sleep(delay)
            # Ensure the underlying connection is still alive before attempting to send
            is_connected = bool(getattr(getattr(ws, "_connection", None), "connected", False))
            if is_connected:
                payload = {"op": "subscribe", "args": [topic]}
                await ws.send(WSJSONRequest(payload))
                try:
                    self.logger().info(f"Retried subscribing topic {topic} after {delay:.1f}s")
                except Exception:
                    pass
            else:
                try:
                    self.logger().debug(f"Skipping retry for {topic} - WS disconnected")
                except Exception:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().warning(f"Retry subscribe failed for topic {topic}", exc_info=True)
