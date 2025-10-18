import asyncio
import json
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.exchange.bing_x.bing_x_constants as CONSTANTS
import hummingbot.connector.exchange.bing_x.bing_x_utils as utils
from hummingbot.connector.exchange.bing_x import bing_x_web_utils as web_utils
from hummingbot.connector.exchange.bing_x.bing_x_order_book import BingXOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bing_x.bing_x_exchange import BingXExchange


class BingXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BingXExchange',
                 api_factory: Optional[WebAssistantsFactory] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__(trading_pairs)
        self._connector = connector
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
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
        # Throttled per-symbol lag logging (ms timestamp)
        self._last_depth_lag_log_ms: Dict[str, int] = {}
        self._last_update_ids: Dict[str, int] = {}
        self._awaiting_first_update: Dict[str, bool] = {}
        self._snapshot_output_queue: Optional[asyncio.Queue] = None

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
            "symbol": trading_pair,
            "limit": "100"
        }
        data = await self._connector._api_request(path_url=CONSTANTS.SNAPSHOT_PATH_URL,
                                                  method=RESTMethod.GET,
                                                  params=params)
        # Some BingX responses can arrive as raw strings (text/plain) or JSON strings; normalize to dict
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                raise IOError(f"Unexpected snapshot response type (string) for {trading_pair}: {data[:120]}")

        if not isinstance(data, dict):
            raise IOError(f"Unexpected snapshot response format for {trading_pair}: type={type(data)}")

        # Extract sub-payload and timestamp robustly
        sub = data.get('data')
        if sub is None:
            # Some variants may place book arrays at top-level
            if all(k in data for k in ("asks", "bids")):
                sub = data
            else:
                raise IOError(f"Snapshot payload missing 'data' for {trading_pair}: keys={list(data.keys())}")

        if not isinstance(sub, dict):
            raise IOError(f"Unexpected 'data' payload type for {trading_pair}: type={type(sub)}")

        ts = data.get('timestamp') or data.get('ts') or int(self._time() * 1e3)
        try:
            sub['timestamp'] = ts
        except Exception:
            # Ensure we can attach timestamp even if sub is a mapping-like
            sub = dict(sub)
            sub['timestamp'] = ts

        # BingX REST API should return version field for sequence validation
        # Store version as lastUpdateId for consistency with WS messages
        version = sub.get('version')
        if version is not None:
            sub['lastUpdateId'] = version

        return sub

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = float(snapshot["timestamp"]) * 1e-3
        snapshot_msg: OrderBookMessage = BingXOrderBook.snapshot_message_from_exchange_rest(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["symbol"])
        trading_pair = raw_message["dataType"].split('@')[0]
        # for trades in raw_message["data"]:
        trade_message: OrderBookMessage = BingXOrderBook.trade_message_from_exchange(
            raw_message["data"], {"trading_pair": trading_pair})
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # self.logger().info(f"parse msg queue: {raw_message}")
        trading_pair = raw_message.get('dataType').split('@')[0]
        # for diff_message in raw_message["data"]:
        #     order_book_message: OrderBookMessage = BingXOrderBook.diff_message_from_exchange(
        #         diff_message, diff_message["t"], {"trading_pair": trading_pair})
        #     message_queue.put_nowait(order_book_message)
        # Prefer server-provided timestamp when available
        ws_time_ms = None
        try:
            if isinstance(raw_message.get('time'), (int, float)):
                ws_time_ms = int(raw_message.get('time'))
            elif isinstance(raw_message.get('ts'), (int, float)):
                ws_time_ms = int(raw_message.get('ts'))
            elif isinstance(raw_message.get('data', {}).get('t'), (int, float)):
                ws_time_ms = int(raw_message.get('data', {}).get('t'))
        except Exception:
            ws_time_ms = None
        time = (ws_time_ms * 1e-3) if ws_time_ms is not None else self._time()
        order_book_message: OrderBookMessage = BingXOrderBook.diff_message_from_exchange(
            raw_message, time, {"trading_pair": trading_pair})
        message_queue.put_nowait(order_book_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        self._snapshot_output_queue = output  # Store reference for recovery
        while True:
            try:
                await asyncio.wait_for(self._process_ob_snapshot(snapshot_queue=output), timeout=self.ONE_HOUR)
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
                # Disable protocol heartbeat; rely on JSON ping/pong; allow larger frames; request gzip
                await ws.connect(
                    ws_url=CONSTANTS.WSS_PUBLIC_URL[self._domain],
                    ping_timeout=None,
                    message_timeout=60,
                    ws_headers={"Accept-Encoding": "gzip"},
                    max_msg_size=16 * 1024 * 1024,
                )
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()
                # Process messages continuously and only respond to server pings
                await self._process_ws_messages(ws=ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(1.0)
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        Respects BingX's 200 subscription limit per connection by batching if needed.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            # BingX allows up to 200 subscriptions per websocket connection
            MAX_SUBSCRIPTIONS_PER_CONNECTION = 200

            # Calculate subscriptions needed: 2 per trading pair (trade + depth)
            total_subscriptions_needed = len(self._trading_pairs) * 2

            if total_subscriptions_needed > MAX_SUBSCRIPTIONS_PER_CONNECTION:
                self.logger().warning(
                    f"Total subscriptions ({total_subscriptions_needed}) exceeds BingX limit "
                    f"({MAX_SUBSCRIPTIONS_PER_CONNECTION}) per connection. Consider reducing trading pairs."
                )

            # Subscribe to all trading pairs (BingX should handle the limit gracefully)
            for trading_pair in self._trading_pairs:
                trade_payload = {
                    "id": f"trade_{trading_pair}",
                    "reqType": "sub",
                    "dataType": trading_pair + "@trade"
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

                depth_payload = {
                    "id": f"depth_{trading_pair}",
                    "reqType": "sub",
                    "dataType": trading_pair + "@incrDepth"
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=depth_payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

                self.logger().info(f"Subscribed to public order book and trade channels of {trading_pair}...")
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
            data = utils.decompress_ws_message(ws_response.data)
        
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    continue
            
            if data.get("msg") == "SUCCESS":
                continue
            
            # Handle ping/pong
            if data.get("ping"):
                payload = {"pong": data.get("ping")}
                if data.get("time") is not None:
                    payload["time"] = data.get("time")
                ping_request = WSJSONRequest(payload=payload)
                await ws.send(request=ping_request)
                self._last_ws_message_sent_timestamp = self._time()
                continue
            
            # Process data messages
            if data.get("dataType"):
                symbol = data.get("dataType").split('@')[0]
                event_type = data.get("dataType").split('@')[1]
                data['symbol'] = symbol
                
                if event_type.startswith("depth"):
                    # Handle incremental depth messages
                    if data.get("dataType") and "@incrDepth" in data.get("dataType", ""):
                        symbol = data.get("dataType").split('@')[0]
                        data['symbol'] = symbol

                        # Extract nested action from data.data
                        data_node = data.get('data', {})
                        action = data_node.get('action')  # 'all' or 'update'
                        last_update_id = data_node.get('lastUpdateId')

                        if action == "all":
                            # Full snapshot (first message)
                            self.logger().info(
                                f"{symbol}: Received full depth snapshot "
                                f"(lastUpdateId={last_update_id})"
                            )
                            self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].put_nowait(data)
                            self._last_update_ids[symbol] = last_update_id
                            # Track that we need to validate the first update
                            self._awaiting_first_update[symbol] = True

                        elif action == "update":
                            # Don't process incremental messages until we have a proper snapshot
                            if symbol not in self._last_update_ids:
                                self.logger().warning(
                                    f"{symbol}: Received incremental update before snapshot. "
                                    f"Ignoring update and waiting for snapshot."
                                )
                                continue

                            # Validate first update after snapshot
                            if self._awaiting_first_update.get(symbol):
                                expected_first_id = self._last_update_ids[symbol] + 1
                                if last_update_id != expected_first_id:
                                    self.logger().error(
                                        f"{symbol}: First update ID mismatch! "
                                        f"Snapshot lastUpdateId={self._last_update_ids[symbol]}, "
                                        f"Expected first update={expected_first_id}, "
                                        f"Got={last_update_id}. Requesting new snapshot."
                                    )
                                    # Clear state and wait for next WS snapshot
                                    self._last_update_ids.pop(symbol, None)
                                    self._awaiting_first_update.pop(symbol, None)
                                    continue
                                self._awaiting_first_update[symbol] = False

                            # Existing incremental diff validation
                            if symbol in self._last_update_ids:
                                expected_id = self._last_update_ids[symbol] + 1
                                if last_update_id != expected_id:
                                    self.logger().warning(
                                        f"{symbol}: Sequence gap detected "
                                        f"(expected {expected_id}, got {last_update_id}). "
                                        "Clearing state and waiting for next WS snapshot."
                                    )
                                    # Clear state and wait for next WS snapshot
                                    self._last_update_ids.pop(symbol, None)
                                    self._awaiting_first_update.pop(symbol, None)
                                    continue  # Don't process this gapped update

                            self._last_update_ids[symbol] = last_update_id
                            self._message_queue[CONSTANTS.DIFF_EVENT_TYPE].put_nowait(data)
                    
                elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                    self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)

    async def _process_ob_snapshot(self, snapshot_queue: asyncio.Queue):
        message_queue = self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE]
        while True:
            try:
                json_msg = await message_queue.get()
                # self.logger().info(f"data in queue: {json_msg}")
                trading_pair = json_msg["symbol"]
                # trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                #     symbol=json_msg["symbol"])
                # Prefer server-provided time if present for monotonic update_ids
                ts_sec = self._time()
                try:
                    if isinstance(json_msg.get("time"), (int, float)):
                        ts_sec = float(json_msg.get("time")) * 1e-3
                except Exception:
                    pass
                # Pass the nested data node, not top-level
                order_book_message: OrderBookMessage = BingXOrderBook.snapshot_message_from_exchange_websocket(
                    json_msg["data"],  # This now contains action, lastUpdateId, bids, asks
                    ts_sec,
                    {"trading_pair": trading_pair}
                )
                snapshot_queue.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error when processing public order book updates from exchange")
                raise

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for incremental order book updates from @incrDepth stream.
        """
        message_queue = self._message_queue[CONSTANTS.DIFF_EVENT_TYPE]
        while True:
            try:
                json_msg = await message_queue.get()
                trading_pair = json_msg["symbol"]

                # Extract timestamp
                ts_sec = self._time()
                # BingX incrDepth has timestamp at root level
                if isinstance(json_msg.get("timestamp"), (int, float)):
                    ts_sec = float(json_msg.get("timestamp")) * 1e-3
                elif isinstance(json_msg.get("time"), (int, float)):
                    ts_sec = float(json_msg.get("time")) * 1e-3

                # Build diff message
                diff_message = BingXOrderBook.diff_message_from_exchange(
                    json_msg,
                    ts_sec,
                    {"trading_pair": trading_pair}
                )
                output.put_nowait(diff_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error processing incremental depth update",
                    exc_info=True
                )

    async def _recover_orderbook_snapshot(self, trading_pair: str):
        """
        Request recovery by clearing state and waiting for next WS snapshot.
        BingX sends periodic full snapshots with action='all' which will resync.
        """
        try:
            self.logger().warning(
                f"{trading_pair}: Detected sequence gap. "
                "Clearing orderbook state. Will resync on next WS snapshot."
            )

            # Clear state for this symbol
            self._last_update_ids.pop(trading_pair, None)
            self._awaiting_first_update.pop(trading_pair, None)

            # The next WS message with action="all" will reinitialize properly

        except Exception:
            self.logger().exception(f"Error during {trading_pair} recovery")

    async def _take_full_order_book_snapshot(self, trading_pairs: List[str], snapshot_queue: asyncio.Queue):
        for trading_pair in trading_pairs:
            try:
                snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair=trading_pair)
                snapshot_timestamp: float = float(snapshot["timestamp"]) * 1e-3
                snapshot_msg: OrderBookMessage = BingXOrderBook.snapshot_message_from_exchange_rest(
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