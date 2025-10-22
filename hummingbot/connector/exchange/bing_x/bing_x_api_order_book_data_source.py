import asyncio
import json
import time
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
        self._domain = domain
        self._time_synchronizer = time_synchronizer
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
        )
        # Get configured depth level, default to 100
        depth_config = getattr(connector._client_config, 'bingx_orderbook_depth', None)
        self._depth_level = depth_config if depth_config else CONSTANTS.DEFAULT_DEPTH_LEVEL

        # Larger queue for high-frequency bursts
        MAX_QUEUE_SIZE = 10000
        self._message_queue: Dict[str, asyncio.Queue] = {
            CONSTANTS.SNAPSHOT_EVENT_TYPE: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
            CONSTANTS.TRADE_EVENT_TYPE: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
        }
        self._last_ws_message_sent_timestamp = 0
        self._snapshot_output_queue: Optional[asyncio.Queue] = None

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Legacy method for framework compatibility - not used in optimized flow"""
        trading_pair = raw_message["dataType"].split('@')[0]
        trade_message = BingXOrderBook.trade_message_from_exchange(
            raw_message["data"], {"trading_pair": trading_pair})
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Legacy method for framework compatibility - not used in optimized flow"""
        trading_pair = raw_message.get('dataType').split('@')[0]
        ts_sec = self._extract_timestamp(raw_message, self._time())
        order_book_message = BingXOrderBook.diff_message_from_exchange(
            raw_message, ts_sec, {"trading_pair": trading_pair})
        message_queue.put_nowait(order_book_message)

    @staticmethod
    def _extract_timestamp(msg: Dict[str, Any], default_time: float) -> float:
        """Fast timestamp extraction - call once per message"""
        time_raw = msg.get("time") or msg.get("ts")
        if isinstance(time_raw, (int, float)):
            return float(time_raw) * 1e-3
        return default_time

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        # Use configured depth for REST snapshot, convert incrDepth to 1000
        limit = "1000" if self._depth_level == "incrDepth" else self._depth_level
        params = {
            "symbol": trading_pair,
            "limit": limit
        }
        data = await self._connector._api_request(
            path_url=CONSTANTS.SNAPSHOT_PATH_URL,
            method=RESTMethod.GET,
            params=params
        )
        
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                raise IOError(f"Invalid snapshot response for {trading_pair}")

        if not isinstance(data, dict):
            raise IOError(f"Unexpected snapshot format for {trading_pair}")

        sub = data.get('data')
        if sub is None:
            if all(k in data for k in ("asks", "bids")):
                sub = data
            else:
                raise IOError(f"Snapshot missing 'data' for {trading_pair}")

        ts = data.get('timestamp') or data.get('ts') or int(self._time() * 1e3)
        if not isinstance(sub, dict):
            sub = dict(sub)
        sub['timestamp'] = ts
        # Use timestamp as update_id for REST snapshots
        sub['lastUpdateId'] = int(ts)

        return sub

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = float(snapshot["timestamp"]) * 1e-3
        return BingXOrderBook.snapshot_message_from_exchange_rest(
            snapshot, snapshot_timestamp, {"trading_pair": trading_pair}
        )

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._snapshot_output_queue = output
        while True:
            try:
                await asyncio.wait_for(
                    self._process_ob_snapshot(snapshot_queue=output), 
                    timeout=self.ONE_HOUR
                )
            except asyncio.TimeoutError:
                await self._take_full_order_book_snapshot(self._trading_pairs, output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Snapshot processing error", exc_info=True)
                await self._take_full_order_book_snapshot(self._trading_pairs, output)
                await self._sleep(5.0)

    async def listen_for_subscriptions(self):
        ws = None
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=CONSTANTS.WSS_PUBLIC_URL[self._domain],
                    ping_timeout=None,
                    message_timeout=60,
                    ws_headers={"Accept-Encoding": "gzip"},
                    max_msg_size=16 * 1024 * 1024,
                )
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()
                await self._process_ws_messages(ws=ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "WS error, reconnecting in 5s...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        MAX_SUBS = 200
        total_needed = len(self._trading_pairs) * 2

        if total_needed > MAX_SUBS:
            self.logger().warning(
                f"Subscriptions ({total_needed}) exceed BingX limit ({MAX_SUBS})"
            )

        # Build depth stream suffix based on configured level
        if self._depth_level == "incrDepth":
            depth_suffix = "@incrDepth"
        else:
            depth_suffix = f"@depth{self._depth_level}"

        for trading_pair in self._trading_pairs:
            """
            trade_req = WSJSONRequest(payload={
                "id": f"trade_{trading_pair}",
                "reqType": "sub",
                "dataType": f"{trading_pair}@trade"
            })
            """
            depth_req = WSJSONRequest(payload={
                "id": f"depth_{trading_pair}",
                "reqType": "sub",
                "dataType": f"{trading_pair}{depth_suffix}"
            })
            #await ws.send(trade_req)
            await ws.send(depth_req)

        self.logger().info(
            f"Subscribed to {len(self._trading_pairs)} trading pairs with depth level: {self._depth_level}"
        )

    async def _process_ws_messages(self, ws: WSAssistant):
        async for ws_response in ws.iter_messages():
            data = utils.decompress_ws_message(ws_response.data)
            
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    continue
            
            if not isinstance(data, dict):
                continue
                
            # Skip success confirmations
            if data.get("msg") == "SUCCESS":
                continue
            
            # Handle ping/pong
            if "ping" in data:
                pong = {"pong": data["ping"]}
                if "time" in data:
                    pong["time"] = data["time"]
                await ws.send(WSJSONRequest(payload=pong))
                self._last_ws_message_sent_timestamp = self._time()
                continue
            
            # Process market data
            data_type = data.get("dataType")
            if not data_type:
                continue
                
            parts = data_type.split('@')
            if len(parts) != 2:
                continue
                
            symbol, event_type = parts
            data['symbol'] = symbol

            if event_type in ("incrDepth", "depth"):
                await self._handle_depth_message(data, symbol)
            elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                try:
                    self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)
                except asyncio.QueueFull:
                    self.logger().warning(f"Trade queue full for {symbol}")

    async def _handle_depth_message(self, data: Dict[str, Any], symbol: str):
        """
        Handle orderbook depth messages from WebSocket.
        For @depth streams: all messages are snapshots, queue directly.
        For @incrDepth stream: handle action="all" (snapshot) and action="update" (diff).
        """
        data_node = data.get('data', {})

        # For incrDepth stream, check action field
        if self._depth_level == "incrDepth":
            action = data_node.get('action')

            # incrDepth snapshot (action="all")
            if action == "all":
                try:
                    self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].put_nowait(data)
                except asyncio.QueueFull:
                    self.logger().warning(f"{symbol}: Snapshot queue full")
                return

            # incrDepth diff (action="update") - not used in default mode
            # Legacy code handles this if user explicitly chooses incrDepth
            # For now, skip diff processing to keep code simple
            return

        # For @depth streams: all messages are full snapshots (no action field)
        # Queue directly to snapshot queue for fast processing
        while True:
            try:
                self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].put_nowait(data)
                break
            except asyncio.QueueFull:
                #self.logger().warning(f"{symbol}: Snapshot queue full, dropping oldest")
                # For real-time snapshots, newest is more important than oldest
                try:
                    self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].get_nowait()
                    self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].put_nowait(data)
                except (asyncio.QueueEmpty):
                    continue

    async def _process_ob_snapshot(self, snapshot_queue: asyncio.Queue):
        message_queue = self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE]
        while True:
            json_msg = await message_queue.get()
            trading_pair = json_msg["symbol"]
            
            # Extract timestamp once
            ts_sec = self._extract_timestamp(json_msg, self._time())
            
            order_book_message = BingXOrderBook.snapshot_message_from_exchange_websocket(
                json_msg, ts_sec, {"trading_pair": trading_pair}
            )
            snapshot_queue.put_nowait(order_book_message)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Framework-required method for diff updates.

        For @depth streams: No diffs, all updates are snapshots (this method does nothing).
        For @incrDepth stream: Would process diffs, but not implemented in simplified version.
        """
        if self._depth_level != "incrDepth":
            # Snapshot-only mode: no diffs to process, just wait indefinitely
            self.logger().info(
                f"Using @depth{self._depth_level} (snapshot-only), diff processing disabled for speed"
            )
            while True:
                await asyncio.sleep(3600)  # Sleep forever
        else:
            # incrDepth mode would need diff processing here
            # For simplicity, we don't implement it - user should use @depth for speed
            self.logger().warning(
                "incrDepth selected but diff processing not implemented. "
                "Use depth 5/10/20/50/100 for optimized snapshot-based updates."
            )
            while True:
                await asyncio.sleep(3600)

    async def _take_full_order_book_snapshot(self, trading_pairs: List[str], snapshot_queue: asyncio.Queue):
        for trading_pair in trading_pairs:
            try:
                snapshot = await self._request_order_book_snapshot(trading_pair)
                snapshot_timestamp = float(snapshot["timestamp"]) * 1e-3

                snapshot_msg = BingXOrderBook.snapshot_message_from_exchange_rest(
                    snapshot, snapshot_timestamp, {"trading_pair": trading_pair}
                )
                snapshot_queue.put_nowait(snapshot_msg)
                
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Snapshot fetch error for {trading_pair}",
                    exc_info=True
                )
                await self._sleep(5.0)

    def _time(self):
        return time.time()
