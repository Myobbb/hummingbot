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
        # Larger queue for high-frequency bursts
        MAX_QUEUE_SIZE = 10000
        self._message_queue: Dict[str, asyncio.Queue] = {
            CONSTANTS.SNAPSHOT_EVENT_TYPE: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
            CONSTANTS.DIFF_EVENT_TYPE: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
            CONSTANTS.TRADE_EVENT_TYPE: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
        }
        self._last_ws_message_sent_timestamp = 0
        # Simplified state tracking - only what we need
        self._last_update_ids: Dict[str, int] = {}
        self._snapshot_output_queue: Optional[asyncio.Queue] = None

        self._level_first_seen: Dict[str, float] = {}  # key: "pair_side_price" -> timestamp
        self._enable_anti_spoofing = CONSTANTS.ENABLE_ANTI_SPOOFING

        self._confirmed_bids: Dict[str, Dict[str, str]] = {}  # pair -> {price: qty}
        self._confirmed_asks: Dict[str, Dict[str, str]] = {}  # pair -> {price: qty}

    def _is_level_stable(self, price: str, side: str, trading_pair: str, current_time: float) -> bool:
        """Check if price level has existed for minimum duration (500ms = ~2 snapshots)"""
        if not self._enable_anti_spoofing:
            return True
            
        key = f"{trading_pair}_{side}_{price}"
        
        if key not in self._level_first_seen:
            self._level_first_seen[key] = current_time
            return False
        
        time_existed_ms = (current_time - self._level_first_seen[key]) * 1000
        return time_existed_ms >= CONSTANTS.SPOOFING_MIN_CONFIRMATION_TIME_MS

    def _update_confirmed_state(self, 
                           bids: List[List[str]], 
                           asks: List[List[str]],
                           trading_pair: str,
                           timestamp: float) -> tuple:
        """
        Update confirmed orderbook state with stable levels, keeping previous confirmed levels.
        Returns (confirmed_bids, confirmed_asks) as lists ready for orderbook message.
        """
        if not self._enable_anti_spoofing:
            return bids, asks
        
        # Initialize confirmed state for new pairs
        if trading_pair not in self._confirmed_bids:
            self._confirmed_bids[trading_pair] = {}
            self._confirmed_asks[trading_pair] = {}
        
        # Clean up stale tracking entries (levels not seen in last 5 seconds)
        cutoff_time = timestamp - 5.0
        self._level_first_seen = {
            k: v for k, v in self._level_first_seen.items() 
            if v > cutoff_time
        }
        
        # Update confirmed state with stable levels only
        for price, qty in bids:
            if self._is_level_stable(price, "bid", trading_pair, timestamp):
                if qty == "0" or qty == 0:
                    # Remove level
                    self._confirmed_bids[trading_pair].pop(price, None)
                else:
                    # Update level
                    self._confirmed_bids[trading_pair][price] = qty
        
        for price, qty in asks:
            if self._is_level_stable(price, "ask", trading_pair, timestamp):
                if qty == "0" or qty == 0:
                    # Remove level
                    self._confirmed_asks[trading_pair].pop(price, None)
                else:
                    # Update level
                    self._confirmed_asks[trading_pair][price] = qty
        
        # Return confirmed state as lists
        confirmed_bids = [[price, qty] for price, qty in self._confirmed_bids[trading_pair].items()]
        confirmed_asks = [[price, qty] for price, qty in self._confirmed_asks[trading_pair].items()]
        
        return confirmed_bids, confirmed_asks


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
        params = {
            "symbol": trading_pair,
            "limit": str(CONSTANTS.SNAPSHOT_DEPTH_LIMIT)
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
        sub['lastUpdateId'] = 0  # Don't trust REST sequence

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
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
            finally:
                ws and await ws.disconnect()
                await self._sleep(5.0)

    async def _subscribe_channels(self, ws: WSAssistant):
        MAX_SUBS = 200
        total_needed = len(self._trading_pairs) * 2

        if total_needed > MAX_SUBS:
            self.logger().warning(
                f"Subscriptions ({total_needed}) exceed BingX limit ({MAX_SUBS})"
            )

        for trading_pair in self._trading_pairs:
            trade_req = WSJSONRequest(payload={
                "id": f"trade_{trading_pair}",
                "reqType": "sub",
                "dataType": f"{trading_pair}@trade"
            })
            depth_req = WSJSONRequest(payload={
                "id": f"depth_{trading_pair}",
                "reqType": "sub",
                "dataType": f"{trading_pair}@depth{CONSTANTS.BINGX_DEPTH_LEVEL}"
            })
            await ws.send(trade_req)
            await ws.send(depth_req)

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

            # Handle depth messages (market depth or incremental depth)
            if event_type.startswith("depth"):
                await self._handle_depth_message(data, symbol)
            elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                try:
                    self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)
                except asyncio.QueueFull:
                    self.logger().warning(f"Trade queue full for {symbol}")

    async def _handle_depth_message(self, data: Dict[str, Any], symbol: str):
        """
        Handle market depth messages (full snapshots only).
        Market depth endpoint sends full snapshots every 300ms, no incremental updates.
        """
        # All depth messages from @depth endpoint are full snapshots
        try:
            self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE].put_nowait(data)
        except asyncio.QueueFull:
            self.logger().warning(f"{symbol}: Snapshot queue full")
            # If queue is full, log but don't trigger recovery since next snapshot will arrive in 300ms

    async def _process_ob_snapshot(self, snapshot_queue: asyncio.Queue):
        message_queue = self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE]
        while True:
            json_msg = await message_queue.get()
            trading_pair = json_msg["symbol"]
            
            # Extract timestamp once
            ts_sec = self._extract_timestamp(json_msg, self._time())
            
            # SURGICAL: Update confirmed state with stable levels
            if self._enable_anti_spoofing and "data" in json_msg:
                data_node = json_msg["data"]
                raw_bids = data_node.get("bids") or data_node.get("b") or []
                raw_asks = data_node.get("asks") or data_node.get("a") or []
                
                normalized_bids = BingXOrderBook._normalize_levels(raw_bids)
                normalized_asks = BingXOrderBook._normalize_levels(raw_asks)
                
                # Update confirmed state and get confirmed levels
                confirmed_bids, confirmed_asks = self._update_confirmed_state(
                    normalized_bids, normalized_asks, trading_pair, ts_sec
                )
                
                # Replace in message
                json_msg["data"]["bids"] = confirmed_bids
                json_msg["data"]["asks"] = confirmed_asks

            
            # Original logic preserved: create and queue message
            order_book_message = BingXOrderBook.snapshot_message_from_exchange_websocket(
                json_msg, ts_sec, {"trading_pair": trading_pair}
            )
            snapshot_queue.put_nowait(order_book_message)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Optimized diff consumer with batching"""
        message_queue = self._message_queue[CONSTANTS.DIFF_EVENT_TYPE]
        
        while True:
            try:
                # Block for first message
                json_msg = await message_queue.get()
                trading_pair = json_msg["symbol"]
                
                # Extract timestamp once
                ts_sec = self._extract_timestamp(json_msg, self._time())
                
                # SURGICAL: Filter spoof levels before creating message
                if self._enable_anti_spoofing and "data" in json_msg:
                    data_node = json_msg["data"]
                    raw_bids = data_node.get("bids") or data_node.get("b") or []
                    raw_asks = data_node.get("asks") or data_node.get("a") or []
                    
                    normalized_bids = BingXOrderBook._normalize_levels(raw_bids)
                    normalized_asks = BingXOrderBook._normalize_levels(raw_asks)
                    
                    # Update confirmed state and get confirmed levels
                    confirmed_bids, confirmed_asks = self._update_confirmed_state(
                        normalized_bids, normalized_asks, trading_pair, ts_sec
                    )
                    
                    json_msg["data"]["bids"] = confirmed_bids
                    json_msg["data"]["asks"] = confirmed_asks
                
                # CRITICAL: Create metadata dict once (reused in diff_message_from_exchange)
                metadata = {"trading_pair": trading_pair}
                diff_message = BingXOrderBook.diff_message_from_exchange(
                    json_msg, ts_sec, metadata
                )
                output.put_nowait(diff_message)

                # Drain queue without blocking (original batch logic)
                while True:
                    try:
                        json_msg = message_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    
                    trading_pair = json_msg["symbol"]
                    ts_sec = self._extract_timestamp(json_msg, self._time())
                    
                    if self._enable_anti_spoofing and "data" in json_msg:
                        data_node = json_msg["data"]
                        raw_bids = data_node.get("bids") or data_node.get("b") or []
                        raw_asks = data_node.get("asks") or data_node.get("a") or []
                        
                        normalized_bids = BingXOrderBook._normalize_levels(raw_bids)
                        normalized_asks = BingXOrderBook._normalize_levels(raw_asks)
                        
                        # Update confirmed state and get confirmed levels
                        confirmed_bids, confirmed_asks = self._update_confirmed_state(
                            normalized_bids, normalized_asks, trading_pair, ts_sec
                        )
                        
                        # Replace in message
                        json_msg["data"]["bids"] = confirmed_bids
                        json_msg["data"]["asks"] = confirmed_asks

                    metadata = {"trading_pair": trading_pair}
                    diff_message = BingXOrderBook.diff_message_from_exchange(
                        json_msg, ts_sec, metadata
                    )
                    output.put_nowait(diff_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Diff processing error", exc_info=True)

    async def _trigger_immediate_recovery(self, trading_pair: str):
        """Clear state and fetch REST snapshot"""
        self._last_update_ids.pop(trading_pair, None)
        
        if self._snapshot_output_queue:
            try:
                await self._take_full_order_book_snapshot(
                    [trading_pair], 
                    self._snapshot_output_queue
                )
            except Exception:
                self.logger().exception(f"{trading_pair}: Recovery failed")

    async def _take_full_order_book_snapshot(self, trading_pairs: List[str], snapshot_queue: asyncio.Queue):
        for trading_pair in trading_pairs:
            try:
                snapshot = await self._request_order_book_snapshot(trading_pair)
                snapshot_timestamp = float(snapshot["timestamp"]) * 1e-3

                # Clear state for fresh start
                self._last_update_ids.pop(trading_pair, None)

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