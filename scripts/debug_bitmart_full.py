#!/usr/bin/env python3
"""
Bitmart Orderbook Debug Script - Mirrors Hummingbot's Processing Logic
Self-contained script that recreates Hummingbot's Bitmart implementation:
1. WS message reception and classification (snapshot vs update/diff)
2. Message queue management with coalescing
3. Version tracking (_last_depth_version_by_symbol)
4. Staleness detection (_symbol_last_update_time)
5. OrderBook maintenance (bid/ask tracking with version UIDs)
6. Diff routing with snapshot_uid checks (OPTIMISTIC approach)

Debugging: diffs received, gaps detected, out-of-order updates, staleness time
"""
import asyncio
import gzip
import io
import json
import time
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import aiohttp
from enum import Enum


# =============================================================================
# CONFIGURATION
# =============================================================================
BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
DEPTH_CHANNEL = "spot/depth/increase100"
RUN_DURATION_SECONDS = 30 * 60  # 30 minutes
REPORT_INTERVAL = 30
MAX_QUEUE_SIZE = 30000
STALE_THRESHOLD_SECONDS = 60

# Trading pairs to debug (using Bitmart BASE-QUOTE format, will convert to BASE_QUOTE)
TRADING_PAIRS = [
    "BTC-USDT",  # High volume - to keep connection alive
    "PHL-USDT",
    "MINDFAK-USDT",
    "FREE-USDT",
    "LOBO-USDT",
    "LAB-USDT",
    "SAHARA-USDT",
    "JYAI-USDT",
    "JELLYJELLY-USDT",
    "EDEN-USDT",
]


# =============================================================================
# MESSAGE TYPES (mirrors OrderBookMessageType)
# =============================================================================
class MessageType(Enum):
    SNAPSHOT = 1
    DIFF = 2


@dataclass
class OrderBookMessage:
    """Mirrors hummingbot.core.data_type.order_book_message.OrderBookMessage"""
    msg_type: MessageType
    trading_pair: str
    update_id: int  # version number in Bitmart
    timestamp: float
    bids: List[Tuple[str, str]]  # [(price, amount), ...]
    asks: List[Tuple[str, str]]


# =============================================================================
# UTILITIES
# =============================================================================
def decompress_ws_message(data):
    """Decompress Bitmart WS messages (gzip or zlib/deflate)"""
    if not isinstance(data, (bytes, bytearray)):
        return data
    try:
        # Try gzip first
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
            return gz.read().decode('utf-8')
    except Exception:
        pass
    try:
        # Try raw deflate (zlib without header)
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('utf-8')
    except Exception:
        pass
    try:
        return data.decode('utf-8', errors='ignore')
    except Exception:
        return None


def to_bitmart_symbol(pair: str) -> str:
    """Convert PAIR-QUOTE to PAIR_QUOTE format"""
    return pair.replace("-", "_")


def from_bitmart_symbol(symbol: str) -> str:
    """Convert PAIR_QUOTE to PAIR-QUOTE format"""
    return symbol.replace("_", "-")


# =============================================================================
# LOCAL ORDERBOOK (mirrors OrderBook Cython class logic)
# =============================================================================
class LocalOrderBook:
    """Mirrors hummingbot.core.data_type.order_book.OrderBook"""
    
    def __init__(self, trading_pair: str):
        self.trading_pair = trading_pair
        self._bid_book: Dict[float, float] = {}  # price -> amount
        self._ask_book: Dict[float, float] = {}
        self._snapshot_uid: int = 0
        self._last_diff_uid: int = 0
        self._best_bid: float = 0.0
        self._best_ask: float = float('inf')
        self._last_applied_diff: float = 0
    
    @property
    def snapshot_uid(self) -> int:
        return self._snapshot_uid
    
    def apply_snapshot(self, bids: List[Tuple[str, str]], asks: List[Tuple[str, str]], update_id: int):
        """Mirrors c_apply_snapshot: clears book, inserts all entries, sets snapshot_uid"""
        self._bid_book.clear()
        self._ask_book.clear()
        
        for price_str, amount_str in bids:
            price, amount = float(price_str), float(amount_str)
            if amount > 0:
                self._bid_book[price] = amount
        
        for price_str, amount_str in asks:
            price, amount = float(price_str), float(amount_str)
            if amount > 0:
                self._ask_book[price] = amount
        
        self._update_best_prices()
        self._snapshot_uid = update_id
        self._last_applied_diff = time.perf_counter()
    
    def apply_diffs(self, bids: List[Tuple[str, str]], asks: List[Tuple[str, str]], update_id: int):
        """Mirrors c_apply_diffs: updates/removes entries based on amount"""
        for price_str, amount_str in bids:
            price, amount = float(price_str), float(amount_str)
            if amount == 0:
                self._bid_book.pop(price, None)
            else:
                self._bid_book[price] = amount
        
        for price_str, amount_str in asks:
            price, amount = float(price_str), float(amount_str)
            if amount == 0:
                self._ask_book.pop(price, None)
            else:
                self._ask_book[price] = amount
        
        self._update_best_prices()
        self._last_diff_uid = update_id
        self._last_applied_diff = time.perf_counter()
    
    def _update_best_prices(self):
        self._best_bid = max(self._bid_book.keys()) if self._bid_book else 0.0
        self._best_ask = min(self._ask_book.keys()) if self._ask_book else float('inf')
    
    @property
    def bid_levels(self) -> int:
        return len(self._bid_book)
    
    @property
    def ask_levels(self) -> int:
        return len(self._ask_book)
    
    @property
    def best_bid(self) -> float:
        return self._best_bid
    
    @property
    def best_ask(self) -> float:
        return self._best_ask
    
    @property
    def spread(self) -> float:
        if self._best_bid > 0 and self._best_ask < float('inf'):
            return self._best_ask - self._best_bid
        return 0.0
    
    @property
    def spread_pct(self) -> float:
        if self._best_bid > 0 and self._best_ask < float('inf'):
            mid = (self._best_bid + self._best_ask) / 2
            return ((self._best_ask - self._best_bid) / mid) * 100 if mid > 0 else 0
        return 0.0


# =============================================================================
# BITMART DATA SOURCE (mirrors BitmartAPIOrderBookDataSource logic)
# =============================================================================
class BitmartDataSource:
    """Mirrors key logic from bitmart_api_order_book_data_source.py"""
    
    def __init__(self, trading_pairs: List[str]):
        self.trading_pairs = trading_pairs
        self.symbols = [to_bitmart_symbol(p) for p in trading_pairs]
        self.symbol_to_pair: Dict[str, str] = {s: p for s, p in zip(self.symbols, trading_pairs)}
        
        # Message queues (mirrors _message_queue)
        self.snapshot_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.diff_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        
        # Version tracking (mirrors _last_depth_version_by_symbol)
        self._last_version_by_symbol: Dict[str, int] = {}
        
        # Staleness tracking (mirrors _symbol_last_update_time)
        self._symbol_last_update_time: Dict[str, float] = {}
        self._symbol_drop_count: Dict[str, int] = defaultdict(int)
        
        # Coalescing (mirrors _latest_diff_by_symbol)
        self._latest_diff_by_symbol: Dict[str, Any] = {}
        
        # First snapshot received tracking
        self._first_snapshot_received: Dict[str, bool] = {p: False for p in trading_pairs}
        
        # Stats
        self.stats: Dict[str, dict] = {p: {
            "snapshots": 0, "diffs": 0, "parsed_diffs": 0,
            "rejected_old_v": 0, "coalesced": 0,
            "tracker_rejected": 0, "applied": 0,
            "version_gaps": 0, "out_of_order": 0,
            "heartbeats": 0, "snapshot_requests": 0,
        } for p in trading_pairs}
        
        self.issues: List[str] = []
        self._update_intervals: Dict[str, List[float]] = {p: [] for p in trading_pairs}
    
    def process_ws_message(self, data: Dict[str, Any]) -> Optional[List[Tuple[str, str]]]:
        """
        Mirrors _process_ws_messages logic:
        1. Classify message by type (snapshot vs diff)
        2. Return list of (trading_pair, channel_type) processed
        """
        table = data.get("table")
        if table != DEPTH_CHANNEL:
            return None
        
        results = []
        data_items = data.get("data", []) or []
        
        for item in data_items:
            symbol = item.get("symbol")
            if symbol not in self.symbol_to_pair:
                continue
            
            trading_pair = self.symbol_to_pair[symbol]
            msg_type = (item.get("type") or "").lower()
            bids = item.get("bids", [])
            asks = item.get("asks", [])
            
            # Check for heartbeat (empty bids and asks)
            if not bids and not asks:
                self.stats[trading_pair]["heartbeats"] += 1
                continue
            
            if msg_type == "snapshot":
                channel = "snapshot"
                self._first_snapshot_received[trading_pair] = True
            elif self._first_snapshot_received[trading_pair]:
                channel = "diff"
            else:
                # Skip diffs before first snapshot
                continue
            
            results.append((trading_pair, channel, item))
        
        return results if results else None
    
    def parse_snapshot_message(self, item: Dict[str, Any], trading_pair: str) -> Optional[OrderBookMessage]:
        """Mirrors _process_ob_snapshot + BitmartOrderBook.snapshot_message_from_exchange_websocket"""
        symbol = item.get("symbol")
        version = item.get("version")
        
        # Update version tracking
        if version is not None:
            new_ver = int(version)
            self._last_version_by_symbol[symbol] = new_ver
            self._symbol_last_update_time[symbol] = time.time()
            self._symbol_drop_count[symbol] = 0
        
        ts = item.get("ms_t", 0)
        bids = item.get("bids", [])
        asks = item.get("asks", [])
        
        self.stats[trading_pair]["snapshots"] += 1
        
        return OrderBookMessage(
            msg_type=MessageType.SNAPSHOT,
            trading_pair=trading_pair,
            update_id=version,
            timestamp=ts * 1e-3 if ts else time.time(),
            bids=bids,
            asks=asks,
        )
    
    def parse_diff_message(self, item: Dict[str, Any], trading_pair: str) -> Optional[OrderBookMessage]:
        """
        Mirrors _parse_order_book_diff_message with OPTIMISTIC approach:
        - Accept all, just track for staleness and version gaps
        """
        symbol = item.get("symbol")
        version = item.get("version")
        
        stats = self.stats[trading_pair]
        stats["diffs"] += 1
        
        # Track update intervals
        now = time.time()
        last_ts = self._symbol_last_update_time.get(symbol, 0)
        if last_ts > 0:
            interval_ms = (now - last_ts) * 1000
            self._update_intervals[trading_pair].append(interval_ms)
            # Keep only last 1000 intervals
            if len(self._update_intervals[trading_pair]) > 1000:
                self._update_intervals[trading_pair] = self._update_intervals[trading_pair][-1000:]
        
        # OPTIMISTIC: Track version but don't reject (mirrors our changes)
        if version is not None:
            new_ver = int(version)
            last_ver = self._last_version_by_symbol.get(symbol)
            
            # Check for version issues (for stats only, NOT rejection)
            if last_ver is not None:
                if new_ver < last_ver:
                    stats["out_of_order"] += 1
                    self.issues.append(f"{trading_pair}: Out of order! last_v={last_ver}, new_v={new_ver}")
                elif new_ver > last_ver + 1:
                    gap = new_ver - last_ver
                    stats["version_gaps"] += 1
                    if gap > 10:
                        self.issues.append(f"{trading_pair}: Version gap of {gap} ({last_ver} -> {new_ver})")
            
            # Update tracking (always, per optimistic approach)
            self._last_version_by_symbol[symbol] = new_ver
            self._symbol_last_update_time[symbol] = now
            self._symbol_drop_count[symbol] = 0
        
        ts = item.get("ms_t", 0)
        bids = item.get("bids", [])
        asks = item.get("asks", [])
        
        stats["parsed_diffs"] += 1
        
        return OrderBookMessage(
            msg_type=MessageType.DIFF,
            trading_pair=trading_pair,
            update_id=version,
            timestamp=ts * 1e-3 if ts else time.time(),
            bids=bids,
            asks=asks,
        )
    
    def get_staleness(self, trading_pair: str) -> float:
        """Get current staleness in seconds"""
        symbol = to_bitmart_symbol(trading_pair)
        last_ts = self._symbol_last_update_time.get(symbol, 0)
        return time.time() - last_ts if last_ts > 0 else 0
    
    def get_interval_stats(self, trading_pair: str) -> Dict[str, float]:
        """Get update interval statistics for a trading pair"""
        intervals = self._update_intervals.get(trading_pair, [])
        if not intervals:
            return {"avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0}
        
        sorted_intervals = sorted(intervals)
        n = len(sorted_intervals)
        return {
            "avg": sum(intervals) / n,
            "min": min(intervals),
            "max": max(intervals),
            "p50": sorted_intervals[n // 2],
            "p95": sorted_intervals[int(n * 0.95)] if n > 20 else sorted_intervals[-1],
        }


# =============================================================================
# ORDER BOOK TRACKER (mirrors OrderBookTracker logic)
# =============================================================================
class LocalOrderBookTracker:
    """Mirrors hummingbot.core.data_type.order_book_tracker.OrderBookTracker"""
    
    def __init__(self, trading_pairs: List[str], data_source: BitmartDataSource):
        self.trading_pairs = trading_pairs
        self.data_source = data_source
        self.order_books: Dict[str, LocalOrderBook] = {p: LocalOrderBook(p) for p in trading_pairs}
        
        # Per-pair message queues (mirrors _tracking_message_queues)
        self.tracking_queues: Dict[str, asyncio.Queue] = {p: asyncio.Queue() for p in trading_pairs}
        
        # Saved messages before snapshot (mirrors _saved_message_queues)
        self.saved_messages: Dict[str, deque] = {p: deque(maxlen=1000) for p in trading_pairs}
        
        # Past diffs window for snapshot replay (mirrors restore_from_snapshot_and_diffs)
        self.past_diffs: Dict[str, deque] = {p: deque(maxlen=1000) for p in trading_pairs}
    
    def route_diff(self, message: OrderBookMessage) -> bool:
        """
        Mirrors _order_book_diff_router with OPTIMISTIC approach:
        - Always route, just track stats
        """
        trading_pair = message.trading_pair
        order_book = self.order_books[trading_pair]
        stats = self.data_source.stats[trading_pair]
        
        # OPTIMISTIC: Route even if version seems stale (mirrors our changes)
        if order_book.snapshot_uid > message.update_id:
            stats["tracker_rejected"] += 1
            # Still route! (per optimistic approach)
        
        # Route to tracking queue
        self.tracking_queues[trading_pair].put_nowait(message)
        return True
    
    def process_snapshot(self, message: OrderBookMessage):
        """Process snapshot message - apply to order book"""
        trading_pair = message.trading_pair
        order_book = self.order_books[trading_pair]
        
        # Apply snapshot directly
        order_book.apply_snapshot(message.bids, message.asks, message.update_id)
        
        # Track in past diffs for potential replay
        self.past_diffs[trading_pair].clear()
    
    def process_diff(self, message: OrderBookMessage):
        """Process diff message - apply to order book"""
        trading_pair = message.trading_pair
        order_book = self.order_books[trading_pair]
        stats = self.data_source.stats[trading_pair]
        
        # Apply diff
        order_book.apply_diffs(message.bids, message.asks, message.update_id)
        stats["applied"] += 1
        
        # Track in past diffs
        self.past_diffs[trading_pair].append(message)


# =============================================================================
# LIVENESS CONSTANTS (mirrors Hummingbot's BitmartAPIOrderBookDataSource)
# =============================================================================
PING_INTERVAL_SECONDS = 15.0  # < 20s per BitMart docs
FORCE_RECONNECT_IDLE_SECONDS = 30.0  # Reconnect if NO messages for this long
MESSAGE_TIMEOUT_SECONDS = 60.0  # Receive timeout per message (like Hummingbot)
WATCHDOG_CHECK_INTERVAL = 5.0  # Check watchdogs every 5s


async def run_debug():
    print("=" * 120)
    print("BITMART ORDERBOOK DEBUG - Mirroring Hummingbot's Processing Logic")
    print(f"Testing {len(TRADING_PAIRS)} pairs for {RUN_DURATION_SECONDS // 60} minutes")
    print(f"Channel: {DEPTH_CHANNEL}")
    print("=" * 120)
    
    # Create data source and tracker
    data_source = BitmartDataSource(TRADING_PAIRS)
    tracker = LocalOrderBookTracker(TRADING_PAIRS, data_source)
    
    start_time = time.time()
    last_report_time = start_time
    reconnect_count = 0
    
    async with aiohttp.ClientSession() as session:
        while time.time() - start_time < RUN_DURATION_SECONDS:
            reconnect_count += 1
            connection_start = time.time()
            print(f"\n[{time.strftime('%H:%M:%S')}] --- CONNECTING TO BITMART WS (attempt #{reconnect_count}) ---")
            
            # Liveness tracking (mirrors Hummingbot's ws_connection.py)
            last_recv_time = time.time()
            last_ping_sent_time = 0.0
            ping_count = 0
            pong_count = 0
            consumer_task = None
            
            # Debug counters
            total_msg_count = 0
            text_msg_count = 0
            binary_msg_count = 0
            
            try:
                async with session.ws_connect(
                    BITMART_WS_URL,
                    headers={"Accept-Encoding": "gzip"},
                    autoping=False,
                    heartbeat=None,  # Matches Hummingbot: ping_timeout=None -> heartbeat=None
                ) as ws:
                    # Subscribe to all symbols in chunks (to avoid rate limits)
                    CHUNK_SIZE = 5
                    for i in range(0, len(data_source.symbols), CHUNK_SIZE):
                        chunk = data_source.symbols[i:i + CHUNK_SIZE]
                        topics = [f"{DEPTH_CHANNEL}:{s}" for s in chunk]
                        await ws.send_json({"op": "subscribe", "args": topics})
                        print(f"[{time.strftime('%H:%M:%S')}] Subscribed: {topics}")
                        await asyncio.sleep(0.12)  # 0.12s like Hummingbot
                    
                    print(f"\n[{time.strftime('%H:%M:%S')}] Starting processing loop (Hummingbot-style)...\n")
                    
                    # Message processing function (mirrors _process_ws_messages_consumer)
                    async def message_consumer():
                        nonlocal last_recv_time, pong_count, total_msg_count, text_msg_count, binary_msg_count
                        nonlocal last_report_time
                        
                        while True:
                            try:
                                # Block with 60s timeout like Hummingbot (message_timeout=60)
                                msg = await asyncio.wait_for(ws.receive(), timeout=MESSAGE_TIMEOUT_SECONDS)
                            except asyncio.TimeoutError:
                                print(f"[{time.strftime('%H:%M:%S')}] ⏱️ Message timeout ({MESSAGE_TIMEOUT_SECONDS}s)")
                                raise ConnectionError("Message receive timeout")
                            
                            # Update last recv time on any message (mirrors ws_connection._update_last_recv_time)
                            last_recv_time = time.time()
                            total_msg_count += 1
                            
                            # Handle message types (mirrors ws_connection._check_msg_types)
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                binary_msg_count += 1
                                text = decompress_ws_message(msg.data)
                                if text is None:
                                    continue
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                text_msg_count += 1
                                text = msg.data
                            elif msg.type == aiohttp.WSMsgType.PING:
                                # Respond to server ping with pong (like Hummingbot does)
                                await ws.pong(msg.data)
                                continue
                            elif msg.type == aiohttp.WSMsgType.PONG:
                                pong_count += 1
                                print(f"[{time.strftime('%H:%M:%S')}] 🏓 Received pong #{pong_count}")
                                continue
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                                close_code = getattr(ws, 'close_code', 'unknown')
                                raise ConnectionError(f"WS closed unexpectedly. Close code={close_code}, data={msg.data}")
                            else:
                                continue
                            
                            # Handle pong TEXT responses (BitMart uses text "pong")
                            if text and text.strip().lower() == "pong":
                                pong_count += 1
                                print(f"[{time.strftime('%H:%M:%S')}] 🏓 Received text pong #{pong_count}")
                                continue
                            
                            # Parse JSON
                            try:
                                json_data = json.loads(text)
                            except Exception:
                                continue
                            
                            # Handle error messages (mirrors _process_ws_messages_consumer error handling)
                            if isinstance(json_data, dict) and ("errorCode" in json_data or "errorMessage" in json_data):
                                print(f"[{time.strftime('%H:%M:%S')}] ❌ Error: {json_data}")
                                raise ConnectionError(f"BitMart WS error: {json_data}")
                            
                            # Handle subscription events
                            if "event" in json_data:
                                event = json_data.get("event")
                                if event == "subscribe":
                                    topic = json_data.get("topic") or json_data.get("arg", {}).get("channel", "unknown")
                                    print(f"[{time.strftime('%H:%M:%S')}] ✅ Subscribed: {topic}")
                                continue
                            
                            # Route to message queues (mirrors _channel_originating_message logic)
                            results = data_source.process_ws_message(json_data)
                            if not results:
                                continue
                            
                            for trading_pair, channel, item in results:
                                if channel == "snapshot":
                                    parsed = data_source.parse_snapshot_message(item, trading_pair)
                                    if parsed:
                                        tracker.process_snapshot(parsed)
                                        print(f"[{time.strftime('%H:%M:%S')}] 📸 {trading_pair} SNAPSHOT: "
                                              f"v={parsed.update_id}, bids={len(parsed.bids)}, asks={len(parsed.asks)}")
                                elif channel == "diff":
                                    parsed = data_source.parse_diff_message(item, trading_pair)
                                    if parsed:
                                        tracker.route_diff(parsed)
                                        tracker.process_diff(parsed)
                    
                    # Create consumer task (mirrors Hummingbot's _ws_consumer_task pattern)
                    consumer_task = asyncio.create_task(message_consumer())
                    
                    # Main event loop with watchdog (mirrors listen_for_subscriptions pattern)
                    while True:
                        now = time.time()
                        elapsed = now - start_time
                        
                        if elapsed >= RUN_DURATION_SECONDS:
                            break
                        
                        # Create timer for next watchdog check
                        watchdog_timer = asyncio.create_task(asyncio.sleep(WATCHDOG_CHECK_INTERVAL))
                        
                        done, pending = await asyncio.wait(
                            {consumer_task, watchdog_timer},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        
                        # Consumer finished -> check for exception and reconnect
                        if consumer_task in done:
                            watchdog_timer.cancel()
                            exc = None
                            try:
                                exc = consumer_task.exception()
                            except asyncio.CancelledError:
                                raise
                            if exc:
                                raise exc
                            raise ConnectionError("Consumer ended unexpectedly")
                        
                        # Watchdog timer expired -> run liveness checks
                        if watchdog_timer in done:
                            # Check for idle/stale connection
                            idle_seconds = now - last_recv_time
                            if idle_seconds >= FORCE_RECONNECT_IDLE_SECONDS:
                                print(f"[{time.strftime('%H:%M:%S')}] ⚠️ No messages for {idle_seconds:.1f}s, forcing reconnect")
                                print(f"[{time.strftime('%H:%M:%S')}]    Debug: total={total_msg_count}, text={text_msg_count}, binary={binary_msg_count}")
                                raise ConnectionError("Idle threshold exceeded")
                            
                            # Send ping if needed (mirrors _check_and_send_ping_if_needed)
                            if (now - last_recv_time) >= PING_INTERVAL_SECONDS and (now - last_ping_sent_time) >= PING_INTERVAL_SECONDS:
                                try:
                                    await ws.send_str("ping")
                                    ping_count += 1
                                    last_ping_sent_time = now
                                    print(f"[{time.strftime('%H:%M:%S')}] 🏓 Sent ping #{ping_count}")
                                except Exception as e:
                                    print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Ping failed: {e}")
                                    raise ConnectionError(f"Ping failed: {e}")
                            
                            # Periodic report
                            if now - last_report_time >= REPORT_INTERVAL:
                                last_report_time = now
                                conn_duration = now - connection_start
                                print(f"\n[{time.strftime('%H:%M:%S')}] 🔗 Connection: duration={conn_duration:.1f}s, idle={idle_seconds:.1f}s, pings={ping_count}, pongs={pong_count}")
                                print_report(data_source, tracker, elapsed)
                    
                    conn_duration = time.time() - connection_start
                    print(f"[{time.strftime('%H:%M:%S')}] 🔌 Connection ended after {conn_duration:.1f}s, pings={ping_count}, pongs={pong_count}")
            
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] ❌ Connection error: {e}")
            
            finally:
                if consumer_task and not consumer_task.done():
                    consumer_task.cancel()
                    try:
                        await consumer_task
                    except Exception:
                        pass
            
            # Reconnect delay
            if time.time() - start_time < RUN_DURATION_SECONDS:
                print(f"[{time.strftime('%H:%M:%S')}] Reconnecting in 3s...")
                await asyncio.sleep(3)
    
    # Final report
    print(f"\n[{time.strftime('%H:%M:%S')}] Total reconnect attempts: {reconnect_count}")
    print_final_report(data_source, tracker, time.time() - start_time)


def print_report(data_source: BitmartDataSource, tracker: LocalOrderBookTracker, elapsed: float):
    """Print periodic status report"""
    mins, secs = int(elapsed // 60), int(elapsed % 60)
    
    print(f"\n{'='*140}")
    print(f"REPORT - Elapsed: {mins}m {secs}s")
    print(f"{'='*140}")
    print(f"{'Pair':16} | {'Snaps':>5} | {'Diffs':>6} | {'Applied':>7} | "
          f"{'Gaps':>4} | {'OoO':>4} | {'Stale':>6} | {'AvgInt':>8} | "
          f"{'Bids':>4} | {'Asks':>4} | {'BestBid':>12} | {'BestAsk':>12} | {'Spread%':>8}")
    print("-" * 140)
    
    for pair in sorted(data_source.trading_pairs):
        stats = data_source.stats[pair]
        ob = tracker.order_books[pair]
        staleness = data_source.get_staleness(pair)
        interval_stats = data_source.get_interval_stats(pair)
        
        status = "🔴" if staleness > 10 or stats["out_of_order"] > 0 or stats["version_gaps"] > 5 else "🟢"
        
        best_bid = f"{ob.best_bid:.6f}" if ob.best_bid > 0 else "N/A"
        best_ask = f"{ob.best_ask:.6f}" if ob.best_ask < float('inf') else "N/A"
        spread_pct = f"{ob.spread_pct:.4f}%" if ob.spread_pct > 0 else "N/A"
        
        print(f"{status}{pair:15} | {stats['snapshots']:>5} | {stats['diffs']:>6} | {stats['applied']:>7} | "
              f"{stats['version_gaps']:>4} | {stats['out_of_order']:>4} | {staleness:>5.1f}s | "
              f"{interval_stats['avg']:>6.1f}ms | "
              f"{ob.bid_levels:>4} | {ob.ask_levels:>4} | {best_bid:>12} | {best_ask:>12} | {spread_pct:>8}")
    
    total_diffs = sum(s["diffs"] for s in data_source.stats.values())
    total_heartbeats = sum(s["heartbeats"] for s in data_source.stats.values())
    print(f"{'='*140}")
    print(f"TOTALS: {total_diffs} diffs, {total_heartbeats} heartbeats")
    print(f"{'='*140}\n")


def print_final_report(data_source: BitmartDataSource, tracker: LocalOrderBookTracker, total_time: float):
    """Print final summary"""
    mins, secs = int(total_time // 60), int(total_time % 60)
    
    print(f"\n{'#'*140}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*140}")
    
    total_diffs = sum(s["diffs"] for s in data_source.stats.values())
    total_applied = sum(s["applied"] for s in data_source.stats.values())
    total_gaps = sum(s["version_gaps"] for s in data_source.stats.values())
    total_ooo = sum(s["out_of_order"] for s in data_source.stats.values())
    total_heartbeats = sum(s["heartbeats"] for s in data_source.stats.values())
    
    print(f"\nOVERALL STATS:")
    print(f"  Total WS Diffs Received: {total_diffs}")
    print(f"  Total Diffs Applied: {total_applied}")
    print(f"  Total Version Gaps: {total_gaps}")
    print(f"  Total Out-of-Order: {total_ooo}")
    print(f"  Total Heartbeats: {total_heartbeats}")
    print(f"  Total Issues: {len(data_source.issues)}")
    
    # Aggregate update intervals
    all_intervals = []
    for pair in data_source.trading_pairs:
        all_intervals.extend(data_source._update_intervals.get(pair, []))
    
    if all_intervals:
        sorted_intervals = sorted(all_intervals)
        n = len(sorted_intervals)
        avg = sum(all_intervals) / n
        p50 = sorted_intervals[n // 2]
        p95 = sorted_intervals[int(n * 0.95)] if n > 20 else sorted_intervals[-1]
        p99 = sorted_intervals[int(n * 0.99)] if n > 100 else sorted_intervals[-1]
        
        print(f"\nUPDATE INTERVAL ANALYSIS:")
        print(f"  Average: {avg:.1f}ms")
        print(f"  Median (p50): {p50:.1f}ms")
        print(f"  p95: {p95:.1f}ms")
        print(f"  p99: {p99:.1f}ms")
        print(f"  Min: {min(all_intervals):.1f}ms")
        print(f"  Max: {max(all_intervals):.1f}ms")
        
        under_150ms = sum(1 for i in all_intervals if i < 150)
        pct_under_150 = (under_150ms / len(all_intervals)) * 100
        print(f"\n  Updates under 150ms: {under_150ms}/{len(all_intervals)} ({pct_under_150:.1f}%)")
        
        if avg < 150:
            print(f"\n✅ CONFIRMED: Receiving ~100ms updates (avg={avg:.1f}ms)")
        else:
            print(f"\n⚠️ Updates slower than expected (avg={avg:.1f}ms, expected ~100ms)")
    
    # Per-symbol summary
    print(f"\nPER-SYMBOL SUMMARY:")
    print("-" * 140)
    print(f"{'Pair':16} | {'Snaps':>5} | {'Diffs':>7} | {'Rate/min':>9} | {'MaxStale':>8} | "
          f"{'Gaps':>5} | {'OoO':>4} | {'AvgInt':>8} | Status")
    print("-" * 140)
    
    for pair in sorted(data_source.trading_pairs):
        stats = data_source.stats[pair]
        rate = stats["diffs"] / (total_time / 60) if total_time > 0 else 0
        staleness = data_source.get_staleness(pair)
        interval_stats = data_source.get_interval_stats(pair)
        
        status = "HEALTHY" if stats["version_gaps"] == 0 and stats["out_of_order"] == 0 else "ISSUES"
        emoji = "✅" if status == "HEALTHY" else "⚠️"
        
        print(f"{pair:16} | {stats['snapshots']:>5} | {stats['diffs']:>7} | {rate:>8.1f}/m | "
              f"{staleness:>7.1f}s | {stats['version_gaps']:>5} | {stats['out_of_order']:>4} | "
              f"{interval_stats['avg']:>6.1f}ms | {emoji} {status}")
    
    print("-" * 140)
    
    if data_source.issues:
        print(f"\nISSUES LOG ({len(data_source.issues)} total):")
        for i, issue in enumerate(data_source.issues[:30], 1):
            print(f"  {i}. {issue}")
        if len(data_source.issues) > 30:
            print(f"  ... and {len(data_source.issues) - 30} more")
    
    print(f"\n{'#'*140}")
    
    if total_gaps == 0 and total_ooo == 0:
        print("\n✅ ALL HEALTHY - No sequence issues detected")
    else:
        print(f"\n⚠️ ISSUES: {total_gaps} gaps, {total_ooo} out-of-order")


async def main():
    print("\n" + "=" * 120)
    print("Starting Bitmart Orderbook Debug...")
    print(f"Pairs: {TRADING_PAIRS}")
    print("Press Ctrl+C to stop early")
    print("=" * 120 + "\n")
    
    try:
        await run_debug()
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
