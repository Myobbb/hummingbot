#!/usr/bin/env python3
"""
Bybit Orderbook Debug Script - Mirrors Hummingbot's Processing Logic
Self-contained script that recreates Hummingbot's Bybit implementation:
1. WS message reception and classification (snapshot vs delta)
2. Message queue management with coalescing
3. Sequence tracking (_last_applied_u_by_symbol)
4. Staleness detection (_symbol_last_update_time)
5. OrderBook maintenance (bid/ask tracking with sequence UIDs)
6. Diff routing with snapshot_uid checks (now OPTIMISTIC)
"""
import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import aiohttp
from enum import Enum


# =============================================================================
# CONFIGURATION
# =============================================================================
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
DEPTH = 50
RUN_DURATION_SECONDS = 30 * 60  # 30 minutes
REPORT_INTERVAL = 30
MAX_QUEUE_SIZE = 30000
STALE_THRESHOLD_SECONDS = 60

TRADING_PAIRS = [
    "ROOT-USDT", "CORN-USDT", "F-USDT", "GAME-USDT", "PINEYE-USDT",
    "QORPO-USDT", "SVL-USDT", "ZKL-USDT", "SAHARA-USDT", "UXLINK-USDT", "FHE-USDT",
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
    update_id: int
    timestamp: float
    bids: List[Tuple[str, str]]  # [(price, amount), ...]
    asks: List[Tuple[str, str]]


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
    def spread(self) -> float:
        if self._best_bid > 0 and self._best_ask < float('inf'):
            return self._best_ask - self._best_bid
        return 0.0


# =============================================================================
# BYBIT DATA SOURCE (mirrors BybitAPIOrderBookDataSource logic)
# =============================================================================
class BybitDataSource:
    """Mirrors key logic from bybit_api_order_book_data_source.py"""
    
    def __init__(self, trading_pairs: List[str]):
        self.trading_pairs = trading_pairs
        self.symbols = [p.replace("-", "") for p in trading_pairs]
        self.symbol_to_pair: Dict[str, str] = {s: p for s, p in zip(self.symbols, trading_pairs)}
        
        # Message queues (mirrors _message_queue)
        self.snapshot_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.diff_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        
        # Sequence tracking (mirrors _last_applied_u_by_symbol)
        self._last_applied_u_by_symbol: Dict[str, int] = {}
        
        # Staleness tracking (mirrors _symbol_last_update_time)
        self._symbol_last_update_time: Dict[str, float] = {}
        self._symbol_drop_count: Dict[str, int] = defaultdict(int)
        
        # Coalescing (mirrors _latest_diff_by_symbol)
        self._latest_diff_by_symbol: Dict[str, Any] = {}
        
        # Stats
        self.stats: Dict[str, dict] = {p: {
            "snapshots": 0, "diffs": 0, "parsed_diffs": 0,
            "rejected_old_u": 0, "coalesced": 0,
            "tracker_rejected": 0, "applied": 0,
            "sequence_gaps": 0, "out_of_order": 0,
        } for p in trading_pairs}
        
        self.issues: List[str] = []
    
    async def process_ws_message(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Mirrors _process_ws_messages logic:
        1. Classify message by type (snapshot vs delta)
        2. Enqueue to appropriate queue
        """
        event_type = data.get("type")
        topic = data.get("topic", "")
        
        if "orderbook" not in topic:
            return None
        
        payload = data.get("data", {})
        symbol = payload.get("s")
        if symbol not in self.symbol_to_pair:
            return None
        
        trading_pair = self.symbol_to_pair[symbol]
        
        if event_type == "snapshot":
            channel = "snapshot"
            try:
                self.snapshot_queue.put_nowait(data)
            except asyncio.QueueFull:
                self._symbol_last_update_time[symbol] = time.time() - STALE_THRESHOLD_SECONDS - 1
                return None
        elif event_type == "delta":
            channel = "diff"
            # Check queue capacity for coalescing (mirrors queue overflow handling)
            if self.diff_queue.qsize() > int(0.9 * MAX_QUEUE_SIZE):
                self._latest_diff_by_symbol[symbol] = data
                self.stats[trading_pair]["coalesced"] += 1
                return None
            try:
                self.diff_queue.put_nowait(data)
            except asyncio.QueueFull:
                self._latest_diff_by_symbol[symbol] = data
                return None
        else:
            return None
        
        return channel
    
    def parse_snapshot_message(self, data: Dict[str, Any]) -> Optional[OrderBookMessage]:
        """Mirrors _process_ob_snapshot + BybitOrderBook.snapshot_message_from_exchange_websocket"""
        payload = data.get("data", {})
        symbol = payload.get("s")
        u = payload.get("u")
        
        if symbol not in self.symbol_to_pair:
            return None
        
        trading_pair = self.symbol_to_pair[symbol]
        
        # Update sequence tracking (mirrors line 822 in _process_ob_snapshot)
        if isinstance(u, int):
            self._last_applied_u_by_symbol[symbol] = u
            self._symbol_last_update_time[symbol] = time.time()
            self._symbol_drop_count[symbol] = 0
        
        ts = data.get("ts", 0)
        bids = payload.get("b", [])
        asks = payload.get("a", [])
        
        self.stats[trading_pair]["snapshots"] += 1
        
        return OrderBookMessage(
            msg_type=MessageType.SNAPSHOT,
            trading_pair=trading_pair,
            update_id=u,
            timestamp=ts * 1e-3 if ts else time.time(),
            bids=bids,
            asks=asks,
        )
    
    def parse_diff_message(self, data: Dict[str, Any]) -> Optional[OrderBookMessage]:
        """
        Mirrors _parse_order_book_diff_message with OPTIMISTIC approach:
        - OLD: if u <= last_u: return None (reject)
        - NEW: Accept all, just track for staleness
        """
        payload = data.get("data", {})
        symbol = payload.get("s")
        u = payload.get("u")
        
        if symbol not in self.symbol_to_pair:
            return None
        
        trading_pair = self.symbol_to_pair[symbol]
        stats = self.stats[trading_pair]
        stats["diffs"] += 1
        
        # OPTIMISTIC: Track u but don't reject (mirrors our changes)
        if isinstance(u, int):
            last_u = self._last_applied_u_by_symbol.get(symbol, -1)
            
            # Check for sequence issues (for stats only, NOT rejection)
            if last_u >= 0:
                if u < last_u:
                    stats["out_of_order"] += 1
                    self.issues.append(f"{trading_pair}: Out of order! last_u={last_u}, new_u={u}")
                elif u > last_u + 1:
                    gap = u - last_u
                    if gap > 10:
                        stats["sequence_gaps"] += 1
                        self.issues.append(f"{trading_pair}: Gap of {gap} ({last_u} -> {u})")
            
            # Update tracking (always, per optimistic approach)
            self._last_applied_u_by_symbol[symbol] = u
            self._symbol_last_update_time[symbol] = time.time()
            self._symbol_drop_count[symbol] = 0
        
        ts = data.get("ts", 0)
        bids = payload.get("b", [])
        asks = payload.get("a", [])
        
        stats["parsed_diffs"] += 1
        
        return OrderBookMessage(
            msg_type=MessageType.DIFF,
            trading_pair=trading_pair,
            update_id=u,
            timestamp=ts * 1e-3 if ts else time.time(),
            bids=bids,
            asks=asks,
        )
    
    def get_staleness(self, trading_pair: str) -> float:
        """Get current staleness in seconds"""
        symbol = trading_pair.replace("-", "")
        last_ts = self._symbol_last_update_time.get(symbol, 0)
        return time.time() - last_ts if last_ts > 0 else 0


# =============================================================================
# ORDER BOOK TRACKER (mirrors OrderBookTracker logic)
# =============================================================================
class LocalOrderBookTracker:
    """Mirrors hummingbot.core.data_type.order_book_tracker.OrderBookTracker"""
    
    def __init__(self, trading_pairs: List[str], data_source: BybitDataSource):
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
        - OLD: if snapshot_uid > update_id: reject
        - NEW: Always route, just track stats
        """
        trading_pair = message.trading_pair
        order_book = self.order_books[trading_pair]
        stats = self.data_source.stats[trading_pair]
        
        # OPTIMISTIC: Route even if sequence seems stale (mirrors our changes)
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
# MAIN DEBUG LOOP
# =============================================================================
async def run_debug():
    print("=" * 100)
    print("BYBIT ORDERBOOK DEBUG - Mirroring Hummingbot's Processing Logic")
    print(f"Testing {len(TRADING_PAIRS)} pairs for {RUN_DURATION_SECONDS // 60} minutes")
    print("=" * 100)
    
    # Create data source and tracker
    data_source = BybitDataSource(TRADING_PAIRS)
    tracker = LocalOrderBookTracker(TRADING_PAIRS, data_source)
    
    start_time = time.time()
    last_report_time = start_time
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(BYBIT_WS_URL) as ws:
            # Subscribe to all symbols
            for symbol in data_source.symbols:
                topic = f"orderbook.{DEPTH}.{symbol}"
                await ws.send_json({"op": "subscribe", "args": [topic]})
                print(f"[{time.strftime('%H:%M:%S')}] Subscribed: {topic}")
            
            # Ping task
            async def ping_loop():
                while True:
                    await asyncio.sleep(20)
                    try:
                        await ws.send_json({"op": "ping"})
                    except Exception:
                        break
            ping_task = asyncio.create_task(ping_loop())
            
            print(f"\n[{time.strftime('%H:%M:%S')}] Starting processing loop...\n")
            
            try:
                async for msg in ws:
                    now = time.time()
                    elapsed = now - start_time
                    
                    if elapsed >= RUN_DURATION_SECONDS:
                        break
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        # Skip control messages
                        if data.get("op") or data.get("success") is not None:
                            if data.get("success") == False:
                                print(f"[{time.strftime('%H:%M:%S')}] ❌ Subscribe failed: {data}")
                            continue
                        
                        # Step 1: Process WS message (classify and queue)
                        channel = await data_source.process_ws_message(data)
                        if not channel:
                            continue
                        
                        # Step 2: Parse and process based on type
                        if channel == "snapshot":
                            parsed = data_source.parse_snapshot_message(data)
                            if parsed:
                                tracker.process_snapshot(parsed)
                                print(f"[{time.strftime('%H:%M:%S')}] 📸 {parsed.trading_pair} SNAPSHOT: "
                                      f"u={parsed.update_id}, bids={len(parsed.bids)}, asks={len(parsed.asks)}")
                        
                        elif channel == "diff":
                            parsed = data_source.parse_diff_message(data)
                            if parsed:
                                tracker.route_diff(parsed)
                                tracker.process_diff(parsed)
                        
                        # Periodic report
                        if now - last_report_time >= REPORT_INTERVAL:
                            last_report_time = now
                            print_report(data_source, tracker, elapsed)
                    
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ WS {msg.type}")
                        break
            
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass
    
    # Final report
    print_final_report(data_source, tracker, time.time() - start_time)


def print_report(data_source: BybitDataSource, tracker: LocalOrderBookTracker, elapsed: float):
    """Print periodic status report"""
    mins, secs = int(elapsed // 60), int(elapsed % 60)
    
    print(f"\n{'='*120}")
    print(f"REPORT - Elapsed: {mins}m {secs}s")
    print(f"{'='*120}")
    print(f"{'Pair':14} | {'Snaps':>5} | {'Diffs':>6} | {'Parsed':>6} | {'Routed':>6} | {'Applied':>7} | "
          f"{'Gaps':>4} | {'OoO':>4} | {'Stale':>6} | {'Bids':>4} | {'Asks':>4} | {'Spread':>12}")
    print("-" * 120)
    
    for pair in sorted(data_source.trading_pairs):
        stats = data_source.stats[pair]
        ob = tracker.order_books[pair]
        staleness = data_source.get_staleness(pair)
        status = "🔴" if staleness > 10 or stats["out_of_order"] > 0 else "🟢"
        
        print(f"{status}{pair:13} | {stats['snapshots']:>5} | {stats['diffs']:>6} | {stats['parsed_diffs']:>6} | "
              f"{stats['applied'] + stats['tracker_rejected']:>6} | {stats['applied']:>7} | "
              f"{stats['sequence_gaps']:>4} | {stats['out_of_order']:>4} | {staleness:>5.1f}s | "
              f"{ob.bid_levels:>4} | {ob.ask_levels:>4} | {ob.spread:>12.8f}")
    
    print(f"{'='*120}\n")


def print_final_report(data_source: BybitDataSource, tracker: LocalOrderBookTracker, total_time: float):
    """Print final summary"""
    mins, secs = int(total_time // 60), int(total_time % 60)
    
    print(f"\n{'#'*120}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*120}")
    
    total_diffs = sum(s["diffs"] for s in data_source.stats.values())
    total_applied = sum(s["applied"] for s in data_source.stats.values())
    total_gaps = sum(s["sequence_gaps"] for s in data_source.stats.values())
    total_ooo = sum(s["out_of_order"] for s in data_source.stats.values())
    
    print(f"\nOVERALL STATS:")
    print(f"  Total WS Diffs Received: {total_diffs}")
    print(f"  Total Diffs Applied: {total_applied}")
    print(f"  Total Sequence Gaps: {total_gaps}")
    print(f"  Total Out-of-Order: {total_ooo}")
    print(f"  Total Issues: {len(data_source.issues)}")
    
    if data_source.issues:
        print(f"\nISSUES ({len(data_source.issues)} total):")
        for i, issue in enumerate(data_source.issues[:20], 1):
            print(f"  {i}. {issue}")
        if len(data_source.issues) > 20:
            print(f"  ... and {len(data_source.issues) - 20} more")
    
    print(f"\n{'#'*120}")
    
    if total_gaps == 0 and total_ooo == 0:
        print("\n✅ ALL HEALTHY - No sequence issues detected")
    else:
        print(f"\n⚠️ ISSUES: {total_gaps} gaps, {total_ooo} out-of-order")


async def main():
    try:
        await run_debug()
    except KeyboardInterrupt:
        print("\n⛔ Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
