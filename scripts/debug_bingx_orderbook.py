#!/usr/bin/env python3
"""
BingX Orderbook Debug Script - Mirrors Hummingbot's Processing Logic
Self-contained script that recreates Hummingbot's BingX implementation:
1. WS connection to wss://open-api-ws.bingx.com/market
2. Gzip decompression per BingX spec
3. Subscription to {symbol}@depth100 (full snapshots every ~300ms)
4. Ping/pong handling
5. Orderbook maintenance with snapshot updates
6. Staleness detection
"""
import asyncio
import gzip
import io
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import aiohttp


# =============================================================================
# CONFIGURATION
# =============================================================================
BINGX_WS_URL = "wss://open-api-ws.bingx.com/market"
DEPTH_LEVEL = 100  # @depth100 subscription
RUN_DURATION_SECONDS = 30 * 60  # 30 minutes
REPORT_INTERVAL = 30
STALE_THRESHOLD_SECONDS = 60

# BingX uses symbol format: PAIR-QUOTE (e.g., SUT-USDT)
TRADING_PAIRS = [
    "SUT-USDT",
    "NUMI-USDT",
    "USECORN-USDT",
    "F-USDT",
    "ARTY-USDT",
    "LAB-USDT",
    "SAHARA-USDT",
    "ZND-USDT",
    "DIGI-USDT",
    "DEFI-USDT",
    "JYAI-USDT",
]


# =============================================================================
# UTILITIES (mirrors bing_x_utils.py)
# =============================================================================
def decompress_ws_message(message):
    """
    Robustly handle BingX WS frames which may be gzip-compressed bytes or plain JSON.
    Mirrors hummingbot/connector/exchange/bing_x/bing_x_utils.py:decompress_ws_message
    """
    try:
        if isinstance(message, bytes):
            # First, try gzip
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb') as gz:
                    decompressed = gz.read()
                return json.loads(decompressed.decode('utf-8'))
            except Exception:
                # Not gzip or bad gzip; try plain UTF-8 JSON
                try:
                    return json.loads(message.decode('utf-8'))
                except Exception:
                    return {}
        elif isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return {}
        else:
            return message
    except Exception:
        return {}


# =============================================================================
# LOCAL ORDERBOOK (mirrors OrderBook for snapshot-only updates)
# =============================================================================
class LocalOrderBook:
    """Mirrors hummingbot orderbook for BingX snapshot-based updates"""
    
    def __init__(self, trading_pair: str):
        self.trading_pair = trading_pair
        self._bid_book: Dict[float, float] = {}
        self._ask_book: Dict[float, float] = {}
        self._snapshot_count: int = 0
        self._best_bid: float = 0.0
        self._best_ask: float = float('inf')
        self._last_applied: float = 0
    
    def apply_snapshot(self, bids: List[List[str]], asks: List[List[str]]):
        """BingX sends full snapshots - replace entire book"""
        self._bid_book.clear()
        self._ask_book.clear()
        
        for entry in bids:
            if len(entry) >= 2:
                price, amount = float(entry[0]), float(entry[1])
                if amount > 0:
                    self._bid_book[price] = amount
        
        for entry in asks:
            if len(entry) >= 2:
                price, amount = float(entry[0]), float(entry[1])
                if amount > 0:
                    self._ask_book[price] = amount
        
        self._update_best_prices()
        self._snapshot_count += 1
        self._last_applied = time.time()
    
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
# BINGX DATA SOURCE (mirrors BingXAPIOrderBookDataSource logic)
# =============================================================================
class BingXDataSource:
    """Mirrors key logic from bing_x_api_order_book_data_source.py"""
    
    def __init__(self, trading_pairs: List[str]):
        self.trading_pairs = trading_pairs
        self.order_books: Dict[str, LocalOrderBook] = {p: LocalOrderBook(p) for p in trading_pairs}
        
        # Stats tracking
        self.stats: Dict[str, dict] = {p: {
            "snapshots": 0,
            "updates_received": 0,
            "last_update_time": 0,
            "bid_levels": 0,
            "ask_levels": 0,
            "max_staleness": 0,
        } for p in trading_pairs}
        
        # Staleness tracking (mirrors _symbol_last_update_time)
        self._symbol_last_update_time: Dict[str, float] = {}
        
        self.issues: List[str] = []
    
    def process_snapshot(self, trading_pair: str, bids: List[List[str]], asks: List[List[str]]):
        """Process a snapshot message - BingX sends full orderbook every ~300ms"""
        if trading_pair not in self.order_books:
            return
        
        now = time.time()
        ob = self.order_books[trading_pair]
        stats = self.stats[trading_pair]
        
        # Track staleness before update
        if trading_pair in self._symbol_last_update_time:
            staleness = now - self._symbol_last_update_time[trading_pair]
            if staleness > stats["max_staleness"]:
                stats["max_staleness"] = staleness
        
        # Apply snapshot
        ob.apply_snapshot(bids, asks)
        
        # Update tracking
        stats["snapshots"] += 1
        stats["updates_received"] += 1
        stats["last_update_time"] = now
        stats["bid_levels"] = ob.bid_levels
        stats["ask_levels"] = ob.ask_levels
        self._symbol_last_update_time[trading_pair] = now
    
    def get_staleness(self, trading_pair: str) -> float:
        """Get current staleness in seconds"""
        last_ts = self._symbol_last_update_time.get(trading_pair, 0)
        return time.time() - last_ts if last_ts > 0 else 0


# =============================================================================
# MAIN DEBUG LOOP
# =============================================================================
async def run_debug():
    print("=" * 100)
    print("BINGX ORDERBOOK DEBUG - Mirroring Hummingbot's Processing Logic")
    print(f"Testing {len(TRADING_PAIRS)} pairs for {RUN_DURATION_SECONDS // 60} minutes")
    print("=" * 100)
    
    # Create data source
    data_source = BingXDataSource(TRADING_PAIRS)
    
    start_time = time.time()
    last_report_time = start_time
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(BINGX_WS_URL) as ws:
            # Subscribe to all trading pairs
            for trading_pair in TRADING_PAIRS:
                # BingX subscription format
                subscribe_req = {
                    "id": f"depth_{trading_pair}",
                    "reqType": "sub",
                    "dataType": f"{trading_pair}@depth{DEPTH_LEVEL}"
                }
                await ws.send_json(subscribe_req)
                print(f"[{time.strftime('%H:%M:%S')}] Subscribed: {trading_pair}@depth{DEPTH_LEVEL}")
            
            print(f"\n[{time.strftime('%H:%M:%S')}] Starting processing loop...\n")
            
            try:
                async for msg in ws:
                    now = time.time()
                    elapsed = now - start_time
                    
                    if elapsed >= RUN_DURATION_SECONDS:
                        break
                    
                    # Handle different message types
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        # BingX sends gzip-compressed binary frames
                        data = decompress_ws_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        data = decompress_ws_message(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ WS {msg.type}")
                        break
                    else:
                        continue
                    
                    if not isinstance(data, dict):
                        continue
                    
                    # Handle ping/pong (mirrors _process_ws_messages)
                    if "ping" in data:
                        pong = {"pong": data["ping"]}
                        if "time" in data:
                            pong["time"] = data["time"]
                        await ws.send_json(pong)
                        continue
                    
                    # Handle subscription acknowledgments
                    if "code" in data and "id" in data and "dataType" not in data:
                        code = data.get("code")
                        sub_id = data.get("id")
                        if code == 0:
                            print(f"[{time.strftime('%H:%M:%S')}] ✅ Subscribed: {sub_id}")
                        else:
                            print(f"[{time.strftime('%H:%M:%S')}] ❌ Subscribe failed: {sub_id} (code={code})")
                            data_source.issues.append(f"Subscribe failed: {sub_id}")
                        continue
                    
                    # Skip SUCCESS acknowledgments
                    if data.get("msg") == "SUCCESS" and "dataType" not in data:
                        continue
                    
                    # Process market data
                    data_type = data.get("dataType")
                    if not data_type:
                        continue
                    
                    parts = data_type.split('@')
                    if len(parts) != 2:
                        continue
                    
                    symbol, event_type = parts
                    
                    # Handle depth messages (full snapshots)
                    if event_type.startswith("depth"):
                        payload = data.get("data", {})
                        
                        # BingX depth data format
                        bids = payload.get("bids") or payload.get("b") or []
                        asks = payload.get("asks") or payload.get("a") or []
                        
                        if symbol in data_source.order_books:
                            data_source.process_snapshot(symbol, bids, asks)
                            
                            # Log periodically
                            stats = data_source.stats[symbol]
                            if stats["snapshots"] % 100 == 0:
                                print(f"[{time.strftime('%H:%M:%S')}] 📊 {symbol}: {stats['snapshots']} snapshots")
                            elif stats["snapshots"] == 1:
                                print(f"[{time.strftime('%H:%M:%S')}] 📸 {symbol} first snapshot: bids={len(bids)}, asks={len(asks)}")
                    
                    # Periodic report
                    if now - last_report_time >= REPORT_INTERVAL:
                        last_report_time = now
                        print_report(data_source, elapsed)
            
            except asyncio.CancelledError:
                pass
    
    # Final report
    print_final_report(data_source, time.time() - start_time)


def print_report(data_source: BingXDataSource, elapsed: float):
    """Print periodic status report"""
    mins, secs = int(elapsed // 60), int(elapsed % 60)
    
    print(f"\n{'='*110}")
    print(f"REPORT - Elapsed: {mins}m {secs}s")
    print(f"{'='*110}")
    print(f"{'Pair':14} | {'Snapshots':>9} | {'Rate/min':>8} | {'Stale':>7} | {'MaxStale':>8} | {'Bids':>4} | {'Asks':>4} | {'Spread':>14}")
    print("-" * 110)
    
    for pair in sorted(data_source.trading_pairs):
        stats = data_source.stats[pair]
        ob = data_source.order_books[pair]
        staleness = data_source.get_staleness(pair)
        rate = stats["snapshots"] / (elapsed / 60) if elapsed > 0 else 0
        status = "🔴" if staleness > 10 or stats["snapshots"] == 0 else "🟢"
        
        print(f"{status}{pair:13} | {stats['snapshots']:>9} | {rate:>8.1f} | {staleness:>6.1f}s | {stats['max_staleness']:>7.1f}s | "
              f"{ob.bid_levels:>4} | {ob.ask_levels:>4} | {ob.spread:>14.8f}")
    
    total_snapshots = sum(s["snapshots"] for s in data_source.stats.values())
    print(f"{'='*110}")
    print(f"TOTALS: {total_snapshots} snapshots, {len(data_source.issues)} issues")
    print(f"{'='*110}\n")


def print_final_report(data_source: BingXDataSource, total_time: float):
    """Print final summary"""
    mins, secs = int(total_time // 60), int(total_time % 60)
    
    print(f"\n{'#'*110}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*110}")
    
    total_snapshots = sum(s["snapshots"] for s in data_source.stats.values())
    total_issues = len(data_source.issues)
    
    print(f"\nOVERALL STATS:")
    print(f"  Total Snapshots Received: {total_snapshots}")
    print(f"  Average Rate: {total_snapshots / (total_time / 60):.1f}/min")
    print(f"  Total Issues: {total_issues}")
    
    print(f"\nPER-SYMBOL DETAILS:")
    print("-" * 110)
    for pair in sorted(data_source.trading_pairs):
        stats = data_source.stats[pair]
        ob = data_source.order_books[pair]
        rate = stats["snapshots"] / (total_time / 60) if total_time > 0 else 0
        print(f"  {pair:14} | Snapshots: {stats['snapshots']:>6} | Rate: {rate:>6.1f}/min | "
              f"MaxStale: {stats['max_staleness']:>6.1f}s | Book: {ob.bid_levels}/{ob.ask_levels}")
    print("-" * 110)
    
    if data_source.issues:
        print(f"\nISSUES ({len(data_source.issues)} total):")
        for i, issue in enumerate(data_source.issues[:20], 1):
            print(f"  {i}. {issue}")
        if len(data_source.issues) > 20:
            print(f"  ... and {len(data_source.issues) - 20} more")
    
    print(f"\n{'#'*110}")
    
    # Verdict
    symbols_with_no_data = [p for p in data_source.trading_pairs if data_source.stats[p]["snapshots"] == 0]
    if total_issues == 0 and not symbols_with_no_data:
        print("\n✅ ALL HEALTHY - All symbols receiving updates")
    elif symbols_with_no_data:
        print(f"\n⚠️ WARNING: {len(symbols_with_no_data)} symbols with no data: {symbols_with_no_data}")
    else:
        print(f"\n⚠️ ISSUES: {total_issues} errors detected")


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
