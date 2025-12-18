#!/usr/bin/env python3
"""
Bitmart Orderbook Debug Script - Verify Update Frequency and Processing
Mirrors Hummingbot's Bitmart implementation to measure:
1. Actual update frequency (checking if 100ms is achieved)
2. Version sequencing behavior
3. Snapshot vs diff distribution
4. Processing latency
"""
import asyncio
import gzip
import io
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import aiohttp


# =============================================================================
# CONFIGURATION
# =============================================================================
BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
DEPTH_CHANNEL = "spot/depth/increase100"
RUN_DURATION_SECONDS = 60  # 1 minute for quick test
REPORT_INTERVAL = 30

# Bitmart uses SYMBOL_QUOTE format (e.g., BTC_USDT)
TRADING_PAIRS = [
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "DOGE_USDT",
    "XRP_USDT",
]


# =============================================================================
# UTILITIES
# =============================================================================
def decompress_ws_message(data):
    """Decompress gzip-compressed Bitmart WS messages"""
    try:
        if isinstance(data, bytes):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
                    return gz.read().decode('utf-8')
            except Exception:
                return data.decode('utf-8')
        return data
    except Exception:
        return None


# =============================================================================
# STATS TRACKING
# =============================================================================
@dataclass
class SymbolStats:
    snapshots: int = 0
    diffs: int = 0
    heartbeats: int = 0
    version_gaps: int = 0
    out_of_order: int = 0
    last_version: Optional[int] = None
    last_update_time: float = 0
    update_intervals: List[float] = field(default_factory=list)
    bids_count: int = 0
    asks_count: int = 0


# =============================================================================
# MAIN DEBUG LOOP
# =============================================================================
async def run_debug():
    print("=" * 100)
    print("BITMART ORDERBOOK DEBUG - Measuring Update Frequency")
    print(f"Channel: {DEPTH_CHANNEL}")
    print(f"Testing {len(TRADING_PAIRS)} pairs for {RUN_DURATION_SECONDS // 60} minutes")
    print("=" * 100)
    
    stats: Dict[str, SymbolStats] = {p: SymbolStats() for p in TRADING_PAIRS}
    start_time = time.time()
    last_report_time = start_time
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(BITMART_WS_URL) as ws:
            # Subscribe to all trading pairs
            topics = [f"{DEPTH_CHANNEL}:{symbol}" for symbol in TRADING_PAIRS]
            subscribe_msg = {"op": "subscribe", "args": topics}
            await ws.send_json(subscribe_msg)
            print(f"\n[{time.strftime('%H:%M:%S')}] Subscribed to {len(topics)} depth channels")
            
            # Start ping task - Bitmart accepts plain text "ping"
            async def ping_task():
                while True:
                    await asyncio.sleep(10)
                    try:
                        await ws.send_str("ping")
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 Sent ping")
                    except Exception as e:
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ Ping failed: {e}")
                        break
            
            ping_handle = asyncio.create_task(ping_task())
            
            try:
                async for msg in ws:
                    now = time.time()
                    elapsed = now - start_time
                    
                    if elapsed >= RUN_DURATION_SECONDS:
                        break
                    
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        text = decompress_ws_message(msg.data)
                        if text is None:
                            continue
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        text = msg.data
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ WS {msg.type}")
                        break
                    else:
                        continue
                    
                    # Skip pong responses
                    if text and text.strip().lower() == "pong":
                        continue
                    
                    try:
                        data = json.loads(text)
                    except Exception:
                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Non-JSON: {text[:100]}")
                        continue
                    
                    # Log every message type for debugging
                    table = data.get("table")
                    if table:
                        data_items = data.get("data", [])
                        if data_items:
                            first_item = data_items[0]
                            msg_type = first_item.get("type", "unknown")
                            symbol = first_item.get("symbol", "unknown")
                            bids_len = len(first_item.get("bids", []))
                            asks_len = len(first_item.get("asks", []))
                            print(f"[{time.strftime('%H:%M:%S')}] 📨 {table}: {symbol} type={msg_type} bids={bids_len} asks={asks_len}")
                    
                    # Skip subscription acknowledgments
                    if "event" in data:
                        event = data.get("event")
                        if event == "subscribe":
                            print(f"[{time.strftime('%H:%M:%S')}] ✅ Subscribed: {data.get('arg', {}).get('channel', 'unknown')}")
                        continue
                    
                    # Process depth data
                    if table != DEPTH_CHANNEL:
                        continue
                    
                    for item in data.get("data", []):
                        symbol = item.get("symbol")
                        if symbol not in stats:
                            continue
                        
                        s = stats[symbol]
                        msg_type = item.get("type", "").lower()
                        version = item.get("version")
                        bids = item.get("bids", [])
                        asks = item.get("asks", [])
                        
                        # Check for heartbeat (empty bids and asks)
                        is_heartbeat = (not bids) and (not asks)
                        
                        if is_heartbeat:
                            s.heartbeats += 1
                            continue
                        
                        # Track update interval
                        if s.last_update_time > 0:
                            interval = (now - s.last_update_time) * 1000  # ms
                            s.update_intervals.append(interval)
                        s.last_update_time = now
                        
                        # Version tracking
                        if version is not None:
                            new_ver = int(version)
                            if s.last_version is not None:
                                if new_ver < s.last_version:
                                    s.out_of_order += 1
                                elif new_ver > s.last_version + 1:
                                    s.version_gaps += 1
                            s.last_version = new_ver
                        
                        # Count by type
                        if msg_type == "snapshot":
                            s.snapshots += 1
                        else:
                            s.diffs += 1
                        
                        s.bids_count = len(bids)
                        s.asks_count = len(asks)
                        
                        # Log first update
                        if s.snapshots + s.diffs == 1:
                            print(f"[{time.strftime('%H:%M:%S')}] 📸 {symbol}: first {msg_type} v{version}, bids={len(bids)}, asks={len(asks)}")
                    
                    # Periodic report
                    if now - last_report_time >= REPORT_INTERVAL:
                        last_report_time = now
                        print_report(stats, elapsed)
            
            except asyncio.CancelledError:
                pass
            finally:
                ping_handle.cancel()
    
    # Final report
    print_final_report(stats, time.time() - start_time)


def print_report(stats: Dict[str, SymbolStats], elapsed: float):
    """Print periodic status report"""
    mins, secs = int(elapsed // 60), int(elapsed % 60)
    
    print(f"\n{'='*120}")
    print(f"REPORT - Elapsed: {mins}m {secs}s")
    print(f"{'='*120}")
    print(f"{'Symbol':12} | {'Snaps':>6} | {'Diffs':>7} | {'HBeat':>5} | {'Gaps':>4} | {'OoO':>4} | {'AvgInt':>8} | {'MinInt':>7} | {'MaxInt':>7}")
    print("-" * 120)
    
    for symbol in sorted(stats.keys()):
        s = stats[symbol]
        total = s.snapshots + s.diffs
        
        # Calculate interval stats
        if s.update_intervals:
            avg_int = sum(s.update_intervals) / len(s.update_intervals)
            min_int = min(s.update_intervals)
            max_int = max(s.update_intervals)
        else:
            avg_int = min_int = max_int = 0
        
        status = "🟢" if s.version_gaps == 0 and s.out_of_order == 0 else "🔴"
        print(f"{status}{symbol:11} | {s.snapshots:>6} | {s.diffs:>7} | {s.heartbeats:>5} | {s.version_gaps:>4} | {s.out_of_order:>4} | {avg_int:>7.1f}ms | {min_int:>6.1f}ms | {max_int:>6.1f}ms")
    
    total_updates = sum(s.snapshots + s.diffs for s in stats.values())
    total_gaps = sum(s.version_gaps for s in stats.values())
    total_ooo = sum(s.out_of_order for s in stats.values())
    print(f"{'='*120}")
    print(f"TOTALS: {total_updates} updates, {total_gaps} gaps, {total_ooo} out-of-order")
    print(f"{'='*120}\n")


def print_final_report(stats: Dict[str, SymbolStats], total_time: float):
    """Print final summary with interval analysis"""
    mins, secs = int(total_time // 60), int(total_time % 60)
    
    print(f"\n{'#'*120}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*120}")
    
    # Aggregate all intervals
    all_intervals = []
    for s in stats.values():
        all_intervals.extend(s.update_intervals)
    
    total_updates = sum(s.snapshots + s.diffs for s in stats.values())
    total_gaps = sum(s.version_gaps for s in stats.values())
    total_ooo = sum(s.out_of_order for s in stats.values())
    
    print(f"\nOVERALL STATS:")
    print(f"  Total Updates: {total_updates}")
    print(f"  Total Version Gaps: {total_gaps}")
    print(f"  Total Out-of-Order: {total_ooo}")
    
    if all_intervals:
        avg = sum(all_intervals) / len(all_intervals)
        p50 = sorted(all_intervals)[len(all_intervals) // 2]
        p95 = sorted(all_intervals)[int(len(all_intervals) * 0.95)]
        p99 = sorted(all_intervals)[int(len(all_intervals) * 0.99)]
        
        print(f"\nUPDATE INTERVAL ANALYSIS (across all symbols):")
        print(f"  Average: {avg:.1f}ms")
        print(f"  Median (p50): {p50:.1f}ms")
        print(f"  p95: {p95:.1f}ms")
        print(f"  p99: {p99:.1f}ms")
        print(f"  Min: {min(all_intervals):.1f}ms")
        print(f"  Max: {max(all_intervals):.1f}ms")
        
        # Check if we're getting 100ms updates
        under_150ms = sum(1 for i in all_intervals if i < 150)
        pct_under_150 = (under_150ms / len(all_intervals)) * 100
        print(f"\n  Updates under 150ms: {under_150ms}/{len(all_intervals)} ({pct_under_150:.1f}%)")
        
        if avg < 150:
            print(f"\n✅ CONFIRMED: Receiving ~100ms updates (avg={avg:.1f}ms)")
        else:
            print(f"\n⚠️ Updates slower than expected (avg={avg:.1f}ms, expected ~100ms)")
    
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
