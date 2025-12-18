#!/usr/bin/env python3
"""
Bybit Orderbook Debug Script
Comprehensive debugging for orderbook management with multiple symbols.
Monitors WS processing, sequence handling, staleness, and detects issues.
"""
import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import aiohttp

# Configuration
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
DEPTH = 50
RUN_DURATION_SECONDS = 30 * 60  # 30 minutes

# Trading pairs to monitor (will be converted to Bybit format)
TRADING_PAIRS = [
    "ROOT-USDT",
    "CORN-USDT",
    "F-USDT",
    "GAME-USDT",
    "PINEYE-USDT",
    "QORPO-USDT",
    "SVL-USDT",
    "ZKL-USDT",
    "SAHARA-USDT",
    "UXLINK-USDT",
    "FHE-USDT",
]


@dataclass
class SymbolStats:
    """Stats for a single symbol"""
    symbol: str
    snapshot_count: int = 0
    delta_count: int = 0
    last_snapshot_u: Optional[int] = None
    last_delta_u: Optional[int] = None
    last_update_time: float = 0
    sequence_gaps: List[tuple] = field(default_factory=list)
    out_of_order_count: int = 0
    max_staleness_seconds: float = 0
    first_snapshot_time: Optional[float] = None
    last_delta_time: Optional[float] = None
    bids_count: int = 0
    asks_count: int = 0
    

def to_bybit_symbol(pair: str) -> str:
    """Convert PAIR-QUOTE to PAIRQUOTE format"""
    return pair.replace("-", "")


async def get_valid_symbols(session: aiohttp.ClientSession) -> set:
    """Fetch valid symbols from Bybit"""
    try:
        async with session.get("https://api.bybit.com/v5/market/instruments-info?category=spot") as resp:
            data = await resp.json()
            return {item["symbol"] for item in data.get("result", {}).get("list", [])}
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return set()


async def debug_bybit_orderbooks():
    """Main debug loop"""
    # Convert pairs to Bybit format
    symbols = [to_bybit_symbol(p) for p in TRADING_PAIRS]
    
    print("=" * 100)
    print("BYBIT ORDERBOOK DEBUG SCRIPT")
    print(f"Monitoring {len(symbols)} symbols for {RUN_DURATION_SECONDS // 60} minutes")
    print("=" * 100)
    
    async with aiohttp.ClientSession() as session:
        # Validate symbols
        valid_symbols = await get_valid_symbols(session)
        invalid = [s for s in symbols if s not in valid_symbols]
        if invalid:
            print(f"\n⚠️  WARNING: Invalid symbols will be skipped: {invalid}")
            symbols = [s for s in symbols if s in valid_symbols]
        
        if not symbols:
            print("No valid symbols to monitor!")
            return
        
        print(f"\nValid symbols: {symbols}\n")
        print("-" * 100)
        
        # Initialize stats
        stats: Dict[str, SymbolStats] = {s: SymbolStats(symbol=s) for s in symbols}
        issues: List[str] = []
        start_time = time.time()
        last_report_time = start_time
        report_interval = 30  # Report every 30 seconds
        
        async with session.ws_connect(BYBIT_WS_URL) as ws:
            # Subscribe to all symbols
            for symbol in symbols:
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
            
            try:
                print(f"\n[{time.strftime('%H:%M:%S')}] Starting monitoring loop...\n")
                
                async for msg in ws:
                    now = time.time()
                    elapsed = now - start_time
                    
                    # Check duration
                    if elapsed >= RUN_DURATION_SECONDS:
                        print(f"\n[{time.strftime('%H:%M:%S')}] Run duration reached. Stopping.")
                        break
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        # Skip control messages
                        if data.get("op") in ("ping", "pong", "subscribe") or data.get("success") is not None:
                            if data.get("success") == False:
                                issue = f"Subscribe failed: {data}"
                                print(f"[{time.strftime('%H:%M:%S')}] ❌ {issue}")
                                issues.append(issue)
                            continue
                        
                        event_type = data.get("type")
                        topic = data.get("topic", "")
                        
                        if "orderbook" in topic:
                            payload = data.get("data", {})
                            symbol = payload.get("s")
                            u = payload.get("u")
                            bids = payload.get("b", [])
                            asks = payload.get("a", [])
                            
                            if symbol not in stats:
                                continue
                            
                            s = stats[symbol]
                            
                            if event_type == "snapshot":
                                s.snapshot_count += 1
                                s.last_snapshot_u = u
                                s.last_update_time = now
                                s.bids_count = len(bids)
                                s.asks_count = len(asks)
                                if s.first_snapshot_time is None:
                                    s.first_snapshot_time = now
                                
                                print(f"[{time.strftime('%H:%M:%S')}] 📸 {symbol} SNAPSHOT #{s.snapshot_count}: u={u}, bids={len(bids)}, asks={len(asks)}")
                                
                            elif event_type == "delta":
                                s.delta_count += 1
                                s.last_delta_time = now
                                
                                # Check sequence
                                if s.last_delta_u is not None and u is not None:
                                    expected_u = s.last_delta_u + 1
                                    if u < s.last_delta_u:
                                        s.out_of_order_count += 1
                                        issue = f"{symbol}: Out of order! last_u={s.last_delta_u}, got u={u}"
                                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️  {issue}")
                                        issues.append(issue)
                                    elif u > expected_u:
                                        gap = u - s.last_delta_u
                                        s.sequence_gaps.append((s.last_delta_u, u, gap))
                                        if gap > 10:  # Only log significant gaps
                                            issue = f"{symbol}: Sequence gap! {s.last_delta_u} -> {u} (gap={gap})"
                                            print(f"[{time.strftime('%H:%M:%S')}] ⚠️  {issue}")
                                            issues.append(issue)
                                
                                s.last_delta_u = u
                                s.last_update_time = now
                                
                                # Track staleness
                                if s.last_delta_time:
                                    staleness = now - s.last_delta_time
                                    if staleness > s.max_staleness_seconds:
                                        s.max_staleness_seconds = staleness
                                
                                # Log periodically (every 100th delta per symbol)
                                if s.delta_count % 100 == 0:
                                    print(f"[{time.strftime('%H:%M:%S')}] 📊 {symbol}: {s.delta_count} deltas, u={u}")
                    
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        issue = f"WS {msg.type}: {msg}"
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ {issue}")
                        issues.append(issue)
                        break
                    
                    # Periodic report
                    if now - last_report_time >= report_interval:
                        last_report_time = now
                        print_periodic_report(stats, elapsed, issues)
            
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass
        
        # Final report
        print_final_report(stats, time.time() - start_time, issues)


def print_periodic_report(stats: Dict[str, SymbolStats], elapsed: float, issues: List[str]):
    """Print periodic status report"""
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    print(f"\n{'='*100}")
    print(f"PERIODIC REPORT - Elapsed: {mins}m {secs}s")
    print(f"{'='*100}")
    
    for symbol, s in sorted(stats.items()):
        staleness = time.time() - s.last_update_time if s.last_update_time > 0 else 0
        gaps = len(s.sequence_gaps)
        status = "🔴" if staleness > 10 or s.out_of_order_count > 0 else "🟢"
        print(f"{status} {symbol:12} | Snaps: {s.snapshot_count:3} | Deltas: {s.delta_count:6} | "
              f"Last u: {s.last_delta_u or s.last_snapshot_u or 'N/A':>10} | "
              f"Staleness: {staleness:.1f}s | Gaps: {gaps} | OoO: {s.out_of_order_count}")
    
    if issues:
        print(f"\n⚠️  Total issues so far: {len(issues)}")
    print(f"{'='*100}\n")


def print_final_report(stats: Dict[str, SymbolStats], total_time: float, issues: List[str]):
    """Print final summary report"""
    mins = int(total_time // 60)
    secs = int(total_time % 60)
    
    print(f"\n{'#'*100}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*100}")
    
    total_deltas = sum(s.delta_count for s in stats.values())
    total_snapshots = sum(s.snapshot_count for s in stats.values())
    total_gaps = sum(len(s.sequence_gaps) for s in stats.values())
    total_ooo = sum(s.out_of_order_count for s in stats.values())
    
    print(f"\nOVERALL STATS:")
    print(f"  Total Snapshots: {total_snapshots}")
    print(f"  Total Deltas: {total_deltas}")
    print(f"  Total Sequence Gaps: {total_gaps}")
    print(f"  Total Out-of-Order: {total_ooo}")
    print(f"  Total Issues: {len(issues)}")
    
    print(f"\nPER-SYMBOL SUMMARY:")
    print("-" * 100)
    print(f"{'Symbol':12} | {'Snaps':>5} | {'Deltas':>7} | {'Rate/min':>8} | {'Max Stale':>10} | {'Gaps':>5} | {'OoO':>4} | Status")
    print("-" * 100)
    
    for symbol, s in sorted(stats.items()):
        rate = s.delta_count / (total_time / 60) if total_time > 0 else 0
        gaps = len(s.sequence_gaps)
        status = "HEALTHY" if gaps == 0 and s.out_of_order_count == 0 else "ISSUES"
        emoji = "✅" if status == "HEALTHY" else "⚠️"
        print(f"{symbol:12} | {s.snapshot_count:>5} | {s.delta_count:>7} | {rate:>8.1f} | "
              f"{s.max_staleness_seconds:>9.1f}s | {gaps:>5} | {s.out_of_order_count:>4} | {emoji} {status}")
    
    print("-" * 100)
    
    if issues:
        print(f"\nISSUES LOG ({len(issues)} total):")
        for i, issue in enumerate(issues[:20], 1):  # Show first 20
            print(f"  {i}. {issue}")
        if len(issues) > 20:
            print(f"  ... and {len(issues) - 20} more")
    
    print(f"\n{'#'*100}")
    
    # Verdict
    if total_gaps == 0 and total_ooo == 0 and len(issues) == 0:
        print("\n✅ ALL HEALTHY - No issues detected during monitoring period")
    else:
        print(f"\n⚠️  ISSUES DETECTED - {total_gaps} gaps, {total_ooo} out-of-order, {len(issues)} errors")
    print()


async def main():
    print("\n" + "=" * 100)
    print("Starting Bybit Orderbook Debug...")
    print("Press Ctrl+C to stop early")
    print("=" * 100 + "\n")
    
    try:
        await debug_bybit_orderbooks()
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
