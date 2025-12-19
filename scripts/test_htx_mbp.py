#!/usr/bin/env python3
"""
HTX MBP Hybrid Orderbook Comprehensive Test

Tests the hybrid approach:
- mbp.5: tick-by-tick incremental (top 5 levels) - PRIORITY/FRESHEST
- mbp.refresh.20: 100ms snapshots (20 levels) - SYNC/RECOVERY

Run for 5 minutes, then print comprehensive report.
"""

import asyncio
import gzip
import json
import time
import websockets
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime


@dataclass
class OrderBookState:
    """Tracks orderbook state per symbol"""
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_seq_num: Optional[int] = None
    last_refresh_seq: Optional[int] = None
    last_incremental_seq: Optional[int] = None
    last_refresh_time: float = 0
    last_incremental_time: float = 0
    initialized: bool = False


@dataclass 
class Stats:
    """Statistics tracker"""
    refresh_count: int = 0
    incremental_count: int = 0
    incremental_applied: int = 0
    incremental_skipped_no_init: int = 0
    incremental_skipped_old: int = 0
    gaps_detected: int = 0
    gaps_recovered: int = 0
    pings_received: int = 0
    
    refresh_intervals: List[float] = field(default_factory=list)
    incremental_intervals: List[float] = field(default_factory=list)
    sequence_jumps: List[Tuple[int, int, int]] = field(default_factory=list)
    times_incremental_newer_than_refresh: int = 0


class HTXMBPTester:
    URL = "wss://api-aws.huobi.pro/feed"  # AWS endpoint
    MBP_INCREMENTAL_DEPTH = 5
    MBP_REFRESH_DEPTH = 20
    DURATION_SECONDS = 300  # 5 minutes
    
    def __init__(self, symbols: List[str]):
        self.symbols = [s.lower().replace("-", "").replace("_", "") for s in symbols]
        self.orderbooks: Dict[str, OrderBookState] = {s: OrderBookState() for s in self.symbols}
        self.stats: Dict[str, Stats] = {s: Stats() for s in self.symbols}
        self.symbols_needing_refresh: set = set()
        
        self.start_time: float = 0
        self.last_any_message_time: float = 0
        self.global_pings: int = 0
    
    def apply_snapshot(self, symbol: str, bids: List, asks: List, seq_num: int, ts: float):
        ob = self.orderbooks[symbol]
        stats = self.stats[symbol]
        
        if ob.last_refresh_time > 0:
            interval = (ts - ob.last_refresh_time) * 1000
            stats.refresh_intervals.append(interval)
        
        if symbol in self.symbols_needing_refresh:
            self.symbols_needing_refresh.discard(symbol)
            stats.gaps_recovered += 1
        
        ob.bids = {float(b[0]): float(b[1]) for b in bids}
        ob.asks = {float(a[0]): float(a[1]) for a in asks}
        ob.last_seq_num = seq_num
        ob.last_refresh_seq = seq_num
        ob.last_refresh_time = ts
        ob.initialized = True
        stats.refresh_count += 1
    
    def apply_incremental(self, symbol: str, bids: List, asks: List, 
                          seq_num: int, prev_seq_num: int, ts: float) -> bool:
        ob = self.orderbooks[symbol]
        stats = self.stats[symbol]
        
        stats.incremental_count += 1
        
        if not ob.initialized or ob.last_seq_num is None:
            stats.incremental_skipped_no_init += 1
            return False
        
        if seq_num <= ob.last_seq_num:
            stats.incremental_skipped_old += 1
            return False
        
        expected_prev = ob.last_seq_num
        if prev_seq_num != expected_prev:
            stats.gaps_detected += 1
            stats.sequence_jumps.append((expected_prev, prev_seq_num, seq_num))
            if symbol not in self.symbols_needing_refresh:
                self.symbols_needing_refresh.add(symbol)
        
        if ob.last_incremental_time > 0:
            interval = (ts - ob.last_incremental_time) * 1000
            stats.incremental_intervals.append(interval)
        
        if ob.last_refresh_seq and seq_num > ob.last_refresh_seq:
            stats.times_incremental_newer_than_refresh += 1
        
        # Apply optimistically
        for bid in bids:
            price, size = float(bid[0]), float(bid[1])
            if size == 0:
                ob.bids.pop(price, None)
            else:
                ob.bids[price] = size
        
        for ask in asks:
            price, size = float(ask[0]), float(ask[1])
            if size == 0:
                ob.asks.pop(price, None)
            else:
                ob.asks[price] = size
        
        ob.last_seq_num = seq_num
        ob.last_incremental_seq = seq_num
        ob.last_incremental_time = ts
        stats.incremental_applied += 1
        return True
    
    def get_orderbook_summary(self, symbol: str) -> dict:
        ob = self.orderbooks[symbol]
        if not ob.bids and not ob.asks:
            return {"empty": True}
        
        sorted_bids = sorted(ob.bids.items(), key=lambda x: -x[0])[:5]
        sorted_asks = sorted(ob.asks.items(), key=lambda x: x[0])[:5]
        
        return {
            "best_bid": sorted_bids[0] if sorted_bids else None,
            "best_ask": sorted_asks[0] if sorted_asks else None,
            "bid_levels": len(ob.bids),
            "ask_levels": len(ob.asks),
            "spread": (sorted_asks[0][0] - sorted_bids[0][0]) if sorted_bids and sorted_asks else None
        }
    
    def print_final_report(self):
        elapsed = time.time() - self.start_time
        
        print()
        print("=" * 80)
        print("HTX MBP HYBRID ORDERBOOK - FINAL REPORT")
        print("=" * 80)
        print(f"Runtime: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        print(f"WebSocket URL: {self.URL}")
        print(f"Symbols tested: {', '.join(self.symbols)}")
        print(f"Total HTX pings received: {self.global_pings}")
        print()
        
        for symbol in self.symbols:
            stats = self.stats[symbol]
            ob = self.orderbooks[symbol]
            
            print("=" * 80)
            print(f"📊 {symbol.upper()}")
            print("=" * 80)
            
            # Message Summary
            print(f"\n📨 MESSAGE COUNTS:")
            print(f"   Refresh snapshots (mbp.refresh.20): {stats.refresh_count}")
            print(f"   Incremental messages (mbp.5): {stats.incremental_count}")
            print(f"     - Applied: {stats.incremental_applied}")
            print(f"     - Skipped (no init): {stats.incremental_skipped_no_init}")
            print(f"     - Skipped (old/dup): {stats.incremental_skipped_old}")
            if stats.incremental_count > 0:
                apply_rate = stats.incremental_applied / stats.incremental_count * 100
                print(f"     - Apply rate: {apply_rate:.1f}%")
            
            # Message Rates
            print(f"\n📈 MESSAGE RATES:")
            print(f"   Refresh: {stats.refresh_count / elapsed:.2f}/s (expected ~10/s for 100ms interval)")
            print(f"   Incremental: {stats.incremental_applied / elapsed:.2f}/s (tick-by-tick)")
            
            # Sequence Analysis
            print(f"\n🔢 SEQUENCE ANALYSIS:")
            print(f"   Gaps detected: {stats.gaps_detected}")
            print(f"   Gaps recovered (via refresh): {stats.gaps_recovered}")
            unrecovered = stats.gaps_detected - stats.gaps_recovered
            if unrecovered > 0:
                print(f"   ⚠️ Unrecovered gaps: {unrecovered}")
            else:
                print(f"   ✅ All gaps recovered automatically")
            
            if stats.sequence_jumps and len(stats.sequence_jumps) <= 10:
                print(f"\n   Gap details:")
                for i, (expected, got, seq) in enumerate(stats.sequence_jumps[:10]):
                    jump = got - expected
                    print(f"     #{i+1}: expected prevSeq={expected}, got={got} (jump={jump:+d})")
            
            # Timing Analysis
            print(f"\n⏱️ TIMING ANALYSIS:")
            
            if stats.incremental_intervals:
                intervals = stats.incremental_intervals
                avg = sum(intervals) / len(intervals)
                sorted_intervals = sorted(intervals)
                p50 = sorted_intervals[len(sorted_intervals) // 2]
                p95_idx = min(int(len(sorted_intervals) * 0.95), len(sorted_intervals) - 1)
                p95 = sorted_intervals[p95_idx]
                
                print(f"\n   Incremental (mbp.5) intervals:")
                print(f"     Avg: {avg:.1f}ms")
                print(f"     Min: {min(intervals):.1f}ms, Max: {max(intervals):.1f}ms")
                print(f"     P50: {p50:.1f}ms, P95: {p95:.1f}ms")
                
                under_10ms = sum(1 for i in intervals if i < 10)
                under_50ms = sum(1 for i in intervals if i < 50)
                under_100ms = sum(1 for i in intervals if i < 100)
                print(f"     <10ms: {under_10ms}/{len(intervals)} ({under_10ms/len(intervals)*100:.0f}%)")
                print(f"     <50ms: {under_50ms}/{len(intervals)} ({under_50ms/len(intervals)*100:.0f}%)")
                print(f"     <100ms: {under_100ms}/{len(intervals)} ({under_100ms/len(intervals)*100:.0f}%)")
            
            if stats.refresh_intervals:
                intervals = stats.refresh_intervals
                avg = sum(intervals) / len(intervals)
                print(f"\n   Refresh (mbp.refresh.20) intervals:")
                print(f"     Avg: {avg:.1f}ms (expected ~100ms)")
                print(f"     Min: {min(intervals):.1f}ms, Max: {max(intervals):.1f}ms")
            
            # Priority Analysis
            print(f"\n🏆 mbp.5 FRESHNESS ADVANTAGE:")
            print(f"   Times mbp.5 provided newer data than last refresh: {stats.times_incremental_newer_than_refresh}")
            if stats.incremental_applied > 0:
                ratio = stats.times_incremental_newer_than_refresh / stats.incremental_applied * 100
                print(f"   Freshness advantage: {ratio:.1f}% of applied incrementals")
            
            # Final Orderbook State
            summary = self.get_orderbook_summary(symbol)
            print(f"\n📖 FINAL ORDERBOOK STATE:")
            if not summary.get("empty"):
                print(f"   Best bid: {summary['best_bid']}")
                print(f"   Best ask: {summary['best_ask']}")
                if summary['spread']:
                    print(f"   Spread: {summary['spread']:.8f}")
                print(f"   Depth: {summary['bid_levels']} bids, {summary['ask_levels']} asks")
                print(f"   Last seqNum: {ob.last_seq_num}")
                print(f"   Last refresh seqNum: {ob.last_refresh_seq}")
                print(f"   Last incremental seqNum: {ob.last_incremental_seq}")
            else:
                print("   Orderbook is empty!")
        
        # Final Verdict
        print()
        print("=" * 80)
        print("📋 VERDICT")
        print("=" * 80)
        
        all_healthy = True
        for symbol in self.symbols:
            stats = self.stats[symbol]
            issues = []
            
            if stats.gaps_detected > stats.gaps_recovered:
                issues.append(f"Unrecovered gaps: {stats.gaps_detected - stats.gaps_recovered}")
                all_healthy = False
            if stats.incremental_applied == 0 and stats.incremental_count > 0:
                issues.append("No incrementals applied")
                all_healthy = False
            if stats.refresh_count == 0:
                issues.append("No refresh snapshots received")
                all_healthy = False
            
            if not issues:
                print(f"✅ {symbol.upper()}: HEALTHY")
                print(f"   - Tick-by-tick updates (mbp.5): {stats.incremental_applied}")
                print(f"   - 100ms snapshots (mbp.refresh.20): {stats.refresh_count}")
                print(f"   - Gap auto-recovery: {stats.gaps_recovered}/{stats.gaps_detected}")
            else:
                print(f"⚠️ {symbol.upper()}: ISSUES")
                for issue in issues:
                    print(f"   - {issue}")
        
        print()
        print("=" * 80)
        if all_healthy:
            print("✅ HTX HYBRID MBP IMPLEMENTATION: WORKING CORRECTLY")
            print()
            print("Summary:")
            print("  - mbp.5 provides tick-by-tick updates for top 5 levels (PRIORITY)")
            print("  - mbp.refresh.20 provides 100ms snapshots for 20 levels (SYNC)")
            print("  - Sequence gaps are auto-recovered via refresh snapshots")
            print("  - This is 10x+ faster than the old depth.step0 (1 second snapshots)")
        else:
            print("⚠️ HTX HYBRID MBP IMPLEMENTATION: ISSUES DETECTED - review above")
        print("=" * 80)
    
    async def run(self):
        self.start_time = time.time()
        self.last_any_message_time = self.start_time
        end_time = self.start_time + self.DURATION_SECONDS
        
        print("=" * 80)
        print("HTX MBP Hybrid Orderbook Test")
        print("=" * 80)
        print(f"URL: {self.URL}")
        print(f"mbp.5 (tick-by-tick) + mbp.refresh.20 (100ms snapshots)")
        print(f"Symbols: {', '.join(self.symbols)}")
        print(f"Duration: {self.DURATION_SECONDS}s ({self.DURATION_SECONDS/60:.0f} minutes)")
        print("=" * 80)
        print()
        
        msg_count = 0
        
        try:
            async with websockets.connect(
                self.URL, 
                ping_interval=None, 
                ping_timeout=None,
                close_timeout=10
            ) as ws:
                # Subscribe
                for symbol in self.symbols:
                    await ws.send(json.dumps({
                        "sub": f"market.{symbol}.mbp.{self.MBP_INCREMENTAL_DEPTH}",
                        "id": f"sub_incr_{symbol}"
                    }))
                    await ws.send(json.dumps({
                        "sub": f"market.{symbol}.mbp.refresh.{self.MBP_REFRESH_DEPTH}",
                        "id": f"sub_refresh_{symbol}"
                    }))
                    print(f"[SUB] {symbol}: mbp.5 + mbp.refresh.20")
                
                print()
                print("Listening for messages...")
                print("-" * 80)
                
                last_progress = self.start_time
                
                while time.time() < end_time:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        now = time.time()
                        self.last_any_message_time = now
                        
                        # Decompress
                        try:
                            data = gzip.decompress(raw)
                            msg = json.loads(data)
                        except:
                            msg = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
                        
                        # Handle ping
                        if "ping" in msg:
                            await ws.send(json.dumps({"pong": msg["ping"]}))
                            self.global_pings += 1
                            continue
                        
                        # Handle ACK
                        if "subbed" in msg:
                            print(f"[ACK] {msg.get('subbed')}")
                            continue
                        
                        # Handle data
                        channel = msg.get("ch", "")
                        if not channel:
                            continue
                        
                        parts = channel.split(".")
                        if len(parts) < 2:
                            continue
                        symbol = parts[1]
                        
                        if symbol not in self.symbols:
                            continue
                        
                        tick = msg.get("tick", {})
                        seq_num = tick.get("seqNum", 0)
                        msg_ts = msg.get("ts", now * 1000) / 1000
                        
                        if ".mbp.refresh." in channel:
                            bids = tick.get("bids", [])
                            asks = tick.get("asks", [])
                            self.apply_snapshot(symbol, bids, asks, seq_num, msg_ts)
                        elif ".mbp." in channel and ".refresh." not in channel:
                            bids = tick.get("bids", [])
                            asks = tick.get("asks", [])
                            prev_seq_num = tick.get("prevSeqNum", 0)
                            self.apply_incremental(symbol, bids, asks, seq_num, prev_seq_num, msg_ts)
                        
                        msg_count += 1
                        
                        # Progress update every 30s
                        if now - last_progress >= 30:
                            elapsed = now - self.start_time
                            remaining = end_time - now
                            total_refresh = sum(s.refresh_count for s in self.stats.values())
                            total_incr = sum(s.incremental_applied for s in self.stats.values())
                            print(f"[{elapsed:.0f}s] {msg_count} msgs | {total_refresh} refresh | {total_incr} incr | {self.global_pings} pings | {remaining:.0f}s remaining")
                            last_progress = now
                            
                    except asyncio.TimeoutError:
                        continue
                        
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        self.print_final_report()


async def main():
    tester = HTXMBPTester(symbols=["DELOREAN_USDT"])
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
