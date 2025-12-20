#!/usr/bin/env python3
"""
HTX MBP Hybrid Orderbook Test - Mirrors Hummingbot's HtxAPIOrderBookDataSource

Tests the hybrid approach exactly as implemented in htx_api_order_book_data_source.py:
- mbp.5: tick-by-tick incremental updates (DIFF messages)
- mbp.refresh.20: 100ms full snapshots (SNAPSHOT messages)

Key Logic Mirrored from Hummingbot:
1. _parse_order_book_diff_message logic for refresh vs incremental
2. Sequence tracking: skip if seq_num <= last_seq
3. Gap detection: if prev_seq_num != last_seq, mark as needing refresh
4. Optimistic application: apply incrementals despite gaps (refresh heals)

Run for configurable duration, show live best bid/ask, then print comprehensive report.
"""

import asyncio
import gzip
import json
import time
import sys
import websockets
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from enum import Enum


class OrderBookMessageType(Enum):
    """Mirrors hummingbot.core.data_type.order_book_message.OrderBookMessageType"""
    SNAPSHOT = 1
    DIFF = 2


@dataclass
class OrderBookMessage:
    """Mirrors hummingbot OrderBookMessage for testing"""
    type: OrderBookMessageType
    trading_pair: str
    update_id: int
    timestamp: float
    bids: List
    asks: List


@dataclass
class OrderBookState:
    """Local orderbook state - mirrors what OrderBook Cython class maintains"""
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0


@dataclass 
class SymbolTracker:
    """Per-symbol tracking - mirrors HtxAPIOrderBookDataSource._last_seq_num"""
    last_seq_num: Optional[int] = None  # Mirrors self._last_seq_num[symbol]
    needs_refresh: bool = False         # Mirrors self._symbols_needing_refresh
    
    # Stats
    snapshot_count: int = 0
    diff_count: int = 0
    diff_applied: int = 0
    diff_skipped_no_init: int = 0
    diff_skipped_old: int = 0
    gaps_detected: int = 0
    gaps_healed: int = 0
    
    # Timing
    last_snapshot_ts: float = 0
    last_diff_ts: float = 0
    snapshot_intervals: List[float] = field(default_factory=list)
    diff_intervals: List[float] = field(default_factory=list)
    
    # Sequence analysis
    sequence_gaps: List[Tuple[int, int, int]] = field(default_factory=list)  # (expected, got_prev, got_seq)


class HTXConnectorTester:
    """
    Mimics HtxAPIOrderBookDataSource._parse_order_book_diff_message
    """
    
    # Constants matching htx_constants.py
    URL = "wss://api-aws.huobi.pro/feed"
    MBP_INCREMENTAL_DEPTH = 5
    MBP_REFRESH_DEPTH = 20
    
    def __init__(self, symbols: List[str], duration_seconds: int = 300):
        # Normalize symbols (HTX uses lowercase, no separators)
        self.symbols = [s.lower().replace("-", "").replace("_", "") for s in symbols]
        self.duration = duration_seconds
        
        # State tracking - mirrors HtxAPIOrderBookDataSource
        self.trackers: Dict[str, SymbolTracker] = {s: SymbolTracker() for s in self.symbols}
        self.orderbooks: Dict[str, OrderBookState] = {s: OrderBookState() for s in self.symbols}
        
        # Message queue simulation - mirrors self._message_queue[channel]
        self.message_queue: List[OrderBookMessage] = []
        
        # Global stats
        self.start_time: float = 0
        self.total_messages: int = 0
        self.pings_received: int = 0
        
        # Display state
        self._last_displayed_bbo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    
    def _get_best_bid(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Get best bid (highest price)"""
        bids = self.orderbooks[symbol].bids
        if not bids:
            return None
        best_price = max(bids.keys())
        return (best_price, bids[best_price])
    
    def _get_best_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Get best ask (lowest price)"""
        asks = self.orderbooks[symbol].asks
        if not asks:
            return None
        best_price = min(asks.keys())
        return (best_price, asks[best_price])
    
    def _update_bbo_display(self, symbol: str):
        """Update best bid/ask display - only if price changed"""
        best_bid = self._get_best_bid(symbol)
        best_ask = self._get_best_ask(symbol)
        
        bid_price = best_bid[0] if best_bid else None
        ask_price = best_ask[0] if best_ask else None
        
        last = self._last_displayed_bbo.get(symbol, (None, None))
        
        if (bid_price, ask_price) != last:
            self._last_displayed_bbo[symbol] = (bid_price, ask_price)
            
            # Format spread
            spread = None
            spread_bps = None
            if bid_price and ask_price:
                spread = ask_price - bid_price
                mid = (bid_price + ask_price) / 2
                spread_bps = (spread / mid) * 10000 if mid > 0 else 0
            
            elapsed = time.time() - self.start_time
            bid_str = f"{bid_price:.8f}" if bid_price else "---"
            ask_str = f"{ask_price:.8f}" if ask_price else "---"
            spread_str = f"{spread_bps:.2f}bps" if spread_bps else "---"
            
            # Overwrite line in place
            line = f"\r[{elapsed:6.1f}s] {symbol.upper()}: Bid {bid_str} | Ask {ask_str} | Spread {spread_str}    "
            sys.stdout.write(line)
            sys.stdout.flush()
    
    def _parse_order_book_diff_message(self, raw_message: Dict) -> Optional[OrderBookMessage]:
        """
        EXACT MIRROR of HtxAPIOrderBookDataSource._parse_order_book_diff_message
        
        Handles three types:
        1. mbp.refresh.X: 100ms full snapshots → SNAPSHOT
        2. mbp.X: tick-by-tick incremental → DIFF (with sequence validation)
        3. req response: explicit snapshot request → SNAPSHOT
        """
        # Check if snapshot response from "req"
        is_req_response = "rep" in raw_message
        
        if is_req_response:
            # Snapshot response from "req" - treat as SNAPSHOT
            msg_channel = raw_message.get("rep", "")
            parts = msg_channel.split(".")
            if len(parts) < 2:
                return None
            symbol = parts[1]
            
            if symbol not in self.symbols:
                return None
            
            tracker = self.trackers[symbol]
            data = raw_message.get("data", {})
            seq_num = data.get("seqNum", 0)
            
            # Update sequence tracking (mirrors connector)
            tracker.last_seq_num = seq_num
            if tracker.needs_refresh:
                tracker.needs_refresh = False
                tracker.gaps_healed += 1
            
            tracker.snapshot_count += 1
            msg_ts = raw_message.get("ts", time.time() * 1000) / 1000
            
            return OrderBookMessage(
                type=OrderBookMessageType.SNAPSHOT,
                trading_pair=symbol,
                update_id=seq_num,
                timestamp=msg_ts,
                bids=data.get("bids", []),
                asks=data.get("asks", [])
            )
        
        # Channel update from "sub"
        msg_channel = raw_message.get("ch", "")
        parts = msg_channel.split(".")
        if len(parts) < 2:
            return None
        symbol = parts[1]
        
        if symbol not in self.symbols:
            return None
        
        tracker = self.trackers[symbol]
        tick = raw_message.get("tick", {})
        seq_num = tick.get("seqNum", 0)
        msg_ts = raw_message.get("ts", time.time() * 1000) / 1000
        
        # Check if refresh (snapshot) or incremental
        is_refresh = ".mbp.refresh." in msg_channel
        
        if is_refresh:
            # mbp.refresh.X - full snapshot at ~100ms intervals
            # ALWAYS update sequence tracking (mirrors connector line 462)
            tracker.last_seq_num = seq_num
            
            if tracker.needs_refresh:
                tracker.needs_refresh = False
                tracker.gaps_healed += 1
            
            # Track timing
            if tracker.last_snapshot_ts > 0:
                interval = (msg_ts - tracker.last_snapshot_ts) * 1000
                tracker.snapshot_intervals.append(interval)
            tracker.last_snapshot_ts = msg_ts
            tracker.snapshot_count += 1
            
            return OrderBookMessage(
                type=OrderBookMessageType.SNAPSHOT,
                trading_pair=symbol,
                update_id=seq_num,
                timestamp=msg_ts,
                bids=tick.get("bids", []),
                asks=tick.get("asks", [])
            )
        
        else:
            # mbp.X - tick-by-tick incremental update
            prev_seq_num = tick.get("prevSeqNum", 0)
            tracker.diff_count += 1
            
            # Check if we have received a snapshot yet (mirrors connector line 488)
            if tracker.last_seq_num is None:
                tracker.diff_skipped_no_init += 1
                return None
            
            # Skip old/duplicate (mirrors connector line 496)
            if seq_num <= tracker.last_seq_num:
                tracker.diff_skipped_old += 1
                return None
            
            # Gap detection (mirrors connector line 502-508)
            if prev_seq_num != tracker.last_seq_num:
                if not tracker.needs_refresh:
                    tracker.gaps_detected += 1
                    tracker.sequence_gaps.append((tracker.last_seq_num, prev_seq_num, seq_num))
                    tracker.needs_refresh = True
            
            # Update sequence tracking (mirrors connector line 511)
            tracker.last_seq_num = seq_num
            
            # Track timing
            if tracker.last_diff_ts > 0:
                interval = (msg_ts - tracker.last_diff_ts) * 1000
                tracker.diff_intervals.append(interval)
            tracker.last_diff_ts = msg_ts
            tracker.diff_applied += 1
            
            return OrderBookMessage(
                type=OrderBookMessageType.DIFF,
                trading_pair=symbol,
                update_id=seq_num,
                timestamp=msg_ts,
                bids=tick.get("bids", []),
                asks=tick.get("asks", [])
            )
    
    def _apply_message_to_orderbook(self, msg: OrderBookMessage):
        """
        Applies OrderBookMessage to local orderbook.
        Mirrors OrderBook.apply_snapshot / apply_diffs behavior.
        """
        ob = self.orderbooks[msg.trading_pair]
        
        if msg.type == OrderBookMessageType.SNAPSHOT:
            # Full replace (mirrors apply_snapshot)
            ob.bids = {float(b[0]): float(b[1]) for b in msg.bids}
            ob.asks = {float(a[0]): float(a[1]) for a in msg.asks}
            ob.last_update_id = msg.update_id
        
        elif msg.type == OrderBookMessageType.DIFF:
            # Incremental update (mirrors apply_diffs)
            for bid in msg.bids:
                price, size = float(bid[0]), float(bid[1])
                if size == 0:
                    ob.bids.pop(price, None)
                else:
                    ob.bids[price] = size
            
            for ask in msg.asks:
                price, size = float(ask[0]), float(ask[1])
                if size == 0:
                    ob.asks.pop(price, None)
                else:
                    ob.asks[price] = size
            
            ob.last_update_id = msg.update_id
        
        # Update display
        self._update_bbo_display(msg.trading_pair)
    
    def print_report(self):
        """Print comprehensive debug report"""
        elapsed = time.time() - self.start_time
        
        print("\n")
        print("=" * 90)
        print("HTX CONNECTOR TEST REPORT - Mirrors HtxAPIOrderBookDataSource Logic")
        print("=" * 90)
        print(f"Runtime: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        print(f"WebSocket: {self.URL}")
        print(f"Channels: mbp.{self.MBP_INCREMENTAL_DEPTH} (DIFF) + mbp.refresh.{self.MBP_REFRESH_DEPTH} (SNAPSHOT)")
        print(f"Symbols: {', '.join(s.upper() for s in self.symbols)}")
        print(f"Total messages: {self.total_messages} | HTX pings: {self.pings_received}")
        print()
        
        for symbol in self.symbols:
            tracker = self.trackers[symbol]
            ob = self.orderbooks[symbol]
            
            print("=" * 90)
            print(f"📊 {symbol.upper()}")
            print("=" * 90)
            
            # Message Type Breakdown (mirrors _parse_order_book_diff_message output)
            print(f"\n📨 OrderBookMessage Generation (mirrors _parse_order_book_diff_message):")
            print(f"   SNAPSHOT messages (mbp.refresh.{self.MBP_REFRESH_DEPTH}): {tracker.snapshot_count}")
            print(f"   DIFF messages (mbp.{self.MBP_INCREMENTAL_DEPTH}):         {tracker.diff_applied}")
            print(f"   DIFF skipped (no init - line 488):    {tracker.diff_skipped_no_init}")
            print(f"   DIFF skipped (old/dup - line 496):    {tracker.diff_skipped_old}")
            
            total_diff_received = tracker.diff_count
            if total_diff_received > 0:
                apply_rate = tracker.diff_applied / total_diff_received * 100
                print(f"   DIFF Apply Rate: {apply_rate:.1f}% ({tracker.diff_applied}/{total_diff_received})")
            
            # Message Rates
            print(f"\n📈 Message Rates:")
            print(f"   SNAPSHOT: {tracker.snapshot_count / elapsed:.2f}/s (expected ~10/s for 100ms)")
            print(f"   DIFF:     {tracker.diff_applied / elapsed:.2f}/s (tick-by-tick)")
            
            # Sequence Analysis (mirrors gap detection logic lines 502-508)
            print(f"\n🔢 Sequence Analysis (mirrors gap detection logic):")
            print(f"   Gaps detected (prev_seq != last_seq): {tracker.gaps_detected}")
            print(f"   Gaps healed (via SNAPSHOT):           {tracker.gaps_healed}")
            unhealed = tracker.gaps_detected - tracker.gaps_healed
            if unhealed > 0:
                print(f"   ⚠️ Unhealed gaps: {unhealed}")
            else:
                print(f"   ✅ All gaps auto-healed via mbp.refresh.{self.MBP_REFRESH_DEPTH}")
            
            if tracker.sequence_gaps and len(tracker.sequence_gaps) <= 5:
                print(f"\n   Gap Details (last_seq → prevSeqNum → seqNum):")
                for i, (last, prev, seq) in enumerate(tracker.sequence_gaps[:5]):
                    jump = prev - last
                    print(f"     #{i+1}: {last} → {prev} → {seq} (gap={jump:+d})")
            elif tracker.sequence_gaps:
                print(f"\n   Gap Details: {len(tracker.sequence_gaps)} gaps (showing first 5)")
                for i, (last, prev, seq) in enumerate(tracker.sequence_gaps[:5]):
                    jump = prev - last
                    print(f"     #{i+1}: {last} → {prev} → {seq} (gap={jump:+d})")
            
            # Timing Analysis
            print(f"\n⏱️ Timing Analysis:")
            
            if tracker.diff_intervals:
                intervals = tracker.diff_intervals
                avg = sum(intervals) / len(intervals)
                sorted_i = sorted(intervals)
                p50 = sorted_i[len(sorted_i) // 2]
                p95_idx = min(int(len(sorted_i) * 0.95), len(sorted_i) - 1)
                p95 = sorted_i[p95_idx]
                
                print(f"\n   DIFF (mbp.{self.MBP_INCREMENTAL_DEPTH}) intervals:")
                print(f"     Mean: {avg:.1f}ms | P50: {p50:.1f}ms | P95: {p95:.1f}ms")
                print(f"     Min: {min(intervals):.1f}ms | Max: {max(intervals):.1f}ms")
                
                under_10 = sum(1 for i in intervals if i < 10)
                under_50 = sum(1 for i in intervals if i < 50)
                under_100 = sum(1 for i in intervals if i < 100)
                print(f"     <10ms: {under_10/len(intervals)*100:.0f}% | <50ms: {under_50/len(intervals)*100:.0f}% | <100ms: {under_100/len(intervals)*100:.0f}%")
            
            if tracker.snapshot_intervals:
                intervals = tracker.snapshot_intervals
                avg = sum(intervals) / len(intervals)
                print(f"\n   SNAPSHOT (mbp.refresh.{self.MBP_REFRESH_DEPTH}) intervals:")
                print(f"     Mean: {avg:.1f}ms (expected ~100ms)")
                print(f"     Min: {min(intervals):.1f}ms | Max: {max(intervals):.1f}ms")
            
            # Final Orderbook State
            best_bid = self._get_best_bid(symbol)
            best_ask = self._get_best_ask(symbol)
            
            print(f"\n📖 Final Orderbook State:")
            if best_bid and best_ask:
                spread = best_ask[0] - best_bid[0]
                mid = (best_bid[0] + best_ask[0]) / 2
                spread_bps = (spread / mid) * 10000 if mid > 0 else 0
                print(f"   Best Bid: {best_bid[0]:.8f} (size: {best_bid[1]:.4f})")
                print(f"   Best Ask: {best_ask[0]:.8f} (size: {best_ask[1]:.4f})")
                print(f"   Spread:   {spread:.8f} ({spread_bps:.2f} bps)")
                print(f"   Depth:    {len(ob.bids)} bids, {len(ob.asks)} asks")
                print(f"   Last update_id: {ob.last_update_id}")
                print(f"   Tracker last_seq_num: {tracker.last_seq_num}")
            else:
                print("   ⚠️ Orderbook empty!")
        
        # Verdict
        print("\n" + "=" * 90)
        print("📋 HUMMINGBOT CONNECTOR COMPATIBILITY VERDICT")
        print("=" * 90)
        
        all_healthy = True
        for symbol in self.symbols:
            tracker = self.trackers[symbol]
            issues = []
            
            if tracker.gaps_detected > tracker.gaps_healed:
                issues.append(f"Unhealed gaps: {tracker.gaps_detected - tracker.gaps_healed}")
                all_healthy = False
            if tracker.diff_applied == 0 and tracker.diff_count > 0:
                issues.append("No DIFF messages applied")
                all_healthy = False
            if tracker.snapshot_count == 0:
                issues.append("No SNAPSHOT messages received")
                all_healthy = False
            if tracker.diff_skipped_no_init > tracker.snapshot_count:
                issues.append(f"Many DIFFs skipped before init ({tracker.diff_skipped_no_init})")
            
            if not issues:
                print(f"✅ {symbol.upper()}: HEALTHY")
                print(f"   Logic validated: _parse_order_book_diff_message ✓")
                print(f"   SNAPSHOT/DIFF flow: {tracker.snapshot_count} snapshots, {tracker.diff_applied} diffs")
                print(f"   Gap auto-recovery: {tracker.gaps_healed}/{tracker.gaps_detected} healed")
            else:
                print(f"⚠️ {symbol.upper()}: ISSUES")
                for issue in issues:
                    print(f"   - {issue}")
                all_healthy = False
        
        print()
        if all_healthy:
            print("✅ HTX Connector Logic: WORKING CORRECTLY")
            print()
            print("Summary:")
            print(f"  - _parse_order_book_diff_message correctly routes mbp.refresh → SNAPSHOT")
            print(f"  - _parse_order_book_diff_message correctly routes mbp.X → DIFF with seq validation")
            print(f"  - Optimistic DIFF application working (apply despite gaps)")
            print(f"  - Gaps auto-heal via periodic SNAPSHOT (mbp.refresh.{self.MBP_REFRESH_DEPTH})")
        else:
            print("⚠️ HTX Connector Logic: ISSUES DETECTED - review above")
        print("=" * 90)
    
    async def run(self):
        self.start_time = time.time()
        end_time = self.start_time + self.duration
        
        print("=" * 90)
        print("HTX Connector Test - Mirrors HtxAPIOrderBookDataSource")
        print("=" * 90)
        print(f"URL: {self.URL}")
        print(f"Channels: mbp.{self.MBP_INCREMENTAL_DEPTH} + mbp.refresh.{self.MBP_REFRESH_DEPTH}")
        print(f"Symbols: {', '.join(s.upper() for s in self.symbols)}")
        print(f"Duration: {self.duration}s ({self.duration/60:.0f} min)")
        print("=" * 90)
        print()
        print("Connecting...")
        
        try:
            async with websockets.connect(
                self.URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=10
            ) as ws:
                # Subscribe to channels (mirrors _subscribe_mbp)
                for symbol in self.symbols:
                    # Incremental mbp.5
                    await ws.send(json.dumps({
                        "sub": f"market.{symbol}.mbp.{self.MBP_INCREMENTAL_DEPTH}",
                        "id": f"sub_diff_{symbol}"
                    }))
                    # Snapshot mbp.refresh.20
                    await ws.send(json.dumps({
                        "sub": f"market.{symbol}.mbp.refresh.{self.MBP_REFRESH_DEPTH}",
                        "id": f"sub_snap_{symbol}"
                    }))
                    print(f"[SUB] {symbol}: mbp.{self.MBP_INCREMENTAL_DEPTH} + mbp.refresh.{self.MBP_REFRESH_DEPTH}")
                
                print()
                print("Live Best Bid/Ask (updates in place):")
                print("-" * 90)
                
                last_progress = self.start_time
                
                while time.time() < end_time:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        
                        # Decompress (mirrors WSAssistant gzip handling)
                        try:
                            data = gzip.decompress(raw)
                            msg = json.loads(data)
                        except:
                            msg = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
                        
                        # Handle ping (mirrors _process_message_for_unknown_channel)
                        if "ping" in msg:
                            await ws.send(json.dumps({"pong": msg["ping"]}))
                            self.pings_received += 1
                            continue
                        
                        # Handle subscription ACK
                        if "subbed" in msg:
                            continue
                        
                        # Parse message (mirrors _parse_order_book_diff_message)
                        ob_msg = self._parse_order_book_diff_message(msg)
                        
                        if ob_msg:
                            self.total_messages += 1
                            # Apply to orderbook (mirrors OrderBookTracker flow)
                            self._apply_message_to_orderbook(ob_msg)
                        
                        # Progress update every 60s
                        now = time.time()
                        if now - last_progress >= 60:
                            elapsed = now - self.start_time
                            remaining = end_time - now
                            total_snap = sum(t.snapshot_count for t in self.trackers.values())
                            total_diff = sum(t.diff_applied for t in self.trackers.values())
                            print(f"\n[{elapsed:.0f}s] Progress: {total_snap} SNAPSHOT, {total_diff} DIFF | {remaining:.0f}s remaining")
                            last_progress = now
                        
                    except asyncio.TimeoutError:
                        continue
        
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        self.print_report()


async def main():
    # Test with DELOREAN_USDT (low volume) to stress-test gap recovery
    tester = HTXConnectorTester(
        symbols=["DELOREAN_USDT"],
        duration_seconds=300  # 5 minutes
    )
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
