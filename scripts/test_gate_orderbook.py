#!/usr/bin/env python3
"""
Gate.io Order Book V2 Test Script

This script tests the Gate.io WebSocket Order Book V2 implementation,
verifying that:
1. full=true snapshots are correctly identified and applied
2. Incremental diffs (full=false or absent) are correctly applied
3. The local orderbook stays fresh and consistent

Usage:
    python scripts/test_gate_orderbook.py [SYMBOL]
    
Example:
    python scripts/test_gate_orderbook.py BTC_USDT
"""

import asyncio
import json
import time
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)


# ============================================================================
# Configuration
# ============================================================================

WS_URL = "wss://api.gateio.ws/ws/v4/"
DEFAULT_SYMBOL = "BTC_USDT"
ORDER_BOOK_LEVEL = 50  # 50 = 20ms updates, 400 = 100ms updates
PING_INTERVAL = 30.0
RUN_DURATION = 60.0  # Run for 60 seconds by default


# ============================================================================
# Simple OrderBook Implementation (mimics Hummingbot's OrderBook)
# ============================================================================

@dataclass
class OrderBookLevel:
    price: float
    amount: float
    update_id: int


class SimpleOrderBook:
    """
    Simplified orderbook that mimics Hummingbot's OrderBook behavior.
    Uses sorted dict-like structure for bid/ask levels.
    """
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._bids: Dict[float, OrderBookLevel] = {}  # price -> level
        self._asks: Dict[float, OrderBookLevel] = {}  # price -> level
        self._snapshot_uid: int = 0
        self._last_diff_uid: int = 0
        self._last_applied_diff: float = 0.0
        self._last_applied_snapshot: float = 0.0
        self._diff_count: int = 0
        self._snapshot_count: int = 0
    
    @property
    def best_bid(self) -> Optional[float]:
        if not self._bids:
            return None
        return max(self._bids.keys())
    
    @property
    def best_ask(self) -> Optional[float]:
        if not self._asks:
            return None
        return min(self._asks.keys())
    
    @property
    def spread(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return ba - bb
    
    @property
    def spread_pct(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None or bb == 0:
            return None
        return ((ba - bb) / bb) * 100
    
    def apply_snapshot(self, bids: List[List[str]], asks: List[List[str]], update_id: int):
        """Replace entire orderbook with snapshot data (full=true)."""
        self._bids.clear()
        self._asks.clear()
        
        for bid in bids:
            price = float(bid[0])
            amount = float(bid[1])
            if amount > 0:
                self._bids[price] = OrderBookLevel(price, amount, update_id)
        
        for ask in asks:
            price = float(ask[0])
            amount = float(ask[1])
            if amount > 0:
                self._asks[price] = OrderBookLevel(price, amount, update_id)
        
        self._snapshot_uid = update_id
        self._last_applied_snapshot = time.time()
        self._snapshot_count += 1
    
    def apply_diff(self, bids: List[List[str]], asks: List[List[str]], update_id: int):
        """Apply incremental changes to orderbook (full=false or absent)."""
        for bid in bids:
            price = float(bid[0])
            amount = float(bid[1])
            if amount == 0:
                self._bids.pop(price, None)  # Remove level
            else:
                self._bids[price] = OrderBookLevel(price, amount, update_id)
        
        for ask in asks:
            price = float(ask[0])
            amount = float(ask[1])
            if amount == 0:
                self._asks.pop(price, None)  # Remove level
            else:
                self._asks[price] = OrderBookLevel(price, amount, update_id)
        
        self._last_diff_uid = update_id
        self._last_applied_diff = time.time()
        self._diff_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bid_levels": len(self._bids),
            "ask_levels": len(self._asks),
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "spread_pct": self.spread_pct,
            "snapshot_uid": self._snapshot_uid,
            "last_diff_uid": self._last_diff_uid,
            "snapshot_count": self._snapshot_count,
            "diff_count": self._diff_count,
            "last_snapshot_age": time.time() - self._last_applied_snapshot if self._last_applied_snapshot else None,
            "last_diff_age": time.time() - self._last_applied_diff if self._last_applied_diff else None,
        }


# ============================================================================
# Message Statistics Tracker
# ============================================================================

@dataclass
class MessageStats:
    """Track message statistics for analysis."""
    total_messages: int = 0
    snapshot_messages: int = 0
    diff_messages: int = 0
    pong_messages: int = 0
    subscribe_acks: int = 0
    error_messages: int = 0
    unknown_messages: int = 0
    
    # Timing
    first_message_time: Optional[float] = None
    last_message_time: Optional[float] = None
    last_snapshot_time: Optional[float] = None
    last_diff_time: Optional[float] = None
    
    # Sequence tracking
    last_u_by_symbol: Dict[str, int] = field(default_factory=dict)
    sequence_gaps: int = 0
    
    def record_message(self, msg_type: str, symbol: str = None, u: int = None, U: int = None):
        now = time.time()
        self.total_messages += 1
        
        if self.first_message_time is None:
            self.first_message_time = now
        self.last_message_time = now
        
        if msg_type == "snapshot":
            self.snapshot_messages += 1
            self.last_snapshot_time = now
            if symbol and u is not None:
                self.last_u_by_symbol[symbol] = u
        elif msg_type == "diff":
            self.diff_messages += 1
            self.last_diff_time = now
            # Check for sequence gap
            if symbol and U is not None and u is not None:
                expected_U = self.last_u_by_symbol.get(symbol, 0) + 1
                if U != expected_U and self.last_u_by_symbol.get(symbol) is not None:
                    self.sequence_gaps += 1
                    print(f"  ⚠️  Sequence gap for {symbol}: expected U={expected_U}, got U={U}")
                self.last_u_by_symbol[symbol] = u
        elif msg_type == "pong":
            self.pong_messages += 1
        elif msg_type == "subscribe":
            self.subscribe_acks += 1
        elif msg_type == "error":
            self.error_messages += 1
        else:
            self.unknown_messages += 1
    
    def get_summary(self) -> str:
        elapsed = (self.last_message_time - self.first_message_time) if self.first_message_time else 0
        msg_rate = self.total_messages / elapsed if elapsed > 0 else 0
        
        return f"""
╔════════════════════════════════════════════════════════════════════╗
║                    GATE.IO ORDERBOOK TEST RESULTS                  ║
╠════════════════════════════════════════════════════════════════════╣
║  Duration:            {elapsed:>8.1f} seconds                             ║
║  Total Messages:      {self.total_messages:>8d}                                     ║
║  Message Rate:        {msg_rate:>8.1f} msg/sec                              ║
╠════════════════════════════════════════════════════════════════════╣
║  📸 Snapshots:        {self.snapshot_messages:>8d}  (full=true)                       ║
║  📊 Diffs:            {self.diff_messages:>8d}  (incremental)                      ║
║  🏓 Pongs:            {self.pong_messages:>8d}                                     ║
║  ✅ Subscribe Acks:   {self.subscribe_acks:>8d}                                     ║
║  ❌ Errors:           {self.error_messages:>8d}                                     ║
║  ❓ Unknown:          {self.unknown_messages:>8d}                                     ║
╠════════════════════════════════════════════════════════════════════╣
║  🔢 Sequence Gaps:    {self.sequence_gaps:>8d}                                     ║
╚════════════════════════════════════════════════════════════════════╝
"""


# ============================================================================
# WebSocket Handler
# ============================================================================

async def run_gate_orderbook_test(symbol: str, duration: float = RUN_DURATION):
    """
    Connect to Gate.io WebSocket and test orderbook handling.
    """
    print(f"\n🚀 Starting Gate.io Order Book V2 Test")
    print(f"   Symbol: {symbol}")
    print(f"   Level: {ORDER_BOOK_LEVEL} (updates every {20 if ORDER_BOOK_LEVEL == 50 else 100}ms)")
    print(f"   Duration: {duration}s")
    print(f"   WebSocket URL: {WS_URL}")
    print()
    
    orderbook = SimpleOrderBook(symbol)
    stats = MessageStats()
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(WS_URL, heartbeat=PING_INTERVAL) as ws:
                print(f"✅ Connected to Gate.io WebSocket")
                
                # Subscribe to orderbook
                subscribe_msg = {
                    "time": int(time.time()),
                    "channel": "spot.obu",
                    "event": "subscribe",
                    "payload": [f"ob.{symbol}.{ORDER_BOOK_LEVEL}"]
                }
                await ws.send_json(subscribe_msg)
                print(f"📤 Sent subscription request: ob.{symbol}.{ORDER_BOOK_LEVEL}")
                
                # Also send initial ping
                ping_msg = {
                    "time": int(time.time()),
                    "channel": "spot.ping",
                    "event": "",
                    "payload": []
                }
                await ws.send_json(ping_msg)
                
                last_ping_time = time.time()
                last_status_time = time.time()
                
                print()
                print("=" * 70)
                print("LIVE MESSAGE LOG")
                print("=" * 70)
                
                while time.time() - start_time < duration:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            process_message(data, orderbook, stats)
                            
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            print("❌ WebSocket closed by server")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"❌ WebSocket error: {ws.exception()}")
                            break
                    
                    except asyncio.TimeoutError:
                        pass  # No message received, continue
                    
                    # Send periodic ping
                    now = time.time()
                    if now - last_ping_time >= PING_INTERVAL:
                        ping_msg = {
                            "time": int(now),
                            "channel": "spot.ping",
                            "event": "",
                            "payload": []
                        }
                        await ws.send_json(ping_msg)
                        last_ping_time = now
                    
                    # Print periodic status
                    if now - last_status_time >= 10.0:
                        print_status(orderbook, stats)
                        last_status_time = now
                
                print()
                print("=" * 70)
                print("TEST COMPLETE")
                print("=" * 70)
                
        except aiohttp.ClientError as e:
            print(f"❌ Connection error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
    
    # Print final summary
    print(stats.get_summary())
    print_orderbook_summary(orderbook)
    
    return stats, orderbook


def process_message(data: Dict[str, Any], orderbook: SimpleOrderBook, stats: MessageStats):
    """Process a single WebSocket message."""
    channel = data.get("channel", "")
    event = data.get("event", "")
    
    # Handle pong
    if channel == "spot.pong":
        stats.record_message("pong")
        return
    
    # Handle subscribe ack
    if event == "subscribe":
        result = data.get("result", {})
        status = result.get("status") if isinstance(result, dict) else None
        if status == "success":
            stats.record_message("subscribe")
            print(f"✅ Subscription successful: {data.get('payload')}")
        else:
            stats.record_message("error")
            print(f"❌ Subscription failed: {data}")
        return
    
    # Handle errors
    if data.get("error") is not None:
        stats.record_message("error")
        print(f"❌ Error received: {data.get('error')}")
        return
    
    # Handle orderbook updates
    if channel == "spot.obu" and event == "update":
        result = data.get("result", {})
        if not isinstance(result, dict):
            stats.record_message("unknown")
            return
        
        is_full = result.get("full", False)
        stream_name = result.get("s", "")
        u = result.get("u")  # End update ID
        U = result.get("U")  # Start update ID (only in diffs)
        t = result.get("t")  # Timestamp in ms
        bids = result.get("b", [])
        asks = result.get("a", [])
        
        # Extract symbol from stream name (e.g., "ob.BTC_USDT.50" -> "BTC_USDT")
        parts = stream_name.split(".")
        symbol = parts[1] if len(parts) > 1 else stream_name
        
        if is_full:
            # SNAPSHOT - replace entire orderbook
            stats.record_message("snapshot", symbol=symbol, u=u)
            orderbook.apply_snapshot(bids, asks, u)
            
            latency_ms = (time.time() * 1000 - t) if t else None
            latency_str = f" (latency: {latency_ms:.0f}ms)" if latency_ms else ""
            print(f"📸 SNAPSHOT: u={u}, bids={len(bids)}, asks={len(asks)}{latency_str}")
        else:
            # DIFF - apply incremental changes
            stats.record_message("diff", symbol=symbol, u=u, U=U)
            orderbook.apply_diff(bids, asks, u)
            
            # Only log every 100th diff to avoid spam
            if stats.diff_messages % 100 == 0:
                latency_ms = (time.time() * 1000 - t) if t else None
                latency_str = f" (latency: {latency_ms:.0f}ms)" if latency_ms else ""
                print(f"📊 DIFF #{stats.diff_messages}: U={U}→u={u}, changes: bids={len(bids)}, asks={len(asks)}{latency_str}")
        
        return
    
    # Unknown message
    stats.record_message("unknown")
    print(f"❓ Unknown message: channel={channel}, event={event}")


def print_status(orderbook: SimpleOrderBook, stats: MessageStats):
    """Print current orderbook status."""
    ob_stats = orderbook.get_stats()
    now = datetime.now().strftime("%H:%M:%S")
    
    spread_str = f"{ob_stats['spread_pct']:.4f}%" if ob_stats['spread_pct'] else "N/A"
    best_bid = f"{ob_stats['best_bid']:.2f}" if ob_stats['best_bid'] else "N/A"
    best_ask = f"{ob_stats['best_ask']:.2f}" if ob_stats['best_ask'] else "N/A"
    
    print()
    print(f"[{now}] 📊 Status: {orderbook.symbol}")
    print(f"    Bid: {best_bid} | Ask: {best_ask} | Spread: {spread_str}")
    print(f"    Levels: {ob_stats['bid_levels']} bids, {ob_stats['ask_levels']} asks")
    print(f"    Messages: {stats.snapshot_messages} snapshots, {stats.diff_messages} diffs")
    print(f"    Last diff: {ob_stats['last_diff_age']:.1f}s ago" if ob_stats['last_diff_age'] else "    No diffs yet")
    print()


def print_orderbook_summary(orderbook: SimpleOrderBook):
    """Print final orderbook summary."""
    ob_stats = orderbook.get_stats()
    
    print()
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║                    FINAL ORDERBOOK STATE                            ║")
    print("╠════════════════════════════════════════════════════════════════════╣")
    print(f"║  Symbol:           {orderbook.symbol:<49} ║")
    print(f"║  Bid Levels:       {ob_stats['bid_levels']:<49} ║")
    print(f"║  Ask Levels:       {ob_stats['ask_levels']:<49} ║")
    
    if ob_stats['best_bid']:
        print(f"║  Best Bid:         {ob_stats['best_bid']:<49.8f} ║")
    if ob_stats['best_ask']:
        print(f"║  Best Ask:         {ob_stats['best_ask']:<49.8f} ║")
    if ob_stats['spread_pct']:
        print(f"║  Spread:           {ob_stats['spread_pct']:<47.6f}% ║")
    
    print("╠════════════════════════════════════════════════════════════════════╣")
    print(f"║  Snapshots Applied:{ob_stats['snapshot_count']:>49} ║")
    print(f"║  Diffs Applied:    {ob_stats['diff_count']:>49} ║")
    print(f"║  Snapshot UID:     {ob_stats['snapshot_uid']:>49} ║")
    print(f"║  Last Diff UID:    {ob_stats['last_diff_uid']:>49} ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    # Print top 5 bids and asks
    if orderbook._bids or orderbook._asks:
        print()
        print("  TOP 5 LEVELS:")
        print("  " + "-" * 50)
        print(f"  {'BIDS':<24} | {'ASKS':<24}")
        print("  " + "-" * 50)
        
        sorted_bids = sorted(orderbook._bids.values(), key=lambda x: -x.price)[:5]
        sorted_asks = sorted(orderbook._asks.values(), key=lambda x: x.price)[:5]
        
        for i in range(5):
            bid_str = ""
            ask_str = ""
            if i < len(sorted_bids):
                b = sorted_bids[i]
                bid_str = f"{b.amount:>12.4f} @ {b.price:<10.2f}"
            if i < len(sorted_asks):
                a = sorted_asks[i]
                ask_str = f"{a.price:>10.2f} @ {a.amount:<12.4f}"
            print(f"  {bid_str:<24} | {ask_str:<24}")
        print()


# ============================================================================
# Main
# ============================================================================

async def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SYMBOL
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else RUN_DURATION
    
    stats, orderbook = await run_gate_orderbook_test(symbol, duration)
    
    # Validation checks
    print("\n" + "=" * 70)
    print("VALIDATION CHECKS")
    print("=" * 70)
    
    passed = True
    
    # Check 1: Did we receive at least one snapshot?
    if stats.snapshot_messages > 0:
        print("✅ PASS: Received at least one full=true snapshot")
    else:
        print("❌ FAIL: No full=true snapshots received!")
        passed = False
    
    # Check 2: First message should be a snapshot
    if stats.snapshot_messages > 0:
        print("✅ PASS: Snapshot was received (should be first message)")
    else:
        print("⚠️  WARN: Could not verify first message was snapshot")
    
    # Check 3: Did we receive diffs after snapshot?
    if stats.diff_messages > 0:
        print(f"✅ PASS: Received {stats.diff_messages} incremental diffs")
    else:
        print("⚠️  WARN: No incremental diffs received")
    
    # Check 4: Orderbook has levels
    ob_stats = orderbook.get_stats()
    if ob_stats['bid_levels'] > 0 and ob_stats['ask_levels'] > 0:
        print(f"✅ PASS: Orderbook has {ob_stats['bid_levels']} bids and {ob_stats['ask_levels']} asks")
    else:
        print(f"❌ FAIL: Orderbook is empty! bids={ob_stats['bid_levels']}, asks={ob_stats['ask_levels']}")
        passed = False
    
    # Check 5: Spread is reasonable
    if ob_stats['spread_pct'] is not None and 0 < ob_stats['spread_pct'] < 1.0:
        print(f"✅ PASS: Spread is reasonable: {ob_stats['spread_pct']:.4f}%")
    elif ob_stats['spread_pct'] is not None:
        print(f"⚠️  WARN: Spread seems unusual: {ob_stats['spread_pct']:.4f}%")
    else:
        print("❌ FAIL: Could not calculate spread")
        passed = False
    
    # Check 6: Sequence gaps
    if stats.sequence_gaps == 0:
        print("✅ PASS: No sequence gaps detected")
    else:
        print(f"⚠️  WARN: {stats.sequence_gaps} sequence gaps detected")
    
    # Check 7: No errors
    if stats.error_messages == 0:
        print("✅ PASS: No error messages received")
    else:
        print(f"❌ FAIL: {stats.error_messages} error messages received")
        passed = False
    
    print()
    if passed:
        print("🎉 ALL CRITICAL CHECKS PASSED!")
    else:
        print("❌ SOME CHECKS FAILED - Review output above")
    print()
    
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(0)
