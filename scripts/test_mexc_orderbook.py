#!/usr/bin/env python3
"""
MEXC Order Book V3 Test Script

Tests the MEXC WebSocket orderbook stream with 10ms updates.
Mimics Hummingbot's implementation to verify the connector works correctly.

Features:
- Connects to MEXC WebSocket (wbs-api.mexc.com)
- Uses Protocol Buffers format for fast parsing
- Subscribes to spot@public.aggre.depth.v3.api.pb@10ms
- Tracks message rates, latency, and orderbook state
- Validates 10ms update frequency

Usage:
    python scripts/test_mexc_orderbook.py [SYMBOL] [DURATION_SECONDS]
    
    Examples:
        python scripts/test_mexc_orderbook.py BTCUSDT 30
        python scripts/test_mexc_orderbook.py ETHUSDT 60
"""

import asyncio
import json
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp is required. Install with: pip install aiohttp")
    sys.exit(1)

# Try to import protobuf definitions from Hummingbot
try:
    sys.path.insert(0, '/Users/pavel/Documents/VS_code_projects/HMB/dev_bb28/hummingbot')
    from hummingbot.connector.exchange.mexc.pb import PushDataV3ApiWrapper_pb2 as PBPush
    PROTOBUF_AVAILABLE = True
except Exception as e:
    print(f"WARNING: Protobuf not available ({e}). Will parse JSON only.")
    PBPush = None
    PROTOBUF_AVAILABLE = False

# MEXC WebSocket endpoints
WSS_API_URL = "wss://wbs-api.mexc.com/ws"
WSS_LEGACY_URL = "wss://wbs.mexc.com/ws"


class MexcOrderBookTester:
    """MEXC Order Book tester mimicking Hummingbot implementation"""
    
    def __init__(self, symbol: str = "BTCUSDT", duration: float = 60.0):
        self.symbol = symbol.upper()
        self.duration = duration
        
        # Message counters
        self.stats = {
            "depth_messages": 0,
            "trade_messages": 0,
            "pings_received": 0,
            "pongs_sent": 0,
            "errors": 0,
            "unknown": 0,
            "subscription_acks": 0,
        }
        
        # Orderbook state
        self.bids: Dict[str, str] = {}  # price -> quantity
        self.asks: Dict[str, str] = {}  # price -> quantity
        self.last_update_id: int = 0
        self.last_update_time: float = 0
        
        # Latency tracking
        self.latencies: List[float] = []
        
        # Update interval tracking
        self.update_times: List[float] = []
        
        # Symbol cache (mimics Hummingbot optimization)
        self._symbol_to_pair_cache: Dict[str, str] = {}
    
    def decode_protobuf(self, payload: bytes) -> Optional[Dict[str, Any]]:
        """Decode protobuf message - mimics MexcWSPostProcessor"""
        if not PROTOBUF_AVAILABLE or PBPush is None:
            return None
            
        try:
            wrapper = PBPush.PushDataV3ApiWrapper()
            wrapper.ParseFromString(payload)
            
            channel = wrapper.channel
            symbol = getattr(wrapper, 'symbol', '') or ''
            create_time = getattr(wrapper, 'createTime', 0) or 0
            send_time = getattr(wrapper, 'sendTime', 0) or 0
            ts = int(create_time or send_time or time.time() * 1000)
            
            # Public aggregated depth (what we subscribe to)
            if wrapper.HasField('publicAggreDepths'):
                body = wrapper.publicAggreDepths
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {
                        "r": getattr(body, 'toVersion', ''),
                        "bids": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'bids', [])],
                        "asks": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'asks', [])],
                    },
                }
            
            # Public aggregated deals
            if wrapper.HasField('publicAggreDeals'):
                body = wrapper.publicAggreDeals
                deals = [{
                    "p": it.price,
                    "v": it.quantity,
                    "S": int(getattr(it, 'tradeType', 0)),
                    "t": int(getattr(it, 'time', ts)),
                } for it in getattr(body, 'deals', [])]
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {"deals": deals},
                }
            
            # Fallback for other message types
            return {"c": channel, "s": symbol, "t": ts, "type": "other"}
            
        except Exception as e:
            return None
    
    def process_depth_message(self, msg: Dict[str, Any]):
        """Process depth/orderbook message - mimics MexcOrderBook.diff_message_from_exchange"""
        try:
            data = msg.get("d", {})
            ts = msg.get("t", 0)
            
            # Calculate latency
            if ts > 0:
                latency_ms = (time.time() * 1000) - ts
                self.latencies.append(latency_ms)
            
            # Track update intervals
            now = time.time()
            if self.last_update_time > 0:
                interval = (now - self.last_update_time) * 1000  # ms
                self.update_times.append(interval)
            self.last_update_time = now
            
            # Update orderbook
            update_id = data.get("r", 0)
            if isinstance(update_id, str) and update_id.isdigit():
                update_id = int(update_id)
            
            # Apply bids
            for bid in data.get("bids", []):
                price = str(bid.get("p", ""))
                qty = str(bid.get("v", ""))
                if float(qty) == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = qty
            
            # Apply asks
            for ask in data.get("asks", []):
                price = str(ask.get("p", ""))
                qty = str(ask.get("v", ""))
                if float(qty) == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = qty
            
            self.last_update_id = update_id
            self.stats["depth_messages"] += 1
            
            return True
        except Exception as e:
            self.stats["errors"] += 1
            return False
    
    def process_trade_message(self, msg: Dict[str, Any]):
        """Process trade message"""
        self.stats["trade_messages"] += 1
    
    def get_best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return max(float(p) for p in self.bids.keys())
    
    def get_best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(float(p) for p in self.asks.keys())
    
    async def run(self):
        """Main test loop"""
        print("=" * 70)
        print(f"🚀 Starting MEXC Order Book V3 Test")
        print(f"   Symbol: {self.symbol}")
        print(f"   Update Frequency: 10ms (aggregated depth)")
        print(f"   Duration: {self.duration}s")
        print(f"   WebSocket URL: {WSS_API_URL}")
        print(f"   Protobuf: {'✅ Available' if PROTOBUF_AVAILABLE else '❌ Not available'}")
        print("=" * 70)
        
        start_time = time.time()
        last_status_time = start_time
        
        try:
            async with aiohttp.ClientSession() as session:
                # Try primary endpoint, fallback to legacy
                ws_url = WSS_API_URL
                try:
                    async with session.ws_connect(ws_url, heartbeat=30) as ws:
                        print(f"\n✅ Connected to {ws_url}")
                        await self._handle_connection(ws, start_time)
                except Exception as e:
                    print(f"⚠️ Primary endpoint failed: {e}")
                    print(f"   Trying legacy endpoint: {WSS_LEGACY_URL}")
                    async with session.ws_connect(WSS_LEGACY_URL, heartbeat=30) as ws:
                        print(f"\n✅ Connected to {WSS_LEGACY_URL}")
                        await self._handle_connection(ws, start_time)
                        
        except Exception as e:
            print(f"\n❌ Connection error: {e}")
        
        # Print final results
        self._print_results(start_time)
    
    async def _handle_connection(self, ws, start_time: float):
        """Handle WebSocket connection"""
        # Subscribe to depth and trades
        # Using 10ms update frequency (the optimization we implemented)
        depth_sub = f"spot@public.aggre.depth.v3.api.pb@10ms@{self.symbol}"
        trade_sub = f"spot@public.aggre.deals.v3.api.pb@10ms@{self.symbol}"
        
        subscribe_msg = {
            "method": "SUBSCRIPTION",
            "params": [depth_sub, trade_sub],
            "id": 1
        }
        
        print(f"📤 Subscribing to: {depth_sub}")
        await ws.send_json(subscribe_msg)
        
        print("\n" + "=" * 70)
        print("LIVE MESSAGE LOG")
        print("=" * 70)
        
        last_status_time = time.time()
        
        async for msg in ws:
            # Check duration
            elapsed = time.time() - start_time
            if elapsed >= self.duration:
                print(f"\n⏱️ Duration reached ({self.duration}s)")
                break
            
            # Periodic status update (every 10 seconds)
            now = time.time()
            if now - last_status_time >= 10:
                self._print_status()
                last_status_time = now
            
            # Process message
            if msg.type == aiohttp.WSMsgType.BINARY:
                # Protobuf message
                decoded = self.decode_protobuf(msg.data)
                if decoded:
                    self._route_message(decoded)
                else:
                    self.stats["errors"] += 1
                    
            elif msg.type == aiohttp.WSMsgType.TEXT:
                # JSON message (subscription acks, pings)
                try:
                    data = json.loads(msg.data)
                    
                    # Subscription acknowledgment
                    if "id" in data:
                        self.stats["subscription_acks"] += 1
                        if data.get("id") == 1:
                            print(f"✅ Subscription acknowledged")
                    
                    # Ping from server
                    elif data.get("method") == "PING":
                        self.stats["pings_received"] += 1
                        pong_msg = {"method": "PONG"}
                        await ws.send_json(pong_msg)
                        self.stats["pongs_sent"] += 1
                    
                    # Other JSON messages
                    else:
                        self._route_message(data)
                        
                except json.JSONDecodeError:
                    self.stats["errors"] += 1
                    
            elif msg.type == aiohttp.WSMsgType.PING:
                await ws.pong()
                self.stats["pongs_sent"] += 1
                
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                print(f"⚠️ WebSocket closed/error: {msg}")
                break
    
    def _route_message(self, msg: Dict[str, Any]):
        """Route message to appropriate handler - mimics _channel_originating_message"""
        channel = msg.get("c", "") or msg.get("channel", "")
        
        if "depth" in channel.lower() or "aggre.depth" in channel.lower():
            self.process_depth_message(msg)
        elif "deals" in channel.lower() or "trade" in channel.lower():
            self.process_trade_message(msg)
        else:
            self.stats["unknown"] += 1
    
    def _print_status(self):
        """Print periodic status update"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        spread = None
        if best_bid and best_ask:
            spread = ((best_ask - best_bid) / best_ask) * 100
        
        # Calculate average update interval
        avg_interval = sum(self.update_times[-100:]) / len(self.update_times[-100:]) if self.update_times else 0
        
        print(f"\n[{time.strftime('%H:%M:%S')}] 📊 Status: {self.symbol}")
        print(f"    Bid: {best_bid:.2f} | Ask: {best_ask:.2f} | Spread: {spread:.4f}%" if spread else "    No prices yet")
        print(f"    Levels: {len(self.bids)} bids, {len(self.asks)} asks")
        print(f"    Messages: {self.stats['depth_messages']} depths, {self.stats['trade_messages']} trades")
        print(f"    Avg interval: {avg_interval:.1f}ms (target: 10ms)")
        if self.latencies:
            print(f"    Avg latency: {sum(self.latencies[-100:])/len(self.latencies[-100:]):.0f}ms")
    
    def _print_results(self, start_time: float):
        """Print final test results"""
        elapsed = time.time() - start_time
        total_messages = self.stats["depth_messages"] + self.stats["trade_messages"]
        msg_rate = total_messages / elapsed if elapsed > 0 else 0
        
        # Calculate statistics
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        min_latency = min(self.latencies) if self.latencies else 0
        max_latency = max(self.latencies) if self.latencies else 0
        
        avg_interval = sum(self.update_times) / len(self.update_times) if self.update_times else 0
        min_interval = min(self.update_times) if self.update_times else 0
        max_interval = max(self.update_times) if self.update_times else 0
        
        print("\n")
        print("=" * 70)
        print("TEST COMPLETE")
        print("=" * 70)
        
        print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                    MEXC ORDERBOOK TEST RESULTS                     ║
╠════════════════════════════════════════════════════════════════════╣
║  Duration:                {elapsed:5.1f} seconds                          ║
║  Total Messages:         {total_messages:6}                                ║
║  Message Rate:           {msg_rate:6.1f} msg/sec                           ║
╠════════════════════════════════════════════════════════════════════╣
║  📊 Depth Messages:      {self.stats['depth_messages']:6}                                ║
║  📈 Trade Messages:      {self.stats['trade_messages']:6}                                ║
║  🏓 Pings/Pongs:         {self.stats['pings_received']:3}/{self.stats['pongs_sent']:3}                                 ║
║  ✅ Sub Acks:            {self.stats['subscription_acks']:6}                                ║
║  ❌ Errors:              {self.stats['errors']:6}                                ║
╠════════════════════════════════════════════════════════════════════╣
║  📡 LATENCY (exchange → client):                                   ║
║      Average:            {avg_latency:6.0f}ms                               ║
║      Min:                {min_latency:6.0f}ms                               ║
║      Max:                {max_latency:6.0f}ms                               ║
╠════════════════════════════════════════════════════════════════════╣
║  ⏱️  UPDATE INTERVAL:                                               ║
║      Average:            {avg_interval:6.1f}ms (target: 10ms)               ║
║      Min:                {min_interval:6.1f}ms                              ║
║      Max:                {max_interval:6.1f}ms                              ║
╚════════════════════════════════════════════════════════════════════╝
""")
        
        # Final orderbook state
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        spread = ((best_ask - best_bid) / best_ask) * 100 if best_bid and best_ask else 0
        
        print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                    FINAL ORDERBOOK STATE                           ║
╠════════════════════════════════════════════════════════════════════╣
║  Symbol:           {self.symbol:20}                          ║
║  Bid Levels:       {len(self.bids):20}                          ║
║  Ask Levels:       {len(self.asks):20}                          ║
║  Best Bid:         {best_bid if best_bid else 'N/A':20}                          ║
║  Best Ask:         {best_ask if best_ask else 'N/A':20}                          ║
║  Spread:           {spread:18.6f}%                          ║
║  Last Update ID:   {self.last_update_id:20}                          ║
╚════════════════════════════════════════════════════════════════════╝
""")
        
        # Top levels
        if self.bids and self.asks:
            sorted_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:5]
            sorted_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:5]
            
            print("  TOP 5 LEVELS:")
            print("  " + "-" * 50)
            print("  BIDS                     | ASKS                    ")
            print("  " + "-" * 50)
            for i in range(5):
                bid_str = f"{float(sorted_bids[i][1]):>10.4f} @ {float(sorted_bids[i][0]):<10.2f}" if i < len(sorted_bids) else ""
                ask_str = f"{float(sorted_asks[i][0]):<10.2f} @ {float(sorted_asks[i][1]):<10.4f}" if i < len(sorted_asks) else ""
                print(f"  {bid_str:25} | {ask_str:25}")
        
        # Validation checks
        print("\n" + "=" * 70)
        print("VALIDATION CHECKS")
        print("=" * 70)
        
        checks_passed = 0
        checks_total = 0
        
        # Check 1: Received depth messages
        checks_total += 1
        if self.stats["depth_messages"] > 0:
            print(f"✅ PASS: Received {self.stats['depth_messages']} depth messages")
            checks_passed += 1
        else:
            print(f"❌ FAIL: No depth messages received")
        
        # Check 2: Update interval close to 10ms
        checks_total += 1
        if avg_interval > 0 and avg_interval < 50:  # Allow some slack
            print(f"✅ PASS: Average update interval {avg_interval:.1f}ms (target: 10ms)")
            checks_passed += 1
        elif avg_interval > 0:
            print(f"⚠️ WARN: Average update interval {avg_interval:.1f}ms (expected ~10ms)")
            checks_passed += 0.5
        else:
            print(f"❌ FAIL: Could not measure update interval")
        
        # Check 3: Orderbook populated
        checks_total += 1
        if len(self.bids) > 0 and len(self.asks) > 0:
            print(f"✅ PASS: Orderbook has {len(self.bids)} bids and {len(self.asks)} asks")
            checks_passed += 1
        else:
            print(f"❌ FAIL: Orderbook not populated")
        
        # Check 4: Reasonable latency
        checks_total += 1
        if avg_latency < 500:
            print(f"✅ PASS: Latency is reasonable ({avg_latency:.0f}ms avg)")
            checks_passed += 1
        else:
            print(f"⚠️ WARN: High latency ({avg_latency:.0f}ms avg)")
            checks_passed += 0.5
        
        # Check 5: No errors
        checks_total += 1
        if self.stats["errors"] == 0:
            print(f"✅ PASS: No errors")
            checks_passed += 1
        else:
            print(f"⚠️ WARN: {self.stats['errors']} errors occurred")
            checks_passed += 0.5
        
        print()
        if checks_passed >= checks_total * 0.8:
            print("🎉 ALL CRITICAL CHECKS PASSED!")
        elif checks_passed >= checks_total * 0.5:
            print("⚠️ SOME CHECKS PASSED WITH WARNINGS")
        else:
            print("❌ CRITICAL CHECKS FAILED")


async def main():
    # Parse command line args
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    
    tester = MexcOrderBookTester(symbol=symbol, duration=duration)
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
