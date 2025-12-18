#!/usr/bin/env python3
"""
Bybit Order Book Debug Script
Subscribes to SAHARA-USDT and logs all order book messages to analyze sequence behavior.
"""
import asyncio
import json
import time
import aiohttp

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
SYMBOL = "SAHARUSDT"  # Bybit format (no hyphen)
DEPTH = 50


async def debug_bybit_orderbook():
    """Subscribe to Bybit orderbook and log all messages with sequence analysis."""
    
    last_snapshot_u = None
    last_diff_u = None
    messages_since_snapshot = 0
    rejected_count = 0
    total_diffs = 0
    
    print(f"[{time.strftime('%H:%M:%S')}] Connecting to Bybit WS: {BYBIT_WS_URL}")
    print(f"[{time.strftime('%H:%M:%S')}] Subscribing to orderbook.{DEPTH}.{SYMBOL}")
    print("-" * 80)
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(BYBIT_WS_URL) as ws:
            # Subscribe to orderbook
            subscribe_msg = {
                "op": "subscribe",
                "args": [f"orderbook.{DEPTH}.{SYMBOL}"]
            }
            await ws.send_json(subscribe_msg)
            print(f"[{time.strftime('%H:%M:%S')}] Sent subscribe: {subscribe_msg}")
            
            # Start ping task
            async def ping_loop():
                while True:
                    await asyncio.sleep(20)
                    try:
                        await ws.send_json({"op": "ping"})
                    except Exception:
                        break
            ping_task = asyncio.create_task(ping_loop())
            
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        # Handle ping/pong
                        if data.get("op") in ("ping", "pong"):
                            if data.get("ret_msg") == "pong":
                                continue
                            continue
                        
                        # Handle subscription response
                        if data.get("success") is True or data.get("op") == "subscribe":
                            print(f"[{time.strftime('%H:%M:%S')}] Subscribe ACK: {data}")
                            continue
                        
                        # Handle orderbook messages
                        event_type = data.get("type")
                        topic = data.get("topic", "")
                        
                        if "orderbook" in topic:
                            payload = data.get("data", {})
                            u = payload.get("u")  # Update ID / sequence
                            seq = payload.get("seq")  # Some exchanges also have seq
                            symbol = payload.get("s")
                            bids = payload.get("b", [])
                            asks = payload.get("a", [])
                            ts = data.get("ts", 0)
                            cts = data.get("cts", 0)  # Engine time
                            
                            if event_type == "snapshot":
                                last_snapshot_u = u
                                messages_since_snapshot = 0
                                print(f"\n{'='*80}")
                                print(f"[{time.strftime('%H:%M:%S')}] 📸 SNAPSHOT received:")
                                print(f"  symbol: {symbol}")
                                print(f"  u (update_id): {u}")
                                print(f"  seq: {seq}")
                                print(f"  ts: {ts}, cts: {cts}")
                                print(f"  bids: {len(bids)} levels, asks: {len(asks)} levels")
                                if bids:
                                    print(f"  best_bid: {bids[0]}")
                                if asks:
                                    print(f"  best_ask: {asks[0]}")
                                print(f"{'='*80}\n")
                                
                            elif event_type == "delta":
                                total_diffs += 1
                                messages_since_snapshot += 1
                                
                                # Check if this diff would be rejected
                                would_reject = False
                                if last_snapshot_u is not None and u is not None:
                                    if last_snapshot_u > u:
                                        would_reject = True
                                        rejected_count += 1
                                
                                # Check sequence gap
                                gap = None
                                if last_diff_u is not None and u is not None:
                                    gap = u - last_diff_u
                                    if gap != 1 and gap > 0:
                                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️  SEQUENCE GAP: last_u={last_diff_u}, new_u={u}, gap={gap}")
                                
                                last_diff_u = u
                                
                                # Log diff details (every 10th or if would be rejected)
                                if would_reject or messages_since_snapshot <= 3 or messages_since_snapshot % 50 == 0:
                                    status = "❌ WOULD_REJECT" if would_reject else "✅"
                                    print(f"[{time.strftime('%H:%M:%S')}] {status} DELTA #{messages_since_snapshot}: u={u}, snapshot_u={last_snapshot_u}, bids={len(bids)}, asks={len(asks)}")
                                
                                # Periodic stats
                                if total_diffs % 100 == 0:
                                    print(f"[{time.strftime('%H:%M:%S')}] 📊 STATS: total_diffs={total_diffs}, rejected={rejected_count}, rejection_rate={rejected_count/total_diffs*100:.1f}%")
                            
                            else:
                                print(f"[{time.strftime('%H:%M:%S')}] Unknown event_type: {event_type}, data: {data}")
                        
                        else:
                            # Log other messages
                            print(f"[{time.strftime('%H:%M:%S')}] Other msg: {data}")
                    
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"[{time.strftime('%H:%M:%S')}] WS Error: {msg}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print(f"[{time.strftime('%H:%M:%S')}] WS Closed")
                        break
            
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass


async def main():
    print("=" * 80)
    print("Bybit Order Book Debug Script")
    print(f"Symbol: {SYMBOL}")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    try:
        await debug_bybit_orderbook()
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
