#!/usr/bin/env python3
"""
Minimal HTX MBP Debug - Raw WebSocket Test
Focuses on identifying why connection goes silent
"""

import asyncio
import gzip
import json
import time
import websockets
from datetime import datetime


async def main():
    URL = "wss://api-aws.huobi.pro/feed"
    SYMBOL = "deloreanusdt"
    
    print("=" * 70)
    print("HTX MBP Raw WebSocket Debug")
    print("=" * 70)
    print(f"URL: {URL}")
    print(f"Symbol: {SYMBOL}")
    print("=" * 70)
    
    reconnect_count = 0
    total_messages = 0
    total_pings = 0
    
    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Connecting (attempt {reconnect_count + 1})...")
            
            async with websockets.connect(
                URL, 
                ping_interval=None,  # We handle HTX pings manually
                ping_timeout=None,
                close_timeout=5,
                max_size=10 * 1024 * 1024  # 10MB max message size
            ) as ws:
                
                # Subscribe
                await ws.send(json.dumps({
                    "sub": f"market.{SYMBOL}.mbp.5",
                    "id": "sub1"
                }))
                await ws.send(json.dumps({
                    "sub": f"market.{SYMBOL}.mbp.refresh.20", 
                    "id": "sub2"
                }))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribed")
                
                last_msg_time = time.time()
                msg_count = 0
                ping_count = 0
                
                while True:
                    try:
                        # Short timeout to detect silence quickly
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        now = time.time()
                        
                        # Decompress
                        try:
                            data = gzip.decompress(raw)
                            msg = json.loads(data)
                        except:
                            msg = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
                        
                        # Handle ping immediately
                        if "ping" in msg:
                            await ws.send(json.dumps({"pong": msg["ping"]}))
                            ping_count += 1
                            total_pings += 1
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🏓 PING #{ping_count} - responded with pong")
                            last_msg_time = now
                            continue
                        
                        # Handle ACK
                        if "subbed" in msg:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ ACK: {msg.get('subbed')}")
                            last_msg_time = now
                            continue
                        
                        # Handle error
                        if "err-code" in msg or "status" in msg and msg.get("status") == "error":
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ ERROR: {msg}")
                            continue
                        
                        # Data message
                        ch = msg.get("ch", "")
                        tick = msg.get("tick", {})
                        seq = tick.get("seqNum", 0)
                        bids = tick.get("bids", [])
                        asks = tick.get("asks", [])
                        
                        msg_count += 1
                        total_messages += 1
                        
                        msg_type = "REFRESH" if ".refresh." in ch else "INCR"
                        interval = (now - last_msg_time) * 1000
                        
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] #{msg_count:3d} {msg_type:7s} seq={seq} bids={len(bids)} asks={len(asks)} interval={interval:.0f}ms")
                        
                        last_msg_time = now
                        
                    except asyncio.TimeoutError:
                        # Check if we've been silent too long
                        silent_time = time.time() - last_msg_time
                        if silent_time > 10:
                            print(f"\n⚠️ [{datetime.now().strftime('%H:%M:%S')}] SILENT for {silent_time:.1f}s - connection dead?")
                            print(f"   This connection: {msg_count} msgs, {ping_count} pings")
                            print(f"   Total: {total_messages} msgs, {total_pings} pings, {reconnect_count} reconnects")
                            break
                        continue
                        
        except websockets.ConnectionClosed as e:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Connection closed: {e}")
            
        except Exception as e:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
            import traceback
            traceback.print_exc()
        
        reconnect_count += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Waiting 2s before reconnect...")
        await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
