#!/usr/bin/env python3
"""
Minimal BitMart WebSocket Test - NO ping/pong, just raw listen
Testing if network cuts connection when idle
"""
import asyncio
import json
import time
import websockets

BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
TRADING_PAIR = "BTC_USDT"
DEPTH_CHANNEL = "spot/depth/increase100"


async def main():
    print("=" * 80)
    print("BITMART RAW LISTEN TEST - NO PING/PONG")
    print(f"URL: {BITMART_WS_URL}")
    print(f"Pair: {TRADING_PAIR}")
    print("Testing if connection dies without any ping/pong intervention")
    print("=" * 80)
    
    try:
        # COMPLETELY disable all ping/pong
        async with websockets.connect(
            BITMART_WS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            print(f"\n[{time.strftime('%H:%M:%S')}] Connected!")
            
            # Subscribe
            await ws.send(json.dumps({"op": "subscribe", "args": [f"{DEPTH_CHANNEL}:{TRADING_PAIR}"]}))
            print(f"[{time.strftime('%H:%M:%S')}] Subscribed to {TRADING_PAIR}")
            print("-" * 80)
            
            last_msg_time = time.time()
            msg_count = 0
            
            while time.time() - last_msg_time < 120:  # Run up to 2 mins
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    msg_count += 1
                    last_msg_time = time.time()
                    
                    if isinstance(raw, str):
                        if msg_count <= 20 or msg_count % 100 == 0:
                            print(f"[{time.strftime('%H:%M:%S')}] MSG #{msg_count}: {raw[:80]}...")
                    else:
                        print(f"[{time.strftime('%H:%M:%S')}] BINARY MSG #{msg_count}: {len(raw)} bytes")
                        
                except asyncio.TimeoutError:
                    elapsed = time.time() - last_msg_time
                    print(f"[{time.strftime('%H:%M:%S')}] ... no message for {elapsed:.0f}s (total: {msg_count})")
                    continue
            
            print("-" * 80)
            print(f"[{time.strftime('%H:%M:%S')}] Finished. Total messages: {msg_count}")
            
    except Exception as e:
        print(f"\n[{time.strftime('%H:%M:%S')}] ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
