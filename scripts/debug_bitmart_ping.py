#!/usr/bin/env python3
"""
Minimal BitMart WebSocket Ping/Pong Test
Tests EXACTLY what Hummingbot does: plain text "ping" keepalive
"""
import asyncio
import json
import time
import websockets

# Configuration - EXACTLY matching Hummingbot
BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
TRADING_PAIR = "BTC_USDT"  # Single high-volume pair
DEPTH_CHANNEL = "spot/depth/increase100"
PING_PAYLOAD = "ping"  # Exact ping as per BitMart docs and Hummingbot: plain text "ping"


async def main():
    print("=" * 80)
    print("BITMART MINIMAL PING/PONG TEST")
    print(f"URL: {BITMART_WS_URL}")
    print(f"Pair: {TRADING_PAIR}")
    print(f"Ping payload: '{PING_PAYLOAD}'")
    print("=" * 80)
    
    start_time = time.time()
    
    try:
        # Enable automatic protocol ping/pong responses (websockets handles server pings)
        # This matches aiohttp's behavior when autoping=False but connection still responds to server pings
        async with websockets.connect(
            BITMART_WS_URL,
            ping_interval=20,  # Send protocol ping every 20s as keepalive
            ping_timeout=20,   # Wait 20s for pong response
            close_timeout=5,
        ) as ws:
            print(f"\n[{time.strftime('%H:%M:%S')}] Connected!")
            
            # Subscribe to ONE pair only
            sub_msg = {"op": "subscribe", "args": [f"{DEPTH_CHANNEL}:{TRADING_PAIR}"]}
            await ws.send(json.dumps(sub_msg))
            print(f"[{time.strftime('%H:%M:%S')}] Sent subscribe: {sub_msg}")
            
            last_recv_time = time.time()
            ping_count = 0
            pong_count = 0
            msg_count = 0
            
            print(f"[{time.strftime('%H:%M:%S')}] Waiting for messages (will send ping every 10s)...")
            print("-" * 80)
            
            while time.time() - start_time < 120:  # 2 minutes
                now = time.time()
                idle = now - last_recv_time
                
                # Send ping every 10s (more aggressive for testing)
                if idle >= 10.0:
                    await ws.send(PING_PAYLOAD)
                    ping_count += 1
                    print(f"[{time.strftime('%H:%M:%S')}] → Sent ping #{ping_count} (idle={idle:.1f}s)")
                    last_recv_time = now  # Reset to avoid spam
                
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                last_recv_time = time.time()
                msg_count += 1
                
                # Handle binary (potentially compressed)
                if isinstance(raw, bytes):
                    # Try to decompress
                    import gzip, zlib
                    text = None
                    try:
                        text = gzip.decompress(raw).decode('utf-8')
                    except:
                        pass
                    if text is None:
                        try:
                            decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                            text = (decompress.decompress(raw) + decompress.flush()).decode('utf-8')
                        except:
                            pass
                    if text is None:
                        try:
                            text = zlib.decompress(raw).decode('utf-8')
                        except:
                            pass
                    if text is None:
                        try:
                            text = raw.decode('utf-8')
                        except:
                            text = f"<binary {len(raw)} bytes>"
                    print(f"[{time.strftime('%H:%M:%S')}] ← BINARY: {text[:80]}...")
                else:
                    text = raw
                
                # Check for pong
                if text.strip().lower() == "pong":
                    pong_count += 1
                    print(f"[{time.strftime('%H:%M:%S')}] ← PONG #{pong_count}! ✓")
                    continue
                
                # Show message (abbreviated for data)
                if len(text) > 100:
                    preview = text[:100] + "..."
                else:
                    preview = text
                
                print(f"[{time.strftime('%H:%M:%S')}] ← MSG #{msg_count}: {preview}")
            
            print("-" * 80)
            print(f"\n[{time.strftime('%H:%M:%S')}] Test complete")
            print(f"Total messages: {msg_count}")
            print(f"Pings sent: {ping_count}")
            print(f"Pongs received: {pong_count}")
            
    except Exception as e:
        print(f"\n[{time.strftime('%H:%M:%S')}] ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
