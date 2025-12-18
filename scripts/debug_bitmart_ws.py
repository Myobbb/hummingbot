#!/usr/bin/env python3
"""
Bitmart WebSocket Debug Script - Using websockets library
Simple script to test BitMart WebSocket connection stability.
"""
import asyncio
import gzip
import json
import time
import zlib
import websockets
from websockets.exceptions import ConnectionClosed

# Configuration
BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
DEPTH_CHANNEL = "spot/depth/increase100"
RUN_DURATION_SECONDS = 5 * 60  # 5 minutes
PING_INTERVAL_SECONDS = 15.0
FORCE_RECONNECT_IDLE_SECONDS = 30.0

# Trading pairs
TRADING_PAIRS = [
    "BTC-USDT",
    "PHL-USDT",
    "MINDFAK-USDT",
    "FREE-USDT",
    "LOBO-USDT",
    "LAB-USDT",
]


def to_bitmart_symbol(pair: str) -> str:
    return pair.replace("-", "_")


def decompress_ws_message(data):
    """Decompress Bitmart WS messages (gzip or zlib/deflate)"""
    if isinstance(data, str):
        return data
    if not isinstance(data, (bytes, bytearray)):
        return data
    try:
        return gzip.decompress(data).decode('utf-8')
    except Exception:
        pass
    try:
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('utf-8')
    except Exception:
        pass
    try:
        return zlib.decompress(data).decode('utf-8')
    except Exception:
        pass
    try:
        return bytes(data).decode('utf-8')
    except Exception:
        return None


async def run_debug():
    print("=" * 100)
    print("BITMART WEBSOCKET DEBUG - Using websockets library")
    print(f"Testing {len(TRADING_PAIRS)} pairs for {RUN_DURATION_SECONDS // 60} minutes")
    print(f"Channel: {DEPTH_CHANNEL}")
    print("=" * 100)
    
    symbols = [to_bitmart_symbol(p) for p in TRADING_PAIRS]
    start_time = time.time()
    reconnect_count = 0
    
    # Stats
    total_snapshots = 0
    total_diffs = 0
    total_pongs = 0
    
    while time.time() - start_time < RUN_DURATION_SECONDS:
        reconnect_count += 1
        connection_start = time.time()
        last_recv_time = time.time()
        last_ping_sent_time = 0.0
        ping_count = 0
        pong_count = 0
        msg_count = 0
        
        print(f"\n[{time.strftime('%H:%M:%S')}] --- CONNECTING (attempt #{reconnect_count}) ---")
        
        try:
            async with websockets.connect(
                BITMART_WS_URL,
                ping_interval=None,  # Disable protocol ping, we use text ping
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                # Subscribe
                topics = [f"{DEPTH_CHANNEL}:{s}" for s in symbols]
                for i in range(0, len(topics), 5):
                    chunk = topics[i:i+5]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))
                    print(f"[{time.strftime('%H:%M:%S')}] Subscribed: {chunk}")
                    await asyncio.sleep(0.12)
                
                print(f"\n[{time.strftime('%H:%M:%S')}] Listening for messages...\n")
                
                    # Message loop
                while True:
                    now = time.time()
                    elapsed = now - start_time
                    
                    if elapsed >= RUN_DURATION_SECONDS:
                        break
                    
                    # Check idle - just LOG, don't force disconnect (like Hummingbot's iter_messages)
                    idle = now - last_recv_time
                    if idle >= FORCE_RECONNECT_IDLE_SECONDS and int(idle) % 30 == 0:
                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Idle for {idle:.0f}s, still waiting...")
                    
                    # Send ping if needed
                    if idle >= PING_INTERVAL_SECONDS and (now - last_ping_sent_time) >= PING_INTERVAL_SECONDS:
                        await ws.send("ping")
                        ping_count += 1
                        last_ping_sent_time = now
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 Sent ping #{ping_count}")
                    
                    # Receive with timeout
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    
                    last_recv_time = time.time()
                    msg_count += 1
                    
                    # Decompress if needed
                    text = decompress_ws_message(raw)
                    if text is None:
                        continue
                    
                    # Handle pong
                    if text.strip().lower() == "pong":
                        pong_count += 1
                        total_pongs += 1
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 Received pong #{pong_count}")
                        continue
                    
                    # Parse JSON
                    try:
                        data = json.loads(text)
                    except Exception:
                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Non-JSON: {text[:50]}...")
                        continue
                    
                    # Handle events
                    if "event" in data:
                        event = data.get("event")
                        if event == "subscribe":
                            topic = data.get("topic", "unknown")
                            print(f"[{time.strftime('%H:%M:%S')}] ✅ Subscribed: {topic}")
                        elif "errorCode" in data:
                            print(f"[{time.strftime('%H:%M:%S')}] ❌ Error: {data}")
                        continue
                    
                    # Handle data
                    if "data" in data and "table" in data:
                        for item in data.get("data", []):
                            symbol = item.get("symbol", "?")
                            msg_type = item.get("type", "?").lower()
                            bids = len(item.get("bids", []))
                            asks = len(item.get("asks", []))
                            version = item.get("version", "?")
                            
                            if msg_type == "snapshot":
                                total_snapshots += 1
                                print(f"[{time.strftime('%H:%M:%S')}] 📸 {symbol} SNAPSHOT v={version} bids={bids} asks={asks}")
                            elif bids > 0 or asks > 0:
                                total_diffs += 1
                                if total_diffs <= 10 or total_diffs % 100 == 0:
                                    print(f"[{time.strftime('%H:%M:%S')}] 📊 {symbol} DIFF v={version} bids={bids} asks={asks} (total diffs: {total_diffs})")
                
                conn_duration = time.time() - connection_start
                print(f"[{time.strftime('%H:%M:%S')}] 🔌 Connection ended after {conn_duration:.1f}s, msgs={msg_count}, pings={ping_count}, pongs={pong_count}")
        
        except ConnectionClosed as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Connection closed: {e}")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Error: {e}")
        
        # Reconnect delay
        if time.time() - start_time < RUN_DURATION_SECONDS:
            print(f"[{time.strftime('%H:%M:%S')}] Reconnecting in 3s...")
            await asyncio.sleep(3)
    
    # Final summary
    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)
    print(f"Total reconnects: {reconnect_count}")
    print(f"Total snapshots: {total_snapshots}")
    print(f"Total diffs: {total_diffs}")
    print(f"Total pongs: {total_pongs}")
    print("=" * 100)


if __name__ == "__main__":
    print("\n" + "=" * 100)
    print("Starting Bitmart WebSocket Debug (websockets library)")
    print("Press Ctrl+C to stop early")
    print("=" * 100 + "\n")
    
    try:
        asyncio.run(run_debug())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
