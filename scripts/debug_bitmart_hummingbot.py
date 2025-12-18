#!/usr/bin/env python3
"""
BitMart Debug - Exact mimic of Hummingbot's WSConnection behavior using aiohttp
"""
import asyncio
import json
import time
import gzip
import zlib
import aiohttp

# Constants from Hummingbot BitMart implementation
WSS_PUBLIC_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
DEPTH_CHANNEL = "spot/depth/increase100"
SYMBOL = "BTC_USDT"

# Liveness constants from BitmartAPIOrderBookDataSource
PING_INTERVAL_SECONDS = 15.0
MESSAGE_TIMEOUT = 60  # message_timeout=60 in _connected_websocket_assistant


def decompress(data):
    """Mirrors bitmart_utils.decompress_ws_message"""
    if isinstance(data, str):
        return data
    if not isinstance(data, (bytes, bytearray)):
        return data
    try:
        return gzip.decompress(data).decode('utf-8')
    except:
        pass
    try:
        d = zlib.decompressobj(-zlib.MAX_WBITS)
        return (d.decompress(data) + d.flush()).decode('utf-8')
    except:
        pass
    try:
        return zlib.decompress(data).decode('utf-8')
    except:
        pass
    try:
        return data.decode('utf-8')
    except:
        return None



async def main():
    print("=" * 80)
    print("BITMART DEBUG - Mimicking Hummingbot WSConnection")
    print(f"URL: {WSS_PUBLIC_URL}")
    print(f"Symbol: {SYMBOL}")
    print(f"aiohttp params: autoping=False, heartbeat=None (ping_timeout=None)")
    print(f"message_timeout: {MESSAGE_TIMEOUT}s")
    print("=" * 80)
    
    start_time = time.time()
    last_recv_time = time.time()
    last_ping_sent_time = 0.0
    msg_count = 0
    ping_count = 0
    pong_count = 0
    
    async with aiohttp.ClientSession() as session:
        # EXACT parameters from WSConnection.connect() when ping_timeout=None
        async with session.ws_connect(
            WSS_PUBLIC_URL,
            headers={"Accept-Encoding": "gzip"},
            autoping=False,  # From WSConnection.connect()
            heartbeat=None,  # heartbeat=ping_timeout, and ping_timeout=None
        ) as ws:
            print(f"\n[{time.strftime('%H:%M:%S')}] Connected!")
            
            # Subscribe (mirrors _subscribe_channels)
            payload = {"op": "subscribe", "args": [f"{DEPTH_CHANNEL}:{SYMBOL}"]}
            await ws.send_json(payload)
            print(f"[{time.strftime('%H:%M:%S')}] Sent: {payload}")
            print("-" * 80)
            
            # Consumer coroutine (mirrors _process_ws_messages_consumer)
            async def message_consumer():
                nonlocal msg_count, pong_count, last_recv_time
                while True:
                    msg = await ws.receive()  # No timeout - consumer blocks
                    last_recv_time = time.time()
                    
                    # Process message types
                    if msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong(msg.data)
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 Received protocol PING, sent PONG")
                        continue
                    elif msg.type == aiohttp.WSMsgType.PONG:
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 Received protocol PONG")
                        continue
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        raise ConnectionError(f"WS closed: {ws.close_code} - {msg.data}")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise ConnectionError(f"WS error: {msg.data}")
                    
                    # Build response
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        text = decompress(msg.data)
                    else:
                        text = msg.data
                    
                    if text is None:
                        continue
                    
                    msg_count += 1
                    
                    # Handle text pong
                    if isinstance(text, str) and text.strip().lower() == "pong":
                        pong_count += 1
                        print(f"[{time.strftime('%H:%M:%S')}] 🏓 PONG #{pong_count}!")
                        continue
                    
                    # Parse and log
                    try:
                        data = json.loads(text) if isinstance(text, str) else text
                    except:
                        print(f"[{time.strftime('%H:%M:%S')}] MSG #{msg_count}: {str(text)[:60]}...")
                        continue
                    
                    if msg_count <= 20 or msg_count % 100 == 0:
                        if "event" in data:
                            print(f"[{time.strftime('%H:%M:%S')}] ✅ {data.get('event')}: {data.get('topic')}")
                        elif "data" in data:
                            items = data.get("data", [])
                            if items:
                                i = items[0]
                                print(f"[{time.strftime('%H:%M:%S')}] 📊 #{msg_count} {i.get('type','?')} bids={len(i.get('bids',[]))} asks={len(i.get('asks',[]))}")
                        else:
                            print(f"[{time.strftime('%H:%M:%S')}] MSG #{msg_count}: {str(data)[:60]}...")
            
            # Create consumer task
            consumer_task = asyncio.create_task(message_consumer())
            
            # Main loop - mirrors listen_for_subscriptions with asyncio.wait
            try:
                while True:
                    now = time.time()
                    
                    # Watchdog timer (5s like Hummingbot)
                    watchdog = asyncio.create_task(asyncio.sleep(5.0))
                    
                    done, _ = await asyncio.wait(
                        {consumer_task, watchdog},
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # If consumer finished, check for error
                    if consumer_task in done:
                        watchdog.cancel()
                        exc = consumer_task.exception()
                        if exc:
                            raise exc
                        raise ConnectionError("Consumer ended unexpectedly")
                    
                    # Watchdog expired - run liveness checks
                    if watchdog in done:
                        now = time.time()
                        idle = now - last_recv_time
                        
                        # Send ping if idle >= 15s
                        if idle >= PING_INTERVAL_SECONDS and (now - last_ping_sent_time) >= PING_INTERVAL_SECONDS:
                            await ws.send_str("ping")
                            ping_count += 1
                            last_ping_sent_time = now
                            print(f"[{time.strftime('%H:%M:%S')}] 🏓 Sent ping #{ping_count} (idle={idle:.1f}s)")
                        
                        # Log if idle
                        if idle >= 30:
                            print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Idle for {idle:.1f}s")
                        
                        # Stop after 2 minutes
                        if now - start_time >= 120:
                            break
            finally:
                consumer_task.cancel()
                try:
                    await consumer_task
                except:
                    pass
            
            print("-" * 80)
            print(f"Messages: {msg_count}, Pings: {ping_count}, Pongs: {pong_count}")


if __name__ == "__main__":
    asyncio.run(main())
