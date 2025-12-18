#!/usr/bin/env python3
"""
Bitmart Multi-Channel Comparison - Compare update speeds across depth channels
Subscribe to both depth50 and depth/increase100 for BTC_USDT and compare:
1. Update frequency
2. Latency between updates
3. Message types
"""
import asyncio
import gzip
import io
import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import aiohttp


# =============================================================================
# CONFIGURATION
# =============================================================================
BITMART_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
RUN_DURATION_SECONDS = 180  # 3 minutes
SYMBOL = "BTC_USDT"

# Channels to compare - include multiple depth/increase variants
CHANNELS = [
    "spot/depth50",            # 50 levels (full snapshots, ~300ms)
    "spot/depth/increase5",    # 5 levels (incremental)
    "spot/depth/increase20",   # 20 levels (incremental)
    "spot/depth/increase50",   # 50 levels (incremental)
    "spot/depth/increase100",  # 100 levels (incremental, ~100ms)
]


def decompress_ws_message(data):
    try:
        if isinstance(data, bytes):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
                    return gz.read().decode('utf-8')
            except Exception:
                return data.decode('utf-8')
        return data
    except Exception:
        return None


@dataclass
class ChannelStats:
    channel: str
    messages: int = 0
    snapshots: int = 0
    diffs: int = 0
    heartbeats: int = 0
    last_update_time: float = 0
    intervals: List[float] = field(default_factory=list)
    first_msg_time: float = 0


async def run_comparison():
    print("=" * 100)
    print(f"BITMART CHANNEL COMPARISON - {SYMBOL}")
    print(f"Channels: {CHANNELS}")
    print(f"Duration: {RUN_DURATION_SECONDS}s")
    print("=" * 100)
    
    stats: Dict[str, ChannelStats] = {ch: ChannelStats(channel=ch) for ch in CHANNELS}
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(BITMART_WS_URL) as ws:
            # Subscribe to all channels for the symbol
            topics = [f"{ch}:{SYMBOL}" for ch in CHANNELS]
            subscribe_msg = {"op": "subscribe", "args": topics}
            await ws.send_json(subscribe_msg)
            print(f"\n[{time.strftime('%H:%M:%S')}] Subscribed to {len(topics)} channels")
            for t in topics:
                print(f"  - {t}")
            
            # Ping task
            async def ping_task():
                while True:
                    await asyncio.sleep(10)
                    try:
                        await ws.send_str("ping")
                    except Exception:
                        break
            
            ping_handle = asyncio.create_task(ping_task())
            last_report = start_time
            
            try:
                async for msg in ws:
                    now = time.time()
                    elapsed = now - start_time
                    
                    if elapsed >= RUN_DURATION_SECONDS:
                        break
                    
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        text = decompress_ws_message(msg.data)
                        if text is None:
                            continue
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        text = msg.data
                    else:
                        continue
                    
                    if text and text.strip().lower() == "pong":
                        continue
                    
                    try:
                        data = json.loads(text)
                    except Exception:
                        continue
                    
                    # Skip acks
                    if "event" in data:
                        continue
                    
                    table = data.get("table")
                    if table not in stats:
                        continue
                    
                    s = stats[table]
                    s.messages += 1
                    
                    for item in data.get("data", []):
                        bids = item.get("bids", [])
                        asks = item.get("asks", [])
                        msg_type = item.get("type", "").lower()
                        
                        is_heartbeat = (not bids) and (not asks)
                        
                        if is_heartbeat:
                            s.heartbeats += 1
                        elif msg_type == "snapshot":
                            s.snapshots += 1
                        else:
                            s.diffs += 1
                        
                        # Track intervals
                        if s.first_msg_time == 0:
                            s.first_msg_time = now
                            print(f"[{time.strftime('%H:%M:%S')}] 📸 {table}: first msg (type={msg_type}, bids={len(bids)}, asks={len(asks)})")
                        
                        if s.last_update_time > 0 and not is_heartbeat:
                            interval = (now - s.last_update_time) * 1000
                            s.intervals.append(interval)
                        
                        if not is_heartbeat:
                            s.last_update_time = now
                    
                    # Print progress every 15s
                    if now - last_report >= 15:
                        last_report = now
                        print(f"\n[{time.strftime('%H:%M:%S')}] Progress ({int(elapsed)}s):")
                        for ch in CHANNELS:
                            cs = stats[ch]
                            total = cs.snapshots + cs.diffs
                            avg = sum(cs.intervals) / len(cs.intervals) if cs.intervals else 0
                            print(f"  {ch:25}: {total:4} updates, {cs.heartbeats:3} HB, avg={avg:.0f}ms")
            
            except asyncio.CancelledError:
                pass
            finally:
                ping_handle.cancel()
    
    # Final report
    print(f"\n{'#'*100}")
    print(f"FINAL COMPARISON - {int(time.time() - start_time)}s runtime")
    print(f"{'#'*100}")
    print(f"\n{'Channel':25} | {'Snaps':>6} | {'Diffs':>6} | {'HBeat':>5} | {'AvgInt':>8} | {'MinInt':>7} | {'MaxInt':>7}")
    print("-" * 100)
    
    for ch in CHANNELS:
        s = stats[ch]
        if s.intervals:
            avg = sum(s.intervals) / len(s.intervals)
            mn = min(s.intervals)
            mx = max(s.intervals)
        else:
            avg = mn = mx = 0
        print(f"{ch:25} | {s.snapshots:>6} | {s.diffs:>6} | {s.heartbeats:>5} | {avg:>7.0f}ms | {mn:>6.0f}ms | {mx:>6.0f}ms")
    
    print(f"\n{'#'*100}")
    
    # Analysis
    print("\nANALYSIS:")
    for ch in CHANNELS:
        s = stats[ch]
        total = s.snapshots + s.diffs
        if s.intervals:
            avg = sum(s.intervals) / len(s.intervals)
            print(f"  {ch}: {total} data updates, avg interval {avg:.0f}ms")
            if avg < 150:
                print(f"    ✅ ~100ms speed detected")
            elif avg < 350:
                print(f"    ⚠️ ~300ms speed")
            else:
                print(f"    ❌ Slower than expected")
        else:
            print(f"  {ch}: {total} updates, no interval data")


async def main():
    try:
        await run_comparison()
    except KeyboardInterrupt:
        print("\n⛔ Stopped")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
