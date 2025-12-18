#!/usr/bin/env python3
"""
BitMart Orderbook Debug Script - Hummingbot Pattern Matching
Comprehensive debugging that accurately mimics Hummingbot's BitmartAPIOrderBookDataSource.
Tracks version sequencing, heartbeats, per-side staleness, and recovery triggers.
"""
import asyncio
import gzip
import json
import time
import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp
from aiohttp_socks import ProxyConnector

# BitMart WebSocket configuration (from bitmart_constants.py)
WSS_PUBLIC_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
DEPTH_CHANNEL = "spot/depth/increase100"  # Incremental depth channel with 100 levels

# Hummingbot thresholds (from BitmartAPIOrderBookDataSource)
# Relaxed for low-volume pairs - focus on version sequencing
DEPTH_STALENESS_SECONDS = 120.0  # Relaxed - low-volume pairs can be stale
FORCE_RECONNECT_IDLE_SECONDS = 60.0  # Connection-level idle threshold
PING_INTERVAL_SECONDS = 15.0  # Ping interval
PER_SIDE_STALENESS_THRESHOLD = 120.0  # Only warn if one side is 2+ minutes stale

# Run configuration
RUN_DURATION_SECONDS = 10 * 60  # 10 minutes
REPORT_INTERVAL_SECONDS = 30

# Symbols to monitor (converted to BitMart format: PAIR_QUOTE)
TRADING_PAIRS = [
    "PHL-USDT",
    "MINDFAK-USDT",
    "FREE-USDT",
    "LOBO-USDT",
    "LAB-USDT",
    "SAHARA-USDT",
    "JYAI-USDT",
    "JELLYJELLY-USDT",
    "EDEN-USDT",
]


def to_bitmart_symbol(pair: str) -> str:
    """Convert PAIR-QUOTE to PAIR_QUOTE format"""
    return pair.replace("-", "_")


def decompress(data) -> Optional[str]:
    """Decompress BitMart binary message (mirrors bitmart_utils.decompress_ws_message)"""
    if isinstance(data, str):
        return data
    if not isinstance(data, (bytes, bytearray)):
        return None
    # Try gzip first
    try:
        return gzip.decompress(data).decode('utf-8')
    except:
        pass
    # Try zlib with various window bits
    for wbits in [-zlib.MAX_WBITS, zlib.MAX_WBITS, 0]:
        try:
            d = zlib.decompressobj(wbits)
            return (d.decompress(data) + d.flush()).decode('utf-8')
        except:
            pass
    # Try raw decode
    try:
        return data.decode('utf-8')
    except:
        return None


@dataclass
class SymbolStats:
    """Stats for a single symbol - mirrors Hummingbot's tracking patterns"""
    symbol: str
    trading_pair: str
    
    # Subscription state
    subscribed: bool = False
    
    # Message counts
    snapshot_count: int = 0
    delta_count: int = 0
    heartbeat_count: int = 0  # Empty bids+asks messages
    
    # Version tracking (mirrors _last_depth_version)
    last_version: Optional[int] = None
    first_version: Optional[int] = None
    
    # Version issues
    version_gaps: List[tuple] = field(default_factory=list)  # (from_v, to_v, gap_size)
    duplicate_count: int = 0  # new_ver <= last_ver
    out_of_order_count: int = 0  # new_ver < last_ver
    
    # Timestamps - dual tracking like Hummingbot
    first_snapshot_time: Optional[float] = None
    last_any_message_time: float = 0  # ANY message including heartbeats
    last_data_update_time: float = 0  # Only non-heartbeat updates
    last_bids_update_time: float = 0  # Per-side tracking
    last_asks_update_time: float = 0  # Per-side tracking
    
    # Staleness events (would have triggered recovery in Hummingbot)
    staleness_events: int = 0
    max_staleness_seconds: float = 0
    
    # Update pattern tracking
    bids_only_updates: int = 0
    asks_only_updates: int = 0
    two_sided_updates: int = 0
    
    # Orderbook depth
    last_bids_count: int = 0
    last_asks_count: int = 0


async def debug_bitmart_orderbooks():
    """Main debug loop - accurately mimics Hummingbot's BitMart handling"""
    symbols = [to_bitmart_symbol(p) for p in TRADING_PAIRS]
    stats: Dict[str, SymbolStats] = {}
    for sym, pair in zip(symbols, TRADING_PAIRS):
        stats[sym] = SymbolStats(symbol=sym, trading_pair=pair)
    
    issues: List[str] = []
    
    print("=" * 110)
    print("BITMART ORDERBOOK DEBUG - Hummingbot Pattern Matching")
    print(f"Monitoring {len(symbols)} symbols for {RUN_DURATION_SECONDS // 60} minutes")
    print(f"Channel: {DEPTH_CHANNEL}")
    print(f"Staleness threshold: {DEPTH_STALENESS_SECONDS}s (matches Hummingbot)")
    print("=" * 110)
    print(f"\nSymbols: {', '.join(symbols)}\n")
    print("-" * 110)
    
    # Use SOCKS5 proxy on port 1080 for proper WebSocket tunnel support
    connector = ProxyConnector.from_url('socks5://127.0.0.1:1080')
    
    start_time = time.time()
    last_report_time = start_time
    last_ping_time = start_time
    last_watchdog_check = start_time
    ping_count = 0
    pong_count = 0
    
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.ws_connect(
            WSS_PUBLIC_URL,
            headers={"Accept-Encoding": "gzip"},
            autoping=False,
            heartbeat=None,
        ) as ws:
            print(f"[{time.strftime('%H:%M:%S')}] Connected to BitMart WebSocket\n")
            
            # Subscribe to all symbols (mirrors _subscribe_channels)
            topics = [f"{DEPTH_CHANNEL}:{sym}" for sym in symbols]
            CHUNK_SIZE = 20  # BitMart allows up to 20 topics per subscription
            for i in range(0, len(topics), CHUNK_SIZE):
                chunk = topics[i:i + CHUNK_SIZE]
                payload = {"op": "subscribe", "args": chunk}
                await ws.send_json(payload)
                print(f"[{time.strftime('%H:%M:%S')}] Subscribed: {chunk}")
                await asyncio.sleep(0.12)  # Throttle per Hummingbot pattern
            
            print(f"\n[{time.strftime('%H:%M:%S')}] Starting monitoring loop...\n")
            
            while True:
                now = time.time()
                elapsed = now - start_time
                
                # Check duration
                if elapsed >= RUN_DURATION_SECONDS:
                    print(f"\n[{time.strftime('%H:%M:%S')}] Run duration reached. Stopping.")
                    break
                
                # Send ping if needed (mirrors _check_and_send_ping_if_needed)
                if now - last_ping_time >= PING_INTERVAL_SECONDS:
                    await ws.send_str("ping")
                    ping_count += 1
                    last_ping_time = now
                
                # Watchdog check (mirrors _check_watchdogs)
                if now - last_watchdog_check >= 5.0:
                    last_watchdog_check = now
                    check_staleness(stats, now, issues)
                
                # Receive message with timeout
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                now = time.time()
                
                if msg.type == aiohttp.WSMsgType.BINARY:
                    text = decompress(msg.data)
                    if text is None:
                        continue
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    text = msg.data
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    issue = f"WS closed: {ws.close_code}"
                    issues.append(issue)
                    print(f"[{time.strftime('%H:%M:%S')}] ❌ {issue}")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    issue = f"WS error: {msg.data}"
                    issues.append(issue)
                    print(f"[{time.strftime('%H:%M:%S')}] ❌ {issue}")
                    break
                else:
                    continue
                
                # Handle pong
                if isinstance(text, str) and text.strip().lower() == "pong":
                    pong_count += 1
                    continue
                
                # Parse JSON
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    continue
                
                # Handle subscription confirmation
                if "event" in data:
                    event = data.get("event")
                    topic = data.get("topic", "")
                    if event == "subscribe":
                        for sym in symbols:
                            if sym in topic:
                                stats[sym].subscribed = True
                                print(f"[{time.strftime('%H:%M:%S')}] ✅ Subscribed: {topic}")
                    elif event == "error":
                        issue = f"Subscription error: {data}"
                        issues.append(issue)
                        print(f"[{time.strftime('%H:%M:%S')}] ❌ {issue}")
                    continue
                
                # Handle orderbook data (mirrors _parse_order_book_diff_message)
                if "data" in data:
                    table = data.get("table", "")
                    items = data.get("data", [])
                    
                    for item in items:
                        symbol = item.get("symbol")
                        if symbol not in stats:
                            continue
                        
                        s = stats[symbol]
                        version = item.get("version")
                        ms_t = item.get("ms_t")
                        bids = item.get("bids", [])
                        asks = item.get("asks", [])
                        msg_type = item.get("type", "update")  # "snapshot" or "update"
                        
                        # Track ANY message (proves subscription is alive)
                        s.last_any_message_time = now
                        
                        # Detect heartbeat (empty bids+asks)
                        is_heartbeat = (not bids) and (not asks)
                        
                        if is_heartbeat:
                            s.heartbeat_count += 1
                            # Don't update data timestamps for heartbeats
                            continue
                        
                        # Non-heartbeat: update data timestamps
                        s.last_data_update_time = now
                        
                        # Per-side timestamp tracking
                        if bids:
                            s.last_bids_update_time = now
                        if asks:
                            s.last_asks_update_time = now
                        
                        # Track update pattern
                        if bids and asks:
                            s.two_sided_updates += 1
                        elif bids:
                            s.bids_only_updates += 1
                        elif asks:
                            s.asks_only_updates += 1
                        
                        s.last_bids_count = len(bids)
                        s.last_asks_count = len(asks)
                        
                        if msg_type == "snapshot":
                            s.snapshot_count += 1
                            if s.first_snapshot_time is None:
                                s.first_snapshot_time = now
                            if version is not None:
                                s.last_version = int(version)
                                if s.first_version is None:
                                    s.first_version = int(version)
                            
                            print(f"[{time.strftime('%H:%M:%S')}] 📸 {symbol} SNAPSHOT #{s.snapshot_count}: "
                                  f"v={version}, bids={len(bids)}, asks={len(asks)}")
                        
                        else:  # update/delta
                            s.delta_count += 1
                            
                            # VERSION TRACKING (NO PROTECTION - process all updates)
                            # We track what Hummingbot would skip, but process anyway
                            if version is not None:
                                new_ver = int(version)
                                
                                if s.last_version is None:
                                    # First update - establish baseline
                                    s.last_version = new_ver
                                    s.first_version = new_ver
                                
                                elif new_ver < s.last_version:
                                    # Out of order - Hummingbot would skip this
                                    s.out_of_order_count += 1
                                    # But we track it and continue processing
                                    # (simulating no sequence protection)
                                
                                elif new_ver == s.last_version:
                                    # Duplicate - Hummingbot would skip this
                                    s.duplicate_count += 1
                                    # But we track it and continue processing
                                
                                elif new_ver != s.last_version + 1:
                                    # Gap detected - track it
                                    gap_size = new_ver - s.last_version - 1
                                    s.version_gaps.append((s.last_version, new_ver, gap_size))
                                    if gap_size > 10:
                                        issue = f"{symbol}: Version gap! {s.last_version} -> {new_ver} (gap={gap_size})"
                                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️  {issue}")
                                        issues.append(issue)
                                
                                # ALWAYS update version (no protection)
                                s.last_version = new_ver
                            
                            # Log periodically (every 50th delta per symbol)
                            if s.delta_count % 50 == 0:
                                print(f"[{time.strftime('%H:%M:%S')}] 📊 {symbol}: {s.delta_count} deltas, "
                                      f"v={s.last_version}, hb={s.heartbeat_count}")
                
                # Periodic report
                if now - last_report_time >= REPORT_INTERVAL_SECONDS:
                    last_report_time = now
                    print_periodic_report(stats, elapsed, issues, ping_count, pong_count)
    
    # Final report
    print_final_report(stats, time.time() - start_time, issues, ping_count, pong_count)


def check_staleness(stats: Dict[str, SymbolStats], now: float, issues: List[str]):
    """Check for staleness (mirrors _check_stale_pairs)"""
    for symbol, s in stats.items():
        if not s.subscribed or s.last_data_update_time == 0:
            continue
        
        # Check data staleness (not any-message staleness)
        data_staleness = now - s.last_data_update_time
        
        if data_staleness >= DEPTH_STALENESS_SECONDS:
            s.staleness_events += 1
            if data_staleness > s.max_staleness_seconds:
                s.max_staleness_seconds = data_staleness
            
            # Only log first occurrence or significant changes
            if s.staleness_events == 1 or s.staleness_events % 10 == 0:
                any_msg_staleness = now - s.last_any_message_time
                issue = (f"{symbol}: STALE DATA for {data_staleness:.1f}s "
                        f"(any_msg: {any_msg_staleness:.1f}s ago, heartbeats: {s.heartbeat_count})")
                print(f"[{time.strftime('%H:%M:%S')}] 🔴 {issue}")
                if s.staleness_events == 1:
                    issues.append(issue)
        
        # Per-side staleness tracking (relaxed - only log extreme cases)
        # Low-volume pairs naturally have asymmetric updates
        # if s.last_bids_update_time > 0 and s.last_asks_update_time > 0:
        #     bids_staleness = now - s.last_bids_update_time
        #     asks_staleness = now - s.last_asks_update_time
        #     if abs(bids_staleness - asks_staleness) > PER_SIDE_STALENESS_THRESHOLD:
        #         ... (disabled to reduce noise)


def print_periodic_report(stats: Dict[str, SymbolStats], elapsed: float, issues: List[str], 
                          ping_count: int, pong_count: int):
    """Print periodic status report"""
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    now = time.time()
    
    print(f"\n{'='*110}")
    print(f"PERIODIC REPORT - Elapsed: {mins}m {secs}s | Pings: {ping_count}, Pongs: {pong_count}")
    print(f"{'='*110}")
    print(f"{'Symbol':18} | {'Sub':<3} | {'Snaps':>5} | {'Deltas':>7} | {'HB':>5} | {'v':>10} | "
          f"{'DataStale':>9} | {'Gaps':>4} | {'Dup':>4} | Status")
    print("-" * 110)
    
    for symbol, s in sorted(stats.items()):
        data_staleness = now - s.last_data_update_time if s.last_data_update_time > 0 else float('inf')
        gaps = len(s.version_gaps)
        
        if data_staleness > DEPTH_STALENESS_SECONDS or s.out_of_order_count > 0:
            status = "🔴"
        elif not s.subscribed or s.snapshot_count == 0:
            status = "⚪"
        elif gaps > 0 or s.duplicate_count > 0:
            status = "🟡"
        else:
            status = "🟢"
        
        v = s.last_version or "N/A"
        sub_mark = "✓" if s.subscribed else "✗"
        stale_str = f"{data_staleness:.1f}s" if data_staleness < 1000 else "N/A"
        
        print(f"{status} {symbol:16} | {sub_mark:^3} | {s.snapshot_count:>5} | {s.delta_count:>7} | "
              f"{s.heartbeat_count:>5} | {v:>10} | {stale_str:>9} | {gaps:>4} | {s.duplicate_count:>4} |")
    
    print("-" * 110)
    if issues:
        print(f"⚠️  Total issues: {len(issues)}")
    print(f"{'='*110}\n")


def print_final_report(stats: Dict[str, SymbolStats], total_time: float, issues: List[str],
                       ping_count: int, pong_count: int):
    """Print final summary report"""
    mins = int(total_time // 60)
    secs = int(total_time % 60)
    
    print(f"\n{'#'*110}")
    print(f"FINAL REPORT - Total Runtime: {mins}m {secs}s")
    print(f"{'#'*110}")
    
    total_deltas = sum(s.delta_count for s in stats.values())
    total_snapshots = sum(s.snapshot_count for s in stats.values())
    total_heartbeats = sum(s.heartbeat_count for s in stats.values())
    total_gaps = sum(len(s.version_gaps) for s in stats.values())
    total_duplicates = sum(s.duplicate_count for s in stats.values())
    total_ooo = sum(s.out_of_order_count for s in stats.values())
    total_staleness_events = sum(s.staleness_events for s in stats.values())
    subscribed_count = sum(1 for s in stats.values() if s.subscribed)
    
    print(f"\n📡 CONNECTION STATS:")
    print(f"  Pings Sent: {ping_count}")
    print(f"  Pongs Received: {pong_count}")
    print(f"  Ping/Pong Match: {'✅' if ping_count == pong_count else '⚠️'}")
    
    print(f"\n📊 MESSAGE STATS:")
    print(f"  Symbols Subscribed: {subscribed_count}/{len(stats)}")
    print(f"  Total Snapshots: {total_snapshots}")
    print(f"  Total Deltas: {total_deltas}")
    print(f"  Total Heartbeats: {total_heartbeats} (empty bids+asks)")
    
    print(f"\n🔢 VERSION SEQUENCING:")
    print(f"  Total Version Gaps: {total_gaps}")
    print(f"  Total Duplicates: {total_duplicates} (would be SKIPPED by Hummingbot)")
    print(f"  Total Out-of-Order: {total_ooo}")
    
    print(f"\n⏱️ STALENESS ANALYSIS:")
    print(f"  Total Staleness Events: {total_staleness_events} (exceeded {DEPTH_STALENESS_SECONDS}s threshold)")
    print(f"  Total Issues: {len(issues)}")
    
    print(f"\nPER-SYMBOL SUMMARY:")
    print("-" * 120)
    print(f"{'Symbol':18} | {'Sub':<3} | {'Snaps':>5} | {'Deltas':>7} | {'HB':>5} | {'Rate/min':>8} | "
          f"{'MaxStale':>8} | {'Gaps':>4} | {'Dup':>4} | {'StaleEvt':>8} | Status")
    print("-" * 120)
    
    for symbol, s in sorted(stats.items()):
        rate = s.delta_count / (total_time / 60) if total_time > 0 else 0
        gaps = len(s.version_gaps)
        
        if not s.subscribed:
            status = "NOT SUBSCRIBED"
            emoji = "⚪"
        elif s.snapshot_count == 0:
            status = "NO DATA"
            emoji = "⚪"
        elif s.staleness_events > 0:
            status = "HAD STALENESS"
            emoji = "🔴"
        elif gaps > 0 or s.out_of_order_count > 0:
            status = "VERSION ISSUES"
            emoji = "🟡"
        else:
            status = "HEALTHY"
            emoji = "✅"
        
        sub_mark = "✓" if s.subscribed else "✗"
        max_stale = f"{s.max_staleness_seconds:.1f}s" if s.max_staleness_seconds > 0 else "-"
        
        print(f"{symbol:18} | {sub_mark:^3} | {s.snapshot_count:>5} | {s.delta_count:>7} | {s.heartbeat_count:>5} | "
              f"{rate:>8.1f} | {max_stale:>8} | {gaps:>4} | {s.duplicate_count:>4} | {s.staleness_events:>8} | "
              f"{emoji} {status}")
    
    print("-" * 120)
    
    # Update pattern analysis
    print(f"\n📈 UPDATE PATTERNS:")
    for symbol, s in sorted(stats.items()):
        if s.delta_count > 0:
            total = s.bids_only_updates + s.asks_only_updates + s.two_sided_updates
            if total > 0:
                two_pct = s.two_sided_updates / total * 100
                bids_pct = s.bids_only_updates / total * 100
                asks_pct = s.asks_only_updates / total * 100
                if s.bids_only_updates > s.two_sided_updates or s.asks_only_updates > s.two_sided_updates:
                    print(f"  ⚠️  {symbol}: Two-sided: {two_pct:.0f}%, Bids-only: {bids_pct:.0f}%, Asks-only: {asks_pct:.0f}%")
    
    if issues:
        print(f"\n🚨 ISSUES LOG ({len(issues)} total):")
        for i, issue in enumerate(issues[:20], 1):
            print(f"  {i}. {issue}")
        if len(issues) > 20:
            print(f"  ... and {len(issues) - 20} more")
    
    print(f"\n{'#'*110}")
    
    # Verdict
    if total_gaps == 0 and total_ooo == 0 and total_staleness_events == 0 and len(issues) == 0:
        print("\n✅ ALL HEALTHY - No issues detected during monitoring period")
    else:
        print(f"\n⚠️  ISSUES DETECTED:")
        if total_gaps > 0:
            print(f"   - {total_gaps} version gaps (Hummingbot applies optimistically)")
        if total_duplicates > 0:
            print(f"   - {total_duplicates} duplicate versions (SKIPPED by Hummingbot - potential freshness issue!)")
        if total_ooo > 0:
            print(f"   - {total_ooo} out-of-order updates")
        if total_staleness_events > 0:
            print(f"   - {total_staleness_events} staleness events (exceeded {DEPTH_STALENESS_SECONDS}s)")
    
    # Check for inactive symbols
    inactive = [sym for sym, s in stats.items() if s.delta_count == 0]
    if inactive:
        print(f"\n📊 Inactive symbols (no updates received): {', '.join(inactive)}")
    
    print()


async def main():
    """Entry point"""
    print("\n" + "=" * 110)
    print("BitMart Orderbook Debug - Hummingbot Pattern Matching")
    print("Uses SOCKS5 proxy on port 1080")
    print("Press Ctrl+C to stop early")
    print("=" * 110 + "\n")
    
    try:
        await debug_bitmart_orderbooks()
    except KeyboardInterrupt:
        print("\n\n[Interrupted by user]")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
