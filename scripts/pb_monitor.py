#!/usr/bin/env python3
"""
pb_monitor.py — Position Balancer Live Monitor
===============================================
Maintains local WS orderbooks for active position-balancer pairs and tails
the HB orchestrator log simultaneously, correlating each balancer cancel/place
with the live top-of-book state at that moment.

Usage:
    python scripts/pb_monitor.py [--log PATH] [--depth N]

Defaults:
    --log   ~/hummingbot/logs/logs_test_multi.log  (read via SSH to myserver)
    --depth 5   (ask/bid levels shown in the OB panel)

Exchanges covered (hardcoded to active pb pairs):
    BitMart  — IR_USDT, 我踏马来了_USDT  (spot/depth20, ~500ms snapshots)
    Gate.io  — MOODENGETH_USDT          (spot.order_book, 100ms snapshots)
    BingX    — ARTX-USDT                (@depth20, ~1s snapshots)
    HTX      — QUICK-USDT               (mbp.refresh.20, ~100ms snapshots)

Output: live terminal view — refreshes on every log event or OB update.

Dependencies:  pip install aiohttp orjson
"""

import asyncio
import argparse
import gzip
import time
import re
import sys
import subprocess
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import ClientWSTimeout
import orjson


# ─── Config ──────────────────────────────────────────────────────────────────

SSH_HOST = "myserver"
DEFAULT_LOG = "~/hummingbot/logs/logs_test_multi.log"

# Pairs to watch: (exchange, ws_symbol, display_name, hb_log_name)
# ws_symbol  = exchange's wire format (for subscribe message)
# display    = short label in the UI
# hb_name    = substring that appears in HB log lines for this asset
PAIRS = [
    ("bitmart", "IR_USDT",             "IR/BM",    "IR"),
    ("bitmart", "我踏马来了_USDT",       "B2/BM",    "我踏马来了"),
    ("gate",    "MOODENGETH_USDT",      "MENG/GT",  "MOODENGETH"),
    ("bingx",   "ARTX-USDT",           "ARTX/BX",  "ARTX"),
    ("htx",     "quickusdt",           "QUICK/HTX","QUICK"),
]

# WS endpoints
WS_BITMART = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
WS_GATE    = "wss://api.gateio.ws/ws/v4/"
WS_BINGX   = "wss://open-api-ws.bingx.com/market"
WS_HTX     = "wss://api-aws.huobi.pro/feed"

# Log filter: lines we care about
LOG_RE = re.compile(
    r"Position balancer: (Cancelled|Placed|buy completed|sell completed)|"
    r"Placed (buy|sell) limit order|"
    r"streak=\d+|"
    r"Buy-in check:|"
    r"Canceling the limit order"
)

# Parse a cancel/place line into structured fields
CANCEL_RE = re.compile(
    r"Position balancer: Cancelled (buy|sell) order \S+ for (.+?) on (\S+) "
    r"\((.+?)\) \[cooldown=(\S+), streak=(\d+)\]"
)
PLACE_RE = re.compile(
    r"Placed (buy|sell) limit order \S+ for ([\d.]+) (.+?) at ([\d.]+) \(spread: (.+?)\)"
)
ARB_CANCEL_RE = re.compile(
    r"\((\S+)\) Canceling the limit order"
)
BUYIN_RE = re.compile(
    r"Buy-in check: asset=(\S+) actual_base=([\d.]+) pending=([\d.]+) bid=([\d.]+) value=([\d.]+) target=([\d.]+) -> (\S+)"
)


# ─── Orderbook state ─────────────────────────────────────────────────────────

@dataclass
class OBLevel:
    price: float
    qty:   float


@dataclass
class LocalOrderBook:
    label:    str
    exchange: str
    symbol:   str          # wire symbol (e.g. IR_USDT)
    hb_name:  str          # substring in HB log

    bids: List[OBLevel] = field(default_factory=list)
    asks: List[OBLevel] = field(default_factory=list)
    last_update: float = 0.0
    update_count: int  = 0

    # Track our current open order (price, side, qty)
    our_order: Optional[Tuple[str, float, float]] = None  # (side, price, qty)

    def apply_snapshot(self, raw_bids: List, raw_asks: List):
        """Accept [[price_str, qty_str], ...] — sorted bids DESC, asks ASC."""
        self.bids = [OBLevel(float(b[0]), float(b[1])) for b in raw_bids]
        self.asks = [OBLevel(float(a[0]), float(a[1])) for a in raw_asks]
        self.last_update = time.time()
        self.update_count += 1

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def age_ms(self) -> float:
        return (time.time() - self.last_update) * 1000 if self.last_update else 9999.0

    def top_levels(self, n: int) -> Tuple[List[OBLevel], List[OBLevel]]:
        return self.bids[:n], self.asks[:n]


# ─── Global state ────────────────────────────────────────────────────────────

books: Dict[str, LocalOrderBook] = {}   # key = hb_name (e.g. "IR")
log_lines: deque = deque(maxlen=40)     # recent structured log events


def get_book(hb_name: str) -> Optional[LocalOrderBook]:
    return books.get(hb_name)


# ─── WS: BitMart ─────────────────────────────────────────────────────────────

async def run_bitmart(symbols: List[str]):
    """Subscribe to spot/depth20 for given symbols (wire format: IR_USDT)."""
    bm_books = {s: b for b in books.values() if b.exchange == "bitmart"
                for s in [b.symbol]}

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_BITMART,
                    autoping=False,
                    heartbeat=None,
                    timeout=ClientWSTimeout(ws_receive=60, ws_close=15),
                ) as ws:
                    # Subscribe in batches of 20
                    args = [f"spot/depth20:{sym}" for sym in symbols]
                    for i in range(0, len(args), 20):
                        await ws.send_json({"op": "subscribe", "args": args[i:i+20]})
                        await asyncio.sleep(0.12)

                    ping_task = asyncio.create_task(_bitmart_ping(ws))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                if msg.data == "pong":
                                    continue
                                try:
                                    data = orjson.loads(msg.data)
                                except Exception:
                                    continue

                                if "data" not in data:
                                    continue
                                if data.get("table") != "spot/depth20":
                                    continue

                                for item in data["data"]:
                                    raw_sym = item.get("symbol", "")   # e.g. IR_USDT
                                    book = bm_books.get(raw_sym)
                                    if book is None:
                                        continue
                                    bids = item.get("bids", [])
                                    asks = item.get("asks", [])
                                    if bids or asks:
                                        book.apply_snapshot(bids, asks)
                    finally:
                        ping_task.cancel()

        except Exception as e:
            pass
        await asyncio.sleep(3)


async def _bitmart_ping(ws):
    while True:
        await asyncio.sleep(15)
        try:
            await ws.send_str("ping")
        except Exception:
            break


# ─── WS: Gate ────────────────────────────────────────────────────────────────

async def run_gate(symbols: List[str]):
    """Subscribe to spot.order_book (100ms full snapshots)."""
    gt_books = {b.symbol: b for b in books.values() if b.exchange == "gate"}

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_GATE,
                    autoping=False,
                    heartbeat=None,
                    timeout=ClientWSTimeout(ws_receive=60, ws_close=15),
                ) as ws:
                    now = int(time.time())
                    for idx, sym in enumerate(symbols):
                        gate_sym = sym  # already in MOODENGETH_USDT format
                        await ws.send_json({
                            "time": now,
                            "channel": "spot.order_book",
                            "event": "subscribe",
                            "payload": [gate_sym, "20", "100ms"],
                            "id": now + idx,
                        })

                    ping_task = asyncio.create_task(_gate_ping(ws))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = orjson.loads(msg.data)
                                except Exception:
                                    continue

                                if (data.get("event") == "update" and
                                        data.get("channel") == "spot.order_book"):
                                    result = data.get("result", {})
                                    ex_sym = result.get("s", "")   # BTC_USDT
                                    book = gt_books.get(ex_sym)
                                    if book is None:
                                        continue
                                    bids = result.get("bids", [])
                                    asks = result.get("asks", [])
                                    if bids or asks:
                                        book.apply_snapshot(bids, asks)
                    finally:
                        ping_task.cancel()

        except Exception:
            pass
        await asyncio.sleep(3)


async def _gate_ping(ws):
    while True:
        await asyncio.sleep(25)
        try:
            await ws.send_json({
                "time": int(time.time()),
                "channel": "spot.ping",
                "event": "",
                "payload": [],
            })
        except Exception:
            break


# ─── WS: BingX ───────────────────────────────────────────────────────────────

async def run_bingx(symbols: List[str]):
    """Subscribe to @depth20 for given symbols (wire format: ARTX-USDT)."""
    # Map normalised key (ARTXUSDT) → book; BingX sends dataType like "ARTX-USDT@depth20"
    bx_books = {}
    for b in books.values():
        if b.exchange == "bingx":
            # normalised key: strip dash
            norm = b.symbol.replace("-", "")
            bx_books[norm] = b

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_BINGX,
                    headers={"Accept-Encoding": "gzip"},
                    autoping=False,
                    heartbeat=None,
                    timeout=ClientWSTimeout(ws_receive=60, ws_close=10),
                ) as ws:
                    # Subscribe — one request per symbol
                    for idx, sym in enumerate(symbols):
                        await ws.send_json({
                            "id": f"sub_{sym}_{idx}",
                            "reqType": "sub",
                            "dataType": f"{sym}@depth20",
                        })

                    # Send immediate ping to trigger first snapshot quickly
                    await ws.send_json({"ping": int(time.time() * 1000)})

                    ping_task = asyncio.create_task(_bingx_ping(ws))
                    try:
                        async for msg in ws:
                            raw = None
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                try:
                                    raw = gzip.decompress(msg.data).decode("utf-8")
                                except Exception:
                                    continue
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                raw = msg.data
                            else:
                                continue

                            # BingX server ping — respond with plain text "Pong"
                            if raw and "Ping" in raw:
                                await ws.send_str("Pong")
                                continue

                            try:
                                data = orjson.loads(raw)
                            except Exception:
                                continue

                            if "pong" in data or "ping" in data:
                                # JSON ping from server — respond
                                if "ping" in data:
                                    await ws.send_json({"pong": data["ping"], "time": int(time.time() * 1000)})
                                continue

                            # Orderbook data: dataType = "ARTX-USDT@depth20"
                            dt = data.get("dataType", "")
                            if "@depth" not in dt:
                                continue

                            # Normalise: "ARTX-USDT@depth20" → "ARTXUSDT"
                            norm = dt.split("@")[0].replace("-", "")
                            book = bx_books.get(norm)
                            if book is None:
                                continue

                            depth_data = data.get("data", {})
                            bids = depth_data.get("bids", [])
                            asks = depth_data.get("asks", [])

                            # BingX sends unsorted data — sort before storing
                            bids = sorted(bids, key=lambda x: float(x[0]), reverse=True)
                            asks = sorted(asks, key=lambda x: float(x[0]))

                            if bids or asks:
                                book.apply_snapshot(bids, asks)
                    finally:
                        ping_task.cancel()

        except Exception:
            pass
        await asyncio.sleep(3)


async def _bingx_ping(ws):
    while True:
        await asyncio.sleep(20)
        try:
            await ws.send_json({"ping": int(time.time() * 1000)})
        except Exception:
            break


# ─── WS: HTX ─────────────────────────────────────────────────────────────────

async def run_htx(symbols: List[str]):
    """Subscribe to mbp.refresh.20 (100ms full snapshots) for given symbols.
    HTX wire format: symbol = lowercase, no separator, e.g. "quickusdt".
    Protocol: gzip-compressed binary frames. Ping: {"ping": ts_ms} → respond {"pong": ts_ms}.
    Subscribe: {"sub": "market.<symbol>.mbp.refresh.20", "id": "<str>"}
    Data: ch = "market.<symbol>.mbp.refresh.20", tick.bids/tick.asks = [[price, qty], ...]
    """
    htx_books = {b.symbol: b for b in books.values() if b.exchange == "htx"}

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_HTX,
                    headers={"Accept-Encoding": "gzip"},
                    autoping=False,
                    heartbeat=None,
                    timeout=ClientWSTimeout(ws_receive=60, ws_close=15),
                ) as ws:
                    # Subscribe to mbp.refresh.20 for each symbol
                    for idx, sym in enumerate(symbols):
                        await ws.send_json({
                            "sub": f"market.{sym}.mbp.refresh.20",
                            "id": f"sub_{sym}_{idx}",
                        })
                        await asyncio.sleep(0.1)

                    async for msg in ws:
                        raw = None
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            try:
                                raw = gzip.decompress(msg.data).decode("utf-8")
                            except Exception:
                                continue
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            raw = msg.data
                        else:
                            continue

                        try:
                            data = orjson.loads(raw)
                        except Exception:
                            continue

                        # HTX server ping — respond with pong
                        if "ping" in data:
                            await ws.send_json({"pong": data["ping"]})
                            continue

                        # Orderbook snapshot: ch = "market.quickusdt.mbp.refresh.20"
                        ch = data.get("ch", "")
                        if ".mbp.refresh." not in ch:
                            continue

                        # Extract symbol: "market.quickusdt.mbp.refresh.20" → "quickusdt"
                        parts = ch.split(".")
                        if len(parts) < 2:
                            continue
                        sym_key = parts[1]  # e.g. "quickusdt"
                        book = htx_books.get(sym_key)
                        if book is None:
                            continue

                        tick = data.get("tick", {})
                        bids = tick.get("bids", [])
                        asks = tick.get("asks", [])
                        if bids or asks:
                            # HTX sends sorted: bids desc, asks asc
                            book.apply_snapshot(bids, asks)

        except Exception:
            pass
        await asyncio.sleep(3)


# ─── Log tailer ──────────────────────────────────────────────────────────────

@dataclass
class LogEvent:
    ts:       str
    kind:     str          # "cancel" | "place" | "other"
    side:     str = ""     # "buy" | "sell"
    asset:    str = ""
    exchange: str = ""
    reason:   str = ""
    streak:   int = 0
    price:    float = 0.0
    qty:      float = 0.0
    # OB snapshot at event time
    ob_best_bid: Optional[float] = None
    ob_best_ask: Optional[float] = None
    ob_age_ms:   float = 0.0
    raw:      str = ""


def parse_log_line(line: str) -> Optional[LogEvent]:
    """Parse a relevant HB log line into a structured LogEvent."""
    # Timestamp
    ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    ts = ts_match.group(1) if ts_match else ""

    # Balancer cancel (with streak)
    m = CANCEL_RE.search(line)
    if m:
        side, asset, exchange, reason, cooldown, streak = m.groups()
        return LogEvent(ts=ts, kind="cancel", side=side, asset=asset.strip(),
                        exchange=exchange, reason=reason, streak=int(streak), raw=line.strip())

    # Place
    m = PLACE_RE.search(line)
    if m:
        side, qty, asset, price, spread = m.groups()
        return LogEvent(ts=ts, kind="place", side=side, asset=asset.strip(),
                        qty=float(qty), price=float(price), reason=spread, raw=line.strip())

    # Arb-timeout cancel: "(QUICK-USDT) Canceling the limit order ..."
    m = ARB_CANCEL_RE.search(line)
    if m:
        pair = m.group(1)  # e.g. QUICK-USDT
        asset = pair.split("-")[0] if "-" in pair else pair.split("_")[0]
        return LogEvent(ts=ts, kind="arb_cancel", asset=asset.strip(),
                        reason="arb order_timeout", raw=line.strip())

    # Buy-in check line
    m = BUYIN_RE.search(line)
    if m:
        asset, actual, pending, bid, value, target, status = m.groups()
        return LogEvent(ts=ts, kind="buyin_check", asset=asset.strip(),
                        price=float(bid), qty=float(pending),
                        reason=f"val={float(value):.2f} tgt={float(target):.2f} pending={float(pending):.1f} → {status}",
                        raw=line.strip())

    # Other interesting lines
    if "Position balancer:" in line:
        return LogEvent(ts=ts, kind="other", raw=line.strip())

    return None


async def tail_log_remote(log_path: str):
    """SSH tail -f the remote log file and parse relevant lines."""
    cmd = ["ssh", SSH_HOST, f"tail -f {log_path}"]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )

    while True:
        line_bytes = await proc.stdout.readline()
        if not line_bytes:
            await asyncio.sleep(0.5)
            continue
        line = line_bytes.decode("utf-8", errors="replace").rstrip()

        if not LOG_RE.search(line):
            continue

        ev = parse_log_line(line)
        if ev is None:
            continue

        # Attach live OB snapshot at this moment
        book = get_book(ev.asset)
        if book:
            ev.ob_best_bid = book.best_bid
            ev.ob_best_ask = book.best_ask
            ev.ob_age_ms   = book.age_ms
            # Update our order tracking
            if ev.kind == "place":
                book.our_order = (ev.side, ev.price, ev.qty)
            elif ev.kind in ("cancel", "arb_cancel"):
                book.our_order = None

        # Always append — even if no book yet (e.g. HTX before first OB snapshot)
        log_lines.appendleft(ev)
        render()


# ─── Renderer ────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"
BLUE   = "\033[94m"


def fmt_price(p: Optional[float], decimals: int = 8) -> str:
    if p is None:
        return "  ----  "
    return f"{p:.{decimals}f}"


def price_decimals(price: Optional[float], book: "LocalOrderBook | None" = None) -> int:
    """Determine display decimals from price magnitude + actual tick size in OB."""
    if price is None:
        return 6
    if price < 0.000001:
        return 12
    if price < 0.00001:
        return 9
    if price < 0.001:
        return 7
    if price < 0.1:
        return 5
    # For prices >= 0.1, detect tick size from OB levels to avoid rounding duplicates
    if book and len(book.asks) >= 2:
        # Find smallest price difference between adjacent ask levels
        min_diff = min(
            abs(book.asks[i+1].price - book.asks[i].price)
            for i in range(min(len(book.asks)-1, 5))
            if abs(book.asks[i+1].price - book.asks[i].price) > 1e-10
        ) if len(book.asks) >= 2 else 0
        if min_diff > 0:
            import math
            needed = max(4, -int(math.floor(math.log10(min_diff))))
            return needed
    return 6  # default to 6 for >= 0.1 to avoid hiding sub-cent ticks


def render():
    """Clear screen and redraw the full UI."""
    lines = []
    lines.append(f"{BOLD}{'─'*90}{RESET}")
    lines.append(f"{BOLD}  Position Balancer Live Monitor{RESET}  "
                 f"{DIM}{time.strftime('%H:%M:%S UTC', time.gmtime())}{RESET}")
    lines.append(f"{BOLD}{'─'*90}{RESET}")

    # ── Orderbook panels ──────────────────────────────────────────────────
    for hb_name, book in books.items():
        dec = price_decimals(book.best_ask or book.best_bid, book)
        age = book.age_ms
        stale_threshold = 15000 if book.exchange == "bingx" else 10000
        age_str = f"{age:5.0f}ms" if age < stale_threshold else f"{DIM}STALE{RESET}"
        our = book.our_order

        header = (f"{BOLD}{CYAN}{book.label:10s}{RESET} "
                  f"{DIM}upd={book.update_count:<5d} age={age_str}{RESET}")
        lines.append(header)

        bids, asks = book.top_levels(5)

        # Side-by-side: bids | asks
        col_w = 30
        asks_rev = list(reversed(asks))  # show lowest ask at bottom (nearest mid)
        rows = max(len(asks_rev), len(bids))

        for i in range(rows):
            ask_lv = asks_rev[i] if i < len(asks_rev) else None
            bid_lv = bids[i]     if i < len(bids)     else None

            # Ask column (right side logically, but we show ask above bid)
            if ask_lv:
                # Highlight our order level
                is_ours = (our and our[0] == "sell" and
                           abs(ask_lv.price - our[1]) < ask_lv.price * 1e-6)
                ask_marker = f"{YELLOW}◄ OUR ORDER{RESET}" if is_ours else ""
                ask_part = (f"{RED}{fmt_price(ask_lv.price, dec):>14s}  "
                            f"{ask_lv.qty:>14.1f}{RESET}  {ask_marker}")
            else:
                ask_part = " " * col_w

            if bid_lv:
                is_ours = (our and our[0] == "buy" and
                           abs(bid_lv.price - our[1]) < bid_lv.price * 1e-6)
                bid_marker = f"{YELLOW}◄ OUR ORDER{RESET}" if is_ours else ""
                bid_part = (f"{GREEN}{fmt_price(bid_lv.price, dec):>14s}  "
                            f"{bid_lv.qty:>14.1f}{RESET}  {bid_marker}")
            else:
                bid_part = " " * col_w

            # Mid separator between ask rows and bid rows
            if i == len(asks_rev) - 1:
                spread = None
                if book.best_bid and book.best_ask:
                    spread = (book.best_ask - book.best_bid) / book.best_bid * 100
                sep = (f"  {DIM}spread {spread:.3f}%{RESET}"
                       if spread else "")
                lines.append(f"  {ask_part}    {sep}")
                lines.append(f"  {'─'*55}")
                lines.append(f"  {bid_part}")
            elif i < len(asks_rev) - 1:
                lines.append(f"  {ask_part}")
            else:
                lines.append(f"  {bid_part}")

        lines.append("")

    # ── Recent log events ─────────────────────────────────────────────────
    lines.append(f"{BOLD}{'─'*90}{RESET}")
    lines.append(f"{BOLD}  Recent Events{RESET}  {DIM}(newest first){RESET}")
    lines.append(f"{BOLD}{'─'*90}{RESET}")

    shown = 0
    for ev in log_lines:
        if shown >= 25:
            break

        book = books.get(ev.asset)
        dec  = price_decimals(ev.price or (book.best_ask if book else None), book)

        if ev.kind == "cancel":
            # Check if cancel was for an order NOT at top of book at cancel time
            anomaly = ""
            if ev.ob_best_ask and ev.side == "sell" and ev.price > 0:
                gap_ticks = abs(ev.price - ev.ob_best_ask)
                if book and book.asks:
                    tick = min(abs(book.asks[i+1].price - book.asks[i].price)
                               for i in range(len(book.asks)-1)) if len(book.asks) > 1 else 0
                    if tick > 0 and gap_ticks > tick * 2.5:
                        anomaly = f"  {RED}⚠ STALE OB AT CANCEL: our={ev.price:.{dec}f} ask={ev.ob_best_ask:.{dec}f} gap={gap_ticks:.{dec}f}{RESET}"

            ob_info = ""
            if ev.ob_best_ask and ev.side == "sell":
                ob_info = (f"  {DIM}[ob_ask={ev.ob_best_ask:.{dec}f} "
                           f"age={ev.ob_age_ms:.0f}ms]{RESET}")
            elif ev.ob_best_bid and ev.side == "buy":
                ob_info = (f"  {DIM}[ob_bid={ev.ob_best_bid:.{dec}f} "
                           f"age={ev.ob_age_ms:.0f}ms]{RESET}")

            streak_col = ""
            if ev.streak >= 5:
                streak_col = f"{YELLOW}streak={ev.streak}{RESET}"
            else:
                streak_col = f"streak={ev.streak}"

            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  "
                f"{RED}CANCEL {ev.side.upper():4s}{RESET}  "
                f"{CYAN}{ev.asset:15s}{RESET}  "
                f"{ev.exchange:8s}  "
                f"{streak_col:20s}  "
                f"{DIM}{ev.reason[:40]}{RESET}"
                f"{ob_info}{anomaly}"
            )

        elif ev.kind == "place":
            # Highlight if placed price is far from current best ask/bid.
            # Log now records full 10-decimal precision so direct comparison is valid.
            anomaly = ""
            if ev.side == "sell" and ev.ob_best_ask and ev.price > 0:
                gap = ev.price - ev.ob_best_ask
                if gap > ev.ob_best_ask * 0.002:  # >0.2% above best ask = suspicious
                    anomaly = (f"  {RED}⚠ PLACED {gap/ev.ob_best_ask*100:.2f}% ABOVE ASK "
                               f"(ob_age={ev.ob_age_ms:.0f}ms){RESET}")
            elif ev.side == "buy" and ev.ob_best_bid and ev.price > 0:
                gap = ev.ob_best_bid - ev.price
                if gap > ev.ob_best_bid * 0.002:
                    anomaly = (f"  {RED}⚠ PLACED {gap/ev.ob_best_bid*100:.2f}% BELOW BID "
                               f"(ob_age={ev.ob_age_ms:.0f}ms){RESET}")

            ob_ref = ev.ob_best_ask if ev.side == "sell" else ev.ob_best_bid
            ob_info = (f"  {DIM}[ob_ref={fmt_price(ob_ref, dec)} "
                       f"age={ev.ob_age_ms:.0f}ms]{RESET}") if ob_ref else ""

            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  "
                f"{GREEN}PLACE  {ev.side.upper():4s}{RESET}  "
                f"{CYAN}{ev.asset:15s}{RESET}  "
                f"{'':8s}  "
                f"price={fmt_price(ev.price, dec)}  "
                f"qty={ev.qty:<12.1f}"
                f"{ob_info}{anomaly}"
            )

        elif ev.kind == "arb_cancel":
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  "
                f"{YELLOW}TIMEOUT{RESET}        "
                f"{CYAN}{ev.asset:15s}{RESET}  "
                f"{'':8s}  "
                f"{DIM}arb order_timeout cancel (order re-placed next tick){RESET}"
            )

        elif ev.kind == "buyin_check":
            book = books.get(ev.asset)
            dec = price_decimals(ev.price, book)
            ob_ask = book.best_ask if book else None
            gap_str = ""
            if ob_ask and ev.price > 0:
                gap_pct = (ob_ask - ev.price) / ob_ask * 100
                if abs(gap_pct) > 0.15:
                    gap_str = f"  {RED}⚠ bid={ev.price:.{dec}f} ask={ob_ask:.{dec}f} gap={gap_pct:.2f}%{RESET}"
                else:
                    gap_str = f"  {DIM}ask={ob_ask:.{dec}f}{RESET}"
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  "
                f"{BLUE}BUY-IN {RESET}        "
                f"{CYAN}{ev.asset:15s}{RESET}  "
                f"{'':8s}  "
                f"{DIM}{ev.reason}{RESET}"
                f"{gap_str}"
            )

        else:
            # Other balancer messages (completions, etc.)
            lines.append(f"  {DIM}{ev.ts[11:]}{RESET}  {DIM}{ev.raw[60:120]}{RESET}")

        shown += 1

    # Clear screen and print
    sys.stdout.write("\033[2J\033[H")  # clear + home
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


async def _periodic_render():
    """Refresh the display every 2s even when no log events arrive."""
    while True:
        await asyncio.sleep(2)
        render()


# ─── Main ────────────────────────────────────────────────────────────────────

async def main(log_path: str):
    # Build book registry
    for exchange, ws_sym, label, hb_name in PAIRS:
        books[hb_name] = LocalOrderBook(
            label=label, exchange=exchange,
            symbol=ws_sym, hb_name=hb_name,
        )

    # Gather symbols per exchange
    bm_symbols = [b.symbol for b in books.values() if b.exchange == "bitmart"]
    gt_symbols = [b.symbol for b in books.values() if b.exchange == "gate"]
    bx_symbols = [b.symbol for b in books.values() if b.exchange == "bingx"]
    htx_symbols = [b.symbol for b in books.values() if b.exchange == "htx"]

    print(f"Connecting orderbook feeds: BitMart({bm_symbols}) Gate({gt_symbols}) "
          f"BingX({bx_symbols}) HTX({htx_symbols})")
    print(f"Tailing log via SSH: {SSH_HOST}:{log_path}")
    print("Press Ctrl+C to exit.\n")
    await asyncio.sleep(1)

    tasks = [
        asyncio.create_task(run_bitmart(bm_symbols)),
        asyncio.create_task(run_gate(gt_symbols)),
        asyncio.create_task(run_bingx(bx_symbols)),
        asyncio.create_task(run_htx(htx_symbols)),
        asyncio.create_task(tail_log_remote(log_path)),
        asyncio.create_task(_periodic_render()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Position Balancer Live Monitor")
    parser.add_argument("--log", default=DEFAULT_LOG,
                        help="Remote log path (on myserver)")
    parser.add_argument("--depth", type=int, default=5,
                        help="Orderbook levels to show")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.log))
    except KeyboardInterrupt:
        print("\nStopped.")
