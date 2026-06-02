#!/usr/bin/env python3
"""
pb_monitor.py — Position Balancer audit & monitor
=================================================
Audits how the live position balancer is performing by **grounding every PB log
event in the real orderbook**. It maintains a local WS orderbook for each active
PB asset on every exchange it trades, tails the HB orchestrator log over SSH, and
folds each cancel/place/refuge/backstop event together with the live top-of-book
into a per-(asset:exchange) audit. The whole point of the local books is to judge
PB's decisions against the actual market — not just count log lines.

Two output modes:
  • DEFAULT — audit summary: monitor for `--duration` seconds, then print a report
    with two layers: (1) CORRECTNESS — does PB behave per its spec
    ([[position-balancer]] MD): placements at top∓1tick, refuge arms at streak≥10,
    parks 2nd-best under the wall, exits on the right condition, step-up gap≥5t &
    never in refuge; violations flagged. (2) PERFORMANCE — time-at-top %, undercut
    depth, refuge sink, spread context, fills/drift → where to focus for viability.
  • --live — the live terminal dashboard (OB panels + event stream); prints the
    same summary on Ctrl+C.

Discovery is automatic (no hardcoded pairs): logs → which assets are PB-active +
side; `test_multi.yml` → every exchange each trades (PB can jump venues). Orderbook
feeds REUSE the ws_book_checker adapters (`Tracker_cex_cex/ws_book_checker/
exchanges/*`); HTX uses a small purpose-built feed; gate/bitmart/htx route through
the SOCKS5 tunnel (ISP throttling), the rest direct.

Usage:
    python scripts/pb_monitor.py                 # audit 10 min, print summary
    python scripts/pb_monitor.py --duration 300  # audit 5 min
    python scripts/pb_monitor.py --live          # live dashboard
    python scripts/pb_monitor.py --pairs "MANYU/kucoin,IR/bitmart"   # pin pairs

Dependencies:  aiohttp orjson aiohttp-socks  (+ ws_book_checker deps: numpy
               sortedcontainers protobuf python-dotenv — already installed there).
"""

import asyncio
import argparse
import os
import sys
import time
import re
import socket
import logging
import contextvars
import warnings
warnings.filterwarnings("ignore")  # silence protobuf gencode version UserWarnings from mexc pb2
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

import aiohttp
import orjson


# ─── SOCKS5 proxy, PER-EXCHANGE (route only the venues that need it) ───────────
# Pavel runs from Saint Petersburg; the ISP throttles/blocks bulk TCP to *some*
# foreign datacenter IPs (see wiki tools/vpn-bypass-russia). Measured per venue
# (BTC-USDT, 2026-06-01):
#   • DIRECT-broken, proxy-fixed → gate (direct: 2.5M WSMsgType.ERROR + 15 upd
#     in 25s; via SOCKS5: 232 upd, 0 err), bitmart (direct: "No PONG" reconnect
#     churn + persistent STALE; via SOCKS5: 0 reconnects, ~4× the update rate —
#     measured 2026-06-02 on BTCUSDT/IRUSDT, see bitmart_ob_test.py). NB: bitmart
#     only truly tunnels because patched_init now REPLACES its hardcoded
#     TCPConnector (see enable_socks_proxy) — before that it was silently direct.
#   • DIRECT-fine → kucoin, mexc, bingx, bybit, bitget, coinex, binance.
#   • htx: works DIRECT (slow — genuine low-freq mbp.refresh.20 snapshots, 5–20s
#     gaps), but FREEZES after ~6 snapshots THROUGH the proxy (gzip/reconnect
#     quirk via SOCKS5). So htx must stay direct.
# → We proxy ONLY the exchanges in PROXY_EXCHANGES, the rest go direct. This is
#   the ws_book_checker network path (its VPS) applied surgically.
#
# Adapters create their own aiohttp.ClientSession() internally (19 sites across
# exchanges/*.py), so we patch ClientSession to inject a SOCKS5 ProxyConnector —
# but only when the connecting exchange is flagged via a ContextVar. The flag is
# set per-adapter right before its connect() runs (in its own asyncio task), so
# concurrent connects don't cross-contaminate.
DEFAULT_SOCKS = "socks5://127.0.0.1:1080"
PROXY_EXCHANGES = {"gate", "bitmart"}   # only these route through the tunnel
_orig_session_init = aiohttp.ClientSession.__init__
_proxy_url_holder = {"url": None}       # set by enable_socks_proxy
# Per-task flag: True while the current exchange's connect() (and its session
# creation) is running, if that exchange should be proxied.
_use_proxy_ctx: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "pb_use_proxy", default=False)


def _socks_port_open(url: str) -> bool:
    try:
        host_port = url.split("://", 1)[-1]
        host, port = host_port.split(":")
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except Exception:
        return False


def enable_socks_proxy(proxy_url: str):
    """Install the per-exchange ClientSession patch. A session created while the
    _use_proxy_ctx flag is True (i.e. inside a PROXY_EXCHANGES adapter's connect)
    gets a SOCKS5 ProxyConnector; all others connect direct.

    NOTE — we REPLACE a passed connector, not only inject when None. BitmartWS
    hardcodes its own aiohttp.TCPConnector (exchanges/bitmart.py L110-118) and
    passes it to ClientSession(connector=…). The old `connector is None` guard
    therefore silently skipped bitmart → it connected DIRECT and got ISP-throttled
    (the "No PONG within 45s" reconnect churn / persistent STALE seen in long
    runs), even though PROXY_EXCHANGES claimed it was proxied. This is the SAME
    class of bug that forces HTX onto a custom feed. We now swap a hardcoded
    connector for the ProxyConnector (closing the orphaned one) so bitmart truly
    tunnels — and because the contextvar is captured by the bg reconnect task,
    every reconnect is covered too. gate uses a bare ClientSession() (no
    connector), so its behaviour is unchanged by this generalisation."""
    try:
        from aiohttp_socks import ProxyConnector
    except ImportError:
        sys.stderr.write("WARNING: aiohttp-socks not installed — cannot proxy. "
                         "`pip install aiohttp-socks`. gate/bitmart may fail.\n")
        return False

    _proxy_url_holder["url"] = proxy_url

    def patched_init(self, *args, **kwargs):
        if _use_proxy_ctx.get():
            try:
                existing = kwargs.get("connector")
                kwargs["connector"] = ProxyConnector.from_url(_proxy_url_holder["url"])
                kwargs.pop("trust_env", None)  # connector + trust_env can conflict
                # Close the connector the adapter built (e.g. bitmart's TCPConnector)
                # so it doesn't leak now that we've replaced it.
                if existing is not None:
                    try:
                        existing.close()
                    except Exception:
                        pass
            except Exception:
                pass
        return _orig_session_init(self, *args, **kwargs)

    aiohttp.ClientSession.__init__ = patched_init
    return True

# ─── Wire in ws_book_checker so we can reuse its exchange adapters ─────────────
# Project layout: /Users/pavel/Documents/VS_code_projects/Tracker_cex_cex/ws_book_checker
WS_BOOK_PARENT = "/Users/pavel/Documents/VS_code_projects/Tracker_cex_cex"
if WS_BOOK_PARENT not in sys.path:
    sys.path.insert(0, WS_BOOK_PARENT)

try:
    from ws_book_checker.exchanges.kucoin import KucoinWS
    from ws_book_checker.exchanges.mexc import MexcWS
    from ws_book_checker.exchanges.htx import HtxWS
    from ws_book_checker.exchanges.bitmart import BitmartWS
    from ws_book_checker.exchanges.bingx import BingxWS
    from ws_book_checker.exchanges.gate import GateWS
    from ws_book_checker.exchanges.bybit import BybitWS
    from ws_book_checker.exchanges.bitget import BitgetWS
    from ws_book_checker.exchanges.coinex import CoinExWS
    from ws_book_checker.exchanges.binance import BinanceWS
    from ws_book_checker.core.connection_manager import ConnectionManager
except Exception as e:  # pragma: no cover
    sys.stderr.write(
        f"\nFATAL: could not import ws_book_checker adapters from {WS_BOOK_PARENT}\n"
        f"  {type(e).__name__}: {e}\n"
        f"Check the path / that the project is present.\n\n"
    )
    raise

# ws_book_checker adapter per exchange. Each adapter's __init__ is simply
# (symbols, callback) — it hardcodes its own name + WS endpoint internally.
EXCHANGE_ADAPTERS = {
    "kucoin":  KucoinWS,
    "mexc":    MexcWS,
    "htx":     HtxWS,
    "bitmart": BitmartWS,
    "bingx":   BingxWS,
    "gate":    GateWS,
    "bybit":   BybitWS,
    "bitget":  BitgetWS,
    "coinex":  CoinExWS,
    "binance": BinanceWS,
}

# Exchange-name normalisation: HB logs / YAML use connector names that differ
# from our EXCHANGE_ADAPTERS keys (e.g. YAML `gate_io`, log `bing_x`).
HB_EXCHANGE_ALIASES = {
    "bing_x": "bingx",
    "gate_io": "gate",
}


# ─── Config ──────────────────────────────────────────────────────────────────

SSH_HOST = "myserver"
DEFAULT_LOG = "~/hummingbot/logs/logs_test_multi.log"

# Log filter: lines we care about for the event feed
LOG_RE = re.compile(
    r"Position balancer: (Cancelled|Placed|buy completed|sell completed|"
    r"SELL refuge|BUY refuge|Spread too tight|Stuck cancel)|"
    r"Placed (buy|sell) limit order|"
    r"refreshed by backstop|"
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
    r"Placed (buy|sell) limit order \S+ for ([\d.eE+-]+) (.+?) at ([\d.eE+-]+) \(spread: (.+?)\)"
)
ARB_CANCEL_RE = re.compile(
    r"\((\S+)\) Canceling the limit order"
)
BUYIN_RE = re.compile(
    r"Buy-in check: asset=(\S+) actual_base=([\d.eE+-]+) pending=([\d.eE+-]+) "
    r"bid=([\d.eE+-]+) value=([\d.eE+-]+) target=([\d.eE+-]+) -> (\S+)"
)
REFUGE_RE = re.compile(
    r"Position balancer: (SELL|BUY) refuge (ARMED|EXITED|placement) for (\S+)"
)
# Refuge sub-detail (for the audit): ARMED streak, EXITED reason, placement wall/jumper.
REFUGE_ARM_RE   = re.compile(r"refuge ARMED for \S+ \(streak=(\d+)\)")
REFUGE_EXIT_RE  = re.compile(r"refuge EXITED for \S+ \(([^)]+)\)")
REFUGE_PLACE_RE = re.compile(r"refuge placement for \S+ — resting (?:under|above) wall "
                             r"([\d.eE+-]+) \((?:jumper|frontrunner) ([\d.eE+-]+)\)")
# Undercut/frontrun cancel embeds the competing top + our price → undercut depth.
UNDERCUT_RE = re.compile(r"(?:undercut|frontrun) \(top (?:ask|bid) ([\d.eE+-]+) [<>] our ([\d.eE+-]+)\)")
# Step-up embeds the tick gap to the next foreign level.
STEPUP_RE   = re.compile(r"step-up into gap \(next (?:ask|bid) [\d.eE+-]+ is ([\d.]+) ticks")
BACKSTOP_RE = re.compile(
    r"Position balancer order \S+ on (\S+) refreshed by backstop"
)
# Maker→taker fallback: a "min" order whose maker price would cross the spread is
# placed at the taker price instead (a real COST event — we pay taker, not maker).
# PB emits the trading_pair (BASE-USDT / BASE_USDT), not a bare base.
TAKER_FALLBACK_RE = re.compile(
    r"Spread too tight for .+? on ([A-Za-z0-9]+)[-_/]USDT\b"
)
# Stuck cancel: a cancel that didn't confirm within the timeout → force-cleanup
# (an anomaly worth surfacing). PB logs the order side as "{order_type} order".
STUCK_CANCEL_RE = re.compile(
    r"Stuck cancel detected for (\S+) order"
)

# Simple, robust discovery: "for <ASSET> on <exch>" + side from place lines.
DISCOVER_PAIR_RE = re.compile(r" for (?:[\d.eE+-]+ )?([A-Za-z0-9]+) on ([a-z_]+)")
DISCOVER_SIDE_RE = re.compile(r"Placed (buy|sell) limit order \S+ for [\d.eE+-]+ ([A-Za-z0-9]+) ")


# ─── Orderbook state ─────────────────────────────────────────────────────────

@dataclass
class OBLevel:
    price: float
    qty:   float


@dataclass
class LocalOrderBook:
    label:    str          # short UI label, e.g. "MANYU/kucoin"
    exchange: str          # normalised exchange key, e.g. "kucoin"
    asset:    str          # canonical asset / hb log name, e.g. "MANYU"
    side:     str = ""     # "buy" (buy-in) | "sell" (sell-off) — from discovery

    bids: List[OBLevel] = field(default_factory=list)
    asks: List[OBLevel] = field(default_factory=list)
    last_update: float = 0.0
    update_count: int  = 0

    # Track our current open order (side, price, qty)
    our_order: Optional[Tuple[str, float, float]] = None

    def apply_levels(self, raw_bids: List, raw_asks: List):
        """Accept [[price, qty], ...] (str or float) — bids DESC, asks ASC."""
        try:
            bids = [OBLevel(float(b[0]), float(b[1])) for b in raw_bids if float(b[1]) > 0]
            asks = [OBLevel(float(a[0]), float(a[1])) for a in raw_asks if float(a[1]) > 0]
        except (ValueError, IndexError, TypeError):
            return
        # Ensure sort order (adapters mostly pre-sort, but be safe).
        bids.sort(key=lambda l: l.price, reverse=True)
        asks.sort(key=lambda l: l.price)
        self.bids = bids
        self.asks = asks
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

    # ── OB primitives the audit reads (side-aware: sell-off works the ask side,
    #    buy-in the bid side) ────────────────────────────────────────────────
    def tick(self) -> float:
        """Estimate the exchange tick = smallest *real* gap between adjacent
        levels. We take the min gap (the true 1-tick spacing) but reject
        near-zero outliers — two levels less than 1% of the median gap apart are
        duplicates/noise, not a genuine 1-tick step. Without that rejection a
        single odd pair on a thin book collapses the estimate and inflates every
        tick metric (the '1144 ticks off' artifact)."""
        diffs = []
        for side in (self.asks, self.bids):
            for i in range(min(len(side) - 1, 10)):
                d = abs(side[i + 1].price - side[i].price)
                if d > 1e-18:
                    diffs.append(d)
        if not diffs:
            return 0.0
        diffs.sort()
        med = diffs[len(diffs) // 2]
        floor = med * 0.01                      # reject gaps < 1% of median (noise)
        real = [d for d in diffs if d >= floor]
        return min(real) if real else med

    def our_levels_from_top(self) -> Optional[int]:
        """How many foreign levels are strictly more aggressive than our order
        (0 = we are at top). Mirrors the PB's c_refuge_foreign_below, read from
        the live book. None if we have no tracked order or the book is empty."""
        if not self.our_order:
            return None
        oside, oprice, _ = self.our_order
        levels = self.asks if oside == "sell" else self.bids
        if not levels:
            return None
        tk = self.tick() or 0.0
        tol = tk * 0.5 if tk else oprice * 1e-9
        cnt = 0
        skipped_own = False
        for lv in levels:
            # more aggressive = lower ask (sell) / higher bid (buy)
            more = (lv.price < oprice - tol) if oside == "sell" else (lv.price > oprice + tol)
            if more:
                cnt += 1
            elif abs(lv.price - oprice) <= tol and not skipped_own:
                skipped_own = True   # skip our own level once
            else:
                break
        return cnt


# ─── Global state ────────────────────────────────────────────────────────────
#
# Books are keyed PER (asset, exchange) leg, e.g. "MANYU@kucoin", "MANYU@mexc" —
# because a PB asset can trade on >1 exchange and PB jumps between them, so we
# maintain ALL its venues' books simultaneously. Callbacks route on
# (exchange, BASEUSDT-symbol) since the same symbol now lives on several books.
books: Dict[str, LocalOrderBook] = {}     # key = "ASSET@exchange"
route: Dict[Tuple[str, str], str] = {}    # (exchange, "MANYUUSDT") → "MANYU@kucoin"
log_assets: set = set()                   # canonical assets the PB is active on (for log-event mapping)
log_lines: deque = deque(maxlen=60)       # recent structured log events
DEPTH = 5
LIVE_MODE = False                         # False = audit-summary mode; True = live dashboard
STALE_MS = {"bingx": 15000, "htx": 20000, "bitmart": 12000}  # per-exchange; default 10000

# ── PB spec constants (MUST mirror position_balancer_handler.pyx) ──────────────
# The audit's PART A correctness checks compare against these. Keep them in sync
# with the .pyx module-level cdefs, or PART A silently mis-judges (e.g. a correct
# refuge arm flagged "premature"). We treat REFUGE_ARM_STREAK as a LOWER BOUND:
# the .pyx value was lowered 10→5 (the pending strategy change), and the VPS may
# run either 5 (after push) or 10 (before). A correct arm fires at streak ≥ the
# deployed value, which is ≥5 under BOTH — so flagging only `arm < 5` never
# false-flags a correct arm on either build, while still catching a genuinely
# premature arm. (Was hardcoded `< 10` inline — false-flagged every arm 5–9 once
# the .pyx ran 5.)
REFUGE_ARM_STREAK = 5          # .pyx: cdef double REFUGE_ARM_STREAK (lowered 10→5 2026-06-01)
STEP_UP_MIN_GAP_TICKS = 5.0    # .pyx: cdef double STEP_UP_MIN_GAP_TICKS


def stale_threshold_ms(exchange: str) -> int:
    return STALE_MS.get(exchange, 10000)


def books_for_asset(asset: str, exchange: str = "") -> List[LocalOrderBook]:
    """Leg books for an asset. If `exchange` is known (e.g. from a cancel line),
    return just that leg; else return all of the asset's legs (a place line names
    no venue — PB has one live order, so we annotate all legs of the asset)."""
    if exchange:
        b = books.get(f"{asset}@{exchange}")
        return [b] if b else []
    return [b for b in books.values() if b.asset == asset]


def primary_book_for_asset(asset: str, exchange: str = "") -> Optional[LocalOrderBook]:
    """One representative leg book for OB-snapshot annotation (prefer the named
    exchange; else the asset's first/most-active leg)."""
    bs = books_for_asset(asset, exchange)
    if not bs:
        return None
    # prefer the freshest (most recently updated) leg when venue is unknown
    return max(bs, key=lambda b: b.last_update)


def nearest_price_leg(asset: str, price: float) -> Optional[LocalOrderBook]:
    """Pick the asset's leg whose live best price is CLOSEST to `price`. The
    placed price itself reveals the venue: a BILL place at 0.0862 matches htx's
    book (~0.086), not bitget's (~0.090). This is the robust attribution for a
    venue-less PLACE — self-correcting, no reliance on having seen a cancel first
    or on YAML market ordering. Falls back to the freshest leg if no leg has a
    usable book/price."""
    if not price or price <= 0:
        return primary_book_for_asset(asset)
    best, best_d = None, None
    for b in books_for_asset(asset):
        ref = b.best_ask if b.side == "sell" else b.best_bid
        ref = ref or b.best_bid or b.best_ask
        if not ref:
            continue
        d = abs(ref - price) / price          # relative distance (scale-agnostic)
        if best_d is None or d < best_d:
            best, best_d = b, d
    return best or primary_book_for_asset(asset)


# ─── Orderbook callback (fed by every ws_book_checker adapter + the htx feed) ──

def on_ob_update(exchange: str, symbol: str, bids: List, asks: List,
                 is_snapshot: bool, update_id=None, timestamp=None):
    """Single callback for all feeds. Routes (exchange, symbol) → leg book.

    Feeds emit `symbol` in internal BASEUSDT form (e.g. MANYUUSDT). We normalise
    and look up the (exchange, symbol) leg, then snapshot the top into its book.
    """
    sym = symbol.replace("-", "").replace("_", "").upper()
    key = route.get((exchange, sym))
    if key is None:
        return
    book = books.get(key)
    if book is None:
        return
    if bids or asks:
        book.apply_levels(bids, asks)


# ─── Pair discovery ───────────────────────────────────────────────────────────

async def discover_pairs(log_path: str, window: int) -> List[Tuple[str, str, str]]:
    """DISCOVERY = YAML (markets) + logs (active assets + side).

    Why both: the YAML CANNOT identify which assets have PB enabled (the
    buy_in_enabled/sell_off_enabled flags are runtime state, toggled via `control`
    and not reliably persisted — only 4/53 strategies even carry the fields, and
    those read False while live-active). So the **logs are authoritative for
    *which* assets are PB-active + their buy/sell side**. The **YAML is
    authoritative for *which exchanges* each asset trades on** (primary +
    secondary + additional_markets) — and PB can jump between them, so we
    subscribe to ALL of them.

    Returns list of (asset, exchange, side) LEGS — one per (active asset × each of
    its YAML markets).
    """
    # 1. Logs → active assets + side (+ venues seen in cancel lines).
    asset_side, asset_log_venues = await discover_active_assets(log_path, window)
    if not asset_side:
        return []
    # 2. YAML → all markets for those assets.
    asset_markets = await fetch_yaml_markets()
    # 3. Cross: one leg per (active asset × each of its markets). Markets come from
    #    the YAML; if an asset isn't in the YAML (it finished + was removed, e.g.
    #    SUP) we fall back to the venue(s) its cancel lines named in the log — so a
    #    removed-but-was-active asset still gets a book + full audit coverage.
    legs: List[Tuple[str, str, str]] = []
    for asset, side in asset_side.items():
        markets = asset_markets.get(asset) or asset_log_venues.get(asset)
        if not markets:
            # active in logs but no markets from YAML nor log cancels (place-only,
            # never cancelled in window) — keep a placeholder so it's reported.
            legs.append((asset, "", side))
            continue
        for exch in markets:
            legs.append((asset, exch, side))
    return legs


async def discover_active_assets(log_path: str, window: int
                                 ) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Logs → ({asset: side}, {asset: [venues seen in cancel lines]}). An asset is
    PB-active if it has ANY recent PB event (place / cancel / refuge / buy-in /
    backstop). Side from `Placed buy|sell` (fallback: cancel side / refuge / buy-in).
    The venues are the fallback for assets that have left the YAML (e.g. SUP finished
    its sell-off and was removed) — cancel lines carry `on <exch>`, so we can still
    open a book + audit them."""
    cmd = (
        f"grep -E 'Position balancer: (Cancelled|Placed|SELL refuge|BUY refuge|"
        f"buy completed|sell completed)|Placed (buy|sell) limit order|"
        f"refreshed by backstop|Buy-in check:' {log_path} | tail -{window}"
    )
    proc = await asyncio.create_subprocess_exec(
        "ssh", SSH_HOST, cmd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await proc.communicate()
    text = out.decode("utf-8", errors="replace")

    asset_side: Dict[str, str] = {}       # asset → side (last seen wins)
    asset_venues: Dict[str, List[str]] = {}   # asset → venues seen in cancel lines
    for line in text.splitlines():
        # "Placed <side> limit order … for <qty> <ASSET> at" → side + asset (strongest signal)
        sm = DISCOVER_SIDE_RE.search(line)
        if sm:
            asset_side[sm.group(2)] = sm.group(1)
            continue
        # "Cancelled <side> order … for <ASSET> on <exch>" → side + asset + venue
        cm = CANCEL_RE.search(line)
        if cm:
            asset = cm.group(2).strip()
            asset_side.setdefault(asset, cm.group(1))
            ex = HB_EXCHANGE_ALIASES.get(cm.group(3), cm.group(3))
            asset_venues.setdefault(asset, [])
            if ex not in asset_venues[asset]:
                asset_venues[asset].append(ex)
            continue
        # refuge "(SELL|BUY) refuge … for <ASSET>" → side + asset
        rm = REFUGE_RE.search(line)
        if rm:
            asset_side.setdefault(rm.group(3).strip(), rm.group(1).lower())
            continue
        # buy-in check → buy-side asset
        bm = BUYIN_RE.search(line)
        if bm:
            asset_side.setdefault(bm.group(1).strip(), "buy")
    return asset_side, asset_venues


async def fetch_yaml_markets() -> Dict[str, List[str]]:
    """YAML → {asset: [normalised exchanges]} from every strategy block
    (primary_market + secondary_market + additional_markets). Asset = the
    strategy's primary_trading_pair base (e.g. MANYU-USDT → MANYU). Run remotely
    so we read the LIVE config the orchestrator is using."""
    py = (
        "import yaml,json;"
        "d=yaml.safe_load(open('conf/scripts/test_multi.yml'));"
        "s=d.get('arbitrage_m_strategies') or d.get('arbitrage_l_strategies') or [];"
        "o={};"
        "\nfor x in s:"
        "\n base=(x.get('primary_trading_pair') or '').split('-')[0]"
        "\n if not base: continue"
        "\n mk=set()"
        "\n for f in ('primary_market','secondary_market'):"
        "\n  if x.get(f): mk.add(x[f])"
        "\n for am in (x.get('additional_markets') or []):"
        "\n  mk.add(am.split(':')[0])"
        "\n o.setdefault(base,set()).update(mk)"
        "\nprint(json.dumps({k:sorted(v) for k,v in o.items()}))"
    )
    proc = await asyncio.create_subprocess_exec(
        "ssh", SSH_HOST, f"cd ~/hummingbot && python3 -c \"{py}\"",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await proc.communicate()
    try:
        import json
        raw = json.loads(out.decode("utf-8", errors="replace").strip().splitlines()[-1])
    except Exception:
        return {}
    # Normalise exchange names to our adapter keys (gate_io→gate, bing_x→bingx).
    norm = {}
    for asset, exchs in raw.items():
        norm[asset] = [HB_EXCHANGE_ALIASES.get(e, e) for e in exchs]
    return norm


def parse_manual_pairs(spec: str) -> List[Tuple[str, str, str]]:
    """Parse --pairs "MANYU/kucoin:sell,NATIX/mexc" → [(asset, exch, side)]."""
    out = []
    for tok in spec.split(","):
        tok = tok.strip()
        if not tok:
            continue
        side = ""
        if ":" in tok:
            tok, side = tok.split(":", 1)
        if "/" not in tok:
            continue
        asset, exch = tok.split("/", 1)
        exch = HB_EXCHANGE_ALIASES.get(exch.strip(), exch.strip())
        out.append((asset.strip(), exch, side.strip()))
    return out


def build_books(legs: List[Tuple[str, str, str]]):
    """Populate `books` (one per asset×exchange leg, key "ASSET@exch") + the
    (exchange, symbol) `route` map. Returns assets/legs skipped (no adapter)."""
    books.clear()
    route.clear()
    log_assets.clear()
    skipped = []
    for asset, exchange, side in legs:
        log_assets.add(asset)             # for log-event → book mapping (any venue)
        if not exchange:
            skipped.append((asset, "(no YAML markets)"))
            continue
        if exchange not in EXCHANGE_ADAPTERS:
            skipped.append((asset, exchange))
            continue
        key = f"{asset}@{exchange}"
        books[key] = LocalOrderBook(
            label=key, exchange=exchange, asset=asset, side=side,
        )
        route[(exchange, f"{asset}USDT")] = key
    return skipped


# ─── Adapter lifecycle ─────────────────────────────────────────────────────────

# One shared ConnectionManager applies the per-exchange WS tuning (strategy,
# max_topics, topics_per_request, delays, burst) and picks the correct connect
# path (strategy, max_topics, delays, burst) AND picks the right connect path
# (incl. connect_with_reconnection for mexc/binance). This MIRRORS ws_book_checker
# main.py's startup exactly — anything it does, we do, because those adapters are
# only proven robust when driven this way.
_CONN_MGR = ConnectionManager()

# Live adapter instances, kept module-level so their background _maintain_connection
# tasks (created inside connect()) are never GC'd for the life of the process.
adapters: List[object] = []


async def setup_adapters(per_exchange: Dict[str, List[str]]):
    """Instantiate + connect every needed adapter, faithfully mirroring the
    ws_book_checker main.py sequence (the only setup under which these adapters
    are proven robust):

      1. build adapter(symbols, callback)
      2. seed subscribed_symbols = set(symbols)   ← inbound-message gate
      3. connect each via ConnectionManager._connect_exchange, per-adapter, with
         the SOCKS5 proxy ContextVar set ONLY for PROXY_EXCHANGES (so gate/bitmart
         route through the tunnel and the rest go direct). The adapter's bg
         _maintain_connection task is created inside _connect_exchange via
         asyncio.create_task, which snapshots the current context → the proxy
         flag propagates to that task's session creation + all its reconnects.
      4. await kucoin.calibration_task (if present) before relying on it
    """
    adapters.clear()
    adapter_by_exchange: Dict[str, object] = {}
    for exchange, symbols in per_exchange.items():
        # HTX is handled by our own purpose-built feed (run_htx_feed), NOT the
        # ws_book_checker HtxWS adapter. Reason: HtxWS hardcodes its own
        # aiohttp.TCPConnector (htx.py line ~135), which silently bypasses our
        # SOCKS5 proxy patch (it only injects when connector is None) → HTX
        # connects DIRECT, gets throttled by the SP ISP, receives 1 snapshot then
        # freezes. Our feed uses a ProxyConnector + the faster mbp.refresh.20
        # channel (proven: 113 ticks/40s sustained on MANYU via proxy).
        if exchange == "htx":
            continue
        cls = EXCHANGE_ADAPTERS[exchange]
        # Every ws_book_checker adapter ctor is (symbols, callback); it sets its
        # own name + WS URL internally.
        adapter = cls(list(symbols), on_ob_update)

        # CRITICAL (main.py line 492): register subscribed symbols BEFORE connect.
        # A bare connect() subscribes to self.symbols but does NOT populate
        # subscribed_symbols, and several adapters (kucoin, coinex, gate…) gate
        # inbound book messages on `symbol in self.subscribed_symbols` → every
        # snapshot silently dropped. main.py seeds this explicitly; so do we.
        adapter.subscribed_symbols = set(symbols)

        # Disable root-level dedup so identical top-of-book updates still refresh
        # the book timestamp (we WANT every update for accurate staleness, not
        # suppression). base.py's Python dedup fallback reads _last_top_bid/_ask
        # when _dedup_cache is None — ensure they exist to avoid AttributeError.
        if hasattr(adapter, "_dedup_cache"):
            adapter._dedup_cache = None
        if getattr(adapter, "_last_top_bid", None) is None:
            adapter._last_top_bid = {}
        if getattr(adapter, "_last_top_ask", None) is None:
            adapter._last_top_ask = {}

        # HOME-NETWORK relaxation. The adapters' pong/zombie/inactivity timeouts
        # are tuned for the VPS's clean low-latency link (ws_book_checker runs on
        # brr_ws). From a home connection, gate/bitmart/htx trip those tight
        # windows (bitmart "No PONG within 15s", gate WSMsgType.ERROR, htx
        # "ZOMBIE: pings OK but NO DATA 65s") → needless reconnect churn even
        # though data is flowing. We relax ONLY the adapters' own threshold
        # attributes (same knobs ConnectionManager sets) — no adapter-code edits,
        # no protocol change. Generous values: we're a passive monitor, a slow
        # feed is fine; we'd rather hold a quiet connection than thrash it.
        for attr, val in (
            ("pong_timeout", 45),            # gate/bitmart: 15 → 45s
            ("inactivity_threshold", 180),   # bitmart/gate: 60/90 → 180s
            ("data_idle_threshold", 180),    # zombie (all 3): 60 → 180s
        ):
            if hasattr(adapter, attr):
                setattr(adapter, attr, val)

        adapters.append(adapter)
        adapter_by_exchange[exchange] = adapter

    # Connect each adapter in its own task with the proxy flag set appropriately,
    # then gather. Setting _use_proxy_ctx at the top of the task means the flag is
    # captured by the bg _maintain_connection task spawned inside _connect_exchange
    # (asyncio.create_task copies the context), so it persists across reconnects.
    async def _connect_one(exchange, adapter):
        _use_proxy_ctx.set(exchange in PROXY_EXCHANGES and _proxy_url_holder["url"] is not None)
        try:
            await _CONN_MGR._connect_exchange(adapter)
        except Exception as e:
            logging.getLogger("pb_monitor").warning(f"{exchange}: connect failed: {e}")

    await asyncio.gather(*(
        _connect_one(exch, ad) for exch, ad in adapter_by_exchange.items()
    ), return_exceptions=True)

    # main.py lines 508-516: KuCoin runs a calibration_task after connect; await
    # it before relying on the feed (prevents sequence-gap spam / empty book).
    kucoin = next((a for a in adapters if getattr(a, "name", "") == "kucoin"), None)
    if kucoin is not None and getattr(kucoin, "calibration_task", None) is not None:
        try:
            await kucoin.calibration_task
        except Exception:
            pass

    # HTX: launch our own feed (bypasses the buggy HtxWS adapter). One task per
    # symbol set; it self-reconnects forever. Kept alive via _htx_tasks.
    htx_symbols = per_exchange.get("htx", [])
    if htx_symbols:
        _htx_tasks.append(asyncio.create_task(run_htx_feed(htx_symbols)))


# ─── Custom HTX feed (purpose-built; bypasses the ws_book_checker HtxWS adapter)
#
# Why: HtxWS hardcodes its own aiohttp.TCPConnector, which bypasses the SOCKS5
# proxy patch → HTX connects direct, gets ISP-throttled, freezes after 1 snapshot.
# This feed is the proven raw logic: /ws endpoint + mbp.refresh.20 channel (3×
# faster than depth.step0 and sustained even on thin assets — verified 113 ticks
# /40s on MANYU via proxy) + gzip frames + app-level ping/pong, over a SOCKS5
# ProxyConnector (HTX is ISP-throttled direct, like gate/bitmart).
HTX_WS_URL = "wss://api-aws.huobi.pro/ws"
_htx_tasks: List[object] = []


async def run_htx_feed(symbols: List[str]):
    """Maintain HTX orderbooks for `symbols` (BASEUSDT form) via mbp.refresh.20,
    routed through the SOCKS5 proxy. Infinite reconnect with backoff."""
    import zlib
    try:
        from aiohttp_socks import ProxyConnector
    except ImportError:
        ProxyConnector = None

    delay = 3.0
    while True:
        ws = None
        session = None
        try:
            # HTX is ISP-throttled direct → always go through the proxy if we have
            # one; fall back to a plain (direct) session otherwise.
            if ProxyConnector is not None and _proxy_url_holder["url"]:
                connector = ProxyConnector.from_url(_proxy_url_holder["url"])
            else:
                connector = None
            session = aiohttp.ClientSession(connector=connector)
            ws = await session.ws_connect(
                HTX_WS_URL, headers={"Accept-Encoding": "gzip"},
                heartbeat=None, timeout=aiohttp.ClientWSTimeout(ws_receive=60, ws_close=15),
            )
            # Subscribe mbp.refresh.20 (array form) for every symbol.
            for i, sym in enumerate(symbols):
                await ws.send_json({"sub": [f"market.{sym.lower()}.mbp.refresh.20"],
                                    "id": f"id_{sym}"})
                if i + 1 < len(symbols):
                    await asyncio.sleep(0.12)
            delay = 3.0  # reset backoff on a clean connect
            last_data = time.time()

            while not ws.closed:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=30)
                except asyncio.TimeoutError:
                    # No frame for 30s — if data is also long-stale, force reconnect.
                    if time.time() - last_data > 90:
                        break
                    continue
                if msg.type == aiohttp.WSMsgType.BINARY:
                    try:
                        d = orjson.loads(zlib.decompress(msg.data, 16 + zlib.MAX_WBITS))
                    except Exception:
                        continue
                    if "ping" in d:                       # HTX app-level ping → pong
                        try:
                            await ws.send_json({"pong": d["ping"]})
                        except Exception:
                            pass
                        continue
                    ch = d.get("ch", "")
                    tick = d.get("tick")
                    if ".mbp.refresh." in ch and isinstance(tick, dict):
                        sym = ch.split(".")[1].upper()    # market.manyuusdt.mbp… → MANYUUSDT
                        bids = tick.get("bids", [])
                        asks = tick.get("asks", [])
                        if bids or asks:
                            last_data = time.time()
                            on_ob_update("htx", sym, bids, asks, True,
                                         tick.get("seqNum"), last_data)
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.CLOSING):
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            try:
                if ws is not None and not ws.closed:
                    await ws.close()
            except Exception:
                pass
            try:
                if session is not None and not session.closed:
                    await session.close()
            except Exception:
                pass
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30.0)


# ─── Log tailer ──────────────────────────────────────────────────────────────

@dataclass
class LogEvent:
    ts:       str
    kind:     str          # cancel|place|arb_cancel|buyin_check|refuge|backstop|other
    side:     str = ""
    asset:    str = ""
    exchange: str = ""
    reason:   str = ""
    streak:   int = 0
    price:    float = 0.0
    qty:      float = 0.0
    ob_best_bid: Optional[float] = None
    ob_best_ask: Optional[float] = None
    ob_age_ms:   float = 0.0
    raw:      str = ""


def parse_log_line(line: str) -> Optional[LogEvent]:
    ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    ts = ts_match.group(1) if ts_match else ""

    m = CANCEL_RE.search(line)
    if m:
        side, asset, exchange, reason, cooldown, streak = m.groups()
        exchange = HB_EXCHANGE_ALIASES.get(exchange, exchange)
        return LogEvent(ts=ts, kind="cancel", side=side, asset=asset.strip(),
                        exchange=exchange, reason=reason, streak=int(streak), raw=line.strip())

    m = PLACE_RE.search(line)
    if m:
        side, qty, asset, price, spread = m.groups()
        return LogEvent(ts=ts, kind="place", side=side, asset=asset.strip(),
                        qty=float(qty), price=float(price), reason=spread, raw=line.strip())

    m = REFUGE_RE.search(line)
    if m:
        side, action, asset = m.groups()
        return LogEvent(ts=ts, kind="refuge", side=side.lower(), asset=asset.strip(),
                        reason=action, raw=line.strip())

    m = BACKSTOP_RE.search(line)
    if m:
        exchange = HB_EXCHANGE_ALIASES.get(m.group(1), m.group(1))
        return LogEvent(ts=ts, kind="backstop", exchange=exchange,
                        reason="refreshed by backstop (5 min)", raw=line.strip())

    m = ARB_CANCEL_RE.search(line)
    if m:
        pair = m.group(1)
        asset = pair.split("-")[0] if "-" in pair else pair.split("_")[0]
        return LogEvent(ts=ts, kind="arb_cancel", asset=asset.strip(),
                        reason="arb order_timeout", raw=line.strip())

    m = BUYIN_RE.search(line)
    if m:
        asset, actual, pending, bid, value, target, status = m.groups()
        return LogEvent(ts=ts, kind="buyin_check", asset=asset.strip(),
                        price=float(bid), qty=float(pending),
                        reason=f"val={float(value):.2f} tgt={float(target):.2f} "
                               f"pending={float(pending):.1f} → {status}",
                        raw=line.strip())

    m = TAKER_FALLBACK_RE.search(line)
    if m:
        # base asset only; venue resolved by the audit via _asset_venue (PB logs the
        # pair, not the venue). side from "bid"/"ask" wording in the same line.
        side = "sell" if "<= bid" in line or "Using bid" in line else "buy"
        return LogEvent(ts=ts, kind="taker_fallback", side=side,
                        asset=m.group(1).strip(), reason="spread too tight → taker",
                        raw=line.strip())

    m = STUCK_CANCEL_RE.search(line)
    if m:
        side = m.group(1).lower()
        side = side if side in ("buy", "sell") else ""
        return LogEvent(ts=ts, kind="stuck_cancel", side=side,
                        reason="stuck cancel → force-cleanup", raw=line.strip())

    if "Position balancer:" in line:
        return LogEvent(ts=ts, kind="other", raw=line.strip())

    return None


async def tail_log_remote(log_path: str):
    """SSH tail -f the remote log and parse relevant lines."""
    while True:
        proc = await asyncio.create_subprocess_exec(
            "ssh", SSH_HOST, f"tail -f {log_path}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            while True:
                line_bytes = await proc.stdout.readline()
                if not line_bytes:
                    break  # ssh dropped — reconnect
                line = line_bytes.decode("utf-8", errors="replace").rstrip()
                if not LOG_RE.search(line):
                    continue
                ev = parse_log_line(line)
                if ev is None:
                    continue

                if ev.asset:
                    # Resolve the event's venue (same priority as the audit, so the
                    # dashboard marker + the audit always agree): explicit venue →
                    # last cancel's venue → nearest-price leg (place) → freshest.
                    if ev.exchange:
                        snap = primary_book_for_asset(ev.asset, ev.exchange)
                    elif _asset_venue.get(ev.asset):
                        snap = primary_book_for_asset(ev.asset, _asset_venue[ev.asset])
                    elif ev.kind == "place" and ev.price > 0:
                        snap = nearest_price_leg(ev.asset, ev.price)
                    else:
                        snap = primary_book_for_asset(ev.asset)
                    if snap:
                        ev.ob_best_bid = snap.best_bid
                        ev.ob_best_ask = snap.best_ask
                        ev.ob_age_ms   = snap.age_ms
                    # OUR ORDER marker: a place marks its resolved leg; a cancel
                    # clears the named leg (PB has one live order per asset).
                    if ev.kind == "place" and snap:
                        snap.our_order = (ev.side, ev.price, ev.qty)
                    elif ev.kind in ("cancel", "arb_cancel"):
                        for b in books_for_asset(ev.asset, ev.exchange):
                            b.our_order = None

                # Fold into the OB-grounded audit (both modes).
                audit_record_event(ev)

                log_lines.appendleft(ev)
                if LIVE_MODE:
                    render()
        except asyncio.CancelledError:
            proc.terminate()
            raise
        except Exception:
            pass
        finally:
            try:
                proc.terminate()
            except Exception:
                pass
        await asyncio.sleep(2)


# ─── Audit (OB-grounded performance + correctness analysis) ───────────────────
#
# The whole point of maintaining local orderbooks is to judge PB's decisions
# against the REAL market. Two layers, both per (asset@exchange) leg:
#   1. CORRECTNESS — does PB behave per its spec (position-balancer MD)? Verified
#      against the live book at each event: placements at top∓1tick; refuge arms
#      at streak≥10, parks 2nd-best under the wall, exits on the right condition;
#      step-up gap ≥5 ticks; step-up never fires in refuge. Violations are flagged.
#   2. PERFORMANCE — given it's correct, how viable is it? time-at-top %, undercut
#      depth, refuge sink, spread context, fill capture → where to focus tuning.
# Both are fed by the same parsed log events + a continuous OB sampler.

@dataclass
class LegAudit:
    asset: str
    exchange: str
    side: str
    # counts
    places: int = 0
    cancels: int = 0
    cancel_reasons: Dict[str, int] = field(default_factory=dict)
    fills: int = 0                       # inferred: streak reset to 1 after being >1
    backstops: int = 0
    stepups: int = 0
    # refuge episodes
    refuge_arms: int = 0
    refuge_exits: Dict[str, int] = field(default_factory=dict)   # reason → count
    refuge_replacements: int = 0         # refuge placement lines (re-parks + initial)
    in_refuge: bool = False
    refuge_armed_at: float = 0.0
    refuge_secs: float = 0.0             # cumulative time in refuge
    streak_peak: int = 0
    # placement accuracy (vs live book at place time)
    place_correct: int = 0               # ≤3 ticks of top (tolerant of placement latency)
    place_offbook: int = 0               # >3 ticks off on a fresh book
    place_no_ob: int = 0                 # book stale/empty at place → can't judge
    # undercut depth (ticks the new top was inside us), from the cancel line
    undercut_depths: List[float] = field(default_factory=list)
    # refuge re-park correctness: at each refuge RE-PLACE, how many foreign orders
    # are ahead of the newly-placed price (read from the live book). 1 = correctly
    # 2nd-best; ≥2 = the re-park did NOT move us back to 2nd-best (real "sink").
    refuge_sink_samples: List[int] = field(default_factory=list)   # per re-park: 0=2nd-best, 1=mis-parked
    _expect_refuge_place: bool = False   # set on a "refuge placement" line; consumed by next place
    _refuge_target: Optional[Tuple[float, float]] = None   # (wall, jumper) from the refuge line
    # INDEPENDENT refuge wall/jumper cross-check: at each "refuge placement" line,
    # verify PB's LOGGED wall/jumper against OUR live book ladder (does jumper = the
    # real L1 foreign level and wall = the real 2nd foreign level?). This catches PB
    # MIS-IDENTIFYING the wall/jumper itself — which the [jumper,wall]-zone re-park
    # check above CANNOT (it trusts PB's own numbers). Sparse-book-safe: matches
    # logged prices to actual book levels by proximity, not by naive level-rank.
    refuge_xcheck_ok:    int = 0         # logged wall/jumper matched the live book
    refuge_xcheck_bad:   int = 0         # mismatch (PB's wall/jumper ≠ real book front)
    refuge_xcheck_noob:  int = 0         # book stale/too-thin to cross-check
    # secondary PB events (cost / anomaly): maker→taker fallback + stuck-cancel
    taker_fallbacks: int = 0             # "spread too tight" → placed at taker price (paid taker)
    stuck_cancels:   int = 0             # cancel didn't confirm → force-cleanup (anomaly)
    # price drift over the window (first/last placement price)
    first_price: float = 0.0
    last_price: float = 0.0
    # max adverse excursion (MAE): the worst placement price vs the most-favourable
    # one this session, on the side that hurts. Sell-off wants HIGH prices → MAE =
    # how far BELOW the best (max) placement the worst (min) fell. Buy-in mirrors.
    # This quantifies the notes-MD's #1 real-money exposure (refuge bails via
    # `sank below N` then chases the book DOWN with no floor) — `drift` (first→last)
    # missed it because a sell-off that dips 14% mid-window then recovers looks flat.
    min_place_price: float = 0.0
    max_place_price: float = 0.0
    # spread context (sampled): fraction of samples where spread ≤ ~1 tick
    spread_samples: int = 0
    tight_spread_samples: int = 0
    # time-at-top sampler (split by posture so we can judge normal vs refuge)
    samples: int = 0                     # fresh-book samples where we had an order on this leg
    at_top_samples: int = 0              # of those, held the correct spot (L1 / 2nd-best)
    normal_samples: int = 0              # samples while NOT in refuge
    normal_at_top: int = 0               # of those, at L1
    refuge_samples: int = 0              # samples while in refuge
    refuge_held_2nd: int = 0             # of those, at 2nd-best (held-2nd-best-thru-drift %)
    last_streak: int = 0
    # correctness violations (human-readable, capped) + positive checks observed
    violations: List[str] = field(default_factory=list)
    checks_seen: Set[str] = field(default_factory=set)   # which spec behaviours were exercised

    def flag(self, msg: str):
        if len(self.violations) < 12 and msg not in self.violations:
            self.violations.append(msg)

    def mae_pct(self) -> Optional[float]:
        """Max adverse excursion as a % — how far the placement price travelled in
        the UNfavourable direction across the session. Sell-off: chased DOWN from
        the best (max) to the worst (min) → (max−min)/max. Buy-in: chased UP →
        (max−min)/min. Always ≥0; None if <2 distinct placements. Quantifies how
        far down/up the book PB followed an undercutter (the price-floor exposure)."""
        lo, hi = self.min_place_price, self.max_place_price
        if lo <= 0 or hi <= 0 or hi == lo:
            return None
        span = hi - lo
        if self.side == "sell":
            return span / hi * 100.0      # dropped this far below the high
        elif self.side == "buy":
            return span / lo * 100.0      # rose this far above the low
        return None


audit: Dict[str, LegAudit] = {}          # key = "ASSET@exchange" (same as books)
audit_start: float = 0.0
_asset_venue: Dict[str, str] = {}        # asset → the venue PB's live order is on (from cancels)
_global_stuck_cancels: int = 0           # stuck-cancel force-cleanups (no asset in the line)


def _audit_leg(asset: str, exchange: str) -> Optional[LegAudit]:
    if not exchange:
        return None
    key = f"{asset}@{exchange}"
    a = audit.get(key)
    if a is None and key in books:
        b = books[key]
        a = audit[key] = LegAudit(asset=asset, exchange=exchange, side=b.side)
    return a


def _refuge_xcheck(a: "LegAudit", book: Optional["LocalOrderBook"], fresh: bool,
                   tk: float, wall: float, jumper: float):
    """INDEPENDENT verification that PB's LOGGED (wall, jumper) actually describe the
    live book front — i.e. PB identified the right levels, not just placed
    consistently with its own (possibly wrong) read.

    Sparse-book-safe by construction: we match PB's logged prices to ACTUAL book
    levels by price proximity and check ORDERING, never level-rank counts (the thing
    that false-flagged correct re-parks on MANYU@htx's irregular grid).
    For a sell (mirror for buy — bids descending, "more aggressive" = higher):
      (1) jumper is genuinely L1: no foreign ask is MATERIALLY cheaper than the
          logged jumper;
      (2) the logged jumper matches the real best foreign ask;
      (3) the logged wall matches the next DISTINCT foreign level above the jumper.
    Records ok/bad/noob on the leg; flags a human-readable mismatch on failure.
    Tolerant: if the book is stale or has <2 foreign levels we can't judge → noob
    (no penalty), exactly like the placement-on-grid check.

    TWO tolerances, deliberately distinct (conflating them false-flagged a benign
    1-tick read-vs-snapshot drift in live testing):
      • level_tol (tight, ½ tick) — for STRUCTURE: collapsing duplicate levels and
        skipping our own resting order. About ladder shape.
      • match_tol (loose, ≥3 ticks AND a real % gap) — for MATCHING PB's logged
        price to a real level. PB reads the book then logs/places ~ms-to-seconds
        later; on a hot book the front legitimately moves a tick or three between
        PB's read and our snapshot. We mirror the placement-on-grid convention
        (≤3 ticks = still a match; only a sustained, >0.3% deviation is a real
        mis-ID). So only a foreign order sitting MATERIALLY ahead of PB's jumper —
        a genuine "PB picked the wrong front" — flags; a 1-tick boundary race does
        not.
    """
    if book is None or not fresh:
        a.refuge_xcheck_noob += 1
        return
    levels = book.asks if a.side == "sell" else book.bids
    if not levels:
        a.refuge_xcheck_noob += 1
        return
    ref = jumper if jumper > 0 else (book.best_ask or book.best_bid or 1.0)
    level_tol = (tk * 0.5) if tk > 0 else (ref * 1e-9)
    # latency-tolerant price-match window: ≥3 ticks, and at least a real price gap
    # so a sparse-book min-tick can't make it absurdly tight (mirrors the on-grid
    # check's dual gate). A mismatch must exceed BOTH to flag.
    match_tol = max((tk * 3.0) if tk > 0 else 0.0, ref * 0.003)

    # Build the FOREIGN ladder: skip our own resting order's level once (at re-park
    # our just-cancelled order may still be in the book — PB's own walk skips it).
    own_price = book.our_order[1] if book.our_order else None
    foreign: List[float] = []
    skipped_own = False
    for lv in levels:
        if (own_price is not None and not skipped_own
                and abs(lv.price - own_price) <= level_tol):
            skipped_own = True
            continue
        # collapse duplicate/near-equal prices into distinct levels
        if not foreign or abs(lv.price - foreign[-1]) > level_tol:
            foreign.append(lv.price)
        if len(foreign) >= 6:
            break
    if len(foreign) < 2:
        a.refuge_xcheck_noob += 1
        return

    best_foreign = foreign[0]          # real L1 foreign
    second_foreign = foreign[1]        # real 2nd foreign (the true wall)

    if a.side == "sell":
        # (1) nothing MATERIALLY cheaper than PB's jumper (jumper really is the front)
        none_cheaper = best_foreign >= jumper - match_tol
        # (2) logged jumper ≈ real best foreign (latency-tolerant)
        jumper_matches = abs(jumper - best_foreign) <= match_tol
        # (3) logged wall ≈ real 2nd foreign (latency-tolerant)
        wall_matches = abs(wall - second_foreign) <= match_tol
    else:
        none_cheaper = best_foreign <= jumper + match_tol    # nothing higher than jumper
        jumper_matches = abs(jumper - best_foreign) <= match_tol
        wall_matches = abs(wall - second_foreign) <= match_tol

    if none_cheaper and jumper_matches and wall_matches:
        a.refuge_xcheck_ok += 1
        a.checks_seen.add("refuge-wall/jumper-match-book")
    else:
        a.refuge_xcheck_bad += 1
        why = []
        if not none_cheaper:
            why.append(f"a foreign level ({fmt_price(best_foreign)}) is ahead of PB's "
                       f"jumper {fmt_price(jumper)}")
        elif not jumper_matches:
            why.append(f"PB's jumper {fmt_price(jumper)} ≠ real L1 {fmt_price(best_foreign)}")
        if not wall_matches:
            why.append(f"PB's wall {fmt_price(wall)} ≠ real 2nd level {fmt_price(second_foreign)}")
        a.flag("refuge wall/jumper mis-ID vs live book: " + "; ".join(why))


def audit_record_event(ev: "LogEvent"):
    """Fold one PB log event into the per-leg audit, reading the live book for
    OB-grounded correctness + performance metrics."""
    global _global_stuck_cancels
    # stuck-cancel carries no asset (PB logs only the order id/side) → global count.
    if ev.kind == "stuck_cancel":
        _global_stuck_cancels += 1
        return
    if not ev.asset:
        return
    # Resolve the venue. PB has ONE live order per asset, on ONE venue at a time.
    # Priority:
    #   1. Explicit venue on the line (cancel/backstop say `on <exch>`) — authoritative;
    #      stamp it as the asset's current PB venue (_asset_venue).
    #   2. Last venue a cancel named (_asset_venue) — PB just cancelled there, so its
    #      next venue-less place/refuge is almost certainly the same venue.
    #   3. (PLACE only, no venue known yet — e.g. before the first cancel in the
    #      window) the leg whose live book price is CLOSEST to the placed price.
    #      The price reveals the venue → fixes the "BILL place before first cancel →
    #      mis-bucketed to bitget → 3746 ticks off" artifact. Stamp it too.
    #   4. Last resort → freshest leg.
    if ev.exchange:
        exch = ev.exchange
        _asset_venue[ev.asset] = ev.exchange
    elif _asset_venue.get(ev.asset):
        exch = _asset_venue[ev.asset]
    elif ev.kind == "place" and ev.price > 0:
        bk = nearest_price_leg(ev.asset, ev.price)
        exch = bk.exchange if bk else ""
        if exch:
            _asset_venue[ev.asset] = exch
    else:
        bk = primary_book_for_asset(ev.asset)
        exch = bk.exchange if bk else ""
    a = _audit_leg(ev.asset, exch)
    if a is None:
        return
    book = books.get(f"{a.asset}@{a.exchange}")
    fresh = book is not None and book.age_ms < stale_threshold_ms(a.exchange)
    tk = book.tick() if book else 0.0

    if ev.kind == "place":
        a.places += 1
        if a.first_price == 0.0:
            a.first_price = ev.price
        a.last_price = ev.price
        if ev.price > 0:
            a.min_place_price = ev.price if a.min_place_price == 0.0 else min(a.min_place_price, ev.price)
            a.max_place_price = max(a.max_place_price, ev.price)
        # REFUGE RE-PARK correctness — verified against PB's OWN stated wall/jumper
        # (from the preceding "refuge placement" line, stashed in _refuge_target),
        # NOT re-derived from book rank. PB's spec is "place at wall∓1tick, between
        # the jumper and the wall"; on a sparse/irregular book (MANYU htx: level
        # gaps swing 2e-12…4e-11) "wall−1tick by price" and "2nd among populated
        # levels" legitimately diverge, so counting book levels would false-flag a
        # correct re-park. We check the placement landed in the [jumper, wall] zone.
        if a._expect_refuge_place:
            a._expect_refuge_place = False
            tgt = a._refuge_target            # (wall, jumper) from the refuge line
            if tgt and ev.price > 0:
                wall, jumper = tgt
                if a.side == "sell":
                    # park strictly below the wall, at/above the jumper (just under wall)
                    ok = jumper - 1e-18 <= ev.price <= wall
                else:
                    ok = wall <= ev.price <= jumper + 1e-18
                a.refuge_sink_samples.append(0 if ok else 1)   # 0=correct, 1=mis-parked
                if not ok:
                    a.flag(f"refuge re-park landed {fmt_price(ev.price)} OUTSIDE the "
                           f"[jumper {fmt_price(jumper)}, wall {fmt_price(wall)}] zone "
                           f"— not wall∓1tick 2nd-best")
            a._refuge_target = None
        # CORRECTNESS: was it placed near top∓1tick? Judge only on a fresh book,
        # and tolerantly: PB reads the book then places ~ms-to-seconds later, so in
        # a hot undercut/frontrun war the top legitimately moves a few ticks between
        # PB's read and our snapshot. So "near top" (≤3 ticks) = correct; only a
        # sustained, large deviation (>8 ticks) is a real anomaly. On a sparse book
        # (irregular level gaps) tick() is the *smallest* gap, so "ticks off" can
        # overstate — we therefore only flag when ALSO >0.3% off in price terms.
        if a.in_refuge:
            pass  # refuge placement judged on the refuge line instead
        elif fresh and tk > 0 and ev.price > 0:
            top = book.best_ask if a.side == "sell" else book.best_bid
            if top:
                off_ticks = abs(ev.price - top) / tk
                off_pct = abs(ev.price - top) / top
                if off_ticks <= 3.0:
                    a.place_correct += 1
                    a.checks_seen.add("placement-on-grid")
                else:
                    a.place_offbook += 1
                    # require BOTH a large tick-count AND a real price gap, so a
                    # sparse-book min-tick doesn't inflate a benign placement into ⚠
                    if off_ticks > 8.0 and off_pct > 0.003:
                        a.flag(f"PLACE {off_pct*100:.1f}% off best "
                               f"({fmt_price(ev.price)} vs top {fmt_price(top)}) — investigate")
        else:
            a.place_no_ob += 1

    elif ev.kind == "cancel":
        a.cancels += 1
        # bucket the reason to its head word (undercut/frontrun/better market/…)
        head = _reason_head(ev.reason)
        a.cancel_reasons[head] = a.cancel_reasons.get(head, 0) + 1
        a.streak_peak = max(a.streak_peak, ev.streak)
        # fill inference: streak dropping back to 1 (after having climbed) = a fill reset
        if ev.streak == 1 and a.last_streak > 1:
            a.fills += 1
        a.last_streak = ev.streak
        # undercut depth (ticks the competitor came inside us) — from the line itself
        um = UNDERCUT_RE.search(ev.reason)
        if um and tk > 0:
            their, ours = float(um.group(1)), float(um.group(2))
            a.undercut_depths.append(abs(ours - their) / tk)
        # CORRECTNESS: step-up must never fire while in refuge
        if head == "step-up" and a.in_refuge:
            a.flag("step-up fired WHILE IN REFUGE (should be suppressed)")

    elif ev.kind == "refuge":
        if ev.reason == "ARMED":
            a.refuge_arms += 1
            a.in_refuge = True
            a.refuge_armed_at = time.time()
            m = REFUGE_ARM_RE.search(ev.raw)
            if m and int(m.group(1)) < REFUGE_ARM_STREAK:
                a.flag(f"refuge ARMED at streak {m.group(1)} "
                       f"(<{REFUGE_ARM_STREAK:.0f} — premature)")
            else:
                a.checks_seen.add("refuge-arm@streak")
        elif ev.reason == "EXITED":
            if a.in_refuge and a.refuge_armed_at:
                a.refuge_secs += time.time() - a.refuge_armed_at
            a.in_refuge = False
            m = REFUGE_EXIT_RE.search(ev.raw)
            reason = m.group(1) if m else "?"
            # normalise "sank below N orders" → "sank below N"
            reason = re.sub(r"sank below (\d+) orders", r"sank below \1+", reason)
            a.refuge_exits[reason] = a.refuge_exits.get(reason, 0) + 1
            a.checks_seen.add("refuge-exit")
        elif ev.reason == "placement":
            a.refuge_replacements += 1
            # CORRECTNESS: wall must be above jumper (sell); placement = wall∓1tick.
            m = REFUGE_PLACE_RE.search(ev.raw)
            if m:
                wall, jumper = float(m.group(1)), float(m.group(2))
                ok = (wall > jumper) if a.side == "sell" else (wall < jumper)
                if ok:
                    a.checks_seen.add("refuge-park-under-wall")
                else:
                    rel = "above jumper" if a.side == "sell" else "below frontrunner"
                    a.flag(f"refuge wall {fmt_price(wall)} NOT {rel} {fmt_price(jumper)}")
                # stash PB's stated target — the NEXT place is the re-park; we
                # verify it landed in the [jumper, wall] zone (PB's own definition
                # of 2nd-best), immune to sparse-book level-rank divergence.
                a._refuge_target = (wall, jumper)
                # INDEPENDENT cross-check of PB's wall/jumper against the LIVE book.
                # The zone check above trusts PB's own numbers; this one asks whether
                # those numbers actually describe the real book front. Sparse-book-
                # safe: we match logged prices to ACTUAL levels by proximity (a tick
                # tolerance), never by level-rank counting (which false-flags on
                # MANYU@htx's irregular grid). We verify three things on the foreign
                # ladder (skipping our own just-cancelled level once):
                #   (1) the jumper is genuinely L1 — no foreign order is cheaper
                #       (sell) / higher (buy) than PB's logged jumper;
                #   (2) PB's logged jumper price matches the real best foreign level;
                #   (3) PB's logged wall price matches the NEXT distinct foreign
                #       level above the jumper.
                _refuge_xcheck(a, book, fresh, tk, wall, jumper)
            a._expect_refuge_place = True

    elif ev.kind == "backstop":
        a.backstops += 1
        a.checks_seen.add("backstop-fired")

    elif ev.kind == "taker_fallback":
        # maker price would have crossed → PB placed at the taker price. A real cost
        # event for a maker-only strategy (paid taker, not maker rebate).
        a.taker_fallbacks += 1

    # step-up count + gap correctness (cancel reason carries the gap)
    if ev.kind == "cancel" and _reason_head(ev.reason) == "step-up":
        a.stepups += 1
        sm = STEPUP_RE.search(ev.reason)
        if sm and float(sm.group(1)) < STEP_UP_MIN_GAP_TICKS:
            a.flag(f"step-up at {sm.group(1)} ticks "
                   f"(<{STEP_UP_MIN_GAP_TICKS:.0f} — below STEP_UP_MIN_GAP_TICKS)")
        else:
            a.checks_seen.add("step-up-gap≥5t")


def _reason_head(reason: str) -> str:
    r = reason.strip().lower()
    for head in ("undercut", "frontrun", "step-up", "better market", "large gap",
                 "refuge", "mode disabled", "periodic"):
        if r.startswith(head):
            return head
    return r.split(" ")[0] if r else "?"


def audit_sample():
    """Continuous sampler (called every ~1s): for each leg with a fresh book and a
    tracked order, record whether we hold the correct spot (top for normal, 2nd-best
    under the wall for refuge) + spread tightness. This is the time-at-top measure."""
    for key, book in books.items():
        a = audit.get(key)
        if a is None:
            b = books[key]
            a = audit[key] = LegAudit(asset=b.asset, exchange=b.exchange, side=b.side)
        if book.age_ms >= stale_threshold_ms(book.exchange):
            continue
        # spread context (a market property — sample on any active leg)
        if book.best_bid and book.best_ask:
            a.spread_samples += 1
            tk = book.tick()
            if tk > 0 and (book.best_ask - book.best_bid) <= tk * 1.5:
                a.tight_spread_samples += 1
        # time-at-top: ONLY on the leg PB's live order is actually on. our_order is
        # mirrored onto all of an asset's legs by the log-tailer, so without this
        # gate the OTHER venues of a multi-venue asset would count as "0% at top".
        active_venue = _asset_venue.get(a.asset, "")
        if active_venue and active_venue != a.exchange:
            continue
        if not book.our_order:
            continue
        a.samples += 1
        cnt = book.our_levels_from_top()
        if cnt is None:
            continue
        if a.in_refuge:
            # 2nd-best = exactly one foreign order ahead. held-2nd-best% over the
            # hold is a positioning/fill signal (it drops when we lag a moving
            # market — expected, not a fault); the re-park CORRECTNESS check lives
            # at the place event (refuge_sink_samples).
            a.refuge_samples += 1
            if cnt == 1:
                a.refuge_held_2nd += 1
                a.at_top_samples += 1
        else:
            # correct normal spot = at top (no foreign order more aggressive)
            a.normal_samples += 1
            if cnt == 0:
                a.normal_at_top += 1
                a.at_top_samples += 1


async def _audit_sampler():
    while True:
        await asyncio.sleep(1.0)
        try:
            audit_sample()
        except Exception:
            pass


def _feed_status_line() -> str:
    """One-line per-exchange feed health (update count + freshest book age), for
    the sanity heartbeat. Groups books by exchange; shows the freshest leg's age
    so a quiet-but-alive feed reads fresh."""
    by_ex: Dict[str, List[LocalOrderBook]] = {}
    for b in books.values():
        by_ex.setdefault(b.exchange, []).append(b)
    parts = []
    for ex in sorted(by_ex):
        bks = by_ex[ex]
        upd = sum(b.update_count for b in bks)
        # freshest leg's age (min age = most recently updated)
        age = min((b.age_ms for b in bks), default=9999.0)
        thr = stale_threshold_ms(ex)
        age_s = f"{age/1000:.1f}s" if age < thr else "STALE"
        parts.append(f"{ex} {upd}u/{age_s}")
    return "  ·  ".join(parts) if parts else "(no feeds)"


async def confirm_first_snapshots(per_exchange: Dict[str, List[str]], timeout: float = 25.0):
    """After connect, wait until every exchange has delivered ≥1 book update (or
    timeout), then print a one-line per-exchange confirmation. Makes the sanity
    check instant: you see immediately whether each venue's data is flowing."""
    print("Confirming first snapshots per venue…")
    t0 = time.time()
    pending = set(per_exchange)
    while pending and time.time() - t0 < timeout:
        for ex in list(pending):
            if any(b.update_count > 0 for b in books.values() if b.exchange == ex):
                pending.discard(ex)
        if not pending:
            break
        await asyncio.sleep(0.5)
    for ex in sorted(per_exchange):
        got = sum(b.update_count for b in books.values() if b.exchange == ex)
        tag = "custom" if ex == "htx" else ("proxy" if ex in PROXY_EXCHANGES else "direct")
        mark = "✓" if got > 0 else "⚠ NO DATA"
        print(f"  {mark:>9}  {ex:9s} [{tag:6s}] → {got} updates")
    if pending:
        print(f"  ⚠ no data yet from: {sorted(pending)} "
              f"(check the tunnel for proxy/custom venues, or it's just slow)")


async def _audit_heartbeat(interval: float = 60.0):
    """Periodic feed-health line in audit mode (so a sanity check can read data-
    flow from the file without waiting for the final report)."""
    t0 = time.time()
    while True:
        await asyncio.sleep(interval)
        mins = (time.time() - t0) / 60.0
        print(f"[heartbeat t={mins:.0f}m]  {_feed_status_line()}", flush=True)


def _pct(n: int, d: int) -> str:
    return f"{100.0*n/d:.0f}%" if d else "—"


def _stats(xs: List[float]) -> str:
    if not xs:
        return "n/a"
    xs2 = sorted(xs)
    med = xs2[len(xs2)//2]
    return f"med {med:.1f}, max {max(xs2):.1f} (n={len(xs2)})"


def _active(a: "LegAudit") -> bool:
    return bool(a.places or a.cancels or a.refuge_arms or a.samples)




def print_summary(elapsed: float):
    """Post-period audit. PART A = CORRECTNESS of the PB workflow (does it follow
    its spec, checked against the live book). PART B = REAL-WORLD PERFORMANCE in
    the OB context (how viable, where to focus). Human-readable + scrutinizing."""
    mins = elapsed / 60.0
    legs = [k for k in sorted(audit) if _active(audit[k])]
    quiet = [k for k in sorted(audit) if not _active(audit[k])]

    tot_plc = sum(a.places for a in audit.values())
    tot_cxl = sum(a.cancels for a in audit.values())
    tot_ref = sum(a.refuge_arms for a in audit.values())
    tot_taker = sum(a.taker_fallbacks for a in audit.values())
    print("\n" + "=" * 100)
    print(f"  POSITION BALANCER AUDIT   ·   {mins:.1f} min window   ·   "
          f"{len(legs)} active / {len(audit)} total legs   ·   grounded in live local orderbooks")
    print(f"  Captured live (every PB event correlated with the book at that instant): "
          f"{tot_plc} places · {tot_cxl} cancels · {tot_ref} refuge episodes.")
    # Secondary cost/anomaly events (maker-only strategy crossing the spread, or a
    # cancel that had to be force-cleaned). 0 is the healthy expectation.
    if tot_taker or _global_stuck_cancels:
        bits = []
        if tot_taker:
            bits.append(f"⚠ {tot_taker} taker-fallback place(s) (maker would cross → paid taker)")
        if _global_stuck_cancels:
            bits.append(f"⚠ {_global_stuck_cancels} stuck-cancel force-cleanup(s)")
        print("  " + " · ".join(bits))
    print("=" * 100)
    if not legs:
        print("\n  No PB decisions captured in the window. Either the balancer is idle, or run longer.")
        print("=" * 100)
        return

    # ══════════════════════════════════════════════════════════════════════════
    # PART A — CORRECTNESS  (is the PB workflow behaving per its spec?)
    # ══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 100)
    print("  PART A — WORKFLOW CORRECTNESS   (does PB follow its spec, verified against the live book?)")
    print("─" * 100)
    print("  Per leg: spec behaviours exercised+passed (✓), and any violations (⚠).\n")

    total_viol = 0
    CHECK_LABEL = {
        "placement-on-grid":    "placement on-grid (top∓1t)",
        "refuge-arm@streak":    f"refuge arms @streak≥{REFUGE_ARM_STREAK:.0f}",
        "refuge-park-under-wall": "refuge parks under wall",
        "refuge-exit":          "refuge exits cleanly",
        "refuge-wall/jumper-match-book": "refuge wall/jumper match live book",
        "step-up-gap≥5t":       "step-up gap≥5t",
        "backstop-fired":       "backstop fired",
    }
    for key in legs:
        a = audit[key]
        # positive checks observed this window
        seen = [CHECK_LABEL[c] for c in
                ("placement-on-grid", "refuge-arm@streak", "refuge-park-under-wall",
                 "refuge-wall/jumper-match-book", "refuge-exit", "step-up-gap≥5t",
                 "backstop-fired") if c in a.checks_seen]
        # REFUGE RE-PARK correctness (the real "sink" check): of the re-parks
        # measured against the live book, how many landed us correctly 2nd-best
        # (exactly 1 order ahead)? A re-park leaving ≥2 ahead is flagged in the
        # place branch — that's the genuine "5-min update didn't move us to 2nd-
        # best" failure. Market drift away from a *correctly* parked order is NOT
        # counted (we measure at the re-park, not continuously).
        if a.refuge_sink_samples:
            ok2 = sum(1 for c in a.refuge_sink_samples if c == 0)
            seen.append(f"refuge re-parked 2nd-best {ok2}/{len(a.refuge_sink_samples)}")
        # INDEPENDENT cross-check: PB's logged wall/jumper vs the live book front
        xj = a.refuge_xcheck_ok + a.refuge_xcheck_bad
        if xj:
            seen.append(f"wall/jumper vs book {a.refuge_xcheck_ok}/{xj}")
        plc_judged = a.place_correct + a.place_offbook
        if plc_judged:
            seen.append(f"placed-on-grid {_pct(a.place_correct, plc_judged)} (n={plc_judged})")

        status = "✓" if not a.violations else f"⚠ {len(a.violations)}"
        print(f"  {status:>4}  {key:18s}  {(' · '.join(seen)) if seen else 'no spec-checkable decisions'}")
        for v in a.violations:
            total_viol += 1
            print(f"          ⚠ {v}")

    print()
    if total_viol == 0:
        print("  VERDICT: ✓ workflow CORRECT — every PB decision in the window matched its spec")
        print(f"  (placements on-grid, refuge armed@streak≥{REFUGE_ARM_STREAK:.0f} & parked under the wall & exited on a")
        print("   valid condition, step-ups gap≥5t & never in refuge, backstop behaving).")
    else:
        print(f"  VERDICT: ⚠ {total_viol} spec deviation(s) flagged above — investigate before trusting perf numbers.")

    # ══════════════════════════════════════════════════════════════════════════
    # PART B — REAL-WORLD PERFORMANCE  (how viable is the logic against the book?)
    # ══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 100)
    print("  PART B — REAL-WORLD PERFORMANCE   (how the PB logic fares against the live orderbook)")
    print("─" * 100)
    print(f"  {'leg':18s} {'side':4s} {'plc':>4s} {'cxl':>4s} {'cxl/m':>5s} {'fill':>4s} "
          f"{'top%':>5s} {'refuge':>8s} {'undercut(t)':>11s} {'tight':>5s} {'MAE%':>6s}")
    print("  " + "-" * 96)
    for key in legs:
        a = audit[key]
        side = "BUY" if a.side == "buy" else "SELL" if a.side == "sell" else "?"
        top = _pct(a.at_top_samples, a.samples)
        cxl_min = f"{a.cancels/mins:.1f}" if mins > 0 else "—"
        refuge = (f"{a.refuge_secs/60:.1f}m" if a.refuge_secs
                  else ("in" if a.in_refuge else "—"))
        ucd = f"{sorted(a.undercut_depths)[len(a.undercut_depths)//2]:.0f}" if a.undercut_depths else "—"
        tight = _pct(a.tight_spread_samples, a.spread_samples)
        mae = a.mae_pct()
        mae_s = f"{mae:.2f}%" if mae is not None else "—"
        print(f"  {key:18s} {side:4s} {a.places:>4d} {a.cancels:>4d} {cxl_min:>5s} {a.fills:>4d} "
              f"{top:>5s} {refuge:>8s} {ucd:>11s} {tight:>5s} {mae_s:>6s}")
    print("\n  Columns: top% = of fresh-book samples we held the correct spot (L1, or 2nd-best in")
    print("  refuge) ON the venue PB is using · cxl/m = cancel rate · undercut(t) = median ticks a")
    print("  competitor came inside us · tight = % of time spread ≤1 tick · MAE% = max adverse")
    print("  excursion: how far placements travelled the WRONG way (sell: below the session high /")
    print("  buy: above the low) — the price-floor exposure (refuge bails, then chases the book down).")

    # refuge effectiveness detail (only legs that entered refuge)
    ref_legs = [k for k in legs if audit[k].refuge_arms]
    if ref_legs:
        print("\n  Refuge 5-min-check outcomes (the crucial part — each backstop must")
        print("  re-park us 2nd-best OR cleanly exit; what happens mid-hold doesn't matter):")
        for key in ref_legs:
            a = audit[key]
            nrep = len(a.refuge_sink_samples)
            ok2 = sum(1 for c in a.refuge_sink_samples if c == 0)
            reparks = (f"{ok2}/{nrep} landed 2nd-best" if nrep else "no re-parks measured")
            exits = ", ".join(f"{k} {v}" for k, v in sorted(a.refuge_exits.items(), key=lambda x: -x[1])) or "none yet"
            verdict = "✓" if nrep == ok2 else f"⚠ {nrep-ok2} mis-parked"
            print(f"    {verdict} {key}: {a.refuge_arms} episode(s) · re-parks: {reparks} · "
                  f"exits: {exits}")

    # cancel-reason mix (what's driving churn)
    print("\n  Cancel-reason mix:")
    for key in legs:
        a = audit[key]
        if not a.cancel_reasons:
            continue
        br = ", ".join(f"{k} {v}" for k, v in sorted(a.cancel_reasons.items(), key=lambda x: -x[1]))
        print(f"    {key}: {br}  (streak-peak {a.streak_peak})")

    # ── FOCUS AREAS (prioritised, actionable) ─────────────────────────────────
    focus = _focus_areas(mins)
    print("\n  FOCUS AREAS (where to improve viability — most severe first):")
    if focus:
        for sev, msg in focus:
            print(f"    {sev} {msg}")
    else:
        print("    • Nothing flagged — legs are either competitive or quietly waiting.")

    if quiet:
        print(f"\n  (Quiet legs, no PB activity in window: {', '.join(quiet)})")
    print("=" * 100)


def _focus_areas(mins: float) -> List[Tuple[str, str]]:
    """Prioritised, OB-grounded tuning leads. Returns (severity-marker, message),
    sorted most-severe first. Severity: ‼ high, • medium."""
    out = []  # (rank, marker, msg)
    for key in sorted(audit):
        a = audit[key]
        if not _active(a):
            continue
        ucd_med = (sorted(a.undercut_depths)[len(a.undercut_depths)//2]
                   if a.undercut_depths else 0)
        # 0. price-floor exposure: large max-adverse-excursion. The notes-MD #1
        #    real-money risk — PB chased the book far the wrong way (refuge bails
        #    via `sank below N`, then chases down with no floor). Ranks ABOVE the
        #    L1 fight (it's a realized-loss signal, not just a positioning one).
        mae = a.mae_pct()
        if mae is not None and mae >= 5.0:
            out.append((-1, "‼", f"{key}: placements travelled {mae:.1f}% the wrong way (max adverse "
                        f"excursion, {a.side}-side) — chased the book {'down' if a.side=='sell' else 'up'} "
                        f"with no price floor. A per-session floor / taker punch-through would cap this."))
        # 1. losing the L1 fight: low top% + heavy churn. Threshold relaxed from
        #    samples>30 to >12 so a thin-but-contested leg (e.g. MANYU@mexc n=12)
        #    isn't silently exempt; still requires real churn (>10 cancels).
        if a.samples > 12 and a.at_top_samples / a.samples < 0.4 and a.cancels > 10:
            out.append((0, "‼", f"{key}: held the correct spot only {_pct(a.at_top_samples, a.samples)} "
                        f"despite {a.cancels} cancels ({a.cancels/mins:.1f}/min) — losing the L1 fight. "
                        f"Arm refuge sooner (lower REFUGE_ARM_STREAK) or chase slower."))
        # 2. undercut war, zero fills = no payoff
        uc = a.cancel_reasons.get("undercut", 0) + a.cancel_reasons.get("frontrun", 0)
        if a.cancels > 12 and a.fills == 0 and uc > 8:
            armed = "refuge engaged" if a.refuge_arms else "refuge NEVER armed"
            out.append((1, "‼", f"{key}: {a.cancels} cancels, 0 fills — pure undercut war, no payoff "
                        f"({armed}). If unarmed, REFUGE_ARM_STREAK too high; if armed+still churning, "
                        f"the chase before arming is the cost."))
        # 3. repeated sank-deep refuge exits = book stacks faster than refuge holds
        sank = sum(v for k, v in a.refuge_exits.items() if k.startswith("sank"))
        if sank >= 2:
            out.append((2, "•", f"{key}: {sank} 'sank below N' refuge exits — multiple jumpers stack "
                        f"below us, refuge's 2nd-best premise breaks. A hard price floor / taker "
                        f"punch-through would cap the loss here."))
        # 4. deep undercuts = competitor jumping far inside, not 1-tick penny
        if ucd_med >= 5 and a.undercut_depths and len(a.undercut_depths) >= 4:
            out.append((3, "•", f"{key}: median undercut is {ucd_med:.0f} ticks deep (not a 1-tick "
                        f"penny-jumper) — chasing it concedes real price; consider holding/refuge."))
        # 5. tight-spread churn = repricing captures ~nothing
        if a.spread_samples > 40 and a.tight_spread_samples / a.spread_samples > 0.8 and a.cancels > 10:
            out.append((4, "•", f"{key}: spread ≤1 tick {_pct(a.tight_spread_samples, a.spread_samples)} "
                        f"of the time — repricing captures almost nothing; widen chase cadence to cut churn."))
        # 6. net directional move against us — complements #0 (MAE/peak). Only
        #    fires when MAE didn't already flag this leg (avoid a near-duplicate
        #    line): a steady adverse grind whose peak excursion stayed <5%.
        if a.first_price and a.last_price and a.first_price > 0 and not (mae is not None and mae >= 5.0):
            d = (a.last_price - a.first_price) / a.first_price * 100
            adverse = d < -3 if a.side == "sell" else d > 3
            if adverse:
                out.append((5, "•", f"{key}: net price moved {d:+.1f}% against the {a.side}-side over the "
                            f"window — fills weren't captured before the move; check fill-pressure / cadence."))
    out.sort(key=lambda x: x[0])
    return [(m, msg) for _, m, msg in out]


# ─── Renderer ────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"
BLUE   = "\033[94m"


def fmt_price(p: Optional[float]) -> str:
    """8 significant figures — magnitude-robust (matches the PB log :.8g fix).

    Fixed-decimal (:.Nf) collapses sub-decimal assets (MANYU ~5.4e-9 → all
    levels render identical). Sig-figs show distinct levels at any magnitude.
    """
    if p is None:
        return "    ----    "
    return f"{p:.8g}"


def _min_tick(book: "LocalOrderBook") -> float:
    """Smallest gap between adjacent ask levels (proxy for exchange tick)."""
    asks = book.asks
    diffs = [abs(asks[i+1].price - asks[i].price)
             for i in range(min(len(asks) - 1, 5))
             if abs(asks[i+1].price - asks[i].price) > 1e-15]
    return min(diffs) if diffs else 0.0


def render():
    lines = []
    lines.append(f"{BOLD}{'─'*96}{RESET}")
    lines.append(f"{BOLD}  Position Balancer Live Monitor{RESET}  "
                 f"{DIM}{time.strftime('%H:%M:%S UTC', time.gmtime())}  "
                 f"· {len(books)} legs{RESET}")
    lines.append(f"{BOLD}{'─'*96}{RESET}")

    # ── Orderbook panels (one per asset×exchange leg) ─────────────────────
    for key, book in books.items():
        age = book.age_ms
        thr = stale_threshold_ms(book.exchange)
        age_str = f"{age:5.0f}ms" if age < thr else f"{RED}STALE{RESET}"
        side_tag = (f"{GREEN}BUY-IN{RESET}" if book.side == "buy"
                    else f"{RED}SELL-OFF{RESET}" if book.side == "sell" else "")

        header = (f"{BOLD}{CYAN}{book.label:18s}{RESET} {side_tag:18s} "
                  f"{DIM}upd={book.update_count:<6d} age={age_str}{RESET}")
        lines.append(header)

        bids, asks = book.top_levels(DEPTH)
        our = book.our_order
        asks_rev = list(reversed(asks))   # lowest ask nearest the mid (bottom)
        rows = max(len(asks_rev), len(bids))

        for i in range(rows):
            ask_lv = asks_rev[i] if i < len(asks_rev) else None
            bid_lv = bids[i]     if i < len(bids)     else None

            if ask_lv:
                is_ours = (our and our[0] == "sell" and
                           abs(ask_lv.price - our[1]) < ask_lv.price * 1e-6)
                marker = f"  {YELLOW}◄ OUR ORDER{RESET}" if is_ours else ""
                ask_part = (f"{RED}{fmt_price(ask_lv.price):>14s}  "
                            f"{ask_lv.qty:>16.4g}{RESET}{marker}")
            else:
                ask_part = ""

            if bid_lv:
                is_ours = (our and our[0] == "buy" and
                           abs(bid_lv.price - our[1]) < bid_lv.price * 1e-6)
                marker = f"  {YELLOW}◄ OUR ORDER{RESET}" if is_ours else ""
                bid_part = (f"{GREEN}{fmt_price(bid_lv.price):>14s}  "
                            f"{bid_lv.qty:>16.4g}{RESET}{marker}")
            else:
                bid_part = ""

            if i == len(asks_rev) - 1:
                spread = None
                if book.best_bid and book.best_ask:
                    spread = (book.best_ask - book.best_bid) / book.best_bid * 100
                sep = f"  {DIM}spread {spread:.3f}%{RESET}" if spread is not None else ""
                lines.append(f"   {ask_part}{sep}")
                lines.append(f"   {DIM}{'─'*48}{RESET}")
                lines.append(f"   {bid_part}")
            elif i < len(asks_rev) - 1:
                lines.append(f"   {ask_part}")
            else:
                lines.append(f"   {bid_part}")

        lines.append("")

    # ── Recent log events ─────────────────────────────────────────────────
    lines.append(f"{BOLD}{'─'*96}{RESET}")
    lines.append(f"{BOLD}  Recent Events{RESET}  {DIM}(newest first){RESET}")
    lines.append(f"{BOLD}{'─'*96}{RESET}")

    shown = 0
    for ev in log_lines:
        if shown >= 28:
            break
        book = primary_book_for_asset(ev.asset, ev.exchange) if ev.asset else None

        if ev.kind == "cancel":
            anomaly = ""
            if ev.ob_best_ask and ev.side == "sell" and ev.price > 0 and book and book.asks:
                tick = _min_tick(book)
                gap = abs(ev.price - ev.ob_best_ask)
                if tick > 0 and gap > tick * 2.5:
                    anomaly = (f"  {RED}⚠ STALE OB AT CANCEL: our={fmt_price(ev.price)} "
                               f"ask={fmt_price(ev.ob_best_ask)} gap={fmt_price(gap)}{RESET}")
            ob_info = ""
            if ev.ob_best_ask and ev.side == "sell":
                ob_info = f"  {DIM}[ob_ask={fmt_price(ev.ob_best_ask)} age={ev.ob_age_ms:.0f}ms]{RESET}"
            elif ev.ob_best_bid and ev.side == "buy":
                ob_info = f"  {DIM}[ob_bid={fmt_price(ev.ob_best_bid)} age={ev.ob_age_ms:.0f}ms]{RESET}"
            streak_col = (f"{YELLOW}streak={ev.streak}{RESET}" if ev.streak >= 5
                          else f"streak={ev.streak}")
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {RED}CANCEL {ev.side.upper():4s}{RESET}  "
                f"{CYAN}{ev.asset:12s}{RESET}  {ev.exchange:8s}  {streak_col:20s}  "
                f"{DIM}{ev.reason[:44]}{RESET}{ob_info}{anomaly}"
            )

        elif ev.kind == "place":
            anomaly = ""
            if ev.side == "sell" and ev.ob_best_ask and ev.price > 0:
                gap = ev.price - ev.ob_best_ask
                if gap > ev.ob_best_ask * 0.002:
                    anomaly = (f"  {RED}⚠ {gap/ev.ob_best_ask*100:.2f}% ABOVE ASK "
                               f"(age={ev.ob_age_ms:.0f}ms){RESET}")
            elif ev.side == "buy" and ev.ob_best_bid and ev.price > 0:
                gap = ev.ob_best_bid - ev.price
                if gap > ev.ob_best_bid * 0.002:
                    anomaly = (f"  {RED}⚠ {gap/ev.ob_best_bid*100:.2f}% BELOW BID "
                               f"(age={ev.ob_age_ms:.0f}ms){RESET}")
            ob_ref = ev.ob_best_ask if ev.side == "sell" else ev.ob_best_bid
            ob_info = (f"  {DIM}[ob_ref={fmt_price(ob_ref)} age={ev.ob_age_ms:.0f}ms]{RESET}"
                       if ob_ref else "")
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {GREEN}PLACE  {ev.side.upper():4s}{RESET}  "
                f"{CYAN}{ev.asset:12s}{RESET}  {'':8s}  price={fmt_price(ev.price):14s}  "
                f"qty={ev.qty:<12.4g}{ob_info}{anomaly}"
            )

        elif ev.kind == "refuge":
            colour = YELLOW
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {colour}REFUGE{RESET} {ev.side.upper():4s}  "
                f"{CYAN}{ev.asset:12s}{RESET}  {'':8s}  "
                f"{colour}{ev.reason}{RESET}  {DIM}{ev.raw.split(' - ')[-1][:50] if ' - ' in ev.raw else ev.raw[-50:]}{RESET}"
            )

        elif ev.kind == "backstop":
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {BLUE}BACKSTOP{RESET}      "
                f"{'':12s}  {ev.exchange:8s}  {DIM}5-min refresh (CANCEL sent){RESET}"
            )

        elif ev.kind == "arb_cancel":
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {YELLOW}TIMEOUT{RESET}       "
                f"{CYAN}{ev.asset:12s}{RESET}  {'':8s}  "
                f"{DIM}arb order_timeout cancel (re-placed next tick){RESET}"
            )

        elif ev.kind == "buyin_check":
            ob_ask = book.best_ask if book else None
            gap_str = ""
            if ob_ask and ev.price > 0:
                gap_pct = (ob_ask - ev.price) / ob_ask * 100
                gap_str = (f"  {RED}⚠ bid={fmt_price(ev.price)} ask={fmt_price(ob_ask)} "
                           f"gap={gap_pct:.2f}%{RESET}" if abs(gap_pct) > 0.15
                           else f"  {DIM}ask={fmt_price(ob_ask)}{RESET}")
            lines.append(
                f"  {DIM}{ev.ts[11:]}{RESET}  {BLUE}BUY-IN{RESET}        "
                f"{CYAN}{ev.asset:12s}{RESET}  {'':8s}  {DIM}{ev.reason}{RESET}{gap_str}"
            )

        else:
            lines.append(f"  {DIM}{ev.ts[11:]}{RESET}  {DIM}{ev.raw[40:120]}{RESET}")

        shown += 1

    sys.stdout.write("\033[2J\033[H")
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


async def _periodic_render():
    while True:
        await asyncio.sleep(2)
        render()


# ─── Main ────────────────────────────────────────────────────────────────────

async def main(log_path: str, window: int, manual: Optional[str], proxy: Optional[str],
               duration: int, live: bool):
    # Quiet the adapter loggers — they're chatty at INFO/DEBUG and would corrupt
    # the dashboard. Only show their warnings/errors.
    logging.basicConfig(level=logging.WARNING)
    for noisy in ("ws_book_checker", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # 0. Install the per-exchange SOCKS5 proxy (only PROXY_EXCHANGES route through
    #    the tunnel; everything else direct). proxy=="" → forced off; proxy set →
    #    use it; proxy is None → auto-detect the default tunnel.
    if proxy == "":
        print(f"Proxy: OFF (--no-proxy) — all direct. {sorted(PROXY_EXCHANGES)} "
              f"may fail from a throttled ISP.")
    else:
        purl = proxy or DEFAULT_SOCKS
        if _socks_port_open(purl):
            if enable_socks_proxy(purl):
                print(f"Proxy: {sorted(PROXY_EXCHANGES)} → {purl} (VPS path); "
                      f"all other venues direct.")
        elif proxy:
            print(f"Proxy: {purl} requested but not reachable — all direct.")
        else:
            print(f"Proxy: {DEFAULT_SOCKS} not detected — all direct. "
                  f"(Start `ssh -D 1080 fin` so {sorted(PROXY_EXCHANGES)} work.)")

    # 1. Discover legs: logs → active assets + side; YAML → all markets per asset.
    if manual:
        legs = parse_manual_pairs(manual)
        print(f"Using manual pairs: {legs}")
    else:
        print(f"Discovering active PB assets from {SSH_HOST} logs (last {window} "
              f"PB events) + their markets from test_multi.yml …")
        legs = await discover_pairs(log_path, window)

    if not legs:
        print("No active PB assets found. Is the balancer running / log path right?")
        return

    skipped = build_books(legs)
    if skipped:
        print(f"  (skipped — no adapter / no YAML markets: {skipped})")

    # 2. Group symbols per exchange for adapter instantiation (dedup per venue —
    #    several assets can share one exchange).
    per_exchange: Dict[str, List[str]] = {}
    for book in books.values():
        sym = f"{book.asset}USDT"
        syms = per_exchange.setdefault(book.exchange, [])
        if sym not in syms:
            syms.append(sym)

    # Show the discovered asset → venues map (so the multi-venue coverage is clear).
    from collections import defaultdict as _dd
    asset_venues = _dd(list)
    for book in books.values():
        asset_venues[book.asset].append(book.exchange)
    print("\nActive PB assets → venues (both legs of multi-venue assets):")
    for asset in sorted(asset_venues):
        bk = next(b for b in books.values() if b.asset == asset)
        side = "BUY-IN" if bk.side == "buy" else "SELL-OFF" if bk.side == "sell" else "?"
        print(f"  {asset:8s} [{side:8s}] → {sorted(set(asset_venues[asset]))}")
    print("\nFeeds per exchange:")
    for exch, syms in per_exchange.items():
        tag = "custom" if exch == "htx" else ("proxy" if exch in PROXY_EXCHANGES else "direct")
        print(f"  {exch:9s} [{tag:6s}] → {', '.join(syms)}")
    print(f"\nTailing log via SSH: {SSH_HOST}:{log_path}")
    print("Connecting feeds (ws_book_checker adapters, parallel)…")

    # 3. Connect all feeds the ws_book_checker way (build → seed → batch-connect
    #    → await kucoin calibration). The adapters' background _maintain_connection
    #    tasks self-sustain + auto-reconnect for the life of the process.
    await setup_adapters(per_exchange)

    # 3b. Sanity: confirm each venue delivered a first snapshot before we commit to
    #     the run (so we don't "wait on nothing" if a feed is dead).
    await confirm_first_snapshots(per_exchange)

    global LIVE_MODE, audit_start
    LIVE_MODE = live
    audit_start = time.time()

    # 4. Long-lived tasks: log-tail + OB sampler (both modes), + render/heartbeat.
    tasks = [
        asyncio.create_task(tail_log_remote(log_path)),
        asyncio.create_task(_audit_sampler()),
    ]
    if live:
        tasks.append(asyncio.create_task(_periodic_render()))
        print("Live dashboard — Ctrl+C to stop (prints the audit summary on exit).\n")
        await asyncio.sleep(1.0)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            print_summary(time.time() - audit_start)
    else:
        # Audit-summary mode: monitor for `duration` seconds, then report + exit.
        # A heartbeat line every 60s makes data-flow readable mid-run (sanity).
        tasks.append(asyncio.create_task(_audit_heartbeat(60.0)))
        print(f"Feeds connected. Auditing for {duration}s "
              f"({duration/60:.1f} min) — grounding every PB event in the live books "
              f"(heartbeat every 60s)…\n")
        try:
            await asyncio.sleep(duration)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.sleep(0.2)
            print_summary(time.time() - audit_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Position Balancer audit/monitor — grounds PB log events in "
                    "live local orderbooks of each active asset:exchange.")
    parser.add_argument("--duration", type=int, default=600,
                        help="Audit-summary mode: monitor N seconds, then print the "
                             "report and exit (default 600 = 10 min)")
    parser.add_argument("--live", action="store_true",
                        help="Live dashboard instead of summary (prints summary on Ctrl+C)")
    parser.add_argument("--log", default=DEFAULT_LOG, help="Remote log path (on myserver)")
    parser.add_argument("--depth", type=int, default=5, help="OB levels to show (live mode)")
    parser.add_argument("--window", type=int, default=600,
                        help="Trailing log lines to scan for asset discovery")
    parser.add_argument("--pairs", default=None,
                        help='Manual override, e.g. "MANYU/kucoin:sell,NATIX/mexc"')
    parser.add_argument("--proxy", default=None,
                        help=f"SOCKS5/HTTP proxy for exchange WS (default auto-detect "
                             f"{DEFAULT_SOCKS}). e.g. socks5://127.0.0.1:1080")
    parser.add_argument("--no-proxy", action="store_true",
                        help="Force direct connections (no proxy)")
    args = parser.parse_args()
    DEPTH = args.depth
    proxy_arg = "" if args.no_proxy else args.proxy  # "" = forced off, None = auto

    try:
        asyncio.run(main(args.log, args.window, args.pairs, proxy_arg,
                         args.duration, args.live))
    except KeyboardInterrupt:
        print("\nStopped.")
