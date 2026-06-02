#!/usr/bin/env python3
"""
Isolated bitmart local-orderbook robustness test.

Drives ONLY the ws_book_checker BitmartWS adapter exactly as pb_monitor.py does
(seed subscribed_symbols, dedup off, relaxed home-network timeouts, connect via
ConnectionManager so the bg _maintain_connection reconnect task runs), and
measures liveness over a fixed window:

  • total updates + updates/min                 (is data flowing?)
  • mean / p95 / MAX inter-update gap (s)        (staleness — the STALE symptom)
  • reconnect count + every ERROR/CRITICAL line  (the "No PONG" churn)

Three modes, to both test robustness AND isolate the cause:

  direct   — no proxy at all (baseline; what bitmart does when its hardcoded
             TCPConnector defeats pb_monitor's contextvar proxy patch — i.e. the
             ACTUAL behaviour during the 12h run despite the "Proxy: bitmart" banner)
  patched  — pb_monitor's exact mechanism: enable_socks_proxy + _use_proxy_ctx set.
             Expected to STILL be direct because BitmartWS hardcodes its connector
             (proves the bypass).
  forced   — monkeypatch the SOCKS5 ProxyConnector INTO BitmartWS.connect so it
             genuinely tunnels (the candidate fix). Expected: clean.

Usage:
  python3 scripts/bitmart_ob_test.py                       # all 3 modes, 120s each
  python3 scripts/bitmart_ob_test.py --duration 300        # 5 min each
  python3 scripts/bitmart_ob_test.py --modes forced        # just the fix
  python3 scripts/bitmart_ob_test.py --symbol IRUSDT
  python3 scripts/bitmart_ob_test.py --proxy socks5://127.0.0.1:1080
"""
import argparse
import asyncio
import contextvars
import logging
import socket
import statistics
import sys
import time

import aiohttp

WS_BOOK_PARENT = "/Users/pavel/Documents/VS_code_projects/Tracker_cex_cex"
if WS_BOOK_PARENT not in sys.path:
    sys.path.insert(0, WS_BOOK_PARENT)

from ws_book_checker.exchanges.bitmart import BitmartWS              # noqa: E402
from ws_book_checker.core.connection_manager import ConnectionManager  # noqa: E402

# ── proxy mechanism, copied verbatim from pb_monitor.py ───────────────────────
_proxy_url_holder = {"url": None}
_use_proxy_ctx = contextvars.ContextVar("pb_use_proxy", default=False)
_orig_session_init = aiohttp.ClientSession.__init__


def socks_port_open(url: str) -> bool:
    try:
        host, port = url.split("://", 1)[-1].split(":")
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except Exception:
        return False


def enable_socks_proxy(proxy_url: str) -> bool:
    """pb_monitor's patch: inject ProxyConnector when the ctx flag is set AND the
    caller didn't already pass a connector."""
    try:
        from aiohttp_socks import ProxyConnector
    except ImportError:
        sys.stderr.write("WARNING: aiohttp-socks not installed.\n")
        return False
    _proxy_url_holder["url"] = proxy_url

    def patched_init(self, *args, **kwargs):
        if _use_proxy_ctx.get() and kwargs.get("connector") is None:
            try:
                kwargs["connector"] = ProxyConnector.from_url(_proxy_url_holder["url"])
                kwargs.pop("trust_env", None)
            except Exception:
                pass
        return _orig_session_init(self, *args, **kwargs)

    aiohttp.ClientSession.__init__ = patched_init
    return True


def force_proxy_into_bitmart(proxy_url: str) -> bool:
    """Candidate FIX: wrap BitmartWS.connect so the ClientSession it builds with a
    hardcoded TCPConnector is instead given a SOCKS5 ProxyConnector. We do it by
    temporarily forcing ClientSession to ignore an incoming TCPConnector and use a
    ProxyConnector, but ONLY for the duration of BitmartWS.connect() (so we don't
    disturb anything else)."""
    try:
        from aiohttp_socks import ProxyConnector
    except ImportError:
        sys.stderr.write("WARNING: aiohttp-socks not installed.\n")
        return False

    orig_connect = BitmartWS.connect

    async def connect_via_proxy(self, *a, **kw):
        def forcing_init(s, *args, **kwargs):
            # Drop bitmart's TCPConnector, swap in a fresh ProxyConnector.
            kwargs.pop("connector", None)
            kwargs["connector"] = ProxyConnector.from_url(proxy_url)
            kwargs.pop("trust_env", None)
            return _orig_session_init(s, *args, **kwargs)

        aiohttp.ClientSession.__init__ = forcing_init
        try:
            return await orig_connect(self, *a, **kw)
        finally:
            aiohttp.ClientSession.__init__ = _orig_session_init

    BitmartWS.connect = connect_via_proxy
    return True


def restore_bitmart_connect(orig):
    BitmartWS.connect = orig


# ── measurement ───────────────────────────────────────────────────────────────
class Meter:
    def __init__(self):
        self.updates = 0
        self.last_ts = None
        self.gaps = []
        self.first = None
        self.tops = []  # (bid, ask) for a sanity check the book is real

    def on_ob_update(self, exchange, symbol, bids, asks, is_snapshot,
                     update_id=None, timestamp=None):
        now = time.monotonic()
        if self.first is None:
            self.first = now
        if self.last_ts is not None:
            self.gaps.append(now - self.last_ts)
        self.last_ts = now
        self.updates += 1
        try:
            b = bids[0][0] if bids else None
            a = asks[0][0] if asks else None
            if len(self.tops) < 3:
                self.tops.append((b, a))
        except Exception:
            pass


class ErrCounter(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.records = []

    def emit(self, record):
        if record.levelno >= logging.WARNING:
            msg = record.getMessage()
            self.records.append((record.levelname, msg))


def configure_adapter_like_monitor(adapter, symbols):
    """The three pb_monitor gotchas + home-network timeout relaxation."""
    adapter.subscribed_symbols = set(symbols)
    if hasattr(adapter, "_dedup_cache"):
        adapter._dedup_cache = None
    if getattr(adapter, "_last_top_bid", None) is None:
        adapter._last_top_bid = {}
    if getattr(adapter, "_last_top_ask", None) is None:
        adapter._last_top_ask = {}
    for attr, val in (("pong_timeout", 45),
                      ("inactivity_threshold", 180),
                      ("data_idle_threshold", 180)):
        if hasattr(adapter, attr):
            setattr(adapter, attr, val)


async def run_mode(mode: str, symbol: str, duration: int, proxy_url: str):
    print(f"\n{'='*70}\n  MODE: {mode}   ·   {symbol}   ·   {duration}s\n{'='*70}")
    meter = Meter()
    errc = ErrCounter()
    # capture the adapter's own logger (it logs 'No PONG'/reconnects there)
    root = logging.getLogger()
    root.addHandler(errc)
    root.setLevel(logging.INFO)

    orig_connect = BitmartWS.connect
    proxied_session = False

    if mode == "patched":
        ok = enable_socks_proxy(proxy_url)
        _use_proxy_ctx.set(ok)
        proxied_session = ok
    elif mode == "forced":
        force_proxy_into_bitmart(proxy_url)

    adapter = BitmartWS([symbol], meter.on_ob_update)
    configure_adapter_like_monitor(adapter, [symbol])

    cm = ConnectionManager()
    reconnections_before = getattr(adapter, "reconnections", 0)

    try:
        # _connect_exchange spawns the bg connect_with_reconnection task (snapshots ctx)
        await asyncio.wait_for(cm._connect_exchange(adapter), timeout=30)
    except Exception as e:
        print(f"  connect failed: {type(e).__name__}: {e}")

    t0 = time.monotonic()
    # heartbeat each 30s so we can watch a long run live
    next_hb = 30
    while time.monotonic() - t0 < duration:
        await asyncio.sleep(1)
        el = time.monotonic() - t0
        if el >= next_hb:
            age = (time.monotonic() - meter.last_ts) if meter.last_ts else None
            print(f"    [t={int(el):>3}s] updates={meter.updates:<5} "
                  f"last_age={age:.1f}s" if age is not None
                  else f"    [t={int(el):>3}s] updates=0 (no data yet)")
            next_hb += 30

    # teardown
    try:
        BitmartWS.connect = orig_connect
        # stop the bg reconnect loop first so it can't reopen sessions mid-teardown
        for attr in ("running", "_running", "should_run"):
            if hasattr(adapter, attr):
                try:
                    setattr(adapter, attr, False)
                except Exception:
                    pass
        if hasattr(adapter, "stop"):
            try:
                res = adapter.stop()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
        for ws in list(getattr(adapter, "websockets", {}).values()):
            try:
                await ws.close()
            except Exception:
                pass
        for s in list(getattr(adapter, "sessions", {}).values()):
            try:
                await s.close()
            except Exception:
                pass
        await asyncio.sleep(0.2)
    except Exception:
        pass
    root.removeHandler(errc)

    # report
    el = time.monotonic() - t0
    reconnects = getattr(adapter, "reconnections", 0) - reconnections_before
    g = meter.gaps
    print(f"\n  ── result [{mode}] ──")
    if proxied_session:
        print(f"    proxy ctx set: True  (but BitmartWS hardcodes its TCPConnector → "
              f"ClientSession patch is bypassed; effectively DIRECT)")
    print(f"    updates        : {meter.updates}  ({meter.updates/el*60:.1f}/min)")
    if g:
        gs = sorted(g)
        p95 = gs[min(len(gs)-1, int(len(gs)*0.95))]
        print(f"    inter-update   : mean {statistics.mean(g):.2f}s · "
              f"p95 {p95:.2f}s · MAX {max(g):.2f}s")
    else:
        print(f"    inter-update   : n/a (≤1 update)")
    print(f"    reconnects     : {reconnects}")
    err_lines = [m for lv, m in errc.records]
    no_pong = sum("No PONG" in m for m in err_lines)
    print(f"    WARN+ logs     : {len(errc.records)}  (No-PONG: {no_pong})")
    for lv, m in errc.records[:6]:
        print(f"        {lv}: {m[:90]}")
    if len(errc.records) > 6:
        print(f"        … +{len(errc.records)-6} more")
    print(f"    sample tops    : {meter.tops}")
    verdict = "HEALTHY" if (meter.updates > el/3 and reconnects == 0 and no_pong == 0) \
        else ("DEGRADED" if meter.updates > 0 else "DEAD")
    print(f"    VERDICT        : {verdict}")
    return {"mode": mode, "updates": meter.updates, "rate_min": meter.updates/el*60,
            "max_gap": max(g) if g else None, "reconnects": reconnects,
            "no_pong": no_pong, "verdict": verdict}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="IRUSDT", help="BASEUSDT form (default IRUSDT — the leg that flapped)")
    ap.add_argument("--duration", type=int, default=120, help="seconds per mode (default 120)")
    ap.add_argument("--modes", default="direct,patched,forced",
                    help="comma list of: direct,patched,forced")
    ap.add_argument("--proxy", default="socks5://127.0.0.1:1080")
    args = ap.parse_args()

    if "socks5" in args.proxy and not socks_port_open(args.proxy):
        print(f"WARNING: SOCKS5 port {args.proxy} not open — start `ssh -D 1080 fin`. "
              f"proxied modes will fail.")

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    results = []
    for m in modes:
        # reset proxy ctx between modes
        _use_proxy_ctx.set(False)
        aiohttp.ClientSession.__init__ = _orig_session_init
        results.append(await run_mode(m, args.symbol, args.duration, args.proxy))
        await asyncio.sleep(2)

    print(f"\n{'='*70}\n  SUMMARY ({args.symbol}, {args.duration}s/mode)\n{'='*70}")
    print(f"  {'mode':<9}{'updates':>9}{'/min':>8}{'max_gap':>9}{'recon':>7}{'noPONG':>8}  verdict")
    for r in results:
        mg = f"{r['max_gap']:.1f}s" if r['max_gap'] is not None else "n/a"
        print(f"  {r['mode']:<9}{r['updates']:>9}{r['rate_min']:>8.1f}{mg:>9}"
              f"{r['reconnects']:>7}{r['no_pong']:>8}  {r['verdict']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
