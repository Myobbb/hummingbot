"""P2-DEX (2026-10-14): the pools of the CAs P2 trades, as P1 handed them over, for the `bsc` connector.

P1 tracks every pool. Its downstream lanes pick the worthy candidates and page them, and the watcher on myserver acts on the
page, exactly as for CEX-CEX (Pavel 2026-10-14). A page with a DEX leg carries `dex`, copied from P1's `dex_pools.json` at
page time: {chain, ca, ticker, decimals, reference_usd, written, pools}, where `pools` are P1's records of that CA's valid pools
(route, key, counter and its quoted path to USDT `counter.hops`, taxes). The watcher writes that payload to `~/p2_dex/<ca>.json`
before it types the `control create` line, and any later page for the CA rewrites it. P1 decides which pools are valid; P2
decides which is best each poll (Q7).

This module reads those files: per CA the pools P2 can trade, each with its DEX family and its counter's hop candidates inside
that family; the pools left out with their reason, never silently (no router: decision 2; no hop inside its family: one
router call chains one family only); and a per-CA signature that changes when P1's pools for the CA change (the re-election
trigger). No network, no web3. The connector (bsc_exchange.BscExchange, step 3) reads it every poll."""
import glob
import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

DATA_DIR = os.path.expanduser("~/p2_dex")
CHAIN = "BSC"
STALE_AFTER_S = 6 * 3600.0     # WIP: a payload whose P1 stamp is older is stated as stale (pages refresh it)

# The quote routes an official router can execute. P1's own-model pools (routerless pairs, unlisted deployments) have no
# router, so they stay out for now (decision 2).
ROUTED_KINDS = frozenset(("v2 router", "QuoterV2", "official quoter", "Algebra quoter"))

# A pool's DEX family, from its quote route. One router call chains pools of one family only, so a counter's hop has to be
# found inside its pool's family (Q5). PancakeSwap's and Uniswap's Universal Routers each chain their own v2, v3 and
# v4 / Infinity pools; any other route (a fork's v2 router, another v3 deployment's quoter, an Algebra quoter) is a family
# of its own. The routers themselves are pinned and checked on-chain in step 4.
FAMILY = {
    "0x10ed43c718714eb63d5aa57b78b54704e256024e": "pancakeswap",   # PancakeSwap v2 router
    "0xb048bbc1ee6b733fffcfb9e9cef7375518e25997": "pancakeswap",   # PancakeSwap v3 QuoterV2
    "0xd0737c9762912dd34c3271197e362aa736df0926": "pancakeswap",   # PancakeSwap Infinity CL quoter
    "0xc631f4b0fc2dd68ad45f74b2942628db117dd359": "pancakeswap",   # PancakeSwap Infinity Bin quoter
    "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24": "uniswap",       # Uniswap v2 router
    "0x78d78e420da98ad378d7799be8f4af69033eb077": "uniswap",       # Uniswap v3 QuoterV2
    "0x9f75dd27d6664c475b90e105573e550ff69437b0": "uniswap",       # Uniswap v4 quoter
}
ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")


def family_of(pool: dict) -> str:
    address = ((pool.get("route") or {}).get("address") or "").lower()
    return FAMILY.get(address) or f"route:{address}"


def check_payload(doc) -> Optional[str]:
    """why a page's `dex` payload can't be used, or None"""
    if not isinstance(doc, dict):
        return f"a {type(doc).__name__}, not an object"
    if doc.get("chain") != CHAIN:
        return f"chain {doc.get('chain')!r}, expected {CHAIN}"
    if not ADDRESS.match(str(doc.get("ca") or "").lower()):
        return f"ca {doc.get('ca')!r} is not an address"
    if not doc.get("ticker"):
        return "no ticker"
    if not isinstance(doc.get("decimals"), int) or doc["decimals"] < 0:
        return f"decimals {doc.get('decimals')!r}"
    if not isinstance(doc.get("reference_usd"), (int, float)) or doc["reference_usd"] <= 0:
        return f"reference_usd {doc.get('reference_usd')!r}"
    if not isinstance(doc.get("pools"), list):
        return "no pool list"
    return None


def hop_path(pool: dict, family: str) -> Tuple[List[List[dict]], Optional[int]]:
    """the counter's links to USDT in path order, each cut to the candidates of `family`
    -> (path, the first link without one, or None). A USDT pool has no link."""
    path, gap = [], None
    for i, link in enumerate((pool.get("counter") or {}).get("hops") or []):
        same = [c for c in link.get("pools") or [] if family_of(c) == family]
        path.append(same)
        if not same and gap is None:
            gap = i
    return path, gap


def signature(pools: List[dict]) -> str:
    """changes when P1's tradable pools for a CA change: pool, route, key, counter, taxes, hop candidates"""
    h = hashlib.sha1()
    for p in sorted(pools, key=lambda x: x.get("pool") or ""):
        h.update(json.dumps([p.get("pool"), p.get("type"), (p.get("route") or {}).get("address"), p.get("key"),
                             (p.get("counter") or {}).get("token"), p.get("tax_pct"), p.get("counter_tax_pct"),
                             [[c.get("pool") for c in link] for link in p["hop_path"]]],
                            sort_keys=True, default=str).encode())
    return h.hexdigest()[:16]


class PoolList:
    """The CAs P2 trades, one file each (`<DATA_DIR>/<ca>.json`, a page's `dex` payload). load() re-reads only the files that
    changed and drops the CAs whose file is gone; a file that fails check_payload() is left out with its reason."""

    def __init__(self, data_dir: str = DATA_DIR):
        self.data_dir = data_dir
        self.mtimes: Dict[str, float] = {}            # path -> mtime last read
        self.cas: Dict[str, dict] = {}                # ca -> {"ticker", "written", "pools": [tradable, annotated]}
        self.left_out: Dict[Tuple[str, str], str] = {}  # (pool, ca) -> why; a pool pairing two of our CAs is under both
        self.bad_files: Dict[str, str] = {}           # path -> why it was not read
        self.sig: Dict[str, str] = {}                 # ca -> signature (the re-election trigger)

    def load(self) -> List[str]:
        """-> the CAs whose pools changed (new, rewritten with a new signature, or gone)"""
        changed, seen = [], set()
        for path in sorted(glob.glob(os.path.join(self.data_dir, "0x*.json"))):
            seen.add(path)
            mtime = os.stat(path).st_mtime
            if self.mtimes.get(path) == mtime:
                continue
            self.mtimes[path] = mtime
            try:
                with open(path) as f:
                    doc = json.load(f)
                problem = check_payload(doc)
            except (OSError, ValueError) as e:
                problem = f"{type(e).__name__}: {str(e)[:120]}"
            if problem:
                self.bad_files[path] = problem
                continue
            self.bad_files.pop(path, None)
            ca = doc["ca"].lower()
            tradable = []
            for key in [k for k in self.left_out if k[1] == ca]:
                del self.left_out[key]
            for p in doc["pools"]:
                kind = (p.get("route") or {}).get("kind")
                if kind not in ROUTED_KINDS:
                    self.left_out[(p.get("pool"), ca)] = f"no router: {kind}"
                    continue
                fam = family_of(p)
                path_, gap = hop_path(p, fam)
                if gap is not None:
                    self.left_out[(p.get("pool"), ca)] = f"no hop inside its family ({fam}) for link {gap + 1}"
                    continue
                tradable.append(dict(p, ca=ca, family=fam, hop_path=path_))
            sig = signature(tradable)
            if self.sig.get(ca) != sig:
                changed.append(ca)
            self.cas[ca] = {"ticker": str(doc["ticker"]).upper(), "decimals": doc["decimals"],
                            "reference_usd": float(doc["reference_usd"]), "written": doc.get("written"), "pools": tradable}
            self.sig[ca] = sig
        for path in [p for p in self.mtimes if p not in seen]:
            del self.mtimes[path]
            ca = os.path.basename(path)[:-len(".json")].lower()
            if self.cas.pop(ca, None) is not None:
                self.sig.pop(ca, None)
                changed.append(ca)
        return changed

    def pools(self, ca: str) -> List[dict]:
        return (self.cas.get(ca.lower()) or {}).get("pools") or []

    def ca_for(self, ticker: str) -> Tuple[Optional[str], str]:
        """the CA behind a trading pair's base (P1's ticker) -> (ca, "") or (None, why)"""
        found = [ca for ca, m in self.cas.items() if m["ticker"] == ticker.upper()]
        if len(found) == 1:
            return found[0], ""
        return None, ("no payload for it" if not found else f"{len(found)} CAs share it")

    def age_s(self, ca: str) -> Optional[float]:
        written = (self.cas.get(ca.lower()) or {}).get("written")
        if not written:
            return None
        stamp = datetime.strptime(written, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return time.time() - stamp.timestamp()

    def summary(self) -> dict:
        tradable = [p for m in self.cas.values() for p in m["pools"]]
        return {
            "cas": len(self.cas),
            "cas_without_tradable_pool": sum(1 for m in self.cas.values() if not m["pools"]),
            "tradable_pools": len(tradable),
            "by_family": dict(Counter(p["family"] for p in tradable).most_common()),
            "usdt_pools": sum(1 for p in tradable if not p["hop_path"]),
            "left_out": dict(Counter(why.split(" (")[0] for why in self.left_out.values())),
            "bad_files": dict(self.bad_files),
            "stale_cas": sum(1 for ca in self.cas if (self.age_s(ca) or 0.0) > STALE_AFTER_S),
        }
