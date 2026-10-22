"""P2-DEX (2026-10-14; the EVM core since 2026-10-22): the pools of the CAs P2 trades, as P1 handed them over, for a DEX
connector on any EVM chain. The chain's facts come from its spec (`C`): P1's name for the chain (P1_CHAIN), where its payloads
are (DATA_DIR), its DEX families (FAMILY: quote route -> family), the route kinds an official router can execute
(ROUTED_KINDS) and its executors (EXECUTORS, through evm_dex_swap.no_executor).

P1 tracks every pool, and records them with each opportunity it catches, exactly as for CEX-CEX (Pavel 2026-10-14):
ws_book_checker ends a CEX<->DEX arb.log line with ` dex={...}`, the CA's records as it priced them: {chain, ca, ticker,
decimals, reference_usd, written, pools}, where `pools` are P1's records of that CA's valid pools (route, key, counter and
its quoted path to USDT `counter.hops`, taxes). The lanes on brr_ws carry it on the page record, and the watcher on
myserver writes it to `<DATA_DIR>/<ca>.json` from every page that carries it. A strategy's leg names its CA
(`control create … bsc:MUBARAK-USDT:0x5c85…`), and the orchestrator binds the pair to it (bind_ca). P1 decides which pools
are valid; P2 decides which is best each poll (Q7). Once an asset is traded arb.log drops its lines, like any CEX-CEX asset,
so its file stays as it was at the create.

This module reads those files: per CA the pools P2 can trade, each with its DEX family and its counter's hop candidates inside
that family; the pools left out with their reason, never silently (no router: decision 2; no hop inside its family: one
router call chains one family only); and a per-CA signature that changes when P1's pools for the CA change (the re-election
trigger). No network, no web3. The connector (evm_dex_exchange.EvmDexExchange) reads it every poll."""
import glob
import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from hummingbot.connector.utilities.evm_dex.evm_dex_swap import no_executor

STALE_AFTER_S = 6 * 3600.0     # a payload whose P1 stamp is older is counted as stale in summary() (information only)
ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")


def family_of(C, pool: dict) -> str:
    address = ((pool.get("route") or {}).get("address") or "").lower()
    return C.FAMILY.get(address) or f"route:{address}"


def check_payload(C, doc) -> Optional[str]:
    """why a page's `dex` payload can't be used, or None"""
    if not isinstance(doc, dict):
        return f"a {type(doc).__name__}, not an object"
    if doc.get("chain") != C.P1_CHAIN:
        return f"chain {doc.get('chain')!r}, expected {C.P1_CHAIN}"
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


def hop_path(C, pool: dict, family: str) -> Tuple[List[List[dict]], Optional[int]]:
    """the counter's links to USDT in path order, each cut to the candidates of `family`
    -> (path, the first link without one, or None). A USDT pool has no link."""
    path, gap = [], None
    for i, link in enumerate((pool.get("counter") or {}).get("hops") or []):
        same = [c for c in link.get("pools") or [] if family_of(C, c) == family]
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
    """The CAs P2 trades, one file each (`<data_dir>/<ca>.json`, a page's `dex` payload; the spec's DATA_DIR unless given).
    load() re-reads only the files that changed and drops the CAs whose file is gone; a file that fails check_payload() is
    left out with its reason."""

    def __init__(self, C, data_dir: Optional[str] = None):
        self.C = C
        self.data_dir = data_dir or C.DATA_DIR
        self.mtimes: Dict[str, float] = {}            # path -> mtime last read
        self.cas: Dict[str, dict] = {}                # ca -> {"ticker", "written", "pools": [tradable, annotated]}
        self.left_out: Dict[Tuple[str, str], str] = {}  # (pool, ca) -> why; a pool pairing two of our CAs is under both
        self.bad_files: Dict[str, str] = {}           # path -> why it was not read
        self.sig: Dict[str, str] = {}                 # ca -> signature (the re-election trigger)

    def load(self) -> List[str]:
        """-> the CAs whose pools changed (new, rewritten with a new signature, or gone)"""
        C = self.C
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
                problem = check_payload(C, doc)
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
                if kind not in C.ROUTED_KINDS:
                    self.left_out[(p.get("pool"), ca)] = f"no router: {kind}"
                    continue
                fam = family_of(C, p)
                why = no_executor(C, dict(p, family=fam))           # step 4: a pool P2 can't swap is no book pool
                if why:
                    self.left_out[(p.get("pool"), ca)] = why
                    continue
                path_, gap = hop_path(C, p, fam)
                path_ = [[c for c in link if not no_executor(C, dict(c, family=fam))] for link in path_]   # executable hops only
                gap = next((i for i, link in enumerate(path_) if not link), gap)
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
