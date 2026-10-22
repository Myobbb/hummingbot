"""P1-DEX ① (2026-10-14): the DEX quote cycle for ws_book_checker's DEX shard. Chain-generic by name; BSC is the first chain.
Replaces core/dex_bsc.py (2026-01, kept for reference: wiki p1-dex-quoter / p1-dex-production §①).

Pavel 2026-10-13: "each second ws_book_checker fires a multicall to get $100 worth quotes both side for all pools in the groups,
building a 'top bid/ask' of $100 volume, if this synthetic bid/ask passes our minimum profitability and volume threshold, then
deeper levels are fetched for all pools of that CA, with the deepest/best potential arb getting reported".
10-14: the ladder ($250 ... $5k) to every pool of the CA, only after its $100 passed against a CEX, one result for all the CEXs it
passed against; "make good use of multicall, no busy-work or wasted resources, requests, PUs".

Input: the DEX members of the groups, as groups_updater writes them (P1-DEX ⓪c): per CA `dex_ca` + `dex_pools` (each pool's
type, tokens, route + address, key, fee, counter and the counter's USD source).
ONE request a second, always: one Multicall3 aggregate3 eth_call holding
  - getBlockNumber + getCurrentBlockTimestamp (the books carry their block);
  - every pool's $100 buy (exact OUTPUT of A CA) and sell (exact INPUT of A CA) through its route exactly as the workbench's 5a
    quotes them; own-model pools: v2 = its reserves at 4b's fee (no quote), CL = the pool's own swap via 5a's helper (state
    override);
  - the HOPS (D31, 2026-10-14, Pavel: "every pool is priced along its executable path to USDT: the CA pool plus the counter's
    hops"): each link of the counters' paths (⓪c: counter.hops; 0 links for USDT, 1 for a standard, 2 for an outside token or
    another of our CAs) has candidate pools, and every candidate is quoted at the same $100, both sides (exact input of the
    link's token -> the next, and of the next -> the token). The best per side is the hop ("picked the way P1 picks the CA's
    pool"), and a counter's USD to pay (buy) / to receive (sell) is its links' rates multiplied. No spot anywhere;
  - the deep ladder (LADDER_USD) for the (CA, side)s the engine asked for since the last call, only for the pools whose ladder
    is not already known for their current state. A pool's ladder is kept while its $100 quote (the same amount in, the same
    amount out) is unchanged: that pool's state has not moved, so its deeper levels have not either; DEEP_TTL bounds the rest
    (a liquidity change away from the price that leaves the $100 quote alone). So a trigger costs no extra request, a cross
    that persists on a quiet pool costs no extra quotes, and one ladder serves every CEX the CA crossed. The hops of those
    CAs' counters are quoted at every rung, every cycle while their ladder is in use (DEEP_TTL): the hop pools move every block.
-> per CA the best ask and the best bid in USD across its pools (a one-level book of A), and on request per pool a multi-level
book (each ladder step's increment at its marginal price, the counter converted at that rung's hop) for the engine's own
matched-volume walk against the CEX book.
A (the CA amount) starts at ⓪'s reference price and follows the cycle's own mid; the counters' input amounts (a route that
refuses exact output buys by exact input of the counter, as 5a) and every hop's amounts follow their path's USD (the mid of buy
and sell); all re-size only when the market moved > RESIZE, so a quiet pool's $100 quote stays byte-identical and its ladder
stays valid.
ABI encoding is written out (static words; the v4 / Infinity PoolKey with empty hookData) and proven byte-equal to eth_abi on
brr_ws (dex/probes/p_dex_quoter_equal.py). No web3, no eth_abi (4cc's venv has neither); the transport is the caller's (post),
so the node's URL and key never enter this module."""
import json
import time

try:
    import orjson
    _dumps, _loads = orjson.dumps, orjson.loads
except ImportError:                                       # (brr_ws's system Python)
    _dumps, _loads = (lambda o: json.dumps(o).encode()), json.loads

CHAINS = {"BSC": {"multicall3": "0xcA11bde05977b3631167028862bE2a173976CA11",
                  "v4_stateview": "0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4",
                  "inf_cl_manager": "0xa0ffb9c1ce1fe56963b0321b32e7a0302114058b",
                  "anchor": "0x55d398326f99059ff775485246999027b3197955",            # USDT: every path ends here
                  "wrapped": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"},           # WBNB = native BNB (wrapping costs gas only)
          # 2026-10-21 (P1-DEX ETH): the chain spec's values (dex/lib/chains/eth.py, verified on-chain 46 / 46); no Infinity on ETH
          "ETH": {"multicall3": "0xcA11bde05977b3631167028862bE2a173976CA11",
                  "v4_stateview": "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
                  "inf_cl_manager": None,
                  "anchor": "0xdac17f958d2ee523a2206206994597c13d831ec7",            # USDT (6 decimals on ETH): every path ends here
                  "wrapped": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}}           # WETH = native ETH
NATIVE = "0x0000000000000000000000000000000000000000"
LADDER_USD = (100, 250, 500, 1000, 2000, 3000, 5000)      # Pavel 10-14 ($100 again: every step from the ladder's own block)
RESIZE = 0.05                                             # A and the counters' input amounts re-size when the price moved > 5%
DEEP_TTL = 60.0                                           # s: a ladder kept on an unchanged $100 quote is re-quoted after this
DEEP_MAX_QUOTES = 600                                     # ladder quotes per call (the rest wait for the next one)
FAILING_RETRY_S = 60.0                                    # D36 (Pavel 10-21): a pool failing $100 both ways is re-tried after this
HELPER = "0x0000000000000000000000000000000000005a5a"     # 5a's own-model CL helper, injected by state override
# entry (caller != the pool stored in slot 0): sstore(0, pool = calldata[0:20]); call pool with calldata[20:]; return its
# returndata. callback (caller == that pool): revert(calldata[4:68]) = (amount0Delta, amount1Delta). The same bytes as 5a's.
_CODE = ("6000" "54" "33" "14" "6035" "57"                         # 0x00 if caller == sload(0) -> 0x35
         "6000" "35" "6060" "1c" "80" "6000" "55"                  # 0x08 pool = calldataload(0) >> 96; sstore(0, pool)
         "6014" "36" "03" "80" "6014" "6000" "37"                  # 0x12 size = cds - 20; calldatacopy(0, 20, size)
         "6000" "6000" "82" "6000" "6000" "86" "5a" "f1"           # 0x1c call(gas, pool, 0, 0, size, 0, 0)
         "50" "50" "50" "3d" "6000" "6000" "3e" "3d" "6000" "f3"   # 0x28 return(returndata)
         "5b" "6040" "6004" "6000" "37" "6040" "6000" "fd")        # 0x35 callback: revert(calldata[4:68])
assert len(_CODE) // 2 == 66 and _CODE[0x35 * 2: 0x35 * 2 + 2] == "5b"
HELPER_CODE = "0x" + _CODE
OVERRIDE = {HELPER: {"code": HELPER_CODE}}
MIN_SQ, MAX_SQ = 4295128740, 1461446703485210103287273052203988822378723970341       # TickMath MIN + 1 / MAX - 1
SEL = {k: bytes.fromhex(v) for k, v in {
    "agg3": "82ad56cb", "block": "42cbb15c", "ts": "0f28c97d",
    "qv2_in": "c6a5026a", "qv2_out": "bd21704a", "alg_in": "2d9ebd1d", "alg_out": "9e73c81d",
    "amounts_out": "d06ca61f", "amounts_in": "1f00ca74", "amount_out": "f140a35a", "amount_in": "1125f13f",
    "v4_in": "aa9d21cb", "v4_out": "58733073", "inf_in": "9938b8ed", "inf_out": "30f42259",
    "reserves": "0902f1ac", "slot0": "3850c7bd", "gstate": "e76c01e4", "getslot0": "c815641c",
    "token0": "0dfe1681", "swap": "128acb08",
    "dy_i": "5e0d443f", "dy_u": "556d6e9f",                 # (P1-DEX v10) Curve get_dy(int128,int128,uint256) · get_dy(uint256,uint256,uint256)
    # (P1-DEX v11) Balancer: v2 BalancerQueries.querySwap(SingleSwap, FundManagement) · v3 Router querySwapSingleTokenExactIn / ExactOut
    # (pool, tokenIn, tokenOut, amount, sender, userData) · the v3 Vault's quoteAndRevert(bytes) · its revert Result(bytes)
    "bq_swap": "e969f6b3", "b3_in": "3ebc54e5", "b3_out": "175d4408", "qar": "757d64b3", "result": "5ab64fb8"}.items()}
EXACT_OUTPUT_UNSUPPORTED = bytes.fromhex("f7ebfa18")


def w(v):
    """one 32-byte ABI word (two's complement for a negative int)"""
    return (int(v) % (1 << 256)).to_bytes(32, "big")


def adr(a):
    return bytes(12) + bytes.fromhex(a[2:])


def b32(h):
    """a bytes32 given as hex: left-aligned, as eth_abi pads bytesN"""
    return bytes.fromhex(h[2:].ljust(64, "0"))


def word(b, i, signed=False):
    v = int.from_bytes(b[32 * i: 32 * i + 32], "big")
    return v - (1 << 256) if signed and v >> 255 else v


def own_v2(p):
    """an own-model v2 pool: quoted from its reserves (5a: own model AND type v2), no quote call"""
    return p["route"]["kind"] == "own model" and p["type"] == "v2"


# ------------------------------------------------------------------ Multicall3 aggregate3: the workbench's lib/bsc.py, verbatim
_W = lambda v: v.to_bytes(32, "big")


def enc_agg3(chunk):
    """aggregate3 calldata for [(target, callData)], every call allowFailure = true (lib/bsc.py: byte-identical to eth_abi)"""
    els = [b"".join((bytes(12), bytes.fromhex(t[2:]), _W(1), _W(0x60), _W(len(c)), bytes(c), bytes(-len(c) % 32))) for t, c in chunk]
    offs, pos = [], 32 * len(els)
    for e in els: offs.append(_W(pos)); pos += len(e)
    return SEL["agg3"] + _W(0x20) + _W(len(els)) + b"".join(offs) + b"".join(els)


def dec_agg3(b, n_calls):
    """aggregate3's answer -> [(ok, bytes)]; raises on a short or cut answer or another number of results (lib/bsc.py)"""
    def wd(at):
        if at + 32 > len(b): raise ValueError(f"aggregate3 answer cut at byte {at} of {len(b)}")
        return int.from_bytes(b[at: at + 32], "big")
    i0 = wd(0); n = wd(i0); base = i0 + 32; out = []
    if n != n_calls: raise ValueError(f"aggregate3 answered {n} results for {n_calls} calls")
    for i in range(n):
        e = base + wd(base + 32 * i); d = e + wd(e + 32); ln = wd(d)
        if d + 32 + ln > len(b): raise ValueError(f"aggregate3 answer cut in result {i}")
        out.append((wd(e) != 0, bytes(b[d + 32: d + 32 + ln])))
    return out


# ------------------------------------------------------------------ one quote call, as 5a's route_call / helper_call
def quote_call(p, side, amount, mc3):
    """(target, calldata) of one quote. side: 'buy' = exact OUTPUT of `amount` CA (the counter paid in) · 'sell' = exact INPUT
    of `amount` CA (the counter received) · 'buy_in' = exact INPUT of `amount` counter (a route that refuses exact output)"""
    kind, addr, key = p["route"]["kind"], p["route"]["address"], p.get("key") or {}
    ca, cn = p["ca"], p["counter"]["token"]
    tin, tout = (ca, cn) if side == "sell" else (cn, ca)
    out = side == "buy"; zfo = tin.lower() == p["token0"].lower()
    if kind == "QuoterV2":
        return addr, SEL["qv2_out" if out else "qv2_in"] + adr(tin) + adr(tout) + w(amount) + w(key["fee"]) + w(0)
    if kind == "Algebra quoter":
        return addr, SEL["alg_out" if out else "alg_in"] + adr(tin) + adr(tout) + w(amount) + w(0)
    if kind == "v2 router":
        return addr, SEL["amounts_in" if out else "amounts_out"] + w(amount) + w(64) + w(2) + adr(tin) + adr(tout)
    if kind == "pair getAmountOut":
        return addr, SEL["amount_in" if out else "amount_out"] + w(amount) + adr(tin)
    if kind == "curve get_dy":                     # (P1-DEX v10, 2026-10-21) the pool's own get_dy: exact input only (its buy is buy_in)
        if out: raise ValueError("Curve quotes exact input only: its buy is buy_in")
        fi, ti = (key["i"], key["j"]) if tin.lower() == p["token0"].lower() else (key["j"], key["i"])
        return addr, SEL["dy_u" if key["kind"] == "crypto" else "dy_i"] + w(fi) + w(ti) + w(amount)
    if kind == "balancer v2 query":                # (P1-DEX v11, 2026-10-21) its Vault's query, exact input / output (GIVEN_IN 0 / GIVEN_OUT 1)
        return addr, (SEL["bq_swap"] + w(0xa0) + w(0) + w(0) + w(0) + w(0) + b32(key["pid"]) + w(1 if out else 0) + adr(tin) + adr(tout)
                      + w(amount) + w(0xc0) + w(0))
    if kind == "balancer v3 query":                # (v11) the Router's query inside the Vault's quoteAndRevert, Multicall3 bounced (isolated)
        q = SEL["b3_out" if out else "b3_in"] + adr(p["pool"].split(":")[0]) + adr(tin) + adr(tout) + w(amount) + adr(NATIVE) + w(0xc0) + w(0)
        a3 = enc_agg3([(p["route"]["router"], q)])
        return addr, SEL["qar"] + w(32) + w(len(a3)) + a3 + bytes(-len(a3) % 32)
    if kind == "own model":                                              # concentrated: the pool's own swap, via the helper
        swap = SEL["swap"] + adr(mc3) + w(zfo) + w(-amount if out else amount) + w(MIN_SQ if zfo else MAX_SQ) + w(160) + w(0)
        return HELPER, bytes.fromhex(p["pool"][2:]) + swap
    if p["type"] == "uni-v4":
        k = adr(p["token0"]) + adr(p["token1"]) + w(key["fee"]) + w(key["tickSpacing"]) + adr(key["hooks"])
        return addr, SEL["v4_out" if out else "v4_in"] + w(32) + k + w(zfo) + w(amount) + w(256) + w(0)
    if p["type"] in ("inf-cl", "inf-bin"):
        k = adr(p["token0"]) + adr(p["token1"]) + adr(key["hooks"]) + adr(key["manager"]) + w(key["fee"]) + b32(key["parameters"])
        return addr, SEL["inf_out" if out else "inf_in"] + w(32) + k + w(zfo) + w(amount) + w(288) + w(0)
    raise ValueError(f"no quote call for {p['type']} / {kind}")


def bal_open(p, ok, ret):
    """(P1-DEX v11) a Balancer v3 answer comes inside the Vault's Result(bytes) revert: opened to the inner aggregate3's one (ok, ret),
    as lib/bsc.balancer_out; any other route's answer as it came"""
    if p["route"]["kind"] != "balancer v3 query" or ret is None: return ok, ret
    if ok or len(ret) < 68 or ret[:4] != SEL["result"]: return False, ret
    try:
        n = word(ret[4:], 1); return dec_agg3(ret[68: 68 + n], 1)[0]
    except Exception: return False, ret


def quote_amount(p, side, ok, ret):
    """the counter paid (buy) / received (sell), or the CA received (buy_in), in raw units; None when the pool cannot fill.
    Own model (CL): (the counter, the CA actually moved), the caller drops a partial fill"""
    ok, ret = bal_open(p, ok, ret)
    if not ok or not ret: return None
    kind = p["route"]["kind"]
    if kind == "v2 router": return word(ret, 2) if side == "buy" else word(ret, 3)
    if kind == "own model":
        if len(ret) != 64: return None
        d0, d1 = word(ret, 0, True), word(ret, 1, True)
        dca, dcn = (d0, d1) if p["ca"].lower() == p["token0"].lower() else (d1, d0)
        return (dcn, -dca) if side == "buy" else (-dcn, dca)
    if kind == "QuoterV2" and side == "sell" and len(ret) >= 64 and word(ret, 1) in (MIN_SQ, MAX_SQ): return None   # ran to its limit
    return word(ret, 0)


def refuses_exact_output(p, ok, ret):
    """5a's rule: a hook's ExactOutputUnsupported() (wrapped by the official quoter), or a Solidly pair without getAmountIn
    (an empty revert) -> buy by exact input of the counter"""
    ok, ret = bal_open(p, ok, ret)
    if ok: return False
    if p["route"]["kind"] == "pair getAmountOut": return len(ret) == 0
    return EXACT_OUTPUT_UNSUPPORTED in ret


def v2_amount(p, side, reserves, amount):
    """own-model v2: the exact constant product on the reserves at 4b's fee (5a's v2_quote)"""
    r0, r1 = reserves; fee = p.get("fee_pips")
    if not r0 or not r1 or fee is None: return None
    rca, rcn = (r0, r1) if p["ca"].lower() == p["token0"].lower() else (r1, r0); k = 10 ** 6 - fee
    if side == "buy":
        return None if amount >= rca else rcn * amount * 10 ** 6 // ((rca - amount) * k) + 1
    ai = amount * k; return ai * rcn // (rca * 10 ** 6 + ai)


def state_call(kind, pool, chain):
    """the state call of a pool (own-model v2: its reserves)"""
    c = CHAINS[chain]
    if kind in ("v2", "solidly"): return pool, SEL["reserves"]
    if kind in ("v3", "cl-ts"): return pool, SEL["slot0"]
    if kind in ("algebra", "alg-custom"): return pool, SEL["gstate"]
    if kind == "uni-v4": return c["v4_stateview"], SEL["getslot0"] + b32(pool)
    if kind == "inf-cl": return (c["inf_cl_manager"], SEL["getslot0"] + b32(pool)) if c.get("inf_cl_manager") else None
    return None


def hop_amount(h, ok, ret):
    """a hop quote's output in raw units of the token received (both directions are exact input), or None when the pool can't
    fill: a revert, an empty answer, a QuoterV2 that ran to its price limit"""
    ok, ret = bal_open(h, ok, ret)
    if not ok or not ret or len(ret) < 32: return None
    kind = h["route"]["kind"]
    if kind == "v2 router": return (word(ret, 3) or None) if len(ret) >= 128 else None
    if kind == "QuoterV2" and len(ret) >= 64 and word(ret, 1) in (MIN_SQ, MAX_SQ): return None
    return word(ret, 0) or None


def resized(cur, price):
    """the sizing price: kept while the market is within RESIZE of it"""
    return price if (not cur or abs(price / cur - 1) > RESIZE) else cur


class DexQuoter:
    """the DEX side of one ws_book_checker process: set_members() on each groups reload, cycle() every second, deep() from the
    engine (thread-safe enough under the GIL: deep() only writes self.pending and reads finished dicts)"""

    def __init__(self, chain, post, deep_max_quotes=DEEP_MAX_QUOTES, deep_ttl=DEEP_TTL, failing_retry_s=FAILING_RETRY_S):
        """post: async (body: bytes) -> bytes, one JSON-RPC POST to the chain's node (the caller owns session, URL and timeout)"""
        self.chain, self.post = chain, post
        self.mc3 = CHAINS[chain]["multicall3"]
        self.anchor, self.wrapped = CHAINS[chain]["anchor"], CHAINS[chain]["wrapped"]
        self.deep_max_quotes, self.deep_ttl, self.failing_retry_s = deep_max_quotes, deep_ttl, failing_retry_s
        # D36 (Pavel 10-21): (pool, ca) -> when its $100 quote last failed both ways. Out of the call (and of the ladders) until it
        # quotes again; the one out longest is re-tried once failing_retry_s passed, one a cycle. 10-21: 17 empty-liquidity v4 /
        # Infinity pools of one CA burnt ~460M of the call's ~563M gas (0.66 s a cycle; 0.19 s without them)
        self.failing = {}
        self.cas, self.pools = {}, []
        self.size_px = {}                            # CA -> USD: the sizing price of A
        self.tsize = {}                              # token -> USD: the sizing price of a counter / hop token (its path's mid)
        self.links, self.paths = {}, {}              # D31: link "A>B" -> {from, to, fd, td, pools: {pool: hop}}; token -> [links]
        self.hop_active = {}                         # (CA, side) -> when that ladder was last asked for: its hops at every rung meanwhile
        self.buy_in = set()                          # pools whose route refused exact output: buy by exact input of the counter
        self.sig = {}                                # (pool, ca, side) -> the last cycle's $100 quote: (call side, amount, raw answer)
        self.ladders = {}                            # (pool, ca, side) -> {"sig", "t", "block", "pts": [(CA raw, counter raw, $ rung)]}
        self.pending = {}                            # (ca, side) -> priority: deep levels the engine asked for
        self.last = None                             # the last cycle: block, ts, books, conv (the counters' USD), hops, reserves
        self.last_error = None                       # the last failed call, as the node or the transport said it
        self.unpriced = {}                           # pool -> why its counter has no USD this cycle (stated, never skipped silently)
        self.stats = {"cycles": 0, "failed": 0, "requests": 0, "ru": 0, "answer bytes": 0, "deep quotes": 0,
                      "deep kept (unchanged)": 0, "deep retried without": 0, "hop quotes": 0, "deep failed (all rungs)": 0,
                      "failing retried": 0}

    # ---------------------------------------------------------------- members
    def set_members(self, members):
        """members: {ca: {"dex_ca": {...}, "pools": [pool entries]}}. Learned sizes, ladders and refusals survive for kept CAs.
        D31: every counter's path to the anchor and each link's candidate hops come from ⓪c (counter.hops, in path order); a link
        named by several pools is one link, its candidates their union"""
        self.cas = {ca.lower(): m for ca, m in members.items() if m.get("pools") and (m.get("dex_ca") or {}).get("decimals") is not None}
        self.pools = [p for m in self.cas.values() for p in m["pools"]]
        for ca, m in self.cas.items():
            if not self.size_px.get(ca): self.size_px[ca] = m["dex_ca"].get("reference_usd")
        self.size_px = {ca: v for ca, v in self.size_px.items() if ca in self.cas}
        keep = {(p["pool"], p["ca"].lower()) for p in self.pools}
        self.sig = {k: v for k, v in self.sig.items() if k[:2] in keep}
        self.ladders = {k: v for k, v in self.ladders.items() if k[:2] in keep}
        self.failing = {k: v for k, v in self.failing.items() if k in keep}
        self.buy_in = {pl for pl in self.buy_in if any(pl == p["pool"] for p in self.pools)}
        self.buy_in |= {p["pool"] for p in self.pools if p["route"]["kind"] == "curve get_dy"}    # (v10) Curve: exact input both ways, as 5a
        self.links, self.paths = {}, {self.anchor: []}
        for p in self.pools:
            hops = p["counter"].get("hops") or []
            keys = [f"{L['from']}>{L['to']}" for L in hops]
            for i, L in enumerate(hops):
                d = self.links.setdefault(keys[i], {"from": L["from"], "to": L["to"], "fd": L.get("from_decimals"), "td": L.get("to_decimals"), "pools": {}})
                for h in L.get("pools") or []:
                    d["pools"].setdefault(h["pool"], dict(h, ca=h["from_token"], counter={"token": h["to_token"]}))
                self.paths.setdefault(L["from"], keys[i:])                    # every token on the path has its own (the rest)
                for tok, u in ((L["from"], L.get("from_usd")), (L["to"], L.get("to_usd"))):
                    if u and not self.tsize.get(tok): self.tsize[tok] = u
        self.tsize[self.anchor] = 1.0
        self.hop_active = {k: t for k, t in self.hop_active.items() if k[0] in self.cas}

    def _canon(self, t):
        t = t.lower()
        return self.wrapped if t == NATIVE else t

    # ---------------------------------------------------------------- transport
    async def _call(self, calls, override=None):
        params = [{"to": self.mc3, "data": "0x" + enc_agg3(calls).hex()}, "latest"]
        if override: params.append(override)
        ans = await self.post(_dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": params}))
        self.stats["requests"] += 1; self.stats["answer bytes"] += len(ans)
        self.stats["ru"] += 20 * (1 + max(0, (len(ans) - 1) // 1_000_000))       # BlockPI: eth_call 20 RU, +100% per extra 1 MB
        j = _loads(ans)
        if "result" not in j: raise RuntimeError(f"eth_call refused: {str(j.get('error'))[:200]}")
        return dec_agg3(bytes.fromhex(j["result"][2:]), len(calls))

    def _ca_amount(self, ca, usd):
        px = self.size_px.get(ca)
        return int(usd / px * 10 ** self.cas[ca]["dex_ca"]["decimals"]) if px else None

    def _counter_amount(self, p, usd):
        px = self.tsize.get(self._canon(p["counter"]["token"]))
        return int(usd / px * 10 ** p["counter"]["decimals"]) if px else None

    def _hop_calls(self, calls, items, sizes):
        """every candidate of these links at these $ sizes, exact input, on the sides asked for: 'sell' = the link's token in, the
        next out; 'buy' = the next in, the token out (appended to calls). items: [(link, sides)]
        -> [(link, hop, size, call side, amount, call index)]"""
        plan = []
        for k, sides in items:
            L = self.links[k]; ua, ub = self.tsize.get(L["from"]), self.tsize.get(L["to"])
            if not ua or not ub or not L["pools"] or L["fd"] is None or L["td"] is None: continue
            for s in sizes:
                aa, ab = int(s / ua * 10 ** L["fd"]), int(s / ub * 10 ** L["td"])
                for h in L["pools"].values():
                    if "sell" in sides: plan.append((k, h, s, "sell", aa, len(calls))); calls.append(quote_call(h, "sell", aa, self.mc3))
                    if "buy" in sides: plan.append((k, h, s, "buy_in", ab, len(calls))); calls.append(quote_call(h, "buy_in", ab, self.mc3))
        return plan

    def _hop_rates(self, plan, res):
        """the best quote per link, size and side, in units of the next token per link token: {link: {size: {"sell": (rate,
        pool), "buy": (rate, pool)}}}; selling wants the highest rate, buying the lowest"""
        rates = {}
        for k, h, s, side, amt, i in plan:
            out = hop_amount(h, *res[i])
            if not out: continue
            L = self.links[k]
            if side == "sell": name, r = "sell", (out / 10 ** L["td"]) / (amt / 10 ** L["fd"])
            else: name, r = "buy", (amt / 10 ** L["td"]) / (out / 10 ** L["fd"])
            slot = rates.setdefault(k, {}).setdefault(s, {})
            if name not in slot or (r > slot[name][0] if name == "sell" else r < slot[name][0]): slot[name] = (r, h["pool"])
        return rates

    def _compose(self, rates):
        """each token's USD along its path, per $ size and side: to pay for it (buy: every link's buy rate multiplied) and to
        receive for it (sell: every link's sell rate); the anchor is 1 -> {token: {size: {"buy": usd, "sell": usd}}}"""
        conv = {}
        for t, path in self.paths.items():
            out = {}
            for s in LADDER_USD:
                o = {}
                for name in ("buy", "sell"):
                    v = 1.0
                    for k in path:
                        r = rates.get(k, {}).get(s, {}).get(name)
                        if r is None: v = None; break
                        v *= r[0]
                    if v: o[name] = v
                if o: out[s] = o
            conv[t] = out
        return conv

    def _hop_gap(self, t, rates):
        """why a token has no $100 USD this cycle: the first link of its path without a candidate or without a quote"""
        if t not in self.paths: return "no hop path in ⓪c (a list built before D31)"
        for k in self.paths[t]:
            L = self.links.get(k)
            if not L or not L["pools"]: return f"hop {k[:10]}…>{k.split('>')[1][:10]}: no candidate in ⓪c"
            r = rates.get(k, {}).get(100, {})
            if "buy" not in r or "sell" not in r:
                return f"hop {k[:10]}…>{k.split('>')[1][:10]}: no {'buy' if 'buy' not in r else 'sell'} quote this cycle ({len(L['pools'])} candidates)"
        return "no USD this cycle"

    def _leg(self, p, ca, side, usd):
        """(call side, amount) of one quote of `usd` on that side, or None (no sizing price yet)"""
        if side == "buy" and p["pool"] in self.buy_in:
            a = self._counter_amount(p, usd); return ("buy_in", a) if a else None
        a = self._ca_amount(ca, usd); return (side, a) if a else None

    # ---------------------------------------------------------------- the 1 s cycle (+ the deep ladders asked for)
    async def cycle(self):
        """-> self.last = {"block", "ts", "books": {ca: {"ask": (usd_per_ca, qty_ca, pool), "bid": (...)}}, ...} or raises"""
        calls, top, deep = [(self.mc3, SEL["block"]), (self.mc3, SEL["ts"])], [], []
        t_now = time.time()                                       # D36: the failing pools stay out; the one out longest is re-tried
        due = [k for k, t in self.failing.items() if t_now - t >= self.failing_retry_s]
        retry = min(due, key=self.failing.get) if due else None
        if retry: self.stats["failing retried"] += 1
        buy_in_before = set(self.buy_in)
        for p in self.pools:
            ca = p["ca"].lower()
            if own_v2(p):
                if self.size_px.get(ca): top.append((p, ca, "reserves", None, None, len(calls))); calls.append(state_call("v2", p["pool"], self.chain))
            else:
                if (p["pool"], ca) in self.failing and (p["pool"], ca) != retry: continue
                for side in ("buy", "sell"):
                    leg = self._leg(p, ca, side, 100)
                    if leg: top.append((p, ca, side, leg[0], leg[1], len(calls))); calls.append(quote_call(p, leg[0], leg[1], self.mc3))
        hop_top = self._hop_calls(calls, [(k, ("buy", "sell")) for k in self.links], (100,))   # D31: every link's candidates at $100
        n_top = len(calls)
        pending, self.pending = self.pending, {}
        now = time.time(); budget = self.deep_max_quotes; left = {}
        for key in pending: self.hop_active[key] = now
        # D31: the hops of the ladders in use (asked now or within DEEP_TTL), at every other rung, on that ladder's side only (an
        # ask converts at the hops' buy, a bid at their sell), each link and side once, in the same budget as the ladders, the most
        # recently asked first; a ladder whose hops don't fit waits for the next call
        need, covered = {}, set()
        for (ca, side), t in sorted(self.hop_active.items(), key=lambda kv: -kv[1]):
            if now - t >= self.deep_ttl or ca not in self.cas: continue
            ks = [k for k in dict.fromkeys(k for p in self.cas[ca]["pools"] for k in self.paths.get(self._canon(p["counter"]["token"]), []))
                  if side not in need.get(k, ())]
            n = sum(len(self.links[k]["pools"]) for k in ks) * (len(LADDER_USD) - 1)
            if n > budget: continue
            budget -= n; covered.add((ca, side))
            for k in ks: need.setdefault(k, set()).add(side)
        hop_deep = self._hop_calls(calls, list(need.items()), LADDER_USD[1:]) if need else []
        for (ca, side), prio in sorted(pending.items(), key=lambda kv: -kv[1]):
            m = self.cas.get(ca)
            if not m: continue
            if (ca, side) not in covered: left[(ca, side)] = prio; continue   # its hops did not fit this call: the next one
            want = []
            for p in m["pools"]:
                if own_v2(p): continue                                    # its ladder is the constant product, no call
                if (p["pool"], ca) in self.failing: continue              # D36: no ladder for a pool that can't quote $100
                k = (p["pool"], ca, side); lad = self.ladders.get(k)
                if lad and lad["sig"] == self.sig.get(k) and now - lad["t"] < self.deep_ttl:
                    self.stats["deep kept (unchanged)"] += 1; continue
                legs = [self._leg(p, ca, side, usd) for usd in LADDER_USD]
                if all(legs): want.append((p, k, legs))
            n = sum(len(LADDER_USD) for _ in want)
            if n > budget: left[(ca, side)] = prio; continue              # next call
            budget -= n
            for p, k, legs in want:
                for (s, amt), usd in zip(legs, LADDER_USD): deep.append((p, k, s, amt, usd, len(calls))); calls.append(quote_call(p, s, amt, self.mc3))
        for kv in left.items(): self.pending.setdefault(*kv)
        own_cl = any(p["route"]["kind"] == "own model" and not own_v2(p) for p in self.pools)
        t0 = time.time()
        try:
            res = await self._call(calls, OVERRIDE if own_cl else None)
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {str(e)[:300]}"
            if not deep and not hop_deep: self.stats["failed"] += 1; raise
            self.stats["deep retried without"] += 1                      # the ladders never cost the top of book: retry without
            calls, deep, hop_deep = calls[:n_top], [], []
            try:
                res = await self._call(calls, OVERRIDE if own_cl else None)
            except Exception as e2:
                self.last_error = f"{type(e2).__name__}: {str(e2)[:300]}"; self.stats["failed"] += 1; raise
        block, ts = word(res[0][1], 0), word(res[1][1], 0)
        quotes, reserves = {}, {}
        for p, ca, side, s, amt, i in top:
            ok, ret = res[i]
            if side == "reserves":
                if ok and len(ret) >= 64: reserves[p["pool"]] = (word(ret, 0), word(ret, 1))
                continue
            if s == "buy" and refuses_exact_output(p, ok, ret): self.buy_in.add(p["pool"])   # 5a: by exact input from the next cycle
            q = quote_amount(p, s, ok, ret)
            quotes[(p["pool"], ca, side)] = (s, amt, q)
            self.sig[(p["pool"], ca, side)] = (s, amt, q)
        # D36: a pool whose $100 quote answered on neither side (a partial fill is an answer) leaves the call; it rejoins once it
        # quotes. Not on the cycle that just switched its buy to exact input: that buy gets its try first
        answered = {}
        for p, ca, side, s, amt, i in top:
            if side != "reserves": answered.setdefault((p["pool"], ca), []).append(quotes[(p["pool"], ca, side)][2] is not None)
        for k, got in answered.items():
            if len(got) == 2 and not any(got) and not (k[0] in self.buy_in and k[0] not in buy_in_before): self.failing[k] = t_now
            else: self.failing.pop(k, None)
        rates = self._hop_rates(hop_top + hop_deep, res)
        conv = self._compose(rates)
        books = self._books(quotes, reserves, conv, rates)
        got = {}
        for p, k, s, amt, usd, i in deep:
            ok, ret = res[i]; q = quote_amount(p, s, ok, ret)
            if isinstance(q, tuple):
                q, moved = q
                if moved != amt: q = None                                 # a partial fill: the pool cannot take the step
            if q: got.setdefault(k, []).append((q, amt, usd) if s == "buy_in" else (amt, q, usd))
            else: got.setdefault(k, [])
        for k, pts in got.items():
            if pts:
                self.ladders[k] = {"sig": self.sig.get(k), "t": now, "block": block, "pts": sorted(pts)}
            else:                               # D33.3 (10-15): no rung came back, so nothing is kept and the next ask re-quotes
                self.ladders.pop(k, None)       # it (a transient failure no longer blinds a quiet pool for DEEP_TTL)
                self.stats["deep failed (all rungs)"] += 1
        self.stats["deep quotes"] += len(deep); self.stats["hop quotes"] += len(hop_top) + len(hop_deep)
        self.last = {"block": block, "ts": ts, "books": books, "conv": conv, "hops": rates, "reserves": reserves,
                     "seconds": round(time.time() - t0, 3), "calls": len(calls), "deep": len(deep), "hop calls": len(hop_top) + len(hop_deep),
                     "failing out": len(self.failing)}
        self.stats["cycles"] += 1
        return self.last

    # ---------------------------------------------------------------- prices
    def _books(self, quotes, reserves, conv, rates):
        """per CA the best ask / bid in USD (one-level book): the counter paid (ask) at its path's buy USD, the counter received
        (bid) at its path's sell USD, both at $100 (D31). A pool whose counter has no USD on a side this cycle is counted
        (self.unpriced: pool -> why), never silently skipped"""
        books = {}
        self.unpriced = {}
        for p in self.pools:
            ca = p["ca"].lower(); t = self._canon(p["counter"]["token"])
            u = (conv.get(t) or {}).get(100) or {}
            if "buy" not in u or "sell" not in u:
                self.unpriced[p["pool"]] = f"counter {p['counter'].get('symbol') or t[:10]}: {self._hop_gap(t, rates)}"
                if not u: continue
            dca, dcn = self.cas[ca]["dex_ca"]["decimals"], p["counter"]["decimals"]
            for side in ("buy", "sell"):
                us = u.get(side)
                if not us: continue
                if own_v2(p):
                    A = self._ca_amount(ca, 100); r = reserves.get(p["pool"])
                    if not A or not r: continue
                    amt_ca, q = A, v2_amount(p, side, r, A)
                else:
                    s, amt, q = quotes.get((p["pool"], ca, side), (None, None, None))
                    if isinstance(q, tuple):
                        q, moved = q
                        if moved != amt: continue                       # a partial fill: the pool cannot take the whole $100
                    amt_ca = amt
                    if s == "buy_in": amt_ca, q = q, amt                 # the CA out = the answer, the counter in = the $100 input
                if not q or not amt_ca: continue
                px = (q / 10 ** dcn * us) / (amt_ca / 10 ** dca)         # USD per CA
                b = books.setdefault(ca, {})
                if side == "buy" and ("ask" not in b or px < b["ask"][0]): b["ask"] = (px, amt_ca / 10 ** dca, p["pool"])
                if side == "sell" and ("bid" not in b or px > b["bid"][0]): b["bid"] = (px, amt_ca / 10 ** dca, p["pool"])
        for ca, b in books.items():                                      # A follows the market
            if "ask" in b and "bid" in b: self.size_px[ca] = resized(self.size_px.get(ca), (b["ask"][0] + b["bid"][0]) / 2)
        for t, out in conv.items():                                      # so do the counters' and the hops' amounts
            u = out.get(100) or {}
            if "buy" in u and "sell" in u and t != self.anchor: self.tsize[t] = resized(self.tsize.get(t), (u["buy"] + u["sell"]) / 2)
        return books

    # ---------------------------------------------------------------- deep levels, for the engine
    def deep(self, ca, side, priority=0.0):
        """the engine, when the CA's $100 crossed a CEX: side 'buy' (buy the CA on the DEX: its asks) or 'sell' (its bids).
        Asks for the ladder (served by the next cycle's call, once for every CEX it crossed) and returns what is known now:
        {pool: [[price_usd, qty_ca], ...]} (the engine's book levels, best first) for every pool whose ladder matches its
        current state, or {} (asked for)"""
        ca = ca.lower(); key = (ca, side)
        if priority > self.pending.get(key, -1.0): self.pending[key] = priority
        return self.levels(ca, side)

    def levels(self, ca, side):
        """each rung's counter amount converted at that rung's hop (D31: the last cycle's quotes at that $ size, re-quoted every
        cycle while the CA's ladder is in use). D33.2 (10-15): a pool's ladder runs up to its first rung whose hop has no quote
        this cycle, so a hop that can't fill the deep rungs (past 3c's $2,500) no longer silences the whole pool; a rung not
        quoted yet comes with the next cycle"""
        last, m = self.last, self.cas.get(ca)
        if not last or not m: return {}
        conv, out, now, name = last["conv"], {}, time.time(), ("buy" if side == "buy" else "sell")
        dca = m["dex_ca"]["decimals"]
        for p in m["pools"]:
            cv = conv.get(self._canon(p["counter"]["token"])) or {}
            if own_v2(p):
                r = last["reserves"].get(p["pool"])
                if not r: continue
                pts = []
                for usd in LADDER_USD:
                    A = self._ca_amount(ca, usd); q = v2_amount(p, side, r, A) if A else None
                    if q: pts.append((A, q, usd))
            else:
                k = (p["pool"], ca, side); lad = self.ladders.get(k)
                if not lad or lad["sig"] != self.sig.get(k) or now - lad["t"] >= self.deep_ttl: continue
                pts = lad["pts"]
            keep = []
            for pt in sorted(pts, key=lambda x: x[2]):                     # rungs in $ order; stop at the first unpriced hop
                if not (cv.get(pt[2]) or {}).get(name): break
                keep.append(pt)
            if not keep: continue
            at = {usd: (cv.get(usd) or {}).get(name) for _, _, usd in keep}
            lv = to_levels(keep, dca, p["counter"]["decimals"], at, side)
            if lv: out[p["pool"]] = lv
        return out


def to_levels(pts, dca, dcn, usd_at, side):
    """[(CA raw, counter raw, $ rung)] cumulative ladder -> [[price_usd, qty_ca], ...]: each step's increment at its marginal
    price, the counter at its rung's hop (usd_at: $ rung -> USD per counter); asks only rise and bids only fall (an AMM's curve
    is convex; a rounding wobble must not make a later step look better)"""
    levels, prev_a, prev_u, prev_px = [], 0.0, 0.0, None
    for a_raw, q_raw, rung in sorted(pts):
        a, u = a_raw / 10 ** dca, q_raw / 10 ** dcn * usd_at[rung]
        da, du = a - prev_a, u - prev_u
        if da <= 0: continue
        px = du / da
        if prev_px is not None: px = max(px, prev_px) if side == "buy" else min(px, prev_px)
        levels.append([px, da]); prev_a, prev_u, prev_px = a, u, px
    return levels
