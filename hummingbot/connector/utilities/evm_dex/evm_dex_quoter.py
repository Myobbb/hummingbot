"""P2-DEX build step 2 (2026-10-14; the EVM core since 2026-10-22): a DEX connector's books, one eth_call a poll, on any EVM
chain (P1's name for the chain picks its row of P1's CHAINS: its Multicall3, its anchor stable, its wrapped native token).

For the CAs P2 trades (evm_dex_pool_list.PoolList: P1's records, handed over by the page -> watcher path), each poll is ONE
Multicall3 aggregate3 eth_call built with P1's own call encoding: evm_dex_p1_quoter.py is P1's
ws_book_checker/core/dex_quoter.py, copied unchanged (md5 d660f253…, 2026-10-22: every chain's row, Curve, Balancer; it is
0621aeea… plus those, the same for every BSC route kind) so a later sync can be checked by md5. The call holds:
  - getBlockNumber + getCurrentBlockTimestamp: every book carries its block, and the poll its own send time (`at`): a swap's
    send estimates the chain's head from these, so placing an order reads nothing (Pavel 2026-10-15);
  - the caller's own reads (`extra`: the connector's wallet, its balances and approvals), at the books' block, in the same
    call: they cost no request of their own (Pavel 2026-10-15; BlockPI bills an eth_call 20 RU whatever it carries);
  - $100 on both sides of EVERY tradable pool: the watch (Q7);
  - the ladder ($100 ... $1,000) of each (CA, side)'s book pool only, every poll: with a few pools, a ladder from the poll's
    own block costs ~8 quotes a CA, and P1's cache (keep a ladder while its $100 quote is unchanged) left a busy pool's book at
    its $100 level every other poll (test 2026-10-14). Since 2026-10-15 a ladder ends at what the wallet can put into one swap
    (Pavel: 350 USDT -> "100 / 250 / 325 quotes and executables"; the connector's caps, ladder_rungs): its buys spend the
    rung's USD of the counter and its sells sell the rung's CA, both exact INPUT as the swap runs, the cap rung exactly the
    wallet's USDT less arb_l's 25 reserve / its whole CA; no balance on a side: the $100 rung only, the price;
  - the hops, inside each pool's own DEX family (one router call chains one family): every link at $100, and at every rung
    for the counters of the pools whose ladder is in use (the hop pools move every block). D31's method: the best candidate
    per link, size and side; a counter's USD to pay / to receive is its links' rates multiplied;
  - at an election: every pool's full ladder, once.
Election (Q7, Pavel 2026-10-14): per (CA, side) the pool with the best price at the largest rung any of the CA's pools fills
($1,000 when one does), at start, every REELECT_S and whenever P1's pools for the CA change. The watch: a pool whose $100 beats
the elected pool's $100 by more than SWITCH_BPS takes the side's book for as long as it does (a dislocation).
A pool a swap can't go through sits out (bench; evm_dex_feed's diagnosis of a failed swap, P2-DEX #rad-blacklisted-pool,
Pavel 2026-10-30): no election, watch, book or hop route takes it, and a CA whose book it was is elected again among its
other pools (none left: no book), until its re-test passes (unbench).
Taxes (P1's per-pool `tax_pct`, D17 / D30: the CA's and its counter's route taxes): every price is net of the trade's own
tax, so a taxed pool competes at what it really pays. An ask is divided by (1 - buy tax) (fewer CA arrive), a bid multiplied by
(1 - sell tax). The `transfer` tax (wallet <-> CEX) is a rebalancing cost, not a trade's.
-> self.last["books"][ca][side] = {"pool", "levels": [[usd_per_ca, qty_ca], ...], "rungs", "switched"}: one book per CA and
side, one pool per book, so every level is executable in one router swap. Sides keep P1's names: "buy" = the CA bought on the
DEX (its asks), "sell" = the CA sold (its bids). Nothing here signs or sends; the node's URL and key stay with the caller."""
import json
import time
from typing import Dict, List, Optional, Set, Tuple

from hummingbot.connector.utilities.evm_dex.evm_dex_p1_quoter import (
    CHAINS,
    MAX_SQ,
    MIN_SQ,
    NATIVE,
    SEL,
    dec_agg3,
    enc_agg3,
    hop_amount,
    quote_amount,
    quote_call,
    refuses_exact_output,
    resized,
    to_levels,
    word,
)

RUNGS_USD = (100, 250, 500, 1000)   # P1's rungs, capped at $1,000 (Pavel 2026-10-14); a side's ladder ends at the wallet's
                                    # balance (ladder_rungs), and the hops are quoted at these sizes
# WIP values (Pavel 2026-10-14: "cadence 1 second is fine for starters ... the rest relaxed, we will tune it later"). The
# cadence (POLL_S) is the chain's: its spec
SWITCH_BPS = 50.0
REELECT_S = 900.0
SIDES = ("buy", "sell")
# an EIP-1559 chain's base fee, asked in the same POST as the poll's eth_call (a JSON-RPC batch): the block after the newest's,
# its last baseFeePerGas. Not Multicall3.getBasefee(): inside an eth_call with no gas price a node runs at base fee 0, so
# it answers 0 there (BlockRazor ETH, 2026-10-22)
FEE_HISTORY = {"jsonrpc": "2.0", "id": 2, "method": "eth_feeHistory", "params": ["0x1", "latest", []]}


def ladder_rungs(cap_usd: Optional[float]) -> List[Tuple[float, bool]]:
    """a side's ladder, ending at what the wallet can put into one swap (Pavel 2026-10-15: "325 gives us 100 / 250 / 325
    quotes and executables"): the standard rungs under the cap, then the cap itself -> [(USD, it is the cap)]. Under the
    $100 minimum, or with nothing held, only the $100 rung (the price); from $1,000 the standard four"""
    if not cap_usd or cap_usd < RUNGS_USD[0]:
        return [(RUNGS_USD[0], False)]
    if cap_usd >= RUNGS_USD[-1]:
        return [(r, False) for r in RUNGS_USD]
    return [(r, False) for r in RUNGS_USD if r < cap_usd] + [(cap_usd, True)]


def ran_to_limit(p: dict, ok: bool, ret: bytes) -> bool:
    """a QuoterV2 exact-input quote that stopped at its price limit: the pool could not take the whole input (P1's rule for
    its sells, in quote_amount; the ladder's buys are exact input too)"""
    return bool(ok) and p["route"]["kind"] == "QuoterV2" and len(ret) >= 64 and word(ret, 1) in (MIN_SQ, MAX_SQ)


def tax_factor(p: dict, side: str) -> Optional[float]:
    """what one unit of the quoted price is worth after the trade's own tax: an ask x 1/(1 - buy), a bid x (1 - sell)"""
    t = float((p.get("tax_pct") or {}).get(side) or 0.0) / 100.0
    if t >= 1.0:
        return None
    return 1.0 / (1.0 - t) if side == "buy" else 1.0 - t


class EvmDexQuoter:
    def __init__(self, post, chain: str, basefee: bool = False):
        """post: async (body: bytes) -> bytes, one JSON-RPC POST to the node (the caller owns the session, the URL and the key);
        chain: P1's name for it (the spec's P1_CHAIN), its row of P1's CHAINS; basefee: the poll also reads the next block's
        base fee, in the same POST (an EIP-1559 chain: the swap's fee comes from it, with no request of its own) ->
        last["basefee"]"""
        c = CHAINS[chain]
        self.basefee = basefee
        self.post, self.mc3, self.anchor, self.wrapped = post, c["multicall3"], c["anchor"], c["wrapped"]
        self.cas: Dict[str, dict] = {}            # ca -> {"decimals", "pools": [PoolList's tradable pools]}
        self.size_px: Dict[str, float] = {}       # ca -> USD: the sizing price of a rung's CA amount (follows the mid)
        self.tsize: Dict[str, float] = {}         # token -> USD: the sizing price of a counter or a hop token
        self.tbuy: Dict[tuple, float] = {}        # (token, family, hop size) -> USD to buy one along its path at that size, the
                                                  # last poll's: a buy rung's counter amount, at the size its hop is valued at
                                                  # (_hop_size), so the cap rung costs the cap (tsize moves only past RESIZE)
        self.links: Dict[tuple, dict] = {}        # (from, to, family) -> {"fd", "td", "pools": {pool: hop}}
        self.paths: Dict[tuple, list] = {}        # (token, family) -> the link keys from it to the anchor
        self.buy_in: Set[str] = set()             # pools whose route refuses exact output: bought by exact input of the counter
        self.ladders: Dict[tuple, dict] = {}      # (pool, ca, side) -> {"t", "block", "pts": [(CA raw, counter raw, rung)]}
        self.elected: Dict[tuple, str] = {}       # (ca, side) -> pool, by depth
        self.switched: Dict[tuple, str] = {}      # (ca, side) -> pool, while its $100 beats the elected one by > SWITCH_BPS
        self.elected_at: Dict[str, float] = {}    # ca -> when its pools were last elected
        self.electing: Set[str] = set()           # CAs whose every pool gets its full ladder in the next poll
        self.benched: Dict[str, dict] = {}        # pool -> why it sits out and its re-test (evm_dex_feed's entry): a pool a
                                                  # swap can't go through, taken by no election, watch, book or hop route
        self.caps: Dict[str, dict] = {}           # ca -> {"buy": USD the wallet can spend, "sell": raw CA it holds}: the
                                                  # connector sets it before each poll ({}: none known, the $100 rung only)
        self.rungs: Dict[tuple, list] = {}        # (ca, side) -> [(USD, raw CA amount or None)]: this poll's ladder
        self.extra: List[Tuple[str, bytes]] = []  # the caller's own reads (the connector's wallet), answered inside the
                                                  # poll's one eth_call at its block: no request of their own (Pavel
                                                  # 2026-10-15). -> self.last["extra"]: [(call, (ok, data)), ...]
        self.last: Optional[dict] = None
        self.stats = {"polls": 0, "failed": 0, "older block": 0, "ru": 0, "quotes": 0, "elections": 0, "switches": 0}

    # ---------------------------------------------------------------- the CAs P2 trades
    def set_pools(self, pool_list, changed=()):
        """every CA PoolList holds; the CAs in `changed` (PoolList.load()'s answer) are elected again"""
        self.cas = {ca: {"decimals": m["decimals"], "pools": m["pools"]} for ca, m in pool_list.cas.items() if m["pools"]}
        for ca in self.cas:
            if not self.size_px.get(ca):
                self.size_px[ca] = pool_list.cas[ca]["reference_usd"]
        self.links, self.paths = {}, {}
        for m in self.cas.values():
            for p in m["pools"]:
                fam, hops = p["family"], (p["counter"].get("hops") or [])
                keys = [(self._canon(link["from"]), self._canon(link["to"]), fam) for link in hops]
                for i, (link, candidates) in enumerate(zip(hops, p["hop_path"])):
                    d = self.links.setdefault(keys[i], {"fd": link.get("from_decimals"), "td": link.get("to_decimals"), "pools": {}})
                    for h in candidates:
                        d["pools"].setdefault(h["pool"], dict(h, ca=h["from_token"], counter={"token": h["to_token"]}))
                    self.paths.setdefault((keys[i][0], fam), keys[i:])
                    for token, usd in ((link["from"], link.get("from_usd")), (link["to"], link.get("to_usd"))):
                        if usd and not self.tsize.get(self._canon(token)):
                            self.tsize[self._canon(token)] = usd
                self.paths.setdefault((self.anchor, fam), [])
        self.tsize[self.anchor] = 1.0
        live = set(self.cas)
        self.electing = {ca for ca in self.electing if ca in live}
        self.electing |= {ca for ca in live if ca not in self.elected_at or ca in set(changed)}
        keep = {(p["pool"], ca) for ca, m in self.cas.items() for p in m["pools"]}
        self.size_px = {ca: v for ca, v in self.size_px.items() if ca in live}
        self.ladders = {k: v for k, v in self.ladders.items() if k[:2] in keep}
        self.elected = {k: pl for k, pl in self.elected.items() if (pl, k[0]) in keep}
        self.switched = {k: pl for k, pl in self.switched.items() if (pl, k[0]) in keep}
        self.elected_at = {ca: t for ca, t in self.elected_at.items() if ca in live}

    def _canon(self, token: str) -> str:
        token = (token or "").lower()
        return self.wrapped if token == NATIVE else token

    # ---------------------------------------------------------------- sizing (P1's)
    def _ca_amount(self, ca: str, usd: float) -> Optional[int]:
        px = self.size_px.get(ca)
        return int(usd / px * 10 ** self.cas[ca]["decimals"]) if px else None

    def _counter_amount(self, p: dict, usd: float) -> Optional[int]:
        px = self.tsize.get(self._canon(p["counter"]["token"]))
        return int(usd / px * 10 ** p["counter"]["decimals"]) if px else None

    def _leg(self, p: dict, ca: str, side: str, usd: float) -> Optional[Tuple[str, int]]:
        """(call side, amount) of one quote of `usd` on that side, or None (no sizing price yet)"""
        if side == "buy" and p["pool"] in self.buy_in:
            a = self._counter_amount(p, usd)
            return ("buy_in", a) if a else None
        a = self._ca_amount(ca, usd)
        return (side, a) if a else None

    def _rung_leg(self, p: dict, ca: str, side: str, usd: float, raw: Optional[int]) -> Optional[Tuple[str, int]]:
        """(call side, amount) of one ladder rung, exact INPUT on both sides, as the swap runs: a buy spends the rung's USD
        worth of the counter (so the cap rung spends exactly what the wallet can), a sell the rung's CA (the cap rung: the
        wallet's whole balance, `raw`)"""
        if side == "buy":                         # at the counter's live rate: the cap rung costs the cap, not +/- RESIZE
            counter = self._canon(p["counter"]["token"])
            px = self.tbuy.get((counter, p["family"], self._hop_size(usd))) or self.tsize.get(counter)
            a = int(usd / px * 10 ** p["counter"]["decimals"]) if px else None
            return ("buy_in", a) if a else None
        a = raw or self._ca_amount(ca, usd)
        return ("sell", a) if a else None

    def _set_rungs(self):
        """each (CA, side)'s ladder for this poll, from the wallet's caps (ladder_rungs); a sell's cap rung sells exactly the
        CA held, its USD at the sizing price"""
        self.rungs = {}
        for ca, m in self.cas.items():
            cap, px = self.caps.get(ca) or {}, self.size_px.get(ca)
            held = cap.get("sell")
            held_usd = held / 10 ** m["decimals"] * px if held and px else None
            self.rungs[(ca, "buy")] = [(usd, None) for usd, _ in ladder_rungs(cap.get("buy"))]
            self.rungs[(ca, "sell")] = [(usd, held if is_cap else None) for usd, is_cap in ladder_rungs(held_usd)]

    # ---------------------------------------------------------------- hops (D31's method, inside each pool's family)
    @staticmethod
    def _hop_size(usd: float) -> float:
        """the hops are quoted at the standard rungs: a cap rung converts at the next one up (the hops are deep; that one
        is the conservative side), past $1,000 at $1,000"""
        return next((r for r in RUNGS_USD if r >= usd), RUNGS_USD[-1])

    def _hop_calls(self, calls: list, items: list, sizes) -> list:
        plan = []
        for k, sides in items:
            link = self.links[k]
            ua, ub = self.tsize.get(k[0]), self.tsize.get(k[1])
            if not ua or not ub or not link["pools"] or link["fd"] is None or link["td"] is None:
                continue
            for s in sizes:
                aa, ab = int(s / ua * 10 ** link["fd"]), int(s / ub * 10 ** link["td"])
                for h in link["pools"].values():
                    if "sell" in sides:
                        plan.append((k, h, s, "sell", aa, len(calls)))
                        calls.append(quote_call(h, "sell", aa, self.mc3))
                    if "buy" in sides:
                        plan.append((k, h, s, "buy_in", ab, len(calls)))
                        calls.append(quote_call(h, "buy_in", ab, self.mc3))
        return plan

    def _hop_rates(self, plan: list, res: list) -> dict:
        """the best quote per link, size and side, in the next token per link token: selling wants the highest, buying the lowest"""
        rates = {}
        for k, h, s, side, amt, i in plan:
            out = hop_amount(h, *res[i]) if h["pool"] not in self.benched else None
            if not out:
                continue
            link = self.links[k]
            if side == "sell":
                name, r = "sell", (out / 10 ** link["td"]) / (amt / 10 ** link["fd"])
            else:
                name, r = "buy", (amt / 10 ** link["td"]) / (out / 10 ** link["fd"])
            slot = rates.setdefault(k, {}).setdefault(s, {})
            if name not in slot or (r > slot[name][0] if name == "sell" else r < slot[name][0]):
                slot[name] = (r, h["pool"])
        return rates

    def _usd(self, rates: dict, token: str, family: str, size: float, side: str) -> Optional[float]:
        """a token's USD along its path in `family` at this $ size: to pay for it ("buy") or to receive for it ("sell")"""
        path = self.paths.get((token, family))
        if path is None:
            return None
        v, size = 1.0, self._hop_size(size)
        for k in path:
            r = rates.get(k, {}).get(size, {}).get(side)
            if r is None:
                return None
            v *= r[0]
        return v

    def _hop_route(self, rates: dict, token: str, family: str, size: float, side: str) -> Optional[List[dict]]:
        """the hop pools that priced _usd(...) at this size and side, link by link from the token toward the anchor (each a
        hop record: pool, type, key, token0/1, from_token, to_token): the swap must run through these. [] for the anchor"""
        path = self.paths.get((token, family))
        if path is None:
            return None
        route, size = [], self._hop_size(size)
        for k in path:
            r = rates.get(k, {}).get(size, {}).get(side)
            if r is None:
                return None
            route.append(self.links[k]["pools"][r[1]])
        return route

    # ---------------------------------------------------------------- pools that sit out
    def bench(self, pool: str, entry: dict) -> List[str]:
        """a pool a swap can't go through sits out from now: no election, watch, book or hop route takes it (_top_prices,
        _ladder_avgs, _hop_rates). The CAs that trade through it are elected again at the next poll: a book it held goes to
        the CA's next-best pool, or to none -> those CAs"""
        self.benched[pool] = entry
        hit = self.users(pool)
        for ca in hit:
            for side in SIDES:
                for chosen in (self.elected, self.switched):
                    if chosen.get((ca, side)) == pool:
                        del chosen[(ca, side)]
        self.electing.update(hit)
        return hit

    def unbench(self, pool: str) -> List[str]:
        """a benched pool whose re-test passed takes part again: the CAs that trade through it are elected again at the next
        poll (it may win its book back) -> those CAs"""
        self.benched.pop(pool, None)
        hit = self.users(pool)
        self.electing.update(hit)
        return hit

    def users(self, pool: str) -> List[str]:
        """the CAs that trade through `pool`: as one of their pools, or as a hop candidate of one"""
        return [ca for ca, m in self.cas.items() if any(
            p["pool"] == pool or any(h["pool"] == pool for link in p["hop_path"] for h in link) for p in m["pools"])]

    # ---------------------------------------------------------------- the poll
    async def poll(self) -> dict:
        now = time.time()
        self._set_rungs()
        extra = list(self.extra)                     # with the top: a retry without the ladders keeps them
        calls = [(self.mc3, SEL["block"]), (self.mc3, SEL["ts"])] + extra
        top = []
        for ca, m in self.cas.items():
            for p in m["pools"]:
                for side in SIDES:
                    leg = self._leg(p, ca, side, RUNGS_USD[0])
                    if leg:
                        top.append((p, ca, side, leg[0], leg[1], len(calls)))
                        calls.append(quote_call(p, leg[0], leg[1], self.mc3))
        hop_top = self._hop_calls(calls, [(k, SIDES) for k in self.links], RUNGS_USD[:1])
        n_top = len(calls)
        in_use = []                                  # the pools whose ladder is in use: each side's book pool, or all at an election
        for ca, m in self.cas.items():
            for side in SIDES:
                book_pool = self.switched.get((ca, side)) or self.elected.get((ca, side))
                in_use += [(p, ca, side) for p in m["pools"]
                           if p["pool"] not in self.benched and (ca in self.electing or p["pool"] == book_pool)]
        deep = []
        for p, ca, side in in_use:
            k = (p["pool"], ca, side)
            rungs = self.rungs.get((ca, side)) or [(RUNGS_USD[0], None)]
            legs = [self._rung_leg(p, ca, side, usd, raw) for usd, raw in rungs]
            if not all(legs):
                continue
            for (s, amt), (usd, _) in zip(legs, rungs):
                deep.append((p, k, s, amt, usd, len(calls)))
                calls.append(quote_call(p, s, amt, self.mc3))
        need = {}
        for p, ca, side in in_use:
            for k in self.paths.get((self._canon(p["counter"]["token"]), p["family"]), []):
                need.setdefault(k, set()).add(side)
        hop_deep = self._hop_calls(calls, list(need.items()), RUNGS_USD[1:])
        try:
            res, basefee = await self._answers(calls)
        except Exception:
            if not deep and not hop_deep:
                self.stats["failed"] += 1
                raise
            calls, deep, hop_deep = calls[:n_top], [], []      # the ladders never cost the top: once more without them
            try:
                res, basefee = await self._answers(calls)
            except Exception:
                self.stats["failed"] += 1
                raise
        block, ts = word(res[0][1], 0), word(res[1][1], 0)
        if self.last and block < self.last["block"]:
            self.stats["older block"] += 1                # a node spreads requests over its backends: keep the newest block's books
            return self.last
        quotes = {}
        for p, ca, side, s, amt, i in top:
            ok, ret = res[i]
            if s == "buy" and refuses_exact_output(p, ok, ret):
                self.buy_in.add(p["pool"])                # P1's 5a rule: bought by exact input from the next poll
            quotes[(p["pool"], ca, side)] = (p, s, amt, quote_amount(p, s, ok, ret))
        rates = self._hop_rates(hop_top + hop_deep, res)
        got = {}
        for p, k, s, amt, usd, i in deep:
            q = quote_amount(p, s, *res[i])
            if isinstance(q, tuple):                      # an own-model answer (PoolList keeps none): a partial fill is no fill
                q = q[0] if q[1] == amt else None
            if s == "buy_in" and ran_to_limit(p, *res[i]):
                q = None                                  # the pool could not take the rung's whole input: no such level
            got.setdefault(k, [])
            if q:
                got[k].append((q, amt, usd) if s == "buy_in" else (amt, q, usd))
        for k, pts in got.items():
            self.ladders[k] = {"t": now, "block": block, "pts": sorted(pts)}
        top_px = self._top_prices(quotes, rates)
        for ca in [ca for ca in self.electing if self._elect(ca, rates, now)]:
            self.electing.discard(ca)
            self.elected_at[ca] = now
            self.stats["elections"] += 1
        for ca in self.cas:
            if ca not in self.electing and now - self.elected_at.get(ca, now) >= REELECT_S:
                self.electing.add(ca)
        self._watch(top_px)
        books = self._books(rates, top_px, now)
        self._resize(top_px, rates)
        self.stats["polls"] += 1
        self.stats["quotes"] += len(calls) - 2 - len(extra)
        self.last = {"block": block, "ts": ts, "at": now, "books": books, "seconds": round(time.time() - now, 3),
                     "calls": len(calls), "ladder quotes": len(deep), "hop quotes": len(hop_top) + len(hop_deep),
                     "electing": len(self.electing), "extra": list(zip(extra, res[2:2 + len(extra)]))}
        if self.basefee:
            self.last["basefee"] = basefee
        return self.last

    async def _call(self, calls: list) -> list:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                           "params": [{"to": self.mc3, "data": "0x" + enc_agg3(calls).hex()}, "latest"]}).encode()
        ans = await self.post(body)
        self.stats["ru"] += 20 * (1 + max(0, (len(ans) - 1) // 1_000_000))      # BlockPI: eth_call 20 RU, +100% per extra 1 MB
        j = json.loads(ans)
        if "result" not in j:
            raise RuntimeError(f"eth_call refused: {str(j.get('error'))[:200]}")
        return dec_agg3(bytes.fromhex(j["result"][2:]), len(calls))

    async def _answers(self, calls: list) -> Tuple[list, Optional[int]]:
        """the poll's call -> (its answers, the next block's base fee: an EIP-1559 chain's, in the same POST; else None)"""
        if not self.basefee:
            return await self._call(calls), None
        return await self._call_fees(calls)

    async def _call_fees(self, calls: list) -> Tuple[list, Optional[int]]:
        """_call's eth_call and FEE_HISTORY in one POST, a JSON-RPC batch (one round trip; answers matched by id) -> (the
        answers, the base fee of the block after the newest, or None when that answer is missing or malformed: no swap is
        signed without it). A node that refuses batches answers one error: the poll fails, loudly"""
        call = {"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                "params": [{"to": self.mc3, "data": "0x" + enc_agg3(calls).hex()}, "latest"]}
        ans = await self.post(json.dumps([call, FEE_HISTORY]).encode())
        self.stats["ru"] += 20 * (1 + max(0, (len(ans) - 1) // 1_000_000))
        got = json.loads(ans)
        got = {x.get("id"): x for x in got if isinstance(x, dict)} if isinstance(got, list) else {1: got}
        j = got.get(1) or {}
        if "result" not in j:
            raise RuntimeError(f"eth_call refused: {str(j.get('error'))[:200]}")
        try:
            fees = got[2]["result"]["baseFeePerGas"]
            basefee = int(fees[-1], 16) if fees else None
        except (KeyError, TypeError, ValueError, IndexError):
            basefee = None
        return dec_agg3(bytes.fromhex(j["result"][2:]), len(calls)), basefee

    # ---------------------------------------------------------------- prices, election, watch, books
    def _top_prices(self, quotes: dict, rates: dict) -> dict:
        """(ca, side) -> {pool: (usd per CA, qty CA)} at $100, the counter converted along its family path, net of the trade's tax"""
        out = {}
        for (pool, ca, side), (p, s, amt, q) in quotes.items():
            if pool in self.benched:
                continue
            usd = self._usd(rates, self._canon(p["counter"]["token"]), p["family"], RUNGS_USD[0], side)
            f = tax_factor(p, side)
            if not q or not amt or not usd or not f:
                continue
            amt_ca, amt_cn = (q, amt) if s == "buy_in" else (amt, q)
            qty = amt_ca / 10 ** self.cas[ca]["decimals"]
            out.setdefault((ca, side), {})[pool] = (amt_cn / 10 ** p["counter"]["decimals"] * usd / qty * f, qty)
        return out

    def _ladder_avgs(self, p: dict, ca: str, side: str, rates: dict, now: float) -> List[float]:
        """the average USD per CA (net of the trade's tax) at each rung this poll's ladder of the pool fills, up to the first
        rung without a hop quote"""
        lad = self.ladders.get((p["pool"], ca, side))
        f = tax_factor(p, side)
        if not lad or lad["t"] != now or not f or p["pool"] in self.benched:
            return []
        out = []
        for a_raw, q_raw, rung in lad["pts"]:
            usd = self._usd(rates, self._canon(p["counter"]["token"]), p["family"], rung, side)
            if not usd:
                break
            out.append(q_raw / 10 ** p["counter"]["decimals"] * usd / (a_raw / 10 ** self.cas[ca]["decimals"]) * f)
        return out

    def _elect(self, ca: str, rates: dict, now: float) -> bool:
        """per side, the pool with the best price at the largest rung any of the CA's pools fills -> False while a side has no ladder"""
        picks = {}
        for side in SIDES:
            avgs = {p["pool"]: self._ladder_avgs(p, ca, side, rates, now) for p in self.cas[ca]["pools"]}
            avgs = {pl: a for pl, a in avgs.items() if a}
            if not avgs:
                return False
            deepest = max(len(a) for a in avgs.values())
            at = {pl: a[deepest - 1] for pl, a in avgs.items() if len(a) == deepest}
            picks[side] = (min if side == "buy" else max)(at, key=at.get)
        for side, pool in picks.items():
            self.elected[(ca, side)] = pool
            self.switched.pop((ca, side), None)
        return True

    def _watch(self, top_px: dict):
        """a pool whose $100 beats the elected pool's by more than SWITCH_BPS takes the side's book, for as long as it does"""
        for (ca, side), prices in top_px.items():
            el = self.elected.get((ca, side))
            if not el or el not in prices:
                continue
            base = prices[el][0]
            edge = {pl: ((base - v[0]) if side == "buy" else (v[0] - base)) / base * 1e4 for pl, v in prices.items() if pl != el}
            better = {pl: e for pl, e in edge.items() if e > SWITCH_BPS}
            if better:
                pick = max(better, key=better.get)
                if self.switched.get((ca, side)) != pick:
                    self.stats["switches"] += 1
                self.switched[(ca, side)] = pick
            else:
                self.switched.pop((ca, side), None)

    def _books(self, rates: dict, top_px: dict, now: float) -> dict:
        """per CA and side the book pool's ladder as levels; its $100 level alone while that ladder isn't current"""
        books = {}
        for ca, m in self.cas.items():
            for side in SIDES:
                pool = self.switched.get((ca, side)) or self.elected.get((ca, side))
                p = next((x for x in m["pools"] if x["pool"] == pool), None)
                if not p:
                    continue
                lad = self.ladders.get((pool, ca, side))
                f = tax_factor(p, side)
                levels, rungs, priced_at = None, 0, ()
                if lad and lad["pts"] and lad["t"] == now and f:
                    usd_at = {rung: self._usd(rates, self._canon(p["counter"]["token"]), p["family"], rung, side) for _, _, rung in lad["pts"]}
                    if all(usd_at.values()):
                        levels = [[px * f, qty] for px, qty in to_levels(lad["pts"], m["decimals"], p["counter"]["decimals"], usd_at, side)]
                        rungs, priced_at = len(lad["pts"]), tuple(sorted(usd_at))
                if not levels and pool in top_px.get((ca, side), {}):
                    px, qty = top_px[(ca, side)][pool]
                    levels, rungs, priced_at = [[px, qty]], 1, (RUNGS_USD[0],)
                if levels:
                    # what a swap must run through: the CA pool's record and, per rung, the hop pools that priced it
                    counter = self._canon(p["counter"]["token"])
                    routes = {rung: self._hop_route(rates, counter, p["family"], rung, side) for rung in priced_at}
                    books.setdefault(ca, {})[side] = {"pool": pool, "levels": levels, "rungs": rungs,
                                                      "switched": (ca, side) in self.switched,
                                                      "record": p, "routes": routes}
        return books

    def _resize(self, top_px: dict, rates: dict):
        """the rungs' CA amounts and the hops' amounts follow the market (P1's RESIZE rule keeps a quiet quote byte-identical)"""
        for ca in self.cas:
            asks, bids = top_px.get((ca, "buy")), top_px.get((ca, "sell"))
            if asks and bids:
                mid = (min(v[0] for v in asks.values()) + max(v[0] for v in bids.values())) / 2
                self.size_px[ca] = resized(self.size_px.get(ca), mid)
        for token, family in self.paths:
            if token == self.anchor:
                continue
            for size in RUNGS_USD:                # each hop size a rung is valued at (2026-10-15: sized at the $100 rate, a
                                                  # hop pool's $325 cap rung cost $325.011, over what the wallet can spend)
                rate = self._usd(rates, token, family, size, "buy")
                if rate:
                    self.tbuy[(token, family, size)] = rate
            b = self._usd(rates, token, family, RUNGS_USD[0], "buy")
            s = self._usd(rates, token, family, RUNGS_USD[0], "sell")
            if b and s:
                self.tsize[token] = resized(self.tsize.get(token), (b + s) / 2)
