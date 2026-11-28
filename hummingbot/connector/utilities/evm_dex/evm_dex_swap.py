"""P2-DEX build step 4 (2026-10-14; the EVM core since 2026-10-22; one call per executor since 2026-11-28): the swap
calldata of one order, through the executor of each of its pools' DEX families, on any EVM chain. The chain's facts come from
its spec (`C`, the chain's constants module: its USDT, its wrapped native token, its FAMILY / KIND_FAMILIES / EXECUTORS and
PINNED_V2_ROUTERS, DEADLINE_S, SWAP_SLIPPAGE); nothing here is a chain's own.

An order is an EXACT-INPUT swap along the book's route, the one that priced it (evm_dex_quoter: the book's `record` and its hop
`routes` per rung):
  buy  (the CA bought): USDT in -> the hops back to the counter -> the CA pool -> the CA out, to the wallet;
  sell (the CA sold):   the CA in -> the CA pool -> the counter -> the hops to USDT -> USDT out, to the wallet.
Exact input everywhere: the v2 fee-on-transfer swaps and every pool type support it, and the wallet always pays a known amount.
The minimum output is the last pool's own output check; intermediate legs carry 0 (the final minimum covers the whole path).
A route's first legs alone (build_prefix) are built the same way: evm_dex_feed simulates them after a swap fails, to name the
pool it broke at (P2-DEX #rad-blacklisted-pool).

Executors (decision 2: official routers; one call chains one family only, Q5), pinned per chain in its spec:
  - a Universal-Router family (`C.EXECUTORS`, "permit2" set): its router (v2, v3 and its v4-type pools), paid through its
    Permit2;
  - a Curve family ("encoding" "curve-router-ng"): Curve's Router NG v1.2, which chains up to 5 Curve pools in one call
    (swap_type 1: int128 for stable pools, uint256 for crypto pools; the route's i / j and the direction come from P1's pool
    record, its key {n, i, j, kind}); paid by an ERC20 allowance to the router;
  - route:<v2 router>: that router's swapExactTokensForTokensSupportingFeeOnTransferTokens, paid by ERC20 allowance, for a
    router pinned in the spec's PINNED_V2_ROUTERS only.
Native coin: a v4 / Infinity pool may hold the chain's native coin (address 0) where its neighbours hold the wrapped token; the
router wraps or unwraps between them. A Curve pool that holds the native coin is no executor yet (its ETH sentinel would need a
wrap step wherever the route crosses a segment boundary): no_executor says so. Taxed tokens: v2 legs measure by balance
(tax-safe); a taxed token paid into a v3 / v4 / Infinity / Curve pool makes the swap revert (decision 2's known downside), and a
taxed output arrives short by the tax P1 already prices.

MORE THAN ONE EXECUTOR (2026-11-28, Pavel: "Full: Curve + split execution"). A route whose links cannot all be carried by one
family (HOP_FALLBACK, the spec's) is split into segments of consecutive legs, each with one executor; each segment is one
transaction, and every transaction of the order goes out in ONE bundle with consecutive nonces, so the order lands whole or not
at all (a builder includes a bundle only if every one of its transactions succeeds). Between two segments the value moves as
follows:
  - into a Universal-Router segment: the router can spend its own balance (CONTRACT_BALANCE, payerIsUser false), so the
    previous segment delivers its output TO THE ROUTER's address and that segment takes no exact input: nothing is left over;
  - into any other executor (Curve's Router NG, a v2 router): the executor pulls an exact amount from the wallet, so the
    previous segment delivers to the WALLET, its minimum output is that exact amount, and the next segment spends exactly it.
    That amount is the route's own expected amount entering the segment (legs_raw, from the book's own ladder and hop quotes)
    less the spec's SEGMENT_SLIPPAGE (a price-move allowance, not the order's protection): an adverse move past it drops the
    bundle, and a favourable one leaves at most that much as dust in the wallet (the intermediate token).
BSC's encodings (sources pinned 2026-10-14; wiki P2-DEX Open Items §step 4), the ones built here:
  - Uniswap Universal Router 2.0.0 (0x1906…, the one the official v4 and v3 deployment pages list): V2/V3 swap inputs of 5
    fields; v4 ExactInputSingleParams without a per-hop field. (Uniswap's 2.1.x routers take a 6th field: not these.)
  - PancakeSwap Infinity Universal Router (0xd9C5…, deployed from commit b303b14): the same V2/V3 inputs; INFI_SWAP (0x10)
    with Infinity's actions; paid through PancakeSwap's own Permit2.
ETH's (sources pinned 2026-10-22: Uniswap/universal-router 2.1.2 = 802fe4c1, its v4-periphery 545a5d2a; wiki p2-dex-eth
§executors): Universal Router 2.1.2 (0x2361…De85), an executor whose "encoding" is "ur-2.1": the V2/V3 swap inputs take a 6th
field, `uint256[] minHopPriceX36` (empty: off), and v4's ExactInputSingleParams a `uint256 minHopPriceX36` before hookData (0:
off); the command and action bytes are 2.0.0's. An executor without "encoding" is "ur-2.0" (BSC's routers).
Curve's (2026-11-28): Router NG v1.2 (0x4531…cd4e), "exchange(address[11],uint256[5][5],uint256,uint256,address[5],address)",
proven on live pools by p2_dex_research/p2_dex/curve_eth_2026-11-28 (the router's own get_dy == the pools' chained get_dy, its
answer that less 1 wei; min_dy enforced). Its route is [token, pool, token, pool, …]; its params are [i, j, swap_type 1,
pool_type, n_coins] per hop, pool_type 1 for a stable kind and 2 for crypto (the router's branch is `in [1, 10]` vs the rest).
Every other encoding is proven by simulating the swap from the wallet on live pools (hmb_local_tools/p2_dex/step4_sim, ETH's in
p2_dex_eth)."""
import time
from typing import Dict, List, Optional, Tuple

from eth_abi import encode

NATIVE = "0x0000000000000000000000000000000000000000"
ZERO5 = ["0x" + "00" * 20] * 5
ZERO11 = ["0x" + "00" * 20] * 11
V2_ROUTER_TYPES = ("v2",)                    # a family `route:<v2 router>`: that router, its fee-on-transfer swap

# Universal Router commands and their constants (both routers' Commands.sol / ActionConstants at the deployed commits)
V3_SWAP_EXACT_IN, V2_SWAP_EXACT_IN, WRAP_ETH, UNWRAP_WETH, POOL_SWAP = 0x00, 0x08, 0x0b, 0x0c, 0x10   # 0x10: V4_SWAP / INFI_SWAP
MSG_SENDER = "0x0000000000000000000000000000000000000001"
ADDRESS_THIS = "0x0000000000000000000000000000000000000002"
CONTRACT_BALANCE = 1 << 255
OPEN_DELTA = 0
# v4 / Infinity actions (Uniswap v4-periphery and PancakeSwap infinity-periphery Actions.sol; the same numbers for these)
SWAP_EXACT_IN_SINGLE = 0x06                  # Uniswap SWAP_EXACT_IN_SINGLE = Infinity CL_SWAP_EXACT_IN_SINGLE
SETTLE, SETTLE_ALL, TAKE, TAKE_ALL = 0x0b, 0x0c, 0x0e, 0x0f
SEL_EXECUTE = bytes.fromhex("3593564c")      # execute(bytes,bytes[],uint256)
SEL_V2_FOT = bytes.fromhex("5c11d795")       # swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)
UNI_POOLKEY = "(address,address,uint24,int24,address)"                    # currency0, currency1, fee, tickSpacing, hooks
INF_POOLKEY = "(address,address,address,address,uint24,bytes32)"          # currency0, currency1, hooks, poolManager, fee, parameters
# Curve Router NG v1.2 (curvefi/curve-router-ng): exchange(address[11],uint256[5][5],uint256,uint256,address[5],address); the
# route is [token, pool, token, pool, ...], the params per hop [i, j, swap_type, pool_type, n_coins]
SEL_CURVE_EXCHANGE = bytes.fromhex("c872a3c5")
CURVE_POOL_TYPE = {"stable": 1, "crypto": 2}   # the router's branch is pool_type in [1, 10] (stable, int128) vs the rest (crypto)


def family_of(C, record: dict) -> str:
    """the DEX family of a pool record: the spec's KIND_FAMILIES first (a route kind whose pools each run on their own, e.g.
    Curve), then the route address's family (FAMILY), else `route:<address>`"""
    kind = (record.get("route") or {}).get("kind")
    families = getattr(C, "KIND_FAMILIES", {}) or {}
    if kind in families:
        return families[kind]
    address = ((record.get("route") or {}).get("address") or "").lower()
    return C.FAMILY.get(address) or f"route:{address}"


def canon(C, token: str) -> str:
    """the chain's native coin and its wrapped token are one token for continuity (the router converts between them)"""
    token = (token or "").lower()
    return C.WRAPPED if token == NATIVE else token


def legs(record: dict, hops: List[dict], side: str) -> List[Tuple[dict, str, str]]:
    """the swap's pools in execution order, each (pool record, token in, token out), tokens as the POOL holds them (the
    native coin stays address 0). `hops` run from the counter toward USDT (evm_dex_quoter._hop_route), each with from_token /
    to_token."""
    ca, cn = record["ca"].lower(), record["counter"]["token"].lower()
    if side == "buy":
        out = [(h, h["to_token"].lower(), h["from_token"].lower()) for h in reversed(hops)]
        out.append((record, cn, ca))
    elif side == "sell":
        out = [(record, ca, cn)]
        out += [(h, h["from_token"].lower(), h["to_token"].lower()) for h in hops]
    else:
        raise ValueError(f"side {side!r}")
    return out


def check_legs(C, path: List[Tuple[dict, str, str]], token_in: str, token_out: str) -> None:
    """raises when the legs don't chain token_in -> ... -> token_out (the native coin = its wrapped token for chaining)"""
    if canon(C, path[0][1]) != canon(C, token_in) or canon(C, path[-1][2]) != canon(C, token_out):
        raise ValueError(f"the route runs {path[0][1]} -> {path[-1][2]}, not {token_in} -> {token_out}")
    for (_, _, out_a), (_, in_b, _) in zip(path, path[1:]):
        if canon(C, out_a) != canon(C, in_b):
            raise ValueError(f"the route breaks between {out_a} and {in_b}")


def executor(C, record: dict) -> Optional[Dict]:
    """the contract that executes a pool of this record's family -> {"router", "permit2" (None: paid by ERC20 allowance),
    "types", "encoding"}, or None when P2 has no executor for it: a family outside the spec's EXECUTORS, or a v2 router not
    in its PINNED_V2_ROUTERS"""
    family = record.get("family") or family_of(C, record)
    if family in C.EXECUTORS:
        return C.EXECUTORS[family]
    router = family[len("route:"):]
    if (family.startswith("route:") and (record.get("route") or {}).get("kind") == "v2 router"
            and router in C.PINNED_V2_ROUTERS):
        return {"router": router, "permit2": None, "types": V2_ROUTER_TYPES}
    return None


def no_executor(C, record: dict) -> Optional[str]:
    """why P2 can't execute this pool, or None"""
    family = record.get("family") or family_of(C, record)
    ex = executor(C, record)
    if ex is None:
        kind = record.get("family") or (record.get("route") or {}).get("kind")
        pin = " (its router is not pinned)" if family.startswith("route:") else ""
        return f"no executor for {record.get('type')} on {kind}{pin}"
    if record.get("type") not in ex["types"]:
        return f"no executor for {record.get('type')} on {family}"
    if "curve" in ex["types"] and NATIVE in ((record.get("token0") or "").lower(), (record.get("token1") or "").lower()):
        return "no executor: a Curve pool that holds the native coin"
    return None


def router_pays(C, ex: Dict) -> bool:
    """whether an executor can spend its own balance (a Universal-Router segment: CONTRACT_BALANCE, payerIsUser false), so a
    previous segment may deliver to it and leave nothing over; a Curve / v2 router pulls an exact amount from the wallet"""
    return bool((ex.get("encoding") or "").startswith("ur-") and ex.get("permit2"))


def route_segments(C, record: dict, path: List[Tuple[dict, str, str]]) -> List[Tuple[Dict, List[Tuple[dict, str, str]]]]:
    """the route's legs grouped into segments of consecutive legs that share one executor: [(executor, [legs])]. A route whose
    links all sit in one family is one segment (the one transaction every swap was before 2026-11-28); a mixed one has more,
    each its own transaction (build_swap)"""
    segs = []
    for leg in path:
        ex = executor(C, dict(leg[0], family=leg[0].get("family") or family_of(C, leg[0])))
        if ex is None:
            raise ValueError(no_executor(C, dict(leg[0], family=family_of(C, leg[0]))) or "no executor")
        if segs and segs[-1][0]["router"] == ex["router"]:
            segs[-1][1].append(leg)
        else:
            segs.append((ex, [leg]))
    return segs


def _engine(pool: dict) -> str:
    return {"v2": "v2", "v3": "v3"}.get(pool["type"], pool["type"])


def _pool_key(pool: dict):
    k = pool.get("key") or {}
    if pool["type"] == "uni-v4":
        return (pool["token0"], pool["token1"], int(k["fee"]), int(k["tickSpacing"]), k["hooks"])
    return (pool["token0"], pool["token1"], k["hooks"], k["manager"], int(k["fee"]), bytes.fromhex(k["parameters"][2:].rjust(64, "0")))


def _pool_swap(pool: dict, tin: str, tout: str, amount: int, minimum: int, first: bool, last: bool, receiver: str,
               v21: bool = False) -> bytes:
    """one v4 / Infinity pool as a POOL_SWAP input: (actions, params). `first`: the wallet pays this pool (SETTLE_ALL after the
    swap, through Permit2); else the router's own balance pays it (SETTLE of CONTRACT_BALANCE first, then the swap of the whole
    credit). The segment's last pool's output goes to `receiver` (TAKE_ALL with the minimum), any other to the router.
    v21: Universal Router 2.1.x's v4 params (minHopPriceX36 before hookData, 0: off)"""
    zero_for_one = tin.lower() == pool["token0"].lower()
    if pool["type"] == "uni-v4" and v21:
        swap = (SWAP_EXACT_IN_SINGLE, encode([f"({UNI_POOLKEY},bool,uint128,uint128,uint256,bytes)"],
                                             [(_pool_key(pool), zero_for_one, amount if first else OPEN_DELTA, minimum, 0, b"")]))
    elif pool["type"] == "uni-v4":
        swap = (SWAP_EXACT_IN_SINGLE, encode([f"({UNI_POOLKEY},bool,uint128,uint128,bytes)"],
                                             [(_pool_key(pool), zero_for_one, amount if first else OPEN_DELTA, minimum, b"")]))
    else:                                                            # inf-cl
        swap = (SWAP_EXACT_IN_SINGLE, encode([f"({INF_POOLKEY},bool,uint128,uint128,bytes)"],
                               [(_pool_key(pool), zero_for_one, amount if first else OPEN_DELTA, minimum, b"")]))
    steps = []
    if not first:
        steps.append((SETTLE, encode(["address", "uint256", "bool"], [tin, CONTRACT_BALANCE, False])))
    steps.append(swap)
    if first:
        steps.append((SETTLE_ALL, encode(["address", "uint256"], [tin, amount])))
    if last:
        steps.append((TAKE_ALL, encode(["address", "uint256"], [tout, minimum])))
    else:
        steps.append((TAKE, encode(["address", "address", "uint256"], [tout, ADDRESS_THIS, OPEN_DELTA])))
    return encode(["bytes", "bytes[]"], [bytes(a for a, _ in steps), [p for _, p in steps]])


def _ur_call(C, ex: Dict, seg: List[Tuple[dict, str, str]], amount: int, minimum: int, receiver: str, pays: bool,
             deadline: int) -> bytes:
    """one Universal-Router transaction for these consecutive legs, in execution order. `pays`: the wallet pays the first leg
    (through Permit2); else the router spends its own balance (a previous segment delivered to it). `receiver`: this
    segment's last leg's output."""
    segments = []                                                    # consecutive v2 legs chain in one command, v3 legs too
    for leg in seg:
        eng = _engine(leg[0])
        if segments and segments[-1][0] == eng and eng in ("v2", "v3"):
            segments[-1][1].append(leg)
        else:
            segments.append((eng, [leg]))
    commands, inputs = [], []
    v21 = ex.get("encoding") == "ur-2.1"                             # Universal Router 2.1.x: a 6th field in the v2 / v3 inputs
    hop_price = (["uint256[]"], [[]]) if v21 else ([], [])           # minHopPriceX36: empty, off (the minimum guards the path)
    for i, (eng, part) in enumerate(segments):
        first, last = i == 0, i == len(segments) - 1
        tin, tout = part[0][1], part[-1][2]
        if not first:                                                # the native coin <-> its wrapped token between two pools
            prev_out = segments[i - 1][1][-1][2]
            if prev_out == C.WRAPPED and tin == NATIVE:
                commands.append(UNWRAP_WETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, 0]))
            elif prev_out == NATIVE and tin == C.WRAPPED:
                commands.append(WRAP_ETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, CONTRACT_BALANCE]))
        amt = amount if first else CONTRACT_BALANCE
        recipient = receiver if last else ADDRESS_THIS
        mini = minimum if last else 0
        payer = pays if first else False                            # a later command is paid from the router's own balance
        if eng == "v2":
            commands.append(V2_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "address[]", "bool"] + hop_price[0],
                                 [recipient, amt, mini, [tin] + [t for _, _, t in part], payer] + hop_price[1]))
        elif eng == "v3":
            packed = bytes.fromhex(tin[2:])
            for pool, _, t in part:
                packed += int(pool["key"]["fee"]).to_bytes(3, "big") + bytes.fromhex(t[2:])
            commands.append(V3_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "bytes", "bool"] + hop_price[0],
                                 [recipient, amt, mini, packed, payer] + hop_price[1]))
        else:
            pool, _, _ = part[0]
            commands.append(POOL_SWAP)
            inputs.append(_pool_swap(pool, tin, tout, amount if (pays and first) else OPEN_DELTA, mini,
                                     first=pays and first, last=last, receiver=recipient, v21=v21))
    return SEL_EXECUTE + encode(["bytes", "bytes[]", "uint256"], [bytes(commands), inputs, deadline])


def _curve_call(C, ex: Dict, seg: List[Tuple[dict, str, str]], amount: int, minimum: int, receiver: str) -> bytes:
    """one Curve Router NG transaction for these consecutive Curve legs, in execution order: the route [token, pool, token,
    ...] and each hop's [i, j, swap_type 1, pool_type, n_coins], i / j from P1's pool record (its key; token0 <-> i) and the
    direction from the leg. The router pulls `amount` of the first token from the wallet and sends the last token to
    `receiver`, at least `minimum`."""
    if len(seg) > 5:
        raise ValueError(f"a Curve segment of {len(seg)} pools: the router takes at most 5")
    route, params = [seg[0][1].lower()], []
    for pool, tin, tout in seg:
        key = pool.get("key") or {}
        if key.get("kind") not in CURVE_POOL_TYPE:
            raise ValueError(f"Curve pool {pool.get('pool')}: kind {key.get('kind')!r}")
        i, j = (key["i"], key["j"]) if tin.lower() == pool["token0"].lower() else (key["j"], key["i"])
        params.append([int(i), int(j), 1, CURVE_POOL_TYPE[key["kind"]], int(key["n"])])
        route += [pool["route"]["address"].lower(), tout.lower()]    # [token, pool, token, pool, ...]
    route += ZERO11[len(route):]
    params += [[0, 0, 0, 0, 0]] * (5 - len(params))
    return SEL_CURVE_EXCHANGE + encode(["address[11]", "uint256[5][5]", "uint256", "uint256", "address[5]", "address"],
                                       [route, params, amount, minimum, ZERO5, receiver.lower()])


def _v2_call(C, ex: Dict, seg: List[Tuple[dict, str, str]], amount: int, minimum: int, receiver: str, deadline: int) -> bytes:
    """a pinned v2 router's one fee-on-transfer swap over these consecutive legs, `amount` of the first token, at least
    `minimum` of the last, sent to `receiver`"""
    tokens = [canon(C, seg[0][1])] + [canon(C, t) for _, _, t in seg]
    return SEL_V2_FOT + encode(["uint256", "uint256", "address[]", "address", "uint256"],
                               [amount, minimum, tokens, receiver, deadline])


def build_swap(C, record: dict, hops: List[dict], side: str, amount_in: int, min_out: int, wallet: str,
               deadline: Optional[int] = None, legs_raw: Optional[List[int]] = None) -> Dict:
    """-> {"calls": [{"to", "value", "data", "token_in", "token_out", "executor", "deadline", "kind"}, ...],
    "token_in", "token_out", "deadline"}: one exact-input swap of `amount_in` (raw units of the token paid) along the book's
    route, the last pool's output at least `min_out` (raw), delivered to `wallet`, void after `deadline` (unix s). One call
    per executor: a route inside one family is one transaction, as it always was (its fields are also at the top level, as
    before); a mixed route is several, sent as one bundle by evm_dex_feed. `legs_raw`: the route's expected raw amounts in
    execution order, len(path) + 1 (the book's own ladder and hop quotes, scaled to this order), needed only where a segment
    boundary pulls an exact amount (a Curve or v2 router after another segment).
    Raises ValueError for a route P2 can't execute (see no_executor) or that doesn't chain."""
    path = legs(record, hops, side)
    token_in, token_out = (C.USDT, record["ca"].lower()) if side == "buy" else (record["ca"].lower(), C.USDT)
    out = _route_calls(C, record, path, token_in, token_out, amount_in, min_out, wallet, deadline, legs_raw)
    if len(out["calls"]) == 1:                                       # one executor: the single call's fields as before
        out.update({k: out["calls"][0][k] for k in ("to", "value", "data", "executor")})
    return out


def build_prefix(C, record: dict, hops: List[dict], side: str, n: int, amount_in: int, wallet: str,
                 deadline: Optional[int] = None, legs_raw: Optional[List[int]] = None) -> Dict:
    """the swap's first `n` legs as a swap of their own (build_swap's dict, `token_out` the n-th pool's output): `amount_in`
    of the order's input, at least 1 raw unit out. Never sent: evm_dex_feed simulates these to name the pool a route that
    reverts breaks at (n = every leg: the whole route at a minimum of 1)"""
    path = legs(record, hops, side)
    if not 1 <= n <= len(path):
        raise ValueError(f"no prefix of {n} legs in a route of {len(path)}")
    token_in = C.USDT if side == "buy" else record["ca"].lower()
    return _route_calls(C, record, path[:n], token_in, path[n - 1][2], amount_in, 1, wallet, deadline,
                        legs_raw[:n + 1] if legs_raw else None)


def _route_calls(C, record: dict, path: List[Tuple[dict, str, str]], token_in: str, token_out: str, amount_in: int,
                 min_out: int, wallet: str, deadline: Optional[int],
                 legs_raw: Optional[List[int]] = None) -> Dict:
    """the executors' calls for these legs (build_swap's dict)"""
    check_legs(C, path, token_in, token_out)
    for pool, _, _ in path:
        problem = no_executor(C, dict(pool, family=pool.get("family") or family_of(C, pool)))
        if problem:
            raise ValueError(problem)
    deadline = int(deadline if deadline is not None else time.time() + C.DEADLINE_S)
    segs = route_segments(C, record, path)
    starts, at = [], 0
    for _, seg in segs:
        starts.append(at)
        at += len(seg)
    slippage = float(getattr(C, "SEGMENT_SLIPPAGE", getattr(C, "SWAP_SLIPPAGE", 0)) or 0)
    amounts: List[Optional[int]] = []
    for i, (ex, seg) in enumerate(segs):
        if i == 0:
            amounts.append(amount_in)
        elif router_pays(C, ex):
            amounts.append(None)                                     # CONTRACT_BALANCE: whatever the last segment left there
        else:                                                        # an exact pull from the wallet, which the last must deliver
            if not legs_raw or len(legs_raw) != len(path) + 1:
                raise ValueError(f"the route crosses {len(segs)} executors: its per-leg amounts (the book's) are needed")
            exact = int(legs_raw[starts[i]] * (1 - slippage))
            if exact <= 0:
                raise ValueError(f"the route's amount entering segment {i + 1} is nothing")
            amounts.append(exact)
    calls = []
    for i, (ex, seg) in enumerate(segs):
        last = i == len(segs) - 1
        nxt = None if last else segs[i + 1][0]
        receiver = wallet if (last or not router_pays(C, nxt)) else nxt["router"]
        if last:
            minimum = min_out
        elif router_pays(C, nxt):
            minimum = 1                                              # the router takes its whole balance; the last minimum guards
        else:
            minimum = amounts[i + 1]                                 # what the next segment will pull from the wallet
        tin, tout = seg[0][1], seg[-1][2]
        kind = ("curve" if "curve" in ex["types"] else
                "v2" if ex.get("permit2") is None else "ur")
        amt = amounts[i] if amounts[i] is not None else CONTRACT_BALANCE
        if kind == "curve":
            data = _curve_call(C, ex, seg, amt, minimum, receiver)
        elif kind == "v2":
            data = _v2_call(C, ex, seg, amt, minimum, receiver, deadline)
        else:
            # the wallet as the receiver is the Universal Router's own MSG_SENDER (0x…01), as every UR swap was before
            # 2026-11-28: the same recipient, and a single-family route's calldata stays byte-identical
            data = _ur_call(C, ex, seg, amt, minimum,
                            MSG_SENDER if receiver.lower() == wallet.lower() else receiver,
                            pays=(i == 0), deadline=deadline)
        calls.append({"to": ex["router"], "value": 0, "data": data, "token_in": tin, "token_out": tout, "executor": ex,
                      "deadline": deadline, "kind": kind})
    return {"calls": calls, "token_in": token_in, "token_out": token_out, "deadline": deadline}
