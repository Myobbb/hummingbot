"""P2-DEX build step 4 (2026-10-14; the EVM core since 2026-10-22): the swap calldata of one order, through the executor of its
pools' DEX family, on any EVM chain. The chain's facts come from its spec (`C`, the chain's constants module: its USDT, its
wrapped native token, its EXECUTORS and PINNED_V2_ROUTERS, DEADLINE_S); nothing here is a chain's own.

An order is an EXACT-INPUT swap along the book's route, the one that priced it (evm_dex_quoter: the book's `record` and its hop
`routes` per rung):
  buy  (the CA bought): USDT in -> the hops back to the counter -> the CA pool -> the CA out, to the wallet;
  sell (the CA sold):   the CA in -> the CA pool -> the counter -> the hops to USDT -> USDT out, to the wallet.
Exact input everywhere: the v2 fee-on-transfer swaps and every pool type support it, and the wallet always pays a known amount.
The minimum output is the last pool's own output check; intermediate legs carry 0 (the final minimum covers the whole path).
A route's first legs alone (build_prefix) are built the same way: evm_dex_feed simulates them after a swap fails, to name the
pool it broke at (P2-DEX #rad-blacklisted-pool).

Executors (decision 2: official routers; one call chains one family only, Q5), pinned per chain in its spec:
  - a Universal-Router family (`C.EXECUTORS`): its router (v2, v3 and its v4-type pools), paid through its Permit2;
  - route:<v2 router>: that router's swapExactTokensForTokensSupportingFeeOnTransferTokens, paid by ERC20 allowance, for a
    router pinned in the spec's PINNED_V2_ROUTERS only.
Native coin: a v4 / Infinity pool may hold the chain's native coin (address 0) where its neighbours hold the wrapped token; the
router wraps or unwraps between them. Taxed tokens: v2 legs measure by balance (tax-safe); a taxed token paid into a v3 / v4 /
Infinity pool makes the swap revert (decision 2's known downside), and a taxed output arrives short by the tax P1 already prices.

BSC's encodings (sources pinned 2026-10-14; wiki P2-DEX Open Items §step 4), the ones built here:
  - Uniswap Universal Router 2.0.0 (0x1906…, the one the official v4 and v3 deployment pages list): V2/V3 swap inputs of 5
    fields; v4 ExactInputSingleParams without a per-hop field. (Uniswap's 2.1.x routers take a 6th field: not these.)
  - PancakeSwap Infinity Universal Router (0xd9C5…, deployed from commit b303b14): the same V2/V3 inputs; INFI_SWAP (0x10)
    with Infinity's actions; paid through PancakeSwap's own Permit2.
ETH's (sources pinned 2026-10-22: Uniswap/universal-router 2.1.2 = 802fe4c1, its v4-periphery 545a5d2a; wiki p2-dex-eth
§executors): Universal Router 2.1.2 (0x2361…De85), an executor whose "encoding" is "ur-2.1": the V2/V3 swap inputs take a 6th
field, `uint256[] minHopPriceX36` (empty: off), and v4's ExactInputSingleParams a `uint256 minHopPriceX36` before hookData (0:
off); the command and action bytes are 2.0.0's. An executor without "encoding" is "ur-2.0" (BSC's routers).
Every encoding is proven by simulating the swap from the wallet on live pools (hmb_local_tools/p2_dex/step4_sim, ETH's in
p2_dex_eth)."""
import time
from typing import Dict, List, Optional, Tuple

from eth_abi import encode

NATIVE = "0x0000000000000000000000000000000000000000"
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
    "types"}, or None when P2 has no executor for it: a family outside the spec's EXECUTORS, or a v2 router not in its
    PINNED_V2_ROUTERS"""
    family = record.get("family") or ""
    if family in C.EXECUTORS:
        return C.EXECUTORS[family]
    router = family[len("route:"):]
    if (family.startswith("route:") and (record.get("route") or {}).get("kind") == "v2 router"
            and router in C.PINNED_V2_ROUTERS):
        return {"router": router, "permit2": None, "types": V2_ROUTER_TYPES}
    return None


def no_executor(C, record: dict) -> Optional[str]:
    """why P2 can't execute this pool, or None"""
    ex = executor(C, record)
    if ex is None:
        family = record.get("family") or (record.get("route") or {}).get("kind")
        pin = " (its router is not pinned)" if (record.get("family") or "").startswith("route:") else ""
        return f"no executor for {record.get('type')} on {family}{pin}"
    if record.get("type") not in ex["types"]:
        return f"no executor for {record.get('type')} on {record.get('family')}"
    return None


def _engine(pool: dict) -> str:
    return {"v2": "v2", "v3": "v3"}.get(pool["type"], pool["type"])


def _pool_key(pool: dict):
    k = pool.get("key") or {}
    if pool["type"] == "uni-v4":
        return (pool["token0"], pool["token1"], int(k["fee"]), int(k["tickSpacing"]), k["hooks"])
    return (pool["token0"], pool["token1"], k["hooks"], k["manager"], int(k["fee"]), bytes.fromhex(k["parameters"][2:].rjust(64, "0")))


def _pool_swap(pool: dict, tin: str, tout: str, amount: int, minimum: int, first: bool, last: bool, v21: bool = False) -> bytes:
    """one v4 / Infinity pool as a POOL_SWAP input: (actions, params). The wallet pays the first pool (SETTLE_ALL after the
    swap, through Permit2); a later pool is paid from the router's balance (SETTLE of CONTRACT_BALANCE first, then the swap
    of the whole credit). The last pool's output goes to the wallet (TAKE_ALL with the minimum), any other to the router.
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


def build_swap(C, record: dict, hops: List[dict], side: str, amount_in: int, min_out: int, wallet: str,
               deadline: Optional[int] = None) -> Dict:
    """-> {"to", "value", "data", "token_in", "token_out", "executor", "deadline"}: one exact-input swap of `amount_in` (raw
    units of the token paid) along the book's route, the last pool's output at least `min_out` (raw), delivered to `wallet`,
    which sends it (a Universal Router names it MSG_SENDER; a v2 router needs the address), void after `deadline` (unix s).
    Raises ValueError for a route P2 can't execute (see no_executor) or that doesn't chain."""
    path = legs(record, hops, side)
    token_in, token_out = (C.USDT, record["ca"].lower()) if side == "buy" else (record["ca"].lower(), C.USDT)
    return _route_call(C, record, path, token_in, token_out, amount_in, min_out, wallet, deadline)


def build_prefix(C, record: dict, hops: List[dict], side: str, n: int, amount_in: int, wallet: str,
                 deadline: Optional[int] = None) -> Dict:
    """the swap's first `n` legs as a swap of their own (build_swap's dict, `token_out` the n-th pool's output): `amount_in`
    of the order's input, at least 1 raw unit out to `wallet`. Never sent: evm_dex_feed simulates these to name the pool a
    route that reverts breaks at (n = every leg: the whole route at a minimum of 1)"""
    path = legs(record, hops, side)
    if not 1 <= n <= len(path):
        raise ValueError(f"no prefix of {n} legs in a route of {len(path)}")
    token_in = C.USDT if side == "buy" else record["ca"].lower()
    return _route_call(C, record, path[:n], token_in, path[n - 1][2], amount_in, 1, wallet, deadline)


def _route_call(C, record: dict, path: List[Tuple[dict, str, str]], token_in: str, token_out: str, amount_in: int,
                min_out: int, wallet: str, deadline: Optional[int]) -> Dict:
    """the executor's call for these legs (build_swap's dict)"""
    check_legs(C, path, token_in, token_out)
    for pool, _, _ in path:
        problem = no_executor(C, dict(pool, family=record.get("family")))
        if problem:
            raise ValueError(problem)
    ex = executor(C, record)
    deadline = int(deadline if deadline is not None else time.time() + C.DEADLINE_S)
    if ex["permit2"] is None:                                        # a v2 router: one fee-on-transfer swap over the path
        tokens = [canon(C, path[0][1])] + [canon(C, t) for _, _, t in path]
        data = SEL_V2_FOT + encode(["uint256", "uint256", "address[]", "address", "uint256"],
                                   [amount_in, min_out, tokens, wallet, deadline])
        return {"to": ex["router"], "value": 0, "data": data, "token_in": token_in, "token_out": token_out, "executor": ex,
                "deadline": deadline}
    segments = []                                                    # consecutive v2 legs chain in one command, v3 legs too
    for leg in path:
        eng = _engine(leg[0])
        if segments and segments[-1][0] == eng and eng in ("v2", "v3"):
            segments[-1][1].append(leg)
        else:
            segments.append((eng, [leg]))
    commands, inputs = [], []
    v21 = ex.get("encoding") == "ur-2.1"                             # Universal Router 2.1.x: a 6th field in the v2 / v3 inputs
    hop_price = (["uint256[]"], [[]]) if v21 else ([], [])           # minHopPriceX36: empty, off (the minimum guards the path)
    for i, (eng, seg) in enumerate(segments):
        first, last = i == 0, i == len(segments) - 1
        tin, tout = seg[0][1], seg[-1][2]
        if not first:                                                # the native coin <-> its wrapped token between two pools
            prev_out = segments[i - 1][1][-1][2]
            if prev_out == C.WRAPPED and tin == NATIVE:
                commands.append(UNWRAP_WETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, 0]))
            elif prev_out == NATIVE and tin == C.WRAPPED:
                commands.append(WRAP_ETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, CONTRACT_BALANCE]))
        amount = amount_in if first else CONTRACT_BALANCE
        recipient = MSG_SENDER if last else ADDRESS_THIS
        minimum = min_out if last else 0
        if eng == "v2":
            commands.append(V2_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "address[]", "bool"] + hop_price[0],
                                 [recipient, amount, minimum, [tin] + [t for _, _, t in seg], first] + hop_price[1]))
        elif eng == "v3":
            packed = bytes.fromhex(tin[2:])
            for pool, _, t in seg:
                packed += int(pool["key"]["fee"]).to_bytes(3, "big") + bytes.fromhex(t[2:])
            commands.append(V3_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "bytes", "bool"] + hop_price[0],
                                 [recipient, amount, minimum, packed, first] + hop_price[1]))
        else:
            pool, _, _ = seg[0]
            commands.append(POOL_SWAP)
            inputs.append(_pool_swap(pool, tin, tout, amount, minimum, first, last, v21))
    data = SEL_EXECUTE + encode(["bytes", "bytes[]", "uint256"], [bytes(commands), inputs, deadline])
    return {"to": ex["router"], "value": 0, "data": data, "token_in": token_in, "token_out": token_out, "executor": ex,
            "deadline": deadline}
