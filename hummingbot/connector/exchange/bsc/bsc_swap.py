"""P2-DEX build step 4 (2026-10-14): the swap calldata of one order, through the executor of its pools' DEX family.

An order is an EXACT-INPUT swap along the book's route, the one that priced it (bsc_quoter: the book's `record` and its hop
`routes` per rung):
  buy  (the CA bought): USDT in -> the hops back to the counter -> the CA pool -> the CA out, to the wallet;
  sell (the CA sold):   the CA in -> the CA pool -> the counter -> the hops to USDT -> USDT out, to the wallet.
Exact input everywhere: the v2 fee-on-transfer swaps and every pool type support it, and the wallet always pays a known amount.
The minimum output is the last pool's own output check; intermediate legs carry 0 (the final minimum covers the whole path).

Executors (decision 2: official routers; one call chains one family only, Q5):
  - pancakeswap: PancakeSwap's Universal Router (v2, v3, Infinity CL), paid through Permit2. (Infinity Bin: P1 lists no Bin pool
    as of 2026-10-14, so no encoding ships unproven; a Bin pool is left out as "no executor" until one exists to prove it on);
  - uniswap:     Uniswap's Universal Router (v2, v3, v4), paid through Permit2;
  - route:<v2 router>: that router's swapExactTokensForTokensSupportingFeeOnTransferTokens, paid by ERC20 allowance, for a
    router pinned in PINNED_V2_ROUTERS only (none yet: Pavel 2026-10-14, PancakeSwap + Uniswap only).
Native BNB: a v4 / Infinity pool may hold native BNB (address 0) where its neighbours hold WBNB; the router wraps or unwraps
between them. Taxed tokens: v2 legs measure by balance (tax-safe); a taxed token paid into a v3 / v4 / Infinity pool makes the
swap revert (decision 2's known downside), and a taxed output arrives short by the tax P1 already prices.

The encodings are the routers' AS DEPLOYED ON BSC (sources pinned 2026-10-14; wiki P2-DEX Open Items §step 4):
  - Uniswap Universal Router 2.0.0 (0x1906…, the one the official v4 and v3 deployment pages list): V2/V3 swap inputs of 5
    fields; v4 ExactInputSingleParams without a per-hop field. (Uniswap's 2.1.x routers on BSC take a 6th field: not these.)
  - PancakeSwap Infinity Universal Router (0xd9C5…, deployed from commit b303b14): the same V2/V3 inputs; INFI_SWAP (0x10)
    with Infinity's actions; paid through PancakeSwap's own Permit2.
Every encoding is proven by simulating the swap from the wallet on live pools (hmb_local_tools/p2_dex/step4_sim)."""
import time
from typing import Dict, List, Optional, Tuple

from eth_abi import encode

NATIVE = "0x0000000000000000000000000000000000000000"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
USDT = "0x55d398326f99059ff775485246999027b3197955"

EXECUTORS = {
    "pancakeswap": {"router": "0xd9c500dff816a1da21a48a732d3498bf09dc9aeb",      # PancakeSwap Infinity Universal Router
                    "permit2": "0x31c2f6fcff4f8759b3bd5bf0e1084a055615c768",     # PancakeSwap's own Permit2 (BSC)
                    "types": ("v2", "v3", "inf-cl")},
    "uniswap": {"router": "0x1906c1d672b88cd1b9ac7593301ca990f94eae07",          # Uniswap Universal Router 2.0.0
                "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",         # Uniswap's Permit2
                "types": ("v2", "v3", "uni-v4")},
}
V2_ROUTER_TYPES = ("v2",)                    # a family `route:<v2 router>`: that router, its fee-on-transfer swap
# The v2 routers of single-router forks the wallet may approve and swap through. Pinned by hand, once each one is checked
# (Pavel 2026-10-14: PancakeSwap + Uniswap only): none yet, so a `route:` pool is left out as "no executor". Every contract the
# wallet ever approves comes from EXECUTORS or this set, never from P1's data.
PINNED_V2_ROUTERS: Tuple[str, ...] = ()

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
DEADLINE_S = 5                                # an arb leg not mined within ~5 s is stale (Pavel 2026-10-14). block.timestamp
                                             # has 1-s resolution (0.45 s blocks): a bundle's maxBlockNumber would be the
                                             # block-precise expiry, if the Sending session takes one


def canon(token: str) -> str:
    """native BNB and WBNB are one token for continuity (the router converts between them)"""
    token = (token or "").lower()
    return WBNB if token == NATIVE else token


def legs(record: dict, hops: List[dict], side: str) -> List[Tuple[dict, str, str]]:
    """the swap's pools in execution order, each (pool record, token in, token out), tokens as the POOL holds them (native
    BNB stays address 0). `hops` run from the counter toward USDT (bsc_quoter._hop_route), each with from_token / to_token."""
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


def check_legs(path: List[Tuple[dict, str, str]], token_in: str, token_out: str) -> None:
    """raises when the legs don't chain token_in -> ... -> token_out (native BNB = WBNB for chaining)"""
    if canon(path[0][1]) != canon(token_in) or canon(path[-1][2]) != canon(token_out):
        raise ValueError(f"the route runs {path[0][1]} -> {path[-1][2]}, not {token_in} -> {token_out}")
    for (_, _, out_a), (_, in_b, _) in zip(path, path[1:]):
        if canon(out_a) != canon(in_b):
            raise ValueError(f"the route breaks between {out_a} and {in_b}")


def executor(record: dict) -> Optional[Dict]:
    """the contract that executes a pool of this record's family -> {"router", "permit2" (None: paid by ERC20 allowance),
    "types"}, or None when P2 has no executor for it: a family outside EXECUTORS, or a v2 router not in PINNED_V2_ROUTERS"""
    family = record.get("family") or ""
    if family in EXECUTORS:
        return EXECUTORS[family]
    router = family[len("route:"):]
    if (family.startswith("route:") and (record.get("route") or {}).get("kind") == "v2 router"
            and router in PINNED_V2_ROUTERS):
        return {"router": router, "permit2": None, "types": V2_ROUTER_TYPES}
    return None


def no_executor(record: dict) -> Optional[str]:
    """why P2 can't execute this pool, or None"""
    ex = executor(record)
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


def _pool_swap(pool: dict, tin: str, tout: str, amount: int, minimum: int, first: bool, last: bool) -> bytes:
    """one v4 / Infinity pool as a POOL_SWAP input: (actions, params). The wallet pays the first pool (SETTLE_ALL after the
    swap, through Permit2); a later pool is paid from the router's balance (SETTLE of CONTRACT_BALANCE first, then the swap
    of the whole credit). The last pool's output goes to the wallet (TAKE_ALL with the minimum), any other to the router."""
    zero_for_one = tin.lower() == pool["token0"].lower()
    if pool["type"] == "uni-v4":
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


def build_swap(record: dict, hops: List[dict], side: str, amount_in: int, min_out: int, wallet: str,
               deadline: Optional[int] = None) -> Dict:
    """-> {"to", "value", "data", "token_in", "token_out", "executor", "deadline"}: one exact-input swap of `amount_in` (raw
    units of the token paid) along the book's route, the last pool's output at least `min_out` (raw), delivered to `wallet`,
    which sends it (a Universal Router names it MSG_SENDER; a v2 router needs the address), void after `deadline` (unix s).
    Raises ValueError for a route P2 can't execute (see no_executor) or that doesn't chain."""
    path = legs(record, hops, side)
    token_in, token_out = (USDT, record["ca"].lower()) if side == "buy" else (record["ca"].lower(), USDT)
    check_legs(path, token_in, token_out)
    for pool, _, _ in path:
        problem = no_executor(dict(pool, family=record.get("family")))
        if problem:
            raise ValueError(problem)
    ex = executor(record)
    deadline = int(deadline if deadline is not None else time.time() + DEADLINE_S)
    if ex["permit2"] is None:                                        # a v2 router: one fee-on-transfer swap over the path
        tokens = [canon(path[0][1])] + [canon(t) for _, _, t in path]
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
    for i, (eng, seg) in enumerate(segments):
        first, last = i == 0, i == len(segments) - 1
        tin, tout = seg[0][1], seg[-1][2]
        if not first:                                                # native BNB <-> WBNB between two pools
            prev_out = segments[i - 1][1][-1][2]
            if prev_out == WBNB and tin == NATIVE:
                commands.append(UNWRAP_WETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, 0]))
            elif prev_out == NATIVE and tin == WBNB:
                commands.append(WRAP_ETH); inputs.append(encode(["address", "uint256"], [ADDRESS_THIS, CONTRACT_BALANCE]))
        amount = amount_in if first else CONTRACT_BALANCE
        recipient = MSG_SENDER if last else ADDRESS_THIS
        minimum = min_out if last else 0
        if eng == "v2":
            commands.append(V2_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "address[]", "bool"],
                                 [recipient, amount, minimum, [tin] + [t for _, _, t in seg], first]))
        elif eng == "v3":
            packed = bytes.fromhex(tin[2:])
            for pool, _, t in seg:
                packed += int(pool["key"]["fee"]).to_bytes(3, "big") + bytes.fromhex(t[2:])
            commands.append(V3_SWAP_EXACT_IN)
            inputs.append(encode(["address", "uint256", "uint256", "bytes", "bool"], [recipient, amount, minimum, packed, first]))
        else:
            pool, _, _ = seg[0]
            commands.append(POOL_SWAP)
            inputs.append(_pool_swap(pool, tin, tout, amount, minimum, first, last))
    data = SEL_EXECUTE + encode(["bytes", "bytes[]", "uint256"], [bytes(commands), inputs, deadline])
    return {"to": ex["router"], "value": 0, "data": data, "token_in": token_in, "token_out": token_out, "executor": ex,
            "deadline": deadline}
