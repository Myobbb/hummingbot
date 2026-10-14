"""P2-DEX build step 4 (2026-10-14): a swap transaction around its calldata (bsc_swap): the approvals it needs, the signed
transaction, and what its receipt says. Nothing here sends: the send method is its own session's (Pavel 2026-10-14), and so
are the nonce and gas POLICIES; this module only takes a nonce, a gas limit and a gas price as given.

- Approvals. A v2-style router pulls the input token by ERC20 allowance (token -> router). A Universal Router pulls it through
  Permit2: ERC20 allowance token -> Permit2, then Permit2's own allowance (wallet, token, router), with an expiry. Each is one
  transaction per token, once (max amounts, the expiry at its maximum).
- The transaction: legacy type 0, EIP-155 (chain id 56), signed locally by the wallet's signer (eth_account).
- The receipt: status 0 = the swap reverted (a failed order; the gas is still paid). Otherwise the fill is read from the ERC20
  Transfer logs of the two tokens to and from the wallet, so it is what really moved, net of any token tax."""
from typing import Dict, List, Optional, Tuple

from eth_utils import to_checksum_address

CHAIN_ID = 56
MAX_UINT256 = (1 << 256) - 1
MAX_UINT160 = (1 << 160) - 1
MAX_UINT48 = (1 << 48) - 1
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"   # Transfer(address,address,uint256)
SEL_APPROVE = bytes.fromhex("095ea7b3")              # approve(address,uint256)
SEL_ALLOWANCE = bytes.fromhex("dd62ed3e")            # allowance(address,address)
SEL_P2_APPROVE = bytes.fromhex("87517c45")           # Permit2.approve(address,address,uint160,uint48)
SEL_P2_ALLOWANCE = bytes.fromhex("927da105")         # Permit2.allowance(address,address,address)


def _w(v: int) -> bytes:
    return int(v).to_bytes(32, "big")


def _a(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


# ---------------------------------------------------------------- approvals
def approve_call(token: str, spender: str, amount: int = MAX_UINT256) -> Tuple[str, bytes]:
    return token, SEL_APPROVE + _a(spender) + _w(amount)


def permit2_approve_call(permit2: str, token: str, spender: str, amount: int = MAX_UINT160,
                         expiration: int = MAX_UINT48) -> Tuple[str, bytes]:
    return permit2, SEL_P2_APPROVE + _a(token) + _a(spender) + _w(amount) + _w(expiration)


def allowance_calls(wallet: str, token: str, executor: dict) -> List[Tuple[str, bytes]]:
    """the reads that tell whether `wallet` can pay `token` into `executor` ({"router", "permit2" or None})"""
    if executor.get("permit2"):
        return [(token, SEL_ALLOWANCE + _a(wallet) + _a(executor["permit2"])),
                (executor["permit2"], SEL_P2_ALLOWANCE + _a(wallet) + _a(token) + _a(executor["router"]))]
    return [(token, SEL_ALLOWANCE + _a(wallet) + _a(executor["router"]))]


def missing_approvals(wallet: str, token: str, executor: dict, answers: List[Tuple[bool, bytes]], amount: int,
                      now: int) -> List[Tuple[str, bytes]]:
    """the approval calls still needed to pay `amount` of `token`, from allowance_calls' answers (an unreadable answer counts
    as missing)"""
    def num(i, word=0):
        ok, ret = answers[i]
        return int.from_bytes(ret[32 * word: 32 * word + 32], "big") if ok and len(ret) >= 32 * (word + 1) else None
    need = []
    if executor.get("permit2"):
        if (num(0) or 0) < amount:
            need.append(approve_call(token, executor["permit2"]))
        p2_amount, p2_expiry = num(1, 0), num(1, 1)
        if p2_amount is None or p2_amount < amount or (p2_expiry or 0) <= now:
            need.append(permit2_approve_call(executor["permit2"], token, executor["router"]))
    elif (num(0) or 0) < amount:
        need.append(approve_call(token, executor["router"]))
    return need


# ---------------------------------------------------------------- the transaction
def build_tx(to: str, data: bytes, value: int, nonce: int, gas: int, gas_price: int) -> Dict:
    """a legacy (type 0) EIP-155 transaction, ready to sign (eth_account wants a checksummed `to`)"""
    return {"to": to_checksum_address(to), "data": "0x" + data.hex(), "value": int(value), "nonce": int(nonce), "gas": int(gas),
            "gasPrice": int(gas_price), "chainId": CHAIN_ID}


def sign(account, tx: Dict) -> Tuple[str, str]:
    """-> (raw transaction hex, transaction hash hex), signed locally by the wallet's signer"""
    signed = account.sign_transaction(tx)
    raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction        # eth_account >= 0.13 / older
    return "0x" + bytes(raw).hex(), "0x" + bytes(signed.hash).hex()


# ---------------------------------------------------------------- the receipt
def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def parse_receipt(receipt: Dict, wallet: str, token_in: str, token_out: str) -> Dict:
    """-> {"status": "filled" | "reverted", "amount_in", "amount_out" (raw units, what really left / reached the wallet),
    "gas_fee_wei", "block"}. Only the two tokens' Transfer logs to and from the wallet count."""
    wallet, token_in, token_out = wallet.lower(), token_in.lower(), token_out.lower()
    gas_fee = int(receipt["gasUsed"], 16) * int(receipt.get("effectiveGasPrice") or receipt.get("gasPrice") or "0x0", 16)
    out = {"status": "reverted" if int(receipt["status"], 16) == 0 else "filled", "amount_in": 0, "amount_out": 0,
           "gas_fee_wei": gas_fee, "block": int(receipt["blockNumber"], 16)}
    if out["status"] == "reverted":
        return out
    for log in receipt.get("logs") or []:
        topics = log.get("topics") or []
        if len(topics) != 3 or topics[0].lower() != TRANSFER_TOPIC:
            continue
        token, src, dst = log["address"].lower(), _topic_address(topics[1]), _topic_address(topics[2])
        value = int(log["data"], 16) if log.get("data") not in (None, "0x") else 0
        if token == token_in:
            out["amount_in"] += value if src == wallet else (-value if dst == wallet else 0)
        if token == token_out:
            out["amount_out"] += value if dst == wallet else (-value if src == wallet else 0)
    return out


def receipt_problem(parsed: Dict) -> Optional[str]:
    """why a 'filled' receipt can't be booked as a fill, or None"""
    if parsed["status"] != "filled":
        return None
    if parsed["amount_in"] <= 0 or parsed["amount_out"] <= 0:
        return f"no transfer of both tokens to and from the wallet (in {parsed['amount_in']}, out {parsed['amount_out']})"
    return None
