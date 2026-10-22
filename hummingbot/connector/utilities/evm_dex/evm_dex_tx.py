"""P2-DEX build step 4 (2026-10-14; the EVM core since 2026-10-22): a swap transaction around its calldata (evm_dex_swap): the
approvals it needs, the signed transaction, and what its receipt says, on any EVM chain (its id from its spec, `C`). Nothing
here sends: the send method is its own session's (Pavel 2026-10-14), and so are the nonce and gas POLICIES; this module only
takes a nonce, a gas limit and a gas price as given.

- Approvals. A v2-style router pulls the input token by ERC20 allowance (token -> router). A Universal Router pulls it through
  Permit2: ERC20 allowance token -> Permit2, then Permit2's own allowance (wallet, token, router), with an expiry. Each is one
  transaction per token, once (max amounts, the expiry at its maximum).
- The transaction: legacy type 0, EIP-155 (the spec's CHAIN_ID; 56 on BSC), signed locally with the wallet's key: by libsecp256k1 directly
  (coincurve), the same bytes as eth_account at a third of its time, or by eth_account when that does not check out
  at import (sign, SIGNER).
- The receipt: status 0 = the swap reverted (a failed order; the gas is still paid). Otherwise the fill is read from the ERC20
  Transfer logs of the two tokens to and from the wallet, so it is what really moved, net of any token tax."""
from typing import Dict, List, Optional, Tuple

from eth_hash.auto import keccak
from eth_utils import to_checksum_address

try:
    from coincurve import PrivateKey
except ImportError:                                  # sign() falls back to eth_account
    PrivateKey = None

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


def approval_parties(to: str, data: bytes) -> Tuple[str, str]:
    """(the token, the spender) an approve_call or permit2_approve_call grants, lowercase: what the swap ledger records"""
    if data[:4] == SEL_APPROVE:
        return to.lower(), "0x" + data[16:36].hex()
    if data[:4] == SEL_P2_APPROVE:
        return "0x" + data[16:36].hex(), "0x" + data[48:68].hex()
    raise ValueError(f"not an approval call: {data[:4].hex()}")


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
def build_tx(C, to: str, data: bytes, value: int, nonce: int, gas: int, gas_price: int) -> Dict:
    """a legacy (type 0) EIP-155 transaction on the spec's chain, ready to sign (eth_account wants a checksummed `to`)"""
    return _legacy(to, data, value, nonce, gas, gas_price, C.CHAIN_ID)


def _legacy(to: str, data: bytes, value: int, nonce: int, gas: int, gas_price: int, chain_id: int) -> Dict:
    return {"to": to_checksum_address(to), "data": "0x" + data.hex(), "value": int(value), "nonce": int(nonce), "gas": int(gas),
            "gasPrice": int(gas_price), "chainId": int(chain_id)}


def sign(account, tx: Dict) -> Tuple[str, str]:
    """-> (raw transaction hex, transaction hash hex), a build_tx transaction signed locally with the wallet's key: by
    libsecp256k1 directly when it gave eth_account's exact bytes at import (SIGNER), else by eth_account (Pavel 2026-10-17:
    "might as well improve ... signer"; eth_account took 0.36 ms p50 of a swap's placement, nearly all of the child's)"""
    if FAST_SIGNER:
        return _sign_direct(bytes(account.key), tx)
    return _sign_eth_account(account, tx)


def _sign_eth_account(account, tx: Dict) -> Tuple[str, str]:
    signed = account.sign_transaction(tx)
    raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction        # eth_account >= 0.13 / older
    return "0x" + bytes(raw).hex(), "0x" + bytes(signed.hash).hex()


def _rlp_head(length: int, offset: int) -> bytes:
    if length < 56:
        return bytes((offset + length,))
    size = length.to_bytes((length.bit_length() + 7) // 8, "big")
    return bytes((offset + 55 + len(size),)) + size


def _rlp_bytes(b: bytes) -> bytes:
    return b if len(b) == 1 and b[0] < 0x80 else _rlp_head(len(b), 0x80) + b


def _rlp_int(v: int) -> bytes:
    return _rlp_bytes(v.to_bytes((v.bit_length() + 7) // 8, "big"))           # 0 is the empty string


def _rlp_list(items: List[bytes]) -> bytes:
    body = b"".join(items)
    return _rlp_head(len(body), 0xc0) + body


def _sign_direct(key: bytes, tx: Dict) -> Tuple[str, str]:
    """a legacy EIP-155 transaction as eth_account signs it, without its object model: the RLP of its fields with the chain
    id, its keccak signed by libsecp256k1 (RFC 6979 nonce, low s: eth_keys' own backend), v = recovery id + 35 + 2 x chain"""
    fields = [_rlp_int(tx["nonce"]), _rlp_int(tx["gasPrice"]), _rlp_int(tx["gas"]), _rlp_bytes(bytes.fromhex(tx["to"][2:])),
              _rlp_int(tx["value"]), _rlp_bytes(bytes.fromhex(tx["data"][2:]))]
    chain = tx["chainId"]
    sig = PrivateKey(key).sign_recoverable(keccak(_rlp_list(fields + [_rlp_int(chain), b"\x80", b"\x80"])), hasher=None)
    raw = _rlp_list(fields + [_rlp_int(sig[64] + 35 + 2 * chain), _rlp_int(int.from_bytes(sig[:32], "big")),
                              _rlp_int(int.from_bytes(sig[32:64], "big"))])
    return "0x" + raw.hex(), "0x" + keccak(raw).hex()


def _check_direct() -> Tuple[bool, str]:
    """the direct signer is used only if it gives eth_account's exact bytes here, at import: a few transactions shaped like
    ours (a swap's calldata, an approval's, none) with a throwaway key -> (used, the signer in words)"""
    if PrivateKey is None:
        return False, "eth_account (coincurve is not installed)"
    try:
        from eth_account import Account
        account = Account.from_key(bytes(range(1, 33)))
        for tx in (_legacy("0x" + "5c" * 20, bytes(range(256)) * 2 + bytes(68), 0, 0, 350_000, 50_000_000, 56),
                   _legacy("0x00000000000000000000000000000000000000ff", bytes(68), 0, 127, 70_000, 1, 56),
                   _legacy("0x" + "ff" * 20, b"", 10 ** 18, 2 ** 40, 21_000, 10 ** 12, 56)):
            if _sign_direct(bytes(account.key), tx) != _sign_eth_account(account, tx):
                return False, "eth_account (the direct signer's bytes differ from eth_account's)"
    except Exception as e:
        return False, f"eth_account (the direct signer did not check out: {type(e).__name__}: {e})"
    return True, "libsecp256k1 (coincurve), the same bytes as eth_account"


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


FAST_SIGNER, SIGNER = _check_direct()
