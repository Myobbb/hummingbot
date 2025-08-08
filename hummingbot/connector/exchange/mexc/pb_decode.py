"""
Best-effort Protobuf decoding helpers for MEXC Spot v3 private streams.

This module attempts to import generated protobuf classes from the official
definitions (see https://github.com/mexcdevelop/websocket-proto). If the
generated modules are not available in the environment, all functions will
return None and callers should fallback to existing REST or heuristic paths.

Expected generated modules (place alongside this file or install as package):
 - PrivateAccountV3Api_pb2.py
 - PrivateOrdersV3Api_pb2.py
 - PrivateDealsV3Api_pb2.py

All parse_* functions return a dict matching the legacy JSON 'd' payload used
by downstream Hummingbot code, or None on failure.
"""

from typing import Optional, Dict

_has_proto = True
try:
    # These names follow protoc --python_out defaults for file names
    from . import PrivateAccountV3Api_pb2 as acct_pb
    from . import PrivateOrdersV3Api_pb2 as ord_pb
    from . import PrivateDealsV3Api_pb2 as deals_pb
except Exception:
    _has_proto = False
    acct_pb = None
    ord_pb = None
    deals_pb = None


def parse_account_pb(frame: bytes) -> Optional[Dict]:
    """Parse account protobuf frame into legacy JSON 'd' fields.

    Returns dict like: { 'a': asset, 'f': free, 'l': locked, 'o': type }
    or None if parsing fails or proto modules not present.
    """
    if not _has_proto:
        return None
    try:
        msg = acct_pb.PrivateAccount()
        msg.ParseFromString(frame)
        # Field names per docs: vcoinName, balanceAmount, frozenAmount, type
        asset = (msg.vcoinName or "").upper()
        bal = float(msg.balanceAmount) if msg.balanceAmount else 0.0
        frozen = float(msg.frozenAmount) if msg.frozenAmount else 0.0
        free = max(0.0, bal - frozen)
        op_type = msg.type if hasattr(msg, 'type') else ''
        return {
            'a': asset,
            'f': str(free),
            'l': str(frozen),
            'o': op_type,
        }
    except Exception:
        return None


def parse_orders_pb(frame: bytes) -> Optional[Dict]:
    """Parse orders protobuf frame into legacy JSON 'd' fields.

    Returns a minimal dict containing keys used downstream:
      - 'i' (orderId), 'c' (clientOrderId), 'S' (side numeric), 's' (status numeric)
      Optionally include 'cv', 'ca', 'ap' if present.
    """
    if not _has_proto:
        return None
    try:
        msg = ord_pb.PrivateOrders()
        msg.ParseFromString(frame)
        # Field guesses based on docs; adjust to actual proto fields when available
        payload: Dict[str, str] = {}
        if hasattr(msg, 'orderId'):
            payload['i'] = str(msg.orderId)
        if hasattr(msg, 'clientOrderId'):
            payload['c'] = str(msg.clientOrderId)
        if hasattr(msg, 'side'):
            payload['S'] = int(msg.side)
        if hasattr(msg, 'status'):
            payload['s'] = int(msg.status)
        if hasattr(msg, 'cumulativeVolume'):
            payload['cv'] = str(msg.cumulativeVolume)
        if hasattr(msg, 'cumulativeAmount'):
            payload['ca'] = str(msg.cumulativeAmount)
        if hasattr(msg, 'avgPrice'):
            payload['ap'] = str(msg.avgPrice)
        return payload or None
    except Exception:
        return None


def parse_deals_pb(frame: bytes) -> Optional[Dict]:
    """Parse deals protobuf frame into legacy JSON 'd' fields used by trade handler.

    Returns a minimal dict containing keys used in _process_trade_message:
      - expects 'c' client order id; map available fields accordingly.
    """
    if not _has_proto:
        return None
    try:
        msg = deals_pb.PrivateDeals()
        msg.ParseFromString(frame)
        payload: Dict[str, str] = {}
        if hasattr(msg, 'clientOrderId'):
            payload['c'] = str(msg.clientOrderId)
        # Include useful trade fields if available
        if hasattr(msg, 'price'):
            payload['p'] = str(msg.price)
        if hasattr(msg, 'quoteAmount'):
            payload['a'] = str(msg.quoteAmount)
        if hasattr(msg, 'baseAmount'):
            payload['v'] = str(msg.baseAmount)
        if hasattr(msg, 'tradeId'):
            payload['t'] = str(msg.tradeId)
        if hasattr(msg, 'side'):
            payload['S'] = int(msg.side)
        return payload or None
    except Exception:
        return None


