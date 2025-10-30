import time
from typing import Any, Dict

from hummingbot.core.web_assistant.connections.data_types import WSResponse
from hummingbot.core.web_assistant.ws_post_processors import WSPostProcessorBase

try:
    # Prefer local compiled protobufs path
    from .pb import PushDataV3ApiWrapper_pb2 as PBPush
except Exception:  # pragma: no cover - fallback for alternative layouts
    PBPush = None  # type: ignore


class MexcWSPostProcessor(WSPostProcessorBase):
    async def post_process(self, response: WSResponse) -> WSResponse:
        message = response.data
        try:
            # Decode protobuf payloads into normalized event dicts
            if isinstance(message, (bytes, bytearray)) and PBPush is not None:
                event = self._decode_pb_payload(bytes(message))
                if event:
                    response.data = event
                    return response
                # If decoding fails, leave original payload

            # Normalize JSON dict shapes (ensure unified keys and important fields)
            if isinstance(message, dict):
                normalized = self._normalize_json_event_dict(message)
                response.data = normalized
                return response
        except Exception:
            # Do not raise; leave message untouched to preserve robustness
            return response

        return response

    def _decode_pb_payload(self, payload: bytes) -> Dict[str, Any]:
        try:
            wrapper = PBPush.PushDataV3ApiWrapper()
            wrapper.ParseFromString(payload)

            channel = wrapper.channel
            symbol = getattr(wrapper, 'symbol', '') or ''
            create_time = getattr(wrapper, 'createTime', 0) or 0
            send_time = getattr(wrapper, 'sendTime', 0) or 0
            ts = int(create_time or send_time or time.time() * 1000)

            # Public aggregated depth
            if wrapper.HasField('publicAggreDepths'):
                body = wrapper.publicAggreDepths
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {
                        "r": getattr(body, 'toVersion', ''),
                        "bids": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'bids', [])],
                        "asks": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'asks', [])],
                    },
                }

            # Public increase depth
            if wrapper.HasField('publicIncreaseDepths'):
                body = wrapper.publicIncreaseDepths
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {
                        "r": getattr(body, 'version', ''),
                        "bids": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'bids', [])],
                        "asks": [{"p": it.price, "v": it.quantity} for it in getattr(body, 'asks', [])],
                    },
                }

            # Public aggregated deals
            if wrapper.HasField('publicAggreDeals'):
                body = wrapper.publicAggreDeals
                deals = [{
                    "p": it.price,
                    "v": it.quantity,
                    "S": int(getattr(it, 'tradeType', 0)),
                    "t": int(getattr(it, 'time', ts)),
                } for it in getattr(body, 'deals', [])]
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {"deals": deals},
                }

            # Public plain deals
            if wrapper.HasField('publicDeals'):
                body = wrapper.publicDeals
                deals = [{
                    "p": it.price,
                    "v": it.quantity,
                    "S": int(getattr(it, 'tradeType', 0)),
                    "t": int(getattr(it, 'time', ts)),
                } for it in getattr(body, 'deals', [])]
                return {
                    "c": channel,
                    "s": symbol,
                    "t": ts,
                    "d": {"deals": deals},
                }

            # Private orders
            if wrapper.HasField('privateOrders'):
                body = wrapper.privateOrders
                return {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'createTime', 0) or 0),
                    "d": {
                        "i": str(getattr(body, 'id', '')),
                        "c": str(getattr(body, 'clientId', '')),
                        "p": str(getattr(body, 'price', '')),
                        "v": str(getattr(body, 'quantity', '')),
                        "V": str(getattr(body, 'quantity', '')),
                        "a": str(getattr(body, 'amount', '')),
                        "A": str(getattr(body, 'amount', '')),
                        "cv": str(getattr(body, 'cumulativeQuantity', '')),
                        "ca": str(getattr(body, 'cumulativeAmount', '')),
                        "ap": str(getattr(body, 'avgPrice', '')),
                        "ot": int(getattr(body, 'orderType', 0)),
                        "tt": int(getattr(body, 'tradeType', 0)),
                        "S": int(getattr(body, 'tradeType', 0)),
                        "m": bool(getattr(body, 'isMaker', False)),
                        "s": int(getattr(body, 'status', 0)),
                        "O": int(getattr(body, 'createTime', 0) or 0),
                    }
                }

            # Private deals
            if wrapper.HasField('privateDeals'):
                body = wrapper.privateDeals
                return {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'time', 0) or 0),
                    "d": {
                        "t": str(getattr(body, 'tradeId', '')),
                        "p": str(getattr(body, 'price', '')),
                        "v": str(getattr(body, 'quantity', '')),
                        "a": str(getattr(body, 'amount', '')),
                        "S": int(getattr(body, 'tradeType', 0)),
                        "i": str(getattr(body, 'orderId', '')),
                        "c": str(getattr(body, 'clientOrderId', '')),
                        "n": str(getattr(body, 'feeAmount', '')),
                        "N": str(getattr(body, 'feeCurrency', '')),
                        "T": int(getattr(body, 'time', 0) or 0),
                    }
                }

            # Private account (balances)
            if wrapper.HasField('privateAccount'):
                body = wrapper.privateAccount
                return {
                    "c": channel,
                    "s": symbol,
                    "t": int(getattr(body, 'time', 0) or 0),
                    "d": {
                        "a": str(getattr(body, 'vcoinName', '')),
                        "f": str(getattr(body, 'balanceAmount', '')),
                        "l": str(getattr(body, 'frozenAmount', '')),
                    }
                }
        except Exception:
            return {}

        return {}

    def _normalize_json_event_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Ensure unified keys for channel and payload
        channel = data.get('c') or data.get('channel')
        payload = data.get('d') if isinstance(data.get('d'), dict) else data.get('data')
        if channel is None and 'channel' in data:
            data['c'] = data.get('channel')
        if payload is None and 'data' in data and isinstance(data['data'], dict):
            data['d'] = data.get('data')

        # Ensure 'T' exists for private deals payloads when missing
        try:
            c_val = str(data.get('c', '') or data.get('channel', ''))
            d_val = data.get('d') or data.get('data') or {}
            if isinstance(d_val, dict) and 'private.deals' in c_val:
                if 'T' not in d_val:
                    if 'time' in d_val:
                        d_val['T'] = int(d_val.get('time') or 0)
                    elif 't' in d_val and str(d_val.get('t', '')).isdigit():
                        d_val['T'] = int(d_val.get('t') or 0)
                # write back
                if 'd' in data:
                    data['d'] = d_val
                elif 'data' in data:
                    data['data'] = d_val
        except Exception:
            pass

        return data


