from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BybitOrderBook(OrderBook):
    @classmethod
    def _tp(cls, msg: Dict[str, any], metadata: Optional[Dict]) -> Optional[str]:
        return (metadata or {}).get("trading_pair") or msg.get("trading_pair") or msg.get("s")

    @classmethod
    def snapshot_message_from_exchange_websocket(
        cls,
        msg: Dict[str, any],
        timestamp: float,
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        payload = {
            "trading_pair": cls._tp(msg, metadata),
            "update_id": msg.get("u"),
            "bids": msg.get("b") or [],
            "asks": msg.get("a") or [],
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, payload, timestamp=timestamp)

    @classmethod
    def snapshot_message_from_exchange_rest(
        cls,
        msg: Dict[str, any],
        timestamp: float,
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        payload = {
            "trading_pair": cls._tp(msg, metadata),
            "update_id": msg.get("u"),
            "bids": msg.get("b") or [],
            "asks": msg.get("a") or [],
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, payload, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(
        cls,
        msg: Dict[str, any],
        timestamp: Optional[float] = None,
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        payload = {
            "trading_pair": cls._tp(msg, metadata),
            "update_id": msg.get("u"),
            "bids": msg.get("b") or [],
            "asks": msg.get("a") or [],
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, payload, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        if metadata:
            msg = dict(msg, **metadata)
        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": msg["trading_pair"],
                "trade_type": float(TradeType.BUY.value) if msg["S"] == "BUY" else float(TradeType.SELL.value),
                "trade_id": msg["i"],
                "update_id": msg["T"],
                "price": msg["p"],
                "amount": msg["v"],
            },
            timestamp=msg["T"],
        )
