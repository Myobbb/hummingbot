from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BybitOrderBook(OrderBook):
    @classmethod
    def snapshot_message_from_exchange_websocket(cls,
                                                 msg: Dict[str, any],
                                                 timestamp: float,
                                                 metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        # Normalize possible RPI format
        def _normalize(levels):
            out = []
            for lvl in levels:
                if isinstance(lvl, list) or isinstance(lvl, tuple):
                    if len(lvl) >= 2:
                        try:
                            size = float(lvl[1]) + (float(lvl[2]) if len(lvl) > 2 else 0.0)
                        except Exception:
                            size = float(lvl[1])
                        out.append([lvl[0], str(size)])
            return out
        if isinstance(msg.get("b"), list) and msg.get("b") and len(msg["b"][0]) > 2:
            msg = dict(msg)
            msg["b"] = _normalize(msg["b"]) 
        if isinstance(msg.get("a"), list) and msg.get("a") and len(msg["a"][0]) > 2:
            msg = dict(msg)
            msg["a"] = _normalize(msg["a"]) 
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["u"],
            "bids": msg["b"],
            "asks": msg["a"]
        }, timestamp=timestamp)

    @classmethod
    def snapshot_message_from_exchange_rest(cls,
                                            msg: Dict[str, any],
                                            timestamp: float,
                                            metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["u"],
            "bids": msg["b"],
            "asks": msg["a"]
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: the changes in the order book
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to the difference data
        :return: a diff message with the changes in the order book notified by the exchange
        """
        if metadata:
            msg.update(metadata)
        # Normalize potential RPI frames (where sizes can be [price, non_rpi, rpi]) to 2-tuple [price, size]
        # For non-RPI topics, arrays are already [price, size]
        def _normalize(levels):
            out = []
            for lvl in levels:
                if isinstance(lvl, list) or isinstance(lvl, tuple):
                    if len(lvl) >= 2:
                        # Sum non-RPI and RPI sizes if present (index 1 and 2)
                        try:
                            size = float(lvl[1]) + (float(lvl[2]) if len(lvl) > 2 else 0.0)
                        except Exception:
                            size = float(lvl[1])
                        out.append([lvl[0], str(size)])
                # else skip malformed level
            return out
        if isinstance(msg.get("b"), list) and msg.get("b") and len(msg["b"][0]) > 2:
            msg = dict(msg)
            msg["b"] = _normalize(msg["b"]) 
        if isinstance(msg.get("a"), list) and msg.get("a") and len(msg["a"][0]) > 2:
            if "a" not in msg:
                pass
            msg = dict(msg)
            msg["a"] = _normalize(msg["a"]) 
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["u"],
            "bids": msg["b"],
            "asks": msg["a"]
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)
        trade_msg = OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg["S"] == "BUY" else float(TradeType.SELL.value),
            "trade_id": msg["i"],
            "update_id": msg["T"],
            "price": msg["p"],
            "amount": msg["v"]
        }, timestamp=msg["T"])
        return trade_msg
