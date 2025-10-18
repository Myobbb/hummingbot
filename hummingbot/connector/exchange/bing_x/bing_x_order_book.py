from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BingXOrderBook(OrderBook):
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
        # Use lastUpdateId instead of timestamp for better ordering
        update_id = msg.get("lastUpdateId", int(timestamp * 1e3))
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,  # Sequential ID from BingX
            "bids": msg["bids"],
            "asks": msg["asks"]
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
        # Use version field from BingX REST API for proper sequence validation
        update_id = msg.get("lastUpdateId")  # This should be set from version field in data source
        if update_id is None:
            # Fallback to timestamp if version not available
            update_id = int(msg["timestamp"])
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": msg["bids"],
            "asks": msg["asks"]
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
        # Extract from nested structure
        data_node = msg.get("data", {})
        update_id = data_node.get("lastUpdateId", int(timestamp * 1e3) if timestamp else None)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": data_node.get("bids", []),
            "asks": data_node.get("asks", [])
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
        ts = msg["T"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg["m"] else float(TradeType.SELL.value),
            "trade_id": ts,
            "update_id": ts,
            "price": msg["p"],
            "amount": msg["q"]
        }, timestamp= ts * 1e-3)