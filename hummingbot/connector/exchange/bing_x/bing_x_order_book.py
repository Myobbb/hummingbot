from typing import Dict, Optional, List, Any

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BingXOrderBook(OrderBook):
    @staticmethod
    def _normalize_levels(levels: List[Any]) -> List[List[Any]]:
        """
        Normalize BingX levels which may arrive as:
        - [price, qty]
        - "price:qty"
        - {"price": "..", "quantity"|"qty": ".."} or {"p": "..", "q": ".."}
        Returns list of [price, qty] (strings/numbers), Hummingbot converts to floats later.
        """
        if not isinstance(levels, list):
            return []
        normalized: List[List[Any]] = []
        for entry in levels:
            price = None
            qty = None
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                price, qty = entry[0], entry[1]
            elif isinstance(entry, str):
                parts = entry.split(":")
                if len(parts) >= 2:
                    price, qty = parts[0], parts[1]
            elif isinstance(entry, dict):
                price = entry.get("price") or entry.get("p")
                qty = entry.get("quantity") or entry.get("qty") or entry.get("q")
            if price is not None and qty is not None:
                normalized.append([price, qty])
        return normalized
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
        # Extract orderbook data from nested structure
        data_node = msg.get("data", {})
        # Use BingX-provided sequence fields when available
        update_id = (
            data_node.get("lastUpdateId")
            or data_node.get("version")
            or data_node.get("sequence")
            or int(timestamp * 1e3)
        )
        try:
            update_id = int(update_id)
        except Exception:
            update_id = int(timestamp * 1e3)
        # BingX returns arrays of [price, qty] for asks/bids
        bids_raw = data_node.get("bids") or data_node.get("b") or []
        asks_raw = data_node.get("asks") or data_node.get("a") or []
        bids = cls._normalize_levels(bids_raw)
        asks = cls._normalize_levels(asks_raw)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,  # Sequential ID from BingX
            "bids": bids,
            "asks": asks
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
        update_id = (
            msg.get("lastUpdateId")
            or msg.get("version")
            or msg.get("sequence")
            or msg.get("timestamp")
        )
        try:
            update_id = int(update_id)
        except Exception:
            update_id = int(timestamp * 1e3)
        bids_raw = msg.get("bids") or msg.get("b") or []
        asks_raw = msg.get("asks") or msg.get("a") or []
        bids = cls._normalize_levels(bids_raw)
        asks = cls._normalize_levels(asks_raw)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": bids,
            "asks": asks
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
        update_id = (
            data_node.get("lastUpdateId")
            or data_node.get("version")
            or data_node.get("sequence")
            or (int(timestamp * 1e3) if timestamp else None)
        )
        try:
            update_id = int(update_id) if update_id is not None else None
        except Exception:
            update_id = int(timestamp * 1e3) if timestamp else None
        bids_raw = data_node.get("bids") or data_node.get("b") or []
        asks_raw = data_node.get("asks") or data_node.get("a") or []
        bids = cls._normalize_levels(bids_raw)
        asks = cls._normalize_levels(asks_raw)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            # BingX diffs don't include a separate first id; use update_id for compatibility
            "first_update_id": (update_id - 1) if update_id is not None else None,
            "bids": bids,
            "asks": asks
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