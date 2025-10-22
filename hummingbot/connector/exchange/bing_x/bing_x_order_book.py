from typing import Dict, Optional, List, Any

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BingXOrderBook(OrderBook):
    @staticmethod
    def _normalize_levels(levels: Any) -> List[List[Any]]:
        """
        Optimized normalization for BingX levels.
        Handles both list and dict formats with fast paths.
        Returns list of [price, qty], Hummingbot converts to floats later.
        """
        # Fast path: list format (most common for WS updates)
        if isinstance(levels, list):
            if not levels:
                return []
            
            # Check first element to determine format
            first = levels[0]
            
            # Most common: [["price", "qty"], ...]
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                # Fast list comprehension with None filtering for safety
                return [[entry[0], entry[1]] for entry in levels 
                        if len(entry) >= 2 and entry[0] is not None and entry[1] is not None]
            
            # Alternative format: ["price:qty", ...]
            if isinstance(first, str) and ':' in first:
                result = []
                for entry in levels:
                    parts = entry.split(':', 1)
                    if len(parts) == 2:
                        result.append([parts[0], parts[1]])
                return result
            
            # Dict format in list: [{"price": "x", "quantity": "y"}, ...]
            if isinstance(first, dict):
                result = []
                for entry in levels:
                    price = entry.get("price") or entry.get("p")
                    qty = entry.get("quantity") or entry.get("qty") or entry.get("q")
                    if price is not None and qty is not None:
                        result.append([price, qty])
                return result
            
            return []
        
        # Medium path: dict format (price-keyed, can happen in WS snapshots)
        if isinstance(levels, dict):
            # Fast comprehension without None checks (BingX shouldn't send None values)
            return [[price, qty] for price, qty in levels.items()]
        
        # Fallback: unknown format
        return []

    @classmethod
    def snapshot_message_from_exchange_websocket(cls,
                                                 msg: Dict[str, any],
                                                 timestamp: float,
                                                 metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message from websocket full depth data.
        """
        if metadata:
            msg.update(metadata)
        
        data_node = msg.get("data", {})
        
        # Extract update ID
        update_id = (
            data_node.get("lastUpdateId") or
            data_node.get("version") or
            data_node.get("sequence")
        )
        
        try:
            update_id = int(update_id) if update_id else int(timestamp * 1e3)
        except (ValueError, TypeError):
            update_id = int(timestamp * 1e3)
        
        # Extract and normalize bids/asks
        bids_raw = data_node.get("bids") or data_node.get("b") or []
        asks_raw = data_node.get("asks") or data_node.get("a") or []
        
        bids = cls._normalize_levels(bids_raw)
        asks = cls._normalize_levels(asks_raw)
        
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp)

    @classmethod
    def snapshot_message_from_exchange_rest(cls,
                                            msg: Dict[str, any],
                                            timestamp: float,
                                            metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message from REST API response.
        """
        if metadata:
            msg.update(metadata)
        
        # REST snapshot sequence is unreliable - use 0
        update_id = 0
        if "lastUpdateId" in msg:
            try:
                update_id = int(msg["lastUpdateId"])
            except (ValueError, TypeError):
                pass
        
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
        Creates a diff message from incremental depth update.
        """
        if metadata:
            msg.update(metadata)
        
        data_node = msg.get("data", {})
        
        # Extract update ID
        update_id = (
            data_node.get("lastUpdateId") or
            data_node.get("version") or
            data_node.get("sequence")
        )
        
        try:
            update_id = int(update_id) if update_id else None
        except (ValueError, TypeError):
            update_id = int(timestamp * 1e3) if timestamp else None
        
        bids_raw = data_node.get("bids") or data_node.get("b") or []
        asks_raw = data_node.get("asks") or data_node.get("a") or []
        
        bids = cls._normalize_levels(bids_raw)
        asks = cls._normalize_levels(asks_raw)
        
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "first_update_id": (update_id - 1) if update_id else None,
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        """
        Creates a trade message from websocket trade event.
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
        }, timestamp=ts * 1e-3)
