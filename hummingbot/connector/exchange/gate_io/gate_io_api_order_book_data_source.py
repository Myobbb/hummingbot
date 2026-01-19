import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.gate_io import gate_io_constants as CONSTANTS, gate_io_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange


class GateIoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Gate.io Order Book V2 data source with optimizations for maximum freshness.
    
    Key optimizations:
    1. Proper full=true snapshot handling (replaces entire orderbook)
    2. Symbol caching to avoid async lookups in hot path
    3. Bounded queues to prevent memory pressure
    4. Optimistic diff application (no sequence-based blocking)
    5. Per-symbol staleness tracking for recovery
    """

    HEARTBEAT_TIME_INTERVAL = 30.0  # Gate.io recommends sending ping every 30s
    MAX_QUEUE_SIZE = 10000  # Bounded queue size
    
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'GateIoExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs: List[str] = trading_pairs
        self._domain = domain

        # Bounded queues to prevent memory pressure under high load
        self._message_queue: Dict[str, asyncio.Queue] = {
            self._trade_messages_queue_key: asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE),
            self._diff_messages_queue_key: asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE),
            self._snapshot_messages_queue_key: asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE),
        }
        
        # Symbol caching to avoid async lookups in hot path
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._pair_to_symbol_cache: Dict[str, str] = {}
        
        # Per-symbol staleness tracking
        self._symbol_last_update_time: Dict[str, float] = {}
        
        # WS timestamps for liveness monitoring
        self._last_ws_message_sent_timestamp: float = 0.0
        self._last_data_recv_ts: float = 0.0

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = self._time()
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": snapshot_response["id"],
                "bids": snapshot_response["bids"],
                "asks": snapshot_response["asks"],
            },
            timestamp=snapshot_timestamp)
        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        import json
        params = {
            "currency_pair": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "with_id": json.dumps(True)
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(endpoint=CONSTANTS.ORDER_BOOK_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_data: Dict[str, Any] = raw_message["result"]
        trade_timestamp: float = float(trade_data["create_time_ms"]) * 1e-3
        
        # Use cache for symbol lookup in hot path
        currency_pair = trade_data["currency_pair"]
        trading_pair = self._symbol_to_pair_cache.get(currency_pair)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                symbol=currency_pair)
            self._symbol_to_pair_cache[currency_pair] = trading_pair
            
        message_content = {
            "trading_pair": trading_pair,
            "trade_type": (float(TradeType.SELL.value)
                           if trade_data["side"] == "sell"
                           else float(TradeType.BUY.value)),
            "trade_id": trade_data["id"],
            "update_id": trade_timestamp,
            "price": trade_data["price"],
            "amount": trade_data["amount"],
        }
        trade_message: Optional[OrderBookMessage] = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=message_content,
            timestamp=trade_timestamp)

        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book diffs - routes to appropriate queue based on full flag."""
        result_payload = raw_message.get("result")
        if isinstance(result_payload, list):
            for item in result_payload:
                await self._emit_single_obu_update(item, message_queue)
        else:
            await self._emit_single_obu_update(result_payload, message_queue)

    async def _emit_single_obu_update(self, diff_data: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Process a single orderbook update message.
        
        CRITICAL: Gate.io sends full=true for snapshots. These must be routed to the
        snapshot queue and processed as SNAPSHOT messages, not DIFFs.
        """
        if not isinstance(diff_data, dict):
            return
            
        # Check if this is a full snapshot (full=true)
        is_full_snapshot = diff_data.get("full", False) is True
        
        # timestamp in ms per Gate, default to now if absent
        try:
            t_val = diff_data.get("t")
            t_ms = int(t_val) if t_val is not None else int(self._time() * 1e3)
        except Exception:
            t_ms = int(self._time() * 1e3)
        timestamp: float = t_ms * 1e-3
        
        try:
            update_id: int = int(diff_data.get("u", 0))
        except Exception:
            update_id = 0

        # For Order Book V2, "s" can be a stream identifier like "ob.BTC_USDT.50".
        stream_name = str(diff_data.get("s", ""))
        ex_symbol = stream_name.split(".")[1] if "." in stream_name else stream_name
        if not ex_symbol:
            return
        
        # Use cache for symbol lookup in hot path
        trading_pair = self._symbol_to_pair_cache.get(ex_symbol)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
            self._symbol_to_pair_cache[ex_symbol] = trading_pair

        # Accept both v2 incremental and potential full snapshot variations
        bids = diff_data.get("b") or diff_data.get("bids") or []
        asks = diff_data.get("a") or diff_data.get("asks") or []

        # Some messages might be heartbeat/keepalive without book deltas
        if not bids and not asks and update_id == 0:
            return

        # Update per-symbol staleness tracking
        self._symbol_last_update_time[ex_symbol] = self._time()
        self._last_data_recv_ts = self._time()

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks,
        }

        if is_full_snapshot:
            # CRITICAL: Full snapshots must be processed as SNAPSHOT type
            # This replaces the entire orderbook rather than applying incremental changes
            snapshot_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                order_book_message_content,
                timestamp)
            try:
                message_queue.put_nowait(snapshot_message)
            except asyncio.QueueFull:
                self.logger().warning("Gate.io snapshot queue full; dropping snapshot")
        else:
            # Incremental update - include first_update_id for sequence tracking
            try:
                first_update_id = int(diff_data.get("U", update_id))
            except Exception:
                first_update_id = update_id
            order_book_message_content["first_update_id"] = first_update_id
            
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.DIFF,
                order_book_message_content,
                timestamp)
            try:
                message_queue.put_nowait(diff_message)
            except asyncio.QueueFull:
                self.logger().warning("Gate.io diff queue full; dropping diff")

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                
                # Pre-seed symbol caches
                self._symbol_to_pair_cache[symbol] = trading_pair
                self._pair_to_symbol_cache[trading_pair] = symbol

                trades_payload = {
                    "time": int(self._time()),
                    "channel": CONSTANTS.TRADES_ENDPOINT_NAME,
                    "event": "subscribe",
                    "payload": [symbol]
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

                order_book_payload = {
                    "time": int(self._time()),
                    "channel": CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME,
                    "event": "subscribe",
                    "payload": [f"ob.{symbol}.{CONSTANTS.ORDER_BOOK_V2_LEVEL}"]
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)
                
                # Initialize staleness tracking
                self._symbol_last_update_time[symbol] = self._time()

            self.logger().info(f"Gate.io: Subscribed to public order book (L{CONSTANTS.ORDER_BOOK_V2_LEVEL}) and trade channels for {len(self._trading_pairs)} pairs")
            
            # Kick off periodic application ping to keep the connection active
            self._last_ws_message_sent_timestamp = self._time()
            try:
                await ws.send(WSJSONRequest(payload={
                    "time": int(self._time()),
                    "channel": "spot.ping",
                    "event": "",
                    "payload": [],
                }))
            except Exception:
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determine which queue a message should be routed to.
        
        CRITICAL: Gate.io Order Book V2 sends full=true for snapshots within the same
        spot.obu channel. We need to detect these and route them appropriately.
        """
        # Ignore application-level pong to avoid misrouting
        if event_message.get("channel") == CONSTANTS.PONG_CHANNEL_NAME:
            return ""
        channel = ""
        if event_message.get("error") is not None:
            err_msg = event_message.get("error", {}).get("message", event_message.get("error"))
            raise IOError(f"Error event received from the server ({err_msg})")
        elif event_message.get("event") == "update":
            if event_message.get("channel") == CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME:
                # Check if this is a full snapshot (full=true in result)
                result = event_message.get("result", {})
                if isinstance(result, dict) and result.get("full", False) is True:
                    # Route full snapshots to snapshot queue
                    channel = self._snapshot_messages_queue_key
                else:
                    channel = self._diff_messages_queue_key
            elif event_message.get("channel") == CONSTANTS.TRADES_ENDPOINT_NAME:
                channel = self._trade_messages_queue_key

        return channel

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse WebSocket snapshot messages (full=true).
        These come from the same channel as diffs but are routed here.
        """
        result_payload = raw_message.get("result")
        if isinstance(result_payload, dict):
            await self._emit_single_obu_update(result_payload, message_queue)
        elif isinstance(result_payload, list):
            for item in result_payload:
                await self._emit_single_obu_update(item, message_queue)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws

    def _time(self) -> float:
        return time.time()

    async def _subscribe_single_trading_pair(self, ws: WSAssistant, trading_pair: str):
        """
        Subscribe to a single trading pair on an active websocket.
        
        Gate.io specific implementation that sends individual subscription messages
        for trades and order book updates.
        
        :param ws: The active websocket assistant
        :param trading_pair: The trading pair to subscribe to
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        
        # Pre-seed symbol caches
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._pair_to_symbol_cache[trading_pair] = symbol
        
        # Subscribe to trades
        trades_payload = {
            "time": int(self._time()),
            "channel": CONSTANTS.TRADES_ENDPOINT_NAME,
            "event": "subscribe",
            "payload": [symbol]
        }
        subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)
        
        # Subscribe to order book updates
        order_book_payload = {
            "time": int(self._time()),
            "channel": CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME,
            "event": "subscribe",
            "payload": [f"ob.{symbol}.{CONSTANTS.ORDER_BOOK_V2_LEVEL}"]
        }
        subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)
        
        await ws.send(subscribe_trade_request)
        await ws.send(subscribe_orderbook_request)
        
        # Initialize staleness tracking
        self._symbol_last_update_time[symbol] = self._time()
        
        self.logger().info(
            f"Gate.io: Dynamically subscribed to {trading_pair} "
            f"(order book L{CONSTANTS.ORDER_BOOK_V2_LEVEL} + trades)"
        )
