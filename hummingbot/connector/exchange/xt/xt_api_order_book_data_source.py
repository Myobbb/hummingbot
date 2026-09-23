import asyncio
import heapq
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.xt.xt_exchange import XtExchange

# Internal queue item: a REST depth snapshot, routed through the diff queue so the one consumer of
# that queue applies snapshots and diffs strictly in arrival order.
_REST_SNAPSHOT = "__xt_rest_snapshot__"
# Diffs buffered while a book waits for its snapshot. At ~10 pushes/s that is ~2 minutes.
_MAX_BUFFERED_DIFFS = 1200
# Retries when a snapshot is older than the stream (its lastUpdateId + 1 < the first buffered fi).
_SNAPSHOT_RETRY_DELAY = 0.5
_SNAPSHOT_MAX_ATTEMPTS = 8
# A book that grew past this many levels on a side is trimmed back to the best half.
_MAX_LEVELS_PER_SIDE = 2000


class _LocalBook:
    """One market's book as XT's 'Orderbook manage' page defines it: absolute quantity per level."""

    __slots__ = ("bids", "asks", "last_id", "synced", "buffer", "bootstrapping", "attempts")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_id: Optional[int] = None
        self.synced: bool = False
        self.buffer: List[Dict[str, Any]] = []
        self.bootstrapping: bool = False
        self.attempts: int = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_id = None
        self.synced = False
        self.buffer.clear()
        self.bootstrapping = False
        self.attempts = 0


class XtAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    XT public market data: `depth_update@{symbol}` (100 ms, sequenced diffs) + `trade@{symbol}`.

    The book is kept HERE, not in Hummingbot's tracker, following XT's documented procedure
    (WebSocket Public/OrderbookManage): buffer the diffs, fetch a REST snapshot, drop every event
    with `i <= lastUpdateId`, start from the event with `fi <= lastUpdateId + 1 <= i`, then require
    each event's `fi` to be the previous `i + 1`. Any break (a gap, or a crossed book) throws the
    book away and bootstraps it again. Verified live 2026-09-23: 0 gaps in ~430 events.

    Every applied update emits a top-EMIT_DEPTH SNAPSHOT message to the tracker, never a DIFF:
      - the tracker's snapshot path replays every diff in its window without checking update ids
        (restore_from_snapshot_and_diffs + OrderBookMessage.__lt__), and c_apply_diffs applies a diff
        unconditionally, so mixing XT snapshots with diffs there would re-apply stale levels;
      - c_apply_snapshot also stamps last_applied_diff, which the orchestrator's stale-book check
        reads, so a snapshot-only feed keeps that check honest;
      - HTX's mbp.refresh.20 already feeds a 100 ms snapshot stream through the same path.

    A runtime add subscribes the new market on the live socket instead of disconnecting it, and
    `_refresh_snapshot_for_pair` gives the orchestrator a per-market repair that leaves every other
    book alone.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "XtExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "XtExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        # Read by the orchestrator's per-market stale-book recovery (method 1), with
        # _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._snapshot_tasks: Dict[str, asyncio.Task] = {}
        self._ping_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0
        self._warned: set = set()

    # ------------------------------------------------------------------ helpers

    def _request_id(self) -> str:
        self._next_request_id += 1
        return str(self._next_request_id)

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.logger().warning(message)

    async def _symbol_for_pair(self, trading_pair: str) -> str:
        symbol = self._pair_to_symbol_cache.get(trading_pair)
        if symbol is None:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            self._pair_to_symbol_cache[trading_pair] = symbol
            self._symbol_to_pair_cache[symbol] = trading_pair
        return symbol

    async def _pair_for_symbol(self, symbol: str) -> str:
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            self._symbol_to_pair_cache[symbol] = trading_pair
            self._pair_to_symbol_cache[trading_pair] = symbol
        return trading_pair

    def _book(self, symbol: str) -> _LocalBook:
        book = self._books.get(symbol)
        if book is None:
            book = self._books[symbol] = _LocalBook()
        return book

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ------------------------------------------------------------------ REST snapshot

    async def _request_depth(self, symbol: str) -> Dict[str, Any]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH, domain=self._domain),
            params={"symbol": symbol, "limit": CONSTANTS.SNAPSHOT_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if web_utils.is_error_response(response):
            raise IOError(f"Error fetching XT depth for {symbol}: {response}")
        return response["result"]

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """REST snapshot for the tracker's initial book (and its hourly fallback). The same result
        also seeds this data source's local book when that book is still waiting for one, so the
        startup does not fetch every market twice."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"XT {trading_pair} has no entry in the connector's symbol map; "
                             f"no depth snapshot was requested") from None
        result = await self._request_depth(symbol)
        book = self._book(symbol)
        if not book.synced:
            self._message_queue[self._diff_messages_queue_key].put_nowait(
                {"topic": _REST_SNAPSHOT, "s": symbol, "result": result})
        update_id = int(result["lastUpdateId"])
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": result.get("bids") or [],
                "asks": result.get("asks") or [],
            },
            timestamp=float(result.get("timestamp") or time.time() * 1e3) * 1e-3,
        )

    def _ensure_bootstrap(self, symbol: str, delay: float = 0.0) -> None:
        book = self._book(symbol)
        if book.bootstrapping or book.synced:
            return
        book.bootstrapping = True
        self._snapshot_tasks[symbol] = asyncio.ensure_future(self._fetch_snapshot(symbol, delay))

    async def _fetch_snapshot(self, symbol: str, delay: float) -> None:
        try:
            if delay:
                await asyncio.sleep(delay)
            result = await self._request_depth(symbol)
            self._message_queue[self._diff_messages_queue_key].put_nowait(
                {"topic": _REST_SNAPSHOT, "s": symbol, "result": result})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            book = self._book(symbol)
            book.bootstrapping = False
            self.logger().warning(f"XT depth snapshot for {symbol} failed ({e}); retrying in 2 s.")
            self._ensure_bootstrap(symbol, delay=2.0)

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_PUBLIC_URL)

    @staticmethod
    def _topics(symbol: str) -> List[str]:
        return [f"{CONSTANTS.WS_DEPTH_UPDATE_TOPIC}@{symbol}", f"{CONSTANTS.WS_TRADE_TOPIC}@{symbol}"]

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            topics: List[str] = []
            subscribed = 0
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would
                    # hit it again every time and leave every XT book empty.
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"XT {trading_pair} has no entry in the connector's symbol map; its market data "
                                    f"is not subscribed. The other markets are.")
                    continue
                self._book(symbol).reset()
                topics.extend(self._topics(symbol))
                subscribed += 1
            for i in range(0, len(topics), CONSTANTS.WS_TOPICS_PER_REQUEST):
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_METHOD_SUBSCRIBE,
                    "params": topics[i:i + CONSTANTS.WS_TOPICS_PER_REQUEST],
                    "id": self._request_id(),
                }))
            self.logger().info(f"Subscribed to XT depth_update and trade channels for {subscribed} markets.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to XT public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket.
        The base default disconnects the shared socket, which blanks every XT book for a reconnect."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).reset()
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_SUBSCRIBE,
            "params": self._topics(symbol),
            "id": self._request_id(),
        }))
        self.logger().info(f"Subscribed XT {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: rebuild this one book from
        a fresh snapshot. The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        book = self._book(symbol)
        task = self._snapshot_tasks.pop(symbol, None)
        if task is not None and not task.done():
            task.cancel()
        book.reset()
        self._ensure_bootstrap(symbol)

    async def _ping_loop(self, ws: WSAssistant) -> None:
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                await ws.send(WSPlainTextRequest(payload="ping"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"XT public WS keepalive stopped: {e}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if not isinstance(data, dict):
                continue  # the text "pong" reply
            channel = self._channel_originating_message(event_message=data)
            if channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(data)
            else:
                await self._process_message_for_unknown_channel(event_message=data, websocket_assistant=websocket_assistant)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        topic = event_message.get("topic")
        if topic == CONSTANTS.WS_DEPTH_UPDATE_TOPIC:
            return self._diff_messages_queue_key
        if topic == CONSTANTS.WS_TRADE_TOPIC:
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant) -> None:
        # Acks look like {"id":"1","code":0,"msg":"SUCCESS","method":"subscribe"} (live 2026-09-23).
        # A failed ack is logged, not raised: re-raising would reconnect and re-send the same topics.
        if "code" in event_message and "topic" not in event_message:
            if event_message.get("code") != 0:
                self.logger().warning(f"XT public WS request {event_message.get('id')} failed: {event_message}")
            return
        self.logger().debug(f"Unrecognised XT public WS message: {event_message}")

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        for task in self._snapshot_tasks.values():
            if not task.done():
                task.cancel()
        self._snapshot_tasks.clear()
        for book in self._books.values():
            book.reset()  # the next connection re-bootstraps every book from scratch

    # ------------------------------------------------------------------ the local book

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        if raw_message.get("topic") == _REST_SNAPSHOT:
            await self._apply_rest_snapshot(raw_message["s"], raw_message["result"], message_queue)
            return

        data = raw_message.get("data") or {}
        symbol = data.get("s")
        if not symbol:
            return
        book = self._book(symbol)
        if not book.synced:
            book.buffer.append(data)
            if len(book.buffer) > _MAX_BUFFERED_DIFFS:
                del book.buffer[: len(book.buffer) - _MAX_BUFFERED_DIFFS]
            self._ensure_bootstrap(symbol)
            return

        fi, i = int(data["fi"]), int(data["i"])
        if i <= book.last_id:
            return  # already contained in the book
        if fi > book.last_id + 1:
            self.logger().warning(
                f"XT {symbol}: depth sequence gap (fi={fi}, expected {book.last_id + 1}); rebuilding the book.")
            book.reset()
            book.buffer.append(data)
            self._ensure_bootstrap(symbol)
            return
        self._apply_levels(book, data)
        book.last_id = i
        await self._emit(symbol, book, message_queue)

    async def _apply_rest_snapshot(self, symbol: str, result: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        book = self._book(symbol)
        if book.synced:
            return  # a late or duplicate snapshot for a book that is already live
        book.bootstrapping = False
        last_update_id = int(result["lastUpdateId"])
        pending = [d for d in book.buffer if int(d["i"]) > last_update_id]
        if pending and int(pending[0]["fi"]) > last_update_id + 1:
            # The snapshot predates the first event we hold; the gap between them is unknowable.
            book.buffer = pending
            book.attempts += 1
            if book.attempts >= _SNAPSHOT_MAX_ATTEMPTS:
                self._warn_once(f"stale-snapshot:{symbol}",
                                f"XT {symbol}: {book.attempts} depth snapshots in a row were older than the stream; still retrying.")
            self._ensure_bootstrap(symbol, delay=_SNAPSHOT_RETRY_DELAY)
            return

        book.bids = {float(p): float(q) for p, q in (result.get("bids") or []) if float(q) > 0}
        book.asks = {float(p): float(q) for p, q in (result.get("asks") or []) if float(q) > 0}
        book.last_id = last_update_id
        for event in pending:
            fi, i = int(event["fi"]), int(event["i"])
            if i <= book.last_id:
                continue
            if fi > book.last_id + 1:
                self.logger().warning(f"XT {symbol}: gap inside the buffered diffs; rebuilding the book.")
                book.reset()
                self._ensure_bootstrap(symbol)
                return
            self._apply_levels(book, event)
            book.last_id = i
        book.buffer.clear()
        book.synced = True
        book.attempts = 0
        await self._emit(symbol, book, message_queue)

    @staticmethod
    def _apply_levels(book: _LocalBook, event: Dict[str, Any]) -> None:
        # Absolute quantity per price; 0 deletes the level, and deleting an unknown level is normal.
        for side, levels in ((book.bids, event.get("b") or ()), (book.asks, event.get("a") or ())):
            for price, qty in levels:
                p, q = float(price), float(qty)
                if q > 0:
                    side[p] = q
                else:
                    side.pop(p, None)
        if len(book.bids) > _MAX_LEVELS_PER_SIDE:
            book.bids = dict(heapq.nlargest(_MAX_LEVELS_PER_SIDE // 2, book.bids.items()))
        if len(book.asks) > _MAX_LEVELS_PER_SIDE:
            book.asks = dict(heapq.nsmallest(_MAX_LEVELS_PER_SIDE // 2, book.asks.items()))

    async def _emit(self, symbol: str, book: _LocalBook, message_queue: asyncio.Queue) -> None:
        bids = heapq.nlargest(CONSTANTS.EMIT_DEPTH, book.bids.items())
        asks = heapq.nsmallest(CONSTANTS.EMIT_DEPTH, book.asks.items())
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen with an unbroken sequence; treat it as corruption and start over.
            self.logger().warning(f"XT {symbol}: crossed book (bid {bids[0][0]} >= ask {asks[0][0]}); rebuilding.")
            book.reset()
            self._ensure_bootstrap(symbol)
            return
        trading_pair = await self._pair_for_symbol(symbol)
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": book.last_id,
                "bids": bids,
                "asks": asks,
            },
            timestamp=time.time(),
        ))

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Live shape (2026-09-23): {"s","i"(string),"t","p","q","b"}; the docs' "oi"/"v" were absent.
        data = raw_message.get("data") or {}
        symbol = data.get("s")
        if not symbol:
            return
        trading_pair = await self._pair_for_symbol(symbol)
        timestamp_ms = int(data.get("t") or time.time() * 1e3)
        # b = buyer is maker, so the taker (the side that traded) sold.
        trade_type = TradeType.SELL if data.get("b") else TradeType.BUY
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": str(data.get("i")),
                "update_id": timestamp_ms,
                "price": data.get("p"),
                "amount": data.get("q"),
            },
            timestamp=timestamp_ms * 1e-3,
        ))
