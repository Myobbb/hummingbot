import asyncio
import datetime as dt
import time
from bisect import bisect_left, insort
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS, nonkyc_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange


def _iso_to_epoch(value: Any) -> Optional[float]:
    """'2026-11-17T02:59:57.123Z' -> epoch seconds; None when unparsable (an int is taken as milliseconds)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) * 1e-3 if value > 1e11 else float(value)
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _update_side(levels: Dict[float, float], prices: List[float], updates: Iterable) -> None:
    # Absolute quantity per price; quantity "0" deletes the level, and deleting an unknown level is normal.
    for level in updates:
        if isinstance(level, dict):
            price, qty = level.get("price"), level.get("quantity")
        else:
            price, qty = level
        try:
            p, q = float(price), float(qty)
        except (TypeError, ValueError):
            continue
        if q > 0:
            if p not in levels:
                insort(prices, p)
            levels[p] = q
        elif p in levels:
            del levels[p]
            del prices[bisect_left(prices, p)]


class _LocalBook:
    """
    One market's book: absolute quantity per level (NonKYC's `updateOrderbook` semantics).

    Each side keeps a dict (price -> quantity) plus its prices in ascending order, so the top N is a slice and a push
    costs a handful of inserts (XT's lesson: rebuilding the top N over the whole book on every push was the largest
    cost per push).
    """

    __slots__ = ("bids", "asks", "bid_prices", "ask_prices", "last_seq", "synced", "pending", "pending_since")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_prices: List[float] = []
        self.ask_prices: List[float] = []
        self.last_seq: Optional[int] = None
        self.synced: bool = False
        # updates that arrived ahead of their predecessor: sequence -> (frame, monotonic arrival time)
        self.pending: Dict[int, Tuple[Dict[str, Any], float]] = {}
        self.pending_since: float = 0.0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.last_seq = None
        self.synced = False
        self.pending.clear()
        self.pending_since = 0.0

    def load(self, bids: Iterable, asks: Iterable) -> None:
        self.bids = {float(level["price"]): float(level["quantity"]) for level in bids or ()
                     if float(level.get("quantity") or 0) > 0}
        self.asks = {float(level["price"]): float(level["quantity"]) for level in asks or ()
                     if float(level.get("quantity") or 0) > 0}
        self.bid_prices = sorted(self.bids)
        self.ask_prices = sorted(self.asks)

    def apply(self, params: Dict[str, Any]) -> None:
        _update_side(self.bids, self.bid_prices, params.get("bids") or ())
        _update_side(self.asks, self.ask_prices, params.get("asks") or ())
        if len(self.bid_prices) > CONSTANTS.MAX_LEVELS_PER_SIDE:
            cut = len(self.bid_prices) - CONSTANTS.MAX_LEVELS_PER_SIDE // 2
            for p in self.bid_prices[:cut]:
                del self.bids[p]
            del self.bid_prices[:cut]
        if len(self.ask_prices) > CONSTANTS.MAX_LEVELS_PER_SIDE:
            keep = CONSTANTS.MAX_LEVELS_PER_SIDE // 2
            for p in self.ask_prices[keep:]:
                del self.asks[p]
            del self.ask_prices[keep:]

    def top(self, depth: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Best `depth` levels per side, best first."""
        bids, asks = self.bids, self.asks
        return ([(p, bids[p]) for p in reversed(self.bid_prices[-depth:])],
                [(p, asks[p]) for p in self.ask_prices[:depth]])


class NonkycAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    NonKYC public market data: `subscribeOrderbook` (snapshot + full-depth deltas, one side per frame) and
    `subscribeTrades`, over one JSON-RPC socket (wss://ws.nonkyc.io).

    The book is kept HERE, not in Hummingbot's tracker, because the venue sends DELTAS:
      - the reply to a subscribe IS the market's `snapshotOrderbook` frame (top `limit` levels, `sequence` as a
        STRING); every later `updateOrderbook` carries one side and an int `sequence` that increments by one;
      - REST /market/orderbook's `sequence` is a DIFFERENT chain (P1, 2026-11-14), so a REST book can never seed the
        WS book -- the local book is seeded by the venue's own snapshot only;
      - adjacent frames with the same server timestamp were seen swapped (P1: +2 then -1; the same on KAS at
        myserver 2026-11-17), so an update that arrives ahead of its predecessor waits REORDER_HOLD_SEC for it. A step
        that stays unfilled is a lost frame: the book is dropped and a fresh subscribe brings a new snapshot.

    Every applied push emits a top-EMIT_DEPTH SNAPSHOT message to the tracker, never a DIFF (the tracker's snapshot
    path replays diffs without checking ids, and c_apply_snapshot stamps last_applied_diff, which the orchestrator's
    stale-book check reads).

    Speed: a push is applied and handed to the tracker inside the WebSocket reader, as soon as it is read.

    A runtime add subscribes the new market on the live socket instead of disconnecting it, and
    `_refresh_snapshot_for_pair` gives the orchestrator a per-market repair that leaves every other book alone.

    The venue's trading switch: a market NonKYC has paused (`isPaused`, or `isActive` false) is shown as an EMPTY book
    -- the connector's cached market list, refreshed with the trading rules every TRADING_RULES_INTERVAL, is the
    source. The symbol and the trading rule stay, so the venue's own answer to an order decides (non-negotiable 3).
    While no connection is live every tracked book is shown empty, like a halt: the stream is the book.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "NonkycExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "NonkycExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        # Read by the orchestrator's per-market stale-book recovery (method 1), with _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._ping_task: Optional[asyncio.Task] = None
        self._hold_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0
        self._warned: set = set()
        self._diff_output: Optional[asyncio.Queue] = None
        # True from a connection's subscribe until its interruption: only then is a book kept current.
        self._stream_live: bool = False
        self._backoff = web_utils.NonkycReconnectBackoff()
        # Counts, not narration: gaps/rebuilds and undecodable frames are logged at most once a minute.
        self._stats = {"pushes": 0, "reorder_held": 0, "rebuilds": 0, "unparsed": 0}
        self._stats_since = time.monotonic()

    # ------------------------------------------------------------------ helpers

    def _request_id(self) -> int:
        self._next_request_id += 1
        return self._next_request_id

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

    # ------------------------------------------------------------------ the tracker's snapshots

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book and its hourly refresh. A synced local book answers it directly: it is newer
        than any REST answer, and REST's sequence chain is unrelated to the WS one, so a REST snapshot can neither
        seed nor correct this book. Before the first WS snapshot the answer is an empty book, and the venue's own
        snapshot fills it in (through the diff queue)."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"NonKYC {trading_pair} has no entry in the connector's symbol map; "
                             f"no depth snapshot was requested") from None
        book = self._book(symbol)
        if book.synced and self._stream_live:
            return self._snapshot_message(trading_pair, symbol, book)
        return self._message(trading_pair, 0, [], [])

    @staticmethod
    def _message(trading_pair: str, update_id: int, bids: list, asks: list) -> OrderBookMessage:
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _LocalBook) -> OrderBookMessage:
        if self._connector.is_market_paused(symbol):
            return self._message(trading_pair, book.last_seq or 0, [], [])
        bids, asks = book.top(CONSTANTS.EMIT_DEPTH)
        return self._message(trading_pair, book.last_seq or 0, bids, asks)

    def _emit(self, symbol: str, book: _LocalBook, output: asyncio.Queue) -> None:
        pair = self._symbol_to_pair_cache.get(symbol)
        if pair is None:
            return
        if self._connector.is_market_paused(symbol):
            # Paused by the venue: the book is kept, the tracker sees it empty, like a halt elsewhere.
            output.put_nowait(self._message(pair, book.last_seq or 0, [], []))
            return
        bids, asks = book.top(CONSTANTS.EMIT_DEPTH)
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen with an unbroken sequence; treat it as corruption and start over.
            self.logger().warning(f"NonKYC {pair}: crossed book (bid {bids[0][0]} >= ask {asks[0][0]}); rebuilding.")
            self._rebuild(symbol, output, "crossed-book")
            return
        output.put_nowait(self._message(pair, book.last_seq or 0, bids, asks))

    def _show_empty(self, symbol: str, output: Optional[asyncio.Queue]) -> None:
        """This market's book is being rebuilt: show it empty until the rebuilt one is emitted. Call before reset()."""
        pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and pair is not None:
            book = self._books.get(symbol)
            output.put_nowait(self._message(pair, (book.last_seq if book else None) or 0, [], []))

    def _rebuild(self, symbol: str, output: Optional[asyncio.Queue], why: str) -> None:
        """Drop this market's book and ask for a new snapshot: the subscribe's answer IS the snapshot."""
        self._stats["rebuilds"] += 1
        self._show_empty(symbol, output)
        book = self._book(symbol)
        book.reset()
        ws = self._active_ws
        if ws is None or not self._stream_live:
            return  # the next connection subscribes every tracked pair and brings every snapshot
        asyncio.ensure_future(self._resubscribe(ws, symbol, why))

    async def _resubscribe(self, ws: WSAssistant, symbol: str, why: str) -> None:
        try:
            await ws.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
                "params": {"symbol": symbol, "limit": CONSTANTS.SUBSCRIBE_LIMIT},
                "id": self._request_id(),
            }))
            self.logger().debug(f"NonKYC {symbol}: re-subscribed for a fresh snapshot ({why}).")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"NonKYC {symbol}: the re-subscribe failed ({e!r}); the next connection "
                                  f"rebuilds every book.")

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            subscribed = 0
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every NonKYC book empty.
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"NonKYC {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
                    continue
                self._book(symbol).reset()
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
                    "params": {"symbol": symbol, "limit": CONSTANTS.SUBSCRIBE_LIMIT},
                    "id": self._request_id(),
                }))
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_METHOD_SUBSCRIBE_TRADES,
                    "params": {"symbol": symbol},
                    "id": self._request_id(),
                }))
                subscribed += 1
            self._stream_live = True
            self._backoff.connected()
            self.logger().log(self._subscribed_log_level(),
                              "Subscribed to NonKYC orderbook and trades for %d markets.", subscribed)
            if self._hold_task is not None:
                self._hold_task.cancel()
            self._hold_task = asyncio.ensure_future(self._reorder_watchdog())
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to NonKYC public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which blanks every NonKYC book for a reconnect."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).reset()
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
            "params": {"symbol": symbol, "limit": CONSTANTS.SUBSCRIBE_LIMIT},
            "id": self._request_id(),
        }))
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_SUBSCRIBE_TRADES,
            "params": {"symbol": symbol},
            "id": self._request_id(),
        }))
        self.logger().debug("Subscribed NonKYC %s on the live connection (no reconnect).", trading_pair)

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: rebuild this one book from a fresh snapshot.
        The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._rebuild(symbol, self._diff_output, "orchestrator-repair")

    async def _reorder_watchdog(self) -> None:
        """An update held for its predecessor past REORDER_HOLD_SEC means the predecessor is lost (or the book
        stalled): rebuild that market. Runs once a second; quiet markets have nothing held."""
        try:
            while True:
                await asyncio.sleep(1.0)
                self._reorder_watchdog_once()
                self._log_stats(time.monotonic())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"NonKYC reorder watchdog stopped: {e!r}")

    def _reorder_watchdog_once(self) -> None:
        """One watchdog pass (separated so the offline suite drives exactly the shipped code)."""
        now = time.monotonic()
        output = self._diff_output
        for symbol, book in list(self._books.items()):
            if book.pending and now - book.pending_since > CONSTANTS.REORDER_HOLD_SEC:
                self._rebuild(symbol, output, "sequence-gap")

    def _log_stats(self, now: float) -> None:
        if now - self._stats_since < 60.0:
            return
        counters = dict(self._stats)
        self._stats = {key: 0 for key in self._stats}
        self._stats_since = now
        level = self.logger().debug if not (counters["rebuilds"] or counters["unparsed"]) else self.logger().info
        level(f"NonKYC public stream: {counters['pushes']} pushes applied, {counters['reorder_held']} held for "
              f"reordering, {counters['rebuilds']} rebuilds, {counters['unparsed']} undecodable frames "
              f"({len(self._books)} books).")

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            if self._hold_task is not None:
                self._hold_task.cancel()
                self._hold_task = None

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        diff_key = self._diff_messages_queue_key
        diff_queue = self._message_queue[diff_key]
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if not isinstance(data, dict):
                continue  # the text "pong" reply
            channel = self._channel_originating_message(event_message=data)
            if channel == diff_key:
                # Applied here, now. The queue is used only while it holds something (a push that came before the
                # listener started), so the arrival order is kept.
                output = self._diff_output
                symbol = (data.get("params") or {}).get("symbol")
                if output is not None and diff_queue.empty() and symbol in self._symbol_to_pair_cache:
                    self._on_book_frame(data, output)
                else:
                    diff_queue.put_nowait(data)
            elif channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(data)
            else:
                await self._process_message_for_unknown_channel(event_message=data,
                                                               websocket_assistant=websocket_assistant)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        method = event_message.get("method")
        if method in (CONSTANTS.WS_FRAME_SNAPSHOT_BOOK, CONSTANTS.WS_FRAME_UPDATE_BOOK):
            return self._diff_messages_queue_key
        if method in (CONSTANTS.WS_FRAME_SNAPSHOT_TRADES, CONSTANTS.WS_FRAME_UPDATE_TRADES):
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any],
                                                   websocket_assistant: WSAssistant) -> None:
        error = event_message.get("error")
        if isinstance(error, dict):
            # A refused subscribe (2001 "Symbol Not Found") keeps the socket open. Logged, not raised: re-raising
            # would reconnect and re-send the same subscribes forever.
            self.logger().warning(f"NonKYC public WS request {event_message.get('id')} failed: {error}")
            return
        if "result" in event_message or "id" in event_message:
            return  # a subscribe's {"result": true} ack
        self.logger().debug(f"Unrecognised NonKYC public WS message: {event_message}")

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
        if self._hold_task is not None:
            self._hold_task.cancel()
            self._hold_task = None
        output = self._diff_output
        if was_live and output is not None:
            # Every book the tracker holds would stay frozen from here on: show them empty until a connection
            # rebuilds them.
            for trading_pair in list(self._trading_pairs):
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                book = self._books.get(symbol) if symbol is not None else None
                output.put_nowait(self._message(trading_pair, (book.last_seq if book else None) or 0, [], []))
        for book in self._books.values():
            book.reset()  # the next connection re-subscribes every pair and gets a fresh snapshot
        await self._backoff.wait()  # the listen loop reconnects right after this

    # ------------------------------------------------------------------ the local book

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """The queued path: a push that could not be applied on arrival."""
        symbol = (raw_message.get("params") or {}).get("symbol")
        if not symbol:
            return
        if symbol not in self._symbol_to_pair_cache:
            await self._pair_for_symbol(symbol)
        self._on_book_frame(raw_message, message_queue)

    def _on_book_frame(self, raw_message: Dict[str, Any], output: asyncio.Queue) -> None:
        """One book frame (snapshot or update). Synchronous: it runs inside the WebSocket reader. The symbol is
        already in _symbol_to_pair_cache."""
        params = raw_message.get("params") or {}
        symbol = params.get("symbol")
        if not symbol:
            return
        book = self._book(symbol)
        try:
            sequence = int(params.get("sequence"))
        except (TypeError, ValueError):
            self._stats["unparsed"] += 1
            return
        if raw_message.get("method") == CONSTANTS.WS_FRAME_SNAPSHOT_BOOK:
            # The reply to a subscribe IS the snapshot. A late snapshot for a book that is already live (a duplicate
            # subscribe) would roll it back: only an unsynced book takes it.
            if book.synced:
                return
            book.load(params.get("bids"), params.get("asks"))
            book.last_seq = sequence
            book.synced = True
            book.pending.clear()
            self._drain_pending(symbol, book, output)
            self._emit(symbol, book, output)
            return
        if not book.synced:
            # An update before its snapshot: buffered by sequence until the snapshot lands, then drained.
            book.pending[sequence] = (params, time.monotonic())
            book.pending_since = book.pending_since or time.monotonic()
            return
        if sequence <= book.last_seq:
            return  # already contained (a duplicate, or a frame the buffer already applied)
        if sequence == book.last_seq + 1:
            book.apply(params)
            book.last_seq = sequence
            self._stats["pushes"] += 1
            self._drain_pending(symbol, book, output)
            self._emit(symbol, book, output)
            return
        # Ahead of its predecessor: held for it (adjacent frames are seen swapped).
        book.pending[sequence] = (params, time.monotonic())
        if not book.pending_since:
            book.pending_since = time.monotonic()
        self._stats["reorder_held"] += 1

    def _drain_pending(self, symbol: str, book: _LocalBook, output: asyncio.Queue) -> None:
        while book.pending:
            nxt = book.last_seq + 1
            entry = book.pending.pop(nxt, None)
            if entry is None:
                return
            book.apply(entry[0])
            book.last_seq = nxt
            self._stats["pushes"] += 1
        book.pending_since = 0.0

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """`snapshotTrades` / `updateTrades`: params.data[] = {id, price, quantity, side, timestamp, timestampms}."""
        params = raw_message.get("params") or {}
        symbol = params.get("symbol")
        if not symbol:
            return
        trading_pair = await self._pair_for_symbol(symbol)
        for row in params.get("data") or ():
            if not isinstance(row, dict):
                continue
            timestamp_ms = row.get("timestampms")
            if timestamp_ms in (None, ""):
                epoch = _iso_to_epoch(row.get("timestamp"))
                timestamp_ms = int(epoch * 1e3) if epoch is not None else int(time.time() * 1e3)
            # `side` is the taker's side as the venue reports it.
            trade_type = TradeType.BUY if str(row.get("side") or "").lower() == "buy" else TradeType.SELL
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": str(row.get("id")),
                    "update_id": int(timestamp_ms),
                    "price": row.get("price"),
                    "amount": row.get("quantity"),
                },
                timestamp=int(timestamp_ms) * 1e-3,
            ))
