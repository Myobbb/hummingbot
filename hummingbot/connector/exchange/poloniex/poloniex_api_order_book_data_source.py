import asyncio
import time
from bisect import bisect_left, insort
from collections import deque
from typing import TYPE_CHECKING, Any, Deque, Dict, Iterable, List, Optional, Tuple

from hummingbot.connector.exchange.poloniex import poloniex_constants as CONSTANTS, poloniex_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.poloniex.poloniex_exchange import PoloniexExchange


def _levels(raw: Any) -> List[Tuple[float, float]]:
    """[[price, qty], ...] — or Poloniex's flat REST form [price, qty, price, qty, ...] — as floats."""
    if not raw:
        return []
    if not isinstance(raw[0], (list, tuple)):
        raw = [raw[i:i + 2] for i in range(0, len(raw) - 1, 2)]
    return [(float(p), float(q)) for p, q in raw]


class _LocalBook:
    """
    One market's book as book_lv2 defines it: a 20-level snapshot, then absolute quantities per level (0 deletes).
    Each side keeps a dict (price -> quantity) plus its prices in ascending order, so the top N is a slice (XT's
    lesson: rebuilding the top N with heapq per push was the largest cost per push). lv2 maintains the first 20 levels
    and sends the deletion of a level that drops out of them (0 of 164,698 updates ever left more than 20, 2026-10-09);
    the cut to BOOK_DEPTH after every update is a safety net only.
    """

    __slots__ = ("bids", "asks", "bid_prices", "ask_prices", "last_id", "synced", "tops")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_prices: List[float] = []
        self.ask_prices: List[float] = []
        self.last_id: Optional[int] = None
        self.synced: bool = False
        # (update id, top-WS_CHECK_DEPTH) of the last updates applied: the `book` cross-check compares a `book` push
        # with the lv2 top at the same id (the two channels share Poloniex's sequence ids).
        self.tops: Deque[Tuple[int, tuple]] = deque(maxlen=200)

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.last_id = None
        self.synced = False
        self.tops.clear()

    def load(self, bids: Iterable, asks: Iterable) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self.bid_prices = sorted(self.bids)
        self.ask_prices = sorted(self.asks)
        self._trim()

    @staticmethod
    def _update_side(levels: Dict[float, float], prices: List[float], updates: Iterable) -> None:
        for p, q in updates:
            if q > 0:
                if p not in levels:
                    insort(prices, p)
                levels[p] = q
            elif p in levels:
                del levels[p]
                del prices[bisect_left(prices, p)]

    def apply(self, bids: Iterable, asks: Iterable) -> None:
        self._update_side(self.bids, self.bid_prices, bids)
        self._update_side(self.asks, self.ask_prices, asks)
        self._trim()

    def _trim(self) -> None:
        depth = CONSTANTS.BOOK_DEPTH
        if len(self.bid_prices) > depth:
            for p in self.bid_prices[:-depth]:
                del self.bids[p]
            del self.bid_prices[:-depth]
        if len(self.ask_prices) > depth:
            for p in self.ask_prices[depth:]:
                del self.asks[p]
            del self.ask_prices[depth:]

    def top(self, depth: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Best `depth` levels per side, best first."""
        bids, asks = self.bids, self.asks
        return ([(p, bids[p]) for p in reversed(self.bid_prices[-depth:])],
                [(p, asks[p]) for p in self.ask_prices[:depth]])

    def note_top(self) -> None:
        bids, asks = self.top(CONSTANTS.WS_CHECK_DEPTH)
        self.tops.append((self.last_id, (tuple(bids), tuple(asks))))


class PoloniexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Poloniex public market data (wss://ws.poloniex.com/ws/public): `book_lv2` for the book, `book` (depth 5) as its
    live cross-check, `trades`, and the exchange-wide `exchange` channel.

    The book is kept HERE from book_lv2, the fastest channel (non-negotiable 1): a 20-level snapshot right after each
    subscribe (0.01 s measured), then level updates chained lastId -> id. Any break — a gap, a crossed book, or the
    `book` cross-check finding the lv2 book stale — shows that market empty and re-subscribes it, which brings a fresh
    snapshot. Every applied update hands the tracker a top-BOOK_DEPTH SNAPSHOT, never a DIFF (XT's lesson: the
    tracker replays windowed diffs over a snapshot without checking update ids).

    Speed: a push is applied and handed to the tracker inside the WebSocket reader (XT measured the extra hand-off to
    the diff-listener task at ~3 ms p99 under HMB's load).

    No REST book is ever applied: Poloniex's REST book is a cache 0.5-2 s behind the stream (P1, 2026-10-05), so a
    REST snapshot landing after the stream's would roll the book back. The tracker's snapshot requests are answered
    from the live local book, or with an empty book until the stream's snapshot arrives (milliseconds).

    A market Poloniex has off (state other than NORMAL, or a tradableStartTime still ahead) is shown EMPTY while its
    symbol and trading rule stay: 5 of the 40 PAUSE markets keep a frozen two-sided book (2026-10-09). The state is
    read from GET /markets/{symbol} every TRADING_SWITCH_INTERVAL and before a market's first snapshot (the `symbols`
    socket channel read NORMAL for 25 of those 40). Maintenance mode or post-only mode (the `exchange` channel) shows
    every book empty while it lasts: no taker order can execute then.

    The stream is the book: while no connection is live, every tracked book is shown EMPTY (XT, 2026-09-30).
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "PoloniexExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "PoloniexExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._diff_output: Optional[asyncio.Queue] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._last_pong: float = 0.0
        self._warned: set = set()
        # Poloniex's per-market switch (GET /markets/{symbol}): symbols it has off, and symbols read at least once.
        self._switched_off: set = set()
        self._switch_known: set = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        # Exchange-wide maintenance / post-only mode (`exchange` channel).
        self._exchange_halt: bool = False
        # The `book` cross-check: symbol -> (monotonic time a `book` push first ran ahead of the lv2 book, the id of the
        # latest such push). Cleared the moment lv2 reaches that id.
        self._divergent_since: Dict[str, Tuple[float, int]] = {}
        self._stale_resubscribes: int = 0
        # symbol -> monotonic time its last re-subscribe was sent (RESUBSCRIBE_MIN_INTERVAL apart)
        self._last_resubscribe: Dict[str, float] = {}
        # Subscribe requests awaiting their ack, in order: (channel, symbols). Poloniex silently leaves an unknown
        # symbol out of the ack's list (P1, 2026-10-05).
        self._pending_acks: Deque[Tuple[str, List[str]]] = deque()
        # Symbols subscribed on the current connection (a second plain subscribe of one is refused).
        self._subscribed: set = set()
        self._stream_live: bool = False
        self._backoff = web_utils.PoloniexReconnectBackoff()

    # ------------------------------------------------------------------ helpers

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

    def _book(self, symbol: str) -> _LocalBook:
        book = self._books.get(symbol)
        if book is None:
            book = self._books[symbol] = _LocalBook()
        return book

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _tracked_symbols(self) -> List[str]:
        symbols = []
        for trading_pair in list(self._trading_pairs):
            try:
                symbols.append(await self._symbol_for_pair(trading_pair))
            except KeyError:
                continue  # unmapped: nothing is subscribed for it either
        return symbols

    # ------------------------------------------------------------------ the tracker's snapshot requests

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book and its hourly refresh: the live local book, or EMPTY until the stream's own
        snapshot arrives (pushed to the tracker the moment it does). Never REST: Poloniex's REST book is a cache that
        could roll the tracker back behind the stream."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"Poloniex {trading_pair} has no entry in the connector's symbol map; "
                             f"no snapshot can be built") from None
        await self._ensure_switch_known(symbol)
        book = self._book(symbol)
        deadline = time.monotonic() + 2.0
        while self._stream_live and not book.synced and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if book.synced:
            return self._snapshot_message(trading_pair, symbol, book)
        return self._message(trading_pair, 0, [], [])

    # ------------------------------------------------------------------ Poloniex's market switch

    @staticmethod
    def _is_tradable(info: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(info, dict):
            return False
        try:
            started = int(info.get("tradableStartTime") or 0) <= int(time.time() * 1e3)
        except (TypeError, ValueError):
            started = True
        return info.get("state") == "NORMAL" and started

    async def _read_switch(self, symbol: str) -> None:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.MARKET_PATH.format(symbol=symbol), domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.MARKET_LIMIT_ID,
        )
        if not isinstance(response, list):
            raise IOError(f"Unexpected Poloniex market answer for {symbol}: {response}")
        info = next((m for m in response if isinstance(m, dict) and m.get("symbol") == symbol), None)
        self._apply_switch(symbol, info)

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = not self._is_tradable(info)
        if off == (symbol in self._switched_off):
            return
        pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = ("not listed" if info is None else
                 f"state={info.get('state')} tradableStartTime={info.get('tradableStartTime')}")
        book = self._book(symbol)
        output = self._diff_output
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"Poloniex {pair}: trading is off ({flags}); its book is shown empty until it is "
                                  f"back on.")
            if output is not None and symbol in self._symbol_to_pair_cache:
                output.put_nowait(self._message(pair, book.last_id or 0, [], []))
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"Poloniex {pair}: trading is on ({flags}); its book is shown again.")
            if book.synced and output is not None and symbol in self._symbol_to_pair_cache:
                self._emit(symbol, book, output)

    async def _ensure_switch_known(self, symbol: str) -> None:
        """Before a market's first snapshot reaches the tracker, so a market that is already off never shows a frozen
        book."""
        if symbol in self._switch_known:
            return
        async with self._switch_lock:
            if symbol in self._switch_known:
                return
            try:
                await self._read_switch(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once(f"switch-first-read:{symbol}",
                                f"Poloniex trading-switch read for {symbol} failed ({e}); it counts as on until the next "
                                f"read (every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            failed: List[str] = []
            for symbol in await self._tracked_symbols():
                try:
                    await self._read_switch(symbol)     # one market's failed read never skips the others
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    failed.append(f"{symbol}: {e}")
            if failed and not self._switch_failing:
                self._switch_failing = True
                self.logger().warning(f"Poloniex trading-switch read failed for {len(failed)} market(s) "
                                      f"({failed[:3]}); they keep their last state. Retrying every "
                                      f"{CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            elif not failed and self._switch_failing:
                self._switch_failing = False
                self.logger().info("Poloniex trading-switch reads work again.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_PUBLIC_URL)

    async def _send_subscribe(self, ws: WSAssistant, channel: str, symbols: List[str]) -> None:
        payload: Dict[str, Any] = {"event": "subscribe", "channel": [channel], "symbols": symbols}
        if channel == CONSTANTS.WS_CHECK_CHANNEL:
            payload["depth"] = CONSTANTS.WS_CHECK_DEPTH
        self._pending_acks.append((channel, list(symbols)))
        await ws.send(WSJSONRequest(payload=payload))

    async def _subscribe_symbols(self, ws: WSAssistant, symbols: List[str]) -> None:
        for i in range(0, len(symbols), CONSTANTS.WS_SYMBOLS_PER_REQUEST):
            batch = symbols[i:i + CONSTANTS.WS_SYMBOLS_PER_REQUEST]
            for channel in (CONSTANTS.WS_BOOK_CHANNEL, CONSTANTS.WS_CHECK_CHANNEL, CONSTANTS.WS_TRADES_CHANNEL):
                await self._send_subscribe(ws, channel, batch)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every Poloniex book empty (XT, 2026-09-23).
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"Poloniex {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
                    continue
                self._book(symbol).reset()
                symbols.append(symbol)
            self._subscribed = set(symbols)
            self._pending_acks.clear()
            self._divergent_since.clear()
            self._stream_live = True
            self._last_pong = time.monotonic()
            self._backoff.connected()
            await ws.send(WSJSONRequest(payload={"event": "subscribe", "channel": [CONSTANTS.WS_EXCHANGE_CHANNEL]}))
            await self._subscribe_symbols(ws, symbols)
            self.logger().info(f"Subscribed to Poloniex book_lv2, book (check) and trades for {len(symbols)} markets.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Poloniex public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which blanks every Poloniex book for a reconnect."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        await self._ensure_switch_known(symbol)
        if symbol in self._subscribed:
            # Already subscribed on this connection (a pair the startup walk hasn't reached, or a second add): a plain
            # subscribe would be refused and a reset book would never get its snapshot.
            book = self._books.get(symbol)
            if book is not None and book.synced:
                if self._diff_output is not None:
                    self._emit(symbol, book, self._diff_output)     # the tracker's book gets the live one now
                return
            await self._resubscribe_book(symbol, f"runtime add of {trading_pair}, subscribed but without a book")
            return
        self._book(symbol).reset()
        await self._subscribe_symbols(ws, [symbol])
        self._subscribed.add(symbol)
        self.logger().info(f"Subscribed Poloniex {trading_pair} on the live connection (no reconnect).")

    def _schedule_resubscribe(self, symbol: str, reason: str) -> None:
        """From the reader: one rebuild at a time per market. The book stops taking pushes at once (its fresh snapshot
        follows the re-subscribe), so a burst of broken pushes schedules one rebuild, not one per push."""
        book = self._books.get(symbol)
        if book is None or not book.synced:
            return
        book.synced = False
        asyncio.ensure_future(self._resubscribe_book(symbol, reason))

    async def _resubscribe_book(self, symbol: str, reason: str) -> None:
        """Rebuild one market's book from a fresh lv2 snapshot: unsubscribe, then subscribe again (a subscribe for a
        symbol already subscribed is refused). The socket and every other book are untouched. Re-subscribes of one
        market are RESUBSCRIBE_MIN_INTERVAL apart: a market whose fresh snapshot is broken again is retried at that
        pace, never in a loop at socket speed; it shows empty meanwhile."""
        ws = self._active_ws
        self._show_empty(symbol, self._diff_output)
        self._book(symbol).reset()
        self._divergent_since.pop(symbol, None)
        if ws is None:
            return  # the next connection resubscribes it
        now = time.monotonic()
        wait = self._last_resubscribe.get(symbol, float("-inf")) + CONSTANTS.RESUBSCRIBE_MIN_INTERVAL - now
        self._last_resubscribe[symbol] = now + max(wait, 0.0)
        if wait > 0:
            await asyncio.sleep(wait)
            if self._active_ws is not ws:
                return  # reconnected meanwhile: the new connection subscribed it afresh
        self.logger().warning(f"Poloniex {self._symbol_to_pair_cache.get(symbol, symbol)}: {reason}; re-subscribing "
                              f"its book for a fresh snapshot.")
        try:
            await ws.send(WSJSONRequest(payload={"event": "unsubscribe", "channel": [CONSTANTS.WS_BOOK_CHANNEL],
                                                 "symbols": [symbol]}))
            await self._send_subscribe(ws, CONSTANTS.WS_BOOK_CHANNEL, [symbol])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Poloniex re-subscribe of {symbol} failed ({e}); reconnecting.")
            await ws.disconnect()

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        await self._resubscribe_book(symbol, "the orchestrator asked for a fresh book")

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """{"event":"ping"} every WS_HEARTBEAT_INTERVAL; a pong that doesn't come within WS_PONG_TIMEOUT ends the
        connection (the listen loop reconnects). The server itself ends a session silent for 30 s."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                sent = time.monotonic()
                await ws.send(WSJSONRequest(payload={"event": "ping"}))
                await asyncio.sleep(CONSTANTS.WS_PONG_TIMEOUT)
                if self._last_pong < sent:
                    self.logger().warning(f"Poloniex public WS: no pong within {CONSTANTS.WS_PONG_TIMEOUT} s; "
                                          f"reconnecting.")
                    await ws.disconnect()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Poloniex public WS keepalive stopped: {e}")

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = asyncio.ensure_future(self._trading_switch_loop())
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        diff_queue = self._message_queue[self._diff_messages_queue_key]
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if not isinstance(data, dict):
                continue
            event = data.get("event")
            if event is not None:
                self._on_event(data)
                continue
            channel = data.get("channel")
            if channel == CONSTANTS.WS_BOOK_CHANNEL:
                output = self._diff_output
                if output is not None and diff_queue.empty():
                    # Applied here, now. The queue is used only while it holds something (a push that came before
                    # the listener started), so the arrival order is kept.
                    self._on_lv2(data, output)
                else:
                    diff_queue.put_nowait(data)
            elif channel == CONSTANTS.WS_CHECK_CHANNEL:
                self._on_check(data)
            elif channel == CONSTANTS.WS_TRADES_CHANNEL:
                trade_queue.put_nowait(data)
            elif channel == CONSTANTS.WS_EXCHANGE_CHANNEL:
                self._on_exchange(data)

    def _on_event(self, data: Dict[str, Any]) -> None:
        event = str(data.get("event") or "").lower()
        if event == "pong":
            self._last_pong = time.monotonic()
        elif event == "subscribe" and data.get("channel") in (CONSTANTS.WS_BOOK_CHANNEL, CONSTANTS.WS_CHECK_CHANNEL,
                                                               CONSTANTS.WS_TRADES_CHANNEL):
            # The oldest pending request of this channel is the one answered (one answer per request, in order).
            request = next((r for r in self._pending_acks if r[0] == data.get("channel")), None)
            if request is not None:
                self._pending_acks.remove(request)
                accepted = {str(s).upper() for s in data.get("symbols") or []}
                for symbol in request[1]:
                    if symbol.upper() not in accepted:
                        self._warn_once(f"refused:{data.get('channel')}:{symbol}",
                                        f"Poloniex left {symbol} out of its {data.get('channel')} subscription (an "
                                        f"unknown or delisted symbol): its book stays empty.")
        elif event == "error":
            request = self._pending_acks.popleft() if self._pending_acks else None
            self.logger().warning(f"Poloniex public WS request refused: {data.get('message')} (request: {request})")

    def _on_lv2(self, raw_message: Dict[str, Any], output: asyncio.Queue) -> None:
        action = raw_message.get("action")
        for item in raw_message.get("data") or []:
            symbol = item.get("symbol")
            if not symbol or symbol not in self._symbol_to_pair_cache:
                continue
            book = self._book(symbol)
            try:
                update_id = int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue
            if action == "snapshot":
                book.load(_levels(item.get("bids")), _levels(item.get("asks")))
                book.last_id = update_id
                book.synced = True
            else:
                if not book.synced:
                    continue  # a late update of a book being rebuilt: its snapshot follows the re-subscribe
                if update_id <= book.last_id:
                    continue
                if int(item.get("lastId") or 0) != book.last_id:
                    self._schedule_resubscribe(
                        symbol, f"book_lv2 sequence gap (lastId {item.get('lastId')}, expected {book.last_id})")
                    continue
                book.apply(_levels(item.get("bids")), _levels(item.get("asks")))
                book.last_id = update_id
            pending = self._divergent_since.get(symbol)
            if pending is not None and book.last_id >= pending[1]:
                del self._divergent_since[symbol]   # lv2 caught up with the `book` push that ran ahead
            self._emit(symbol, book, output)

    def _emit(self, symbol: str, book: _LocalBook, output: asyncio.Queue) -> None:
        pair = self._symbol_to_pair_cache[symbol]
        if symbol in self._switched_off or self._exchange_halt:
            output.put_nowait(self._message(pair, book.last_id or 0, [], []))
            return
        bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen with an unbroken sequence; treat it as corruption and start over.
            self._schedule_resubscribe(symbol, f"crossed book (bid {bids[0][0]} >= ask {asks[0][0]})")
            return
        book.note_top()
        output.put_nowait(self._message(pair, book.last_id, bids, asks))

    def _on_check(self, raw_message: Dict[str, Any]) -> None:
        """
        The `book` channel (depth 5, the same sequence ids) against the lv2 book:
          - a `book` push at an id the lv2 book has passed: its top must equal the lv2 top at that same id (identical
            on 23,916 of 23,916 compared ids, 2026-10-09); a difference means the lv2 book is wrong: rebuilt at once;
          - a `book` push AHEAD of the lv2 book with a top lv2 never showed: normal for a moment (lv2 leads on 96 %, p99
            ~100 ms). The clock starts at the first such push and stops the moment lv2 reaches the latest one's id;
            BOOK_DIVERGENCE_S without that and the lv2 stream of the market is stale: rebuilt.
        """
        now = time.monotonic()
        for item in raw_message.get("data") or []:
            symbol = item.get("symbol")
            book = self._books.get(symbol)
            if book is None or not book.synced or symbol in self._switched_off:
                continue
            try:
                check_id = int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue
            top = (tuple(_levels(item.get("bids"))[:CONSTANTS.WS_CHECK_DEPTH]),
                   tuple(_levels(item.get("asks"))[:CONSTANTS.WS_CHECK_DEPTH]))
            if check_id <= book.last_id:
                seen = next((t for i, t in reversed(book.tops) if i == check_id), None)
                if seen is not None and seen != top:
                    self._schedule_resubscribe(symbol, f"book_lv2 top differs from the book channel's at the same id "
                                                       f"{check_id}")
                continue
            if any(seen == top for _, seen in book.tops):
                # A state lv2 already showed: `book` re-sends states under ids lv2 never sends (2,316 a 300-s run,
                # each equal to lv2's earlier state, 2026-10-09). Not a lead.
                self._divergent_since.pop(symbol, None)
                continue
            pending = self._divergent_since.get(symbol)
            first = pending[0] if pending is not None else now
            self._divergent_since[symbol] = (first, check_id)
            if now - first >= CONSTANTS.BOOK_DIVERGENCE_S:
                self._stale_resubscribes += 1
                self._schedule_resubscribe(
                    symbol, f"book_lv2 behind the book channel for {now - first:.1f} s (lv2 id {book.last_id}, book id "
                            f"{check_id}; stale re-subscribes so far: {self._stale_resubscribes})")

    def _on_exchange(self, raw_message: Dict[str, Any]) -> None:
        for item in raw_message.get("data") or []:
            flags = {str(k).lower(): str(v).upper() for k, v in (item or {}).items()}
            halt = flags.get("mm") == "ON" or flags.get("pom") == "ON"
            if halt == self._exchange_halt:
                continue
            self._exchange_halt = halt
            output = self._diff_output
            if halt:
                self.logger().warning(f"Poloniex is in maintenance / post-only mode ({flags}); every book is shown "
                                      f"empty until it ends.")
            else:
                self.logger().info(f"Poloniex left maintenance / post-only mode ({flags}); books are shown again.")
            if output is None:
                continue
            for symbol, book in self._books.items():
                if symbol not in self._symbol_to_pair_cache:
                    continue
                if halt:
                    output.put_nowait(self._message(self._symbol_to_pair_cache[symbol], book.last_id or 0, [], []))
                elif book.synced:
                    self._emit(symbol, book, output)

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
        self._subscribed.clear()
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        output = self._diff_output
        if was_live and output is not None:
            # Every book the tracker holds would stay frozen from here on: show them empty until the next connection
            # rebuilds them.
            for trading_pair in list(self._trading_pairs):
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                book = self._books.get(symbol) if symbol is not None else None
                output.put_nowait(self._message(trading_pair, (book.last_id if book else None) or 0, [], []))
        for book in self._books.values():
            book.reset()
        await self._backoff.wait()  # the listen loop reconnects right after this

    # ------------------------------------------------------------------ the local book

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """The queued path (pushes that came before the listener started)."""
        for item in raw_message.get("data") or []:
            symbol = item.get("symbol")
            if symbol and symbol not in self._symbol_to_pair_cache:
                try:
                    pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                except KeyError:
                    continue
                self._symbol_to_pair_cache[symbol] = pair
                self._pair_to_symbol_cache[pair] = symbol
        self._on_lv2(raw_message, message_queue)

    @staticmethod
    def _message(trading_pair: str, update_id: int, bids: list, asks: list) -> OrderBookMessage:
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _LocalBook) -> OrderBookMessage:
        if symbol in self._switched_off or self._exchange_halt:
            return self._message(trading_pair, book.last_id or 0, [], [])
        bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
        return self._message(trading_pair, book.last_id, bids, asks)

    def _show_empty(self, symbol: str, output: Optional[asyncio.Queue]) -> None:
        """This market's book is being rebuilt: show it empty (a halt, as for Poloniex's switch) until the rebuilt book
        is emitted. Call before book.reset()."""
        pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and pair is not None:
            book = self._books.get(symbol)
            output.put_nowait(self._message(pair, (book.last_id if book else None) or 0, [], []))

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # {"channel":"trades","data":[{symbol, amount, takerSide (buy|sell), quantity, createTime, price, id, ts}]}
        for item in raw_message.get("data") or []:
            symbol = item.get("symbol")
            if not symbol:
                continue
            try:
                trading_pair = self._symbol_to_pair_cache.get(symbol) or \
                    await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except KeyError:
                continue
            timestamp_ms = int(item.get("createTime") or time.time() * 1e3)
            trade_type = TradeType.BUY if str(item.get("takerSide") or "").lower() == "buy" else TradeType.SELL
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": str(item.get("id")),
                    "update_id": timestamp_ms,
                    "price": item.get("price"),
                    "amount": item.get("quantity"),
                },
                timestamp=timestamp_ms * 1e-3,
            ))

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message.get("channel")
        if channel == CONSTANTS.WS_BOOK_CHANNEL:
            return self._diff_messages_queue_key
        if channel == CONSTANTS.WS_TRADES_CHANNEL:
            return self._trade_messages_queue_key
        return ""
