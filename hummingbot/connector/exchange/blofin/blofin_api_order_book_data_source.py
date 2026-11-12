import asyncio
import time
from bisect import bisect_left, insort
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

from hummingbot.connector.exchange.blofin import blofin_constants as CONSTANTS, blofin_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.blofin.blofin_exchange import BlofinExchange


def _levels(raw: Any) -> List[Tuple[float, float]]:
    """[[price, qty], ...] (strings) as floats."""
    return [(float(level[0]), float(level[1])) for level in raw or [] if len(level) >= 2]


class _LocalBook:
    """
    One market's book from BloFin's sequenced DEPTH channel. Each side keeps a dict (price -> quantity) plus its prices in
    ascending order, so the top N is a slice (XT's lesson: rebuilding the top N per push was the largest cost per push).
    Kept to BOOK_KEEP_LEVELS a side (what DEPTH's snapshot carries).
    """

    __slots__ = ("bids", "asks", "bid_prices", "ask_prices", "last_id", "synced", "subscribed_at", "last_frame",
                 "last_change", "emitted", "emit_id", "xcheck_misses")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_prices: List[float] = []
        self.ask_prices: List[float] = []
        self.last_id: Optional[int] = None     # the update_id of the last DEPTH frame applied
        self.synced = False
        self.subscribed_at = 0.0               # monotonic time its channels were (re)subscribed
        self.last_frame = 0.0                  # monotonic time of this market's last book frame (either channel)
        self.last_change = 0.0                 # monotonic time the local book last changed
        self.emitted: Optional[tuple] = None   # the top last handed to the tracker
        self.emit_id = 0
        self.xcheck_misses = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.last_id = None
        self.synced = False
        self.emitted = None
        self.xcheck_misses = 0

    def load(self, bids: Iterable[Tuple[float, float]], asks: Iterable[Tuple[float, float]], update_id: int) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self.bid_prices = sorted(self.bids)
        self.ask_prices = sorted(self.asks)
        self._trim()
        self.last_id = update_id
        self.synced = True
        self.last_change = time.monotonic()
        self.xcheck_misses = 0

    def apply(self, bids: Iterable[Tuple[float, float]], asks: Iterable[Tuple[float, float]], update_id: int) -> None:
        """A DEPTH UPDATE: absolute quantities per price level; 0 deletes the level."""
        for levels, prices, side in ((self.bids, self.bid_prices, bids), (self.asks, self.ask_prices, asks)):
            for price, qty in side:
                if qty > 0:
                    if price not in levels:
                        insort(prices, price)
                    levels[price] = qty
                elif price in levels:
                    del levels[price]
                    del prices[bisect_left(prices, price)]
        self._trim()
        self.last_id = update_id
        self.last_change = time.monotonic()

    def _trim(self) -> None:
        keep = CONSTANTS.BOOK_KEEP_LEVELS
        if len(self.bid_prices) > keep:
            for p in self.bid_prices[:-keep]:
                del self.bids[p]
            del self.bid_prices[:-keep]
        if len(self.ask_prices) > keep:
            for p in self.ask_prices[keep:]:
                del self.asks[p]
            del self.ask_prices[keep:]

    def top(self, depth: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Best `depth` levels per side, best first."""
        bids, asks = self.bids, self.asks
        return ([(p, bids[p]) for p in reversed(self.bid_prices[-depth:])],
                [(p, asks[p]) for p in self.ask_prices[:depth]])


class BlofinAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    BloFin public market data from the website's spot socket (wss://ws-public.blofin.com/ws/spot/public; the API socket
    is perpetuals only): the book from DEPTH, DEPTH-20 as its liveness signal and cross-check, TRADE for trades.

    Non-negotiable 1 (the freshest CORRECT local book) decides the design, measured on myserver 2026-11-11
    (hmb_local_tools/blofin/debug_blofin_ws_cadence.py: 8 markets x 120 s, both channels at 100ms on one socket):
      - DEPTH: a snapshot (<= 400 levels a side) on subscribe, then UPDATE deltas chained by last_update_id = the
        previous frame's update_id, 0 gaps. It showed each new top first, ahead of DEPTH-20 by p50 43-66 ms (p90
        128-182 ms), and every top DEPTH-20 showed. A re-subscribe returns a fresh snapshot in ~7 ms.
      - DEPTH-20 (500ms): a 20-level snapshot at a fixed cadence, changed or not, equal to the DEPTH book at arrival on
        99.9% of pushes. Not a book source here: it lags. Used as the per-market liveness signal (a market with no
        DEPTH-20 frame for MARKET_SILENCE_S has a dead stream: shown EMPTY and re-subscribed) and as the cross-check (a
        frame that arrives after the local book was quiet for XCHECK_QUIET_S must equal its top 20; XCHECK_MISSES in a
        row that don't mean the book diverged: re-subscribed).
    A DEPTH frame that doesn't continue the book's chain (a gap), and a crossed book, re-subscribe the market: shown
    EMPTY until its fresh snapshot (~7 ms). No REST book is ever applied: it carries no sequence.

    Every change of the top BOOK_DEPTH hands the tracker a SNAPSHOT, never a DIFF (XT's lesson: the tracker replays
    windowed diffs over a snapshot without checking update ids), applied in the WebSocket reader (XT measured the
    hand-off to the diff-listener task at ~3 ms p99 under HMB's load).

    A market BloFin has off (instrument state not `live`) is shown EMPTY while its symbol and trading rule stay
    (non-negotiable 3). While no connection is live, every tracked book is shown EMPTY.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "BlofinExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "BlofinExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._diff_output: Optional[asyncio.Queue] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._last_pong: float = 0.0
        self._last_socket_frame: float = 0.0
        self._warned: Set[str] = set()
        self._switched_off: Set[str] = set()
        self._switch_known = False
        self._switch_failing = False
        self._subscribed: Set[str] = set()
        self._resubscribing: Set[str] = set()     # markets with a re-subscribe scheduled or under way
        self._last_resubscribe: Dict[str, float] = {}
        self._dead_streak: Dict[str, int] = {}    # watchdog repairs in a row that brought no snapshot (_watchdog_loop)
        self._stream_live = False
        self._backoff = web_utils.BlofinReconnectBackoff()
        self._request_id = 0
        # counters, logged with each re-subscribe warning
        self._gap_resubscribes = 0
        self._xcheck_resubscribes = 0
        self._crossed_resubscribes = 0

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

    # ------------------------------------------------------------------ the tracker's snapshot requests

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book and its hourly refresh: the live local book, or EMPTY until the stream's first
        snapshot arrives (pushed to the tracker the moment it does). Never REST: BloFin's REST book carries no sequence,
        so it could roll the book back behind the stream."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"BloFin {trading_pair} has no entry in the connector's symbol map; "
                             f"no snapshot can be built") from None
        await self._ensure_switch_known()
        book = self._book(symbol)
        deadline = time.monotonic() + 2.0
        while self._stream_live and not book.synced and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if book.synced and symbol not in self._switched_off:
            bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
            return self._message(trading_pair, book, bids, asks)
        return self._message(trading_pair, book, [], [])

    # ------------------------------------------------------------------ BloFin's trading switch

    async def _read_switches(self) -> None:
        off = await self._connector.markets_switched_off()
        self._switch_known = True
        changed_off = off - self._switched_off
        changed_on = self._switched_off - off
        self._switched_off = set(off)
        output = self._diff_output
        for symbol in changed_off:
            if symbol in self._symbol_to_pair_cache:
                self.logger().warning(f"BloFin {self._symbol_to_pair_cache[symbol]}: the instrument is not live "
                                      f"(state); its book is shown empty until it is.")
                if output is not None:
                    book = self._book(symbol)
                    output.put_nowait(self._message(self._symbol_to_pair_cache[symbol], book, [], []))
                    book.emitted = ()
        for symbol in changed_on:
            if symbol in self._symbol_to_pair_cache:
                self.logger().info(f"BloFin {self._symbol_to_pair_cache[symbol]}: the instrument is live again; its "
                                   f"book is shown.")
                book = self._books.get(symbol)
                if book is not None and book.synced and output is not None:
                    self._emit(symbol, book, output, force=True)

    async def _ensure_switch_known(self) -> None:
        if self._switch_known:
            return
        try:
            await self._read_switches()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._warn_once("switch-first-read", f"BloFin trading-switch read failed ({type(e).__name__}: {e}); "
                                                 f"markets count as live until the next read (every "
                                                 f"{CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("BloFin trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"BloFin trading-switch read failed ({type(e).__name__}: {e}); markets keep "
                                          f"their last state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _send(self, ws: WSAssistant, op: str, channel: str, speed: Optional[str], symbols: List[str]) -> None:
        per = CONSTANTS.WS_TOPICS_PER_REQUEST
        for i in range(0, len(symbols), per):
            args = []
            for symbol in symbols[i:i + per]:
                arg = {"channel": channel, "symbol": symbol}
                if speed:
                    arg["update_speed"] = speed
                args.append(arg)
            await ws.send(WSJSONRequest(payload={"id": self._next_id(), "op": op, "name": channel, "args": args}))

    async def _send_markets(self, ws: WSAssistant, symbols: List[str], op: str, book_only: bool = False) -> None:
        """DEPTH first (its snapshot starts the book), then DEPTH-20 (liveness + cross-check), then the trades."""
        await self._send(ws, op, CONSTANTS.WS_DEPTH_CHANNEL, CONSTANTS.WS_DEPTH_SPEED, symbols)
        await self._send(ws, op, CONSTANTS.WS_CHECK_CHANNEL, CONSTANTS.WS_CHECK_SPEED, symbols)
        if not book_only:
            await self._send(ws, op, CONSTANTS.WS_TRADE_CHANNEL, None, symbols)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every BloFin book empty (XT, 2026-09-23).
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"BloFin {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
                    continue
                symbols.append(symbol)
            now = time.monotonic()
            for symbol in symbols:
                book = self._book(symbol)
                book.reset()
                book.subscribed_at = now
                book.last_frame = now
            self._subscribed = set(symbols)
            self._stream_live = True
            self._last_pong = now
            self._last_socket_frame = now
            self._backoff.connected()
            if symbols:
                await self._send_markets(ws, symbols, "SUBSCRIBE")
            self.logger().info(f"Subscribed to BloFin DEPTH ({CONSTANTS.WS_DEPTH_SPEED}), DEPTH-20 "
                               f"({CONSTANTS.WS_CHECK_SPEED}) and TRADE for {len(symbols)} markets.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to BloFin public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which blanks every BloFin book for a reconnect."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        await self._ensure_switch_known()
        if symbol in self._subscribed:
            book = self._books.get(symbol)
            if book is not None and book.synced and self._diff_output is not None:
                self._emit(symbol, book, self._diff_output, force=True)
                return
            await self._resubscribe(symbol, f"runtime add of {trading_pair}, subscribed but without a book")
            return
        book = self._book(symbol)
        book.reset()
        book.subscribed_at = book.last_frame = time.monotonic()
        await self._send_markets(ws, [symbol], "SUBSCRIBE")
        self._subscribed.add(symbol)
        self.logger().info(f"Subscribed BloFin {trading_pair} on the live connection (no reconnect).")

    def _schedule_resubscribe(self, symbol: str, reason: str, quiet: bool = False) -> None:
        """From the frame path (a gap, a cross-check miss, a crossed book) and the watchdog: the book is unsynced at
        once, so the frames that follow are ignored until the fresh snapshot instead of each scheduling another repair,
        and at most one re-subscribe per market is ever scheduled. `quiet`: logged at DEBUG (a dead market's repeats)."""
        if symbol in self._resubscribing:
            return
        self._resubscribing.add(symbol)
        self._book(symbol).reset()
        self._show_empty(symbol)
        asyncio.ensure_future(self._resubscribe(symbol, reason, quiet=quiet))

    async def _resubscribe(self, symbol: str, reason: str, quiet: bool = False) -> None:
        """Rebuild one market's book from a fresh DEPTH snapshot: UNSUBSCRIBE then SUBSCRIBE its two book channels on the
        live socket (~7 ms to the snapshot, live); the socket and every other book are untouched. At most once per
        RESUBSCRIBE_MIN_INTERVAL per market; it shows empty meanwhile."""
        self._resubscribing.add(symbol)
        try:
            ws = self._active_ws
            self._show_empty(symbol)
            book = self._book(symbol)
            book.reset()
            if ws is None:
                return
            now = time.monotonic()
            wait = self._last_resubscribe.get(symbol, float("-inf")) + CONSTANTS.RESUBSCRIBE_MIN_INTERVAL - now
            self._last_resubscribe[symbol] = now + max(wait, 0.0)
            if wait > 0:
                await asyncio.sleep(wait)
                if self._active_ws is not ws:
                    return
            log = self.logger().debug if quiet else self.logger().warning
            log(f"BloFin {self._symbol_to_pair_cache.get(symbol, symbol)}: {reason}; re-subscribing its book (gaps "
                f"{self._gap_resubscribes}, cross-check {self._xcheck_resubscribes}, crossed "
                f"{self._crossed_resubscribes} so far).")
            try:
                book.reset()
                book.subscribed_at = book.last_frame = time.monotonic()
                await self._send_markets(ws, [symbol], "UNSUBSCRIBE", book_only=True)
                await self._send_markets(ws, [symbol], "SUBSCRIBE", book_only=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"BloFin re-subscribe of {symbol} failed ({type(e).__name__}); reconnecting.")
                await ws.disconnect()
        finally:
            self._resubscribing.discard(symbol)

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        await self._resubscribe(symbol, "the orchestrator asked for a fresh book")

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """{"op":"PING"} every WS_PING_INTERVAL; no PONG for WS_PONG_TIMEOUT ends the connection."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_PING_INTERVAL)
                if time.monotonic() - self._last_pong > CONSTANTS.WS_PONG_TIMEOUT + CONSTANTS.WS_PING_INTERVAL:
                    self.logger().warning(f"BloFin public WS: no PONG for "
                                          f"{time.monotonic() - self._last_pong:.0f} s; reconnecting.")
                    await ws.disconnect()
                    return
                await ws.send(WSJSONRequest(payload={"op": "PING"}))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"BloFin public WS keepalive stopped: {type(e).__name__}")

    async def _watchdog_loop(self) -> None:
        """Every second: a socket that went silent while markets are subscribed is reconnected; a market with no book
        frame for MARKET_SILENCE_S (DEPTH-20 pushes twice a second, changed or not) or still without a snapshot
        UNSYNCED_LIMIT_S after its subscribe is shown empty and re-subscribed. BloFin acks a subscription to any
        symbol, so a market that never streams would be repaired every few seconds forever (review, 2026-11-11): the
        limits double with each repair in a row that brought no snapshot, up to DEAD_MARKET_RETRY_CAP_S, with one
        WARNING per streak and one more at the cap; a snapshot ends the streak. Markets BloFin has off are shown empty
        and not repaired."""
        while True:
            await asyncio.sleep(1.0)
            try:
                await self._watchdog_check()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"BloFin book watchdog: {type(e).__name__}: {e}")

    async def _watchdog_check(self) -> None:
        """One pass of the watchdog (_watchdog_loop)."""
        ws = self._active_ws
        if ws is None or not self._stream_live:
            return
        now = time.monotonic()
        if self._subscribed and now - self._last_socket_frame > CONSTANTS.SOCKET_SILENCE_S:
            self.logger().warning(f"BloFin public WS: no frame for {now - self._last_socket_frame:.1f} s with "
                                  f"{len(self._subscribed)} markets subscribed; reconnecting.")
            await ws.disconnect()
            return
        for symbol in list(self._subscribed):
            book = self._books.get(symbol)
            if book is None or symbol in self._resubscribing or symbol in self._switched_off:
                continue
            streak = self._dead_streak.get(symbol, 0)
            unsynced_limit = min(CONSTANTS.UNSYNCED_LIMIT_S * 2 ** streak, CONSTANTS.DEAD_MARKET_RETRY_CAP_S)
            silence_limit = min(CONSTANTS.MARKET_SILENCE_S * 2 ** streak, CONSTANTS.DEAD_MARKET_RETRY_CAP_S)
            if not book.synced and now - book.subscribed_at > unsynced_limit:
                reason = f"no DEPTH snapshot {unsynced_limit:.0f} s after its subscribe"
            elif now - book.last_frame > silence_limit:
                reason = f"no book frame for {silence_limit:.0f} s (its stream died)"
            else:
                continue
            book.subscribed_at = book.last_frame = now
            self._dead_streak[symbol] = streak + 1
            at_cap = min(unsynced_limit, silence_limit) >= CONSTANTS.DEAD_MARKET_RETRY_CAP_S
            if streak > 0 and at_cap and not self._dead_at_cap(symbol):
                self.logger().warning(f"BloFin {self._symbol_to_pair_cache.get(symbol, symbol)}: still no book after "
                                      f"{streak} re-subscribes; retrying every "
                                      f"{CONSTANTS.DEAD_MARKET_RETRY_CAP_S:.0f} s (shown empty meanwhile).")
            self._schedule_resubscribe(symbol, reason, quiet=streak > 0)

    def _dead_at_cap(self, symbol: str) -> bool:
        """True after the first time a dead market's retries reached the cap (its cap WARNING is then said)."""
        key = f"dead-at-cap:{symbol}"
        if key in self._warned:
            return True
        self._warned.add(key)
        return False

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = asyncio.ensure_future(self._trading_switch_loop())
        self._watchdog_task = asyncio.ensure_future(self._watchdog_loop())
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()
            self._watchdog_task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        diff_queue = self._message_queue[self._diff_messages_queue_key]
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            self._last_socket_frame = time.monotonic()
            if not isinstance(data, dict):
                continue
            channel = data.get("channel")
            if channel is None:
                self._on_control_frame(data)
            elif channel == CONSTANTS.WS_TRADE_CHANNEL:
                trade_queue.put_nowait(data)
            elif channel in (CONSTANTS.WS_DEPTH_CHANNEL, CONSTANTS.WS_CHECK_CHANNEL):
                output = self._diff_output
                if output is not None and diff_queue.empty():
                    # Applied here, now. The queue is used only while it holds something (a frame that came before the
                    # listener started), so the arrival order is kept.
                    self._on_book_frame(data, output)
                else:
                    diff_queue.put_nowait(data)

    def _on_control_frame(self, data: Dict[str, Any]) -> None:
        op = data.get("op")
        if op == "PONG":
            self._last_pong = time.monotonic()
            return
        if op == "PING":
            ws = self._active_ws
            if ws is not None:
                asyncio.ensure_future(ws.send(WSJSONRequest(payload={"op": "PONG"})))
            return
        code = data.get("code")
        if code is not None and str(code) != "200":
            # An ack is {"code":200,...,"id":N} even for an unknown symbol (it then never streams: the watchdog's
            # silence check catches it). A bad request answers 500 "Invalid request".
            self._warn_once(f"refused:{code}:{data.get('message')}",
                            f"BloFin refused a public subscription request: {data}")

    # ------------------------------------------------------------------ the local book

    def _on_book_frame(self, data: Dict[str, Any], output: asyncio.Queue) -> None:
        symbol = data.get("symbol")
        if symbol not in self._symbol_to_pair_cache or symbol not in self._subscribed:
            return
        book = self._book(symbol)
        book.last_frame = time.monotonic()
        try:
            if data.get("channel") == CONSTANTS.WS_DEPTH_CHANNEL:
                self._on_depth(symbol, book, data, output)
            else:
                self._on_check(symbol, book, data)
        except (KeyError, TypeError, ValueError, IndexError) as e:
            self._warn_once(f"bad-frame:{data.get('channel')}",
                            f"BloFin {data.get('channel')} frame not understood ({type(e).__name__}: {e}); "
                            f"skipped: {str(data)[:200]}")

    def _on_depth(self, symbol: str, book: _LocalBook, data: Dict[str, Any], output: asyncio.Queue) -> None:
        update_id = int(data["update_id"])
        if data.get("data_type") == "SNAPSHOT":
            book.load(_levels(data.get("bids")), _levels(data.get("asks")), update_id)
            streak = self._dead_streak.pop(symbol, 0)
            if streak > 1:
                self._warned.discard(f"dead-at-cap:{symbol}")
                self.logger().info(f"BloFin {self._symbol_to_pair_cache.get(symbol, symbol)}: book frames again after "
                                   f"{streak} re-subscribes.")
            self._emit(symbol, book, output)
            return
        if not book.synced:
            return  # before the first snapshot of this subscription
        previous = data.get("last_update_id")
        if previous is None or int(previous) != book.last_id:
            if previous is not None and int(previous) < book.last_id and update_id <= book.last_id:
                return  # a frame the book already holds (it can't come twice on one chain; harmless if it does)
            self._gap_resubscribes += 1
            self._schedule_resubscribe(symbol, f"a DEPTH gap (last_update_id {previous} after update_id {book.last_id})")
            return
        book.apply(_levels(data.get("bids")), _levels(data.get("asks")), update_id)
        self._emit(symbol, book, output)

    def _on_check(self, symbol: str, book: _LocalBook, data: Dict[str, Any]) -> None:
        """A DEPTH-20 frame: liveness (last_frame, above), and the cross-check once the local book has been quiet long
        enough for DEPTH-20 to have caught up with it (it trails DEPTH by p90 ~150 ms)."""
        if not book.synced or time.monotonic() - book.last_change < CONSTANTS.XCHECK_QUIET_S:
            return
        theirs = (tuple(_levels(data.get("bids"))[:CONSTANTS.BOOK_DEPTH]),
                  tuple(_levels(data.get("asks"))[:CONSTANTS.BOOK_DEPTH]))
        bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
        if (tuple(bids), tuple(asks)) == theirs:
            book.xcheck_misses = 0
            return
        book.xcheck_misses += 1
        if book.xcheck_misses >= CONSTANTS.XCHECK_MISSES:
            self._xcheck_resubscribes += 1
            self._schedule_resubscribe(
                symbol, f"the local book differs from DEPTH-20 on {book.xcheck_misses} quiet checks in a row "
                        f"(local top {bids[:1]} / {asks[:1]}, DEPTH-20 {theirs[0][:1]} / {theirs[1][:1]})")

    def _emit(self, symbol: str, book: _LocalBook, output: asyncio.Queue, force: bool = False) -> None:
        pair = self._symbol_to_pair_cache[symbol]
        if symbol in self._switched_off:
            if book.emitted != ():
                output.put_nowait(self._message(pair, book, [], []))
                book.emitted = ()
            return
        bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen on an unbroken chain: corruption. Shown empty and re-subscribed (a fresh snapshot in ~7 ms).
            self._crossed_resubscribes += 1
            self._schedule_resubscribe(
                symbol, f"crossed local book (bid {bids[0][0]} >= ask {asks[0][0]}) at update_id {book.last_id}")
            return
        key = (tuple(bids), tuple(asks))
        if not force and key == book.emitted:
            return  # nothing in the top BOOK_DEPTH moved
        book.emitted = key
        output.put_nowait(self._message(pair, book, bids, asks))

    def _show_empty(self, symbol: str) -> None:
        pair = self._symbol_to_pair_cache.get(symbol)
        output = self._diff_output
        if output is not None and pair is not None:
            book = self._book(symbol)
            output.put_nowait(self._message(pair, book, [], []))
            book.emitted = ()

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
        self._subscribed.clear()
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        output = self._diff_output
        if was_live and output is not None:
            # Every book the tracker holds would stay frozen from here on: shown empty until the next connection
            # rebuilds them.
            for trading_pair in list(self._trading_pairs):
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                book = self._books.get(symbol) if symbol is not None else None
                output.put_nowait(self._message(trading_pair, book or _LocalBook(), [], []))
        for book in self._books.values():
            book.reset()
        await self._backoff.wait()  # the listen loop reconnects right after this

    async def _parse_order_book_diff_message(self, raw_message: Any, message_queue: asyncio.Queue) -> None:
        """The queued path (frames that came before the listener started)."""
        if isinstance(raw_message, dict):
            self._on_book_frame(raw_message, message_queue)

    @staticmethod
    def _message(trading_pair: str, book: _LocalBook, bids: list, asks: list) -> OrderBookMessage:
        book.emit_id += 1
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": book.emit_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue) -> None:
        # {"channel":"TRADE","symbol":"BTC-USDT","id":12859510195,"price":"74438.96","quantity":"0.00713",
        #  "amount":"530.7497848","side":"buy","ts":1794418471278}  (live 2026-11-11; recent trades replay on subscribe)
        if not isinstance(raw_message, dict):
            return
        trading_pair = self._symbol_to_pair_cache.get(raw_message.get("symbol"))
        if trading_pair is None:
            return
        timestamp_ms = web_utils.to_ms(raw_message.get("ts")) or int(time.time() * 1e3)
        trade_type = TradeType.BUY if str(raw_message.get("side") or "").lower() == "buy" else TradeType.SELL
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": str(raw_message.get("id")),
                "update_id": timestamp_ms,
                "price": raw_message.get("price"),
                "amount": raw_message.get("quantity"),
            },
            timestamp=timestamp_ms * 1e-3,
        ))

    def _channel_originating_message(self, event_message: Any) -> str:
        if not isinstance(event_message, dict):
            return ""
        channel = event_message.get("channel")
        if channel == CONSTANTS.WS_TRADE_CHANNEL:
            return self._trade_messages_queue_key
        if channel in (CONSTANTS.WS_DEPTH_CHANNEL, CONSTANTS.WS_CHECK_CHANNEL):
            return self._diff_messages_queue_key
        return ""
