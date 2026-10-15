import asyncio
import json
import re
import time
from bisect import bisect_left, insort
from collections import deque
from typing import TYPE_CHECKING, Any, Deque, Dict, Iterable, List, Optional, Set, Tuple

from hummingbot.connector.exchange.coinw import coinw_constants as CONSTANTS, coinw_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinw.coinw_exchange import CoinwExchange

_I32 = 1 << 32
# Read from the RAW frame before any JSON parsing: most frames move nothing and are skipped unparsed. Frames are
# {"biz":"exchange","pairCode":"78","data":"<the payload, a JSON string>","type":"depth",…}, so the payload's keys
# appear escaped (\"startSeq\":…).
_RE_PAIRCODE = re.compile(r'"pairCode"\s*:\s*"?(\d+)')
_RE_TYPE = re.compile(r'"type"\s*:\s*"([A-Za-z_]+)"')
_RE_ACTION = re.compile(r'"action"\s*:\s*"([A-Za-z_]+)"')
_RE_START = re.compile(r'startSeq\\?"\s*:\s*(-?\d+)')
_RE_END = re.compile(r'endSeq\\?"\s*:\s*(-?\d+)')
_RE_SEQ = re.compile(r'\\?"seq\\?"\s*:\s*(-?\d+)')
_RE_LEVEL_SEP = re.compile(r'\]\s*,\s*\[')     # between two levels: counts a frame's levels without parsing it


def seq_distance(a: int, b: int) -> int:
    """a - b as a signed 32-bit distance: depth_snapshot carries only the low 32 bits of CoinW's sequence, the delta
    channels and depth_sync's snapshot the full value. Valid while the two are within 2^31 changes of each other."""
    d = (a - b) % _I32
    return d - _I32 if d >= (1 << 31) else d


def _levels(raw: Any) -> List[Tuple[float, float]]:
    """[[price, qty], ...] (snapshots) as floats."""
    return [(float(level[0]), float(level[1])) for level in raw or [] if len(level) >= 2]


class _LocalBook:
    """
    One market's book, built from two of CoinW's book channels (depth_sync + depth_snapshot) on their one sequence
    (see the data source; `depth` is not a book source).
    Each side keeps a dict (price -> quantity) plus its prices in ascending order, so the top N is a slice (XT's lesson:
    rebuilding the top N per push was the largest cost per push). Kept to BOOK_KEEP_LEVELS a side.
    """

    __slots__ = ("bids", "asks", "bid_prices", "ask_prices", "seq", "synced", "snap_seq", "snap_levels",
                 "since_snap", "pending", "tops", "checked_seq", "last_frame", "emitted", "emit_id")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_prices: List[float] = []
        self.ask_prices: List[float] = []
        self.seq: Optional[int] = None
        self.synced = False
        self.snap_seq: Optional[int] = None    # the seq of the snapshot the book was last loaded from
        self.snap_levels = 0                   # levels per side that snapshot carried
        self.since_snap: List[Tuple[list, int]] = []   # delta entries applied since that snapshot (the twin replay)
        self.pending: Dict[int, Tuple[int, list, float]] = {}   # startSeq -> (endSeq, entries, arrival)
        self.tops: Deque[Tuple[int, tuple]] = deque(maxlen=200)  # (seq, top BOOK_DEPTH) for the cross-check
        self.checked_seq: Optional[int] = None  # the last seq a same-seq snapshot was compared at
        self.last_frame = 0.0                  # monotonic time of this market's last book frame of any channel
        self.emitted: Optional[tuple] = None    # the top last handed to the tracker
        self.emit_id = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.seq = None
        self.synced = False
        self.snap_seq = None
        self.snap_levels = 0
        self.since_snap = []
        self.pending.clear()
        self.tops.clear()
        self.checked_seq = None
        self.emitted = None

    def load(self, bids: Iterable, asks: Iterable, seq: int, levels: int) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self.bid_prices = sorted(self.bids)
        self.ask_prices = sorted(self.asks)
        self._trim()
        self.seq = seq
        self.synced = True
        self.snap_seq = seq
        self.snap_levels = levels
        self.since_snap = []
        self.tops.append((seq, self.top_key()))

    def apply(self, entries: List[Tuple[int, bool, float, float]], end: int, replay: bool = False) -> None:
        """Delta entries (seq, is_bid, price, absolute quantity; 0 deletes), in seq order, those past the book's seq."""
        for entry_seq, is_bid, price, qty in entries:
            if seq_distance(entry_seq, self.seq) <= 0:
                continue
            levels, prices = (self.bids, self.bid_prices) if is_bid else (self.asks, self.ask_prices)
            if qty > 0:
                if price not in levels:
                    insort(prices, price)
                levels[price] = qty
            elif price in levels:
                del levels[price]
                del prices[bisect_left(prices, price)]
        self._trim()
        self.seq = end
        if not replay:
            self.since_snap.append((entries, end))
            if len(self.since_snap) > 500:
                del self.since_snap[:250]
        self.tops.append((end, self.top_key()))

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

    def top_key(self) -> tuple:
        bids, asks = self.top(CONSTANTS.BOOK_DEPTH)
        return tuple(bids), tuple(asks)


class CoinwAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    CoinW public market data (wss://ws.futurescw.com, one market per subscription by its pairCode): the book from
    depth_sync + depth_snapshot, `fills` for trades.

    Non-negotiable 1 (the freshest CORRECT local book) decides the design, measured on myserver 2026-10-15 (11 markets x
    300 s per run, hmb_local_tools/coinw/debug_coinw_book_freshness.py, debug_coinw_merged_book.py,
    debug_coinw_channel_content.py). CoinW publishes one book sequence on three channels, each conflated server-side:
      - depth_sync (the website's own feed, undocumented): a snapshot (<= 100 levels, the full 64-bit seq), then
        increments carrying EVERY level update with its own seq; gap-free; a book from it alone equalled CoinW's own
        snapshot at the same seq on every comparison (4,000+ and 582 of 582);
      - depth_snapshot (documented): every ~300 ms, changed or not, a 20-level frame and its <= 100-level twin, labelled
        with the low 32 bits of the same seq;
      - depth (documented deltas): NOT used. Its batches carry the net change of their range and leave out changes to
        levels deep in the book: a book that took them showed stale levels rising into the top 20 (6 of 6,610 same-seq
        checks), and taken mid-batch, phantom levels (47 of 8,029) -- the one-legged-arb failure.
    Measured with this algorithm: the book trailed the earliest of all three channels by 0 / 134 / 235 ms (p50 / p90 /
    p99) over 22,513 changes, against 10 / 200 / 355 for depth_sync alone and 141 / 275 / 346 for depth_snapshot alone
    (P1's channel); 0 of 5,683 same-seq checks off; 52 us of CPU per frame at p50.
    The book is kept HERE: a depth_sync increment is applied from any point inside it (its levels with their own seq
    past the book's, in seq order); a snapshot newer than the book replaces it, and its 100-level twin (~1 ms later)
    reloads the book at the twin's seq and replays the increments applied since; a snapshot at the book's own seq is a
    cross-check (a different top BOOK_DEPTH rebuilds the book from it). A frame that ends at or before the book is stale
    and is skipped from its raw text, unparsed.

    Every change of the top BOOK_DEPTH hands the tracker a SNAPSHOT, never a DIFF (XT's lesson: the tracker replays
    windowed diffs over a snapshot without checking update ids), applied in the WebSocket reader (XT measured the
    hand-off to the diff-listener task at ~3 ms p99 under HMB's load). No REST book is ever applied: CoinW's REST book
    has no sequence and no timestamp.

    Liveness: depth_snapshot arrives every ~300 ms for every market, changed or not, so a market with no frame for
    MARKET_SILENCE_S has a dead stream (P1: XDC's depth died alone): it is shown EMPTY and re-subscribed; a socket with
    nothing for SOCKET_SILENCE_S is reconnected. While no connection is live, every tracked book is shown EMPTY.

    A market CoinW has off (returnSymbol state 2, or returnTicker isFrozen 1) is shown EMPTY while its symbol and
    trading rule stay (non-negotiable 3).
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinwExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "CoinwExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._code_to_symbol: Dict[str, str] = {}
        self._symbol_to_code: Dict[str, str] = {}
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
        self._last_resubscribe: Dict[str, float] = {}
        self._stream_live = False
        self._backoff = web_utils.CoinwReconnectBackoff()
        # counters, logged with each re-subscribe / cross-check warning
        self._xcheck_rebuilds = 0
        self._crossed_rebuilds = 0

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

    async def _code_for_symbol(self, symbol: str) -> Optional[str]:
        """The socket's pairCode (returnTicker `id`) for a market, re-read once when it isn't known yet."""
        code = self._symbol_to_code.get(symbol)
        if code is None:
            codes = await self._connector.pair_codes(refresh_for=symbol)
            for sym, c in codes.items():
                self._symbol_to_code[sym] = c
                self._code_to_symbol[c] = sym
            code = self._symbol_to_code.get(symbol)
        return code

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
                continue
        return symbols

    # ------------------------------------------------------------------ the tracker's snapshot requests

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book and its hourly refresh: the live local book, or EMPTY until the stream's first
        snapshot arrives (pushed to the tracker the moment it does). Never REST: CoinW's REST book carries no sequence
        and no timestamp, so it could roll the book back behind the stream."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"CoinW {trading_pair} has no entry in the connector's symbol map; "
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

    # ------------------------------------------------------------------ CoinW's market switches

    async def _read_switches(self) -> None:
        off = await self._connector.markets_switched_off()
        self._switch_known = True
        changed_off = off - self._switched_off
        changed_on = self._switched_off - off
        self._switched_off = set(off)
        output = self._diff_output
        for symbol in changed_off:
            if symbol in self._symbol_to_pair_cache:
                self.logger().warning(f"CoinW {self._symbol_to_pair_cache[symbol]}: trading is off (returnSymbol state /"
                                      f" returnTicker isFrozen); its book is shown empty until it is back on.")
                if output is not None:
                    book = self._book(symbol)
                    output.put_nowait(self._message(self._symbol_to_pair_cache[symbol], book, [], []))
                    book.emitted = None
        for symbol in changed_on:
            if symbol in self._symbol_to_pair_cache:
                self.logger().info(f"CoinW {self._symbol_to_pair_cache[symbol]}: trading is on again; its book is shown.")
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
            self._warn_once("switch-first-read", f"CoinW trading-switch read failed ({type(e).__name__}: {e}); markets "
                                                 f"count as on until the next read (every "
                                                 f"{CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("CoinW trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"CoinW trading-switch read failed ({type(e).__name__}: {e}); markets keep "
                                          f"their last state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL, raw=True)

    async def _send_market(self, ws: WSAssistant, symbol: str, event: str) -> bool:
        """sub / unsub of every channel of one market: depth_sync first (its snapshot carries the full sequence),
        then depth_snapshot and the public trades."""
        code = await self._code_for_symbol(symbol)
        if code is None:
            self._warn_once(f"no-code:{symbol}", f"CoinW {self._symbol_to_pair_cache.get(symbol, symbol)}: no pairCode "
                                                 f"in returnTicker; its market data is not subscribed.")
            return False
        self._code_to_symbol[code] = symbol
        for channel in CONSTANTS.WS_BOOK_CHANNELS + (CONSTANTS.WS_TRADES_CHANNEL,):
            await ws.send(WSJSONRequest(payload={"event": event, "params": {"biz": "exchange", "type": channel,
                                                                            "pairCode": code}}))
        return True

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every CoinW book empty (XT, 2026-09-23).
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"CoinW {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
                    continue
                self._book(symbol).reset()
                symbols.append(symbol)
            self._subscribed = set()
            self._stream_live = True
            now = time.monotonic()
            self._last_pong = now
            self._last_socket_frame = now
            self._backoff.connected()
            for symbol in symbols:
                if await self._send_market(ws, symbol, "sub"):
                    self._subscribed.add(symbol)
                    self._book(symbol).last_frame = now
            self.logger().info(f"Subscribed to CoinW depth_sync, depth_snapshot and fills for "
                               f"{len(self._subscribed)} markets.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinW public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which blanks every CoinW book for a reconnect."""
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
        book.last_frame = time.monotonic()
        if await self._send_market(ws, symbol, "sub"):
            self._subscribed.add(symbol)
            self.logger().info(f"Subscribed CoinW {trading_pair} on the live connection (no reconnect).")

    async def _resubscribe(self, symbol: str, reason: str) -> None:
        """Rebuild one market's book from fresh frames: unsub then sub its channels on the live socket; the socket and
        every other book are untouched. At most once per RESUBSCRIBE_MIN_INTERVAL per market; it shows empty
        meanwhile."""
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
        self.logger().warning(f"CoinW {self._symbol_to_pair_cache.get(symbol, symbol)}: {reason}; re-subscribing its "
                              f"market data.")
        try:
            await self._send_market(ws, symbol, "unsub")
            await self._send_market(ws, symbol, "sub")
            book.last_frame = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinW re-subscribe of {symbol} failed ({type(e).__name__}); reconnecting.")
            await ws.disconnect()

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        await self._resubscribe(symbol, "the orchestrator asked for a fresh book")

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """{"event":"ping"} every WS_HEARTBEAT_INTERVAL; no pong within WS_PONG_TIMEOUT ends the connection."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                sent = time.monotonic()
                await ws.send(WSJSONRequest(payload={"event": "ping"}))
                await asyncio.sleep(CONSTANTS.WS_PONG_TIMEOUT)
                if self._last_pong < sent:
                    self.logger().warning(f"CoinW public WS: no pong within {CONSTANTS.WS_PONG_TIMEOUT} s; "
                                          f"reconnecting.")
                    await ws.disconnect()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinW public WS keepalive stopped: {type(e).__name__}")

    async def _watchdog_loop(self) -> None:
        """Every second: a market whose stream went silent is shown empty and re-subscribed; a socket that went silent
        while markets are subscribed is reconnected; a buffered gap frame that waited BOOK_GAP_WAIT_S (a newer snapshot
        normally carries the book past it within ~300 ms) is dropped and its market re-subscribed."""
        while True:
            await asyncio.sleep(1.0)
            try:
                ws = self._active_ws
                if ws is None or not self._stream_live:
                    continue
                now = time.monotonic()
                if self._subscribed and now - self._last_socket_frame > CONSTANTS.SOCKET_SILENCE_S:
                    self.logger().warning(f"CoinW public WS: no frame for {now - self._last_socket_frame:.1f} s with "
                                          f"{len(self._subscribed)} markets subscribed; reconnecting.")
                    await ws.disconnect()
                    continue
                for symbol in list(self._subscribed):
                    book = self._books.get(symbol)
                    if book is None:
                        continue
                    expired = [s for s, (_, _, t) in book.pending.items() if now - t > CONSTANTS.BOOK_GAP_WAIT_S]
                    for start in expired:
                        del book.pending[start]
                    if expired:
                        # A gap no newer snapshot healed: this market's depth_snapshot stream may be dead (P1: XDC's died
                        # alone), and its frames keep last_frame fresh while the book stands still. Rebuilt from fresh
                        # frames (shown empty meanwhile).
                        asyncio.ensure_future(self._resubscribe(
                            symbol, f"a sequence gap no snapshot healed within {CONSTANTS.BOOK_GAP_WAIT_S:.0f} s"))
                    elif now - book.last_frame > CONSTANTS.MARKET_SILENCE_S:
                        book.last_frame = now
                        asyncio.ensure_future(self._resubscribe(
                            symbol, f"no book frame for {CONSTANTS.MARKET_SILENCE_S:.0f} s (its stream died)"))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"CoinW book watchdog: {type(e).__name__}: {e}")

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
            raw = ws_response.data
            if not isinstance(raw, str):
                continue
            self._last_socket_frame = time.monotonic()
            if '"pong"' in raw[:40]:
                self._last_pong = self._last_socket_frame
                continue
            kind = self._frame_type(raw)
            if kind == CONSTANTS.WS_TRADES_CHANNEL:
                trade_queue.put_nowait(raw)
            elif kind in CONSTANTS.WS_BOOK_CHANNELS:
                output = self._diff_output
                if output is not None and diff_queue.empty():
                    # Applied here, now. The queue is used only while it holds something (a frame that came before
                    # the listener started), so the arrival order is kept.
                    self._on_book_frame(raw, kind, output)
                else:
                    diff_queue.put_nowait(raw)
            else:
                self._on_control_frame(raw)

    @staticmethod
    def _frame_type(raw: str) -> Optional[str]:
        if '"channel"' in raw[:200] or '"event"' in raw[:60]:
            return None  # an ack, an error or a ping/pong
        m = _RE_TYPE.search(raw)
        return m.group(1) if m else None

    def _on_control_frame(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(data, dict):
            return
        if data.get("event") == "ping":
            ws = self._active_ws
            if ws is not None:
                asyncio.ensure_future(ws.send(WSJSONRequest(payload={"event": "pong"})))
            return
        result = data.get("data") if isinstance(data.get("data"), dict) else {}
        if data.get("channel") == "subscribe" and result.get("result") is False:
            symbol = self._code_to_symbol.get(str(data.get("pairCode")), data.get("pairCode"))
            self._warn_once(f"refused:{data.get('type')}:{symbol}",
                            f"CoinW refused the {data.get('type')} subscription of {symbol}: errorCode "
                            f"{result.get('errorCode')} ({result.get('errorMsg')}).")

    # ------------------------------------------------------------------ the local book

    def _on_book_frame(self, raw: str, kind: str, output: asyncio.Queue) -> None:
        m = _RE_PAIRCODE.search(raw)
        symbol = self._code_to_symbol.get(m.group(1)) if m else None
        if symbol is None or symbol not in self._symbol_to_pair_cache:
            return
        book = self._book(symbol)
        book.last_frame = time.monotonic()
        try:
            if kind == CONSTANTS.WS_SNAPSHOT_CHANNEL:
                self._on_snapshot(symbol, book, raw, output, full_seq=False)
            elif kind == CONSTANTS.WS_SYNC_CHANNEL and (_RE_ACTION.search(raw) or [None, None])[1] == "snapshot":
                self._on_snapshot(symbol, book, raw, output, full_seq=True)
            else:
                self._on_delta(symbol, book, raw, output)
        except (KeyError, TypeError, ValueError, AttributeError, IndexError) as e:
            self._warn_once(f"bad-frame:{kind}", f"CoinW {kind} frame not understood ({type(e).__name__}: {e}); "
                                                 f"skipped: {raw[:200]}")

    @staticmethod
    def _payload(raw: str) -> Dict[str, Any]:
        data = json.loads(raw).get("data")
        return json.loads(data) if isinstance(data, str) else data

    def _on_delta(self, symbol: str, book: _LocalBook, raw: str, output: asyncio.Queue) -> None:
        """A depth_sync increment [startSeq, endSeq]: every level update of the range, each with its own seq. Applied
        from any point inside it (its entries past the book's seq); one that starts past the book waits for a newer
        snapshot (depth_sync had no gap in 4 runs; a gap is still handled)."""
        start_m, end_m = _RE_START.search(raw), _RE_END.search(raw)
        if start_m is None or end_m is None:
            raise ValueError("no startSeq / endSeq")
        start, end = int(start_m.group(1)), int(end_m.group(1))
        if not book.synced or seq_distance(end, book.seq) <= 0:
            return  # before the first snapshot, or stale: skipped unparsed
        entries = self._entries(self._payload(raw))
        if seq_distance(start, book.seq + 1) > 0:
            book.pending[start] = (end, entries, time.monotonic())
            return
        book.apply(entries, end)
        self._drain(book)
        self._emit(symbol, book, output)

    @staticmethod
    def _entries(payload: Dict[str, Any]) -> List[Tuple[int, bool, float, float]]:
        entries = [(int(lvl[2]), True, float(lvl[0]), float(lvl[1])) for lvl in payload.get("bids") or []]
        entries += [(int(lvl[2]), False, float(lvl[0]), float(lvl[1])) for lvl in payload.get("asks") or []]
        entries.sort(key=lambda e: e[0])
        return entries

    @staticmethod
    def _drain(book: _LocalBook) -> None:
        """Buffered frames that now continue the book are applied; those the book has passed are dropped."""
        moved = True
        while moved and book.pending:
            moved = False
            for start in list(book.pending):
                end, entries, _ = book.pending[start]
                if seq_distance(end, book.seq) <= 0:
                    del book.pending[start]
                elif seq_distance(start, book.seq + 1) <= 0:
                    del book.pending[start]
                    book.apply(entries, end)
                    moved = True

    def _on_snapshot(self, symbol: str, book: _LocalBook, raw: str, output: asyncio.Queue, full_seq: bool) -> None:
        m = _RE_SEQ.search(raw)
        if m is None:
            raise ValueError("no seq")
        seq = int(m.group(1))
        if book.synced:
            seq = book.seq + seq_distance(seq, book.seq)   # widened to the book's full sequence
        if not book.synced or seq_distance(seq, book.seq) > 0:
            payload = self._payload(raw)
            bids, asks = _levels(payload.get("bids")), _levels(payload.get("asks"))
            book.load(bids, asks, seq, max(len(bids), len(asks)))
            self._drain(book)
            self._emit(symbol, book, output)
            return
        distance = seq_distance(seq, book.seq)
        if full_seq or distance > 0:
            deep = False
        else:
            # depth_snapshot's <= 100-level twin has more than BOOK_DEPTH levels a side; its 20-level frame has 2 x 19
            # level separators at most
            deep = len(_RE_LEVEL_SEP.findall(raw)) > 2 * CONSTANTS.BOOK_DEPTH
        if (not full_seq and deep and book.snap_seq is not None and seq_distance(seq, book.snap_seq) == 0
                and 0 < book.snap_levels <= CONSTANTS.BOOK_DEPTH):
            # The twin of the 20-level frame the book was loaded from: reload at its seq with the full depth and
            # replay the deltas applied since (identical top 20 on 46,778 of 46,778 twins, P1 2026-09-30).
            payload = self._payload(raw)
            replay = list(book.since_snap)
            bids, asks = _levels(payload.get("bids")), _levels(payload.get("asks"))
            book.load(bids, asks, seq, max(len(bids), len(asks), CONSTANTS.BOOK_DEPTH + 1))
            for entries, end in replay:
                book.apply(entries, end, replay=True)
            book.since_snap = replay
            self._emit(symbol, book, output)
            return
        if distance == 0 and book.checked_seq != book.seq and not deep:
            # A snapshot at the book's own seq: the cross-check (once per seq; a quiet book repeats it every 300 ms).
            book.checked_seq = book.seq
            payload = self._payload(raw)
            bids, asks = _levels(payload.get("bids")), _levels(payload.get("asks"))
            theirs = (tuple(sorted(bids, key=lambda lv: -lv[0])[:CONSTANTS.BOOK_DEPTH]),
                      tuple(sorted(asks, key=lambda lv: lv[0])[:CONSTANTS.BOOK_DEPTH]))
            mine = book.top_key()
            if theirs != mine and not self._shallow_match(mine, theirs, book):
                self._xcheck_rebuilds += 1
                self.logger().warning(f"CoinW {self._symbol_to_pair_cache.get(symbol, symbol)}: the local book differs "
                                      f"from CoinW's snapshot at the same seq {book.seq}; rebuilt from the snapshot "
                                      f"(rebuilds so far: {self._xcheck_rebuilds}).")
                book.load(bids, asks, book.seq, max(len(bids), len(asks)))
                self._emit(symbol, book, output)

    @staticmethod
    def _shallow_match(mine: tuple, theirs: tuple, book: _LocalBook) -> bool:
        """A book loaded from a 20-level frame only knows 20 levels: after a level left the top it shows fewer. Equal on
        every level it has is a match, not a corruption."""
        if book.snap_levels > CONSTANTS.BOOK_DEPTH or book.snap_levels == 0:
            return False
        return all(side_mine == side_theirs[:len(side_mine)] for side_mine, side_theirs in zip(mine, theirs))

    def _emit(self, symbol: str, book: _LocalBook, output: asyncio.Queue, force: bool = False) -> None:
        pair = self._symbol_to_pair_cache[symbol]
        if symbol in self._switched_off:
            if book.emitted != ():
                output.put_nowait(self._message(pair, book, [], []))
                book.emitted = ()
            return
        bids, asks = book.top(CONSTANTS.BOOK_DEPTH)
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen on an unbroken sequence: corruption. Shown empty; the next snapshot (~300 ms) reloads it.
            self._crossed_rebuilds += 1
            self.logger().warning(f"CoinW {pair}: crossed local book (bid {bids[0][0]} >= ask {asks[0][0]}) at seq "
                                  f"{book.seq}; shown empty until the next snapshot (crossed so far: "
                                  f"{self._crossed_rebuilds}).")
            book.reset()
            book.last_frame = time.monotonic()
            output.put_nowait(self._message(pair, book, [], []))
            book.emitted = ()
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
        if not isinstance(raw_message, str):
            return
        kind = self._frame_type(raw_message)
        if kind in CONSTANTS.WS_BOOK_CHANNELS:
            self._on_book_frame(raw_message, kind, message_queue)

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
        # {"biz":"exchange","pairCode":"78","data":"[{\"price\":…,\"seq\":…,\"side\":\"BUY\",\"size\":…,
        #  \"symbol\":\"78\",\"time\":\"<ms>\"}]","type":"fills"}
        if not isinstance(raw_message, str):
            return
        try:
            data = json.loads(raw_message)
            items = json.loads(data["data"]) if isinstance(data.get("data"), str) else data.get("data")
        except (TypeError, ValueError, KeyError):
            return
        symbol = self._code_to_symbol.get(str(data.get("pairCode")))
        trading_pair = self._symbol_to_pair_cache.get(symbol) if symbol else None
        if trading_pair is None:
            return
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            try:
                timestamp_ms = int(item.get("time") or time.time() * 1e3)
            except (TypeError, ValueError):
                timestamp_ms = int(time.time() * 1e3)
            trade_type = TradeType.BUY if str(item.get("side") or "").upper() == "BUY" else TradeType.SELL
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": str(item.get("seq")),
                    "update_id": timestamp_ms,
                    "price": item.get("price"),
                    "amount": item.get("size"),
                },
                timestamp=timestamp_ms * 1e-3,
            ))

    def _channel_originating_message(self, event_message: Any) -> str:
        if not isinstance(event_message, str):
            return ""
        kind = self._frame_type(event_message)
        if kind == CONSTANTS.WS_TRADES_CHANNEL:
            return self._trade_messages_queue_key
        if kind in CONSTANTS.WS_BOOK_CHANNELS:
            return self._diff_messages_queue_key
        return ""
