import asyncio
import time
from collections import deque
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp import WSServerHandshakeError

from hummingbot.connector.exchange.bexc import bexc_constants as CONSTANTS, bexc_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bexc.bexc_exchange import BexcExchange

# Trade ids remembered per market, so a trade pushed again (after a re-subscribe) is not emitted twice.
TRADE_ID_MEMORY = 256


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] strings as
    bexc sends them) and the state of its subscription."""

    __slots__ = ("bids", "asks", "update_id", "venue_uid", "served", "pushed_at", "subscribed_at", "awaiting",
                 "unanswered", "socket", "trade_at", "frozen_uid")

    def __init__(self) -> None:
        self.bids: List[List[str]] = []
        self.asks: List[List[str]] = []
        self.update_id: int = 0                 # the tracker's id: local ms of the push, strictly increasing per market
        self.venue_uid: Optional[int] = None    # bexc's update_id of the last frame (gaps and restarts are counted)
        self.served: bool = False               # the held book came from the current subscription and is shown
        self.pushed_at: float = 0.0             # monotonic time of the last valid push
        self.subscribed_at: float = 0.0         # monotonic time of the last subscribe sent
        self.awaiting: bool = False             # a subscribe went out and no book has answered it yet
        self.unanswered: int = 0                # subscribes in a row that no book answered
        self.socket: Optional[WSAssistant] = None   # the socket the current subscription was sent on
        self.trade_at: float = 0.0              # the first trade since the stream last moved (0: none waiting)
        self.frozen_uid: Optional[int] = None   # bexc's stream for this market is frozen at this update_id

    def clear(self) -> None:
        """Nothing current: the subscription is gone or being rebuilt. update_id stays (it only ever rises), and so
        does frozen_uid: the freeze is bexc's, not the socket's."""
        self.bids = []
        self.asks = []
        self.venue_uid = None
        self.served = False
        self.pushed_at = 0.0
        self.awaiting = False
        self.unanswered = 0
        self.socket = None
        self.trade_at = 0.0


class _Connection:
    """One engine socket and the markets it carries: at most WS_MAX_MARKETS_PER_SOCKET, and a market stays on its socket
    for the life of the process (pairs are only ever added at runtime)."""

    __slots__ = ("number", "symbols", "ws", "frame_at", "sent_at", "send_lock", "backoff", "task", "refused",
                 "closing")

    def __init__(self, number: int) -> None:
        self.number = number
        self.symbols: List[str] = []
        self.ws: Optional[WSAssistant] = None   # the open socket, from its connect to its loss
        self.frame_at: float = 0.0              # monotonic time of the last frame of any kind
        self.sent_at: float = 0.0               # monotonic time of the last subscribe / unsubscribe sent
        self.send_lock = asyncio.Lock()
        self.backoff = web_utils.BexcReconnectBackoff()
        self.task: Optional[asyncio.Task] = None
        self.refused: int = 0                   # connect attempts failed in a row
        self.closing: bool = False              # closed by the liveness check: its end is not a loss


def _unusable(bids: list, asks: list) -> Optional[Tuple[str, str]]:
    """Why a book push can't be shown, or None: (the kind, what is wrong with it).
      - empty: no bid and no ask. Not taken as the market's book, so a subscribe it answers stays unanswered and is
        asked again (2026-11-14: after bexc's 14:52 mass drop, ALPINE's re-subscribe counted as answered, yet the
        strategy found the book empty 21 and 29 s later; bexc sends empty snapshots, as TEXT frames: P1). A market
        that really is empty is shown empty all the same;
      - crossed: the best bid at or above the best ask;
      - frozen: one price listed twice within the levels shown or the next one (it may be the other half of the last
        level shown). Only the snapshots of frozen streams did this (2026-11-13: PEPE_USDT 14 of 15, LTC_USDT 1 of 15;
        0 in >100 snapshots of live streams and in every push): individual orders at one price as adjacent levels,
        order_count 1 each, while live pushes list each price once. The tracker's book would keep one of them."""
    if not bids and not asks:
        return "empty", "came empty (no bid, no ask)"
    if bids and asks and Decimal(bids[0]["price"]) >= Decimal(asks[0]["price"]):
        return "crossed", f"came crossed (bid {bids[0]['price']} >= ask {asks[0]['price']})"
    for side, levels in (("bids", bids), ("asks", asks)):
        window = levels[:CONSTANTS.EMIT_DEPTH + 1]
        if len({level["price"] for level in window}) < len(window):
            prices = [level["price"] for level in window]
            repeated = next(price for i, price in enumerate(prices) if price in prices[:i])
            return "frozen", f"a book listed the price {repeated} twice on the {side}"
    return None


class BexcAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    bexc public market data from the engine's socket: one channel per market, `book_update`, every push the market's
    FULL book (<= 100 levels a side, best first), plus `trade_update`; both come with one subscribe message per market.

    Every push IS the book, so there is no local diff book: each push becomes a SNAPSHOT message (the top EMIT_DEPTH
    levels), handed to the tracker inside the WebSocket reader (no hand-off to the diff listener task; XT's latency
    path, runbook §1.4). Only snapshots ever reach the tracker, and c_apply_snapshot stamps last_applied_diff, which the
    orchestrator's stale-book check reads. A missed push (an update_id gap) costs nothing: the next one carries the
    whole book; gaps are only counted. Measured from myserver 2026-11-13 (the numbers in bexc_constants;
    hmb_local_tools/bexc/debug_bexc_ws_cadence.py): 67/67 REST books equal the push with the same update_id; 0 gaps
    in steady state.

    bexc caps a socket at 30 subscriptions, so markets are spread over up to WS_MAX_SOCKETS sockets of
    WS_MAX_MARKETS_PER_SOCKET each (MEXC's lesson: one shared socket can't take every market).
    listen_for_subscriptions supervises one task per socket; each task connects, subscribes its markets one message at
    a time (WS_SUBSCRIBE_SPACING apart, with the reader already running, so each snapshot is applied the moment it
    lands), reads until the socket ends, shows that socket's books EMPTY (the stream is the book: XT, 2026-09-30),
    waits its own backoff and reconnects. A drop of one socket leaves the others' books untouched. A runtime add
    subscribes on the market's socket (a new socket when every one is full); `_refresh_snapshot_for_pair`
    re-subscribes one market for the orchestrator.

    What bexc does NOT give, and what stands in for it:
      - No server time on a book and no periodic re-push: a quiet market and a dead stream look alike. Asking tells
        them apart: a market silent for MARKET_SILENCE_S is re-subscribed (its book stays shown), and the snapshot that
        answers in ~0.3 s confirms it. A subscribe left unanswered for SUBSCRIBE_ANSWER_S shows the book empty and is
        repeated with a doubling wait; a socket with markets and no frame at all for SOCKET_SILENCE_S is reconnected.
      - A crossed push: the held book is superseded and the new one unusable, so the market is shown empty until a
        valid push. So is an empty push (no bid, no ask): bexc sends empty snapshots, and one answering a subscribe
        leaves the subscribe unanswered, asked again like any other (_unusable).
      - A frozen stream: bexc can stop pushing a market's book for minutes while it trades, its update_id standing
        still (PEPE_USDT and LTC_USDT, 2026-11-13: 150 s, 86 and 76 trades, fresh snapshots showing another book under
        the same update_id). Seen as a trade with no push of a new update_id within FROZEN_AFTER_TRADE_S, a subscribe
        answer under the held update_id with another book, or a frame listing one price twice; the market is then
        shown empty until a push with another update_id (_freeze).
      - The trading switch: /markets `status` and `is_active`. While a tracked market is not trading its book is shown
        EMPTY (a halt, the XT rule 2026-09-26); the symbol, the trading rule and the held book stay. A market the list
        doesn't carry at all is never subscribed: bexc answers each subscribe "Market not found" (FARTCOIN-USDT,
        2026-11-14: one refusal every 5 min), against decision 4 (listed symbols only). A read that lists it
        subscribes it.
    REST /orderbook is ~17 s stale, so it is never used: a market the stream doesn't serve is shown empty.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "BexcExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "BexcExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _Book] = {}
        # Read by the orchestrator's per-market stale-book recovery, with _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        # The tracker's diff stream, known once listen_for_order_book_diffs runs; books go straight into it.
        self._diff_output: Optional[asyncio.Queue] = None
        self._connections: List[_Connection] = []
        self._connection_of: Dict[str, _Connection] = {}
        self._wakeup = asyncio.Event()   # the supervisor: a socket was added
        self._recent_trades: Dict[str, Deque[str]] = {}
        # bexc's trading switch: markets switched off (shown empty), and markets whose switch has been read.
        self._switched_off: Set[str] = set()
        self._switch_known: Set[str] = set()
        self._unlisted: Set[str] = set()        # missing from the market list: never subscribed until it is listed
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        self._warned: Set[str] = set()
        self._stats: Dict[str, int] = dict.fromkeys(
            ("uid_gaps", "uid_restarts", "silence_resubscribes", "unanswered_subscribes", "crossed", "empty", "freezes",
             "undecodable"), 0)
        self._stats_at = time.monotonic()

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

    def _book(self, symbol: str) -> _Book:
        book = self._books.get(symbol)
        if book is None:
            book = self._books[symbol] = _Book()
        return book

    def _pair_name(self, symbol: str) -> str:
        return self._symbol_to_pair_cache.get(symbol, symbol)

    async def _tracked_symbols(self) -> List[str]:
        symbols = []
        for trading_pair in list(self._trading_pairs):
            try:
                symbols.append(await self._symbol_for_pair(trading_pair))
            except KeyError:
                # One unknown pair must not stop the others: subscribing it would fail the same way every time.
                self._warn_once(f"unmapped:{trading_pair}",
                                f"bexc {trading_pair} has no entry in the connector's symbol map; its market data is "
                                f"not subscribed. The other markets are.")
        return symbols

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ------------------------------------------------------------------ messages to the tracker

    @staticmethod
    def _message(trading_pair: str, update_id: int, bids: list, asks: list) -> OrderBookMessage:
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _Book) -> OrderBookMessage:
        if symbol in self._switched_off or not book.served:
            return self._message(trading_pair, book.update_id or int(time.time() * 1e3), [], [])
        return self._message(trading_pair, book.update_id, book.bids, book.asks)

    def _emit(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is None or trading_pair is None:
            return
        output.put_nowait(self._snapshot_message(trading_pair, symbol, self._book(symbol)))

    def _show_empty(self, symbol: str) -> None:
        """The tracker's copy of this market is no longer kept current: show it empty (a halt) until a book is served
        again. The id repeats the last one emitted (the tracker drops only LOWER ids); the next book's is higher."""
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and trading_pair is not None:
            book = self._books.get(symbol)
            output.put_nowait(self._message(trading_pair, (book.update_id if book else 0) or int(time.time() * 1e3),
                                            [], []))

    @staticmethod
    def _store(book: _Book, bids: list, asks: list) -> None:
        book.bids = [[level["price"], level["quantity"]] for level in bids[:CONSTANTS.EMIT_DEPTH]]
        book.asks = [[level["price"], level["quantity"]] for level in asks[:CONSTANTS.EMIT_DEPTH]]
        # Strictly increasing per market: the tracker's router drops a message whose id is below the book's.
        book.update_id = max(int(time.time() * 1e3), book.update_id + 1)
        book.served = True

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh, all from the stream: a market
        subscribed on a connected socket gets up to SNAPSHOT_REQUEST_WAIT_S for its snapshot (it follows the subscribe
        in ~0.3 s); otherwise the book starts empty and the stream fills it. Never REST (~17 s stale)."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            # An empty book and one warning: raising would end the hourly refresh at this pair, every hour.
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"bexc {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, int(time.time() * 1e3), [], [])
        await self._ensure_switch_known(symbol)
        book = self._book(symbol)
        connection = self._assign(symbol)
        if not book.served and symbol not in self._unlisted and connection is not None and connection.ws is not None:
            deadline = time.monotonic() + CONSTANTS.SNAPSHOT_REQUEST_WAIT_S
            while not book.served and connection.ws is not None and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        # On a runtime add the tracker replays the pushes it parked meanwhile as DIFFS over this snapshot, which would
        # merge two whole books: the same book, emitted again shortly, replaces the merge.
        asyncio.get_running_loop().call_later(CONSTANTS.SNAPSHOT_REEMIT_DELAY, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, book)

    def _reemit(self, symbol: str) -> None:
        if self._book(symbol).served:
            self._emit(symbol)

    # ------------------------------------------------------------------ bexc's trading switch

    async def _read_switches(self) -> None:
        """Read `status` / `is_active` for every tracked market from the shared market list and apply them. A market
        missing from a non-empty list is no longer listed and counts as off; an empty list is not trusted (one bad
        reply must not empty every tracked book)."""
        listed = await self._connector.market_list_by_symbol()
        if not listed:
            raise IOError("bexc's market list came back empty")
        for symbol in await self._tracked_symbols():
            self._apply_switch(symbol, listed.get(symbol))

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        trading_pair = self._pair_name(symbol)
        if info is None:
            self._unlisted.add(symbol)
        elif symbol in self._unlisted:
            self._unlisted.discard(symbol)
            self._subscribe_listed(symbol)
        if info is not None and (info.get("force_post_only_sell") is True or info.get("protected_pair") is True):
            self._warn_once(f"flags:{symbol}",
                            f"bexc {trading_pair}: force_post_only_sell={info.get('force_post_only_sell')}, "
                            f"protected_pair={info.get('protected_pair')}: bexc may rest or refuse a taker sell here. "
                            f"The book is shown as usual (the venue decides; refusals are logged in full).")
        off = info is None or not (info.get("status") == "trading" and info.get("is_active") is True)
        if off == (symbol in self._switched_off):
            return
        flags = "not listed" if info is None else f"status={info.get('status')}, is_active={info.get('is_active')}"
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"bexc {trading_pair}: trading is switched off by bexc ({flags}); its book is shown "
                                  f"empty until bexc switches it back on.")
            self._emit(symbol)
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"bexc {trading_pair}: trading is back on ({flags}); its book is shown again.")
            if self._book(symbol).served:
                self._emit(symbol)

    def _subscribe_listed(self, symbol: str) -> None:
        """bexc lists a market it didn't: subscribed now if its socket is open (else that socket's next connect does
        it, with the others)."""
        connection = self._connection_of.get(symbol)
        if connection is not None and connection.ws is not None:
            safe_ensure_future(self._subscribe(connection, connection.ws, symbol))

    async def _ensure_switch_known(self, symbol: str) -> None:
        """Before a market's first snapshot reaches the tracker, so a market that is already off never shows a book.
        One read covers every tracked market."""
        if symbol in self._switch_known:
            return
        async with self._switch_lock:
            if symbol in self._switch_known:
                return
            try:
                await self._read_switches()
                if symbol not in self._switch_known:
                    self._switch_known.add(symbol)  # not tracked (yet): read with the others next time
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once("switch-first-read",
                                f"bexc trading-switch read failed ({e}); markets count as on until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL:.0f} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("bexc trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"bexc trading-switch read failed ({e}); every market keeps its last state. "
                                          f"Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL:.0f} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ sockets

    def _assign(self, symbol: str) -> Optional[_Connection]:
        """The socket that carries this market: its own, else the first with room, else a new one (the supervisor
        starts it). None when every socket is full: the market can't be subscribed and its book stays empty."""
        connection = self._connection_of.get(symbol)
        if connection is not None:
            return connection
        connection = next((c for c in self._connections if len(c.symbols) < CONSTANTS.WS_MAX_MARKETS_PER_SOCKET), None)
        if connection is None:
            if len(self._connections) >= CONSTANTS.WS_MAX_SOCKETS:
                key = f"capacity:{symbol}"
                if key not in self._warned:
                    self._warned.add(key)
                    self.logger().error(
                        f"bexc {self._pair_name(symbol)} can't be subscribed: {CONSTANTS.WS_MAX_SOCKETS} sockets x "
                        f"{CONSTANTS.WS_MAX_MARKETS_PER_SOCKET} markets are all taken (bexc allows 30 markets a "
                        f"socket and 10 sockets an IP). Its book stays empty.")
                return None
            connection = _Connection(len(self._connections) + 1)
            self._connections.append(connection)
            self._wakeup.set()
        connection.symbols.append(symbol)
        self._connection_of[symbol] = connection
        return connection

    def _refresh_active_ws(self) -> None:
        # The base's has_active_websocket and the orchestrator read _active_ws: the first open socket.
        self._active_ws = next((c.ws for c in self._connections if c.ws is not None), None)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _send(self, connection: _Connection, ws: WSAssistant, action: str, symbol: str) -> bool:
        """One subscribe / unsubscribe, at least WS_SUBSCRIBE_SPACING after the socket's previous one. False when the
        socket is gone or closing (its replacement subscribes every market of the connection)."""
        async with connection.send_lock:
            pause = connection.sent_at + CONSTANTS.WS_SUBSCRIBE_SPACING - time.monotonic()
            if pause > 0:
                await asyncio.sleep(pause)
            if connection.ws is not ws:
                return False
            try:
                await ws.send(WSJSONRequest(payload={"action": action, "symbol": symbol}))
            except (ConnectionError, RuntimeError, aiohttp.ClientError) as e:
                self.logger().debug(f"bexc public socket #{connection.number}: {action} {symbol} not sent ({e!r}); "
                                    f"the socket is closing.")
                return False
            connection.sent_at = time.monotonic()
            return True

    async def _subscribe(self, connection: _Connection, ws: WSAssistant, symbol: str, again: bool = False) -> bool:
        """Subscribe one market on this socket (again: unsubscribe it first). The answer is its snapshot."""
        book = self._book(symbol)
        book.awaiting = True
        book.socket = ws
        book.subscribed_at = time.monotonic()
        if again and not await self._send(connection, ws, CONSTANTS.WS_ACTION_UNSUBSCRIBE, symbol):
            return False
        if not await self._send(connection, ws, CONSTANTS.WS_ACTION_SUBSCRIBE, symbol):
            return False
        book.subscribed_at = time.monotonic()
        return True

    async def _subscribe_connection(self, connection: _Connection, ws: WSAssistant, symbols: List[str]) -> None:
        """The burst after a connect: every market the socket carried at that moment, one message each, paced; the
        reader runs meanwhile. A market added during the burst subscribes itself (it sees the socket open)."""
        sent = 0
        try:
            for symbol in symbols:
                if symbol in self._unlisted:
                    continue                # not on bexc's market list: the subscribe would be refused
                if not await self._subscribe(connection, ws, symbol):
                    return
                sent += 1
            self.logger().info(f"Subscribed bexc book_update for {sent} markets on public socket #{connection.number}.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error subscribing bexc public socket #{connection.number}; "
                                    f"{sent}/{len(symbols)} markets subscribed.")

    def _connect_failed(self, connection: _Connection, reason: str) -> None:
        """A connect attempt failed: one WARNING per streak of failures (the backoff paces the retries); the line that
        reports the reconnect closes the streak."""
        connection.refused += 1
        if connection.refused == 1:
            self.logger().warning(f"bexc public socket #{connection.number} can't connect ({reason}); retrying with a "
                                  f"backoff up to {CONSTANTS.WS_RETRY_CAP:.0f} s.")

    async def _run_connection(self, connection: _Connection) -> None:
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
            except asyncio.CancelledError:
                raise
            except WSServerHandshakeError as e:
                note = ": the IP's socket budget, or the gateway after a mass drop" if e.status == 429 else ""
                self._connect_failed(connection, f"handshake refused, HTTP {e.status}{note}")
                await connection.backoff.wait()
                continue
            except (ConnectionError, aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                self._connect_failed(connection, f"{type(e).__name__}: {e}")
                await connection.backoff.wait()
                continue
            if connection.refused:
                self.logger().info(f"bexc public socket #{connection.number} connected after {connection.refused} "
                                   f"failed attempt(s).")
                connection.refused = 0
            subscriber: Optional[asyncio.Task] = None
            try:
                connection.backoff.connected()
                connection.frame_at = time.monotonic()
                connection.sent_at = 0.0
                connection.closing = False
                # No await from here to the burst's list: a market added meanwhile is either in the list or sees the
                # socket open and subscribes itself (_subscribe_single_trading_pair).
                connection.ws = ws
                self._refresh_active_ws()
                symbols = list(connection.symbols)
                for symbol in symbols:
                    self._book(symbol).clear()   # nothing is current until the stream serves it
                subscriber = asyncio.ensure_future(self._subscribe_connection(connection, ws, symbols))
                await self._process_websocket_messages(ws, connection)
                # Ends with the socket. Our own close raises like a loss ("closed unexpectedly": the fork's
                # WSConnection marks itself closed only after the close), so `closing` tells the two apart.
            except asyncio.CancelledError:
                raise
            except (ConnectionError, aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                if connection.closing:
                    # The liveness check closed it and has said why: one WARNING per event.
                    self.logger().debug(f"bexc public socket #{connection.number} closed ({e!r}); reconnecting.")
                else:
                    self.logger().warning(f"bexc public socket #{connection.number} lost ({type(e).__name__}: {e}); "
                                          f"reconnecting.")
            except Exception:
                self.logger().exception(f"Unexpected error on bexc public socket #{connection.number}; reconnecting.")
            finally:
                if subscriber is not None and not subscriber.done():
                    subscriber.cancel()
                connection.ws = None
                self._refresh_active_ws()
                for symbol in connection.symbols:
                    book = self._books.get(symbol)
                    if book is not None:
                        # The tracker's copy would stay frozen from here on: shown empty, a halt, until the next
                        # socket serves it.
                        self._show_empty(symbol)
                        book.clear()
                await ws.disconnect()
            await connection.backoff.wait()

    async def listen_for_subscriptions(self):
        """Supervises one task per socket (the class docstring); returns only when cancelled."""
        try:
            while True:
                self._wakeup.clear()
                try:
                    for symbol in await self._tracked_symbols():
                        self._assign(symbol)
                    for connection in self._connections:
                        if connection.symbols and (connection.task is None or connection.task.done()):
                            if connection.task is not None and not connection.task.cancelled() \
                                    and connection.task.exception() is not None:
                                self.logger().error(f"bexc public socket #{connection.number}'s task ended: "
                                                    f"{connection.task.exception()!r}; restarting it.")
                            connection.task = asyncio.ensure_future(self._run_connection(connection))
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._warn_once(f"supervise:{type(e).__name__}",
                                    f"bexc public stream: supervising the sockets failed ({e!r}); retrying every "
                                    f"{CONSTANTS.LIVENESS_INTERVAL:.0f} s.")
                try:
                    await asyncio.wait_for(self._wakeup.wait(), timeout=CONSTANTS.LIVENESS_INTERVAL)
                except asyncio.TimeoutError:
                    pass
        finally:
            for connection in self._connections:
                if connection.task is not None:
                    connection.task.cancel()

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on its own socket, a new one when every
        socket is full. The base default disconnects the shared socket, which would blank every bexc book."""
        symbol = await self._symbol_for_pair(trading_pair)
        await self._ensure_switch_known(symbol)
        connection = self._assign(symbol)
        if connection is None or symbol in self._unlisted:
            return               # an unlisted market: subscribed once a read lists it
        live = connection.ws
        book = self._book(symbol)
        if live is None:
            self._wakeup.set()   # no socket yet: its connect burst subscribes this market
            return
        if book.socket is live:
            return               # already subscribed on this socket (its connect burst took it)
        book.clear()
        if await self._subscribe(connection, live, symbol):
            self.logger().info(f"Subscribed bexc {trading_pair} on public socket #{connection.number} (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: re-subscribe this one market (the answer is a
        fresh snapshot). Its socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        connection = self._assign(symbol)
        self._show_empty(symbol)
        self._book(symbol).clear()
        if connection is not None and connection.ws is not None and symbol not in self._unlisted:
            await self._subscribe(connection, connection.ws, symbol, again=True)

    # ------------------------------------------------------------------ liveness (no server time, no re-push)

    async def _liveness_loop(self) -> None:
        while True:
            await asyncio.sleep(CONSTANTS.LIVENESS_INTERVAL)
            try:
                await self._liveness_check()
                self._log_stats()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"bexc liveness check skipped: {e!r}")

    async def _liveness_check(self) -> None:
        """One round (every LIVENESS_INTERVAL s), per socket: no frame at all for SOCKET_SILENCE_S reconnects it; per
        market: an unanswered subscribe shows the book empty and subscribes again (the wait doubling, capped); a market
        silent for MARKET_SILENCE_S is re-subscribed, its book still shown."""
        now = time.monotonic()
        for connection in list(self._connections):
            ws = connection.ws
            if ws is None:
                continue
            if connection.symbols and now - connection.frame_at > CONSTANTS.SOCKET_SILENCE_S:
                self.logger().warning(f"bexc public socket #{connection.number}: no frame for "
                                      f"{now - connection.frame_at:.0f} s with {len(connection.symbols)} markets "
                                      f"subscribed; reconnecting.")
                connection.closing = True
                await ws.disconnect()
                continue
            for symbol in list(connection.symbols):
                book = self._books.get(symbol)
                if book is None or connection.ws is not ws or book.socket is not ws or symbol in self._unlisted:
                    continue
                if book.awaiting:
                    wait = min(CONSTANTS.SUBSCRIBE_ANSWER_S * 2 ** min(book.unanswered, 8),
                               CONSTANTS.RESUBSCRIBE_RETRY_CAP_S)
                    if now - book.subscribed_at < wait:
                        continue
                    book.unanswered += 1
                    self._stats["unanswered_subscribes"] += 1
                    if book.served:
                        self._show_empty(symbol)
                        book.bids, book.asks, book.served = [], [], False
                    if book.unanswered == 1 and book.frozen_uid is None:   # a freeze has said why already
                        self.logger().warning(f"bexc {self._pair_name(symbol)}: no valid book {wait:.0f} s after its "
                                              f"subscribe on public socket #{connection.number}; shown empty and "
                                              f"subscribed again (the wait doubles while it stays unanswered).")
                    await self._subscribe(connection, ws, symbol, again=True)
                elif now - max(book.pushed_at, book.subscribed_at) >= CONSTANTS.MARKET_SILENCE_S:
                    self._stats["silence_resubscribes"] += 1
                    await self._subscribe(connection, ws, symbol, again=True)

    def _log_stats(self) -> None:
        now = time.monotonic()
        if now - self._stats_at < CONSTANTS.STATS_INTERVAL:
            return
        self._stats_at = now
        moved = {key: value for key, value in self._stats.items() if value}
        if not moved:
            return
        for key in self._stats:
            self._stats[key] = 0
        line = ", ".join(f"{key} {value}" for key, value in moved.items())
        if CONSTANTS.LIVE_AUDIT_LOGGING:
            self.logger().info(f"[BX-AUDIT] book-stats last {CONSTANTS.STATS_INTERVAL:.0f} s: {line}")
        else:
            self.logger().debug(f"bexc book stats, last {CONSTANTS.STATS_INTERVAL:.0f} s: {line}")

    # ------------------------------------------------------------------ the reader

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        tasks = [safe_ensure_future(self._trading_switch_loop()), safe_ensure_future(self._liveness_loop())]
        try:
            # Books go straight into `output` from the WebSocket reader; the diff queue stays empty.
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            for task in tasks:
                task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, connection: _Connection) -> None:
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        async for ws_response in websocket_assistant.iter_messages():
            now = time.monotonic()
            connection.frame_at = now
            message = ws_response.data
            if message is None:
                continue          # a skipped frame (candle_update)
            if isinstance(message, (bytes, bytearray)):
                try:
                    message = web_utils.decode_book_frame(message)
                except Exception as e:
                    self._stats["undecodable"] += 1
                    self._warn_once(f"undecodable:{type(e).__name__}",
                                    f"bexc public socket #{connection.number}: a frame that doesn't decode ({e!r}); "
                                    f"skipped and counted.")
                    continue
            if not isinstance(message, dict):
                continue
            kind = message.get("type")
            if kind == "book_update":
                self._on_book(connection, message, now)
            elif kind == "trade_update":
                self._on_trade(message.get("symbol"), now)
                trade_queue.put_nowait(message)
            elif kind == "error":
                text = str(message.get("message") or message.get("error") or message)[:200]
                self._warn_once(f"ws-error:{text}", f"bexc public socket #{connection.number}: server error: {text}")

    def _on_book(self, connection: _Connection, message: Dict[str, Any], now: float) -> None:
        """One book_update: the market's whole book. Synchronous, inside the WebSocket reader."""
        symbol = message.get("symbol")
        book = self._books.get(symbol)
        if book is None or self._connection_of.get(symbol) is not connection:
            return  # not a market this socket carries (an unsubscribe in flight)
        uid = message.get("update_id")
        if not isinstance(uid, int) or isinstance(uid, bool):
            uid = None
        if book.frozen_uid is not None:
            if uid is None or uid == book.frozen_uid:
                return  # the frozen book again (a subscribe's answer), not the stream
            book.frozen_uid = None
            self.logger().info(f"bexc {self._pair_name(symbol)}: its book stream moves again (update_id {uid}).")
        last = book.venue_uid
        if uid is not None and uid != last:
            if last is not None:
                if uid > last + 1:
                    self._stats["uid_gaps"] += uid - last - 1
                elif uid < last:
                    self._stats["uid_restarts"] += 1     # the market's stream restarted (it counts from 0 again)
            book.venue_uid = uid
            book.trade_at = 0.0                          # the stream moved: no trade is waiting for a push
        bids = message.get("bids") or []
        asks = message.get("asks") or []
        try:
            fault = _unusable(bids, asks)
            if fault is None and uid is not None and uid == last and book.served \
                    and not self._same_book(book, bids, asks):
                # A subscribe's answer carries the market's current update_id: under the held one, with another book,
                # the book has changed and no push said so.
                fault = ("frozen", "a fresh snapshot under the held update_id shows another book")
            if fault is None:
                self._store(book, bids, asks)
        except (KeyError, TypeError, ArithmeticError) as e:
            self._stats["undecodable"] += 1
            self._warn_once(f"bad-book:{symbol}", f"bexc {self._pair_name(symbol)}: a book push with unreadable levels "
                                                  f"({e!r}); skipped and counted.")
            return
        if fault is not None:
            kind, what = fault
            if kind == "frozen":
                self._freeze(symbol, book, what)
                return
            # The held book is superseded and the new one unusable: shown empty (a halt) until a valid push. Not stamped
            # as served, so the liveness check keeps asking for a valid one.
            self._stats[kind] += 1
            self._warn_once(f"{kind}:{symbol}", f"bexc {self._pair_name(symbol)}: a book push {what}; the book is "
                                                f"shown empty until a valid one.")
            if book.served:
                self._show_empty(symbol)
                book.bids, book.asks, book.served = [], [], False
            return
        book.pushed_at = now
        book.awaiting = False
        book.unanswered = 0
        self._emit(symbol)

    @staticmethod
    def _same_book(book: _Book, bids: list, asks: list) -> bool:
        depth = CONSTANTS.EMIT_DEPTH
        return (book.bids == [[level["price"], level["quantity"]] for level in bids[:depth]]
                and book.asks == [[level["price"], level["quantity"]] for level in asks[:depth]])

    # ------------------------------------------------------------------ frozen streams

    def _on_trade(self, symbol: Any, now: float) -> None:
        """A trade changes the book, and a live stream pushes it with a new update_id: within 1.25 s of every trade
        on 9 live markets (745 trades, 2026-11-13). The first trade since the stream last moved starts the clock."""
        book = self._books.get(symbol)
        if book is None or book.trade_at or book.venue_uid is None or book.frozen_uid is not None:
            return
        book.trade_at = now
        asyncio.get_running_loop().call_later(CONSTANTS.FROZEN_AFTER_TRADE_S, self._check_frozen, symbol, now)

    def _check_frozen(self, symbol: str, started: float) -> None:
        book = self._books.get(symbol)
        if book is not None and book.trade_at == started and book.frozen_uid is None:
            self._freeze(symbol, book, f"a trade and no book push in {CONSTANTS.FROZEN_AFTER_TRADE_S:.0f} s")

    def _freeze(self, symbol: str, book: _Book, why: str) -> None:
        """bexc stopped pushing this market's book (the class docstring): the held book is stale. Shown empty until a
        push with another update_id; frames under the frozen one are not shown. One re-subscribe now (a fresh
        subscription answers with a newer update_id when only ours had stalled), then the liveness check's doubling
        wait."""
        book.frozen_uid = book.venue_uid
        book.trade_at = 0.0
        self._stats["freezes"] += 1
        self.logger().warning(f"bexc {self._pair_name(symbol)}: its book stream is frozen at update_id "
                              f"{book.frozen_uid} ({why}); the book is shown empty until bexc pushes a new one.")
        if book.served:
            self._show_empty(symbol)
            book.bids, book.asks, book.served = [], [], False
        connection = self._connection_of.get(symbol)
        if connection is not None and connection.ws is not None:
            safe_ensure_future(self._subscribe(connection, connection.ws, symbol, again=True))

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""   # unused: the reader routes every frame itself

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Unused: book pushes are applied in the WebSocket reader (_on_book), never queued.
        return

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """trade_update: {symbol, trade_id (a UUID), price, quantity, taker_side, timestamp (ISO, ns, UTC)}. A trade id
        seen lately for the market is not emitted again."""
        symbol = raw_message.get("symbol")
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        trade_id = raw_message.get("trade_id")
        if trading_pair is None or trading_pair not in self._trading_pairs or not trade_id:
            return
        recent = self._recent_trades.get(symbol)
        if recent is None:
            recent = self._recent_trades[symbol] = deque(maxlen=TRADE_ID_MEMORY)
        if trade_id in recent:
            return
        recent.append(trade_id)
        timestamp_ms = web_utils.iso_to_ms(raw_message.get("timestamp")) or int(time.time() * 1e3)
        trade_type = TradeType.BUY if str(raw_message.get("taker_side")).lower() == "buy" else TradeType.SELL
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": trade_id,
                "update_id": timestamp_ms,
                "price": raw_message.get("price"),
                "amount": raw_message.get("quantity"),
            },
            timestamp=timestamp_ms * 1e-3,
        ))
