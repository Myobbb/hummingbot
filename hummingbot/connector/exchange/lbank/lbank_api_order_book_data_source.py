import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.lbank.lbank_exchange import LbankExchange


class LbankMarketRefused(IOError):
    """LBank does not know the market: the socket's "Invalid order pairs:[...]" or REST 10008."""


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] as LBank
    sends them) and where and when it came from."""

    __slots__ = ("bids", "asks", "update_id", "source", "pushed_at", "confirmed_at")

    def __init__(self) -> None:
        self.bids: list = []
        self.asks: list = []
        self.update_id: int = 0          # server ms: the push's TS, or the REST answer's `ts`
        self.source: Optional[str] = None   # "ws" | "rest"; None = nothing current (shown empty)
        self.pushed_at: float = 0.0      # monotonic time of the last valid push
        self.confirmed_at: float = 0.0   # monotonic time the book was last known current (a push, a seed, a probe)

    def clear(self) -> None:
        self.bids = []
        self.asks = []
        self.source = None
        self.pushed_at = 0.0
        self.confirmed_at = 0.0


class _Market:
    """One market's socket: the task that keeps it connected, and the live connection (None while down)."""

    __slots__ = ("task", "ws")

    def __init__(self, task: asyncio.Task) -> None:
        self.task = task
        self.ws: Optional[WSAssistant] = None


def _crossed(bids: list, asks: list) -> bool:
    return bool(bids and asks and float(bids[0][0]) >= float(asks[0][0]))


class LbankAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    LBank public market data: the `depth` channel of wss://api.lbkex.com/ws/V2/, ONE SOCKET PER MARKET.

    LBank's gateway delivers only ~14 KB/s a socket and queues the rest, so markets sharing a socket fall seconds behind
    (P1 2026-10-04: 10 markets a socket put 14% of pushes > 3 s late, p99 55 s; 1 a socket 0.34%). Each tracked market
    therefore has its own socket, kept by its own task (_market_stream): a socket that drops, stalls past
    WS_MESSAGE_TIMEOUT or is repaired affects that market alone, and a runtime add opens one more socket without
    touching the others. A supervisor (listen_for_subscriptions) starts and stops the market tasks as markets are added
    and removed. Measured from myserver (2026-10-07, 10 markets, 5 min): book age p50 29 ms, p99 435 ms, no push past
    3 s.

    Every depth push IS the book (the market's top WS_DEPTH levels, sent on change on a ~600 ms tick and once right
    after the subscribe), so there is no local diff book: each push becomes a SNAPSHOT message handed to the tracker
    inside the WebSocket reader (XT's latency path, runbook §1.4). c_apply_snapshot stamps last_applied_diff, which the
    orchestrator's stale-book check reads.

    P1's rules for this feed (wiki lbank-api, ws-book-checker's LBank notes), kept here:
      - Two publishing nodes per market, two different books: only `t-` nodes' books are used (P1: t- 91% equal to
        REST's top, m- 2%; JEANPHIL's m- book sat 3-5% off for 45 s).
      - The push's own TS is the freshness signal: a push older than STALE_PUSH_SEC carries a stale book (a socket's
        backlog flushing after a stall). It is never shown, and the market's book is shown EMPTY until a fresh push
        or, SEED_DELAY s later, a REST seed. No silence guard: silence can't tell a stall from a quiet market, which
        pushes only on change. What silence can hide (a missed update, a dropped subscription, a long stall) is caught
        by LBank's own book instead: _freshness_loop compares the market silent the longest with REST and reconnects
        its socket when they differ on two reads with no push in between (Hotcoin's probe).
      - A dropped or dead socket shows its market's book empty until the new socket's first book (the stream is the
        book, XT 2026-09-30).
    What stands in for what LBank does not give:
      - The first push after a subscribe can come from an m- node (P1: 56 of 677), and a quiet market then pushes only
        on change: a market with no t- book SEED_DELAY s after its subscribe gets a REST /v2/depth.do book.
      - No trading flag per market: a market missing from /v2/currencyPairs.do (delisted), or every market while
        /v2/supplement/system_status.do says "0" (maintenance), is shown EMPTY; its symbol, trading rule and held book
        stay (the XT rule, 2026-09-26).
      - A crossed book is never shown: the market goes empty until a valid push or seed.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "LbankExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "LbankExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _Book] = {}
        self._markets: Dict[str, _Market] = {}      # exchange symbol -> its socket
        # Read by the orchestrator's per-market stale-book recovery, with _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        # The tracker's diff stream, known once listen_for_order_book_diffs runs; books go straight into it.
        self._diff_output: Optional[asyncio.Queue] = None
        self._wakeup: Optional[asyncio.Event] = None
        # LBank's trading switch: the listed pairs (None until read), maintenance, and the markets shown empty for them.
        self._listed: Optional[Set[str]] = None
        self._maintenance: bool = False
        self._switched_off: Set[str] = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        # Markets the socket named invalid: not reconnected until LBank lists them again.
        self._refused: Set[str] = set()
        self._warned: Set[str] = set()
        # late pushes dropped, books emptied for them, other-node pushes ignored, pushes without a node tag, since
        self._window = [0, 0, 0, 0, time.monotonic()]
        self._ping_seq = 0

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

    async def _tracked_symbols(self) -> Dict[str, str]:
        """symbol -> trading pair for every tracked pair the symbol map knows."""
        tracked: Dict[str, str] = {}
        for trading_pair in list(self._trading_pairs):
            try:
                tracked[await self._symbol_for_pair(trading_pair)] = trading_pair
            except KeyError:
                self._warn_once(f"unmapped:{trading_pair}",
                                f"LBank {trading_pair} has no entry in the connector's symbol map; its market data is "
                                f"not subscribed. The other markets are.")
        return tracked

    def _socket_live(self, symbol: str) -> bool:
        market = self._markets.get(symbol)
        return market is not None and market.ws is not None

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

    def _is_off(self, symbol: str) -> bool:
        return self._maintenance or symbol in self._switched_off

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _Book) -> OrderBookMessage:
        if self._is_off(symbol) or book.source is None:
            return self._message(trading_pair, book.update_id or int(time.time() * 1e3), [], [])
        return self._message(trading_pair, book.update_id, book.bids, book.asks)

    def _emit(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is None or trading_pair is None:
            return
        output.put_nowait(self._snapshot_message(trading_pair, symbol, self._book(symbol)))

    def _show_empty(self, symbol: str) -> None:
        """The tracker's copy of this market's book is no longer kept current: show it empty (a halt) until the next
        valid book is emitted."""
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and trading_pair is not None:
            book = self._books.get(symbol)
            output.put_nowait(self._message(trading_pair, (book.update_id if book else 0) or int(time.time() * 1e3),
                                            [], []))

    @staticmethod
    def _store(book: _Book, bids: list, asks: list, update_id: int, source: str) -> None:
        book.bids = bids[:CONSTANTS.EMIT_DEPTH]
        book.asks = asks[:CONSTANTS.EMIT_DEPTH]
        book.update_id = update_id
        book.source = source
        book.confirmed_at = time.monotonic()

    # ------------------------------------------------------------------ REST

    async def _rest_book(self, symbol: str) -> Tuple[list, list, int]:
        """(bids, asks, server ms) from GET /v2/depth.do (best first)."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH, domain=self._domain),
            params={"symbol": symbol, "size": CONSTANTS.EMIT_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if web_utils.error_code(response) == CONSTANTS.CODE_PAIR_NOT_SUPPORTED:
            raise LbankMarketRefused(f"LBank refused the depth of {symbol}: {response}")
        if not web_utils.is_ok(response):
            raise IOError(f"Error fetching LBank depth for {symbol}: {response}")
        data = response.get("data") or {}
        return (list(data.get("bids") or []), list(data.get("asks") or []),
                int(response.get("ts") or data.get("timestamp") or time.time() * 1e3))

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh. A book the stream (or a seed)
        already serves answers it directly: a REST answer applied over it could roll the tracker back. While the
        market's socket is down, nothing would keep a REST book current, so the answer is an empty book."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            # An empty book and one warning: raising would end the hourly refresh at this pair, every hour.
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"LBank {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, int(time.time() * 1e3), [], [])
        await self._ensure_switch_known()
        book = self._book(symbol)
        if book.source is None and self._socket_live(symbol):
            requested_at = time.monotonic()
            try:
                bids, asks, server_ms = await self._rest_book(symbol)
            except LbankMarketRefused as refused:
                self._warn_once(f"refused:{symbol}", f"LBank {trading_pair}: {refused}. Its book stays empty.")
                return self._message(trading_pair, int(time.time() * 1e3), [], [])
            if book.pushed_at < requested_at and self._socket_live(symbol) and not _crossed(bids, asks):
                self._store(book, bids, asks, server_ms, "rest")
        # On a runtime add the tracker replays the pushes it parked meanwhile as DIFFS over this snapshot, which would
        # merge two whole books: the same book, emitted again shortly, replaces the merge.
        asyncio.get_event_loop().call_later(CONSTANTS.SNAPSHOT_REEMIT_DELAY, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, book)

    def _reemit(self, symbol: str) -> None:
        if self._socket_live(symbol) and self._book(symbol).source is not None:
            self._emit(symbol)

    async def _seed_later(self, symbol: str, ws: WSAssistant) -> None:
        """A market with no t- book SEED_DELAY s after its subscribe gets a REST book (the first push can come from an
        m- node, and a quiet market pushes only on change). A push landing during the REST call wins."""
        await asyncio.sleep(CONSTANTS.SEED_DELAY)
        market = self._markets.get(symbol)
        book = self._book(symbol)
        if market is None or market.ws is not ws or book.source is not None:
            return
        requested_at = time.monotonic()
        try:
            bids, asks, server_ms = await self._rest_book(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"LBank depth seed for {symbol} failed: {e}")
            return
        if market.ws is not ws or book.source is not None or book.pushed_at >= requested_at:
            return
        if _crossed(bids, asks):
            self.logger().debug(f"LBank depth seed for {symbol} came crossed; the book stays empty until a push.")
            return
        self._store(book, bids, asks, server_ms, "rest")
        self._emit(symbol)

    # ------------------------------------------------------------------ LBank's trading switch

    async def _read_switches(self) -> None:
        """The listed pairs (/v2/currencyPairs.do) and the system status (/v2/supplement/system_status.do), applied to
        every tracked market. An empty or refused pair list is not trusted (one bad reply must not empty every book);
        a status other than an explicit "0" or "1" leaves the maintenance flag as it was."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.PAIRS_PATH, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.PAIRS_PATH,
        )
        listed = response.get("data") if web_utils.is_ok(response) else None
        if not isinstance(listed, list) or not listed:
            raise IOError(f"LBank's pair list came back empty or refused: {str(response)[:300]}")
        self._listed = {str(symbol).lower() for symbol in listed}
        status = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.SYSTEM_STATUS_PATH, domain=self._domain),
            method=RESTMethod.POST,
            throttler_limit_id=CONSTANTS.SYSTEM_STATUS_PATH,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        value = str(((status.get("data") or {}) if web_utils.is_ok(status) else {}).get("status"))
        if value in ("0", "1") and (value == "0") != self._maintenance:
            self._maintenance = value == "0"
            if self._maintenance:
                self.logger().warning("LBank reports system maintenance (system_status 0): every LBank book is shown "
                                      "empty until it reports 1.")
            else:
                self.logger().info("LBank's system status is back to normal (1): its books are shown again.")
            for symbol in list(self._symbol_to_pair_cache):
                self._emit(symbol)
        for symbol in (await self._tracked_symbols()):
            self._apply_listing(symbol, symbol in self._listed)

    async def _read_switches_safely(self) -> None:
        try:
            await self._read_switches()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"LBank trading-switch read failed: {e}")

    def _apply_listing(self, symbol: str, listed: bool) -> None:
        if listed and symbol in self._refused:
            self._refused.discard(symbol)
            self._wake()    # its socket is started again
        if (not listed) == (symbol in self._switched_off):
            return
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        if not listed:
            self._switched_off.add(symbol)
            self.logger().warning(f"LBank {trading_pair} is no longer in LBank's pair list ({CONSTANTS.PAIRS_PATH}); "
                                  f"its book is shown empty until LBank lists it again.")
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"LBank {trading_pair} is listed again; its book is shown again.")
        self._emit(symbol)

    async def _ensure_switch_known(self) -> None:
        """Before a market's first snapshot reaches the tracker, so a delisted market never shows a book."""
        if self._listed is not None:
            return
        async with self._switch_lock:
            if self._listed is not None:
                return
            try:
                await self._read_switches()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once("switch-first-read",
                                f"LBank trading-switch read failed ({e}); markets count as listed until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("LBank trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"LBank trading-switch read failed ({e}); every market keeps its last state. "
                                          f"Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ freshness (no sequence id)

    @staticmethod
    def _top(levels: list) -> List[Tuple[Decimal, Decimal]]:
        return [(Decimal(str(price)), Decimal(str(qty))) for price, qty, *_ in levels[:CONSTANTS.FRESHNESS_LEVELS]]

    def _same_as_rest(self, book: _Book, bids: list, asks: list) -> bool:
        return self._top(book.bids) == self._top(bids) and self._top(book.asks) == self._top(asks)

    def _probe_eligible(self, symbol: str) -> bool:
        book = self._books.get(symbol)
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        return (book is not None and book.source is not None and not self._is_off(symbol) and self._socket_live(symbol)
                and trading_pair is not None and trading_pair in self._trading_pairs)

    def _moved(self, symbol: str, ws: Optional[WSAssistant], book: _Book, requested_at: float) -> bool:
        """The stream delivered since the probe asked REST, or the book or its socket changed: the probe judges
        nothing this time."""
        market = self._markets.get(symbol)
        return (ws is None or market is None or market.ws is not ws or book.source is None
                or book.pushed_at >= requested_at)

    async def _freshness_loop(self) -> None:
        """Every FRESHNESS_INTERVAL s one market is checked against REST: the one silent the longest (at least
        FRESHNESS_QUIET_SECONDS since it was last known current). Busy markets push every ~600 ms and are never picked.
          - Same top FRESHNESS_LEVELS a side: confirmed and re-emitted (which also keeps the orchestrator's stale check
            from repairing a quiet but correct book: quiet != dropped).
          - Different: the stream gets FRESHNESS_GRACE to push. No push, and a second REST read still differs (a level a
            bot placed and pulled inside one push tick is no miss): the market's socket missed an update, stalled, or
            lost its subscription. Its socket is reconnected; the new subscribe brings the current book.
        A probe whose market pushes meanwhile judges nothing; 3 failed probes in a row are a WARNING."""
        errors = 0
        while True:
            await asyncio.sleep(CONSTANTS.FRESHNESS_INTERVAL)
            symbol: Optional[str] = None
            try:
                oldest = time.monotonic() - CONSTANTS.FRESHNESS_QUIET_SECONDS
                quiet = [(book.confirmed_at, sym) for sym, book in list(self._books.items())
                         if book.confirmed_at <= oldest and self._probe_eligible(sym)]
                if not quiet:
                    continue
                _, symbol = min(quiet)
                book = self._books[symbol]
                ws = self._markets[symbol].ws
                requested_at = time.monotonic()
                bids, asks, _ = await self._rest_book(symbol)
                if errors >= 3:
                    self.logger().info("LBank freshness probes work again.")
                errors = 0
                if self._moved(symbol, ws, book, requested_at):
                    continue
                if self._same_as_rest(book, bids, asks):
                    book.confirmed_at = time.monotonic()
                    self._emit(symbol)
                    continue
                await asyncio.sleep(CONSTANTS.FRESHNESS_GRACE)
                if self._moved(symbol, ws, book, requested_at):
                    continue
                bids, asks, _ = await self._rest_book(symbol)
                if self._moved(symbol, ws, book, requested_at):
                    continue
                if self._same_as_rest(book, bids, asks):
                    book.confirmed_at = time.monotonic()   # the difference didn't last: nothing was missed
                    self._emit(symbol)
                    continue
                if _crossed(bids, asks):
                    continue  # REST's own book is unusable: judged on a later probe
                silent = time.monotonic() - (book.pushed_at or book.confirmed_at or requested_at)
                self.logger().warning(
                    f"LBank {self._symbol_to_pair_cache.get(symbol, symbol)}: the book differs from REST on two reads "
                    f"with no push for {silent:.0f} s (a missed update, a stalled socket or a dropped subscription); "
                    f"reconnecting its socket.")
                await ws.disconnect()   # its task shows the book empty and reconnects
            except asyncio.CancelledError:
                raise
            except LbankMarketRefused as refused:
                self._warn_once(f"refused:{symbol}", f"LBank {self._symbol_to_pair_cache.get(symbol, symbol)}: "
                                                     f"{refused}; reading LBank's pair list.")
                await self._read_switches_safely()
            except Exception as e:
                errors += 1
                if errors == 3:
                    self.logger().warning(f"LBank freshness probes failing ({errors} in a row, last: {e}); quiet books "
                                          f"are unchecked until they work again.")
                else:
                    self.logger().debug(f"LBank freshness probe skipped: {e}")

    def _reseed(self, symbol: str) -> None:
        """A late or crossed push emptied this market's book: REST seeds it SEED_DELAY s later unless a fresh push
        comes first (a quiet market would otherwise stay empty until it changes)."""
        market = self._markets.get(symbol)
        if market is not None and market.ws is not None:
            web_utils.spawn(self, self._seed_later, symbol, market.ws)

    # ------------------------------------------------------------------ the market sockets

    def _wake(self) -> None:
        if self._wakeup is not None:
            self._wakeup.set()

    def _ensure_market(self, symbol: str) -> None:
        """Start this market's socket task unless it runs (or the socket named the market invalid)."""
        market = self._markets.get(symbol)
        if symbol in self._refused or (market is not None and not market.task.done()):
            return
        self._markets[symbol] = _Market(safe_ensure_future(self._market_stream(symbol)))

    async def listen_for_subscriptions(self):
        """The supervisor: one socket task per tracked market. Markets added at runtime are started at once by
        _subscribe_single_trading_pair; this loop also starts any market whose task ended, and stops the tasks of
        markets no longer tracked. Sockets are opened WS_CONNECT_SPACING apart (LBank throttles connection bursts)."""
        self._wakeup = asyncio.Event()
        try:
            while True:
                tracked = await self._tracked_symbols()
                for symbol in [s for s in self._markets if s not in tracked]:
                    self._markets.pop(symbol).task.cancel()
                    self._book(symbol).clear()
                for symbol in tracked:
                    market = self._markets.get(symbol)
                    if symbol not in self._refused and (market is None or market.task.done()):
                        self._ensure_market(symbol)
                        await asyncio.sleep(CONSTANTS.WS_CONNECT_SPACING)
                self._wakeup.clear()
                try:
                    await asyncio.wait_for(self._wakeup.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
        finally:
            tasks = [market.task for market in self._markets.values()]
            for task in tasks:
                task.cancel()
            self._markets.clear()
            # Detached at once: a data source started right after this one opens a new session, never this one.
            session = web_utils.detach_market_session()
            try:
                # Each cancelled task closes its socket first; then the session they shared goes too.
                await asyncio.wait(tasks, timeout=5.0) if tasks else None
                if session is not None and not session.closed:
                    await session.close()
            except Exception as e:
                self.logger().debug(f"LBank: closing the market sockets' session failed: {e!r}")

    def _depth_request(self, action: str, symbol: str) -> Dict[str, Any]:
        return {"action": action, "subscribe": "depth", "depth": str(CONSTANTS.WS_DEPTH), "pair": symbol}

    async def _market_stream(self, symbol: str) -> None:
        """Keeps one market's socket: connect, subscribe its depth, read; on any end the book is shown empty and the
        socket reconnects after a backoff (a connection that lived WS_RETRY_RESET_SEC reconnects at once)."""
        backoff = web_utils.LbankReconnectBackoff()
        failures = 0
        while True:
            ws: Optional[WSAssistant] = None
            helpers: List[asyncio.Task] = []
            try:
                ws = await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL, market_socket=True)
                market = self._markets.get(symbol)
                if market is not None:
                    market.ws = ws
                self._book(symbol).clear()
                await ws.send(WSJSONRequest(payload=self._depth_request("subscribe", symbol)))
                backoff.connected()
                if failures:
                    self.logger().info(f"LBank {self._symbol_to_pair_cache.get(symbol, symbol)}: socket connected "
                                       f"again after {failures} failed attempt(s).")
                failures = 0
                helpers = [web_utils.spawn(self, self._seed_later, symbol, ws), web_utils.spawn(self, self._ping_loop, ws)]
                await self._read(symbol, ws)
            except asyncio.CancelledError:
                raise
            except LbankMarketRefused as refused:
                self._refused.add(symbol)
                self._warn_once(f"refused:{symbol}", f"LBank {self._symbol_to_pair_cache.get(symbol, symbol)}: "
                                                     f"{refused}. Its book stays empty; its socket is reopened only "
                                                     f"if LBank lists it again.")
            except asyncio.TimeoutError:
                failures += 1
                self._log_failure(symbol, failures, f"no message for {CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s "
                                                    f"(pings every {CONSTANTS.WS_PING_INTERVAL:.0f} s)")
            except Exception as e:
                failures += 1
                self._log_failure(symbol, failures, f"{type(e).__name__}: {e}")
            finally:
                for helper in helpers:
                    helper.cancel()
                market = self._markets.get(symbol)
                if market is not None and market.ws is ws:
                    market.ws = None
                book = self._book(symbol)
                if book.source is not None:
                    self._show_empty(symbol)
                book.clear()
                if ws is not None:
                    try:
                        await ws.disconnect()
                    except Exception as e:
                        self.logger().debug(f"LBank {symbol}: closing the socket failed: {e!r}")
            if symbol in self._refused:
                return
            await backoff.wait()

    def _log_failure(self, symbol: str, failures: int, reason: str) -> None:
        if failures == 1 or failures % 10 == 0:
            self.logger().warning(f"LBank {self._symbol_to_pair_cache.get(symbol, symbol)}: socket lost ({reason}; "
                                  f"{failures} in a row); its book is shown empty until it reconnects.")

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """The client's keepalive. A send that fails means the socket is going; its reader ends the connection."""
        while True:
            await asyncio.sleep(CONSTANTS.WS_PING_INTERVAL)
            self._ping_seq += 1
            try:
                await ws.send(WSJSONRequest(payload={"action": "ping", "ping": f"hb{self._ping_seq}"}))
            except asyncio.CancelledError:
                raise
            except Exception:
                return

    async def _read(self, symbol: str, ws: WSAssistant) -> None:
        async for response in ws.iter_messages():
            message = response.data
            if not isinstance(message, dict):
                continue
            if message.get("type") == "depth":
                self._on_depth(symbol, message)
                continue
            action = message.get("action")
            if action == "ping":
                await ws.send(WSJSONRequest(payload={"action": "pong", "pong": message.get("ping")}))
                continue
            if action == "pong":
                continue
            if message.get("status") == "error":
                # The socket carries one subscription, so any error is about it: a pair LBank doesn't list is not
                # asked for again; any other error reconnects (a subscription that failed would otherwise leave a
                # seeded book on display while pongs keep the socket alive).
                text = str(message.get("message") or "")
                if "Invalid order pairs" in text:
                    raise LbankMarketRefused(f"the socket refused it ({text[:120]})")
                raise ConnectionError(f"LBank answered the subscription with an error: {text[:200]}")

    def _on_depth(self, symbol: str, message: Dict[str, Any]) -> None:
        """One depth push: the market's whole top-WS_DEPTH book. Synchronous, inside the WebSocket reader."""
        now = time.monotonic()
        window = self._window
        node = message.get("s")
        if not str(node or "").startswith(CONSTANTS.BOOK_NODE_PREFIX):
            # Another node's book (m-NN): different and often stale. Never shown. A push with no node tag at all
            # would mean LBank changed the feed: said once, and counted.
            window[2 if node else 3] += 1
            if not node:
                self._warn_once("no-node-tag", "LBank depth pushes carry no publishing-node tag (`s`); books are shown "
                                               f"only from {CONSTANTS.BOOK_NODE_PREFIX} nodes, so these are not used.")
            self._log_window(now)
            return
        book = self._book(symbol)
        ts = web_utils.ts_ms(message.get("TS"))
        if ts is not None and time.time() * 1e3 - ts > CONSTANTS.STALE_PUSH_SEC * 1e3:
            # A queued push: its book is already stale, and the one held is older still. The market shows empty
            # until a fresh push.
            window[0] += 1
            if book.source is not None:
                window[1] += 1
                self._show_empty(symbol)
                book.clear()
                self._reseed(symbol)
            self._log_window(now)
            return
        if ts is not None and book.source is not None and ts < book.update_id:
            return  # older than the book held (a REST seed answered after this push was sent)
        depth = message.get("depth") or {}
        bids = list(depth.get("bids") or [])
        asks = list(depth.get("asks") or [])
        if _crossed(bids, asks):
            self._warn_once(f"crossed:{symbol}", f"LBank {symbol}: a depth push came crossed (bid {bids[0][0]} >= "
                                                 f"ask {asks[0][0]}); the book is shown empty until a valid one.")
            if book.source is not None:
                self._show_empty(symbol)
                book.clear()
                self._reseed(symbol)
            return
        book.pushed_at = now
        self._store(book, bids, asks, int(ts if ts is not None else time.time() * 1e3), "ws")
        self._emit(symbol)

    def _log_window(self, now: float) -> None:
        """Once a minute: late pushes dropped (INFO when a book was emptied for one, else DEBUG) and other-node pushes
        ignored."""
        window = self._window
        if now - window[4] < CONSTANTS.STALE_LOG_EVERY_SEC:
            return
        log = self.logger().info if window[1] else self.logger().debug
        log(f"LBank: in {now - window[4]:.0f} s {window[0]} depth push(es) more than {CONSTANTS.STALE_PUSH_SEC:.0f} s "
            f"old by LBank's clock dropped ({window[1]} book(s) shown empty until their next fresh push); {window[2]} "
            f"push(es) from nodes other than {CONSTANTS.BOOK_NODE_PREFIX}NN ignored, {window[3]} without a node tag.")
        self._window = [0, 0, 0, 0, now]

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): the new market gets its own socket at once. No other socket is
        touched (the base default would disconnect the shared one)."""
        symbol = await self._symbol_for_pair(trading_pair)
        self._refused.discard(symbol)
        self._ensure_market(symbol)
        self._wake()
        self.logger().info(f"LBank {trading_pair}: subscribing on its own socket (no other market touched).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: this market's socket is reconnected, and the
        server answers the new subscribe with the current book. Every other socket and book is untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._refused.discard(symbol)
        market = self._markets.get(symbol)
        if market is not None and market.ws is not None:
            await market.ws.disconnect()   # its task shows the book empty and reconnects
        else:
            self._ensure_market(symbol)
            self._wake()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = safe_ensure_future(self._trading_switch_loop())
        freshness_task = safe_ensure_future(self._freshness_loop())
        try:
            # Books go straight into `output` from the market readers; the diff queue stays empty.
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()
            freshness_task.cancel()

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Unused: depth pushes are applied in the market readers (_on_depth), never queued.
        return

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Unused: public trades are not subscribed (see WS_DEPTH in lbank_constants).
        return

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""
