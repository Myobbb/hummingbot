import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.exchange.hotcoin import hotcoin_constants as CONSTANTS, hotcoin_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hotcoin.hotcoin_exchange import HotcoinExchange


class HotcoinMarketRefused(IOError):
    """REST /v1/depth answered 40008: Hotcoin does not list the market, or has disabled it."""


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] strings
    as Hotcoin sends them) and where and when it came from."""

    __slots__ = ("bids", "asks", "update_id", "source", "pushed_at", "confirmed_at", "misses")

    def __init__(self) -> None:
        self.bids: List[List[str]] = []
        self.asks: List[List[str]] = []
        self.update_id: int = 0          # server ms: the push's `ts`, or the REST envelope's `time`
        self.source: Optional[str] = None   # "ws" | "rest"; None = nothing current since the last (re)subscribe
        self.pushed_at: float = 0.0      # monotonic time of the last valid WS push
        self.confirmed_at: float = 0.0   # monotonic time the book was last known current (a push, a seed, a probe)
        self.misses: int = 0             # freshness misses in a row, reset by any valid push

    def clear(self) -> None:
        self.bids = []
        self.asks = []
        self.source = None
        self.pushed_at = 0.0
        self.confirmed_at = 0.0
        self.misses = 0


def _crossed(bids: list, asks: list) -> bool:
    return bool(bids and asks and float(bids[0][0]) >= float(asks[0][0]))


class HotcoinAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Hotcoin public market data: `market.<s>.trade.depth` (the whole book, <=100 levels a side, pushed on change) and
    `market.<s>.trade.detail` (public trades), one gzip WebSocket for every market.

    Every depth push IS the book, so there is no local diff book to keep: each push becomes a top-EMIT_DEPTH
    SNAPSHOT message, handed to the tracker inside the WebSocket reader (no hand-off to the diff listener task;
    XT's latency path, runbook §1.4). Only snapshots ever reach the tracker (its snapshot path replays windowed
    diffs unchecked), and c_apply_snapshot stamps last_applied_diff, which the orchestrator's stale-book check reads.
    Measured from myserver 2026-10-05: ~1 push/s on active markets, 37-40 ms after the push's own ts; `trade.bbo`
    pushes at the same moments, so depth is the fastest book channel.

    What Hotcoin does NOT give, and what stands in for it:
      - No book on subscribe for a market that doesn't change (P1: 16 of 284 silent for 150 s while REST showed
        live books). After every (re)subscribe, each market the stream has not served SEED_DELAY s later gets a
        REST /v1/depth book (P1's snapshot-first bootstrap).
      - No sequence id, and every sub is ACKed even for a market that doesn't exist, so neither a lost push nor a
        dead subscription is visible in-band. _freshness_loop compares the market silent the longest with REST
        (top FRESHNESS_LEVELS a side): a book REST disagrees with on two reads, with no push in between, missed an
        update; it takes REST's book, is re-subscribed and probed again, and FRESHNESS_MISSES_TO_RECONNECT misses of
        the same market reconnect the stream. A quiet market whose book REST confirms is re-emitted, which also
        keeps the orchestrator's stale check honest without a reconnect (quiet != dropped).
      - A crossed push: the held book is superseded and the new one unusable, so the market is shown empty until a
        valid push or a REST seed.
      - The trading switch: /v1/common/symbols `state` (enable | disable). While a tracked market is not listed or
        not `enable`, the tracker gets an EMPTY book (a halt, as for XT, 2026-09-26); the symbol, the trading rule
        and the held book stay, so switching back on shows the book at once.

    The stream is the book (XT, 2026-09-30): while no connection is live every tracked book is shown EMPTY, and a
    REST answer that lands while the stream is down is dropped. A runtime add subscribes on the live socket (no
    reconnect), and `_refresh_snapshot_for_pair` re-subscribes and re-seeds one market for the orchestrator.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "HotcoinExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "HotcoinExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _Book] = {}
        # Read by the orchestrator's per-market stale-book recovery, with _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        # The tracker's diff stream, known once listen_for_order_book_diffs runs; books go straight into it.
        self._diff_output: Optional[asyncio.Queue] = None
        # True from a connection's subscribe until its interruption: only then is a book kept current.
        self._stream_live: bool = False
        self._seed_pending: Set[str] = set()
        self._seed_task: Optional[asyncio.Task] = None
        # Hotcoin's trading switch: markets switched off (shown empty), and markets whose switch has been read.
        self._switched_off: Set[str] = set()
        self._switch_known: Set[str] = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        self._warned: Set[str] = set()
        self._backoff = web_utils.HotcoinReconnectBackoff()

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

    async def _pair_for_symbol(self, symbol: str) -> str:
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            self._symbol_to_pair_cache[symbol] = trading_pair
            self._pair_to_symbol_cache[trading_pair] = symbol
        return trading_pair

    def _book(self, symbol: str) -> _Book:
        book = self._books.get(symbol)
        if book is None:
            book = self._books[symbol] = _Book()
        return book

    async def _tracked_symbols(self) -> List[str]:
        symbols = []
        for trading_pair in list(self._trading_pairs):
            try:
                symbols.append(await self._symbol_for_pair(trading_pair))
            except KeyError:
                continue  # unmapped: nothing is subscribed for it either
        return symbols

    def _is_tracked(self, symbol: str) -> bool:
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        return trading_pair is not None and trading_pair in self._trading_pairs

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
        if symbol in self._switched_off or book.source is None:
            return self._message(trading_pair, book.update_id or int(time.time() * 1e3), [], [])
        return self._message(trading_pair, book.update_id, book.bids, book.asks)

    def _emit(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is None or trading_pair is None:
            return
        output.put_nowait(self._snapshot_message(trading_pair, symbol, self._book(symbol)))

    def _show_empty(self, symbol: str) -> None:
        """This market's book is being rebuilt, so the tracker's copy is no longer kept current: show it empty (a
        halt) until the rebuilt book is emitted."""
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
        """(bids, asks, server ms) from GET /v1/depth (100 levels a side, best first)."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH, domain=self._domain),
            params={"symbol": symbol},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if web_utils.error_code(response) == CONSTANTS.CODE_SYMBOL_INVALID:
            raise HotcoinMarketRefused(f"Hotcoin refused the depth of {symbol}: {response}")
        if not web_utils.is_ok(response):
            raise IOError(f"Error fetching Hotcoin depth for {symbol}: {response}")
        depth = (response.get("data") or {}).get("depth") or {}
        return (list(depth.get("bids") or []), list(depth.get("asks") or []),
                int(response.get("time") or time.time() * 1e3))

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh. A book the stream (or a seed)
        already serves answers it directly: a REST answer applied over it could roll the tracker back."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            # An empty book and one warning: raising would end the hourly refresh at this pair, every hour.
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"Hotcoin {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, int(time.time() * 1e3), [], [])
        await self._ensure_switch_known(symbol)
        book = self._book(symbol)
        if book.source is None and self._stream_live:
            requested_at = time.monotonic()
            try:
                bids, asks, server_ms = await self._rest_book(symbol)
            except HotcoinMarketRefused as refused:
                self._warn_once(f"refused:{symbol}", f"Hotcoin {trading_pair}: {refused}. Its book stays empty.")
                safe_ensure_future(self._read_switches_safely())
                return self._message(trading_pair, int(time.time() * 1e3), [], [])
            if book.pushed_at < requested_at and self._stream_live and not _crossed(bids, asks):
                self._store(book, bids, asks, server_ms, "rest")
        # Otherwise served, or (stream down) nothing would keep a REST book current: empty until the stream rebuilds
        # it. On a runtime add the tracker replays the pushes it parked meanwhile as DIFFS over this snapshot, which
        # would merge two whole books: the same book, emitted again shortly, replaces the merge.
        asyncio.get_event_loop().call_later(CONSTANTS.SNAPSHOT_REEMIT_DELAY, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, book)

    def _reemit(self, symbol: str) -> None:
        if self._stream_live and self._book(symbol).source is not None:
            self._emit(symbol)

    # ------------------------------------------------------------------ seeds: no book on subscribe

    def _schedule_seed(self, symbols: List[str], delay: float = CONSTANTS.SEED_DELAY) -> None:
        self._seed_pending.update(symbols)
        if self._seed_task is None or self._seed_task.done():
            self._seed_task = safe_ensure_future(self._seed_loop(delay))

    async def _seed_loop(self, delay: float) -> None:
        """REST book for each pending market the stream has not served since its (re)subscribe. Active markets push
        within ~1.2 s, so after `delay` most are served and skipped; only quiet ones hit REST, SEED_INTERVAL apart."""
        if delay:
            await asyncio.sleep(delay)
        failures: Dict[str, int] = {}
        while self._seed_pending:
            if not self._stream_live:
                self._seed_pending.clear()  # the next connection seeds every market again
                return
            symbol = self._seed_pending.pop()
            book = self._book(symbol)
            if book.source is not None or not self._is_tracked(symbol):
                continue
            requested_at = time.monotonic()
            try:
                bids, asks, server_ms = await self._rest_book(symbol)
            except asyncio.CancelledError:
                raise
            except HotcoinMarketRefused as refused:
                self._warn_once(f"refused:{symbol}", f"Hotcoin {self._symbol_to_pair_cache.get(symbol, symbol)}: "
                                                     f"{refused}. Its book stays empty.")
                safe_ensure_future(self._read_switches_safely())
                continue
            except Exception as e:
                failures[symbol] = failures.get(symbol, 0) + 1
                if failures[symbol] % 10 == 1:
                    self.logger().warning(f"Hotcoin depth seed for {symbol} failed ({e}; {failures[symbol]} in a row); "
                                          f"retrying every 2 s.")
                self._seed_pending.add(symbol)
                await asyncio.sleep(2.0)
                continue
            if _crossed(bids, asks):
                self.logger().debug(f"Hotcoin depth seed for {symbol} came crossed; the book stays empty until a "
                                    f"valid push.")
            elif self._stream_live and book.source is None and book.pushed_at < requested_at:
                self._store(book, bids, asks, server_ms, "rest")
                self._emit(symbol)
            await asyncio.sleep(CONSTANTS.SEED_INTERVAL)

    # ------------------------------------------------------------------ Hotcoin's trading switch

    async def _read_switches(self) -> None:
        """Read Hotcoin's switch (/v1/common/symbols `state`) for every tracked market and apply it. A market missing
        from a non-empty answer is no longer listed and counts as off; an empty answer is not trusted (one bad reply
        must not empty every tracked book)."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.SYMBOLS_PATH, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SYMBOLS_PATH,
        )
        if not web_utils.is_ok(response):
            raise IOError(f"Error reading Hotcoin's market list: {response}")
        listed = {str(info.get("symbol")): info for info in response.get("data") or [] if isinstance(info, dict)}
        if not listed:
            raise IOError("Hotcoin's market list came back empty")
        for symbol in await self._tracked_symbols():
            self._apply_switch(symbol, listed.get(symbol))

    async def _read_switches_safely(self) -> None:
        try:
            await self._read_switches()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"Hotcoin trading-switch read failed: {e}")

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = info is None or str(info.get("state")).lower() != "enable"
        if off == (symbol in self._switched_off):
            return
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = "not listed" if info is None else f"state={info.get('state')}"
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"Hotcoin {trading_pair}: trading is switched off by Hotcoin ({flags}); its book is "
                                  f"shown empty until Hotcoin switches it back on.")
            self._emit(symbol)
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"Hotcoin {trading_pair}: trading is back on ({flags}); its book is shown again.")
            if self._book(symbol).source is not None:
                self._emit(symbol)
            elif self._stream_live:
                self._schedule_seed([symbol], delay=0.0)

    async def _ensure_switch_known(self, symbol: str) -> None:
        """Before a market's first snapshot reaches the tracker, so a market that is already off never shows a book.
        One request covers every tracked market."""
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
                                f"Hotcoin trading-switch read failed ({e}); markets count as on until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("Hotcoin trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"Hotcoin trading-switch read failed ({e}); every market keeps its last "
                                          f"state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ freshness (no sequence id)

    @staticmethod
    def _top(levels: list) -> List[Tuple[Decimal, Decimal]]:
        return [(Decimal(str(price)), Decimal(str(qty))) for price, qty, *_ in levels[:CONSTANTS.FRESHNESS_LEVELS]]

    def _same_as_rest(self, book: _Book, bids: list, asks: list) -> bool:
        return self._top(book.bids) == self._top(bids) and self._top(book.asks) == self._top(asks)

    def _probe_eligible(self, symbol: str) -> bool:
        book = self._books.get(symbol)
        return (book is not None and book.source is not None and symbol not in self._switched_off
                and self._is_tracked(symbol))

    async def _freshness_loop(self) -> None:
        """Every FRESHNESS_INTERVAL s one market is checked against REST: the one that missed last time, else the
        tracked market silent the longest (>= FRESHNESS_QUIET_SECONDS since it was last known current).
          - Same top FRESHNESS_LEVELS a side: confirmed and re-emitted (which also keeps the orchestrator's stale check
            from repairing a quiet but correct book: quiet != dropped).
          - Different: the stream gets FRESHNESS_GRACE to push it. No push, and a second REST read still differs from
            the held book (a level placed and pulled inside one push tick is no miss), is a MISS: REST's newer book is
            taken, the market re-subscribed and probed again next. FRESHNESS_MISSES_TO_RECONNECT misses of that same
            market with no valid push in between reconnect the stream (the counter lives on the book, so a healthy
            market elsewhere can't reset it).
        A probe whose market pushes meanwhile judges nothing; 3 failed probes in a row are a WARNING (the stream is
        unchecked meanwhile)."""
        errors = 0
        again: Optional[str] = None
        while True:
            await asyncio.sleep(CONSTANTS.FRESHNESS_INTERVAL)
            symbol: Optional[str] = None
            try:
                ws = self._active_ws
                if ws is None or not self._stream_live:
                    again = None
                    continue
                if again is not None and self._probe_eligible(again):
                    symbol = again
                else:
                    oldest = time.monotonic() - CONSTANTS.FRESHNESS_QUIET_SECONDS
                    quiet = [(book.confirmed_at, sym) for sym, book in self._books.items()
                             if self._probe_eligible(sym) and book.confirmed_at <= oldest]
                    if not quiet:
                        again = None
                        continue
                    _, symbol = min(quiet)
                again = None
                book = self._books[symbol]
                requested_at = time.monotonic()
                bids, asks, server_ms = await self._rest_book(symbol)
                if errors >= 3:
                    self.logger().info("Hotcoin freshness probes work again.")
                errors = 0
                if book.pushed_at >= requested_at or self._active_ws is not ws or book.source is None:
                    continue
                if self._same_as_rest(book, bids, asks):
                    book.confirmed_at = time.monotonic()
                    self._emit(symbol)
                    continue
                await asyncio.sleep(CONSTANTS.FRESHNESS_GRACE)
                if (self._active_ws is not ws or not self._stream_live or book.source is None
                        or book.pushed_at >= requested_at):
                    continue  # the stream delivered (it was only behind by its own throttle), or the book is rebuilt
                bids, asks, server_ms = await self._rest_book(symbol)
                if book.pushed_at >= requested_at or self._active_ws is not ws or book.source is None:
                    continue
                if self._same_as_rest(book, bids, asks):
                    book.confirmed_at = time.monotonic()   # the difference didn't last: nothing was missed
                    self._emit(symbol)
                    continue
                if _crossed(bids, asks):
                    continue  # REST's own book is unusable: judged on a later probe
                book.misses += 1
                silent = time.monotonic() - (book.pushed_at or requested_at)
                trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
                # REST's book is newer than the one held: take it, then make the stream serve the market again.
                self._store(book, bids, asks, server_ms, "rest")
                self._emit(symbol)
                if book.misses >= CONSTANTS.FRESHNESS_MISSES_TO_RECONNECT:
                    self.logger().warning(f"Hotcoin public stream: {trading_pair} missed updates {book.misses} times "
                                          f"in a row (no push for {silent:.0f} s while REST's book changed; "
                                          f"re-subscribing didn't help); reconnecting.")
                    await ws.disconnect()
                    continue
                self.logger().warning(f"Hotcoin {trading_pair}: the stream missed an update (REST's book differs on "
                                      f"two reads, no push for {silent:.0f} s); took REST's book and re-subscribed "
                                      f"({book.misses}/{CONSTANTS.FRESHNESS_MISSES_TO_RECONNECT}).")
                await self._send_subscription(ws, symbol, "unsub")
                await self._send_subscription(ws, symbol, "sub")
                again = symbol
            except asyncio.CancelledError:
                raise
            except HotcoinMarketRefused as refused:
                self._warn_once(f"refused:{symbol}", f"Hotcoin {self._symbol_to_pair_cache.get(symbol, symbol)}: "
                                                     f"{refused}; reading Hotcoin's trading switch.")
                await self._read_switches_safely()
            except Exception as e:
                errors += 1
                if errors == 3:
                    self.logger().warning(f"Hotcoin freshness probes failing ({errors} in a row, last: {e}); the "
                                          f"public stream is unchecked until they work again.")
                else:
                    self.logger().debug(f"Hotcoin freshness probe skipped: {e}")

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _send_subscription(self, ws: WSAssistant, symbol: str, verb: str) -> None:
        # One topic per message: a list or a comma-joined `sub` is ACKed but never streams (P1).
        for topic in (CONSTANTS.WS_DEPTH_TOPIC, CONSTANTS.WS_TRADE_TOPIC):
            await ws.send(WSJSONRequest(payload={verb: topic.format(symbol=symbol)}))
            await asyncio.sleep(CONSTANTS.WS_SUBSCRIBE_SPACING)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in list(self._trading_pairs):
                try:
                    symbols.append(await self._symbol_for_pair(trading_pair))
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every Hotcoin book empty.
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"Hotcoin {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
            for symbol in symbols:
                self._book(symbol).clear()  # nothing is current until the stream or a seed serves it
            self._stream_live = True
            self._backoff.connected()
            for symbol in symbols:
                await self._send_subscription(ws, symbol, "sub")
            self.logger().info(f"Subscribed to Hotcoin depth and trade channels for {len(symbols)} markets.")
            self._schedule_seed(symbols)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Hotcoin public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which blanks every Hotcoin book until the reconnect re-seeds them."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).clear()
        await self._send_subscription(ws, symbol, "sub")
        self._schedule_seed([symbol])
        self.logger().info(f"Subscribed Hotcoin {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: re-subscribe this one market and seed it from
        REST. The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._show_empty(symbol)
        self._book(symbol).clear()
        ws = self._active_ws
        if ws is not None:
            await self._send_subscription(ws, symbol, "unsub")
            await self._send_subscription(ws, symbol, "sub")
        if self._stream_live:
            self._schedule_seed([symbol], delay=0.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = safe_ensure_future(self._trading_switch_loop())
        freshness_task = safe_ensure_future(self._freshness_loop())
        try:
            # Books go straight into `output` from the WebSocket reader; the diff queue stays empty.
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()
            freshness_task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        try:
            async for ws_response in websocket_assistant.iter_messages():
                try:
                    message = web_utils.decode_ws_frame(ws_response.data)
                except Exception as e:
                    self._warn_once("undecodable", f"Hotcoin public WS frame could not be decoded ({e}); skipped.")
                    continue
                if not isinstance(message, dict):
                    continue
                if "ping" in message:
                    await websocket_assistant.send(WSJSONRequest(payload={"pong": "pong"}))
                    continue
                channel = message.get("ch") or ""
                data = message.get("data")
                if data is not None and channel.endswith(".trade.depth"):
                    self._on_depth(channel, message, data)
                elif data is not None and channel.endswith(".trade.detail"):
                    trade_queue.put_nowait(message)
                else:
                    self._on_control_message(message)
        except asyncio.TimeoutError:
            raise ConnectionError(f"no message from Hotcoin for {CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s "
                                  f"(the server pings every 5 s)") from None

    def _on_depth(self, channel: str, message: Dict[str, Any], data: Dict[str, Any]) -> None:
        """One depth push: the whole book. Synchronous, inside the WebSocket reader."""
        parts = channel.split(".")
        if len(parts) < 4:
            return
        symbol = parts[1] if parts[1] in self._symbol_to_pair_cache else parts[1].lower()
        if symbol not in self._symbol_to_pair_cache:
            return  # not a market this connector tracks (an unsubscribe in flight)
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        book = self._book(symbol)
        if _crossed(bids, asks):
            # The held book is superseded and the new one unusable: shown empty (a halt) until a valid push or a seed.
            # Not stamped as delivered, so the freshness probe never counts it as the stream keeping up.
            self._warn_once(f"crossed:{symbol}", f"Hotcoin {symbol}: a depth push came crossed (bid {bids[0][0]} >= "
                                                 f"ask {asks[0][0]}); the book is shown empty until a valid one.")
            if book.source is not None:
                self._show_empty(symbol)
                book.clear()
                if self._stream_live:
                    self._schedule_seed([symbol])
            return
        book.pushed_at = time.monotonic()
        book.misses = 0
        self._store(book, bids, asks, int(message.get("ts") or time.time() * 1e3), "ws")
        self._emit(symbol)

    def _on_control_message(self, message: Dict[str, Any]) -> None:
        # Live 2026-10-05: greeting {"status":"ok","ts"}, sub/unsub acks {"ch","code":200,"msg":"SUCCESS",
        # "status":"ok","ts"} (also for markets that don't exist). Anything else that isn't ok is logged as is.
        if web_utils.is_ok(message) or (message.get("status") == "ok" and "code" not in message):
            return
        self.logger().warning(f"Hotcoin public WS: {message}")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message.get("ch") or ""
        if event_message.get("data") is not None and channel.endswith(".trade.depth"):
            return self._diff_messages_queue_key
        if event_message.get("data") is not None and channel.endswith(".trade.detail"):
            return self._trade_messages_queue_key
        return ""

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
        self._seed_pending.clear()
        if self._seed_task is not None and not self._seed_task.done():
            self._seed_task.cancel()
        self._seed_task = None
        if was_live:
            # Every book the tracker holds would stay frozen from here on: show them empty, a halt, until the next
            # connection rebuilds them.
            for trading_pair in list(self._trading_pairs):
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                if symbol is not None:
                    self._show_empty(symbol)
        for book in self._books.values():
            book.clear()
        await self._backoff.wait()  # the listen loop reconnects right after this

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Unused: depth pushes are applied in the WebSocket reader (_on_depth), never queued.
        return

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """market.<s>.trade.detail: a LIST of recent trades per push, {amount, direction, price, tradeId, ts}. Live
        2026-10-05: `tradeId` is the same value on every trade of a market (900003 on BTC), so the trade id here is
        built from the trade's own ts and its place in the push."""
        parts = (raw_message.get("ch") or "").split(".")
        if len(parts) < 4:
            return
        try:
            trading_pair = await self._pair_for_symbol(parts[1])
        except KeyError:
            return
        trades = raw_message.get("data") or []
        if isinstance(trades, dict):
            trades = [trades]
        ordered = sorted((t for t in trades if isinstance(t, dict)), key=lambda t: int(t.get("ts") or 0))
        for index, trade in enumerate(ordered):
            timestamp_ms = int(trade.get("ts") or time.time() * 1e3)
            trade_type = TradeType.BUY if str(trade.get("direction")).lower() == "buy" else TradeType.SELL
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": f"{timestamp_ms}-{index}",
                    "update_id": timestamp_ms,
                    "price": trade.get("price"),
                    "amount": trade.get("amount"),
                },
                timestamp=timestamp_ms * 1e-3,
            ))
