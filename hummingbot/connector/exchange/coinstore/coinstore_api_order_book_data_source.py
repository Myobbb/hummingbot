import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] strings as
    Coinstore sends them) and where and when it came from."""

    __slots__ = ("bids", "asks", "update_id", "source", "pushed_at", "confirmed_at", "misses", "resubscribed_at")

    def __init__(self) -> None:
        self.bids: List[List[str]] = []
        self.asks: List[List[str]] = []
        self.update_id: int = 0             # local ms of the push (Coinstore's pushes carry no server time)
        self.source: Optional[str] = None   # "ws" | "rest"; None = nothing current since the last (re)subscribe
        self.pushed_at: float = 0.0         # monotonic time of the last valid WS push
        self.confirmed_at: float = 0.0      # monotonic time the book was last known current (a push, a seed, a probe)
        self.misses: int = 0                # freshness misses in a row, reset by any valid push
        self.resubscribed_at: float = 0.0   # monotonic time of the last silence re-subscribe

    def clear(self) -> None:
        self.bids = []
        self.asks = []
        self.source = None
        self.pushed_at = 0.0
        self.confirmed_at = 0.0
        self.misses = 0


def _crossed(bids: list, asks: list) -> bool:
    return bool(bids and asks and Decimal(str(bids[0][0])) >= Decimal(str(asks[0][0])))


class CoinstoreAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Coinstore public market data: `<sym>@depth@20` (the top 20 levels a side as a full snapshot on every push, pushed on
    change and right after each SUB) and `<sym>@trade`, for every market on ONE socket (P1 carries all 373 USDT
    markets on one; no per-socket cap).

    Every depth push IS the book, so there is no local diff book: each push becomes a SNAPSHOT message, handed to the
    tracker inside the WebSocket reader (no hand-off to the diff listener task; XT's latency path, runbook §1.4). Only
    snapshots ever reach the tracker (its snapshot path replays windowed diffs unchecked), and c_apply_snapshot stamps
    last_applied_diff, which the orchestrator's stale-book check reads.

    Measured from myserver 2026-11-08 (hmb_local_tools/coinstore/debug_coinstore_ws_cadence.py and
    debug_coinstore_book_sources.py): an active book pushes every ~800 ms, the same moments on all five depth channels
    of one socket, so @20 is as fast as any; a quiet market re-pushes about every 60 s even with no change.

    What Coinstore does NOT give, and what stands in for it:
      - No sequence id and no server time in a push. Every frame carries the session's message number `S`: a gap means
        frames were lost, and since it can't tell which markets lost one, every market is re-subscribed (each SUB
        brings a fresh snapshot), at most once per S_GAP_RESUBSCRIBE_EVERY s.
      - No proof a subscription is alive, except Coinstore's ~60 s re-push: a tracked market silent for
        MARKET_SILENCE_SECONDS is re-subscribed; silent again after that, the socket is reconnected.
      - A node could serve an outdated book: _freshness_loop compares the market silent the longest with REST (top
        FRESHNESS_LEVELS a side), Hotcoin's method: a book REST disagrees with on two reads, with no push in between,
        missed an update; it takes REST's book and is re-subscribed.
      - A crossed push: the held book is superseded and the new one unusable, so the market is shown empty until a
        valid push.
      - The trading switch: the symbol list's `openTrade`. While a tracked market is not listed or not open, the
        tracker gets an EMPTY book (a halt, as for XT, 2026-09-26); the symbol, the trading rule and the held book stay.

    The stream is the book (XT, 2026-09-30): while no connection is live every tracked book is shown EMPTY, and a REST
    answer that lands while the stream is down is dropped. A runtime add subscribes on the live socket (no reconnect),
    and `_refresh_snapshot_for_pair` re-subscribes one market for the orchestrator.
    """

    S_GAP_RESUBSCRIBE_EVERY = 30.0

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinstoreExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "CoinstoreExchange" = connector
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
        self._last_seq: int = 0
        self._last_gap_resubscribe: float = 0.0
        self._refused: Set[str] = set()      # markets whose SUB Coinstore refused (shown empty)
        # The last trade id seen per market (the trade channel's first push after a SUB is recent history).
        self._last_trade_id: Dict[str, int] = {}
        # Coinstore's trading switch: markets switched off (shown empty), and markets whose switch has been read.
        self._switched_off: Set[str] = set()
        self._switch_known: Set[str] = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        self._warned: Set[str] = set()
        self._request_id = 0
        self._backoff = web_utils.CoinstoreReconnectBackoff()

    # ------------------------------------------------------------------ helpers

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.logger().warning(message)

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

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

    @staticmethod
    def _channels(symbol: str) -> List[str]:
        wire = symbol.lower()   # every channel name is lower case (docs)
        return [CONSTANTS.WS_DEPTH_CHANNEL.format(symbol=wire), CONSTANTS.WS_TRADE_CHANNEL.format(symbol=wire)]

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
        if symbol in self._switched_off or symbol in self._refused or book.source is None:
            return self._message(trading_pair, book.update_id or int(time.time() * 1e3), [], [])
        return self._message(trading_pair, book.update_id, book.bids, book.asks)

    def _emit(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is None or trading_pair is None:
            return
        output.put_nowait(self._snapshot_message(trading_pair, symbol, self._book(symbol)))

    def _show_empty(self, symbol: str) -> None:
        """This market's book is being rebuilt, so the tracker's copy is no longer kept current: show it empty (a halt)
        until the rebuilt book is emitted."""
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and trading_pair is not None:
            book = self._books.get(symbol)
            output.put_nowait(self._message(trading_pair, (book.update_id if book else 0) or int(time.time() * 1e3),
                                            [], []))

    @staticmethod
    def _store(book: _Book, bids: list, asks: list, source: str) -> None:
        book.bids = [[str(level[0]), str(level[1])] for level in bids[:CONSTANTS.EMIT_DEPTH]]
        book.asks = [[str(level[0]), str(level[1])] for level in asks[:CONSTANTS.EMIT_DEPTH]]
        # Monotonic per market: the tracker orders snapshots by update id.
        book.update_id = max(int(time.time() * 1e3), book.update_id + 1)
        book.source = source
        book.confirmed_at = time.monotonic()

    # ------------------------------------------------------------------ REST

    async def _rest_book(self, symbol: str) -> Tuple[list, list]:
        """(bids, asks) from GET /v1/market/depth/{SYMBOL}?depth=20: [price, qty, side] strings, best first."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.depth_url(symbol, domain=self._domain),
            params={"depth": CONSTANTS.EMIT_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if not web_utils.is_ok(response):
            raise IOError(f"Error fetching Coinstore depth for {symbol}: {response}")
        data = response.get("data") or {}
        return list(data.get("b") or []), list(data.get("a") or [])

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh. A book the stream already serves
        answers it directly: a REST answer applied over it could roll the tracker back."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            # An empty book and one warning: raising would end the hourly refresh at this pair, every hour.
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"Coinstore {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, int(time.time() * 1e3), [], [])
        await self._ensure_switch_known(symbol)
        book = self._book(symbol)
        if book.source is None and self._stream_live:
            requested_at = time.monotonic()
            bids, asks = await self._rest_book(symbol)
            if book.pushed_at < requested_at and self._stream_live and not _crossed(bids, asks):
                self._store(book, bids, asks, "rest")
        # Otherwise served, or (stream down) nothing would keep a REST book current: empty until the stream rebuilds
        # it. On a runtime add the tracker replays the pushes it parked meanwhile as DIFFS over this snapshot, which
        # would merge two whole books: the same book, emitted again shortly, replaces the merge.
        asyncio.get_event_loop().call_later(CONSTANTS.SNAPSHOT_REEMIT_DELAY, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, book)

    def _reemit(self, symbol: str) -> None:
        if self._stream_live and self._book(symbol).source is not None:
            self._emit(symbol)

    # ------------------------------------------------------------------ Coinstore's trading switch

    async def _read_switches(self) -> None:
        """Read `openTrade` for every tracked market from the symbol list and apply it. A market missing from a
        non-empty answer is no longer listed and counts as off; an empty answer is not trusted (one bad reply must
        not empty every tracked book)."""
        listed = await self._connector.symbol_list_by_symbol()
        if not listed:
            raise IOError("Coinstore's symbol list came back empty")
        for symbol in await self._tracked_symbols():
            self._apply_switch(symbol, listed.get(symbol))

    async def _read_switches_safely(self) -> None:
        try:
            await self._read_switches()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"Coinstore trading-switch read failed: {e}")

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = info is None or info.get("openTrade") is not True
        if off == (symbol in self._switched_off):
            return
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = "not listed" if info is None else f"openTrade={info.get('openTrade')}"
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"Coinstore {trading_pair}: trading is switched off by Coinstore ({flags}); its book "
                                  f"is shown empty until Coinstore switches it back on.")
            self._emit(symbol)
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"Coinstore {trading_pair}: trading is back on ({flags}); its book is shown again.")
            if self._book(symbol).source is not None:
                self._emit(symbol)

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
                                f"Coinstore trading-switch read failed ({e}); markets count as on until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("Coinstore trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"Coinstore trading-switch read failed ({e}); every market keeps its last "
                                          f"state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ liveness and freshness (no sequence id)

    async def _liveness_loop(self) -> None:
        while True:
            await asyncio.sleep(CONSTANTS.LIVENESS_INTERVAL)
            try:
                await self._liveness_check()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"Coinstore liveness check skipped: {e}")

    async def _liveness_check(self) -> None:
        """One round (every LIVENESS_INTERVAL s): a tracked, served market silent for MARKET_SILENCE_SECONDS (a live
        subscription re-pushes about every 60 s) is re-subscribed; one still silent MARKET_SILENCE_SECONDS after that
        reconnects the socket."""
        ws = self._active_ws
        if ws is None or not self._stream_live:
            return
        now = time.monotonic()
        for symbol in await self._tracked_symbols():
            book = self._books.get(symbol)
            if book is None or symbol in self._refused:
                continue
            heard = max(book.pushed_at, book.resubscribed_at)
            if heard and now - heard < CONSTANTS.MARKET_SILENCE_SECONDS:
                continue
            if not heard:
                book.resubscribed_at = now   # subscribed but never served: start its clock
                continue
            trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
            if book.resubscribed_at > book.pushed_at:
                self.logger().warning(f"Coinstore public stream: {trading_pair} silent {now - book.pushed_at:.0f} s, "
                                      f"again after a re-subscribe; reconnecting.")
                await ws.disconnect()
                return
            self.logger().warning(f"Coinstore {trading_pair}: no push for {now - book.pushed_at:.0f} s (a live "
                                  f"subscription re-pushes about every 60 s); re-subscribing it.")
            book.resubscribed_at = now
            await self._send(ws, "UNSUB", [symbol])
            await self._send(ws, "SUB", [symbol])

    @staticmethod
    def _top(levels: list) -> List[Tuple[Decimal, Decimal]]:
        return [(Decimal(str(level[0])), Decimal(str(level[1]))) for level in levels[:CONSTANTS.FRESHNESS_LEVELS]]

    def _same_as_rest(self, book: _Book, bids: list, asks: list) -> bool:
        return self._top(book.bids) == self._top(bids) and self._top(book.asks) == self._top(asks)

    def _probe_eligible(self, symbol: str) -> bool:
        book = self._books.get(symbol)
        return (book is not None and book.source is not None and symbol not in self._switched_off
                and symbol not in self._refused and self._is_tracked(symbol))

    async def _freshness_loop(self) -> None:
        """Every FRESHNESS_INTERVAL s one market is checked against REST: the one that missed last time, else the
        tracked market silent the longest (>= FRESHNESS_QUIET_SECONDS since it was last known current).
          - Same top FRESHNESS_LEVELS a side: confirmed and re-emitted (which also keeps the orchestrator's stale check
            from repairing a quiet but correct book: quiet != dropped).
          - Different: the stream gets FRESHNESS_GRACE to push it. No push, and a second REST read still differs from
            the held book, is a MISS: REST's book is taken, the market re-subscribed and probed again next.
            FRESHNESS_MISSES_TO_RECONNECT misses of that same market in a row (no push in between) reconnect the stream.
        A probe whose market pushes meanwhile judges nothing; 3 failed probes in a row are a WARNING."""
        errors = 0
        again: Optional[str] = None
        while True:
            await asyncio.sleep(CONSTANTS.FRESHNESS_INTERVAL)
            try:
                again = await self._freshness_round(again)
                if errors >= 3:
                    self.logger().info("Coinstore freshness probes work again.")
                errors = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                again = None
                errors += 1
                if errors == 3:
                    self.logger().warning(f"Coinstore freshness probes failing ({errors} in a row, last: {e}); the "
                                          f"public stream is unchecked until they work again.")
                else:
                    self.logger().debug(f"Coinstore freshness probe skipped: {e}")

    async def _freshness_round(self, again: Optional[str]) -> Optional[str]:
        """One probe (see _freshness_loop). Returns the market to probe again next round (after a miss), or None."""
        ws = self._active_ws
        if ws is None or not self._stream_live:
            return None
        if again is not None and self._probe_eligible(again):
            symbol = again
        else:
            oldest = time.monotonic() - CONSTANTS.FRESHNESS_QUIET_SECONDS
            quiet = [(book.confirmed_at, sym) for sym, book in self._books.items()
                     if self._probe_eligible(sym) and book.confirmed_at <= oldest]
            if not quiet:
                return None
            _, symbol = min(quiet)
        book = self._books[symbol]
        requested_at = time.monotonic()
        bids, asks = await self._rest_book(symbol)
        if book.pushed_at >= requested_at or self._active_ws is not ws or book.source is None:
            return None
        if self._same_as_rest(book, bids, asks):
            book.confirmed_at = time.monotonic()
            self._emit(symbol)
            return None
        await asyncio.sleep(CONSTANTS.FRESHNESS_GRACE)
        if (self._active_ws is not ws or not self._stream_live or book.source is None
                or book.pushed_at >= requested_at):
            return None  # the stream delivered (it was only waiting for its push cycle), or the book is rebuilt
        bids, asks = await self._rest_book(symbol)
        if book.pushed_at >= requested_at or self._active_ws is not ws or book.source is None:
            return None
        if self._same_as_rest(book, bids, asks):
            book.confirmed_at = time.monotonic()   # the difference didn't last: nothing was missed
            self._emit(symbol)
            return None
        if _crossed(bids, asks):
            return None  # REST's own book is unusable: judged on a later probe
        book.misses += 1
        silent = time.monotonic() - (book.pushed_at or requested_at)
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        self._store(book, bids, asks, "rest")
        self._emit(symbol)
        if book.misses >= CONSTANTS.FRESHNESS_MISSES_TO_RECONNECT:
            self.logger().warning(f"Coinstore public stream: {trading_pair} missed updates {book.misses} times in a "
                                  f"row (no push for {silent:.0f} s while REST's book changed; re-subscribing didn't "
                                  f"help); reconnecting.")
            await ws.disconnect()
            return None
        self.logger().warning(f"Coinstore {trading_pair}: the stream missed an update (REST's book differs on two "
                              f"reads, no push for {silent:.0f} s); took REST's book and re-subscribed "
                              f"({book.misses}/{CONSTANTS.FRESHNESS_MISSES_TO_RECONNECT}).")
        await self._send(ws, "UNSUB", [symbol])
        await self._send(ws, "SUB", [symbol])
        return symbol

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(self._api_factory, CONSTANTS.WSS_URL)

    async def _send(self, ws: WSAssistant, op: str, symbols: List[str]) -> None:
        """SUB / UNSUB the depth and trade channels of these markets, WS_SUBSCRIBE_BATCH channels a message."""
        channels = [channel for symbol in symbols for channel in self._channels(symbol)]
        for i in range(0, len(channels), CONSTANTS.WS_SUBSCRIBE_BATCH):
            await ws.send(WSJSONRequest(payload={"op": op, "channel": channels[i:i + CONSTANTS.WS_SUBSCRIBE_BATCH],
                                                 "id": self._next_id()}))
            if i + CONSTANTS.WS_SUBSCRIBE_BATCH < len(channels):
                await asyncio.sleep(CONSTANTS.WS_SUBSCRIBE_SPACING)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in list(self._trading_pairs):
                try:
                    symbols.append(await self._symbol_for_pair(trading_pair))
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would hit it again
                    # every time and leave every Coinstore book empty.
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"Coinstore {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
            for symbol in symbols:
                self._book(symbol).clear()   # nothing is current until the stream serves it
            self._refused.clear()
            self._last_seq = 0
            self._stream_live = True
            self._backoff.connected()
            await self._send(ws, "SUB", symbols)
            self.logger().info(f"Subscribed to Coinstore depth@{CONSTANTS.EMIT_DEPTH} and trade channels for "
                               f"{len(symbols)} markets.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Coinstore public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket. The base default
        disconnects the shared socket, which would blank every Coinstore book until the reconnect re-serves them."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).clear()
        self._refused.discard(symbol)
        await self._send(ws, "SUB", [symbol])
        self.logger().info(f"Subscribed Coinstore {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: re-subscribe this one market (a SUB brings a
        fresh snapshot). The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._show_empty(symbol)
        self._book(symbol).clear()
        ws = self._active_ws
        if ws is not None:
            await self._send(ws, "UNSUB", [symbol])
            await self._send(ws, "SUB", [symbol])

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        tasks = [safe_ensure_future(self._trading_switch_loop()), safe_ensure_future(self._freshness_loop()),
                 safe_ensure_future(self._liveness_loop())]
        try:
            # Books go straight into `output` from the WebSocket reader; the diff queue stays empty.
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            for task in tasks:
                task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        try:
            async for ws_response in websocket_assistant.iter_messages():
                message = ws_response.data
                if not isinstance(message, dict):
                    continue
                self._check_sequence(websocket_assistant, message)
                kind = message.get("T")
                if kind == "depth":
                    self._on_depth(message)
                elif kind == "trade":
                    trade_queue.put_nowait(message)
                else:
                    self._on_control_message(message)
        except asyncio.TimeoutError:
            raise ConnectionError(f"no message from Coinstore for {CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s") from None

    def _check_sequence(self, ws: WSAssistant, message: Dict[str, Any]) -> None:
        """`S` numbers every frame of the session (docs: "can be used to tell whether messages were missed")."""
        seq = message.get("S")
        if not isinstance(seq, int):
            return
        last, self._last_seq = self._last_seq, seq
        if not last or seq <= last + 1:
            return
        now = time.monotonic()
        if now - self._last_gap_resubscribe < self.S_GAP_RESUBSCRIBE_EVERY:
            self.logger().info(f"Coinstore public stream lost {seq - last - 1} frame(s) (S {last} -> {seq}); "
                               f"re-subscribed under {self.S_GAP_RESUBSCRIBE_EVERY:.0f} s ago, not again.")
            return
        self._last_gap_resubscribe = now
        self.logger().warning(f"Coinstore public stream lost {seq - last - 1} frame(s) (S {last} -> {seq}): "
                              f"re-subscribing every market for fresh snapshots.")
        safe_ensure_future(self._resubscribe_all(ws))

    async def _resubscribe_all(self, ws: WSAssistant) -> None:
        try:
            symbols = await self._tracked_symbols()
            await self._send(ws, "UNSUB", symbols)
            await self._send(ws, "SUB", symbols)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Coinstore re-subscribe after a sequence gap failed ({e}); reconnecting.")
            await ws.disconnect()

    def _symbol_of(self, message: Dict[str, Any]) -> Optional[str]:
        symbol = str(message.get("symbol") or "").upper()
        if not symbol:
            channel = str(message.get("channel") or "")
            symbol = channel.split("@", 1)[0].upper() if "@" in channel else ""
        return symbol if symbol in self._symbol_to_pair_cache else None

    def _on_depth(self, message: Dict[str, Any]) -> None:
        """One depth push: the whole top 20. Synchronous, inside the WebSocket reader."""
        symbol = self._symbol_of(message)
        if symbol is None:
            return  # not a market this connector tracks (an unsubscribe in flight)
        bids = message.get("b") or []
        asks = message.get("a") or []
        book = self._book(symbol)
        if _crossed(bids, asks):
            # The held book is superseded and the new one unusable: shown empty (a halt) until a valid push. Not stamped
            # as delivered, so neither the liveness check nor the freshness probe counts it as the stream keeping up.
            self._warn_once(f"crossed:{symbol}", f"Coinstore {symbol}: a depth push came crossed (bid {bids[0][0]} >= "
                                                 f"ask {asks[0][0]}); the book is shown empty until a valid one.")
            if book.source is not None:
                self._show_empty(symbol)
                book.clear()
            return
        book.pushed_at = time.monotonic()
        book.misses = 0
        self._store(book, bids, asks, "ws")
        self._emit(symbol)

    def _on_control_message(self, message: Dict[str, Any]) -> None:
        """Live: the greeting {"T":"req","C":200,"M":"established"}, an echo per command ("T":"echo"), and per channel
        {"C":200,"M":"sub.channel.success"} or {"C":503,"M":"sub.channel_name.invalid[not found]","echo":"<channel>"};
        UNSUB answers {"M":"success"}, a repeated SUB "duplicate sub"."""
        if message.get("T") == "echo":
            return
        code = message.get("C")
        if code in (None, 200):
            return
        channel = str(message.get("echo") or "")
        meaning = str(message.get("M") or "")
        if "invalid" in meaning and "@depth@" in channel:
            symbol = channel.split("@", 1)[0].upper()
            if symbol in self._symbol_to_pair_cache and symbol not in self._refused:
                self._refused.add(symbol)
                self.logger().warning(f"Coinstore refused the book of {self._symbol_to_pair_cache[symbol]} ({code} "
                                      f"{meaning}); it is shown empty. The other markets are unaffected.")
                self._emit(symbol)
            return
        if "duplicate" in meaning:
            return
        self._warn_once(f"ws:{code}:{meaning}:{channel}", f"Coinstore public WS: {message}")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        kind = event_message.get("T")
        if kind == "depth":
            return self._diff_messages_queue_key
        if kind == "trade":
            return self._trade_messages_queue_key
        return ""

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
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
        """<sym>@trade: right after a SUB a `data` list of recent trades (history: it only sets the starting trade id;
        the frame names its market only inside each entry, live 2026-11-08), then one push per trade {tradeId, price,
        volume, takerSide BUY|SELL, ts ms, time s, symbol}. A trade id at or below the last one seen for the market is
        not emitted again."""
        history = raw_message.get("data")
        if isinstance(history, list):
            entries = [t for t in history if isinstance(t, dict)]
            symbol = self._symbol_of(entries[0]) if entries else None
            ids = [int(t["tradeId"]) for t in entries if t.get("tradeId") is not None]
            if symbol is not None and ids:
                self._last_trade_id[symbol] = max(self._last_trade_id.get(symbol, 0), max(ids))
            return
        symbol = self._symbol_of(raw_message)
        if symbol is None:
            return
        trading_pair = await self._pair_for_symbol(symbol)
        try:
            trade_id = int(raw_message.get("tradeId"))
        except (TypeError, ValueError):
            return
        if trade_id <= self._last_trade_id.get(symbol, 0):
            return
        self._last_trade_id[symbol] = trade_id
        timestamp_ms = int(raw_message.get("ts") or (int(raw_message.get("time") or 0) * 1000) or time.time() * 1e3)
        trade_type = TradeType.BUY if str(raw_message.get("takerSide")).upper() == "BUY" else TradeType.SELL
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": trade_id,
                "update_id": timestamp_ms,
                "price": raw_message.get("price"),
                "amount": raw_message.get("volume"),
            },
            timestamp=timestamp_ms * 1e-3,
        ))
