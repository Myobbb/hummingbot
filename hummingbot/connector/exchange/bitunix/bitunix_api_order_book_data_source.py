import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.exchange.bitunix import bitunix_constants as CONSTANTS, bitunix_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitunix.bitunix_exchange import BitunixExchange


# After three immediate re-subscribes, a silent market is re-subscribed at most this often (s).
STALE_RESUB_BACKOFF = 60.0


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] strings)
    and where and when it came from."""

    __slots__ = ("bids", "asks", "update_id", "source", "pushed_at", "stale_resubs")

    def __init__(self) -> None:
        self.bids: List[List[str]] = []
        self.asks: List[List[str]] = []
        self.update_id: int = 0
        self.source: Optional[str] = None   # "ws" | "rest"; None = nothing current since the last (re)subscribe
        self.pushed_at: float = 0.0         # monotonic time of the last valid push
        self.stale_resubs: int = 0          # re-subscribes for silence since the last valid push

    def clear(self) -> None:
        self.bids = []
        self.asks = []
        self.source = None
        self.pushed_at = 0.0


def _crossed(bids: list, asks: list) -> bool:
    return bool(bids and asks and float(bids[0][0]) >= float(asks[0][0]))


def _levels(rows: Any) -> List[List[str]]:
    """Bitunix's level rows {"width": [price, qty, cumulative qty]} -> [price, qty], size-0 rows dropped (P1's fix,
    2026-10-03: the site sends them)."""
    out: List[List[str]] = []
    for row in rows or []:
        width = row.get("width") if isinstance(row, dict) else row
        if not width or len(width) < 2:
            continue
        price, qty = str(width[0]), str(width[1])
        try:
            if float(qty) <= 0:
                continue
        except ValueError:
            continue
        out.append([price, qty])
        if len(out) >= CONSTANTS.EMIT_DEPTH:
            break
    return out


class BitunixAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Bitunix public market data from the website's market socket (wss://api.bitunix.com/ws-tide-batch/?from=trad), the
    only push feed Bitunix has (its API's WS is signed request/response) and P1's book source since 2026-09-26.

      depth   spot_<symbol lowercase>_depth_<precisions[0]>: the FULL 50-level book of a market at a fixed ~510 ms,
              changed or not; measured from myserver 2026-10-07: 1.83-1.98 pushes/s, 6-7 ms after the frame's server
              ts. The sub reply carries every channel's current book at once, so there is no book-less start.
      ticker  spot_simple_market_<symbol>: close + 24h amount ~1/s, the last traded price (there is no trade channel).

    Every push IS the book: each becomes a top-EMIT_DEPTH SNAPSHOT handed to the tracker inside the WebSocket reader
    (XT's latency path, runbook §1.4); only snapshots ever reach the tracker.

    What the feed does not give, and what stands in for it:
      - No sequence id, and no error for a channel that never streams (an unknown symbol or a wrong step). But every
        subscribed market pushes about twice a second whether or not it changed, so silence is the signal: a market with
        no push for STALE_MARKET_SECONDS is shown EMPTY and re-subscribed; a socket silent for WS_MESSAGE_TIMEOUT is
        reconnected. REST is never a freshness reference: the stream leads it (REST older by p50 832 ms, live).
      - A crossed push: the market is shown empty until a valid one.
      - The trading switch: coin_pair/list `isOpen`. While a tracked market is not open, the tracker gets an EMPTY book
        (a halt, the XT rule, 2026-09-26); the symbol, the trading rule and the held book stay.
    The stream is the book: while no connection is live every tracked book is shown EMPTY.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "BitunixExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "BitunixExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _Book] = {}                  # exchange symbol (BTCUSDT) -> book
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._channel_symbol: Dict[str, Tuple[str, str]] = {}   # channel -> (exchange symbol, "depth" | "ticker")
        self._last_prices: Dict[str, Tuple[float, float]] = {}  # exchange symbol -> (close, monotonic time)
        self._diff_output: Optional[asyncio.Queue] = None
        self._stream_live: bool = False
        self._switched_off: Set[str] = set()
        self._switch_known: Set[str] = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        self._warned: Set[str] = set()
        self._backoff = web_utils.BitunixReconnectBackoff()
        self._ping_task: Optional[asyncio.Task] = None
        self._last_update_id: int = 0
        self._subscribed_at: Dict[str, float] = {}         # exchange symbol -> monotonic time of its (re)subscribe

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

    def _is_tracked(self, symbol: str) -> bool:
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        return trading_pair is not None and trading_pair in self._trading_pairs

    async def _tracked_symbols(self) -> List[str]:
        symbols = []
        for trading_pair in list(self._trading_pairs):
            try:
                symbols.append(await self._symbol_for_pair(trading_pair))
            except KeyError:
                continue
        return symbols

    def _channels(self, symbol: str) -> List[str]:
        """The market's depth and ticker channels; the depth channel needs the market's finest price step."""
        step = self._connector.depth_step(symbol)
        out = []
        if step:
            depth = CONSTANTS.WS_DEPTH_CHANNEL.format(symbol=symbol.lower(), step=step)
            self._channel_symbol[depth] = (symbol, "depth")
            out.append(depth)
        else:
            self._warn_once(f"no-step:{symbol}", f"Bitunix {symbol}: no price step known (not in coin_pair/list); its "
                                                 f"book can't be subscribed and stays empty.")
        ticker = CONSTANTS.WS_TICKER_CHANNEL.format(symbol=symbol.lower())
        self._channel_symbol[ticker] = (symbol, "ticker")
        out.append(ticker)
        return out

    def last_price(self, symbol: str, max_age: float = 30.0) -> Optional[float]:
        entry = self._last_prices.get(symbol)
        if entry is None or time.monotonic() - entry[1] > max_age:
            return None
        return entry[0]

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ------------------------------------------------------------------ messages to the tracker

    def _next_update_id(self) -> int:
        now_ms = int(time.time() * 1e3)
        self._last_update_id = max(self._last_update_id + 1, now_ms)
        return self._last_update_id

    @staticmethod
    def _message(trading_pair: str, update_id: int, bids: list, asks: list) -> OrderBookMessage:
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _Book) -> OrderBookMessage:
        if symbol in self._switched_off or book.source is None:
            return self._message(trading_pair, book.update_id or self._next_update_id(), [], [])
        return self._message(trading_pair, book.update_id, book.bids, book.asks)

    def _emit(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is None or trading_pair is None:
            return
        output.put_nowait(self._snapshot_message(trading_pair, symbol, self._book(symbol)))

    def _show_empty(self, symbol: str) -> None:
        output = self._diff_output
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if output is not None and trading_pair is not None:
            output.put_nowait(self._message(trading_pair, self._next_update_id(), [], []))

    def _store(self, book: _Book, bids: list, asks: list, source: str) -> None:
        book.bids = bids
        book.asks = asks
        book.update_id = self._next_update_id()
        book.source = source

    # ------------------------------------------------------------------ REST (bootstrap only)

    async def _rest_book(self, symbol: str) -> Tuple[list, list]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH, domain=self._domain),
            params={"symbol": symbol},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if not web_utils.is_ok(response):
            raise IOError(f"Error fetching Bitunix depth for {symbol}: {response}")
        data = response.get("data") or {}
        bids = [[str(r.get("price")), str(r.get("volume"))] for r in data.get("bids") or [] if isinstance(r, dict)
                and float(r.get("volume") or 0) > 0][:CONSTANTS.EMIT_DEPTH]
        asks = [[str(r.get("price")), str(r.get("volume"))] for r in data.get("asks") or [] if isinstance(r, dict)
                and float(r.get("volume") or 0) > 0][:CONSTANTS.EMIT_DEPTH]
        return bids, asks

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh. A book the stream already serves
        answers it directly (REST is older than the stream: applying it could roll the tracker back); REST only
        bootstraps a market the stream hasn't served yet, while the stream is live."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"Bitunix {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, self._next_update_id(), [], [])
        await self._ensure_switch_known(symbol)
        book = self._book(symbol)
        if book.source is None and self._stream_live:
            requested_at = time.monotonic()
            try:
                bids, asks = await self._rest_book(symbol)
            except Exception as e:
                self.logger().debug(f"Bitunix REST depth for {symbol} failed ({e}); the stream serves it.")
                bids, asks = [], []
            if (bids or asks) and book.pushed_at < requested_at and self._stream_live and not _crossed(bids, asks):
                self._store(book, bids, asks, "rest")
        # On a runtime add the tracker replays pushes it parked meanwhile as DIFFS over this snapshot, which would merge
        # two whole books: the same book, emitted again shortly, replaces the merge (Hotcoin's lesson).
        asyncio.get_event_loop().call_later(0.5, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, book)

    def _reemit(self, symbol: str) -> None:
        if self._stream_live and self._book(symbol).source is not None:
            self._emit(symbol)

    # ------------------------------------------------------------------ Bitunix's trading switch

    async def _read_switches(self, max_age: float = 0.0) -> None:
        """coin_pair/list `isOpen` for every tracked market, from the connector's market_list (one read shared with the
        symbol map and the trading rules when younger than max_age s; it raises on a refused or empty list). A market
        missing from a non-empty answer is no longer listed and counts as off; an empty answer is not trusted."""
        response = await self._connector.market_list(max_age=max_age)
        listed = {f"{p.get('base')}{p.get('quote')}".upper(): p for p in response.get("data") or [] if isinstance(p, dict)}
        if not listed:
            raise IOError("Bitunix's market list came back empty")
        for symbol in await self._tracked_symbols():
            self._apply_switch(symbol, listed.get(symbol))

    async def _read_switches_safely(self) -> None:
        try:
            await self._read_switches()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"Bitunix trading-switch read failed: {e}")

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = info is None or str(info.get("isOpen")) != "1"
        if off == (symbol in self._switched_off):
            return
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = "not listed" if info is None else f"isOpen={info.get('isOpen')}"
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"Bitunix {trading_pair}: trading is switched off by Bitunix ({flags}); its book is "
                                  f"shown empty until Bitunix opens it again.")
            self._emit(symbol)
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"Bitunix {trading_pair}: trading is open again ({flags}); its book is shown again.")
            if self._book(symbol).source is not None:
                self._emit(symbol)

    async def _ensure_switch_known(self, symbol: str) -> None:
        if symbol in self._switch_known:
            return
        async with self._switch_lock:
            if symbol in self._switch_known:
                return
            try:
                await self._read_switches(max_age=CONSTANTS.MARKET_LIST_SHARE_SECONDS)
                if symbol not in self._switch_known:
                    self._switch_known.add(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once("switch-first-read",
                                f"Bitunix trading-switch read failed ({e}); markets count as open until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("Bitunix trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"Bitunix trading-switch read failed ({e}); every market keeps its last "
                                          f"state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ silence = death

    async def _stale_market_loop(self) -> None:
        """Every subscribed market pushes ~2/s, changed or not, so a market silent for STALE_MARKET_SECONDS (since its
        last push, or since it was subscribed) is shown EMPTY and re-subscribed on the live socket — at once the first
        three times, then at most every STALE_RESUB_BACKOFF s (a channel Bitunix never serves, e.g. a changed price
        step, must not churn the socket). One market never reconnects the socket: a dead socket is caught by the
        receive timeout (WS_MESSAGE_TIMEOUT)."""
        while True:
            await asyncio.sleep(CONSTANTS.STALE_CHECK_INTERVAL)
            try:
                ws = self._active_ws
                if ws is None or not self._stream_live:
                    continue
                now = time.monotonic()
                for symbol in await self._tracked_symbols():
                    if symbol in self._switched_off:
                        continue
                    book = self._book(symbol)
                    subscribed = self._subscribed_at.get(symbol, 0.0)
                    last = max(book.pushed_at, subscribed)
                    if not last or now - last < CONSTANTS.STALE_MARKET_SECONDS:
                        continue
                    if book.stale_resubs >= 3 and now - subscribed < STALE_RESUB_BACKOFF:
                        continue
                    if book.source is not None:
                        self._show_empty(symbol)
                        book.clear()
                    book.stale_resubs += 1
                    if book.stale_resubs <= 3 or book.stale_resubs % 10 == 0:
                        silent = now - book.pushed_at if book.pushed_at else now - subscribed
                        self.logger().warning(
                            f"Bitunix {self._symbol_to_pair_cache.get(symbol, symbol)}: no book push for {silent:.1f} s "
                            f"(every market pushes ~2/s); shown empty and re-subscribed (attempt {book.stale_resubs}).")
                    await self._send(ws, "unsub", self._channels(symbol))
                    await self._send(ws, "sub", self._channels(symbol))
                    self._subscribed_at[symbol] = time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"Bitunix stale-market check skipped: {e}")

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _send(self, ws: WSAssistant, event: str, channels: List[str]) -> None:
        for i in range(0, len(channels), CONSTANTS.WS_CHANNELS_PER_SUB):
            batch = channels[i:i + CONSTANTS.WS_CHANNELS_PER_SUB]
            await ws.send(WSJSONRequest(payload={"event": event, "channel": ",".join(batch)}))

    async def _ping_loop(self, ws: WSAssistant) -> None:
        while True:
            await asyncio.sleep(CONSTANTS.WS_PING_INTERVAL)
            await ws.send(WSJSONRequest(payload={"event": "ping", "ping": int(time.time())}))

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols: List[str] = []
            for trading_pair in list(self._trading_pairs):
                try:
                    symbols.append(await self._symbol_for_pair(trading_pair))
                except KeyError:
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"Bitunix {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
            channels: List[str] = []
            for symbol in symbols:
                self._book(symbol).clear()
                channels.extend(self._channels(symbol))
            self._stream_live = True
            now = time.monotonic()
            for symbol in symbols:
                self._subscribed_at[symbol] = now
            self._backoff.connected()
            if channels:
                await self._send(ws, "sub", channels)
            self._ping_task = safe_ensure_future(self._ping_loop(ws))
            self.logger().info(f"Subscribed to Bitunix depth and ticker channels for {len(symbols)} markets.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Bitunix public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add: subscribe the new market on the live socket (the base default disconnects the shared socket,
        which blanks every Bitunix book). The sub reply carries its book."""
        if ws is None:
            return
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).clear()
        await self._send(ws, "sub", self._channels(symbol))
        self._subscribed_at[symbol] = time.monotonic()
        self.logger().info(f"Subscribed Bitunix {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: show the book empty and re-subscribe this one
        market; the sub reply brings its book back. The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._show_empty(symbol)
        self._book(symbol).clear()
        ws = self._active_ws
        if ws is not None:
            await self._send(ws, "unsub", self._channels(symbol))
            await self._send(ws, "sub", self._channels(symbol))
            self._subscribed_at[symbol] = time.monotonic()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = safe_ensure_future(self._trading_switch_loop())
        stale_task = safe_ensure_future(self._stale_market_loop())
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()
            stale_task.cancel()

    @staticmethod
    def _decode(data: Any) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        if isinstance(data, str):
            return json.loads(data)
        return data

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        try:
            async for ws_response in websocket_assistant.iter_messages():
                try:
                    message = self._decode(ws_response.data)
                except Exception as e:
                    self._warn_once("undecodable", f"Bitunix public WS frame could not be decoded ({e}); skipped.")
                    continue
                if not isinstance(message, dict):
                    continue
                items = message.get("market_items")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            self._on_item(item)
                    continue
                event = message.get("event")
                if event not in ("pong", "connected", "sub", "unsub"):
                    self._warn_once(f"event:{event}", f"Bitunix public WS: {json.dumps(message)[:300]}")
        except asyncio.TimeoutError:
            raise ConnectionError(f"no message from Bitunix for {CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s "
                                  f"(every market pushes ~2/s)") from None

    def _on_item(self, item: Dict[str, Any]) -> None:
        """One channel's payload, synchronous inside the WebSocket reader."""
        target = self._channel_symbol.get(str(item.get("channel") or ""))
        if target is None:
            return
        symbol, kind = target
        if kind == "ticker":
            market = item.get("simpleMarket") or {}
            try:
                close = float(market.get("close"))
                if close > 0:
                    self._last_prices[symbol] = (close, time.monotonic())
            except (TypeError, ValueError):
                pass
            return
        if not self._is_tracked(symbol):
            return   # an unsubscribe in flight
        depth = item.get("depth") or {}
        bids, asks = _levels(depth.get("bids")), _levels(depth.get("asks"))
        book = self._book(symbol)
        if _crossed(bids, asks):
            self._warn_once(f"crossed:{symbol}", f"Bitunix {symbol}: a depth push came crossed (bid {bids[0][0]} >= "
                                                 f"ask {asks[0][0]}); the book is shown empty until a valid one.")
            if book.source is not None:
                self._show_empty(symbol)
                book.clear()
            return
        book.pushed_at = time.monotonic()
        book.stale_resubs = 0
        self._store(book, bids, asks, "ws")
        self._emit(symbol)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""   # books are applied in the reader (_on_item), never queued

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        if self._ping_task is not None and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = None
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        was_live, self._stream_live = self._stream_live, False
        if was_live:
            for trading_pair in list(self._trading_pairs):
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                if symbol is not None:
                    self._show_empty(symbol)
        for book in self._books.values():
            book.clear()
        await self._backoff.wait()

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        return

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        return   # no trade channel on Bitunix's socket (13 names tried, 2026-10-07)
