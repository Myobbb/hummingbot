import asyncio
import json
import time
import zlib
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from hummingbot.connector.exchange.coinup import coinup_constants as CONSTANTS, coinup_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinup.coinup_exchange import CoinupExchange


# After three immediate re-subscribes, a silent market is re-subscribed at most this often (s).
SILENT_RESUB_BACKOFF = 60.0
CHANNEL_PREFIX = "market_"
CHANNEL_SUFFIX = "_depth_step0"


class _Book:
    """What the data source holds for one market: its last book (top EMIT_DEPTH levels a side, [price, qty] strings)
    and when it came."""

    __slots__ = ("bids", "asks", "update_id", "live", "pushed_at", "silent_resubs")

    def __init__(self) -> None:
        self.bids: List[List[str]] = []
        self.asks: List[List[str]] = []
        self.update_id: int = 0
        self.live: bool = False          # a fresh push has been stored since the last (re)subscribe or clear
        self.pushed_at: float = 0.0      # monotonic time of the last push for the market (fresh or stale)
        self.silent_resubs: int = 0      # re-subscribes for silence since the last push

    def clear(self) -> None:
        self.bids = []
        self.asks = []
        self.live = False


def _crossed(bids: list, asks: list) -> bool:
    return bool(bids and asks and float(bids[0][0]) >= float(asks[0][0]))


def _levels(rows: Any) -> List[List[str]]:
    """CoinUp's [[price, qty], ...] (JSON numbers) -> [price, qty] strings, best first, zero sizes dropped."""
    out: List[List[str]] = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        try:
            if float(row[1]) <= 0 or float(row[0]) <= 0:
                continue
        except (TypeError, ValueError):
            continue
        out.append([str(row[0]), str(row[1])])
        if len(out) >= CONSTANTS.EMIT_DEPTH:
            break
    return out


class CoinupAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    CoinUp public market data from the kline-api socket (wss://ws.marketketac.com/kline-api/ws — the site's own wsUrl;
    ws.coinup.io answers Cloudflare's 403 at the handshake, this host is NOT challenged): market_<wire>_depth_step0, the
    FULL book in every push (gzip binary frames), on a fixed ~0.75 s server tick for every market, changed or not (P1,
    2026-11-27: 1.34-1.38 pushes/s per market). Wire = <base_coin>.lower() + "usdt", with WIRE_OVERRIDES for the one
    USDC-quoted market.

    Every push IS the book: each becomes a top-EMIT_DEPTH SNAPSHOT handed to the tracker inside the WebSocket reader
    (XT's latency path, runbook §1.4); only snapshots ever reach the tracker.

    What the feed does not give, and what stands in for it:
      - No sequence id, no ACK, and an unknown market is silently ignored. But every subscribed market pushes ~1.35/s,
        so silence is the signal: a market with no push for MARKET_SILENCE_SECONDS is shown EMPTY and re-subscribed on
        the live socket; a socket with markets and no frame for SOCKET_SILENCE_SECONDS is reconnected. The server sends
        {"ping": ms} every ~10 s; the reader answers {"pong": ms}.
      - Freshness from the push's own `ts` (the send time): a push more than STALE_PUSH_SECONDS behind CoinUp's clock is
        not shown and its market is shown empty until a fresh one.
      - REST is never a book source: /sapi/v1/depth equals the socket's book, but it sits behind the same Cloudflare
        challenge and adds nothing the socket lacks. Before its first fresh push a market's book is EMPTY.
      - A crossed push: the market is shown empty until a valid one.
      - The trading switch: get_allticker `isShow`. While a tracked market is hidden or no longer listed, the tracker gets
        an EMPTY book (a halt, the XT rule, 2026-09-26); the symbol, the trading rule and the held book stay.
    The stream is the book: while no connection is live every tracked book is shown EMPTY.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinupExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "CoinupExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _Book] = {}                  # exchange symbol (btcusdt) -> book
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._diff_output: Optional[asyncio.Queue] = None
        self._stream_live: bool = False
        self._switched_off: Set[str] = set()
        self._switch_known: Set[str] = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False
        self._warned: Set[str] = set()
        self._backoff = web_utils.CoinupReconnectBackoff()
        self._last_update_id: int = 0
        self._subscribed_at: Dict[str, float] = {}          # exchange symbol -> monotonic time of its (re)subscribe
        self._last_frame_at: float = 0.0                    # monotonic time of the socket's last frame
        self.stale_pushes: int = 0
        self.logger().info("[CU-BOOT] data source constructed for %d pair(s)", len(trading_pairs))

    # ------------------------------------------------------------------ helpers

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.logger().warning(message)

    async def _symbol_for_pair(self, trading_pair: str) -> str:
        """The wire is derived from the pair (P1's rule: `symbol == base.lower() + "usdt"` on all 317 USDT markets,
        with WIRE_OVERRIDES for the one USDC-quoted market). No REST at connect: the bulk market list is a PUBLIC route
        behind Cloudflare's challenge, and blocking the socket's first subscribe on it left the book empty (LEO,
        2026-11-28). The trading-rule map still comes from the REST when it can."""
        symbol = self._pair_to_symbol_cache.get(trading_pair)
        if symbol is None:
            base, quote = trading_pair.split("-")
            symbol = CONSTANTS.WIRE_OVERRIDES.get(base.upper()) or f"{base}{quote}".lower()
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

    @staticmethod
    def _channel(symbol: str) -> str:
        return CONSTANTS.WS_DEPTH_CHANNEL.format(symbol=symbol)

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
        if symbol in self._switched_off or not book.live or not self._stream_live:
            return self._message(trading_pair, self._next_update_id(), [], [])
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

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """The tracker's initial book, a runtime add's book and the hourly refresh: the stream's book, or an EMPTY one
        before the market's first fresh push. Never REST: /sapi/v1/depth equals the socket's book, so a REST round-trip
        behind Cloudflare would add nothing the stream lacks."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            self._warn_once(f"unmapped-snapshot:{trading_pair}",
                            f"CoinUp {trading_pair} has no entry in the connector's symbol map; its book stays empty.")
            return self._message(trading_pair, self._next_update_id(), [], [])
        await self._ensure_switch_known(symbol)
        # On a runtime add the tracker replays pushes it parked meanwhile as DIFFS over this snapshot, which would merge
        # two whole books: the same book, emitted again shortly, replaces the merge (Hotcoin's lesson).
        asyncio.get_event_loop().call_later(0.5, self._reemit, symbol)
        return self._snapshot_message(trading_pair, symbol, self._book(symbol))

    def _reemit(self, symbol: str) -> None:
        """Always: the snapshot the tracker built may have parked pushes merged into it, and only a whole book (or an
        empty one when the market has none to show) replaces the merge (review 2026-10-09)."""
        self._emit(symbol)

    # ------------------------------------------------------------------ CoinUp's trading switch

    async def _read_switches(self, max_age: float = 0.0) -> None:
        """get_allticker `isShow` for every tracked market (the connector's all_tickers: one read shared with the last
        prices when younger than max_age s; it raises on a refused or empty answer). A market missing from a non-empty
        answer is no longer listed and counts as off."""
        listed = await self._connector.all_tickers(max_age=max_age)
        if not listed:
            raise IOError("CoinUp's ticker list came back empty")
        for symbol in await self._tracked_symbols():
            self._apply_switch(symbol, listed.get(symbol))

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = info is None or str(info.get("isShow")) != "1"
        if off == (symbol in self._switched_off):
            return
        trading_pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = "not listed" if info is None else f"isShow={info.get('isShow')}"
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"CoinUp {trading_pair}: hidden by CoinUp ({flags}); its book is shown empty until "
                                  f"CoinUp shows the market again.")
            self._emit(symbol)
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"CoinUp {trading_pair}: shown again ({flags}); its book is shown again.")
            if self._book(symbol).live:
                self._emit(symbol)

    async def _ensure_switch_known(self, symbol: str) -> None:
        if symbol in self._switch_known:
            return
        async with self._switch_lock:
            if symbol in self._switch_known:
                return
            try:
                await self._read_switches(max_age=CONSTANTS.ALL_TICKER_SHARE_SECONDS)
                self._switch_known.add(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once("switch-first-read",
                                f"CoinUp trading-switch read failed ({e}); markets count as shown until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL:.0f} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                await self._read_switches()
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("CoinUp trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"CoinUp trading-switch read failed ({e}); every market keeps its last state. "
                                          f"Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL:.0f} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ silence = a dead stream

    async def _silence_loop(self) -> None:
        """Every subscribed market pushes at least every ~11 s, changed or not. A market silent for
        MARKET_SILENCE_SECONDS (since its last push, or since it was subscribed) is shown EMPTY and re-subscribed on the
        live socket — at once the first three times, then at most every SILENT_RESUB_BACKOFF s (a market CoinUp doesn't
        serve must not churn the socket). A socket that carries markets and has had no frame for
        SOCKET_SILENCE_SECONDS is closed, and the base reconnects it."""
        while True:
            await asyncio.sleep(CONSTANTS.STALE_CHECK_INTERVAL)
            try:
                ws = self._active_ws
                if ws is None or not self._stream_live:
                    continue
                now = time.monotonic()
                symbols = [s for s in await self._tracked_symbols() if s not in self._switched_off]
                if symbols and now - self._last_frame_at > CONSTANTS.SOCKET_SILENCE_SECONDS:
                    self.logger().warning(f"CoinUp market socket: no frame for {now - self._last_frame_at:.0f} s with "
                                          f"{len(symbols)} markets subscribed; reconnecting.")
                    connection = getattr(ws, "_connection", None)
                    if isinstance(connection, web_utils.CoinupWSConnection):
                        connection.close_now()
                    continue
                for symbol in symbols:
                    book = self._book(symbol)
                    subscribed = self._subscribed_at.get(symbol, 0.0)
                    last = max(book.pushed_at, subscribed)
                    if not last or now - last < CONSTANTS.MARKET_SILENCE_SECONDS:
                        continue
                    if book.silent_resubs >= 3 and now - subscribed < SILENT_RESUB_BACKOFF:
                        continue
                    if book.live:
                        self._show_empty(symbol)
                    book.clear()
                    book.silent_resubs += 1
                    if book.silent_resubs <= 3 or book.silent_resubs % 10 == 0:
                        silent = now - book.pushed_at if book.pushed_at else now - subscribed
                        self.logger().warning(
                            f"CoinUp {self._symbol_to_pair_cache.get(symbol, symbol)}: no book push for {silent:.0f} s "
                            f"(every market pushes at least every ~11 s); shown empty and re-subscribed "
                            f"(attempt {book.silent_resubs}).")
                    await self._send(ws, "unsub", symbol)
                    await self._send(ws, "sub", symbol)
                    self._subscribed_at[symbol] = time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"CoinUp silence check skipped: {e}")

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _send(self, ws: WSAssistant, event: str, symbol: str) -> None:
        """One channel per message (CoinUp's protocol); cb_id echoes the symbol."""
        await ws.send(WSJSONRequest(payload={"event": event,
                                             "params": {"channel": self._channel(symbol), "cb_id": symbol}}))

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        self.logger().info("[CU-BOOT] _subscribe_channels entered (%d pairs)", len(self._trading_pairs))
        try:
            symbols: List[str] = []
            for trading_pair in list(self._trading_pairs):
                try:
                    symbols.append(await self._symbol_for_pair(trading_pair))
                except KeyError:
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"CoinUp {trading_pair} has no entry in the connector's symbol map; its market "
                                    f"data is not subscribed. The other markets are.")
            for symbol in symbols:
                self._book(symbol).clear()
            self._stream_live = True
            self._last_frame_at = time.monotonic()
            self._backoff.connected()
            for symbol in symbols:
                await self._send(ws, "sub", symbol)
                self._subscribed_at[symbol] = time.monotonic()
                if CONSTANTS.WS_SUB_DELAY:
                    await asyncio.sleep(CONSTANTS.WS_SUB_DELAY)
            self.logger().info(f"Subscribed to CoinUp depth for {len(symbols)} markets.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinUp public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add: subscribe the new market on the live socket (the base default disconnects the shared socket,
        which blanks every CoinUp book). Its book is its first fresh push."""
        if ws is None:
            return
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).clear()
        await self._send(ws, "sub", symbol)
        self._subscribed_at[symbol] = time.monotonic()
        self.logger().info(f"Subscribed CoinUp {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: show the book empty and re-subscribe this one
        market; its next fresh push brings the book back. The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        self._show_empty(symbol)
        self._book(symbol).clear()
        ws = self._active_ws
        if ws is not None:
            await self._send(ws, "unsub", symbol)
            await self._send(ws, "sub", symbol)
            self._subscribed_at[symbol] = time.monotonic()

    async def listen_for_subscriptions(self) -> None:
        """[CU-BOOT] the tracker starts THIS loop; entry here proves the tracker started the data source."""
        self.logger().info("[CU-BOOT] listen_for_subscriptions entered")
        await super().listen_for_subscriptions()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = safe_ensure_future(self._trading_switch_loop())
        silence_task = safe_ensure_future(self._silence_loop())
        # ⚠️ The tracker starts `listen_for_subscriptions()` for the WS; this fork's `listen_for_order_book_diffs` is
        # only a queue reader. The GroveX clone put the whole socket in the latter, so nothing ever connected (found
        # live on LEO, 2026-11-28: "waiting for connector to initialize: coinup", zero data-source log lines).
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()
            silence_task.cancel()

    @staticmethod
    def _decode(data: Any) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = zlib.decompress(bytes(data), 31).decode("utf-8")   # gzip (every server frame, P1)
        if isinstance(data, str):
            return json.loads(data)
        return data

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            self._last_frame_at = time.monotonic()
            try:
                message = self._decode(ws_response.data)
            except Exception as e:
                self._warn_once("undecodable", f"CoinUp public WS frame could not be decoded ({e}); skipped.")
                continue
            if not isinstance(message, dict):
                continue
            if "ping" in message:                      # the server's heartbeat (~10 s): answered, never initiating one
                try:
                    await websocket_assistant.send(WSJSONRequest(payload={"pong": message.get("ping")}))
                except Exception:
                    pass
                continue
            channel = str(message.get("channel") or "")
            if isinstance(message.get("tick"), dict) and channel.startswith(CHANNEL_PREFIX) \
                    and channel.endswith(CHANNEL_SUFFIX):
                self._on_push(channel[len(CHANNEL_PREFIX):-len(CHANNEL_SUFFIX)], message)
                continue
            if message.get("event_rep") in ("subed", "unsubed") or message.get("status") == "ok":
                continue   # a subscription status (the docs show one; P1 never saw it)
            self._warn_once(f"frame:{channel or sorted(message)[:4]}",
                            f"CoinUp public WS: {json.dumps(message)[:300]}")

    def _server_now_ms(self) -> float:
        return self._connector.server_time_ms()

    def _on_push(self, symbol: str, message: Dict[str, Any]) -> None:
        """One depth push, synchronous inside the WebSocket reader."""
        if not self._is_tracked(symbol):
            return   # an unsubscribe in flight
        book = self._book(symbol)
        book.pushed_at = time.monotonic()
        book.silent_resubs = 0
        try:
            ts = float(message.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        if ts and self._server_now_ms() - ts > CONSTANTS.STALE_PUSH_SECONDS * 1e3:
            # A late push, or the cached snapshot a subscribe returns first: the book it carries is already stale.
            # Never shown; the book it would replace (older still) leaves too.
            self.stale_pushes += 1
            if book.live:
                self._show_empty(symbol)
                book.clear()
            return
        tick = message["tick"]
        bids, asks = _levels(tick.get("buys") or tick.get("bids")), _levels(tick.get("asks"))
        if _crossed(bids, asks):
            self._warn_once(f"crossed:{symbol}", f"CoinUp {symbol}: a depth push came crossed (bid {bids[0][0]} >= "
                                                 f"ask {asks[0][0]}); the book is shown empty until a valid one.")
            if book.live:
                self._show_empty(symbol)
                book.clear()
            return
        book.bids, book.asks = bids, asks
        book.update_id = self._next_update_id()
        book.live = True
        self._emit(symbol)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""   # books are applied in the reader (_on_push), never queued

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
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
        return   # the trade channel isn't used: arb_l and the PB price from books; last prices come from get_allticker
