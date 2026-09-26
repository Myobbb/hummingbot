import asyncio
import time
from bisect import bisect_left, insort
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.xt.xt_exchange import XtExchange

# Internal queue item: a REST depth snapshot, routed through the diff queue so the one consumer of
# that queue applies snapshots and diffs strictly in arrival order.
_REST_SNAPSHOT = "__xt_rest_snapshot__"
# Diffs buffered while a book waits for its snapshot. At ~10 pushes/s that is ~2 minutes.
_MAX_BUFFERED_DIFFS = 1200
# Retries when a snapshot is older than the stream (its lastUpdateId + 1 < the first buffered fi).
_SNAPSHOT_RETRY_DELAY = 0.5
_SNAPSHOT_MAX_ATTEMPTS = 8
# A book that grew past this many levels on a side is trimmed back to the best half.
_MAX_LEVELS_PER_SIDE = 2000


def _update_side(levels: Dict[float, float], prices: List[float], updates: Iterable) -> None:
    # Absolute quantity per price; 0 deletes the level, and deleting an unknown level is normal.
    for price, qty in updates:
        p, q = float(price), float(qty)
        if q > 0:
            if p not in levels:
                insort(prices, p)
            levels[p] = q
        elif p in levels:
            del levels[p]
            del prices[bisect_left(prices, p)]


class _LocalBook:
    """
    One market's book as XT's 'Orderbook manage' page defines it: absolute quantity per level.

    Each side keeps a dict (price -> quantity) plus its prices in ascending order, so the top N is
    a slice. Rebuilding the top 50 from the whole book on every push (heapq over 500-2000 levels)
    was the largest cost per push (S7, 2026-09-23).
    """

    __slots__ = ("bids", "asks", "bid_prices", "ask_prices", "last_id", "synced", "buffer", "bootstrapping",
                 "attempts")

    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_prices: List[float] = []
        self.ask_prices: List[float] = []
        self.last_id: Optional[int] = None
        self.synced: bool = False
        self.buffer: List[Dict[str, Any]] = []
        self.bootstrapping: bool = False
        self.attempts: int = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.last_id = None
        self.synced = False
        self.buffer.clear()
        self.bootstrapping = False
        self.attempts = 0

    def load(self, bids: Iterable, asks: Iterable) -> None:
        self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
        self.bid_prices = sorted(self.bids)
        self.ask_prices = sorted(self.asks)

    def apply(self, event: Dict[str, Any]) -> None:
        _update_side(self.bids, self.bid_prices, event.get("b") or ())
        _update_side(self.asks, self.ask_prices, event.get("a") or ())
        if len(self.bid_prices) > _MAX_LEVELS_PER_SIDE:
            cut = len(self.bid_prices) - _MAX_LEVELS_PER_SIDE // 2
            for p in self.bid_prices[:cut]:
                del self.bids[p]
            del self.bid_prices[:cut]
        if len(self.ask_prices) > _MAX_LEVELS_PER_SIDE:
            keep = _MAX_LEVELS_PER_SIDE // 2
            for p in self.ask_prices[keep:]:
                del self.asks[p]
            del self.ask_prices[keep:]

    def top(self, depth: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Best `depth` levels per side, best first."""
        bids, asks = self.bids, self.asks
        return ([(p, bids[p]) for p in reversed(self.bid_prices[-depth:])],
                [(p, asks[p]) for p in self.ask_prices[:depth]])


class XtAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    XT public market data: `depth_update@{symbol}` (100 ms, sequenced diffs) + `trade@{symbol}`.

    The book is kept HERE, not in Hummingbot's tracker, following XT's documented procedure
    (WebSocket Public/OrderbookManage): buffer the diffs, fetch a REST snapshot, drop every event
    with `i <= lastUpdateId`, start from the event with `fi <= lastUpdateId + 1 <= i`, then require
    each event's `fi` to be the previous `i + 1`. Any break (a gap, or a crossed book) throws the
    book away and bootstraps it again. Verified live 2026-09-23: 0 gaps in ~430 events.

    Every applied update emits a top-EMIT_DEPTH SNAPSHOT message to the tracker, never a DIFF:
      - the tracker's snapshot path replays every diff in its window without checking update ids
        (restore_from_snapshot_and_diffs + OrderBookMessage.__lt__), and c_apply_diffs applies a diff
        unconditionally, so mixing XT snapshots with diffs there would re-apply stale levels;
      - c_apply_snapshot also stamps last_applied_diff, which the orchestrator's stale-book check
        reads, so a snapshot-only feed keeps that check honest;
      - HTX's mbp.refresh.20 already feeds a 100 ms snapshot stream through the same path.

    Speed: a depth push is applied and handed to the tracker inside the WebSocket reader, as soon as
    it is read. Queueing it for the diff listener task first cost one more event-loop hand-off,
    which under Hummingbot's 10 ms strategy ticks added up to ~3 ms at p99 (S7). REST snapshots still
    go through that queue: until one is applied the book only buffers, so arrival order is kept.

    A runtime add subscribes the new market on the live socket instead of disconnecting it, and
    `_refresh_snapshot_for_pair` gives the orchestrator a per-market repair that leaves every other
    book alone.

    XT's trading switch: a market XT switches off keeps its book two-sided (FUSD 2026-09-26:
    tradingEnabled=false, the frozen book still quoting against live venues). Every other venue's halt
    ends in an empty book or no symbol, which is what `status` ('<venue> empty order book') and arb_l's
    top-of-book gate react to. So while XT has a tracked market switched off, the tracker gets an EMPTY
    book for it. The rule is P1's (openapiEnabled + tradingEnabled) plus state ONLINE, read from
    GET /v4/public/symbol every TRADING_SWITCH_INTERVAL and before a market's first snapshot. The local
    book is still kept, and the market's symbol and trading rule stay (the B2 KeyError), so switching
    back on shows the book at once.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "XtExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "XtExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._books: Dict[str, _LocalBook] = {}
        # Read by the orchestrator's per-market stale-book recovery (method 1), with
        # _refresh_snapshot_for_pair.
        self._pair_to_symbol_cache: Dict[str, str] = {}
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._snapshot_tasks: Dict[str, asyncio.Task] = {}
        self._ping_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0
        self._warned: set = set()
        # The tracker's diff stream, known once listen_for_order_book_diffs runs; depth pushes are
        # delivered straight into it from the WebSocket reader.
        self._diff_output: Optional[asyncio.Queue] = None
        # XT's trading switch: symbols XT has switched off (shown empty), and symbols read at least once.
        self._switched_off: set = set()
        self._switch_known: set = set()
        self._switch_lock = asyncio.Lock()
        self._switch_failing: bool = False

    # ------------------------------------------------------------------ helpers

    def _request_id(self) -> str:
        self._next_request_id += 1
        return str(self._next_request_id)

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

    # ------------------------------------------------------------------ REST snapshot

    async def _request_depth(self, symbol: str) -> Dict[str, Any]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH, domain=self._domain),
            params={"symbol": symbol, "limit": CONSTANTS.SNAPSHOT_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH,
        )
        if web_utils.is_error_response(response):
            raise IOError(f"Error fetching XT depth for {symbol}: {response}")
        return response["result"]

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Snapshot for the tracker's initial book and its hourly refresh. A live (synced) local book
        answers it directly: it is newer than any REST answer, and a REST snapshot applied after it
        would roll the tracker's book back until the next push. Otherwise REST, and the same result
        seeds the local book, so the startup does not fetch every market twice."""
        try:
            symbol = await self._symbol_for_pair(trading_pair)
        except KeyError:
            raise ValueError(f"XT {trading_pair} has no entry in the connector's symbol map; "
                             f"no depth snapshot was requested") from None
        book = self._book(symbol)
        await self._ensure_switch_known(symbol)
        if book.synced:
            return self._snapshot_message(trading_pair, symbol, book)
        result = await self._request_depth(symbol)
        if not book.synced:
            self._message_queue[self._diff_messages_queue_key].put_nowait(
                {"topic": _REST_SNAPSHOT, "s": symbol, "result": result})
        update_id = int(result["lastUpdateId"])
        off = symbol in self._switched_off
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [] if off else (result.get("bids") or []),
                "asks": [] if off else (result.get("asks") or []),
            },
            timestamp=float(result.get("timestamp") or time.time() * 1e3) * 1e-3,
        )

    def _ensure_bootstrap(self, symbol: str, delay: float = 0.0) -> None:
        book = self._book(symbol)
        if book.bootstrapping or book.synced:
            return
        book.bootstrapping = True
        self._snapshot_tasks[symbol] = asyncio.ensure_future(self._fetch_snapshot(symbol, delay))

    async def _fetch_snapshot(self, symbol: str, delay: float) -> None:
        try:
            if delay:
                await asyncio.sleep(delay)
            result = await self._request_depth(symbol)
            self._message_queue[self._diff_messages_queue_key].put_nowait(
                {"topic": _REST_SNAPSHOT, "s": symbol, "result": result})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            book = self._book(symbol)
            book.bootstrapping = False
            self.logger().warning(f"XT depth snapshot for {symbol} failed ({e}); retrying in 2 s.")
            self._ensure_bootstrap(symbol, delay=2.0)

    # ------------------------------------------------------------------ XT's trading switch

    @staticmethod
    def _is_tradable(info: Optional[Dict[str, Any]]) -> bool:
        # P1's rule (groups_updater keeps a market only with openapiEnabled + tradingEnabled) plus state.
        return (info is not None and info.get("state") == "ONLINE"
                and info.get("tradingEnabled") is True and info.get("openapiEnabled") is True)

    async def _tracked_symbols(self) -> List[str]:
        symbols = []
        for trading_pair in list(self._trading_pairs):
            try:
                symbols.append(await self._symbol_for_pair(trading_pair))
            except KeyError:
                continue  # unmapped: nothing is subscribed for it either
        return symbols

    async def _read_switches(self, symbols: List[str]) -> None:
        """Read XT's switch for these markets and apply it. A market missing from the answer is no longer
        listed (XT drops a delisted market), so it counts as off.

        Live 2026-09-26: `symbols=` naming only unknown markets returns XT's WHOLE list (~1.2 MB), and
        `symbol=` answers an unknown market with an empty list. So a lone market is asked with `symbol=`,
        and a multi-market answer that lists none of them is not trusted (one bad reply must not empty
        every tracked book)."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        for i in range(0, len(symbols), CONSTANTS.TRADING_SWITCH_BATCH):
            batch = symbols[i:i + CONSTANTS.TRADING_SWITCH_BATCH]
            response = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(CONSTANTS.SYMBOL_PATH, domain=self._domain),
                params={"symbol": batch[0]} if len(batch) == 1 else {"symbols": ",".join(batch)},
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.SYMBOL_PATH,
            )
            if web_utils.is_error_response(response):
                raise IOError(f"Error reading XT's trading switch: {response}")
            listed = {s.get("symbol"): s for s in (response.get("result") or {}).get("symbols") or []
                      if isinstance(s, dict)}
            if len(batch) > 1 and not any(symbol in listed for symbol in batch):
                raise IOError(f"XT's trading switch lists none of {batch[:3]}{'...' if len(batch) > 3 else ''}")
            for symbol in batch:
                self._apply_switch(symbol, listed.get(symbol))

    def _apply_switch(self, symbol: str, info: Optional[Dict[str, Any]]) -> None:
        self._switch_known.add(symbol)
        off = not self._is_tradable(info)
        if off == (symbol in self._switched_off):
            return
        pair = self._symbol_to_pair_cache.get(symbol, symbol)
        flags = ("not listed" if info is None else
                 f"state={info.get('state')} tradingEnabled={info.get('tradingEnabled')} "
                 f"openapiEnabled={info.get('openapiEnabled')}")
        book = self._book(symbol)
        output = self._diff_output
        if off:
            self._switched_off.add(symbol)
            self.logger().warning(f"XT {pair}: trading is switched off by XT ({flags}); its book is shown "
                                  f"empty until XT switches it back on.")
            if output is not None and symbol in self._symbol_to_pair_cache:
                output.put_nowait(self._message(pair, book.last_id or 0, [], []))
        else:
            self._switched_off.discard(symbol)
            self.logger().info(f"XT {pair}: trading is back on ({flags}); its book is shown again.")
            if book.synced and output is not None and symbol in self._symbol_to_pair_cache:
                self._emit(symbol, book, output)
            else:
                self._ensure_bootstrap(symbol)

    async def _ensure_switch_known(self, symbol: str) -> None:
        """Before a market's first snapshot reaches the tracker, so a market that is already switched off
        never shows its frozen book. One request covers every tracked market not read yet."""
        if symbol in self._switch_known:
            return
        async with self._switch_lock:
            if symbol in self._switch_known:
                return
            try:
                symbols = sorted(({symbol} | set(await self._tracked_symbols())) - self._switch_known)
                await self._read_switches(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_once("switch-first-read",
                                f"XT trading-switch read failed ({e}); markets count as on until the next read "
                                f"(every {CONSTANTS.TRADING_SWITCH_INTERVAL} s).")

    async def _trading_switch_loop(self) -> None:
        while True:
            try:
                symbols = await self._tracked_symbols()
                if symbols:
                    await self._read_switches(symbols)
                if self._switch_failing:
                    self._switch_failing = False
                    self.logger().info("XT trading-switch reads work again.")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._switch_failing:
                    self._switch_failing = True
                    self.logger().warning(f"XT trading-switch read failed ({e}); every market keeps its last "
                                          f"state. Retrying every {CONSTANTS.TRADING_SWITCH_INTERVAL} s.")
            await asyncio.sleep(CONSTANTS.TRADING_SWITCH_INTERVAL)

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_PUBLIC_URL)

    @staticmethod
    def _topics(symbol: str) -> List[str]:
        return [f"{CONSTANTS.WS_DEPTH_UPDATE_TOPIC}@{symbol}", f"{CONSTANTS.WS_TRADE_TOPIC}@{symbol}"]

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            topics: List[str] = []
            subscribed = 0
            for trading_pair in self._trading_pairs:
                try:
                    symbol = await self._symbol_for_pair(trading_pair)
                except KeyError:
                    # One unknown pair must not fail the whole subscription: the reconnect loop would
                    # hit it again every time and leave every XT book empty.
                    self._warn_once(f"unmapped:{trading_pair}",
                                    f"XT {trading_pair} has no entry in the connector's symbol map; its market data "
                                    f"is not subscribed. The other markets are.")
                    continue
                self._book(symbol).reset()
                topics.extend(self._topics(symbol))
                subscribed += 1
            for i in range(0, len(topics), CONSTANTS.WS_TOPICS_PER_REQUEST):
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_METHOD_SUBSCRIBE,
                    "params": topics[i:i + CONSTANTS.WS_TOPICS_PER_REQUEST],
                    "id": self._request_id(),
                }))
            self.logger().info(f"Subscribed to XT depth_update and trade channels for {subscribed} markets.")
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to XT public channels...")
            raise

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str) -> None:
        """Runtime add (control create / add_market): subscribe the new market on the live socket.
        The base default disconnects the shared socket, which blanks every XT book for a reconnect."""
        if ws is None:
            return  # the next connection subscribes every tracked pair, this one included
        symbol = await self._symbol_for_pair(trading_pair)
        self._book(symbol).reset()
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_SUBSCRIBE,
            "params": self._topics(symbol),
            "id": self._request_id(),
        }))
        self.logger().info(f"Subscribed XT {trading_pair} on the live connection (no reconnect).")

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str) -> None:
        """Per-market repair for the orchestrator's stale-book recovery: rebuild this one book from
        a fresh snapshot. The socket and every other book are untouched."""
        self._pair_to_symbol_cache[trading_pair] = symbol
        self._symbol_to_pair_cache[symbol] = trading_pair
        book = self._book(symbol)
        task = self._snapshot_tasks.pop(symbol, None)
        if task is not None and not task.done():
            task.cancel()
        book.reset()
        self._ensure_bootstrap(symbol)

    async def _ping_loop(self, ws: WSAssistant) -> None:
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                await ws.send(WSPlainTextRequest(payload="ping"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"XT public WS keepalive stopped: {e}")

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        switch_task = asyncio.ensure_future(self._trading_switch_loop())
        try:
            await super().listen_for_order_book_diffs(ev_loop, output)
        finally:
            switch_task.cancel()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        diff_key = self._diff_messages_queue_key
        diff_queue = self._message_queue[diff_key]
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if not isinstance(data, dict):
                continue  # the text "pong" reply
            channel = self._channel_originating_message(event_message=data)
            if channel == diff_key:
                # Applied here, now. The queue is used only while it holds something (a REST snapshot,
                # or a push that came before the listener started), so the arrival order is kept.
                output = self._diff_output
                if (output is not None and diff_queue.empty()
                        and (data.get("data") or {}).get("s") in self._symbol_to_pair_cache):
                    self._on_depth_update(data, output)
                else:
                    diff_queue.put_nowait(data)
            elif channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(data)
            else:
                await self._process_message_for_unknown_channel(event_message=data, websocket_assistant=websocket_assistant)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        topic = event_message.get("topic")
        if topic == CONSTANTS.WS_DEPTH_UPDATE_TOPIC:
            return self._diff_messages_queue_key
        if topic == CONSTANTS.WS_TRADE_TOPIC:
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant) -> None:
        # Acks look like {"id":"1","code":0,"msg":"SUCCESS","method":"subscribe"} (live 2026-09-23).
        # A failed ack is logged, not raised: re-raising would reconnect and re-send the same topics.
        if "code" in event_message and "topic" not in event_message:
            if event_message.get("code") != 0:
                self.logger().warning(f"XT public WS request {event_message.get('id')} failed: {event_message}")
            return
        self.logger().debug(f"Unrecognised XT public WS message: {event_message}")

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        for task in self._snapshot_tasks.values():
            if not task.done():
                task.cancel()
        self._snapshot_tasks.clear()
        for book in self._books.values():
            book.reset()  # the next connection re-bootstraps every book from scratch

    # ------------------------------------------------------------------ the local book

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        """The queued path (REST snapshots, and pushes that could not be applied on arrival)."""
        if raw_message.get("topic") == _REST_SNAPSHOT:
            symbol = raw_message["s"]
            if symbol not in self._symbol_to_pair_cache:
                await self._pair_for_symbol(symbol)
            self._apply_rest_snapshot(symbol, raw_message["result"], message_queue)
            return
        symbol = (raw_message.get("data") or {}).get("s")
        if not symbol:
            return
        if symbol not in self._symbol_to_pair_cache:
            await self._pair_for_symbol(symbol)
        self._on_depth_update(raw_message, message_queue)

    def _on_depth_update(self, raw_message: Dict[str, Any], output: asyncio.Queue) -> None:
        """One depth_update push, per the 'Orderbook manage' procedure. Synchronous: it runs inside
        the WebSocket reader. The symbol is already in _symbol_to_pair_cache."""
        data = raw_message.get("data") or {}
        symbol = data.get("s")
        if not symbol:
            return
        book = self._book(symbol)
        if not book.synced:
            book.buffer.append(data)
            if len(book.buffer) > _MAX_BUFFERED_DIFFS:
                del book.buffer[: len(book.buffer) - _MAX_BUFFERED_DIFFS]
            self._ensure_bootstrap(symbol)
            return

        fi, i = int(data["fi"]), int(data["i"])
        if i <= book.last_id:
            return  # already contained in the book
        if fi > book.last_id + 1:
            self.logger().warning(
                f"XT {symbol}: depth sequence gap (fi={fi}, expected {book.last_id + 1}); rebuilding the book.")
            book.reset()
            book.buffer.append(data)
            self._ensure_bootstrap(symbol)
            return
        book.apply(data)
        book.last_id = i
        self._emit(symbol, book, output)

    def _apply_rest_snapshot(self, symbol: str, result: Dict[str, Any], output: asyncio.Queue) -> None:
        book = self._book(symbol)
        if book.synced:
            return  # a late or duplicate snapshot for a book that is already live
        book.bootstrapping = False
        last_update_id = int(result["lastUpdateId"])
        pending = [d for d in book.buffer if int(d["i"]) > last_update_id]
        if pending and int(pending[0]["fi"]) > last_update_id + 1:
            # The snapshot predates the first event we hold; the gap between them is unknowable.
            book.buffer = pending
            book.attempts += 1
            if book.attempts >= _SNAPSHOT_MAX_ATTEMPTS:
                self._warn_once(f"stale-snapshot:{symbol}",
                                f"XT {symbol}: {book.attempts} depth snapshots in a row were older than the stream; still retrying.")
            self._ensure_bootstrap(symbol, delay=_SNAPSHOT_RETRY_DELAY)
            return

        book.load(result.get("bids") or [], result.get("asks") or [])
        book.last_id = last_update_id
        for event in pending:
            fi, i = int(event["fi"]), int(event["i"])
            if i <= book.last_id:
                continue
            if fi > book.last_id + 1:
                self.logger().warning(f"XT {symbol}: gap inside the buffered diffs; rebuilding the book.")
                book.reset()
                self._ensure_bootstrap(symbol)
                return
            book.apply(event)
            book.last_id = i
        book.buffer.clear()
        book.synced = True
        book.attempts = 0
        self._emit(symbol, book, output)

    @staticmethod
    def _message(trading_pair: str, update_id: int, bids: list, asks: list) -> OrderBookMessage:
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={"trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks},
            timestamp=time.time(),
        )

    def _snapshot_message(self, trading_pair: str, symbol: str, book: _LocalBook) -> OrderBookMessage:
        if symbol in self._switched_off:
            return self._message(trading_pair, book.last_id, [], [])
        bids, asks = book.top(CONSTANTS.EMIT_DEPTH)
        return self._message(trading_pair, book.last_id, bids, asks)

    def _emit(self, symbol: str, book: _LocalBook, output: asyncio.Queue) -> None:
        if symbol in self._switched_off:
            # Switched off by XT: the book is kept, the tracker sees it empty, like a halt elsewhere.
            output.put_nowait(self._message(self._symbol_to_pair_cache[symbol], book.last_id, [], []))
            return
        bids, asks = book.top(CONSTANTS.EMIT_DEPTH)
        if bids and asks and bids[0][0] >= asks[0][0]:
            # Cannot happen with an unbroken sequence; treat it as corruption and start over.
            self.logger().warning(f"XT {symbol}: crossed book (bid {bids[0][0]} >= ask {asks[0][0]}); rebuilding.")
            book.reset()
            self._ensure_bootstrap(symbol)
            return
        output.put_nowait(self._message(self._symbol_to_pair_cache[symbol], book.last_id, bids, asks))

    # ------------------------------------------------------------------ trades

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        # Live shape (2026-09-23): {"s","i"(string),"t","p","q","b"}; the docs' "oi"/"v" were absent.
        data = raw_message.get("data") or {}
        symbol = data.get("s")
        if not symbol:
            return
        trading_pair = await self._pair_for_symbol(symbol)
        timestamp_ms = int(data.get("t") or time.time() * 1e3)
        # b = buyer is maker, so the taker (the side that traded) sold.
        trade_type = TradeType.SELL if data.get("b") else TradeType.BUY
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": str(data.get("i")),
                "update_id": timestamp_ms,
                "price": data.get("p"),
                "amount": data.get("q"),
            },
            timestamp=timestamp_ms * 1e-3,
        ))
