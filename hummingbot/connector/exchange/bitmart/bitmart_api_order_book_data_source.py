import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitmart import (
    bitmart_constants as CONSTANTS,
    bitmart_utils as utils,
    bitmart_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitmart.bitmart_exchange import BitmartExchange


class BitmartAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None
    _PING_INTERVAL_SECONDS: float = 15.0           # < 20s per BitMart docs
    _FORCE_RECONNECT_IDLE_SECONDS: float = 30.0    # disconnect if no frames (including pong) for 30s
    _DEPTH_STALENESS_SECONDS: float = 60.0         # trigger per-pair snapshot refresh after 60s of silence
    _PERIODIC_SNAPSHOT_REFRESH_SECONDS: float = 180.0

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BitmartExchange',
                 api_factory: WebAssistantsFactory,
                 use_depth_increase: bool = CONSTANTS.USE_DEPTH_INCREASE,
                 subscribe_trades: bool = False,
                 seed_snapshot_via_request: bool = False):
        super().__init__(trading_pairs)
        self._connector: BitmartExchange = connector
        self._api_factory = api_factory
        self._use_depth_increase: bool = use_depth_increase
        # Reduce message volume: trades are optional for order book maintenance
        self._subscribe_trades: bool = subscribe_trades
        # Depth-Increase supports explicit WS snapshot request; default to off to reduce bursts on reconnect
        self._seed_snapshot_via_request: bool = seed_snapshot_via_request
        self._keepalive_task: Optional[asyncio.Task] = None
        self._depth_watchdog_task: Optional[asyncio.Task] = None
        self._reconnect_attempts: int = 0
        self._last_ping_sent_time: float = 0.0
        # Per-trading_pair last depth update time (unix seconds)
        self._last_depth_update_ts: Dict[str, float] = {}
        # Per-trading_pair last ANY message time (including heartbeats/snapshots)
        self._last_any_message_ts: Dict[str, float] = {}
        # Per-trading_pair last applied version (Depth-Increase only)
        self._last_depth_version: Dict[str, int] = {}
        # Flag to drop diffs when waiting for snapshot after version gap
        self._waiting_for_snapshot: Dict[str, bool] = {}
        # Track when we started waiting for snapshot (to detect timeout)
        self._waiting_for_snapshot_since: Dict[str, float] = {}
        # Per-trading_pair per-side last update time (to detect one-sided staleness)
        self._last_bids_update_ts: Dict[str, float] = {}
        self._last_asks_update_ts: Dict[str, float] = {}
        # Per-trading_pair last full snapshot refresh time
        self._last_snapshot_refresh_ts: Dict[str, float] = {}
        self._active_ws: Optional[WSAssistant] = None
        self._ws_consumer_task: Optional[asyncio.Task] = None
        self._watchdog_check_interval: float = 5.0  # Check watchdogs every 5s
        # Cache for symbol -> trading pair to avoid blocking async lookups in hot path
        self._symbol_to_pair_cache: Dict[str, str] = {}
        # Cache for trading pair -> symbol (for recovery operations)
        self._pair_to_symbol_cache: Dict[str, str] = {}

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        For Depth-Increase: process incremental updates with version sequencing.
        For depth50 (default), no diffs are produced (method will idle with empty queue).
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                diff_event = await message_queue.get()
                await self._parse_order_book_diff_message(raw_message=diff_event, message_queue=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Processes full snapshots (depth50 or Depth-Increase 'snapshot' messages).

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                snapshot_event = await message_queue.get()
                await self._parse_order_book_snapshot_message(raw_message=snapshot_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response["data"]
        snapshot_timestamp: float = int(snapshot_data["ts"]) * 1e-3
        update_id: int = int(snapshot_data["ts"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(bid[0], bid[1]) for bid in snapshot_data["bids"]],
            "asks": [(ask[0], ask[1]) for ask in snapshot_data["asks"]],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "size": 200
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.GET_ORDER_BOOK_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.GET_ORDER_BOOK_PATH_URL,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_updates = raw_message["data"]

        for trade_data in trade_updates:
            symbol = trade_data["symbol"]
            # Fast path: use cached symbol -> trading_pair mapping to avoid async lookup
            trading_pair = self._symbol_to_pair_cache.get(symbol)
            if trading_pair is None:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                self._symbol_to_pair_cache[symbol] = trading_pair
            ms_ts = int(trade_data["ms_t"]) if "ms_t" in trade_data else int(trade_data["s_t"]) * 1000
            message_content = {
                "trade_id": ms_ts,
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                    TradeType.SELL.value),
                "amount": trade_data["size"],
                "price": trade_data["price"]
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=ms_ts * 1e-3)

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Depth-Increase incremental updates only (depth50 has no diffs)
        if not isinstance(raw_message, dict):
            return
        if raw_message.get("table") != CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
            return
        diff_updates = raw_message.get("data") or []
        for diff_data in diff_updates:
            bids_list = diff_data.get("bids", [])
            asks_list = diff_data.get("asks", [])
            ms_t = diff_data.get("ms_t")
            version = diff_data.get("version")
            symbol = diff_data.get("symbol")
            if symbol is None or ms_t is None or version is None:
                continue

            trading_pair = self._symbol_to_pair_cache.get(symbol)
            if trading_pair is None:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                self._symbol_to_pair_cache[symbol] = trading_pair

            current_time = time.time()
            self._last_any_message_ts[trading_pair] = current_time

            # BitMart sends empty-body updates as keepalives — no book data, skip
            is_heartbeat = (not bids_list) and (not asks_list)
            if is_heartbeat:
                continue

            self._last_depth_update_ts[trading_pair] = current_time
            if bids_list:
                self._last_bids_update_ts[trading_pair] = current_time
            if asks_list:
                self._last_asks_update_ts[trading_pair] = current_time

            new_ver = int(version)

            # --- Strict version sequencing per BitMart Depth-Increase protocol ---
            # Docs: snapshot → apply update only if version == last+1
            #       version <= last  → discard (old/duplicate)
            #       version >  last+1 → discard + request new snapshot

            if self._waiting_for_snapshot.get(trading_pair, False):
                # Waiting for snapshot baseline — drop all diffs until it arrives.
                # No timeout escape: a stale snapshot is safer than a gappy diff.
                continue

            last_ver = self._last_depth_version.get(trading_pair)

            if last_ver is None:
                # No snapshot received yet — drop and (re-)request one
                if not self._waiting_for_snapshot.get(trading_pair, False):
                    self._waiting_for_snapshot[trading_pair] = True
                    self._waiting_for_snapshot_since[trading_pair] = current_time
                    asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                continue

            if new_ver <= last_ver:
                # Old or duplicate: discard (applying would remove still-valid levels)
                self.logger().debug(
                    f"BitMart {trading_pair}: discarding stale diff v{new_ver} (have v{last_ver})"
                )
                continue

            if new_ver != last_ver + 1:
                # Gap: we are missing at least one version — book would be corrupt if applied
                gap = new_ver - last_ver - 1
                self.logger().warning(
                    f"BitMart {trading_pair}: version gap (expected v{last_ver + 1}, got v{new_ver}, "
                    f"gap={gap}). Discarding diff and requesting snapshot."
                )
                self._waiting_for_snapshot[trading_pair] = True
                self._waiting_for_snapshot_since[trading_pair] = current_time
                self._last_depth_version.pop(trading_pair, None)
                asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                continue

            # Sequential update (new_ver == last_ver + 1) — safe to emit
            self._last_depth_version[trading_pair] = new_ver
            timestamp: float = int(ms_t) * 1e-3
            message_queue.put_nowait(OrderBookMessage(
                OrderBookMessageType.DIFF,
                {
                    "trading_pair": trading_pair,
                    "update_id": int(ms_t),
                    "bids": [(bid[0], bid[1]) for bid in bids_list],
                    "asks": [(ask[0], ask[1]) for ask in asks_list],
                },
                timestamp))

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        for diff_data in (raw_message.get("data") or []):
            ms_t = diff_data.get("ms_t")
            symbol = diff_data.get("symbol")
            if ms_t is None or symbol is None:
                continue

            trading_pair = self._symbol_to_pair_cache.get(symbol)
            if trading_pair is None:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                self._symbol_to_pair_cache[symbol] = trading_pair

            bids_list = diff_data.get("bids", [])
            asks_list = diff_data.get("asks", [])

            # Empty-body snapshots are keepalives — no book data
            if (not bids_list) and (not asks_list):
                self._last_any_message_ts[trading_pair] = time.time()
                continue

            current_time = time.time()
            self._last_any_message_ts[trading_pair] = current_time
            self._last_depth_update_ts[trading_pair] = current_time
            self._last_bids_update_ts[trading_pair] = current_time
            self._last_asks_update_ts[trading_pair] = current_time
            self._last_snapshot_refresh_ts[trading_pair] = current_time

            # For Depth-Increase WS snapshots: has a version field that anchors sequencing.
            # For REST fallback snapshots: no version field — we synthesise version from ms_t
            # so that diffs arriving after (with larger ms_t-derived update_ids) still sequence
            # correctly in restore_from_snapshot_and_diffs.
            if raw_message.get("table") == CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
                version_value = diff_data.get("version")
                was_waiting = self._waiting_for_snapshot.get(trading_pair, False)
                self._waiting_for_snapshot[trading_pair] = False
                self._waiting_for_snapshot_since.pop(trading_pair, None)

                if version_value is not None:
                    new_version = int(version_value)
                    old_version = self._last_depth_version.get(trading_pair)
                    self._last_depth_version[trading_pair] = new_version
                    self.logger().info(
                        f"BitMart {trading_pair}: snapshot v{new_version} received"
                        f"{f' (was waiting, prev v{old_version})' if was_waiting else f' (prev v{old_version})'}"
                        f" — {len(bids_list)} bids / {len(asks_list)} asks"
                    )
                else:
                    # REST fallback: no WS version. Use ms_t as the version anchor so
                    # restore_from_snapshot_and_diffs can bisect diffs correctly.
                    # Also keep _last_depth_version pointing at ms_t so subsequent WS
                    # diffs (which DO have versions) know to re-request a fresh WS snapshot.
                    old_version = self._last_depth_version.get(trading_pair)
                    self._last_depth_version.pop(trading_pair, None)
                    self.logger().info(
                        f"BitMart {trading_pair}: REST snapshot received (prev v{old_version})"
                        f" — {len(bids_list)} bids / {len(asks_list)} asks"
                        f" — waiting for next WS snapshot to resume version tracking"
                    )
                    # Stay in waiting-for-snapshot state so diffs are held until the
                    # next WS snapshot (which will carry a proper version).
                    self._waiting_for_snapshot[trading_pair] = True
                    self._waiting_for_snapshot_since[trading_pair] = current_time
                    asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))

            message_queue.put_nowait(OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                {
                    "trading_pair": trading_pair,
                    "update_id": int(ms_t),
                    "bids": [(bid[0], bid[1]) for bid in bids_list],
                    "asks": [(ask[0], ask[1]) for ask in asks_list],
                },
                int(ms_t) * 1e-3))

    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str):
        """
        Request a fresh snapshot for one pair.
        Always prefers WS 'request' op (returns versioned snapshot).
        Falls back to REST only if WS is unavailable; REST snapshots have no version
        so the caller must handle the versionless case.
        """
        try:
            if self._use_depth_increase and self._active_ws is not None:
                payload = {"op": "request", "args": [f"{CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME}:{symbol}"]}
                req = WSJSONRequest(payload=payload)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await self._active_ws.send(req)
                self.logger().debug(f"BitMart {trading_pair}: sent WS snapshot request")
                return
        except Exception as e:
            self.logger().warning(f"BitMart {trading_pair}: WS snapshot request failed ({e}), falling back to REST")

        try:
            snapshot = await self._request_order_book_snapshot(trading_pair)
            snap_data = snapshot.get("data") or {}
            ts = int(snap_data.get("ts", int(time.time() * 1000)))
            synthetic = {
                "table": (CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME
                          if self._use_depth_increase else CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME),
                "data": [{
                    "asks": snap_data.get("asks", []),
                    "bids": snap_data.get("bids", []),
                    "ms_t": ts,
                    "symbol": symbol,
                    "type": "snapshot",
                    # No "version" field — signals to _parse_order_book_snapshot_message
                    # that this is a versionless REST snapshot.
                }]
            }
            self._message_queue[self._snapshot_messages_queue_key].put_nowait(synthetic)
            self.logger().info(f"BitMart {trading_pair}: queued REST snapshot fallback")
        except Exception as e:
            self.logger().error(f"BitMart {trading_pair}: REST snapshot fallback failed: {e}")

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                symbols.append(symbol)
                # Pre-seed symbol caches to avoid async lookups in hot path
                self._symbol_to_pair_cache[symbol] = trading_pair
                self._pair_to_symbol_cache[trading_pair] = symbol

            # BitMart allows up to 20 topics per subscription message
            trade_topics = [f"{CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME}:{symbol}" for symbol in symbols] if self._subscribe_trades else []
            depth_channel = (CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME
                             if self._use_depth_increase else CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME)
            depth_topics = [f"{depth_channel}:{symbol}" for symbol in symbols]

            async def send_chunked(topics: List[str]):
                CHUNK_SIZE = 20
                for i in range(0, len(topics), CHUNK_SIZE):
                    chunk = topics[i:i + CHUNK_SIZE]
                    payload = {"op": "subscribe", "args": chunk}
                    req = WSJSONRequest(payload=payload)
                    async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                        await ws.send(req)
                    # Pace messages to stay comfortably under 100 msgs/10s (ping texts count too)
                    await asyncio.sleep(0.12)

            await send_chunked(trade_topics)
            await send_chunked(depth_topics)

            self.logger().info("Subscribed to public order book and trade channels...")

            # After every (re)connect: all version state is invalid.
            # Mark each pair as waiting for its first versioned snapshot before
            # accepting any diffs — this is the correct Depth-Increase startup sequence.
            now = time.time()
            for tp in self._trading_pairs:
                self._last_any_message_ts[tp] = now
                self._last_depth_update_ts[tp] = now
                self._last_bids_update_ts[tp] = now
                self._last_asks_update_ts[tp] = now
                self._last_snapshot_refresh_ts[tp] = now
                self._last_depth_version.pop(tp, None)
                self._waiting_for_snapshot[tp] = True
                self._waiting_for_snapshot_since[tp] = now

            # Always proactively request WS snapshots for Depth-Increase.
            # BitMart sends an initial snapshot automatically on subscribe, but
            # requesting explicitly guarantees we get one even on reconnect bursts.
            if self._use_depth_increase:
                CHUNK_SIZE = 20
                for i in range(0, len(depth_topics), CHUNK_SIZE):
                    chunk = depth_topics[i:i + CHUNK_SIZE]
                    payload = {"op": "request", "args": chunk}
                    req = WSJSONRequest(payload=payload)
                    async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                        await ws.send(req)
                    await asyncio.sleep(0.12)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    async def _process_ws_messages_consumer(self, websocket_assistant: WSAssistant):
        """
        Consumer task that processes WebSocket messages.
        Runs continuously until socket closes or error occurs.
        """
        try:
            async for ws_response in websocket_assistant.iter_messages():
                data: Dict[str, Any] = ws_response.data
                decompressed_data = utils.decompress_ws_message(data)
                try:
                    if isinstance(decompressed_data, str):
                        # Gracefully ignore plain-text 'pong'
                        if decompressed_data.strip().lower() == "pong":
                            continue
                        json_data = json.loads(decompressed_data)
                    else:
                        json_data = decompressed_data
                except Exception:
                    # Ignore unparsable frames (e.g., raw pongs)
                    continue

                # Handle exchange error messages gracefully
                if isinstance(json_data, dict) and ("errorCode" in json_data or "errorMessage" in json_data):
                    # Convert to ConnectionError to trigger reconnect with backoff at higher level
                    self.logger().error(f"BitMart public WS error: {json_data}")
                    raise ConnectionError(f"BitMart WS error: {json_data}")

                channel: str = self._channel_originating_message(event_message=json_data)
                if channel in [self._diff_messages_queue_key, self._trade_messages_queue_key, self._snapshot_messages_queue_key]:
                    self._message_queue[channel].put_nowait(json_data)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "data" in event_message:
            event_channel = event_message.get("table")
            if event_channel == CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME:
                # depth50 sends full snapshots only
                channel = self._snapshot_messages_queue_key
            elif event_channel == CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
                # Route by message type
                try:
                    data = event_message.get("data") or []
                    msg_type = str(data[0].get("type", "")).lower() if data else ""
                    if msg_type == "snapshot":
                        channel = self._snapshot_messages_queue_key
                    elif msg_type == "update":
                        channel = self._diff_messages_queue_key
                    else:
                        channel = self._diff_messages_queue_key
                except Exception:
                    channel = self._diff_messages_queue_key

        return channel

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECT):
            # Disable protocol ping frames; use text 'ping' keepalive per BitMart docs
            await ws.connect(
                ws_url=CONSTANTS.WSS_PUBLIC_URL,
                ping_timeout=None,
                message_timeout=60,
                ws_headers={"Accept-Encoding": "gzip"},
            )
        self._active_ws = ws
        return ws

    async def _check_and_send_ping_if_needed(self, ws: WSAssistant):
        """Send ping if idle for >= interval"""
        last_recv = getattr(ws, "last_recv_time", 0) or 0
        now = time.time()
        if (now - last_recv) >= self._PING_INTERVAL_SECONDS and (now - self._last_ping_sent_time) >= self._PING_INTERVAL_SECONDS:
            try:
                await ws.send(WSPlainTextRequest(payload="ping"))
                self._last_ping_sent_time = now
                self.logger().debug("BitMart public WS: sent ping")
            except Exception as e:
                raise ConnectionError(f"BitMart public WS: failed to send ping: {e}")

    async def _check_watchdogs(self, ws: WSAssistant):
        """
        Check for connection health and stale orderbook data.
        Raises ConnectionError if reconnect is needed.
        """
        last_recv = getattr(ws, "last_recv_time", 0) or 0
        now = time.time()
        
        # Check for complete silence (no messages at all, including pongs)
        if last_recv > 0 and (now - last_recv) >= self._FORCE_RECONNECT_IDLE_SECONDS:
            self.logger().warning("BitMart public WS: no messages for 30s, forcing reconnect")
            raise ConnectionError("BitMart WS idle exceeded threshold; forcing reconnect")
        
        # Check for stale orderbook data per trading pair
        await self._check_stale_pairs(ws)

    async def _check_stale_pairs(self, ws: WSAssistant):
        """
        Detect symbols whose depth stream has stalled and take corrective action.
        Called periodically from main loop's watchdog check.
        
        Recovery strategy:
        - ALL pairs stale → subscription likely dead, force full WS reconnect
        - SOME pairs stale → per-pair snapshot refresh (non-disruptive)
        """
        now = time.time()
        stale_pairs = []
        total_initialized = 0
        
        # Check all trading pairs for staleness
        for trading_pair in list(self._trading_pairs):
            # Use _last_any_message_ts which includes heartbeats and snapshots
            last_any_ts = float(self._last_any_message_ts.get(trading_pair, 0.0) or 0.0)
            
            # Only consider pairs that have been initialized (have a timestamp)
            if last_any_ts > 0:
                total_initialized += 1
                if (now - last_any_ts) >= self._DEPTH_STALENESS_SECONDS:
                    stale_pairs.append(trading_pair)
        
        if not stale_pairs:
            return
        
        # ALL initialized pairs are stale → subscription-level failure
        # Force full reconnect to re-subscribe all channels
        if total_initialized > 0 and len(stale_pairs) >= total_initialized:
            oldest_stale = max(
                (now - float(self._last_any_message_ts.get(tp, 0.0) or 0.0))
                for tp in stale_pairs
            )
            self.logger().warning(
                f"BitMart: ALL {len(stale_pairs)} pair(s) stale "
                f"(oldest: {int(oldest_stale)}s). Subscription likely dead, forcing reconnect."
            )
            raise ConnectionError(
                f"BitMart WS: all {len(stale_pairs)} subscriptions stale, forcing reconnect"
            )
        
        # SOME pairs stale → attempt per-pair recovery via snapshot refresh
        for trading_pair in stale_pairs:
            try:
                # Fast path: use cached trading_pair -> symbol mapping
                symbol = self._pair_to_symbol_cache.get(trading_pair)
                if symbol is None:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    self._pair_to_symbol_cache[trading_pair] = symbol
                
                last_any_msg = self._last_any_message_ts.get(trading_pair, 0)
                stale_duration = int(now - last_any_msg) if last_any_msg > 0 else 0
                
                # Log warning but ONLY request snapshot (non-disruptive)
                self.logger().warning(
                    f"BitMart {trading_pair}: No messages for {stale_duration}s "
                    f"(threshold: {self._DEPTH_STALENESS_SECONDS}s), requesting snapshot to provoke exchange"
                )
                
                # Do NOT set _waiting_for_snapshot (would filter updates)
                # Just fire the request; the response (snapshot) will update timestamps and clear staleness
                await self._refresh_snapshot_for_pair(trading_pair, symbol)
                
                # Reset staleness timer temporarily to avoid spamming requests
                # If real data comes in, it will overwrite this
                self._last_any_message_ts[trading_pair] = now
                
            except Exception as e:
                self.logger().warning(f"BitMart depth watchdog: failed to recover {trading_pair}: {e}")


    async def listen_for_subscriptions(self):
        """
        Main WebSocket loop using Bybit-style architecture with asyncio.wait() and periodic watchdog checks.
        """
        ws: Optional[WSAssistant] = None
        while True:
            reconnect_delay = 1.0
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                self._last_ping_sent_time = time.time()
                self._active_ws = ws
                self._reconnect_attempts = 0
                
                # Create message consumer task
                if self._ws_consumer_task is None or self._ws_consumer_task.done():
                    self._ws_consumer_task = asyncio.create_task(self._process_ws_messages_consumer(ws))
                
                # Main event loop: multiplex between consumer and watchdog checks
                while True:
                    # Create a timer for next watchdog check
                    watchdog_timer = asyncio.create_task(asyncio.sleep(self._watchdog_check_interval))
                    
                    done, pending = await asyncio.wait(
                        {self._ws_consumer_task, watchdog_timer},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    
                    # Consumer finished -> check for exception and reconnect
                    if self._ws_consumer_task in done:
                        exc = None
                        try:
                            exc = self._ws_consumer_task.exception()
                        except asyncio.CancelledError:
                            raise
                        finally:
                            watchdog_timer.cancel()
                        if exc:
                            raise exc
                        raise ConnectionError("BitMart WS consumer ended unexpectedly")
                    
                    # Watchdog timer expired -> run checks
                    if watchdog_timer in done:
                        # Send ping if needed
                        await self._check_and_send_ping_if_needed(ws)
                        # Check for staleness and connection health
                        await self._check_watchdogs(ws)
                        
            except asyncio.CancelledError:
                raise
            except (ConnectionError, Exception) as e:
                if self.check_and_clear_reconnect_request():
                    # Disconnect was triggered by subscribe_to_trading_pair — expected, not an error
                    self._reconnect_attempts = 0
                    self.logger().info(
                        f"BitMart public WS reconnecting to subscribe new pair "
                        f"({len(self._trading_pairs)} pairs total)"
                    )
                    reconnect_delay = 0.0
                else:
                    # Inspect close codes to decide transient backoff
                    text = str(e)
                    code = None
                    try:
                        if "Close code" in text:
                            code = text.split("Close code =")[1].split()[0]
                    except Exception:
                        pass
                    is_transient = any(tok in text for tok in ["1000", "1001", "1005", "1006", "1012", "1013"])
                    if is_transient:
                        self.logger().warning(f"BitMart public WS transient close ({code or 'unknown'}). Reconnecting...")
                        reconnect_delay = 1.0
                    else:
                        self.logger().error("BitMart public WS error; reconnecting...", exc_info=True)
                        self._reconnect_attempts += 1
                        exponent = min(self._reconnect_attempts, 5)
                        reconnect_delay = float(min(30, 2 ** max(1, exponent)))
            finally:
                # Clean up websocket resources
                self._active_ws = None
                
                # CRITICAL: Clear ALL per-pair state to prevent issues on reconnect
                # BitMart may reset version numbering on new connection, so old versions are invalid
                self._last_depth_version.clear()
                self._waiting_for_snapshot.clear()
                self._waiting_for_snapshot_since.clear()
                
                # Clear timestamp trackers to prevent immediate staleness warnings after reconnect
                # All pairs will get fresh timestamps when new data arrives
                self._last_any_message_ts.clear()
                self._last_depth_update_ts.clear()
                self._last_bids_update_ts.clear()
                self._last_asks_update_ts.clear()
                
                # Cancel consumer task if still running
                if self._ws_consumer_task and not self._ws_consumer_task.done():
                    self._ws_consumer_task.cancel()
                    try:
                        await self._ws_consumer_task
                    except Exception:
                        pass
                self._ws_consumer_task = None
                
                # Wait before reconnecting
                await self._sleep(reconnect_delay)
                
                # Clean disconnect from old websocket
                if ws:
                    await ws.disconnect()