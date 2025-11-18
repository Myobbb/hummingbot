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
    _PING_INTERVAL_SECONDS: float = 15.0  # < 20s per BitMart docs
    _FORCE_RECONNECT_IDLE_SECONDS: float = 30.0  # Increased margin beyond BitMart's 20s threshold
    _DEPTH_STALENESS_SECONDS: float = 45.0  # per-symbol watchdog
    _PERIODIC_SNAPSHOT_REFRESH_SECONDS: float = 300.0  # Refresh snapshot every 5 minutes to prevent one-sided staleness

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
        # Maximum time to wait for snapshot before forcing recovery
        # Only blocks on initial connect; version gaps no longer block (optimistic mode)
        self._SNAPSHOT_WAIT_TIMEOUT_SECONDS: float = 10.0
        self._active_ws: Optional[WSAssistant] = None
        self._ws_consumer_task: Optional[asyncio.Task] = None
        self._watchdog_check_interval: float = 5.0  # Check watchdogs every 5s

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
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["symbol"])
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
        # Depth-Increase incremental updates
        if not isinstance(raw_message, dict):
            return
        event_table = raw_message.get("table")
        if event_table != CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
            # No diffs expected for depth50
            return
        diff_updates: Dict[str, Any] = raw_message.get("data") or []
        for diff_data in diff_updates:
            # Ignore empty heartbeats (asks=[], bids=[]) and invalid entries
            bids_list = diff_data.get("bids", [])
            asks_list = diff_data.get("asks", [])
            ms_t = diff_data.get("ms_t")
            version = diff_data.get("version")
            symbol = diff_data.get("symbol")
            if symbol is None or ms_t is None or version is None:
                continue
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            
            # Track ANY message received for this pair (proves subscription is alive)
            current_time = time.time()
            try:
                self._last_any_message_ts[trading_pair] = current_time
            except Exception:
                pass
            
            # Check if this is a heartbeat (both sides empty)
            is_heartbeat = (not bids_list) and (not asks_list)
            
            # Update data timestamp ONLY for non-heartbeat messages (actual orderbook changes)
            if not is_heartbeat:
                try:
                    self._last_depth_update_ts[trading_pair] = current_time
                    # Track per-side updates to detect one-sided staleness
                    if bids_list:
                        self._last_bids_update_ts[trading_pair] = current_time
                    if asks_list:
                        self._last_asks_update_ts[trading_pair] = current_time
                except Exception:
                    pass
            # Skip heartbeats early (no version check needed as they don't have actual data)
            if is_heartbeat:
                continue
                
            # Version sequencing for actual data updates
            last_ver = self._last_depth_version.get(trading_pair)
            new_ver = int(version)
            
            # Debug logging for version tracking and one-sided updates
            update_type = "two-sided"
            if not bids_list and asks_list:
                update_type = "asks-only"
            elif bids_list and not asks_list:
                update_type = "bids-only"
            
            # If explicitly waiting for snapshot (only happens on initial connect or manual refresh)
            # Note: With optimistic gap handling, we don't enter this state on version gaps anymore
            if self._waiting_for_snapshot.get(trading_pair, False):
                wait_start = self._waiting_for_snapshot_since.get(trading_pair, 0)
                wait_duration = time.time() - wait_start if wait_start > 0 else 0
                
                # OPTIMISTIC: Much shorter timeout (10s) since we only block on initial connect
                # After 10s, accept ANY update to keep orderbook moving
                if wait_duration >= 10.0:  # Reduced from 30s
                    # Timeout exceeded - force recovery by accepting this update as new baseline
                    #self.logger().warning(
                    #    f"BitMart {trading_pair}: Snapshot wait timeout ({int(wait_duration)}s). "
                    #    f"Forcing recovery: accepting {update_type} v{new_ver} as new baseline"
                    #)
                    self._waiting_for_snapshot[trading_pair] = False
                    self._waiting_for_snapshot_since.pop(trading_pair, None)
                    self._last_depth_version[trading_pair] = new_ver
                    # Continue processing this update below
                else:
                    # Still waiting (only in first 10s after connect)
                    self.logger().debug(
                        f"BitMart {trading_pair}: Waiting for initial snapshot "
                        f"({int(wait_duration)}s elapsed, will accept updates after 10s) - dropping {update_type} v{new_ver}"
                    )
                    continue
            
            # Version validation and gap detection
            # CORE PRINCIPLE: Prefer orderbook updates over perfect version tracking
            # Better to have slightly gappy data than NO data
            skip_version_checks = False
            
            if last_ver is None:
                # No baseline version - this can happen after:
                # 1. Initial connection (haven't received first snapshot yet)
                # 2. REST snapshot fallback (REST snapshots don't have versions)
                # 
                # OPTIMISTIC STRATEGY: Accept ANY update as baseline after reasonable timeout
                last_snapshot_ts = self._last_snapshot_refresh_ts.get(trading_pair, 0)
                time_since_init = current_time - last_snapshot_ts if last_snapshot_ts > 0 else 999999
                
                # If we have ANY recent snapshot OR have been waiting >10s, accept this as baseline
                # 10s gives time for initial snapshot, but doesn't block updates forever
                should_establish_baseline = (
                    time_since_init < 30.0  # Recent snapshot (within 30s)
                    or time_since_init > 999998  # Never had snapshot (first update ever)
                    or (current_time - self._last_any_message_ts.get(trading_pair, 0)) > 10.0  # Been receiving messages >10s
                )
                
                if should_establish_baseline:
                    # Establish this update as baseline - ALWAYS prefer data over blocking
                    elapsed_msg = f" {int(time_since_init)}s after snapshot" if time_since_init < 1000 else " (first update)"
                    self.logger().info(
                        f"BitMart {trading_pair}: Establishing version baseline v{new_ver}{elapsed_msg} "
                        f"({update_type} update)"
                    )
                    self._last_depth_version[trading_pair] = new_ver
                    skip_version_checks = True  # Skip remaining version checks for this update
                    # Trigger background snapshot request for proper baseline, but don't wait for it
                    if not self._waiting_for_snapshot.get(trading_pair, False):
                        asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                else:
                    # Only block if we literally just started (<10s) and haven't received any messages
                    self.logger().debug(
                        f"BitMart {trading_pair}: No baseline version, requesting snapshot for {update_type} update (v{new_ver})"
                    )
                    self._waiting_for_snapshot[trading_pair] = True
                    self._waiting_for_snapshot_since[trading_pair] = current_time
                    await self._refresh_snapshot_for_pair(trading_pair, symbol)
                    continue
            
            if not skip_version_checks and new_ver <= last_ver:
                # Old or duplicate - this is CORRECT to skip
                self.logger().debug(f"BitMart {trading_pair}: Skipping old/duplicate {update_type} update v{new_ver} (current v{last_ver})")
                continue
            
            if not skip_version_checks and new_ver != last_ver + 1:
                # Version gap detected - could be transient network issue, exchange rate limiting, etc.
                # OPTIMISTIC STRATEGY: Apply the update anyway, request snapshot in background for correction
                gap_size = new_ver - last_ver - 1
                self.logger().debug(
                    f"BitMart {trading_pair}: Version gap detected in {update_type} update! "
                    f"Expected v{last_ver + 1}, got v{new_ver} (gap of {gap_size}). "
                    f"Applying update anyway, requesting snapshot for correction."
                )
                
                # Request snapshot in background to correct any gaps, but DON'T wait for it
                # Only request if we're not already waiting for one
                if not self._waiting_for_snapshot.get(trading_pair, False):
                    # Fire-and-forget snapshot request (background task)
                    asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                
                # CRITICAL: Still apply this update to keep orderbook moving
                # The snapshot will correct any accumulated errors when it arrives
            
            # Build and emit DIFF for actual updates (includes one-sided updates)
            timestamp: float = int(ms_t) * 1e-3
            update_id: int = int(ms_t)
            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [(bid[0], bid[1]) for bid in bids_list],
                "asks": [(ask[0], ask[1]) for ask in asks_list],
            }
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.DIFF,
                order_book_message_content,
                timestamp)
            
            # Log one-sided updates for debugging
            if update_type != "two-sided":
                self.logger().debug(
                    f"BitMart {trading_pair}: Applying {update_type} diff v{last_ver}→v{new_ver} "
                    f"({len(bids_list)} bids, {len(asks_list)} asks)"
                )
            
            # Advance version (skip if already set during baseline establishment)
            if not skip_version_checks:
                self._last_depth_version[trading_pair] = new_ver
            message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_updates: Dict[str, Any] = raw_message["data"]

        for diff_data in diff_updates:
            timestamp: float = int(diff_data["ms_t"]) * 1e-3
            update_id: int = int(diff_data["ms_t"])
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                symbol=diff_data["symbol"])

            # Check for heartbeats with empty asks/bids
            bids_list = diff_data.get("bids", [])
            asks_list = diff_data.get("asks", [])
            is_heartbeat = (not bids_list) and (not asks_list)
            
            # Track ANY message received for this pair (proves subscription is alive)
            current_time = time.time()
            try:
                self._last_any_message_ts[trading_pair] = current_time
            except Exception:
                pass
            
            # Update data timestamp ONLY for non-heartbeat snapshots (actual orderbook data)
            if not is_heartbeat:
                try:
                    self._last_depth_update_ts[trading_pair] = current_time
                    # Track per-side updates and snapshot refresh time
                    if bids_list:
                        self._last_bids_update_ts[trading_pair] = current_time
                    if asks_list:
                        self._last_asks_update_ts[trading_pair] = current_time
                    # Full snapshot refreshes both sides
                    self._last_snapshot_refresh_ts[trading_pair] = current_time
                except Exception:
                    pass
            
            # Skip heartbeats (no data to process)
            if is_heartbeat:
                continue
            # For Depth-Increase 'snapshot' set base version
            if raw_message.get("table") == CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
                version_value = diff_data.get("version")
                was_waiting = self._waiting_for_snapshot.get(trading_pair, False)
                wait_start = self._waiting_for_snapshot_since.get(trading_pair, 0)
                wait_duration = int(current_time - wait_start) if (was_waiting and wait_start > 0) else 0
                
                # Always clear waiting flag when snapshot arrives
                self._waiting_for_snapshot[trading_pair] = False
                self._waiting_for_snapshot_since.pop(trading_pair, None)
                
                if version_value is not None:
                    # WS snapshot with proper version - this is the normal case
                    try:
                        new_version = int(version_value)
                        old_version = self._last_depth_version.get(trading_pair)
                        
                        # Log snapshot receipt
                        if was_waiting:
                            wait_msg = f" after {wait_duration}s wait" if wait_duration > 0 else ""
                            self.logger().debug(
                                f"BitMart {trading_pair}: Received requested SNAPSHOT v{new_version}{wait_msg} "
                                f"(previous v{old_version}, {len(bids_list)} bids, {len(asks_list)} asks) - resuming diffs"
                            )
                        elif old_version is not None and new_version == old_version:
                            self.logger().debug(
                                f"BitMart {trading_pair}: Received SNAPSHOT v{new_version} "
                                f"(unchanged, {len(bids_list)} bids, {len(asks_list)} asks)"
                            )
                        else:
                            self.logger().debug(
                                f"BitMart {trading_pair}: Received SNAPSHOT v{new_version} "
                                f"(previous v{old_version}, {len(bids_list)} bids, {len(asks_list)} asks)"
                            )
                        
                        self._last_depth_version[trading_pair] = new_version
                    except Exception as e:
                        self.logger().warning(f"BitMart {trading_pair}: Failed to parse snapshot version: {e}")
                        self._last_depth_version.pop(trading_pair, None)
                else:
                    # REST snapshot without version - clear version tracking
                    # Next incremental update will establish new baseline
                    old_version = self._last_depth_version.get(trading_pair)
                    self._last_depth_version.pop(trading_pair, None)
                    if was_waiting:
                        wait_msg = f" after {wait_duration}s wait" if wait_duration > 0 else ""
                        self.logger().info(
                            f"BitMart {trading_pair}: Received REST SNAPSHOT{wait_msg} "
                            f"(previous v{old_version}, {len(bids_list)} bids, {len(asks_list)} asks) - "
                            f"version tracking reset, will re-establish on next update"
                        )
                    else:
                        self.logger().debug(
                            f"BitMart {trading_pair}: Received REST SNAPSHOT "
                            f"({len(bids_list)} bids, {len(asks_list)} asks)"
                        )

            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [(bid[0], bid[1]) for bid in bids_list],
                "asks": [(ask[0], ask[1]) for ask in asks_list],
            }
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                order_book_message_content,
                timestamp)

            message_queue.put_nowait(diff_message)
    
    async def _refresh_snapshot_for_pair(self, trading_pair: str, symbol: str):
        """
        Refresh a single pair snapshot. Prefer WS 'request' for Depth-Increase to obtain a versioned snapshot.
        """
        #self.logger().info(f"BitMart {trading_pair}: Requesting snapshot refresh")
        try:
            if self._use_depth_increase and self._active_ws is not None:
                payload = {"op": "request", "args": [f"{CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME}:{symbol}"]}
                req = WSJSONRequest(payload=payload)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await self._active_ws.send(req)
                self.logger().debug(f"BitMart {trading_pair}: Sent WS snapshot request")
                return
        except Exception as e:
            # Fall back to REST below
            self.logger().warning(f"BitMart {trading_pair}: WS snapshot request failed ({e}), falling back to REST")
            pass
        try:
            snapshot = await self._request_order_book_snapshot(trading_pair)
            snap_data = snapshot.get("data") or {}
            ts = int(snap_data.get("ts", int(time.time() * 1000)))
            
            # REST snapshots don't have proper version numbers, so we need to reset version tracking
            # The next incremental update will re-establish the version baseline
            synthetic_data = {
                "asks": snap_data.get("asks", []),
                "bids": snap_data.get("bids", []),
                "ms_t": ts,
                "symbol": symbol,
                "type": "snapshot",
            }
            # DON'T set version for REST snapshots - it would break version tracking
            # Instead, clear version so next incremental update establishes new baseline
            
            synthetic = {
                "table": (CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME
                          if self._use_depth_increase else CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME),
                "data": [synthetic_data]
            }
            self._message_queue[self._snapshot_messages_queue_key].put_nowait(synthetic)
            
            # Clear version tracking - next incremental update will set new baseline
            if self._use_depth_increase:
                self._last_depth_version.pop(trading_pair, None)
                self.logger().info(f"BitMart {trading_pair}: Applied REST snapshot fallback, version tracking reset (will re-establish on next update)")
            else:
                self.logger().debug(f"BitMart {trading_pair}: Applied REST snapshot fallback")
        except Exception as e:
            # If REST fails, let watchdog handle later
            self.logger().error(f"BitMart {trading_pair}: REST snapshot fallback also failed: {e}")

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                       for trading_pair in self._trading_pairs]

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
            # Initialize per-pair liveness timestamps on reconnect
            now = time.time()
            for tp in self._trading_pairs:
                self._last_any_message_ts[tp] = now  # Track any message
                self._last_depth_update_ts[tp] = now  # Track actual data
                self._last_bids_update_ts[tp] = now  # Track bids updates
                self._last_asks_update_ts[tp] = now  # Track asks updates
                self._last_snapshot_refresh_ts[tp] = now  # Track full snapshot refreshes
                self._last_depth_version.pop(tp, None)  # Clear version to wait for first snapshot
                self._waiting_for_snapshot.pop(tp, None)  # Clear waiting flag on reconnect
                self._waiting_for_snapshot_since.pop(tp, None)  # Clear wait timer on reconnect

            # Depth-Increase optional: proactively request full snapshot per symbol to seed version state
            # Disabled by default to reduce subscribe-burst load; fallback refresh logic remains in place.
            if self._use_depth_increase and self._seed_snapshot_via_request:
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
        
        This method detects three types of staleness:
        1. Complete staleness: No messages at all (original behavior)
        2. One-sided staleness: Only bids OR asks updating (new detection)
        3. Periodic refresh: Refresh snapshot every N minutes (preventive maintenance)
        """
        now = time.time()
        stale_pairs = []
        one_sided_stale_pairs = []
        periodic_refresh_pairs = []
        
        # Check all trading pairs for staleness
        for trading_pair in list(self._trading_pairs):
            # Use _last_any_message_ts which includes heartbeats and snapshots
            last_any_ts = float(self._last_any_message_ts.get(trading_pair, 0.0) or 0.0)
            
            # Check 1: Complete staleness (no messages at all)
            if last_any_ts > 0 and (now - last_any_ts) >= self._DEPTH_STALENESS_SECONDS:
                stale_pairs.append(trading_pair)
                continue  # Handle complete staleness first
            
            # Check 2: One-sided staleness (only for Depth-Increase with incremental updates)
            if self._use_depth_increase and last_any_ts > 0:
                last_bids_ts = float(self._last_bids_update_ts.get(trading_pair, 0.0) or 0.0)
                last_asks_ts = float(self._last_asks_update_ts.get(trading_pair, 0.0) or 0.0)
                
                # If one side hasn't updated in 2x the staleness threshold while the other side is updating
                if last_bids_ts > 0 and last_asks_ts > 0:
                    bids_stale_duration = now - last_bids_ts
                    asks_stale_duration = now - last_asks_ts
                    
                    # One side is stale while the other is fresh (received updates recently)
                    if (asks_stale_duration >= self._DEPTH_STALENESS_SECONDS * 2 and 
                        bids_stale_duration < self._DEPTH_STALENESS_SECONDS):
                        one_sided_stale_pairs.append((trading_pair, "asks"))
                        continue
                    elif (bids_stale_duration >= self._DEPTH_STALENESS_SECONDS * 2 and 
                          asks_stale_duration < self._DEPTH_STALENESS_SECONDS):
                        one_sided_stale_pairs.append((trading_pair, "bids"))
                        continue
            
            # Check 3: Periodic refresh (preventive maintenance for incremental depth)
            if self._use_depth_increase:
                last_snapshot_ts = float(self._last_snapshot_refresh_ts.get(trading_pair, 0.0) or 0.0)
                if last_snapshot_ts > 0 and (now - last_snapshot_ts) >= self._PERIODIC_SNAPSHOT_REFRESH_SECONDS:
                    periodic_refresh_pairs.append(trading_pair)
        
        # Handle complete staleness (most critical)
        if stale_pairs:
            # If more than 50% of pairs are stale, it's a systematic issue - force full reconnect
            if len(stale_pairs) > len(self._trading_pairs) // 2:
                self.logger().warning(
                    f"BitMart depth watchdog: {len(stale_pairs)}/{len(self._trading_pairs)} pairs stale "
                    f"(threshold: {self._DEPTH_STALENESS_SECONDS}s), forcing full reconnect"
                )
                raise ConnectionError("BitMart orderbook data stale for multiple pairs; forcing reconnect")
            
            # Try targeted recovery for individual stale pairs
            for trading_pair in stale_pairs:
                try:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    depth_channel = (CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME
                                     if self._use_depth_increase else CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME)
                    topic = f"{depth_channel}:{symbol}"
                    
                    last_any_msg = self._last_any_message_ts.get(trading_pair, 0)
                    stale_duration = int(now - last_any_msg) if last_any_msg > 0 else 0
                    self.logger().warning(
                        f"BitMart {trading_pair}: No messages for {stale_duration}s "
                        f"(threshold: {self._DEPTH_STALENESS_SECONDS}s), attempting recovery"
                    )
                    
                    # Unsubscribe (best-effort) then subscribe
                    try:
                        unsub = WSJSONRequest(payload={"op": "unsubscribe", "args": [topic]})
                        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                            await ws.send(unsub)
                    except Exception:
                        pass
                    
                    sub = WSJSONRequest(payload={"op": "subscribe", "args": [topic]})
                    async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                        await ws.send(sub)
                    
                    # Mark that we're waiting for snapshot (diffs will be dropped until it arrives)
                    self._waiting_for_snapshot[trading_pair] = True
                    self._waiting_for_snapshot_since[trading_pair] = now
                    
                    # Refresh snapshot using WS request (Depth-Increase) or REST fallback
                    await self._refresh_snapshot_for_pair(trading_pair, symbol)
                    
                    # Reset staleness timer to avoid repeated attempts every check cycle
                    # The snapshot response will update _last_any_message_ts when received
                    self._last_any_message_ts[trading_pair] = now
                    
                except Exception as e:
                    self.logger().warning(f"BitMart depth watchdog: failed to recover {trading_pair}: {e}")
                    # If we can't send resubscribe (e.g., WS disconnected), force full reconnect
                    raise ConnectionError(f"BitMart depth watchdog recovery failed; forcing reconnect")
        
        # Handle one-sided staleness (asks or bids stale while other side updates)
        if one_sided_stale_pairs:
            for trading_pair, stale_side in one_sided_stale_pairs:
                try:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    
                    last_bids_ts = self._last_bids_update_ts.get(trading_pair, 0)
                    last_asks_ts = self._last_asks_update_ts.get(trading_pair, 0)
                    stale_duration = int(now - (last_asks_ts if stale_side == "asks" else last_bids_ts))
                    
                    #self.logger().warning(
                    #    f"BitMart {trading_pair}: One-sided staleness detected - {stale_side} stale for {stale_duration}s "
                    #    f"while {'bids' if stale_side == 'asks' else 'asks'} updating. Requesting snapshot (non-blocking)."
                    #)
                    
                    # Don't need to resubscribe, just refresh snapshot to get both sides fresh
                    # IMPORTANT: Don't block updates - request snapshot in background
                    # The snapshot will fix any staleness when it arrives, but updates keep flowing
                    asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                    
                except Exception as e:
                    self.logger().warning(f"BitMart {trading_pair}: Failed to refresh snapshot for one-sided staleness: {e}")
        
        # Handle periodic refresh (preventive maintenance - defensive, not required by protocol)
        # This is completely non-blocking: fire-and-forget background requests
        if periodic_refresh_pairs:
            # Limit number of refreshes per cycle to avoid bursts
            refresh_batch = periodic_refresh_pairs[:5]  # Max 5 per cycle
            for trading_pair in refresh_batch:
                try:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    
                    last_snapshot_ts = self._last_snapshot_refresh_ts.get(trading_pair, 0)
                    elapsed = int(now - last_snapshot_ts) if last_snapshot_ts > 0 else 0
                    
                    # DEBUG only - no need to spam logs for routine maintenance
                    self.logger().debug(
                        f"BitMart {trading_pair}: Periodic snapshot refresh ({elapsed}s since last refresh)"
                    )
                    
                    # CRITICAL: Don't set _waiting_for_snapshot - this would block updates!
                    # Just fire-and-forget the snapshot request in background
                    # The snapshot will arrive and update the orderbook without blocking live updates
                    asyncio.create_task(self._refresh_snapshot_for_pair(trading_pair, symbol))
                    
                except Exception as e:
                    # Silence errors - periodic refresh is optional defensive measure
                    pass

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
                # Clean up
                self._active_ws = None
                if self._ws_consumer_task and not self._ws_consumer_task.done():
                    self._ws_consumer_task.cancel()
                    try:
                        await self._ws_consumer_task
                    except Exception:
                        pass
                self._ws_consumer_task = None
                await self._sleep(reconnect_delay)
                if ws:
                    await ws.disconnect()