import asyncio
import uuid
import time
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.exchange.htx.htx_constants as CONSTANTS
from hummingbot.connector.exchange.htx.htx_web_utils import public_rest_url
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.htx.htx_exchange import HtxExchange


class HtxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'HtxExchange',
                 api_factory: WebAssistantsFactory,
                 ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._diff_messages_queue_key = CONSTANTS.ORDERBOOK_CHANNEL_SUFFIX
        self._trade_messages_queue_key = CONSTANTS.TRADE_CHANNEL_SUFFIX
        self._api_factory = api_factory
        
        # Keep-alive tracking for stable connections
        # Key: connection name ("MBP", "Trade"), Value: last activity timestamp
        self._last_activity_timestamps: Dict[str, float] = {}
        self._ping_interval = 30  # Rely on server pings; only monitor liveness
        self._pong_timeout = 10

        # Reconnect backoff tracking
        self._reconnect_attempts = 0

        # No per-channel silence tracking (keep minimal state)
        self._suppress_reconnect_logs = False

        # Track transient disconnects to emit a single concise INFO after successful resubscribe
        self._pending_reconnect_notice: Optional[Dict[str, Any]] = None
        
        # Symbol cache for hot path optimization (avoid async lookups per message)
        self._symbol_to_pair_cache: Dict[str, str] = {}
        
        # MBP sequence tracking per symbol for gap detection
        # Key: exchange symbol (lowercase), Value: last received seqNum
        self._last_seq_num: Dict[str, int] = {}

        # Flash-order (ghost-wall) suppression state, per exchange symbol (lowercase).
        # Maintained only from raw mbp.refresh.20 snapshots; see _apply_flash_filter and
        # htx_constants.FLASH_FILTER_*. Each entry:
        #   {"bid_px", "bid_cnt", "ask_px", "ask_cnt", "spread_ema"}
        # Cleared alongside _last_seq_num on (re)subscribe so a reconnect starts clean.
        self._flash_state: Dict[str, Dict[str, float]] = {}
        
        # Track active WebSocket for snapshot requests
        self._active_ws: Optional[WSAssistant] = None
        self._trade_ws: Optional[WSAssistant] = None

    def _extract_close_code(self, exc: Exception) -> Optional[str]:
        try:
            text = str(exc)
        except Exception:
            return None
        match = re.search(r"Close code\s*=\s*(\d+)", text)
        return match.group(1) if match else None

    def _is_transient_ws_close_exception(self, exc: Exception) -> bool:
        """
        Returns True for close codes typically associated with transient network/CDN resets
        that should be auto-recovered without noisy error logs.
        """
        code = self._extract_close_code(exc)
        # Treat these as transient: 1000 normal closure, 1001 going away, 1003 policy,
        # 1005 no status, 1006 abnormal closure (often network), 1012 service restart,
        # 1013 try again later
        return code in {"1000", "1001", "1003", "1005", "1006", "1012", "1013"}

    def _is_ws_close_code_1003_exception(self, exc: Exception) -> bool:
        """
        Returns True if the exception indicates WS close code 1003 (unsupported data / policy violation).
        We treat it specially to avoid noisy logs and allow the outer loop to reconnect quietly.
        """
        try:
            text = str(exc)
        except Exception:
            return False
        # Matches formats raised by WSConnection._check_msg_closed_type and aiohttp errors.
        return ("Close code = 1003" in text) or ("code=1003" in text)

    async def _get_ws_assistant(self, url: str) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        throttler = getattr(self._api_factory, "throttler", None)
        
        connection_params = {
            "ws_url": url,
            "ping_timeout": None,
            "message_timeout": None,
            "ws_headers": {"Accept-Encoding": "gzip"},
            "max_msg_size": 32 * 1024 * 1024,
        }
        
        if throttler is not None:
            async with throttler.execute_task(CONSTANTS.WS_CONNECTION_LIMIT_ID):
                await ws.connect(**connection_params)
        else:
            await ws.connect(**connection_params)
            
        return ws

    async def _subscribe_single_trading_pair(self, ws: Optional[WSAssistant], trading_pair: str):
        """
        Subscribe a new pair at runtime without disrupting existing connections.

        Strategy:
        - MBP connection: disconnect and let _manage_connection reconnect, which
          calls _subscribe_mbp with the full updated _trading_pairs list including
          the new pair.  The immediate `req` snapshot in _subscribe_mbp ensures the
          C++ OB is seeded before the balancer fires.
        - Trade connection: send a `sub` message on the existing open WS so we
          receive trade events for the new pair without disrupting all other pairs.

        Note: the `ws` parameter (from the base class contract) is ignored here;
        HTX manages its own _active_ws and _trade_ws references internally.
        _reconnect_requested is set before disconnecting MBP so _manage_connection
        can recognise the close as intentional and log INFO instead of ERROR.
        """
        # Subscribe the new pair on the Trade WS in-place (no reconnect needed)
        if self._trade_ws is not None:
            try:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(
                    trading_pair=trading_pair
                )
                exchange_symbol = exchange_symbol.lower()
                await self._trade_ws.send(WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.trade.detail",
                    "id": str(uuid.uuid4())
                }))
                self.logger().debug(f"Sent trade.detail sub for {exchange_symbol} on existing Trade WS")
            except Exception as e:
                self.logger().warning(f"Could not subscribe {trading_pair} trade on existing WS: {e}")

        # Signal that the upcoming MBP disconnect is intentional before triggering it
        self._reconnect_requested = True
        # Disconnect MBP to force reconnect + re-subscribe all pairs (including new one)
        if self._active_ws:
            await self._active_ws.disconnect()
        # Do NOT disconnect _trade_ws: we already sent the sub inline above.

    async def listen_for_subscriptions(self):
        """
        Main entry point. Manages two separate WebSocket connections:
        1. MBP Orderbook (/feed) - Priority freshness
        2. Trade Details (/ws) - Public trades (not available on /feed)
        """
        tasks = [
            self._manage_connection(CONSTANTS.WS_MBP_URL, self._subscribe_mbp, "MBP"),
            self._manage_connection(CONSTANTS.WS_PUBLIC_URL, self._subscribe_trades, "Trade")
        ]
        await asyncio.gather(*tasks)

    async def _manage_connection(self, url: str, subscribe_func, name: str):
        """
        Generic loop to manage a single WebSocket connection (connect, subscribe, process, retry).
        """
        ws = None
        while True:
            try:
                self.logger().info(f"Connecting to {name} WebSocket: {url}")
                ws = await self._get_ws_assistant(url)
                
                # Reset tracking for this connection type
                self._last_activity_timestamps[name] = time.time()
                
                if name == "MBP":
                    self._active_ws = ws
                elif name == "Trade":
                    self._trade_ws = ws

                self._reconnect_attempts = 0
                await subscribe_func(ws)

                # Create tasks for message processing and keep-alive
                message_processor_task = asyncio.create_task(
                    self._process_websocket_messages(ws, name)
                )
                keep_alive_task = asyncio.create_task(
                    self._keep_alive_loop(ws, name)
                )
                
                # Wait for either task to complete
                done, pending = await asyncio.wait(
                    [message_processor_task, keep_alive_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                for task in done:
                    exception = task.exception()
                    if exception:
                        raise exception
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if name == "MBP" and self.check_and_clear_reconnect_request():
                    # Disconnect was triggered by _subscribe_single_trading_pair — expected
                    self._reconnect_attempts = 0
                    self.logger().info(
                        f"HTX MBP WS reconnecting to subscribe new pair "
                        f"({len(self._trading_pairs)} pairs total)"
                    )
                elif self._is_transient_ws_close_exception(e):
                    self._reconnect_attempts += 1
                    self.logger().debug(f"Transient close on {name} WS: {e}. Reconnecting...")
                else:
                    self._reconnect_attempts += 1
                    self.logger().error(f"Unexpected error on {name} WS: {e}", exc_info=True)
                await asyncio.sleep(2.0)  # Backoff
            finally:
                # Clear the shared ws reference so callers (e.g. _subscribe_single_trading_pair)
                # see None rather than a stale disconnected socket during the reconnect window.
                if name == "MBP":
                    self._active_ws = None
                elif name == "Trade":
                    self._trade_ws = None
                if ws is not None:
                    await ws.disconnect()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, name: str):
        """
        Process incoming WebSocket messages and route them to the appropriate queue.
        """
        async for ws_response in websocket_assistant.iter_messages():
            try:
                # Any message (ping, pong, data) updates liveness for this specific connection
                self._last_activity_timestamps[name] = time.time()
                
                data = ws_response.data
                if not isinstance(data, dict):
                    continue
                
                channel = self._channel_originating_message(data)
                
                if channel:
                    self._message_queue[channel].put_nowait(data)
                else:
                    await self._process_message_for_unknown_channel(data, websocket_assistant, name)
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._is_ws_close_code_1003_exception(e):
                    raise
                self.logger().error(f"Error processing {name} WS messages", exc_info=True)

    async def _keep_alive_loop(self, ws: WSAssistant, name: str):
        while True:
            try:
                await asyncio.sleep(self._ping_interval)
                
                # Check liveness for this specific connection
                now = time.time()
                last_activity = self._last_activity_timestamps.get(name, 0)
                
                if last_activity and (now - last_activity) > 60:
                    self.logger().warning(f"{name} WS inactivity threshold (60s) exceeded, disconnecting")
                    await ws.disconnect()
                    break
                
            except Exception as e:
                self.logger().error(f"Error in {name} keep-alive loop: {e}")
                break

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        HTX provides snapshots through the depth channel, not separate REST calls.
        The base class expects this method but we don't need to implement it.
        """
        pass

    def snapshot_message_from_exchange(self,
                                       msg: Dict[str, Any],
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        """
        if metadata:
            msg.update(metadata)
        msg_ts = msg["tick"]["ts"] * 1e-3
        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["tick"]["ts"],
            "bids": msg["tick"].get("bids", []),
            "asks": msg["tick"].get("asks", [])
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=msg_ts)

    def trade_message_from_exchange(self,
                                    msg: Dict[str, Any],
                                    metadata: Dict[str, Any] = None) -> OrderBookMessage:
        """
        Creates a trade message with the information from the trade event
        """
        if metadata:
            msg.update(metadata)

        msg_ts = msg["ts"] * 1e-3
        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg["direction"] == "buy" else float(TradeType.SELL.value),
            "trade_id": msg["id"],
            "update_id": msg["ts"],
            "amount": msg["amount"],
            "price": msg["price"]
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=msg_ts)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        url = public_rest_url(CONSTANTS.DEPTH_URL)
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params: Dict = {"symbol": exchange_symbol, "type": "step0"}
        snapshot_data = await rest_assistant.execute_request(
            url=url,
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_URL,
        )
        return snapshot_data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
            msg=snapshot,
            metadata={"trading_pair": trading_pair},
        )
        return snapshot_msg

    async def _subscribe_mbp(self, ws: WSAssistant):
        """Subscribe to MBP Incremental (5) and Refresh (20) channels.

        After sending `sub` messages (which start the 100ms periodic refresh stream),
        we also send an immediate `req` snapshot request for each pair.  The `rep`
        response arrives within ~1 RTT (~50ms from VPS) and seeds the C++ order book
        before any incremental delta can corrupt it.  This is especially important for
        thin/low-volume pairs (e.g. QUICK-USDT) where the first periodic `mbp.refresh`
        message may be delayed and the balancer fires immediately on strategy resume.
        """
        try:
            self._last_seq_num.clear()
            self._flash_state.clear()

            symbols_to_snapshot: List[str] = []

            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                self._symbol_to_pair_cache[exchange_symbol.lower()] = trading_pair
                exchange_symbol = exchange_symbol.lower()

                # Incremental mbp.5
                await ws.send(WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.mbp.{CONSTANTS.MBP_INCREMENTAL_DEPTH}",
                    "id": str(uuid.uuid4())
                }))

                # Snapshot mbp.refresh.20 (starts 100ms periodic stream)
                await ws.send(WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.mbp.refresh.{CONSTANTS.MBP_REFRESH_DEPTH}",
                    "id": str(uuid.uuid4())
                }))

                symbols_to_snapshot.append(exchange_symbol)

            self.logger().info("Subscribed to MBP channels...")

            # Request an immediate snapshot for each symbol via `req`.
            # The `rep` response (handled as SNAPSHOT in _parse_order_book_diff_message)
            # seeds the C++ OB right away instead of waiting for the first periodic
            # mbp.refresh message, eliminating the stale-book race on runtime strategy add.
            for exchange_symbol in symbols_to_snapshot:
                await self._request_mbp_snapshot(ws, exchange_symbol)

        except Exception as e:
            self.logger().error("Error subscribing to MBP channels", exc_info=True)
            raise

    async def _subscribe_trades(self, ws: WSAssistant):
        """Subscribe to Trade Detail channels"""
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                exchange_symbol = exchange_symbol.lower()
                
                await ws.send(WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.trade.detail",
                    "id": str(uuid.uuid4())
                }))
                
            self.logger().info("Subscribed to Trade channels...")
            
        except Exception as e:
            self.logger().error("Error subscribing to Trade channels", exc_info=True)
            raise

    async def _request_mbp_snapshot(self, ws: WSAssistant, exchange_symbol: str):
        """
        Request a full MBP snapshot for a symbol (used for initial sync and gap recovery).
        """
        mbp_channel = f"market.{exchange_symbol}.mbp.{CONSTANTS.MBP_REFRESH_DEPTH}"
        snapshot_request: WSJSONRequest = WSJSONRequest({
            "req": mbp_channel,
            "id": str(uuid.uuid4())
        })
        throttler = getattr(self._api_factory, "throttler", None)
        if throttler is not None:
            async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                await ws.send(snapshot_request)
        else:
            await ws.send(snapshot_request)
        self.logger().debug(f"Requested MBP snapshot for {exchange_symbol}")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identify which channel a message belongs to based on the channel name.
        """
        # Check both "ch" (incremental) and "rep" (snapshot response)
        channel = event_message.get("ch", "") or event_message.get("rep", "")
        
        # MBP incremental updates and snapshot responses: market.{symbol}.mbp.{depth}
        if ".mbp." in channel:
            return self._diff_messages_queue_key
        elif "trade.detail" in channel:
            return self._trade_messages_queue_key
        
        return ""

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse trade messages from the raw dictionary.
        Called by the base class with messages from the queue.
        """
        ex_symbol = raw_message["ch"].split(".")[1]
        # Use cache for O(1) lookup, fallback to async if not cached
        trading_pair = self._symbol_to_pair_cache.get(ex_symbol)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
            self._symbol_to_pair_cache[ex_symbol] = trading_pair
        
        for data in raw_message["tick"]["data"]:
            trade_message: OrderBookMessage = self.trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": trading_pair}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse MBP orderbook messages. Handles three types:
        1. mbp.refresh.X: 100ms full snapshots (20 levels)
        2. mbp.X: tick-by-tick incremental updates (5 levels)
        3. req response: snapshot from explicit request
        
        Format:
        - Refresh:     {"ch":"market.btcusdt.mbp.refresh.20", "tick":{"seqNum":X, "bids":[...], "asks":[...]}}
        - Incremental: {"ch":"market.btcusdt.mbp.5", "tick":{"seqNum":X, "prevSeqNum":Y, "bids":[...], "asks":[...]}}
        - Snapshot:    {"rep":"market.btcusdt.mbp.5", "data":{"seqNum":X, "bids":[...], "asks":[...]}}
        """
        # Check if this is a snapshot response (from req) or a channel update (from sub)
        is_req_response = "rep" in raw_message
        
        if is_req_response:
            # Snapshot response from "req"
            msg_channel = raw_message.get("rep", "")
            parts = msg_channel.split(".")
            if len(parts) >= 2:
                order_book_symbol = parts[1]
            else:
                return
            
            data = raw_message.get("data", {})
            seq_num = data.get("seqNum", 0)
            
            # Update sequence tracking
            self._last_seq_num[order_book_symbol] = seq_num

            
            # Use cache for O(1) lookup
            trading_pair = self._symbol_to_pair_cache.get(order_book_symbol)
            if trading_pair is None:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
                self._symbol_to_pair_cache[order_book_symbol] = trading_pair
            
            # Create snapshot message
            msg_ts = raw_message.get("ts", time.time() * 1000) * 1e-3
            content = {
                "trading_pair": trading_pair,
                "update_id": seq_num,
                "bids": data.get("bids", []),
                "asks": data.get("asks", [])
            }
            snapshot_msg = OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=msg_ts)
            message_queue.put_nowait(snapshot_msg)
            
        else:
            # Channel update from "sub" - either mbp.refresh.X or mbp.X
            msg_channel = raw_message.get("ch", "")
            parts = msg_channel.split(".")
            if len(parts) >= 2:
                order_book_symbol = parts[1]
            else:
                return
            
            tick = raw_message.get("tick", {})
            seq_num = tick.get("seqNum", 0)
            
            # Check if this is a refresh (snapshot) or incremental
            is_refresh = ".mbp.refresh." in msg_channel
            
            if is_refresh:
                # mbp.refresh.X - full snapshot at 100ms intervals
                # Always update sequence tracking from refresh
                self._last_seq_num[order_book_symbol] = seq_num
                
                trading_pair = self._symbol_to_pair_cache.get(order_book_symbol)
                if trading_pair is None:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
                    self._symbol_to_pair_cache[order_book_symbol] = trading_pair
                
                msg_ts = raw_message.get("ts", time.time() * 1000) * 1e-3
                bids = tick.get("bids", [])
                asks = tick.get("asks", [])

                # Flash-order (ghost-wall) suppression. Applied ONLY here, on the
                # raw 100ms mbp.refresh.20 full snapshot — never on mbp.5 diffs or
                # `req` responses (those leave the seqNum machinery untouched). Off
                # the hot path; fail-open. See _apply_flash_filter + htx_constants.
                if CONSTANTS.FLASH_FILTER_ENABLED:
                    bids, asks = self._apply_flash_filter(
                        order_book_symbol, trading_pair, bids, asks
                    )

                content = {
                    "trading_pair": trading_pair,
                    "update_id": seq_num,
                    "bids": bids,
                    "asks": asks
                }
                snapshot_msg = OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=msg_ts)
                message_queue.put_nowait(snapshot_msg)

            else:
                # mbp.X - tick-by-tick incremental update
                prev_seq_num = tick.get("prevSeqNum", 0)
                
                # Check if we have received a snapshot yet for this symbol
                last_seq = self._last_seq_num.get(order_book_symbol)
                
                if last_seq is None:
                    # No snapshot received yet - mbp.refresh will provide initial sync
                    return
                
                # Optimistic processing: apply incrementals if they're moving forward
                # The next refresh snapshot (within 100ms) will correct any inconsistencies
                
                if seq_num <= last_seq:
                    # Old/duplicate message - skip
                    return
                
                # Log sequence gaps at debug level; mbp.refresh.20 auto-corrects within 100ms
                if prev_seq_num != last_seq:
                    self.logger().debug(
                        f"MBP incremental gap for {order_book_symbol}: had seq={last_seq}, got prevSeq={prev_seq_num}, seqNum={seq_num}"
                    )
                
                # Update sequence tracking
                self._last_seq_num[order_book_symbol] = seq_num
                
                # Use cache for O(1) lookup
                trading_pair = self._symbol_to_pair_cache.get(order_book_symbol)
                if trading_pair is None:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
                    self._symbol_to_pair_cache[order_book_symbol] = trading_pair
                
                # Create diff message (incremental update)
                msg_ts = raw_message.get("ts", time.time() * 1000) * 1e-3
                content = {
                    "trading_pair": trading_pair,
                    "update_id": seq_num,
                    "bids": tick.get("bids", []),
                    "asks": tick.get("asks", [])
                }
                diff_msg = OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp=msg_ts)
                message_queue.put_nowait(diff_msg)

    def _pair_flash_filtered(self, trading_pair: str) -> bool:
        """Whether the flash filter applies to this pair (master switch already checked)."""
        return CONSTANTS.FLASH_FILTER_ALL_PAIRS or (trading_pair in CONSTANTS.FLASH_FILTER_PAIRS)

    def _apply_flash_filter(self,
                            symbol: str,
                            trading_pair: str,
                            bids: List[Any],
                            asks: List[Any]):
        """
        Withhold an *improving* inside level until it persists across N consecutive raw
        mbp.refresh.20 snapshots (L2), with a stricter gate on chronically-wide,
        flash-prone books (L4). Returns possibly-trimmed (bids, asks).

        Runs in the async MBP parse coroutine — OFF the Cython hot path. The strategy's
        c_tick reads the already-trimmed C++ book, so the fire path pays zero added cost.
        Mirrors the empirically-validated ws_book_checker HTX filter (L2 raw-snapshot
        persistence + L4 wide-gap); L1/L3 are deferred (clean seams).

        Contract / safety:
          - Only invoked from the mbp.refresh.20 SNAPSHOT branch (full-book reset every
            ~100ms), so withholding a level is self-healing: a genuine level re-enters on
            the next refresh once confirmed.
          - Only *improving* inside levels are ever withheld; a receding/worse top is
            always passed through (never clip a real market move).
          - Per-side, price-keyed (wide-book lesson: never mid-keyed).
          - Fail-open: any exception returns the original (bids, asks) untouched.
        """
        try:
            if not self._pair_flash_filtered(trading_pair):
                return bids, asks

            best_bid = self._best_price(bids, want_max=True)
            best_ask = self._best_price(asks, want_max=False)
            # Need a two-sided book to reason about the inside and the spread.
            if best_bid is None or best_ask is None or best_bid <= 0.0 or best_ask <= 0.0:
                return bids, asks

            st = self._flash_state.get(symbol)
            if st is None:
                # First refresh for this symbol: seed baselines, suppress nothing
                # (no prior to compare against — avoid a cold-start false drop).
                mid = 0.5 * (best_bid + best_ask)
                st = {
                    "bid_px": best_bid, "bid_cnt": 1,
                    "ask_px": best_ask, "ask_cnt": 1,
                    "spread_ema": ((best_ask - best_bid) / mid * 100.0) if mid > 0 else 0.0,
                }
                self._flash_state[symbol] = st
                return bids, asks

            eps = CONSTANTS.FLASH_FILTER_PRICE_EPS
            need = CONSTANTS.FLASH_FILTER_PERSIST_RAW_SNAPS

            # --- L2 persistence counters (per side, on raw snapshots) ---
            # Same inside price (within eps) => increment confirmation; else this is a
            # new inside level => reset to 1 and remember it.
            if self._prices_equal(best_bid, st["bid_px"], eps):
                st["bid_cnt"] += 1
            else:
                st["bid_cnt"] = 1
            prev_bid = st["bid_px"]
            st["bid_px"] = best_bid

            if self._prices_equal(best_ask, st["ask_px"], eps):
                st["ask_cnt"] += 1
            else:
                st["ask_cnt"] = 1
            prev_ask = st["ask_px"]
            st["ask_px"] = best_ask

            # --- L4 baseline spread (EMA) ---
            mid = 0.5 * (best_bid + best_ask)
            spread_pct = ((best_ask - best_bid) / mid * 100.0) if mid > 0 else 0.0
            a = CONSTANTS.FLASH_FILTER_SPREAD_EMA_ALPHA
            st["spread_ema"] = (1.0 - a) * st["spread_ema"] + a * spread_pct
            wide = st["spread_ema"] >= CONSTANTS.FLASH_FILTER_WIDE_SPREAD_PCT

            # A side is a suppression candidate when its inside level is NEW this snapshot
            # (cnt == 1) and not yet confirmed (need > 1). On normal books we only withhold
            # an *improving* inside (better bid / better ask) — a receding top is a real
            # market move and is always kept. On chronically-wide (flash-prone) books we
            # also withhold a brand-new inside that closes the gap, regardless of direction,
            # since that is exactly the flash gap-fill pattern.
            bid_improves = best_bid > prev_bid * (1.0 + eps)
            ask_improves = best_ask < prev_ask * (1.0 - eps)

            drop_bid = (st["bid_cnt"] < need) and (bid_improves or wide)
            drop_ask = (st["ask_cnt"] < need) and (ask_improves or wide)

            if not drop_bid and not drop_ask:
                return bids, asks

            new_bids = self._drop_inside(bids, best_bid, eps) if drop_bid else bids
            new_asks = self._drop_inside(asks, best_ask, eps) if drop_ask else asks

            if CONSTANTS.FLASH_FILTER_LOG_SUPPRESSIONS:
                self.logger().debug(
                    "HTX flash-filter withheld inside level on %s "
                    "(bid=%s drop=%s cnt=%s | ask=%s drop=%s cnt=%s | "
                    "spread_ema=%.3f%% wide=%s need=%s)",
                    trading_pair, best_bid, drop_bid, st["bid_cnt"],
                    best_ask, drop_ask, st["ask_cnt"], st["spread_ema"], wide, need,
                )
            return new_bids, new_asks

        except Exception:
            # Fail-open: never lose a real book update to a filter bug.
            self.logger().debug("HTX flash-filter error; passing snapshot through", exc_info=True)
            return bids, asks

    @staticmethod
    def _best_price(levels: List[Any], want_max: bool) -> Optional[float]:
        """Best (max for bids / min for asks) positive price over [[price, amount], ...].
        Robust to input ordering and to string-encoded numbers."""
        best: Optional[float] = None
        for lvl in levels:
            try:
                px = float(lvl[0])
                amt = float(lvl[1])
            except (TypeError, ValueError, IndexError):
                continue
            if px <= 0.0 or amt <= 0.0:
                continue
            if best is None or (px > best if want_max else px < best):
                best = px
        return best

    @staticmethod
    def _prices_equal(a: float, b: float, eps: float) -> bool:
        if b == 0.0:
            return a == 0.0
        return abs(a - b) <= eps * abs(b)

    @classmethod
    def _drop_inside(cls, levels: List[Any], inside_px: float, eps: float) -> List[Any]:
        """Return levels with the single inside price (within eps) removed. The book then
        falls back to the next-best confirmed level on that side; the next refresh restores
        the level if it persists."""
        out = []
        removed = False
        for lvl in levels:
            try:
                px = float(lvl[0])
            except (TypeError, ValueError, IndexError):
                out.append(lvl)
                continue
            if not removed and cls._prices_equal(px, inside_px, eps):
                removed = True
                continue
            out.append(lvl)
        return out

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant, name: str):
        # Server may send ping - respond immediately
        if "ping" in event_message:
            # Echo back the timestamp
            ts = event_message.get("ping")
            pong_payload = {"pong": ts} if ts else {"pong": int(time.time() * 1000)}
            pong_request = WSJSONRequest(payload=pong_payload)
            await websocket_assistant.send(request=pong_request)
            # Record server ping receipt
            self._last_activity_timestamps[name] = time.time()
            return
        
        # HTX v2 format
        action = event_message.get("action")
        if action == "ping":
            # Respond with pong echoing the timestamp
            ts = (event_message.get("data") or {}).get("ts")
            pong = {"action": "pong", "data": {"ts": ts} if ts else {}}
            await websocket_assistant.send(WSJSONRequest(payload=pong))
            # Record server ping receipt
            self._last_activity_timestamps[name] = time.time()
            return
        
        # Handle subscription confirmations and errors
        try:
            status = event_message.get("status")
            if status == "ok" and ("subbed" in event_message or "rep" in event_message):
                ch = event_message.get("subbed") or event_message.get("rep")
                if ch:
                    # Clear suppression once we have a successful subscription ack
                    if self._suppress_reconnect_logs:
                        self._suppress_reconnect_logs = False
                    self.logger().debug(f"Successfully subscribed to: {ch}")
                return
                
            if status == "error" or ("err-code" in event_message or "err-msg" in event_message):
                err_msg = str(event_message.get("err-msg", "Unknown error"))
                # Known-benign: alt pairs don't offer the tick-by-tick mbp.5 channel — only the
                # ~9 major pairs do. We always send both mbp.5 and mbp.refresh.20; the refresh sub
                # succeeds and feeds the book, so the rejected mbp.5 sub is expected, not an error.
                # Log it at DEBUG to avoid flooding launch logs; keep all other WS errors at WARNING.
                if "does not currently offer subscription services" in err_msg:
                    self.logger().debug(
                        f"WebSocket subscription declined (expected for non-major pairs): {err_msg}",
                        extra={"event": event_message}
                    )
                else:
                    self.logger().warning(
                        f"WebSocket error: {err_msg}",
                        extra={"event": event_message}
                    )
                # Disconnect to trigger clean resubscribe when server rejects channels
                if "invalid" in err_msg.lower():
                    await websocket_assistant.disconnect()
                return
        except Exception:
            pass