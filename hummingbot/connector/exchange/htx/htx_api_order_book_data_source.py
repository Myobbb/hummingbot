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
        self._last_ping_timestamp = 0
        self._last_pong_timestamp = 0
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
        
        # Track symbols needing snapshot refresh (due to sequence gaps)
        self._symbols_needing_refresh: set = set()
        
        # Track active WebSocket for snapshot requests
        self._active_ws: Optional[WSAssistant] = None

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

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()

        throttler = getattr(self._api_factory, "throttler", None)
        
        # IMPORTANT: Use /feed endpoint for MBP incremental updates (not /ws)
        # MBP provides 100ms incremental updates vs 1s snapshots from depth.step0
        ws_url = CONSTANTS.WS_MBP_URL
        connection_params = {
            "ws_url": ws_url,
            "ping_timeout": None,  # Disable protocol ping, use JSON ping instead
            # Do not enforce a client-side receive timeout; HTX sends server pings every ~5s.
            # Rely on _keep_alive_loop inactivity checks to recycle stale connections.
            "message_timeout": None,
            "ws_headers": {"Accept-Encoding": "gzip"},
            "max_msg_size": 32 * 1024 * 1024,
        }
        
        if throttler is not None:
            async with throttler.execute_task(CONSTANTS.WS_CONNECTION_LIMIT_ID):
                await ws.connect(**connection_params)
        else:
            await ws.connect(**connection_params)
            
        # Reset ping/pong tracking
        self._last_ping_timestamp = 0
        self._last_pong_timestamp = time.time()
        
        return ws

    async def listen_for_subscriptions(self):
        """
        Main entry point that maintains the WebSocket connection and processes messages.
        This is the missing method that connects everything together.
        """
        ws = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                
                # Create tasks for message processing and keep-alive
                message_processor_task = asyncio.create_task(
                    self._process_websocket_messages(websocket_assistant=ws)
                )
                keep_alive_task = asyncio.create_task(
                    self._keep_alive_loop(ws)
                )
                
                # Wait for either task to complete (likely due to disconnection)
                done, pending = await asyncio.wait(
                    [message_processor_task, keep_alive_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel the other task
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                # Check if any task raised an exception
                for task in done:
                    exception = task.exception()
                    if exception:
                        raise exception
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._reconnect_attempts += 1
                # For transient close codes, defer noisy logs and emit a concise INFO after resubscribe
                if self._is_transient_ws_close_exception(e):
                    code = self._extract_close_code(e)
                    self._pending_reconnect_notice = {
                        "code": code or "unknown",
                        "t0": time.time(),
                    }
                    # Avoid extra subscription DEBUG spam on the next cycle
                    self._suppress_reconnect_logs = True
                    # Keep only a terse debug here to aid deep troubleshooting if needed
                    self.logger().debug(
                        f"Transient WS close detected (code={code}); attempting fast reconnect..."
                    )
                else:
                    self.logger().error(
                        f"Unexpected error with WebSocket connection. Retrying... Error: {e}",
                        exc_info=True
                    )
                # No backoff; reconnect immediately
            finally:
                if ws is not None:
                    await ws.disconnect()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        """
        Process incoming WebSocket messages and route them to the appropriate queue.
        """
        async for ws_response in websocket_assistant.iter_messages():
            try:
                # Any message from server proves connection is alive
                self._last_pong_timestamp = time.time()
                
                # Data should already be decompressed/parsed by WS post-processor (gzip) and assistant
                data = ws_response.data
                if not isinstance(data, dict):
                    # Ignore non-JSON frames to avoid extra branching here
                    continue
                
                # First check if this is a channel message we care about
                channel = self._channel_originating_message(data)
                
                if channel:
                    # This is a data message, put it in the appropriate queue
                    self._message_queue[channel].put_nowait(data)
                else:
                    # Not a recognized channel, handle control messages
                    await self._process_message_for_unknown_channel(data, websocket_assistant)
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Silence logs for WS close code 1003 and re-raise to allow outer loop to handle reconnection
                if self._is_ws_close_code_1003_exception(e):
                    raise
                self.logger().error("Unexpected error in message processing", exc_info=True)
                # Keep minimal behavior on errors

    async def _keep_alive_loop(self, ws: WSAssistant):
        while True:
            try:
                await asyncio.sleep(self._ping_interval)

                # Do not proactively ping; rely on server ping/pong for v1 market WS.
                # Only monitor inactivity and recycle the connection if needed.
                now = time.time()
                if self._last_pong_timestamp and (now - self._last_pong_timestamp) > 60:
                    self.logger().warning("Inactivity threshold exceeded, disconnecting")
                    await ws.disconnect()
                    break

                # Per-channel silence detection disabled for minimal behavior

            except Exception as e:
                self.logger().error(f"Error in keep-alive loop: {e}")
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

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to MBP orderbook (incremental) and trade channels with proper rate limiting.
        MBP = Market By Price incremental updates (100ms for mbp.150)
        """
        try:
            # Store reference to active WS for snapshot requests
            self._active_ws = ws
            
            # Reset sequence tracking on new connection
            self._last_seq_num.clear()
            self._symbols_needing_refresh.clear()
            
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # Pre-populate symbol cache for hot path optimization (HTX uses lowercase)
                self._symbol_to_pair_cache[exchange_symbol.lower()] = trading_pair
                # HTX WS expects lowercase symbols in channel names
                exchange_symbol = exchange_symbol.lower()
                
                # Subscribe to MBP incremental orderbook (tick-by-tick for top levels)
                mbp_incremental_channel = f"market.{exchange_symbol}.mbp.{CONSTANTS.MBP_INCREMENTAL_DEPTH}"
                subscribe_incremental_request: WSJSONRequest = WSJSONRequest({
                    "sub": mbp_incremental_channel,
                    "id": str(uuid.uuid4())
                })
                
                if not self._suppress_reconnect_logs:
                    self.logger().debug(f"Subscribing to MBP incremental: {mbp_incremental_channel}")
                
                throttler = getattr(self._api_factory, "throttler", None)
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await ws.send(subscribe_incremental_request)
                else:
                    await ws.send(subscribe_incremental_request)

                # Subscribe to MBP refresh (100ms full snapshots with deeper levels)
                mbp_refresh_channel = f"market.{exchange_symbol}.mbp.refresh.{CONSTANTS.MBP_REFRESH_DEPTH}"
                subscribe_refresh_request: WSJSONRequest = WSJSONRequest({
                    "sub": mbp_refresh_channel,
                    "id": str(uuid.uuid4())
                })
                
                if not self._suppress_reconnect_logs:
                    self.logger().debug(f"Subscribing to MBP refresh: {mbp_refresh_channel}")
                
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await ws.send(subscribe_refresh_request)
                else:
                    await ws.send(subscribe_refresh_request)

                # Subscribe to trades (still uses the old /ws endpoint implicitly, 
                # but /feed also supports trade.detail)
                subscribe_trade_request: WSJSONRequest = WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.trade.detail",
                    "id": str(uuid.uuid4())
                })
                
                if not self._suppress_reconnect_logs:
                    self.logger().debug(f"Subscribing to trades: market.{exchange_symbol}.trade.detail")
                
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await ws.send(subscribe_trade_request)
                else:
                    await ws.send(subscribe_trade_request)
                
                # No need for explicit snapshot request - mbp.refresh provides them continuously
                

            if self._pending_reconnect_notice is not None:
                try:
                    elapsed = max(0.0, time.time() - self._pending_reconnect_notice.get("t0", time.time()))
                    code = self._pending_reconnect_notice.get("code", "unknown")
                    self.logger().info(
                        f"WS reconnected after transient close (code={code}) in {elapsed:.1f}s; "
                        f"subscribed to public orderbook and trade channels"
                    )
                finally:
                    self._pending_reconnect_notice = None
                    # Clear suppression after successful resubscribe
                    self._suppress_reconnect_logs = False
            elif not self._suppress_reconnect_logs:
                self.logger().info("Subscribed to public orderbook and trade channels...")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", 
                exc_info=True
            )
            raise

    async def _request_mbp_snapshot(self, ws: WSAssistant, exchange_symbol: str):
        """
        Request a full MBP snapshot for a symbol (used for initial sync and gap recovery).
        """
        mbp_channel = f"market.{exchange_symbol}.mbp.{CONSTANTS.MBP_DEPTH}"
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
            if order_book_symbol in self._symbols_needing_refresh:
                self._symbols_needing_refresh.discard(order_book_symbol)
            
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
                if order_book_symbol in self._symbols_needing_refresh:
                    self._symbols_needing_refresh.discard(order_book_symbol)
                
                trading_pair = self._symbol_to_pair_cache.get(order_book_symbol)
                if trading_pair is None:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
                    self._symbol_to_pair_cache[order_book_symbol] = trading_pair
                
                msg_ts = raw_message.get("ts", time.time() * 1000) * 1e-3
                content = {
                    "trading_pair": trading_pair,
                    "update_id": seq_num,
                    "bids": tick.get("bids", []),
                    "asks": tick.get("asks", [])
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
                # This allows us to get real-time top-5 updates while refresh handles sync
                
                if seq_num <= last_seq:
                    # Old/duplicate message - skip
                    return
                
                # Forward-looking message - apply it optimistically
                # Track the sequence for logging but don't block on gaps
                if prev_seq_num != last_seq and order_book_symbol not in self._symbols_needing_refresh:
                    # Log gap at debug level since refresh will auto-correct within 100ms
                    self.logger().debug(
                        f"MBP incremental gap for {order_book_symbol}: had seq={last_seq}, got prevSeq={prev_seq_num}, seqNum={seq_num}"
                    )
                    # Mark for clarity but don't skip - apply optimistically
                    self._symbols_needing_refresh.add(order_book_symbol)
                
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

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        # Server may send ping - respond immediately
        if "ping" in event_message:
            # Echo back the timestamp
            ts = event_message.get("ping")
            pong_payload = {"pong": ts} if ts else {"pong": int(time.time() * 1000)}
            pong_request = WSJSONRequest(payload=pong_payload)
            await websocket_assistant.send(request=pong_request)
            # Record server ping receipt
            self._last_pong_timestamp = time.time()
            return
        
        # HTX v2 format
        action = event_message.get("action")
        if action == "ping":
            # Respond with pong echoing the timestamp
            ts = (event_message.get("data") or {}).get("ts")
            pong = {"action": "pong", "data": {"ts": ts} if ts else {}}
            await websocket_assistant.send(WSJSONRequest(payload=pong))
            # Record server ping receipt
            self._last_pong_timestamp = time.time()
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
                self.logger().warning(
                    f"WebSocket error: {event_message.get('err-msg', 'Unknown error')}",
                    extra={"event": event_message}
                )
                # Disconnect to trigger clean resubscribe when server rejects channels
                if "invalid" in str(event_message.get("err-msg", "")).lower():
                    await websocket_assistant.disconnect()
                return
        except Exception:
            pass