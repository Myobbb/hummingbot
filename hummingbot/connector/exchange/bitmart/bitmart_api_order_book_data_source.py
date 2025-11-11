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
        # Per-trading_pair last applied version (Depth-Increase only)
        self._last_depth_version: Dict[str, int] = {}
        self._active_ws: Optional[WSAssistant] = None

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
            # Update liveness timestamp
            try:
                self._last_depth_update_ts[trading_pair] = time.time()
            except Exception:
                pass
            # Heartbeat (empty) — do not emit, but keep local version unchanged per BitMart doc (version equals previous)
            if (not bids_list) and (not asks_list):
                continue
            # Version sequencing
            last_ver = self._last_depth_version.get(trading_pair)
            new_ver = int(version)
            if last_ver is None:
                # We don't have a baseline; fetch snapshot to seed and skip this diff
                await self._refresh_snapshot_for_pair(trading_pair, symbol)
                continue
            if new_ver <= last_ver:
                # Old or duplicate
                continue
            if new_ver != last_ver + 1:
                # Gap detected, refresh snapshot and skip
                await self._refresh_snapshot_for_pair(trading_pair, symbol)
                continue
            # Build and emit DIFF
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
            # Advance version
            self._last_depth_version[trading_pair] = new_ver
            message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_updates: Dict[str, Any] = raw_message["data"]

        for diff_data in diff_updates:
            timestamp: float = int(diff_data["ms_t"]) * 1e-3
            update_id: int = int(diff_data["ms_t"])
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
                symbol=diff_data["symbol"])

            # Discard heartbeats with empty asks/bids; update liveness timestamp regardless
            bids_list = diff_data.get("bids", [])
            asks_list = diff_data.get("asks", [])
            try:
                self._last_depth_update_ts[trading_pair] = time.time()
            except Exception:
                pass
            if (not bids_list) and (not asks_list):
                continue
            # For Depth-Increase 'snapshot' set base version
            if raw_message.get("table") == CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME:
                try:
                    self._last_depth_version[trading_pair] = int(diff_data.get("version"))
                except Exception:
                    self._last_depth_version.pop(trading_pair, None)

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
        try:
            if self._use_depth_increase and self._active_ws is not None:
                payload = {"op": "request", "args": [f"{CONSTANTS.PUBLIC_DEPTH_INCREASE_CHANNEL_NAME}:{symbol}"]}
                req = WSJSONRequest(payload=payload)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await self._active_ws.send(req)
                return
        except Exception:
            # Fall back to REST below
            pass
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
                }]
            }
            self._message_queue[self._snapshot_messages_queue_key].put_nowait(synthetic)
        except Exception:
            # If REST fails, let watchdog handle later
            pass

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
            # Initialize per-pair liveness timestamps
            now = time.time()
            for tp in self._trading_pairs:
                self._last_depth_update_ts[tp] = now
                self._last_depth_version.pop(tp, None)

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

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        # Start keepalive task
        self._keepalive_task = asyncio.create_task(self._keepalive_ping_loop(websocket_assistant))
        # Start per-symbol depth watchdog
        self._depth_watchdog_task = asyncio.create_task(self._depth_watchdog_loop(websocket_assistant))
        
        try:
            self._active_ws = websocket_assistant
            async for ws_response in websocket_assistant.iter_messages():
                # Check if background tasks failed and need to trigger reconnect
                if self._keepalive_task and self._keepalive_task.done():
                    # Keepalive task exited - check for exception
                    try:
                        self._keepalive_task.result()  # Will raise if task failed
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self.logger().warning(f"BitMart public WS keepalive failed: {e}")
                        raise
                
                if self._depth_watchdog_task and self._depth_watchdog_task.done():
                    # Depth watchdog task exited - check for exception
                    try:
                        self._depth_watchdog_task.result()  # Will raise if task failed
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self.logger().warning(f"BitMart public WS depth watchdog failed: {e}")
                        raise
                
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
        finally:
            # Clean up tasks
            if self._keepalive_task is not None and not self._keepalive_task.done():
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except Exception:
                    pass
            self._keepalive_task = None
            if self._depth_watchdog_task is not None and not self._depth_watchdog_task.done():
                self._depth_watchdog_task.cancel()
                try:
                    await self._depth_watchdog_task
                except Exception:
                    pass
            self._depth_watchdog_task = None
            self._active_ws = None

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

    async def _keepalive_ping_loop(self, ws: WSAssistant):
        try:
            while True:
                await asyncio.sleep(1.0)
                # If idle for >= interval, send text 'ping'
                last_recv = getattr(ws, "last_recv_time", 0) or 0
                now = time.time()
                # Send at most one ping per interval and only when idle
                if (now - last_recv) >= self._PING_INTERVAL_SECONDS and (now - self._last_ping_sent_time) >= self._PING_INTERVAL_SECONDS:
                    try:
                        await ws.send(WSPlainTextRequest(payload="ping"))
                        self._last_ping_sent_time = now
                        self.logger().debug("BitMart public WS: sent ping")
                    except Exception:
                        # Force reconnect by raising
                        raise
                # If still no messages beyond max idle threshold, force reconnect (no messages at all, including pongs)
                if (now - last_recv) >= self._FORCE_RECONNECT_IDLE_SECONDS:
                    self.logger().warning("BitMart public WS: no messages for 30s, forcing reconnect")
                    raise ConnectionError("BitMart WS idle exceeded threshold; forcing reconnect")

        except asyncio.CancelledError:
            raise
        except Exception:
            # Bubble up to reconnect loop
            raise

    async def _depth_watchdog_loop(self, ws: WSAssistant):
        """
        Detect symbols whose depth stream has stalled (while trades or other traffic may still flow)
        and attempt a targeted resubscribe plus REST snapshot refresh.
        If multiple symbols are stale or resubscribe fails, force full reconnection.
        """
        try:
            while True:
                await asyncio.sleep(5.0)
                now = time.time()
                stale_pairs = []
                
                # Check all trading pairs for staleness
                for trading_pair in list(self._trading_pairs):
                    last_ts = float(self._last_depth_update_ts.get(trading_pair, 0.0) or 0.0)
                    if last_ts <= 0:
                        continue
                    if (now - last_ts) >= self._DEPTH_STALENESS_SECONDS:
                        stale_pairs.append(trading_pair)
                
                if not stale_pairs:
                    continue
                
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
                        
                        # Refresh snapshot using WS request (Depth-Increase) or REST fallback
                        try:
                            await self._refresh_snapshot_for_pair(trading_pair, symbol)
                            self.logger().warning(f"BitMart depth watchdog: resubscribed and refreshed {trading_pair}")
                        except Exception:
                            pass
                        
                        # Reset timer to avoid repeated attempts
                        self._last_depth_update_ts[trading_pair] = now
                        
                    except Exception as e:
                        self.logger().warning(f"BitMart depth watchdog: failed to refresh {trading_pair}: {e}")
                        # If we can't even send resubscribe, force full reconnect
                        raise ConnectionError(f"BitMart depth watchdog failed to resubscribe; forcing reconnect")
                        
        except asyncio.CancelledError:
            raise
        except Exception:
            # Escalate to reconnect; the outer caller will handle
            raise

    async def listen_for_subscriptions(self):
        """
        Override to add graceful transient reconnect/backoff and keepalive-driven reconnects.
        """
        ws: Optional[WSAssistant] = None
        while True:
            reconnect_delay = 1.0
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
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
                await self._sleep(reconnect_delay)
                ws and await ws.disconnect()