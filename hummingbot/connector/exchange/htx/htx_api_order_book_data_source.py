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

    def _is_ws_close_code_1003_exception(self, exc: Exception) -> bool:
        """
        Return True if the given exception string indicates a WebSocket closed event with code 1000 or 1003.
        We match the diagnostic message produced by the shared WS connection layer. Treat 1000 (normal closure)
        and 1003 (unsupported data / policy) as expected reconnect scenarios to avoid noisy error logs.
        """
        try:
            text = str(exc)
        except Exception:
            return False
        if "Close code = 1000" in text or "Close code = 1003" in text:
            return True
        match = re.search(r"Close code\s*=\s*(\d+)", text)
        return bool(match and match.group(1) in {"1000", "1003"})

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()

        throttler = getattr(self._api_factory, "throttler", None)
        
        # IMPORTANT: Disable protocol-level ping; HTX uses JSON ping/pong
        ws_url = CONSTANTS.WS_PUBLIC_URL
        connection_params = {
            "ws_url": ws_url,
            "ping_timeout": None,  # Disable protocol ping, use JSON ping instead
            "message_timeout": 60,
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
                # Silence disconnect/reconnect logs for WS close code 1003
                if not self._is_ws_close_code_1003_exception(e):
                    self.logger().error(
                        f"Unexpected error with WebSocket connection. Retrying... Error: {e}",
                        exc_info=True
                    )
                else:
                    # Flag next subscription cycle to avoid logging reconnection spam
                    self._suppress_reconnect_logs = True
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
                if self._last_pong_timestamp and (now - self._last_pong_timestamp) > 180:
                    self.logger().error("Inactivity threshold exceeded, disconnecting")
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
        Subscribe to orderbook and trade channels with proper rate limiting.
        """
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # HTX WS expects lowercase symbols in channel names
                exchange_symbol = exchange_symbol.lower()
                
                # Subscribe to orderbook
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest({
                    "sub": f"market.{exchange_symbol}.depth.step0",
                    "id": str(uuid.uuid4())
                })
                
                if not self._suppress_reconnect_logs:
                    self.logger().debug(f"Subscribing to orderbook: market.{exchange_symbol}.depth.step0")
                
                throttler = getattr(self._api_factory, "throttler", None)
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await ws.send(subscribe_orderbook_request)
                else:
                    await ws.send(subscribe_orderbook_request)

                # Subscribe to trades
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
                

            if not self._suppress_reconnect_logs:
                self.logger().info("Subscribed to public orderbook and trade channels...")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", 
                exc_info=True
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identify which channel a message belongs to based on the channel name.
        """
        channel = event_message.get("ch", "")
        
        # HTX uses "depth.step0" for orderbook and "trade.detail" for trades
        if "depth.step0" in channel:
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
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
        
        for data in raw_message["tick"]["data"]:
            trade_message: OrderBookMessage = self.trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": trading_pair}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse orderbook snapshot messages.
        HTX sends snapshots through the depth channel, not diffs.
        """
        msg_channel = raw_message["ch"]
        order_book_symbol = msg_channel.split(".")[1]
        snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
            msg=raw_message,
            metadata={
                "trading_pair": await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
            }
        )
        message_queue.put_nowait(snapshot_msg)

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        # Server may send ping - respond immediately
        if "ping" in event_message:
            # Echo back the timestamp
            ts = event_message.get("ping")
            pong_payload = {"pong": ts} if ts else {"pong": int(time.time() * 1000)}
            pong_request = WSJSONRequest(payload=pong_payload)
            await websocket_assistant.send(request=pong_request)
            # Record server ping receipt
            return
        
        # HTX v2 format
        action = event_message.get("action")
        if action == "ping":
            # Respond with pong echoing the timestamp
            ts = (event_message.get("data") or {}).get("ts")
            pong = {"action": "pong", "data": {"ts": ts} if ts else {}}
            await websocket_assistant.send(WSJSONRequest(payload=pong))
            # Record server ping receipt
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