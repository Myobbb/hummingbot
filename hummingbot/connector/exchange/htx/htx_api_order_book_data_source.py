import asyncio
import uuid
import time
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
        
        # Keep-alive tracking
        self._last_ping_timestamp = 0
        self._last_pong_timestamp = 0
        self._ping_interval = 20  # HTX recommends 20-30 seconds
        self._pong_timeout = 10  # If no pong received within 10s, reconnect
        

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        
        # Add small jitter to avoid connection storms
        import random
        await asyncio.sleep(random.uniform(0.1, 0.5))
        
        throttler = getattr(self._api_factory, "throttler", None)
        
        # Important: Set ping_timeout to None to handle ping/pong manually
        # HTX uses JSON ping/pong, not WebSocket protocol ping/pong
        connection_params = {
            "ws_url": CONSTANTS.WS_PUBLIC_URL,
            "ping_timeout": None,  # Disable protocol-level ping, use JSON ping instead
            "message_timeout": 60,
            "ws_headers": {
                "Accept-Encoding": "gzip",
            },
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
        Override to add keep-alive monitoring task
        """
        ws = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                
                # Create tasks for message processing and keep-alive
                message_processor_task = asyncio.create_task(
                    self._process_websocket_messages(websocket_assistant=ws, queue=self._message_queue)
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
                
                # Re-raise any exceptions from completed tasks
                for task in done:
                    exception = task.exception()
                    if exception:
                        raise exception
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(
                    f"Unexpected error with WebSocket connection. Retrying after 5 seconds... Error: {e}",
                    exc_info=True
                )
                await self._sleep(5.0)
            finally:
                if ws is not None:
                    await ws.disconnect()

    async def _keep_alive_loop(self, ws: WSAssistant):
        """
        Dedicated task for sending periodic pings and monitoring connection health
        """
        while True:
            try:
                await asyncio.sleep(self._ping_interval)
                
                # Check if we need to send a ping
                current_time = time.time()
                
                # Check pong timeout
                if self._last_ping_timestamp > 0:
                    time_since_ping = current_time - self._last_ping_timestamp
                    time_since_pong = current_time - self._last_pong_timestamp
                    
                    if time_since_ping > self._pong_timeout and time_since_pong > self._pong_timeout:
                        self.logger().warning(
                            f"No pong received for {time_since_pong:.1f}s, disconnecting..."
                        )
                        await ws.disconnect()
                        break
                
                # Send ping
                ping_payload = {"ping": int(current_time * 1000)}
                ping_request = WSJSONRequest(payload=ping_payload)
                
                await ws.send(request=ping_request)
                self._last_ping_timestamp = current_time
                
                self.logger().debug(f"Sent ping at {current_time}")
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in keep-alive loop: {e}")
                break

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            # Wait for connection to stabilize
            await asyncio.sleep(1.0)
            
            subscription_requests = []
            
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # HTX requires lowercase symbols in channel names
                exchange_symbol = exchange_symbol.lower()
                
                # Prepare subscription requests
                subscription_requests.append({
                    "sub": f"market.{exchange_symbol}.depth.step0",
                    "id": str(uuid.uuid4())
                })
                subscription_requests.append({
                    "sub": f"market.{exchange_symbol}.trade.detail",
                    "id": str(uuid.uuid4())
                })
            
            # Send subscriptions with proper rate limiting
            throttler = getattr(self._api_factory, "throttler", None)
            
            for request_data in subscription_requests:
                subscribe_request = WSJSONRequest(request_data)
                
                self.logger().debug(f"Subscribing to: {request_data['sub']}")
                
                if throttler is not None:
                    async with throttler.execute_task(CONSTANTS.WS_REQUEST_LIMIT_ID):
                        await ws.send(subscribe_request)
                else:
                    await ws.send(subscribe_request)
                
                # Important: Add delay between subscriptions to avoid overwhelming the server
                # HTX may close connection with 1003 if subscriptions are sent too fast
                await asyncio.sleep(0.5)
            
            self.logger().info("Subscribed to public orderbook and trade channels...")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", 
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Override the base implementation to handle messages properly
        """
        async for ws_response in websocket_assistant.iter_messages():
            try:
                data = ws_response.data
                
                # Handle subscription confirmation/error
                if "status" in data:
                    if data["status"] == "ok":
                        # Subscription successful
                        if "subbed" in data:
                            self.logger().debug(f"Successfully subscribed to {data['subbed']}")
                        continue
                    elif data["status"] == "error":
                        self.logger().error(f"Subscription error: {data}")
                        continue
                
                # Handle ping/pong at message processing level too
                if "ping" in data:
                    pong_payload = {"pong": data["ping"]}
                    pong_request = WSJSONRequest(payload=pong_payload)
                    await websocket_assistant.send(request=pong_request)
                    self.logger().debug(f"Responded to ping with pong: {data['ping']}")
                    continue
                
                if "pong" in data:
                    self._last_pong_timestamp = time.time()
                    self.logger().debug(f"Received pong: {data['pong']}")
                    continue
                
                # Process actual channel data
                channel = self._channel_originating_message(data)
                if channel == self._diff_messages_queue_key:
                    await self._parse_order_book_diff_message(data, queue)
                elif channel == self._trade_messages_queue_key:
                    await self._parse_trade_message(data, queue)
                    
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in message processing", exc_info=True)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message.get("ch", "")
        retval = ""
        if channel.endswith(self._trade_messages_queue_key):
            retval = self._trade_messages_queue_key
        if channel.endswith(self._diff_messages_queue_key):
            retval = self._diff_messages_queue_key
        return retval

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        ex_symbol = raw_message["ch"].split(".")[1]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
        for data in raw_message["tick"]["data"]:
            trade_message: OrderBookMessage = self.trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": trading_pair}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        msg_channel = raw_message["ch"]
        order_book_symbol = msg_channel.split(".")[1]
        snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
            msg=raw_message,
            metadata={
                "trading_pair": await self._connector.trading_pair_associated_to_exchange_symbol(order_book_symbol)
            }
        )
        message_queue.put_nowait(snapshot_msg)

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        HTX WebSocket provides snapshots through the depth channel, not separate REST calls
        """
        pass

    def snapshot_message_from_exchange(self,
                                       msg: Dict[str, Any],
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
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
        if metadata:
            msg.update(metadata)
        msg_ts = int(round(msg["ts"] / 1e3))
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