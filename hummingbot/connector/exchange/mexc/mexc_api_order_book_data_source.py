import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from . import mexc_constants as CONSTANTS, mexc_web_utils as web_utils
from .mexc_order_book import MexcOrderBook
from .mexc_websocket_subscription_manager import MexcWebsocketSubscriptionManager, WSConnectionInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange


class MexcAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'MexcExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory
        
        # Initialize websocket subscription manager for handling multiple connections
        self._ws_manager = MexcWebsocketSubscriptionManager(api_factory, domain)
        self._ws_manager.set_connector(connector)
        self._active_connections: List[WSConnectionInfo] = []

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "1000"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
            headers={"Content-Type": "application/json"}
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        This method is called by the base class but we override it to use our subscription manager.
        The ws parameter is ignored since we manage our own connections.
        """
        try:
            # Use the subscription manager to handle multiple connections
            self._active_connections = await self._ws_manager.subscribe_to_pairs(self._trading_pairs)
            
            # Log subscription summary
            summary = self._ws_manager.get_subscription_summary()
            self.logger().info(f"MEXC subscription summary: {summary}")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def listen_for_subscriptions(self):
        """
        Override the base class method to handle multiple websocket connections.
        """
        while True:
            try:
                # Create a dummy websocket for the base class _subscribe_channels call
                # This will trigger our custom subscription logic
                dummy_ws = await self._api_factory.get_ws_assistant()
                await self._subscribe_channels(dummy_ws)
                
                # Process messages from all active connections concurrently
                if self._active_connections:
                    tasks = [
                        self._process_websocket_messages(websocket_assistant=conn.ws_assistant)
                        for conn in self._active_connections
                    ]
                    # Wait for any task to complete (usually means connection closed)
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    
                    # Cancel remaining tasks
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    
                    # Check if any task completed with an exception
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            self.logger().warning(f"Websocket connection closed: {e}")
                            break
                else:
                    self.logger().warning("No active websocket connections")
                    await asyncio.sleep(5)
                    
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await asyncio.sleep(5.0)
            finally:
                # Clean up connections
                await self._cleanup_connections()

    async def _cleanup_connections(self):
        """Clean up websocket connections"""
        try:
            await self._ws_manager.close_all_connections()
            self._active_connections.clear()
        except Exception as e:
            self.logger().warning(f"Error during connection cleanup: {e}")

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        This method is still needed by the base class but we don't use it directly.
        Our subscription manager handles connection creation.
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # Try new endpoint first, then fallback to legacy
        try:
            await ws.connect(ws_url=CONSTANTS.WSS_API_URL.format(self._domain),
                             ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        except Exception:
            await ws.connect(ws_url=CONSTANTS.WSS_URL.format(self._domain),
                             ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        # Retry logic for order book snapshot fetch to handle transient network errors (e.g., 504)
        max_retries = 5
        retry_delay = 2  # Start with 2 seconds
        last_exception = None

        for attempt in range(max_retries):
            try:
                snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
                snapshot_timestamp: float = time.time()
                snapshot_msg: OrderBookMessage = MexcOrderBook.snapshot_message_from_exchange(
                    snapshot,
                    snapshot_timestamp,
                    metadata={"trading_pair": trading_pair}
                )
                if attempt > 0:
                    self.logger().info(f"Successfully fetched order book snapshot for {trading_pair} after {attempt + 1} attempts")
                return snapshot_msg
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    self.logger().warning(
                        f"Failed to fetch order book snapshot for {trading_pair} "
                        f"(attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30s
                else:
                    self.logger().error(
                        f"Failed to fetch order book snapshot for {trading_pair} "
                        f"after {max_retries} attempts: {e}"
                    )

        # All retries failed, re-raise the last exception
        raise last_exception

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if isinstance(raw_message, dict) and "code" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message.get("s", ""))
            for single_msg in raw_message.get('d', {}).get('deals', []):
                trade_message = MexcOrderBook.trade_message_from_exchange(
                    single_msg, timestamp=raw_message.get('t'), metadata={"trading_pair": trading_pair})
                message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if isinstance(raw_message, dict) and "code" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message.get("s", ""))
            order_book_message: OrderBookMessage = MexcOrderBook.diff_message_from_exchange(
                raw_message, raw_message.get('t'), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if isinstance(event_message, dict) and "code" not in event_message:
            event_type = event_message.get("c", "") or event_message.get("channel", "")
            if (CONSTANTS.DIFF_EVENT_TYPE in event_type) or ("aggre.depth" in event_type) or ("depth" in event_type):
                channel = self._diff_messages_queue_key
            else:
                channel = self._trade_messages_queue_key
        return channel