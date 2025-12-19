"""
MEXC Websocket Subscription Manager

Handles multiple websocket connections to respect MEXC's subscription limits.
MEXC allows maximum 30 subscriptions per websocket connection.

This manager automatically creates additional connections when needed
and distributes subscriptions across them.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

from . import mexc_constants as CONSTANTS
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


@dataclass
class WSConnectionInfo:
    """Information about a websocket connection and its subscriptions"""
    ws_assistant: WSAssistant
    subscriptions: Set[str]
    trade_subscriptions: List[str]
    depth_subscriptions: List[str]
    connection_id: int

    @property
    def total_subscriptions(self) -> int:
        return len(self.subscriptions)

    @property
    def can_add_pair(self) -> bool:
        # Each trading pair needs 2 subscriptions (trade + depth)
        return self.total_subscriptions + 2 <= CONSTANTS.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION


class MexcWebsocketSubscriptionManager:
    """
    Manages multiple websocket connections for MEXC to handle subscription limits.
    
    MEXC allows maximum 30 subscriptions per connection. When using the orchestrator
    with many trading pairs, we need multiple connections to stay within limits.
    """
    
    _logger: Optional[HummingbotLogger] = None

    def __init__(self, api_factory: WebAssistantsFactory, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        self._api_factory = api_factory
        self._domain = domain
        self._connections: Dict[int, WSConnectionInfo] = {}
        self._next_connection_id = 1
        self._connector = None  # Will be set by the data source

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def set_connector(self, connector):
        """Set the connector reference for symbol conversion"""
        self._connector = connector

    async def create_connection(self) -> WSConnectionInfo:
        """Create a new websocket connection"""
        ws_assistant = await self._api_factory.get_ws_assistant()
        
        # Try new endpoint first, then fallback to legacy
        try:
            await ws_assistant.connect(
                ws_url=CONSTANTS.WSS_API_URL.format(self._domain),
                ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
            )
        except Exception:
            await ws_assistant.connect(
                ws_url=CONSTANTS.WSS_URL.format(self._domain),
                ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
            )

        connection_info = WSConnectionInfo(
            ws_assistant=ws_assistant,
            subscriptions=set(),
            trade_subscriptions=[],
            depth_subscriptions=[],
            connection_id=self._next_connection_id
        )
        
        self._connections[self._next_connection_id] = connection_info
        self._next_connection_id += 1
        
        self.logger().info(f"Created MEXC websocket connection #{connection_info.connection_id}")
        return connection_info

    async def subscribe_to_pairs(self, trading_pairs: List[str]) -> List[WSConnectionInfo]:
        """
        Subscribe to trading pairs across multiple connections as needed.
        
        Returns list of connections that were used for subscriptions.
        """
        if not self._connector:
            raise RuntimeError("Connector must be set before subscribing")

        # Wait for the symbol map to be initialized (exchange info fetched)
        # This handles the race condition where data source starts before exchange info is ready
        max_wait = 30  # Maximum 30 seconds
        wait_interval = 0.5
        waited = 0
        while waited < max_wait:
            try:
                # Test if symbol map is ready by trying to convert first trading pair
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pairs[0])
                break  # Symbol map is ready
            except KeyError:
                self.logger().debug(f"Waiting for MEXC symbol map to initialize... ({waited:.1f}s)")
                await asyncio.sleep(wait_interval)
                waited += wait_interval
        else:
            raise RuntimeError(f"MEXC symbol map not initialized after {max_wait}s")

        used_connections = []
        current_connection = None

        for trading_pair in trading_pairs:
            # Find or create a connection that can handle this pair
            if current_connection is None or not current_connection.can_add_pair:
                current_connection = await self._find_or_create_available_connection()
                if current_connection not in used_connections:
                    used_connections.append(current_connection)

            # Add subscriptions for this trading pair
            await self._add_pair_to_connection(current_connection, trading_pair)

        # Send all subscription requests
        for connection in used_connections:
            await self._send_subscriptions(connection)

        self.logger().info(
            f"Subscribed to {len(trading_pairs)} trading pairs across "
            f"{len(used_connections)} MEXC websocket connections"
        )
        
        return used_connections

    async def _find_or_create_available_connection(self) -> WSConnectionInfo:
        """Find an existing connection with capacity or create a new one"""
        # Look for existing connection with capacity
        for connection in self._connections.values():
            if connection.can_add_pair:
                return connection
        
        # No available connection, create new one
        return await self.create_connection()

    async def _add_pair_to_connection(self, connection: WSConnectionInfo, trading_pair: str):
        """Add a trading pair's subscriptions to a connection"""
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        
        # Create subscription strings
        trade_subscription = f"spot@public.aggre.deals.v3.api.pb@10ms@{symbol}"
        depth_subscription = f"spot@public.aggre.depth.v3.api.pb@100ms@{symbol}"
        
        # Add to connection
        connection.trade_subscriptions.append(trade_subscription)
        connection.depth_subscriptions.append(depth_subscription)
        connection.subscriptions.add(trade_subscription)
        connection.subscriptions.add(depth_subscription)
        
        self.logger().debug(
            f"Added {trading_pair} to connection #{connection.connection_id} "
            f"({connection.total_subscriptions}/{CONSTANTS.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION})"
        )

    async def _send_subscriptions(self, connection: WSConnectionInfo):
        """Send subscription requests for a connection"""
        if not connection.trade_subscriptions and not connection.depth_subscriptions:
            return

        try:
            # Send trade subscriptions
            if connection.trade_subscriptions:
                trade_payload = {
                    "method": "SUBSCRIPTION",
                    "params": connection.trade_subscriptions,
                    "id": connection.connection_id * 2 - 1  # Unique ID for trades
                }
                await connection.ws_assistant.send(WSJSONRequest(payload=trade_payload))

            # Send depth subscriptions
            if connection.depth_subscriptions:
                depth_payload = {
                    "method": "SUBSCRIPTION",
                    "params": connection.depth_subscriptions,
                    "id": connection.connection_id * 2  # Unique ID for depth
                }
                await connection.ws_assistant.send(WSJSONRequest(payload=depth_payload))

            self.logger().info(
                f"Connection #{connection.connection_id}: Sent {len(connection.trade_subscriptions)} trade + "
                f"{len(connection.depth_subscriptions)} depth subscriptions "
                f"(total: {connection.total_subscriptions}/{CONSTANTS.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION})"
            )

        except Exception as e:
            self.logger().error(
                f"Failed to send subscriptions on connection #{connection.connection_id}: {e}",
                exc_info=True
            )
            raise

    def get_all_connections(self) -> List[WSConnectionInfo]:
        """Get all active websocket connections"""
        return list(self._connections.values())

    async def close_all_connections(self):
        """Close all websocket connections"""
        for connection in self._connections.values():
            try:
                await connection.ws_assistant.disconnect()
            except Exception as e:
                self.logger().warning(f"Error closing connection #{connection.connection_id}: {e}")
        
        self._connections.clear()
        self.logger().info("Closed all MEXC websocket connections")

    def get_subscription_summary(self) -> Dict[str, any]:
        """Get a summary of current subscriptions"""
        return {
            "total_connections": len(self._connections),
            "connections": [
                {
                    "id": conn.connection_id,
                    "subscriptions": conn.total_subscriptions,
                    "capacity_used": f"{conn.total_subscriptions}/{CONSTANTS.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION}",
                    "utilization_pct": round(conn.total_subscriptions / CONSTANTS.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION * 100, 1)
                }
                for conn in self._connections.values()
            ]
        }
