import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coinex import (
    coinex_constants as CONSTANTS,
    coinex_utils as utils,
    coinex_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Public market data for CoinEx spot: order book depth and trades.

    Depth handling is snapshot-only by design. The `depth.subscribe` request sets if_full=True
    (CONSTANTS.WS_DEPTH_FULL_PUSH), so CoinEx pushes a COMPLETE book roughly every 200ms and a
    guaranteed full refresh every minute. That removes any need to rebuild from incremental diffs
    or to verify the CRC32 depth checksum. Anything arriving with is_full=False is refused rather
    than applied — see _parse_order_book_snapshot_message.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinexExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: "CoinexExchange" = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._domain = domain
        self._ping_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0
        # symbol -> trading_pair, cached to keep the message hot path free of async lookups
        self._symbol_to_pair_cache: Dict[str, str] = {}
        self._warned_incremental_push: bool = False

    def _request_id(self) -> int:
        self._next_request_id += 1
        return self._next_request_id

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _trading_pair_for_symbol(self, symbol: str) -> str:
        trading_pair = self._symbol_to_pair_cache.get(symbol)
        if trading_pair is None:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
            self._symbol_to_pair_cache[symbol] = trading_pair
        return trading_pair

    # ------------------------------------------------------------------ REST snapshot

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        # All three params are mandatory on GET /spot/depth (spot/market/http/list-market-depth).
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.PUBLIC_DEPTH_ENDPOINT, domain=self._domain),
            params={
                "market": symbol,
                "limit": CONSTANTS.WS_DEPTH_LEVELS,
                "interval": CONSTANTS.WS_DEPTH_INTERVAL,
            },
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.PUBLIC_DEPTH_ENDPOINT,
        )
        if web_utils.is_error_response(response):
            raise IOError(f"Error fetching CoinEx order book for {trading_pair}: {response}")

        depth = (response.get("data") or {}).get("depth") or {}
        # An empty book is legitimate on a thin market; updated_at may then be absent.
        timestamp_ms = int(depth.get("updated_at") or time.time() * 1e3)
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": timestamp_ms,
                "bids": depth.get("bids") or [],
                "asks": depth.get("asks") or [],
            },
            timestamp=timestamp_ms * 1e-3,
        )

    # ------------------------------------------------------------------ WebSocket

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # ping_timeout=None disables protocol-level ping frames: CoinEx expects an application
        # level `server.ping` message instead (common/ws/ping), which _ping_loop sends.
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=None,
            message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            symbols = [
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=tp)
                for tp in self._trading_pairs
            ]

            # depth.subscribe takes positional params: [market, limit, interval, if_full].
            # Batched because oversized subscribe frames proved unreliable in P1 production, even
            # though CoinEx publishes no per-request market cap.
            for i in range(0, len(symbols), CONSTANTS.WS_MAX_MARKETS_PER_SUBSCRIBE):
                batch = symbols[i:i + CONSTANTS.WS_MAX_MARKETS_PER_SUBSCRIBE]
                market_list = [
                    [s, CONSTANTS.WS_DEPTH_LEVELS, CONSTANTS.WS_DEPTH_INTERVAL, CONSTANTS.WS_DEPTH_FULL_PUSH]
                    for s in batch
                ]
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_DEPTH_SUBSCRIBE,
                    "params": {"market_list": market_list},
                    "id": self._request_id(),
                }))

            # deals.subscribe takes a plain market name list.
            for i in range(0, len(symbols), CONSTANTS.WS_MAX_MARKETS_PER_SUBSCRIBE):
                batch = symbols[i:i + CONSTANTS.WS_MAX_MARKETS_PER_SUBSCRIBE]
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_DEALS_SUBSCRIBE,
                    "params": {"market_list": batch},
                    "id": self._request_id(),
                }))

            self.logger().info(f"Subscribed to CoinEx depth and trade channels for {len(symbols)} markets.")

            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.create_task(self._ping_loop(ws))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinEx public channels...")
            raise

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """Application-level keepalive: CoinEx answers `server.ping` with a pong payload."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                await ws.send(WSJSONRequest(payload={
                    "method": CONSTANTS.WS_PING_METHOD,
                    "params": {},
                    "id": self._request_id(),
                }))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinEx public WS keepalive stopped: {e}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant) -> None:
        """
        Override of the base loop: CoinEx frames arrive as gzip-compressed binary, so each one has
        to be inflated and parsed before the standard channel dispatch can run.
        """
        async for ws_response in websocket_assistant.iter_messages():
            raw = ws_response.data
            if raw is None:
                continue
            decoded = utils.decompress_ws_message(raw)
            if isinstance(decoded, (bytes, bytearray)):
                self.logger().debug("Skipping undecodable CoinEx frame.")
                continue
            if isinstance(decoded, str):
                try:
                    data = json.loads(decoded)
                except Exception:
                    self.logger().debug(f"Skipping non-JSON CoinEx frame: {decoded[:120]}")
                    continue
            else:
                data = decoded

            channel = self._channel_originating_message(event_message=data)
            if channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(data)
            else:
                await self._process_message_for_unknown_channel(
                    event_message=data, websocket_assistant=websocket_assistant
                )

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        method = event_message.get("method")
        if method == CONSTANTS.WS_DEPTH_UPDATE:
            return self._snapshot_messages_queue_key
        if method == CONSTANTS.WS_DEALS_UPDATE:
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ) -> None:
        # Subscribe acknowledgements and server.ping replies both come back as {"id": n, "code": c}.
        if "id" in event_message and "method" not in event_message:
            code = event_message.get("code")
            if code not in (None, CONSTANTS.RET_CODE_OK):
                raise IOError(
                    f"CoinEx public WS request {event_message.get('id')} failed: "
                    f"{event_message.get('message')} (code {code})"
                )
            return
        self.logger().debug(f"Unrecognised CoinEx public WS message: {event_message}")

    async def _parse_order_book_snapshot_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ) -> None:
        data = raw_message.get("data") or {}
        symbol = data.get("market")
        depth = data.get("depth") or {}
        if symbol is None:
            return

        # Refuse to apply an incremental push as if it were a snapshot: doing so would silently
        # drop every level absent from the diff and leave a corrupt book. The subscription asks
        # for if_full=True, so this should never fire.
        if data.get("is_full") is not True:
            if not self._warned_incremental_push:
                self._warned_incremental_push = True
                self.logger().warning(
                    "CoinEx sent an incremental depth push despite an if_full=True subscription. "
                    "Ignoring it to avoid corrupting the book; depth will refresh on the next full push."
                )
            return

        bids = depth.get("bids") or []
        asks = depth.get("asks") or []
        # One-sided books are legitimate on thin CoinEx markets (proven in P1); an empty side is
        # forwarded as-is and resolves to no liquidity on that side.
        trading_pair = await self._trading_pair_for_symbol(symbol)
        timestamp_ms = int(depth.get("updated_at") or time.time() * 1e3)

        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": timestamp_ms,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp_ms * 1e-3,
        ))

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue) -> None:
        data = raw_message.get("data") or {}
        symbol = data.get("market")
        if symbol is None:
            return
        trading_pair = await self._trading_pair_for_symbol(symbol)

        for deal in data.get("deal_list") or []:
            timestamp_ms = int(deal["created_at"])
            # `side` is the TAKER side (spot/market/ws/market-deals).
            trade_type = TradeType.BUY if deal.get("side") == "buy" else TradeType.SELL
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": deal["deal_id"],
                    "update_id": timestamp_ms,
                    "price": deal["price"],
                    "amount": deal["amount"],
                },
                timestamp=timestamp_ms * 1e-3,
            ))

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None) -> None:
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        self._symbol_to_pair_cache.clear()
