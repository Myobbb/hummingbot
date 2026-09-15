import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS, coinex_utils as utils
from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private CoinEx spot stream: order updates and balance updates.

    The socket is authenticated once with `server.sign` (signing the timestamp alone), after which
    three channels are subscribed with EMPTY filter lists — `market_list: []` for orders and user
    deals, `ccy_list: []` for balances — which CoinEx documents as "subscribe to all". That keeps
    the stream independent of how many pairs the strategies happen to trade, so there is no
    per-symbol subscription budget to manage on a shared connector.

    Channels:
      order.update       order lifecycle events (put / update / modify / finish)
      balance.update     per-asset available/frozen
      user_deals.update  individual fills, with fee and maker/taker role
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: CoinexAuth,
        connector: "CoinexExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ping_task: Optional[asyncio.Task] = None
        self._next_request_id: int = 0

    def _request_id(self) -> int:
        self._next_request_id += 1
        return self._next_request_id

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=None,
            message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
        )
        await self._authenticate(ws)
        return ws

    async def _authenticate(self, ws: WSAssistant) -> None:
        """Send server.sign and block until CoinEx accepts it."""
        request_id = self._request_id()
        await ws.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_SIGN_METHOD,
            "params": self._auth.get_ws_auth_payload(),
            "id": request_id,
        }))

        async for ws_response in ws.iter_messages():
            data = self._decode(ws_response.data)
            if data is None:
                continue
            if data.get("id") == request_id:
                if data.get("code") != CONSTANTS.RET_CODE_OK:
                    raise IOError(
                        f"CoinEx private WS authentication failed: {data.get('message')} "
                        f"(code {data.get('code')})"
                    )
                self.logger().info("Authenticated on the CoinEx private WebSocket.")
                return
        raise IOError("CoinEx private WS closed before the authentication response arrived.")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            # Empty lists mean "all markets" / "all assets" (spot/order/ws/user-order,
            # assets/balance/ws/spot_balance).
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_ORDER_SUBSCRIBE,
                "params": {"market_list": []},
                "id": self._request_id(),
            }))
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_BALANCE_SUBSCRIBE,
                "params": {"ccy_list": []},
                "id": self._request_id(),
            }))
            # Per-fill stream. Without it, fills would only surface on the REST poll of
            # /spot/order-deals, which is far too slow for the arbitrage strategies.
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_USER_DEALS_SUBSCRIBE,
                "params": {"market_list": []},
                "id": self._request_id(),
            }))
            self.logger().info("Subscribed to CoinEx private order, balance and user-deal channels.")

            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.create_task(self._ping_loop(websocket_assistant))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinEx private channels...")
            raise

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        """
        Use CoinEx's application-level `server.ping` rather than the base class's protocol-level
        WebSocket ping frame.

        The base calls this right after subscribing "to update last_recv_timestamp". A protocol
        ping is answered with a PONG control frame, which does not necessarily surface as a
        message and so may not refresh that liveness accounting. `server.ping` returns a normal
        payload that does. (A protocol ping is harmless to CoinEx — verified live — this is about
        the liveness bookkeeping, not compatibility.)
        """
        await websocket_assistant.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_PING_METHOD, "params": {}, "id": self._request_id(),
        }))

    async def _ping_loop(self, ws: WSAssistant) -> None:
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                await self._send_ping(ws)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinEx private WS keepalive stopped: {e}")

    @staticmethod
    def _decode(raw: Any) -> Optional[Dict[str, Any]]:
        """Inflate a gzip frame and parse it; return None for anything unusable."""
        if raw is None:
            return None
        decoded = utils.decompress_ws_message(raw)
        if isinstance(decoded, (bytes, bytearray)):
            return None
        if isinstance(decoded, str):
            try:
                return json.loads(decoded)
            except Exception:
                return None
        return decoded if isinstance(decoded, dict) else None

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        async for ws_response in websocket_assistant.iter_messages():
            data = self._decode(ws_response.data)
            if data is None:
                continue
            # NOTE: liveness is tracked by the base's last_recv_time property, which reads
            # self._ws_assistant.last_recv_time (updated by the connection on every frame).
            # Assigning self._last_recv_time here would be dead code — nothing reads it.
            method = data.get("method")
            if method in (CONSTANTS.WS_ORDER_UPDATE,
                          CONSTANTS.WS_BALANCE_UPDATE,
                          CONSTANTS.WS_USER_DEALS_UPDATE):
                queue.put_nowait(data)
            elif "id" in data and method is None:
                # Subscribe ack or ping reply.
                if data.get("code") not in (None, CONSTANTS.RET_CODE_OK):
                    raise IOError(
                        f"CoinEx private WS request {data.get('id')} failed: "
                        f"{data.get('message')} (code {data.get('code')})"
                    )
            else:
                self.logger().debug(f"Unrecognised CoinEx private WS message: {data}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
