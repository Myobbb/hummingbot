import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Optional

from hummingbot.connector.exchange.coinw import coinw_constants as CONSTANTS, coinw_web_utils as web_utils
from hummingbot.connector.exchange.coinw.coinw_auth import CoinwAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinw.coinw_exchange import CoinwExchange


class CoinwAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    CoinW private stream (wss://ws.futurescw.com, the same socket host as the public one): channels `order` and `assets`.

    Authorisation is a login event carrying the api key and the SECRET itself (CoinwAuth.ws_login_payload): it is sent
    as plain text and never logged, and nothing that can echo it is either. Its answer is awaited before anything is
    subscribed; a refused login (result false, e.g. errorCode 1003) or a refused subscription raises into the reconnect
    backoff: without the private channels, order and balance pushes would silently stop.

    What the pushes carry (docs; CoinW.Net CoinWOrderUpdate / CoinWBalanceUpdate):
      order   {"biz":"exchange","type":"order","data":{order_id, client_id, product_id, side, order_type, size,
              remaining_size, price, dealFunds, dealAvgPrice, fee, type RECEIVED|DONE, reason FILLED|CANCELLED|REJECTED,
              time}} — no push for a partial fill of a resting order (only RECEIVED and DONE exist);
      assets  {"biz":"exchange","type":"assets","data":{currency, available, hold, type "change", ledger_id, account
              "spot", time}} after each balance change.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: CoinwAuth,
        connector: "CoinwExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ping_task: Optional[asyncio.Task] = None
        self._last_pong: float = 0.0
        self._backoff = web_utils.CoinwReconnectBackoff()
        # pushes that arrived while the login / subscription acks were awaited: queued first by the listen loop
        self._early_frames: list = []

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _await_ack(self, ws: WSAssistant, *, channel: str, kind: Optional[str], deadline: float) -> None:
        """The answer to a login (channel "login") or a subscription (channel "subscribe", type `kind`). A push read
        meanwhile (an order push can follow the `order` ack before the `assets` ack) is kept for the listen loop,
        which queues it first."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConnectionError(f"CoinW private stream: no {channel} answer within {CONSTANTS.WS_LOGIN_TIMEOUT} s")
            try:
                response = await asyncio.wait_for(ws.receive(), timeout=remaining)
            except asyncio.TimeoutError:
                raise ConnectionError(f"CoinW private stream: no {channel} answer within "
                                      f"{CONSTANTS.WS_LOGIN_TIMEOUT} s") from None
            data: Any = response.data if response is not None else None
            if not isinstance(data, dict):
                continue
            if data.get("event") == "pong":
                self._last_pong = time.monotonic()
                continue
            if data.get("channel") != channel or (kind is not None and data.get("type") != kind):
                if data.get("type") in (CONSTANTS.WS_ORDER_CHANNEL, CONSTANTS.WS_ASSETS_CHANNEL) and not data.get("channel"):
                    self._early_frames.append(data)
                continue
            result = data.get("data") if isinstance(data.get("data"), dict) else {}
            if result.get("result") is True:
                return
            # Never echo the request: the login carries the secret. The answer carries only result / errorCode / msg.
            raise ConnectionError(f"CoinW private stream refused the {channel}"
                                  f"{' ' + kind if kind else ''}: errorCode {result.get('errorCode')} "
                                  f"({result.get('errorMsg')})")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            self._early_frames = []
            deadline = time.monotonic() + CONSTANTS.WS_LOGIN_TIMEOUT
            # Sent as a pre-serialised text frame: no request object holding the secret outlives this call.
            await websocket_assistant.send(WSPlainTextRequest(payload=json.dumps(self._auth.ws_login_payload())))
            await self._await_ack(websocket_assistant, channel="login", kind=None, deadline=deadline)
            for kind in (CONSTANTS.WS_ORDER_CHANNEL, CONSTANTS.WS_ASSETS_CHANNEL):
                await websocket_assistant.send(WSJSONRequest(payload={
                    "event": "sub", "params": {"biz": "exchange", "type": kind}}))
                await self._await_ack(websocket_assistant, channel="subscribe", kind=kind, deadline=deadline)
            self.logger().info("Logged in and subscribed to the CoinW private order and assets channels.")
            self._backoff.connected()
            self._last_pong = time.monotonic()
            if self._ping_task is not None:
                self._ping_task.cancel()
            self._ping_task = asyncio.ensure_future(self._ping_loop(websocket_assistant))
            # The pushes only report changes: whatever moved while no socket was up is read over REST now.
            self._connector.on_private_stream_connected()
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception as e:
            self.logger().error(f"Unexpected error subscribing to the CoinW private channels: {type(e).__name__}")
            raise ConnectionError(f"CoinW private subscription failed: {type(e).__name__}") from None

    async def _ping_loop(self, ws: WSAssistant) -> None:
        """{"event":"ping"} every WS_HEARTBEAT_INTERVAL; no pong within WS_PONG_TIMEOUT ends the connection."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_INTERVAL)
                sent = time.monotonic()
                await ws.send(WSJSONRequest(payload={"event": "ping"}))
                await asyncio.sleep(CONSTANTS.WS_PONG_TIMEOUT)
                if self._last_pong < sent:
                    self.logger().warning(f"CoinW private WS: no pong within {CONSTANTS.WS_PONG_TIMEOUT} s; "
                                          f"reconnecting.")
                    await ws.disconnect()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinW private WS keepalive stopped: {type(e).__name__}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        early, self._early_frames = self._early_frames, []
        for data in early:
            queue.put_nowait(data)
        async for ws_response in websocket_assistant.iter_messages():
            data: Any = ws_response.data
            if not isinstance(data, dict):
                continue
            if data.get("event") == "pong":
                self._last_pong = time.monotonic()
                continue
            if data.get("event") == "ping":
                await websocket_assistant.send(WSJSONRequest(payload={"event": "pong"}))
                continue
            channel = data.get("channel")
            if channel in ("login", "subscribe", "unsub"):
                result = data.get("data") if isinstance(data.get("data"), dict) else {}
                if result.get("result") is False:
                    raise ConnectionError(f"CoinW private stream: {channel} refused (errorCode "
                                          f"{result.get('errorCode')}, {result.get('errorMsg')}); reconnecting")
                continue
            if data.get("type") in (CONSTANTS.WS_ORDER_CHANNEL, CONSTANTS.WS_ASSETS_CHANNEL):
                queue.put_nowait(data)
            else:
                self.logger().debug(f"Unrecognised CoinW private WS message type: {data.get('type')!r}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        await self._backoff.wait()  # the listen loop reconnects right after this
