import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.hotcoin import hotcoin_constants as CONSTANTS, hotcoin_web_utils as web_utils
from hummingbot.connector.exchange.hotcoin.hotcoin_auth import HotcoinAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hotcoin.hotcoin_exchange import HotcoinExchange


class HotcoinAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Hotcoin private stream: the market-data WebSocket (wss://wss.hotcoinfin.com/trade/multiple, gzip frames), signed in
    with a `signin` frame, then `market.trade.entrust.change` (order created / trade / canceled, each push carrying the
    order's cumulative filled quantity, value and fee) and `market.trade.asset.balance` (free / frozen / total).

    Topic subs are ACKed code 200 on an anonymous socket too (live 2026-10-05), so the ack proves nothing: the signin
    answer is awaited before subscribing. {"ch":"signin","code":200,"status":"ok"} is a login (docs); a bad key gets
    {"code":106,"msg":"登录失败","status":"error"} (live), which fails the connection, and the listen loop retries
    after a backoff. Pushes sent while the socket was down are lost, so every reconnect after the first asks the
    connector for an immediate REST poll of orders and balances.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: HotcoinAuth,
        connector: "HotcoinExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._signed_in_before = False
        self._backoff = web_utils.HotcoinReconnectBackoff()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            await websocket_assistant.send(WSJSONRequest(payload=self._auth.ws_signin_message()))
            await self._await_signin(websocket_assistant)
            for topic in (CONSTANTS.WS_TOPIC_ORDERS, CONSTANTS.WS_TOPIC_BALANCE):
                await websocket_assistant.send(WSJSONRequest(payload={"sub": topic}))
            self.logger().info("Signed in to the Hotcoin private stream; subscribed to order and asset updates.")
            self._backoff.connected()
            if self._signed_in_before:
                # Pushes sent while the socket was down are gone: poll orders, fills and balances over REST now.
                self._connector._poll_notifier.set()
            self._signed_in_before = True
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to the Hotcoin private stream...")
            raise

    async def _await_signin(self, websocket_assistant: WSAssistant) -> None:
        """Read until the signin answer, answering the server's pings meanwhile. The greeting {"status":"ok","ts"}
        and anything else that is not the answer is skipped; nothing private streams before the login."""
        deadline = time.monotonic() + CONSTANTS.WS_SIGNIN_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConnectionError(
                    f"Hotcoin private stream: no signin answer within {CONSTANTS.WS_SIGNIN_TIMEOUT:.0f} s")
            try:
                response = await asyncio.wait_for(websocket_assistant.receive(), timeout=remaining)
            except asyncio.TimeoutError:
                continue
            if response is None:
                raise ConnectionError("Hotcoin private stream closed during signin")
            message = web_utils.decode_ws_frame(response.data, exact_numbers=True)
            if not isinstance(message, dict):
                continue
            if "ping" in message:
                await websocket_assistant.send(WSJSONRequest(payload={"pong": "pong"}))
                continue
            if message.get("ch") == "signin" and web_utils.is_ok(message):
                return
            if message.get("ch") == "signin" or message.get("status") == "error":
                raise ConnectionError(f"Hotcoin refused the private-stream signin: {message}")

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        # Hotcoin's keepalive runs the other way: the server pings every 5 s and _process_websocket_messages answers.
        # The signin answer has already set last_recv_time.
        return

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        try:
            async for ws_response in websocket_assistant.iter_messages():
                try:
                    message: Any = web_utils.decode_ws_frame(ws_response.data, exact_numbers=True)
                except Exception as e:
                    self.logger().warning(f"Hotcoin private WS frame could not be decoded ({e}); skipped.")
                    continue
                if not isinstance(message, dict):
                    continue
                if "ping" in message:
                    await websocket_assistant.send(WSJSONRequest(payload={"pong": "pong"}))
                    continue
                channel = message.get("ch")
                topics = (CONSTANTS.WS_TOPIC_ORDERS, CONSTANTS.WS_TOPIC_BALANCE)
                if channel in topics and message.get("data") is not None:
                    queue.put_nowait(message)
                elif web_utils.error_code(message) == CONSTANTS.CODE_WS_LOGIN_FAILED:
                    # Signed out (session expired or the key revoked): orders and balances would stop silently.
                    raise ConnectionError(f"Hotcoin private stream signed out: {message}")
                elif not self._is_benign(message):
                    self.logger().warning(f"Hotcoin private WS: {message}")
        except asyncio.TimeoutError:
            raise ConnectionError(f"no message from the Hotcoin private stream for "
                                  f"{CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s (the server pings every 5 s)") from None

    @staticmethod
    def _is_benign(message: Dict[str, Any]) -> bool:
        # Sub acks {"ch","code":200,"msg":"SUCCESS","status":"ok"}, the signin answer, the greeting {"status":"ok"}.
        return web_utils.is_ok(message) or (message.get("status") == "ok" and "code" not in message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        await self._backoff.wait()  # the listen loop reconnects right after this
