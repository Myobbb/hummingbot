import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils
from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange


class CoinstoreAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Coinstore private stream, on the same socket host as the books (wss://ws.coinstore.com/s/ws): a LOGIN frame
    (coinstore_auth.ws_login_payload; undocumented, found live 2026-11-08), then `!@order` (every order of the account)
    and `!@account` (every currency).

    Only the server's answers prove anything: LOGIN answers {"T":"req","C":200,"M":"login.success"} (a bad signature C
    503 "auth.session.needed"), and each channel {"C":200,"M":"sub.channel.success"}. Both are awaited before the stream
    counts as up; a refusal fails the connection and the listen loop retries after a backoff. Pushes sent while the
    socket was down are lost, so every reconnect after the first asks the connector for an immediate REST poll of orders
    and balances; so does a gap in the frames' `S` numbers.

    Even acknowledged subscriptions can stay silent: live 2026-11-08 one socket pushed nothing for 154 orders, and its
    subscribe answers named another server node (echo startEpochMill) than the two sockets that pushed. The login line
    names the node; the connector drops a socket that misses an order's push (reconnect).
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: CoinstoreAuth,
        connector: "CoinstoreExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._signed_in_before = False
        self._request_id = 0
        self._backoff = web_utils.CoinstoreReconnectBackoff()
        # the connection's last frame number (S) once subscribed, and the server node its subscriptions named
        self._last_seq: Optional[int] = None
        self._nodes: Set[Any] = set()
        # order / account pushes that arrived while the subscriptions were being acknowledged
        self._early: List[Dict[str, Any]] = []

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _connected_websocket_assistant(self) -> WSAssistant:
        return await web_utils.connected_ws_assistant(self._api_factory, CONSTANTS.WSS_URL)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            self._last_seq = None
            self._nodes = set()
            await websocket_assistant.send(WSJSONRequest(payload=self._auth.ws_login_payload(self._next_id())))
            await self._await_answers(websocket_assistant, login=True, channels=set())
            channels = {CONSTANTS.WS_ORDER_CHANNEL, CONSTANTS.WS_ACCOUNT_CHANNEL}
            await websocket_assistant.send(WSJSONRequest(payload={"op": "SUB", "channel": sorted(channels),
                                                                  "id": self._next_id()}))
            await self._await_answers(websocket_assistant, login=False, channels=channels)
            self.logger().info(f"Logged in to the Coinstore private stream; subscribed to order and account updates "
                               f"(server node {sorted(self._nodes, key=str)}).")
            self._backoff.connected()
            # Pushes sent while the socket was down (or before this first login) are gone: poll orders, fills and
            # balances over REST now (review F12: the first login too).
            self._connector._poll_notifier.set()
            self._signed_in_before = True
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to the Coinstore private stream...")
            raise

    async def _await_answers(self, websocket_assistant: WSAssistant, login: bool, channels: Set[str]) -> None:
        """Read until the login succeeded (login=True) or every channel in `channels` is subscribed. Echoes and anything
        else are skipped; a private push can't arrive before its subscription."""
        pending = set(channels)
        deadline = time.monotonic() + CONSTANTS.WS_LOGIN_TIMEOUT
        while login or pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                what = "login answer" if login else f"subscription answers for {sorted(pending)}"
                raise ConnectionError(f"Coinstore private stream: no {what} within {CONSTANTS.WS_LOGIN_TIMEOUT:.0f} s")
            try:
                response = await asyncio.wait_for(websocket_assistant.receive(), timeout=remaining)
            except asyncio.TimeoutError:
                continue
            if response is None:
                raise ConnectionError("Coinstore private stream closed during login")
            message = response.data
            if not isinstance(message, dict) or message.get("T") == "echo":
                continue
            if message.get("T") in ("order", "account"):
                self._early.append(message)   # a push between the acks: delivered first (review F12)
                continue
            meaning, code = str(message.get("M") or ""), message.get("C")
            if login:
                if code == 200 and meaning == "login.success":
                    login = False
                    continue
                if code not in (None, 200) or "auth" in meaning:
                    raise ConnectionError(f"Coinstore refused the private-stream login: {self._safe(message)}")
                continue
            echo = message.get("echo")
            name = echo.get("chName") if isinstance(echo, dict) else echo
            if code == 200 and meaning == "sub.channel.success" and name in pending:
                pending.discard(name)
                if isinstance(echo, dict) and echo.get("startEpochMill") is not None:
                    self._nodes.add(echo["startEpochMill"])
                if isinstance(message.get("S"), int):
                    self._last_seq = message["S"]   # frames are counted from the last subscribe answer on
            elif code not in (None, 200):
                raise ConnectionError(f"Coinstore refused a private channel: {self._safe(message)}")

    @staticmethod
    def _safe(message: Dict[str, Any]) -> Dict[str, Any]:
        """A server answer without the echoed LOGIN frame (it repeats the API key and the signature)."""
        return {k: v for k, v in message.items() if k != "echo"}

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        # The fork's WSConnection keeps the link alive (aiohttp heartbeat PINGs, answers the server's PING frames).
        return

    def _note_seq(self, message: Dict[str, Any]) -> None:
        """`S` numbers every frame of the connection: a jump means frames were lost, so orders and balances are read
        over REST now."""
        seq = message.get("S")
        if not isinstance(seq, int):
            return
        last, self._last_seq = self._last_seq, seq
        if last is not None and seq > last + 1:
            self.logger().warning(f"Coinstore private stream skipped frames {last + 1}..{seq - 1}: reading orders and "
                                  f"balances over REST now.")
            self._connector._poll_notifier.set()

    async def reconnect(self, reason: str) -> None:
        """Drop the private socket: the listen loop logs in again at once (and the connector polls REST). Asked by the
        connector when an order got no push."""
        websocket_assistant = self._ws_assistant
        if websocket_assistant is None:
            return
        self.logger().warning(f"Reconnecting the Coinstore private stream (server node {sorted(self._nodes, key=str)}): "
                              f"{reason}.")
        await websocket_assistant.disconnect()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        early, self._early = self._early, []
        for message in early:
            queue.put_nowait(message)
        try:
            async for ws_response in websocket_assistant.iter_messages():
                message = ws_response.data
                if not isinstance(message, dict):
                    continue
                self._note_seq(message)
                kind = message.get("T")
                if kind in ("order", "account"):
                    queue.put_nowait(message)
                elif message.get("C") not in (None, 200):
                    meaning = str(message.get("M") or "")
                    if "auth" in meaning:
                        # Logged out (the session expired or the key was revoked): orders and balances would stop
                        # silently.
                        raise ConnectionError(f"Coinstore private stream logged out: {self._safe(message)}")
                    self.logger().warning(f"Coinstore private WS: {self._safe(message)}")
                elif kind not in (None, "echo", "req", "resp", "ping"):
                    self.logger().debug(f"Coinstore private WS frame of an unknown type: {self._safe(message)}")
        except asyncio.TimeoutError:
            raise ConnectionError(f"no frame from the Coinstore private stream for "
                                  f"{CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s") from None

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        await self._backoff.wait()  # the listen loop reconnects right after this
