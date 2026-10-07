import asyncio
import time
from typing import TYPE_CHECKING, Any, List, Optional

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_web_utils as web_utils
from hummingbot.connector.exchange.lbank.lbank_auth import LbankAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.lbank.lbank_exchange import LbankExchange


class LbankAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    LBank private stream (docs "WebSocket API (Asset & Order)"): the public socket host, authorised by a subscribeKey
    taken over signed REST (POST /v2/subscribe/get_key.do), then `orderUpdate` for every pair (pair "all") and
    `assetUpdate`. The key is valid 60 minutes from its creation or its last refresh (/v2/subscribe/refresh_key.do).
    Its age is kept across connections (a key reused by short-lived connections would otherwise never be refreshed):
    it is refreshed once SUBSCRIBE_KEY_REFRESH s old, by the live connection or before the next one; a key that could
    not be refreshed, or is SUBSCRIBE_KEY_MAX_AGE s old, is replaced by a new one (and the live connection closed).

    The docs show no acknowledgement for a subscribe, so a socket that is connected proves nothing about the
    subscription; the connector's order-push watchdog (LbankExchange._expect_order_push) polls REST when an order gets
    no push. Any error message naming the key drops it and reconnects with a new one. Keepalive: the client pings
    every WS_PING_INTERVAL s and answers the server's pings; no message for WS_MESSAGE_TIMEOUT s is a dead socket.
    Pushes sent while the socket was down are lost, so every reconnect after the first asks the connector for an
    immediate REST poll of orders and balances.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: LbankAuth,
        connector: "LbankExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._subscribe_key: Optional[str] = None
        self._key_born: float = 0.0      # monotonic time of the key's creation or last successful refresh
        self._helpers: List[asyncio.Task] = []
        self._connected_before = False
        self._ping_seq = 0
        self._backoff = web_utils.LbankReconnectBackoff()

    async def _get_subscribe_key(self) -> str:
        response = await self._connector._api_post(path_url=CONSTANTS.SUBSCRIBE_KEY_PATH, is_auth_required=True,
                                                   limit_id=CONSTANTS.SUBSCRIBE_KEY_PATH)
        # Live shape per CCXT (2024): {"result":true,"data":"<64 hex>","error_code":0,"ts":...}; the docs' example
        # shows {"key": "..."}. Both are read.
        key = response.get("data") if isinstance(response, dict) else None
        if isinstance(key, dict):
            key = key.get("key")
        if not key and isinstance(response, dict):
            key = response.get("key")
        if not web_utils.is_ok(response) or not key:
            raise IOError(f"LBank refused a subscribeKey for the private stream: {response}")
        self._connector._audit_once("subscribe-key-shape", keys=sorted(response.keys()), key_len=len(str(key)))
        return str(key)

    def _key_age(self) -> float:
        return time.monotonic() - self._key_born

    async def _refresh_key(self, key: str) -> bool:
        """Extends the key's validity by 60 min from now (refresh_key.do). False when LBank refuses or doesn't answer."""
        try:
            response = await self._connector._api_post(
                path_url=CONSTANTS.REFRESH_KEY_PATH, data={"subscribeKey": key}, is_auth_required=True,
                limit_id=CONSTANTS.REFRESH_KEY_PATH)
            refused = None if web_utils.is_ok(response) else response
        except asyncio.CancelledError:
            raise
        except Exception as e:
            refused = repr(e)
        self._connector._audit_once("refresh-key-answer", refused=refused)
        if refused is None:
            if self._subscribe_key == key:
                self._key_born = time.monotonic()
            return True
        self.logger().warning(f"LBank refused to extend the private stream's subscribeKey ({refused}); a new key is "
                              f"taken.")
        return False

    async def _ensure_subscribe_key(self) -> str:
        """A key good for the next connection: refreshed if SUBSCRIBE_KEY_REFRESH s old, new if there is none, the
        refresh was refused, or it is SUBSCRIBE_KEY_MAX_AGE s old."""
        if (self._subscribe_key is not None and CONSTANTS.SUBSCRIBE_KEY_REFRESH <= self._key_age()
                < CONSTANTS.SUBSCRIBE_KEY_MAX_AGE and not await self._refresh_key(self._subscribe_key)):
            self._subscribe_key = None
        if self._subscribe_key is None or self._key_age() >= CONSTANTS.SUBSCRIBE_KEY_MAX_AGE:
            self._subscribe_key = await self._get_subscribe_key()
            self._key_born = time.monotonic()
        return self._subscribe_key

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ensure_subscribe_key()
        return await web_utils.connected_ws_assistant(CONSTANTS.WSS_URL)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant) -> None:
        try:
            key = self._subscribe_key
            await websocket_assistant.send(WSJSONRequest(payload=dict(CONSTANTS.WS_SUBSCRIBE_ORDERS, subscribeKey=key)))
            await websocket_assistant.send(WSJSONRequest(payload=dict(CONSTANTS.WS_SUBSCRIBE_ASSETS, subscribeKey=key)))
            self._helpers = [web_utils.spawn(self, self._ping_loop, websocket_assistant),
                             web_utils.spawn(self, self._refresh_key_loop, websocket_assistant, key)]
            self.logger().info("Subscribed to LBank order and asset updates.")
            self._backoff.connected()
            if self._connected_before:
                # Pushes sent while the socket was down are gone: poll orders, fills and balances over REST now.
                self._connector._poll_notifier.set()
            self._connected_before = True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to the LBank private stream...")
            raise

    async def _ping_loop(self, websocket_assistant: WSAssistant) -> None:
        """The client's keepalive. A send that fails means the socket is going; the listen loop ends the connection."""
        while True:
            await asyncio.sleep(CONSTANTS.WS_PING_INTERVAL)
            self._ping_seq += 1
            try:
                await websocket_assistant.send(WSJSONRequest(payload={"action": "ping",
                                                                      "ping": f"hb{self._ping_seq}"}))
            except asyncio.CancelledError:
                raise
            except Exception:
                return

    async def _refresh_key_loop(self, websocket_assistant: WSAssistant, key: str) -> None:
        """While this connection lives, its key is refreshed each time it is SUBSCRIBE_KEY_REFRESH s old (its age, not
        the connection's). A refused refresh drops the key and closes the connection: the next one takes a new key."""
        while True:
            await asyncio.sleep(max(0.0, CONSTANTS.SUBSCRIBE_KEY_REFRESH - self._key_age()))
            if self._subscribe_key != key:
                return  # replaced: the connection that uses the new key refreshes it
            if self._key_age() < CONSTANTS.SUBSCRIBE_KEY_REFRESH:
                continue
            if await self._refresh_key(key):
                continue
            self._subscribe_key = None
            await websocket_assistant.disconnect()
            return

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        # The ping task started in _subscribe_channels keeps the socket alive.
        return

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue) -> None:
        try:
            async for ws_response in websocket_assistant.iter_messages():
                message: Any = ws_response.data
                if not isinstance(message, dict):
                    continue
                message_type = message.get("type")
                if message_type in ("orderUpdate", "assetUpdate"):
                    queue.put_nowait(message)
                    continue
                action = message.get("action")
                if action == "ping":
                    await websocket_assistant.send(WSJSONRequest(payload={"action": "pong", "pong": message.get("ping")}))
                    continue
                if action == "pong":
                    continue
                if message.get("status") == "error":
                    text = str(message.get("message") or message)
                    if "key" in text.lower():
                        # The subscription was refused for its key (expired, unknown): a new key, a new connection.
                        self._subscribe_key = None
                        raise ConnectionError(f"LBank refused the private subscription: {text[:200]}")
                    self.logger().warning(f"LBank private WS: {text[:300]}")
                    continue
                # Anything else (an acknowledgement the docs don't show?) is logged once per shape.
                self._connector._audit_once(f"private-ws-other:{sorted(message.keys())}", message=str(message)[:300])
        except asyncio.TimeoutError:
            raise ConnectionError(f"no message from the LBank private stream for {CONSTANTS.WS_MESSAGE_TIMEOUT:.0f} s "
                                  f"(pings every {CONSTANTS.WS_PING_INTERVAL:.0f} s)") from None

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]) -> None:
        for helper in self._helpers:
            helper.cancel()
        self._helpers = []
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        await self._backoff.wait()  # the listen loop reconnects right after this

    async def stop(self):
        for helper in self._helpers:
            helper.cancel()
        self._helpers = []
        self._subscribe_key = None   # a restart much later must not reuse an expired key
        await super().stop()
