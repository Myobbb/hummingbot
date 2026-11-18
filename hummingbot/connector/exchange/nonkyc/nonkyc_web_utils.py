import asyncio
import time
from typing import Any, Callable, Dict, Optional

import aiohttp
import ujson

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSResponse
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler))
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """NonKYC server time in MILLISECONDS: GET /time -> {"serverTime": <ms>} (live 2026-11-17; the mirror's own fork
    reads a different route, nonkyc.io/api/v2/getservertime, which answers `unix_microsec` -- /time is the documented
    one and its unit is checked here, the CoinEx seconds bug)."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
    )
    server_ms = response.get("serverTime") if isinstance(response, dict) else None
    if isinstance(server_ms, bool) or not isinstance(server_ms, (int, float)) or server_ms < 1e12:
        raise IOError(f"NonKYC /time answered no millisecond serverTime: {response}")
    return float(server_ms)


def parse_error_body(text: str) -> Dict[str, Any]:
    """A refusal body as a dict: {"error": {"code", "message", "description", "errorType"}}. A non-JSON body (the 401's
    plain "Not Authorized") is kept as `raw`."""
    try:
        parsed = ujson.loads(text)
        return parsed if isinstance(parsed, dict) else {"raw": text}
    except (TypeError, ValueError):
        return {"raw": (text or "")[:300]}


def error_code(parsed: Any) -> Optional[int]:
    """The venue's own code ({"error": {"code": 20002}}), or None."""
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict) and error.get("code") is not None:
            try:
                return int(error["code"])
            except (TypeError, ValueError):
                return None
    return None


class NonkycWSConnection(WSConnection):
    """The fork's WSConnection with aiohttp answering the server's PING frames itself (autoping: the venue pings every
    ~60 s) and pinging after WS_HEARTBEAT_S of silence; a socket with no PONG within half of that is closed, which is
    the liveness signal for a venue whose quiet markets push nothing for minutes. No message timeout: a quiet market
    legitimately sends nothing, and the data source's own sequence checks decide freshness."""

    _MAX_MSG_SIZE = CONSTANTS.WS_MAX_MSG_SIZE

    async def connect(
        self,
        ws_url: str,
        ping_timeout: float = 10,
        message_timeout: Optional[float] = None,
        ws_headers: Optional[Dict] = {},
        max_msg_size: Optional[int] = None,
    ):
        self._ensure_not_connected()
        self._connection = await self._client_session.ws_connect(
            ws_url,
            headers=ws_headers or {},
            autoping=True,
            heartbeat=CONSTANTS.WS_HEARTBEAT_S,
            max_msg_size=max_msg_size or CONSTANTS.WS_MAX_MSG_SIZE,
        )
        self._message_timeout = None
        self._connected = True

    @staticmethod
    def _build_resp(msg: aiohttp.WSMessage) -> WSResponse:
        text = msg.data
        if isinstance(text, str):
            try:
                return WSResponse(ujson.loads(text))
            except ValueError:
                return WSResponse(text)
        return WSResponse(text)


class NonkycReconnectBackoff:
    """The pause before a connection's next attempt (XT's hot-loop lesson): a connection that failed, or died within
    WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to WS_RETRY_CAP. The exponent is bounded
    (XT 2026-10-10: 2 ** n on an unbounded counter overflowed after 1,025 short-lived connections)."""

    def __init__(self) -> None:
        self._connected_at: Optional[float] = None
        self._failures: int = 0

    def connected(self) -> None:
        self._connected_at = time.monotonic()

    def delay(self) -> float:
        lived = time.monotonic() - self._connected_at if self._connected_at is not None else 0.0
        self._connected_at = None
        if lived >= CONSTANTS.WS_RETRY_RESET_SEC:
            self._failures = 0
            return 0.0
        self._failures = min(self._failures + 1, CONSTANTS.WS_RETRY_MAX_EXPONENT)
        return min(CONSTANTS.WS_RETRY_BASE * (2 ** (self._failures - 1)), CONSTANTS.WS_RETRY_CAP)

    async def wait(self) -> None:
        pause = self.delay()
        if pause > 0:
            await asyncio.sleep(pause)


async def connected_ws_assistant(ws_url: str = CONSTANTS.WSS_URL) -> WSAssistant:
    base = await ConnectionsFactory().get_ws_connection()
    assistant = WSAssistant(connection=NonkycWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_S, message_timeout=None)
    return assistant
