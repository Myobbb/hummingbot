import asyncio
import json
import time
from typing import Any, Callable, Dict, Optional

from hummingbot.connector.exchange.poloniex import poloniex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
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


def error_code(response: Any) -> Optional[str]:
    """Poloniex's refusal code ({"code": 21301, "message": ...}) as a string, else None. A success is a bare object
    or list without `code` (a success object never carries one, bar the cancel answer's `code` 200)."""
    if isinstance(response, dict) and response.get("code") not in (None, "", 200, "200"):
        return str(response.get("code"))
    return None


def parse_error_text(text: str) -> Dict[str, Any]:
    """A refusal body as a dict. Some are malformed JSON (`"data":}` after an invalid symbol): read the code and the
    message anyway, never raise."""
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"raw": text}
    except (TypeError, ValueError):
        out: Dict[str, Any] = {"raw": (text or "")[:300]}
        for key in ("code", "message"):
            marker = f'"{key}":'
            start = (text or "").find(marker)
            if start >= 0:
                value = text[start + len(marker):].lstrip().split(",")[0].strip().strip('"').rstrip("}").strip('"')
                out[key] = value
        return out


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """Poloniex server time in MILLISECONDS: GET /timestamp -> {"serverTime": <ms>}. TimeSynchronizer compares this
    with a millisecond local clock (CoinEx's bug 1 was seconds)."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        method=RESTMethod.GET,
    )
    return float(response["serverTime"])


class PoloniexWSConnection(WSConnection):
    """The fork's WSConnection with no protocol-level ping: Poloniex's keepalive is the JSON {"event":"ping"} ->
    {"event":"pong"} exchange, which the data sources send themselves (the server ends a session silent for 30 s)."""

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
            headers=ws_headers,
            autoping=False,
            heartbeat=None,
            max_msg_size=max_msg_size,
        )
        self._message_timeout = message_timeout
        self._connected = True


class PoloniexReconnectBackoff:
    """The pause before a data source's next connection attempt (XT's lesson: the listen loops reconnect at once after
    a ConnectionError, so a refusal repeats at handshake speed). A connection that failed, or died within
    WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, doubling up to WS_RETRY_CAP; one that lived longer reconnects at once."""

    def __init__(self) -> None:
        self._connected_at: Optional[float] = None
        self._failures: int = 0

    def connected(self) -> None:
        self._connected_at = time.monotonic()

    async def wait(self) -> None:
        lived = time.monotonic() - self._connected_at if self._connected_at is not None else 0.0
        self._connected_at = None
        if lived >= CONSTANTS.WS_RETRY_RESET_SEC:
            self._failures = 0
            return
        self._failures += 1
        await asyncio.sleep(min(CONSTANTS.WS_RETRY_BASE * 2 ** (self._failures - 1), CONSTANTS.WS_RETRY_CAP))


async def connected_ws_assistant(ws_url: str) -> WSAssistant:
    """A WSAssistant over the shared aiohttp session, without protocol pings (see PoloniexWSConnection)."""
    base = await ConnectionsFactory().get_ws_connection()
    assistant = WSAssistant(connection=PoloniexWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(
        ws_url=ws_url,
        ping_timeout=CONSTANTS.WS_HEARTBEAT_INTERVAL,
        message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
    )
    return assistant
