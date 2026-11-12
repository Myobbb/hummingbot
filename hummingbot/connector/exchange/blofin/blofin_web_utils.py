import asyncio
import json
import time
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Optional

from hummingbot.connector.exchange.blofin import blofin_constants as CONSTANTS
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


def is_ok(response: Any) -> bool:
    """BloFin answers HTTP 200 for business errors too: success is the STRING code "0" (live)."""
    return isinstance(response, dict) and str(response.get("code")) == CONSTANTS.CODE_OK


def error_code(response: Any) -> Optional[str]:
    if not isinstance(response, dict) or response.get("code") is None:
        return None
    return str(response["code"])


def loads(text: str) -> Any:
    """JSON with numbers kept exact. BloFin sends decimal strings; a stray number stays a Decimal, not a float."""
    return json.loads(text, parse_float=Decimal)


def to_ms(value: Any) -> Optional[int]:
    """A BloFin time (epoch ms as a string or number) -> int ms; seconds (< 1e11) are scaled. None when unreadable."""
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return int(v if v >= 1e11 else v * 1000)


def date_header_ms(date_header: Optional[str]) -> Optional[float]:
    """The HTTP Date header -> epoch ms at the middle of its second (1 s resolution)."""
    if not date_header:
        return None
    try:
        return parsedate_to_datetime(date_header).timestamp() * 1e3 + 500.0
    except (TypeError, ValueError):
        return None


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    BloFin server time in MILLISECONDS. The docs name no time route, and the spot answers carry no server-time field;
    the HTTP `Date` header of the smallest public answer (one market's ticker) does, to the second. TimeSynchronizer
    compares it against a millisecond clock (CoinEx's seconds bug). A second's resolution is plenty: the signed
    timestamp may be 60 s off (152405 beyond, live).
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request_and_get_response(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        params=dict(CONSTANTS.SERVER_TIME_PARAMS),
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        method=RESTMethod.GET,
    )
    server_ms = date_header_ms((response.headers or {}).get("Date") or (response.headers or {}).get("date"))
    await response.text()
    if server_ms is None:
        raise IOError("BloFin response carried no Date header")
    return float(server_ms)


class BlofinWSConnection(WSConnection):
    """A WSConnection with no protocol-level heartbeat and a large message cap: the socket's keepalive is our JSON
    {"op":"PING"} (WS_PING_INTERVAL), and a DEPTH snapshot carries up to 400 levels a side. A dead socket is caught by
    the pong timeout and the silence watchdog. The cap is the class attribute: WSAssistant.connect always passes the
    connection's _MAX_MSG_SIZE."""

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
            headers=ws_headers,
            autoping=True,
            heartbeat=None,
            max_msg_size=max_msg_size or CONSTANTS.WS_MAX_MSG_SIZE,
        )
        self._message_timeout = message_timeout
        self._connected = True


class BlofinReconnectBackoff:
    """The pause before the data source's next connection attempt (XT's hot-loop lesson, 2026-09-30): a connection that
    failed, or died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to WS_RETRY_CAP.
    The exponent is bounded (XT 2026-10-10: 2 ** n on an unbounded counter overflowed after 1,025 short-lived
    connections and ended the public stream for good)."""

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
    assistant = WSAssistant(connection=BlofinWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(
        ws_url=ws_url,
        ping_timeout=CONSTANTS.WS_PONG_TIMEOUT,
        message_timeout=CONSTANTS.SOCKET_SILENCE_S,
    )
    return assistant
