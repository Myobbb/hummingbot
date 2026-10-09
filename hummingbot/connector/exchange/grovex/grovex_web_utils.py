import asyncio
import json
import time
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlsplit

import aiohttp

from hummingbot.connector.exchange.grovex import grovex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, RESTResponse
from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


# ---------------------------------------------------------------------------------------------- connections (relay)

class GrovexRESTConnection(RESTConnection):
    """A REST connection on GroveX's own sessions: placements and cancels (TRADE_PATHS) on the trade session, every other
    request on the read session, so a slow read (user/account ~22 s, get_allticker 6-9 s) never holds the connection a
    placement needs. Both sessions go through the relay (CONSTANTS.PROXY_URL)."""

    def __init__(self, sessions: "GrovexConnectionsFactory") -> None:
        super().__init__(aiohttp_client_session=None)
        self._sessions = sessions

    async def call(self, request: RESTRequest) -> RESTResponse:
        path = urlsplit(request.url).path
        session = self._sessions.trade_session() if path in CONSTANTS.TRADE_PATHS else self._sessions.read_session()
        aiohttp_resp = await session.request(
            method=request.method.value,
            url=request.url,
            params=request.params,
            data=request.data,
            headers=request.headers,
        )
        return await self._build_resp(aiohttp_resp)


class GrovexWSConnection(WSConnection):
    """The market socket on GroveX's read session (through the relay). aiohttp's heartbeat (RFC 6455 PING, which GroveX
    answers) is the keepalive and closes a socket whose PONG goes missing; a JSON ping would make GroveX close it (P1).
    No receive timeout: a socket with no market subscribed hears nothing, legitimately; the data source's silence guard
    reconnects one that has markets and goes quiet. The message cap is the class attribute (WSAssistant always passes
    the connection's _MAX_MSG_SIZE)."""

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
        headers = {"User-Agent": CONSTANTS.USER_AGENT}
        headers.update(ws_headers or {})
        self._connection = await self._client_session.ws_connect(
            ws_url,
            headers=headers,
            autoping=True,
            heartbeat=CONSTANTS.WS_HEARTBEAT,
            max_msg_size=max_msg_size or CONSTANTS.WS_MAX_MSG_SIZE,
        )
        self._message_timeout = None
        self._connected = True

    def close_now(self) -> None:
        """Drop the socket from outside the reader (the silence guard): the reader then ends with a ConnectionError and
        the base reconnects."""
        if self._connection is not None and not self._connection.closed:
            asyncio.ensure_future(self._connection.close())


class GrovexConnectionsFactory:
    """GroveX's own aiohttp sessions — NOT the fork's shared ConnectionsFactory singleton (a subclass of it would hand
    back the shared instance from __new__): two sessions with the relay as their default proxy, a browser User-Agent and
    long keep-alives (a fresh connection through the relay costs ~1 s). Duck-types ConnectionsFactory for
    WebAssistantsFactory (get_rest_connection, get_ws_connection, close). One instance per process."""

    _instance: Optional["GrovexConnectionsFactory"] = None

    def __init__(self) -> None:
        self._trade: Optional[aiohttp.ClientSession] = None
        self._read: Optional[aiohttp.ClientSession] = None

    @classmethod
    def shared(cls) -> "GrovexConnectionsFactory":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @staticmethod
    def _new_session() -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, keepalive_timeout=CONSTANTS.KEEPALIVE_SECONDS),
            proxy=CONSTANTS.PROXY_URL,
            headers={"User-Agent": CONSTANTS.USER_AGENT},
            trust_env=False,
        )

    def trade_session(self) -> aiohttp.ClientSession:
        if self._trade is None or self._trade.closed:
            self._trade = self._new_session()
        return self._trade

    def read_session(self) -> aiohttp.ClientSession:
        if self._read is None or self._read.closed:
            self._read = self._new_session()
        return self._read

    async def get_rest_connection(self) -> GrovexRESTConnection:
        return GrovexRESTConnection(self)

    async def get_ws_connection(self) -> GrovexWSConnection:
        return GrovexWSConnection(aiohttp_client_session=self.read_session())

    async def warm(self) -> None:
        """Keep WARM_*_CONNECTIONS connections open in each pool: that many concurrent light public reads (get_ticker)
        through each session. aiohttp keeps them for KEEPALIVE_SECONDS."""
        url = public_rest_url(CONSTANTS.TICKER_PATH)

        async def one(session: aiohttp.ClientSession) -> None:
            try:
                async with session.get(url, params=dict(CONSTANTS.NETWORK_CHECK_PARAMS),
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    await resp.read()
            except Exception:
                pass

        await asyncio.gather(*([one(self.trade_session()) for _ in range(CONSTANTS.WARM_TRADE_CONNECTIONS)]
                               + [one(self.read_session()) for _ in range(CONSTANTS.WARM_READ_CONNECTIONS)]))

    async def close(self) -> None:
        for session in (self._trade, self._read):
            if session is not None and not session.closed:
                await session.close()
        self._trade = self._read = None


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
        connections_factory=GrovexConnectionsFactory.shared(),
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler, connections_factory=GrovexConnectionsFactory.shared())


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def is_ok(response: Any) -> bool:
    """GroveX answers HTTP 200 for business errors too: success is the STRING code "0" (live)."""
    return isinstance(response, dict) and str(response.get("code")) == CONSTANTS.CODE_OK


def error_code(response: Any) -> Optional[str]:
    if not isinstance(response, dict) or response.get("code") is None:
        return None
    return str(response["code"])


def loads(text: str) -> Any:
    """JSON with numbers kept exact: get_allticker sends `last`, `buy`, `sell` as JSON numbers (live)."""
    return json.loads(text, parse_float=Decimal)


def to_ms(value: Any) -> Optional[int]:
    """A GroveX time -> epoch ms: list rows carry ms integers (created_at, ctime); numbers below 1e11 are seconds.
    None when unreadable (order_info's documented "09-22 12:22" strings are not readable as a time)."""
    if value is None or isinstance(value, bool) or value == "":
        return None
    if isinstance(value, (int, float, Decimal)):
        v = float(value)
        return int(v if v >= 1e11 else v * 1000)
    s = str(value).strip()
    if s.isdigit():
        return to_ms(int(s))
    return None


def date_header_ms(date_header: Optional[str]) -> Optional[float]:
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
    GroveX server time in MILLISECONDS: market_dept's `time` field is the server's clock (live 2026-10-09: 126 ms old
    on arrival from Tokyo); its book is not used. The HTTP Date header (1 s) stands in when the field is missing.
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
    header_ms = date_header_ms((response.headers or {}).get("Date") or (response.headers or {}).get("date"))
    try:
        body = loads(await response.text())
    except Exception:
        body = None
    data = body.get("data") if isinstance(body, dict) else None
    tick = data.get("tick") if isinstance(data, dict) else None
    server_ms = to_ms((tick or {}).get("time") if isinstance(tick, dict) else None)
    if server_ms is None:
        server_ms = header_ms
    if server_ms is None:
        raise IOError("GroveX's server time: market_dept carried no `time` and no Date header")
    return float(server_ms)


class GrovexReconnectBackoff:
    """The pause before a data source's next connection attempt (XT's hot-loop lesson, 2026-09-30): a connection that
    failed, or died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to WS_RETRY_CAP."""

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


async def connected_ws_assistant(ws_url: str = CONSTANTS.WSS_URL) -> WSAssistant:
    connection = await GrovexConnectionsFactory.shared().get_ws_connection()
    assistant = WSAssistant(connection=connection)
    await assistant.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT, message_timeout=None)
    return assistant
