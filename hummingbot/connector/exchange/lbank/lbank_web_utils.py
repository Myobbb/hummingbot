import asyncio
import json
import random
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

import aiohttp

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS
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


def is_ok(response: Any) -> bool:
    """LBank answers HTTP 200 for business errors too: {"result": "true" | true | "false" | false, "error_code": <int>,
    "msg", "ts", "data"}. `result` is a STRING on most endpoints and a boolean on some (place order, price). An answer
    with no error_code (refresh_key's {"result":"true"}) counts by `result` alone."""
    if not isinstance(response, dict):
        return False
    if str(response.get("result")).lower() != "true":
        return False
    return error_code(response) in (None, 0)


def error_code(response: Any) -> Optional[int]:
    if not isinstance(response, dict) or response.get("error_code") is None:
        return None
    try:
        return int(response["error_code"])
    except (TypeError, ValueError):
        return None


def loads(text: str) -> Any:
    """JSON with numbers kept exact: the order queries send quantities as JSON numbers (e.g. "executedQty":
    0.01000000000000000000), which a float would round."""
    return json.loads(text, parse_float=Decimal)


def ts_ms(ts: Any) -> Optional[float]:
    """A push's TS ("2026-10-04T11:39:49.031", LBank's clock in UTC+8, no zone in the string) as epoch ms, or None."""
    try:
        return datetime.fromisoformat(str(ts)).replace(tzinfo=CONSTANTS.SERVER_TZ).timestamp() * 1e3
    except (TypeError, ValueError):
        return None


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """LBank server time in MILLISECONDS from GET /v2/timestamp.do (`data`; live 2026-10-07). TimeSynchronizer compares
    it against a millisecond local clock: returning seconds was CoinEx's bug 1."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        method=RESTMethod.GET,
    )
    server_time = response.get("data") if isinstance(response, dict) else None
    if server_time is None:
        raise IOError(f"LBank response carried no server time: {response}")
    return float(server_time)


class LbankWSConnection(WSConnection):
    """
    A WSConnection with no protocol-level heartbeat and exact numbers. LBank's keepalive is the JSON
    {"action":"ping","ping":id} -> {"action":"pong","pong":id}, both ways; it answers RFC 6455 ping frames unreliably
    (CCXT's note), so aiohttp's heartbeat would close healthy sockets. The data sources ping and answer the server's
    pings; a dead socket is caught by the receive timeout. Text frames are parsed with Decimal numbers.
    """

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

    @staticmethod
    def _build_resp(msg) -> WSResponse:
        data = msg.data
        if isinstance(data, str):
            try:
                data = loads(data)
            except ValueError:
                pass
        return WSResponse(data)


def spawn(owner: Any, coroutine_function: Callable, *args: Any) -> asyncio.Task:
    """A background task for a socket's helper (keepalive, seed, key refresh). The coroutine is created only when the
    task starts: a helper cancelled before it ever ran (a socket that ended at once) leaves no "coroutine was never
    awaited" warning, which safe_ensure_future's wrapper does. An unexpected error is logged by the owner's logger."""
    async def run() -> None:
        try:
            await coroutine_function(*args)
        except asyncio.CancelledError:
            raise
        except Exception:
            owner.logger().exception(f"Unexpected error in the LBank helper {coroutine_function.__name__}.")
    return asyncio.ensure_future(run())


class LbankReconnectBackoff:
    """
    The pause before a socket's next connection attempt. Hummingbot's listen loops reconnect at once after a
    ConnectionError, so a refusal repeats at handshake speed (XT 2026-09-30: 18,924 refusals in 2.6 h), and LBank's
    host throttles connection BURSTS per IP (P1 2026-10-04: a 677-connection burst refused whole). A connection that
    failed, or died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to WS_RETRY_CAP.
    One that lived longer reconnects at once. Every wait adds a random 0..WS_RETRY_JITTER s: sockets that dropped
    together (a network blip) neither reconnect in one burst nor in lockstep at each backoff step.
    """

    def __init__(self) -> None:
        self._connected_at: Optional[float] = None
        self._failures: int = 0

    def connected(self) -> None:
        self._connected_at = time.monotonic()

    async def wait(self) -> None:
        lived = time.monotonic() - self._connected_at if self._connected_at is not None else 0.0
        self._connected_at = None
        jitter = random.uniform(0.0, CONSTANTS.WS_RETRY_JITTER)
        if lived >= CONSTANTS.WS_RETRY_RESET_SEC:
            self._failures = 0
            await asyncio.sleep(jitter)
            return
        # Capped: 2 ** n overflows a float past n = 1023, and that OverflowError, raised in the listen loop's
        # `finally`, ended the stream for good (XT 2026-10-10: 1,026 short-lived connections in a row).
        self._failures = min(self._failures + 1, 16)
        await asyncio.sleep(min(CONSTANTS.WS_RETRY_BASE * 2 ** (self._failures - 1), CONSTANTS.WS_RETRY_CAP) + jitter)


_market_socket_session: Optional[aiohttp.ClientSession] = None


def _market_session() -> aiohttp.ClientSession:
    """
    The session LBank's market sockets use: their own, with no connection cap. Hummingbot's shared session
    (ConnectionsFactory) pools every connector's REST and WebSocket connections under aiohttp's default cap of 100,
    and each open socket holds one for its lifetime (live HMB held 44 on 2026-10-07). One socket per LBank market there
    would crowd the pool, and every venue's REST calls (order placements included) would queue for a free slot.
    """
    global _market_socket_session
    if _market_socket_session is None or _market_socket_session.closed:
        _market_socket_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
    return _market_socket_session


def detach_market_session() -> Optional[aiohttp.ClientSession]:
    """The book data source stopped: the market sockets' session is detached at once (synchronously, so a data source
    started right after opens a new one) and returned for the caller to close once its sockets are closed."""
    global _market_socket_session
    session, _market_socket_session = _market_socket_session, None
    return session


async def connected_ws_assistant(ws_url: str, market_socket: bool = False) -> WSAssistant:
    """A WSAssistant with no protocol heartbeat (see LbankWSConnection): over LBank's own market-socket session for a
    market's book socket, over Hummingbot's shared session otherwise (the private stream: one socket). A handshake
    that takes longer than WS_CONNECT_TIMEOUT is a ConnectionError."""
    if market_socket:
        session = _market_session()
    else:
        session = (await ConnectionsFactory().get_ws_connection())._client_session
    assistant = WSAssistant(connection=LbankWSConnection(aiohttp_client_session=session))
    try:
        await asyncio.wait_for(assistant.connect(
            ws_url=ws_url,
            ping_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
            message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
        ), timeout=CONSTANTS.WS_CONNECT_TIMEOUT)
    except asyncio.TimeoutError:
        raise ConnectionError(f"no WebSocket handshake with {ws_url} within {CONSTANTS.WS_CONNECT_TIMEOUT:.0f} s") \
            from None
    return assistant
