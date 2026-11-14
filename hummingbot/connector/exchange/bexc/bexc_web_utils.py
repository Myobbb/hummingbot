import asyncio
import json
import time
import zlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

import aiohttp
import ujson

from hummingbot.connector.exchange.bexc import bexc_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSResponse
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


class BexcHeadersRESTPreProcessor(RESTPreProcessorBase):
    """The descriptive User-Agent on every request, public and signed (aiohttp's default one is what a WAF profiles)."""

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers or {})
        headers.setdefault("User-Agent", CONSTANTS.USER_AGENT)
        headers.setdefault("Accept", "application/json")
        request.headers = headers
        return request


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
            BexcHeadersRESTPreProcessor(),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler, rest_pre_processors=[BexcHeadersRESTPreProcessor()])


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def loads(text: str) -> Any:
    """JSON with numbers kept exact. bexc sends decimal strings; a stray number stays a Decimal, not a float."""
    return json.loads(text, parse_float=Decimal)


def iso_to_ms(value: Any) -> Optional[int]:
    """A bexc ISO-8601 UTC time ("2026-11-13T19:42:11.123456789Z": nanoseconds, which datetime can't parse) -> epoch
    ms. None when it is not one."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1]
    elif text.endswith("+00:00"):
        text = text[:-6]
    else:
        return None
    whole, _, fraction = text.partition(".")
    try:
        ms = int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp()) * 1000
    except ValueError:
        return None
    if fraction:
        digits = (fraction + "00")[:3]
        if not digits.isdigit():
            return None
        ms += int(digits)
    return ms


def decode_book_frame(data: bytes) -> Any:
    """A BINARY frame: one raw-deflate stream (zlib wbits -15) of JSON. ujson (a declared dependency of the fork) parses
    the ~11.5 KB book in about half stdlib json's time; prices and quantities are strings, so no float is involved."""
    return ujson.loads(zlib.decompress(data, -15))


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """bexc server time in MILLISECONDS: GET /time -> {"server_time": ms} (docs; live 2026-11-13). Signed requests are
    accepted within ±10 s of it. TimeSynchronizer compares it against a millisecond clock (CoinEx's seconds bug)."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        timeout=CONSTANTS.REQUEST_TIMEOUT,      # the poll loop waits on it every TIME_SYNC_INTERVAL (review D)
    )
    server_ms = response.get("server_time") if isinstance(response, dict) else None
    if isinstance(server_ms, bool) or not isinstance(server_ms, (int, float)) or server_ms < 1e12:
        raise IOError(f"bexc /time answered no millisecond server_time: {response}")
    return float(server_ms)


class BexcWSConnection(WSConnection):
    """The engine socket, as P1 runs it: aiohttp answers the server's PINGs itself (autoping) and pings after
    WS_HEARTBEAT_S of silence, closing the socket when no PONG comes within half of it; no message timeout (a quiet
    market can go minutes without a push: the data source's own silence checks decide). The handshake carries the
    descriptive User-Agent.

    BINARY frames (books) are handed on as the raw bytes: the data source decodes them. candle_update frames, ~5 a
    second on a handful of markets and unused, are dropped before any JSON parse (P1 skips them by prefix too)."""

    _MAX_MSG_SIZE = CONSTANTS.WS_MAX_MSG_SIZE
    _SKIPPED_TEXT = ('{"type":"candle_update"',)

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
            headers={"User-Agent": CONSTANTS.USER_AGENT, **(ws_headers or {})},
            autoping=True,
            heartbeat=CONSTANTS.WS_HEARTBEAT_S,
            max_msg_size=max_msg_size or CONSTANTS.WS_MAX_MSG_SIZE,
        )
        self._message_timeout = None
        self._connected = True

    @staticmethod
    def _build_resp(msg: aiohttp.WSMessage) -> WSResponse:
        if msg.type == aiohttp.WSMsgType.BINARY:
            return WSResponse(msg.data)
        text = msg.data
        if isinstance(text, str):
            if text.startswith(BexcWSConnection._SKIPPED_TEXT):
                return WSResponse(None)
            try:
                return WSResponse(ujson.loads(text))
            except ValueError:
                return WSResponse(text)
        return WSResponse(text)


class BexcReconnectBackoff:
    """The pause before a connection's next attempt (XT's hot-loop lesson, 2026-09-30): a connection that failed, or
    died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to WS_RETRY_CAP. The exponent
    is bounded (XT 2026-10-10: 2 ** n on an unbounded counter overflowed after 1,025 short-lived connections and ended
    the public stream for good)."""

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
    assistant = WSAssistant(connection=BexcWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_S, message_timeout=None)
    return assistant
