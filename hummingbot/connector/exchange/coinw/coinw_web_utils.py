import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, Optional

import aiohttp

from hummingbot.connector.exchange.coinw import coinw_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.data_types import WSResponse
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
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """No time-synchroniser pre-processor: CoinW signs no timestamp, and has no server-time endpoint."""
    return WebAssistantsFactory(throttler=throttler or create_throttler(), auth=auth)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """This host's clock, in MILLISECONDS. CoinW has no server-time endpoint (the public commands are returnSymbol,
    returnTicker, returnCurrencies, returnOrderBook, returnTradeHistory, returnChartData, return24hVolume), and nothing
    it accepts carries a timestamp (the MD5 signature covers the parameters and the key only), so there is no clock to
    agree with."""
    return time.time() * 1e3


def loads(text: str) -> Any:
    """CoinW's answer, parsed with exact numbers: prices and amounts as Decimal, never float. An empty body (what an
    unauthenticated private request gets: HTTP 200, no body, live 2026-10-15) is None."""
    if not text:
        return None
    return json.loads(text, parse_float=Decimal)


def error_code(response: Any) -> Optional[str]:
    """CoinW's envelope code as a string when the answer is not a success, else None. A success is
    {"code":"200","data":…,"msg":"SUCCESS","success":true,"failed":false} (`code` is the STRING "200"); refusals use the
    same envelope with another code ("-105" wrong signature, "-200" business exception, "200012" order does not exist,
    "29001" too frequent...), HTTP 200 throughout."""
    if not isinstance(response, dict):
        return "no-envelope"
    code = response.get("code")
    if str(code) == "200" and response.get("success", True) is not False:
        return None
    return str(code) if code not in (None, "") else "no-code"


def is_ok(response: Any) -> bool:
    return error_code(response) is None


class CoinwWSConnection(WSConnection):
    """The fork's WSConnection with CoinW's handshake: `Origin: https://www.coinw.com` is required (the CDN answers 403
    without it, live 2026-10-15 from myserver), and no protocol-level ping: CoinW's keepalive is the JSON
    {"event":"ping"} -> {"event":"pong"} exchange, which the data sources send themselves.

    raw=True hands text frames over UNPARSED (the public book stream: its data source reads the market and the
    sequence from the raw text first and parses only what it applies)."""

    def __init__(self, aiohttp_client_session, raw: bool = False):
        super().__init__(aiohttp_client_session=aiohttp_client_session)
        self._raw = raw

    def _build_resp(self, msg):  # the base's is a staticmethod called through self
        if self._raw and msg.type == aiohttp.WSMsgType.TEXT:
            return WSResponse(msg.data)
        return WSConnection._build_resp(msg)

    async def connect(
        self,
        ws_url: str,
        ping_timeout: float = 10,
        message_timeout: Optional[float] = None,
        ws_headers: Optional[Dict] = {},
        max_msg_size: Optional[int] = None,
    ):
        self._ensure_not_connected()
        headers = dict(ws_headers or {})
        headers.update(CONSTANTS.WS_HEADERS)
        self._connection = await self._client_session.ws_connect(
            ws_url,
            headers=headers,
            autoping=False,
            heartbeat=None,
            max_msg_size=max_msg_size if max_msg_size is not None else 0,
        )
        self._message_timeout = message_timeout
        self._connected = True


class CoinwReconnectBackoff:
    """The pause before a data source's next connection attempt (XT's lesson: the listen loops reconnect at once after
    a ConnectionError, so a refusal repeats at handshake speed). A connection that failed, or died within
    WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, doubling up to WS_RETRY_CAP; one that lived longer reconnects at once.
    The failure count is capped, so the wait can't overflow (XT's OverflowError, 2026-10-10)."""

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
        self._failures = min(self._failures + 1, 16)
        await asyncio.sleep(min(CONSTANTS.WS_RETRY_BASE * 2 ** (self._failures - 1), CONSTANTS.WS_RETRY_CAP))


async def connected_ws_assistant(ws_url: str = CONSTANTS.WSS_URL, raw: bool = False) -> WSAssistant:
    """A WSAssistant over the shared aiohttp session, with CoinW's Origin header and no protocol pings."""
    base = await ConnectionsFactory().get_ws_connection()
    assistant = WSAssistant(connection=CoinwWSConnection(aiohttp_client_session=base._client_session, raw=raw))
    await assistant.connect(
        ws_url=ws_url,
        ping_timeout=CONSTANTS.WS_HEARTBEAT_INTERVAL,
        message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
    )
    return assistant
