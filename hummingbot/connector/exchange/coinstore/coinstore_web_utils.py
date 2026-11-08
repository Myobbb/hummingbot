import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Callable, Optional

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def depth_url(symbol: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return public_rest_url(CONSTANTS.DEPTH_PATH.format(symbol=symbol), domain)


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


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def code_of(response: Any) -> Optional[str]:
    """The envelope's code as text: the integer 0 on the market / trading routes, the string "0" on the config and
    funding routes (live). None when the answer carries no code (Cloudflare's HTML 429 page never gets this far)."""
    if not isinstance(response, dict) or response.get("code") is None:
        return None
    return str(response["code"])


def is_ok(response: Any) -> bool:
    return code_of(response) == CONSTANTS.CODE_OK


def loads(text: str) -> Any:
    """JSON with numbers kept exact: Coinstore sends some amounts as JSON numbers (the fills route: execQty
    40.900000000000000000, fee 0.0818...), which a float would round."""
    return json.loads(text, parse_float=Decimal)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Server time in MILLISECONDS, for TimeSynchronizer. Coinstore has no time endpoint, and none is needed: the signed
    X-CS-EXPIRES is only an HMAC input (floor(expires / 30000) keys the signature), and a value 300 s behind and 120 s
    ahead was accepted live (2026-11-08). So the host's own (NTP) clock is the reference: an offset of 0, always.
    """
    return time.time() * 1e3


class CoinstoreReconnectBackoff:
    """
    The pause before a data source's next connection attempt. Hummingbot's listen loops reconnect at once after a
    ConnectionError, so a refusal would repeat at handshake speed (XT 2026-09-30: 18,924 refusals in 2.6 h). A
    connection that failed, or died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up
    to WS_RETRY_CAP. One that lived longer reconnects at once. The counter is capped: 2 ** n overflows a float past
    n = 1023, and that OverflowError ended XT's stream for good (2026-10-10).
    """

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


async def connected_ws_assistant(api_factory: WebAssistantsFactory, ws_url: str = CONSTANTS.WSS_URL) -> WSAssistant:
    """A WSAssistant on the shared session. The fork's WSConnection sends an aiohttp heartbeat PING every
    WS_PING_INTERVAL s when idle and answers the server's PING frames (Coinstore: every 3 min, a link that hasn't
    PONGed within 10 min is dropped)."""
    ws = await api_factory.get_ws_assistant()
    await ws.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_PING_INTERVAL,
                     message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT)
    return ws
