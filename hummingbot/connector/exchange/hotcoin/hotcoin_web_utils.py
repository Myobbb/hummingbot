import asyncio
import json
import time
import zlib
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

from hummingbot.connector.exchange.hotcoin import hotcoin_constants as CONSTANTS
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
    """Hotcoin answers HTTP 200 for business errors too. Success is `code` 200, except for the ticker, whose envelope
    is {"status": "ok", "ticker": [...]} with no code."""
    if not isinstance(response, dict):
        return False
    code = response.get("code")
    if code is None:
        return response.get("status") == "ok"
    return str(code) == str(CONSTANTS.CODE_OK)


def error_code(response: Any) -> Optional[int]:
    if not isinstance(response, dict) or response.get("code") is None:
        return None
    try:
        return int(response["code"])
    except (TypeError, ValueError):
        return None


def is_timestamp_error(response: Any) -> bool:
    """Code 1000 is any parameter error; only its 'Timestamp out of range' message means clock drift (live)."""
    return (error_code(response) == CONSTANTS.CODE_PARAM_ERROR
            and CONSTANTS.MSG_TIMESTAMP_OUT_OF_RANGE.lower() in str(response.get("msg", "")).lower())


def loads(text: str) -> Any:
    """JSON with numbers kept exact: the order push and detailById send quantities as JSON numbers
    (e.g. "tradecount": 0.08140000000000000000), which a float would round."""
    return json.loads(text, parse_float=Decimal)


def decode_ws_frame(data: Any, exact_numbers: bool = False) -> Any:
    """Every Hotcoin server frame is BINARY gzip JSON (live, public and private sockets); text is accepted too.
    `exact_numbers` keeps JSON numbers as Decimal (the private stream); book levels are strings anyway."""
    parse = loads if exact_numbers else json.loads
    if isinstance(data, (bytes, bytearray)):
        return parse(zlib.decompress(data, 31).decode("utf-8"))   # wbits 31 = gzip
    if isinstance(data, str):
        return parse(data)
    return data


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Hotcoin server time in MILLISECONDS. There is no time endpoint (every candidate path answers 10170); every
    response envelope carries `time` in ms instead (live 2026-10-05: /v1/trade, /v1/depth, /v1/common/symbols and
    the error envelopes alike). The smallest public answer is /v1/trade?symbol=btc_usdt&count=1. TimeSynchronizer
    compares this against a millisecond local clock: returning seconds was CoinEx's bug 1.
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        params=dict(CONSTANTS.SERVER_TIME_PARAMS),
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        method=RESTMethod.GET,
    )
    server_time = response.get("time") if isinstance(response, dict) else None
    if server_time is None:
        raise IOError(f"Hotcoin response carried no server time: {response}")
    return float(server_time)


class HotcoinWSConnection(WSConnection):
    """
    A WSConnection with no protocol-level heartbeat. Hotcoin's keepalive is the SERVER's {"ping":"ping"} every 5 s,
    answered with {"pong":"pong"} by the data sources; protocol PING frames are not documented for Hotcoin, and
    aiohttp's heartbeat would close a socket whose server doesn't answer them. A dead socket is caught by the
    receive timeout instead (WS_MESSAGE_TIMEOUT: six missed server pings).
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


class HotcoinReconnectBackoff:
    """
    The pause before a data source's next connection attempt. Hummingbot's listen loops reconnect at once after a
    ConnectionError, so a refusal repeats at handshake speed (XT 2026-09-30: 18,924 refusals in 2.6 h). A connection
    that failed, or died within WS_RETRY_RESET_SEC, waits WS_RETRY_BASE, then twice as long each time, up to
    WS_RETRY_CAP. One that lived longer reconnects at once.
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
        self._failures += 1
        await asyncio.sleep(min(CONSTANTS.WS_RETRY_BASE * 2 ** (self._failures - 1), CONSTANTS.WS_RETRY_CAP))


async def connected_ws_assistant(ws_url: str = CONSTANTS.WSS_URL) -> WSAssistant:
    """A WSAssistant over the shared aiohttp session, with no protocol heartbeat (see HotcoinWSConnection)."""
    base = await ConnectionsFactory().get_ws_connection()
    assistant = WSAssistant(connection=HotcoinWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(
        ws_url=ws_url,
        ping_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
        message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
    )
    return assistant
