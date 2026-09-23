from typing import Any, Callable, Dict, Optional

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS
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


def is_error_response(response: Any) -> bool:
    """XT answers HTTP 200 for business errors too; only rc == 0 is success (ResponseCode)."""
    return not isinstance(response, dict) or response.get("rc") != CONSTANTS.RC_OK


def error_code(response: Any) -> Optional[str]:
    return response.get("mc") if isinstance(response, dict) else None


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    XT server time in MILLISECONDS: GET /v4/public/time -> {"result": {"serverTime": <ms>}}.

    TimeSynchronizer compares this against a millisecond local clock. Returning seconds was CoinEx's
    bug 1 (every signed request rejected), hence the explicit unit.
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH, domain=domain),
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH,
        method=RESTMethod.GET,
    )
    return float(response["result"]["serverTime"])


class XtWSConnection(WSConnection):
    """
    A WSConnection that negotiates permessage-deflate and refuses a connection without it.

    Hummingbot's WSConnection never passes `compress` to aiohttp, so no connector here has ever
    requested deflate. XT documents it as a request header, and in P1 the nodes that did not grant
    it pushed ~40% of the needed rate and served books minutes to hours old (2026-09-23). Raising
    ConnectionError hands the connection back to the data source's reconnect loop, which retries
    until a node grants it (1-3 attempts in P1).

    `heartbeat=None`: XT's keepalive is the text "ping"/"pong" exchange (Heartbeat), which the data
    sources send themselves; protocol-level PING frames are not documented for XT.
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
            compress=CONSTANTS.WS_COMPRESS,
        )
        if not self._connection.compress:
            await self._connection.close()
            self._connection = None
            raise ConnectionError(f"XT refused permessage-deflate on {ws_url} (lagging node); reconnecting")
        self._message_timeout = message_timeout
        self._connected = True

    @property
    def negotiated_compress(self) -> int:
        return self._connection.compress if self._connection is not None else 0


async def connected_ws_assistant(ws_url: str) -> WSAssistant:
    """A WSAssistant over the shared aiohttp session, connected with deflate enforced."""
    base = await ConnectionsFactory().get_ws_connection()
    assistant = WSAssistant(connection=XtWSConnection(aiohttp_client_session=base._client_session))
    await assistant.connect(
        ws_url=ws_url,
        ping_timeout=CONSTANTS.WS_HEARTBEAT_INTERVAL,
        message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
    )
    return assistant
