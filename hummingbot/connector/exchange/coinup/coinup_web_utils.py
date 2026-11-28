import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, Optional

import aiohttp

from hummingbot.connector.exchange.coinup import coinup_constants as CONSTANTS
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


def is_challenge(status: int, headers: Dict[str, str], text: str) -> bool:
    """Cloudflare's managed challenge. Every 403 from this host is treated as one: the API itself answers its refusals
    in an HTTP 200 body, so a 403 is the edge (measured 2026-11-27 — the page carries "Just a moment..." and a
    `Cf-Mitigated: challenge` header, but neither is guaranteed on every challenge variant)."""
    return status == 403


class _CurlResponse:
    """Duck-types the aiohttp response RESTResponse wraps (url, method, status, headers, content_type, json/text/read)."""

    def __init__(self, url: str, method: str, status: int, headers: Dict[str, str], text: str) -> None:
        self._url = url
        self._method = method
        self._status = status
        self._headers = headers
        self._text = text

    @property
    def url(self) -> str:
        return self._url

    @property
    def method(self) -> str:
        return self._method

    @property
    def status(self) -> int:
        return self._status

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @property
    def content_type(self) -> str:
        ct = ""
        for key, value in self._headers.items():
            if str(key).lower() == "content-type":
                ct = str(value)
                break
        return ct.split(";")[0].strip()

    async def json(self) -> Any:
        return json.loads(self._text)

    async def text(self) -> str:
        return self._text

    async def read(self) -> bytes:
        return self._text.encode("utf-8")


class CoinupRESTConnection(RESTConnection):
    """CoinUp's REST on curl_cffi, because aiohttp cannot pass Cloudflare's managed challenge.

    Every request goes through one lock and a floor between calls (the API host challenges a burst: 2026-11-27), with a
    browser TLS fingerprint and a retry across profiles on a challenge. A response is handed back as a RESTResponse;
    Cloudflare pages never reach the caller. Blocking curl calls run in a thread so the event loop stays free.
    """

    _lock = asyncio.Lock()
    _last_call = 0.0

    def __init__(self, sessions: "CoinupSessions") -> None:
        super().__init__(aiohttp_client_session=None)
        self._sessions = sessions

    @staticmethod
    def _url(request: RESTRequest) -> str:
        from urllib.parse import urlencode
        params = request.params or {}
        if not params:
            return request.url
        return request.url + "?" + urlencode({str(k): str(v) for k, v in params.items() if v is not None})

    def _do(self, method: str, url: str, data: Optional[str], headers: Dict[str, str]) -> _CurlResponse:
        last: Optional[_CurlResponse] = None
        for attempt, profile in enumerate(CONSTANTS.IMPERSONATE_PROFILES[:CONSTANTS.CHALLENGE_TRIES]):
            session = self._sessions.session(profile)
            try:
                resp = session.request(method, url, data=data, headers=headers, timeout=35)
            except Exception as e:
                raise IOError(f"CoinUp request failed ({type(e).__name__}: {e})") from e
            text = resp.text or ""
            if is_challenge(resp.status_code, dict(resp.headers or {}), text):
                last = _CurlResponse(url, method, resp.status_code, dict(resp.headers or {}), text)
                time.sleep(CONSTANTS.CHALLENGE_BACKOFF_SECONDS[min(attempt, len(CONSTANTS.CHALLENGE_BACKOFF_SECONDS) - 1)])
                continue
            return _CurlResponse(url, method, resp.status_code, dict(resp.headers or {}), text)
        raise IOError(f"CoinUp: Cloudflare challenge survived {CONSTANTS.CHALLENGE_TRIES} profiles for {url}")

    async def call(self, request: RESTRequest) -> RESTResponse:
        url = self._url(request)
        data = request.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        headers = {str(k): str(v) for k, v in (request.headers or {}).items()}
        headers.setdefault("Accept", "application/json")
        headers.setdefault("User-Agent", CONSTANTS.USER_AGENT)
        async with self._lock:
            floor = CONSTANTS.REQUEST_FLOOR_SECONDS - (time.monotonic() - CoinupRESTConnection._last_call)
            if floor > 0:
                await asyncio.sleep(floor)
            resp = await asyncio.to_thread(self._do, request.method.value, url, data, headers)
            CoinupRESTConnection._last_call = time.monotonic()
        return RESTResponse(resp)


class CoinupWSConnection(WSConnection):
    """The market socket: plain aiohttp (ws.marketketac.com is NOT Cloudflare-challenged), a browser UA and the site
    Origin, RFC 6455 autoping as the keepalive. The server's JSON {"ping": ms} is answered by the data source; no client
    JSON ping is ever sent."""

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
        headers = {"User-Agent": CONSTANTS.USER_AGENT, "Origin": CONSTANTS.ORIGIN}
        headers.update(ws_headers or {})
        self._connection = await self._client_session.ws_connect(
            ws_url,
            headers=headers,
            autoping=True,
            heartbeat=CONSTANTS.WS_HEARTBEAT_SECONDS,
            max_msg_size=max_msg_size or CONSTANTS.WS_MAX_MSG_SIZE,
        )
        self._message_timeout = None
        self._connected = True

    def close_now(self) -> None:
        if self._connection is not None and not self._connection.closed:
            asyncio.ensure_future(self._connection.close())


class CoinupSessions:
    """One curl_cffi session per impersonation profile (reused so the TLS handshake is not paid on every call) and one
    aiohttp session for the socket. Duck-types ConnectionsFactory for WebAssistantsFactory. One instance per process."""

    _instance: Optional["CoinupSessions"] = None

    def __init__(self) -> None:
        self._curl: Dict[str, Any] = {}
        self._ws: Optional[aiohttp.ClientSession] = None

    @classmethod
    def shared(cls) -> "CoinupSessions":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def session(self, profile: str):
        session = self._curl.get(profile)
        if session is None:
            # Imported here, not at module load: Hummingbot must start even before `curl-cffi` is installed in its
            # environment (a deployment prerequisite, like P1's venvs); the first CoinUp request raises a clear error
            # without it. The package cannot be imported by a process without it at all if the import were top-level.
            from curl_cffi import requests as curl_requests
            session = curl_requests.Session(impersonate=profile)
            self._curl[profile] = session
        return session

    def ws_session(self) -> aiohttp.ClientSession:
        if self._ws is None or self._ws.closed:
            self._ws = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=0, keepalive_timeout=60),
                headers={"User-Agent": CONSTANTS.USER_AGENT},
                trust_env=False,
            )
        return self._ws

    async def get_rest_connection(self) -> CoinupRESTConnection:
        return CoinupRESTConnection(self)

    async def get_ws_connection(self) -> CoinupWSConnection:
        return CoinupWSConnection(aiohttp_client_session=self.ws_session())

    async def close(self) -> None:
        for session in self._curl.values():
            try:
                session.close()
            except Exception:
                pass
        self._curl = {}
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        self._ws = None


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    time_provider: Optional[Any] = None,
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
        connections_factory=CoinupSessions.shared(),
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler, connections_factory=CoinupSessions.shared())


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def error_code(response: Any) -> Optional[str]:
    if isinstance(response, dict) and response.get("code") is not None:
        return str(response["code"])
    return None


def is_error(response: Any) -> bool:
    """Business errors ride in HTTP 200; success is a body without `code`, or with code "0"."""
    code = error_code(response)
    return code is not None and code != CONSTANTS.CODE_OK


def loads(text: str) -> Any:
    return json.loads(text, parse_float=Decimal)


async def get_current_server_time(throttler: Optional[AsyncThrottler] = None,
                                  domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """CoinUp's server time in MILLISECONDS: /sapi/v1/time answers {"timezone","serverTime": ms} (live 2026-11-27)."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request_and_get_response(
        url=public_rest_url(path_url=CONSTANTS.TIME_PATH, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id="coinup_public",
    )
    body = loads(await response.text())
    server_ms = body.get("serverTime") if isinstance(body, dict) else None
    if server_ms is None:
        raise IOError("CoinUp's /sapi/v1/time carried no serverTime")
    return float(server_ms)


class CoinupReconnectBackoff:
    """The pause before a data source's next connection attempt: a failed or short-lived connection waits WS_RETRY_BASE,
    doubling up to WS_RETRY_CAP; a connection that lived WS_RETRY_RESET_SEC resets the count."""

    def __init__(self) -> None:
        self._connected_at: Optional[float] = None
        self._failures = 0

    def connected(self) -> None:
        self._connected_at = time.monotonic()

    async def wait(self) -> None:
        lived = time.monotonic() - self._connected_at if self._connected_at is not None else 0.0
        self._connected_at = None
        if lived >= CONSTANTS.WS_RETRY_RESET_SECONDS:
            self._failures = 0
            return
        self._failures += 1
        await asyncio.sleep(min(CONSTANTS.WS_RETRY_BASE_SECONDS * 2 ** (self._failures - 1),
                                CONSTANTS.WS_RETRY_CAP_SECONDS))


async def connected_ws_assistant(ws_url: str = CONSTANTS.WSS_URL) -> WSAssistant:
    connection = await CoinupSessions.shared().get_ws_connection()
    assistant = WSAssistant(connection=connection)
    await assistant.connect(ws_url=ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_SECONDS, message_timeout=None)
    return assistant