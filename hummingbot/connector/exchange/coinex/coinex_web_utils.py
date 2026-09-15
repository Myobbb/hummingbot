from typing import Any, Callable, Dict, Optional

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Build a full URL for a public REST endpoint.

    `path_url` is expected to be a bare path such as "/spot/depth"; CONSTANTS.REST_URL already
    carries the /v2 prefix that CoinEx's signing scheme also expects to see.
    """
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Build a full URL for a private REST endpoint. Same shape as the public one."""
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


def is_error_response(response: Dict[str, Any]) -> bool:
    """
    CoinEx wraps every REST response as {"code": int, "data": ..., "message": str}, where code 0
    means success (error.md). A non-zero code is an application-level error even though the HTTP
    status is 200.
    """
    return isinstance(response, dict) and response.get("code", CONSTANTS.RET_CODE_OK) != CONSTANTS.RET_CODE_OK


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Return CoinEx server time in MILLISECONDS.

    GET /time returns {"code":0,"data":{"timestamp": <ms>},"message":"OK"} (common/http/time).

    The unit matters: TimeSynchronizer.update_server_time_offset_with_time_provider compares this
    value against a millisecond local clock (`server_time_ms - local_server_time_pre_image_ms`), so
    returning seconds here corrupts the offset by ~1.8e12 ms and every signed request then carries a
    timestamp CoinEx rejects. The value is passed through unscaled, matching the other connectors.
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()

    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.PUBLIC_TIME_ENDPOINT, domain=domain),
        throttler_limit_id=CONSTANTS.PUBLIC_TIME_ENDPOINT,
        method=RESTMethod.GET,
    )
    return float(response["data"]["timestamp"])
