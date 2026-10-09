import asyncio
import time
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.grovex.grovex_exchange import GrovexExchange


class GrovexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    GroveX has NO private push stream (ChainUp open/api v1: the socket is market data only): orders, fills and balances
    are polled over REST by GrovexExchange. This data source yields nothing.

    It stands in for a stream in one respect: `last_recv_time` is the time of the connector's last SUCCESSFUL signed
    REST read, so the base's `user_stream_initialized` readiness check means "private REST works" — True once a signed
    read answered, False again if none has for PRIVATE_SILENCE_LIMIT seconds (a revoked key, a relay or venue outage),
    which the base then reports as the connector not ready. With no order open the balance read (~22 s, every ~27 s) is
    the only signed read.
    """

    _logger: Optional[HummingbotLogger] = None
    PRIVATE_SILENCE_LIMIT = 120.0

    def __init__(self, connector: "GrovexExchange") -> None:
        super().__init__()
        self._connector = connector

    @property
    def last_recv_time(self) -> float:
        last = self._connector.last_private_read_time
        if last <= 0 or time.time() - last > self.PRIVATE_SILENCE_LIMIT:
            return 0
        return last

    async def listen_for_user_stream(self, output: asyncio.Queue):
        while True:
            await asyncio.sleep(3600)

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("GroveX has no private stream")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("GroveX has no private stream")
