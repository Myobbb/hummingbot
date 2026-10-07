import asyncio
import time
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitunix.bitunix_exchange import BitunixExchange


class BitunixAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Bitunix has NO private push stream (its API's WS is signed request/response; probed 2026-10-07): orders, fills
    and balances are polled over REST by BitunixExchange. This data source yields nothing.

    It stands in for a stream in one respect: `last_recv_time` is the time of the connector's last SUCCESSFUL signed
    REST read (the balance poll), so the base's `user_stream_initialized` readiness check means "private REST works"
    — True once a signed read answered, False again if none has for PRIVATE_SILENCE_LIMIT seconds (a revoked key, an
    outage), which the base then reports as the connector not ready.
    """

    _logger: Optional[HummingbotLogger] = None
    PRIVATE_SILENCE_LIMIT = 120.0

    def __init__(self, connector: "BitunixExchange") -> None:
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
        raise NotImplementedError("Bitunix has no private stream")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("Bitunix has no private stream")
