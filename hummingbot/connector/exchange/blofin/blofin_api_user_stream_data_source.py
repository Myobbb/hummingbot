import asyncio
import time
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.blofin.blofin_exchange import BlofinExchange


class BlofinAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    BloFin has NO spot push stream for an API key (probed 2026-11-11 from myserver): the API's private socket takes
    futures `orders` / `account` only (every spot channel name answers 60012), and the website's private spot socket
    wants a website session (100017 "user no login"). Orders, fills and balances are polled over REST by
    BlofinExchange. This data source yields nothing.

    It stands in for a stream in one respect: `last_recv_time` is the time of the connector's last SUCCESSFUL signed
    REST read, so the base's `user_stream_initialized` readiness check means "private REST works": True once a signed
    read answered, False again if none has for PRIVATE_SILENCE_LIMIT seconds (a revoked key, an outage), which the base
    then reports as the connector not ready.
    """

    _logger: Optional[HummingbotLogger] = None
    PRIVATE_SILENCE_LIMIT = 120.0

    def __init__(self, connector: "BlofinExchange") -> None:
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
        raise NotImplementedError("BloFin has no spot private stream for an API key")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("BloFin has no spot private stream for an API key")
