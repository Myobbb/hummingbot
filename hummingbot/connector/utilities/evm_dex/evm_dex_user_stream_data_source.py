from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class EvmDexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """The chain has no user stream: a swap's fill comes from its receipt (step 4). ExchangePyBase builds a user stream
    tracker in its constructor; EvmDexExchange.start_network never starts it. Each chain subclasses it (its logger's name)."""

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("a DEX connector has no user stream")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("a DEX connector has no user stream")
