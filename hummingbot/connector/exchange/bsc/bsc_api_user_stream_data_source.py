from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class BscAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """The chain has no user stream: a swap's fill comes from its receipt (step 4). ExchangePyBase builds a user stream
    tracker in its constructor; BscExchange.start_network never starts it."""

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("bsc has no user stream")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("bsc has no user stream")
