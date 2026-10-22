from hummingbot.connector.utilities.evm_dex.evm_dex_user_stream_data_source import EvmDexAPIUserStreamDataSource


class EthAPIUserStreamDataSource(EvmDexAPIUserStreamDataSource):
    """eth has no user stream: a swap's fill comes from its receipt (the EVM core's; its own class for its logger)"""
