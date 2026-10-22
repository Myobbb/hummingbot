from hummingbot.connector.utilities.evm_dex.evm_dex_order_book_data_source import EvmDexAPIOrderBookDataSource


class EthAPIOrderBookDataSource(EvmDexAPIOrderBookDataSource):
    """eth's books: the EVM core's polled data source (its own class, so its logger is named for eth)"""
