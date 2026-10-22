"""P2-DEX: the `eth` connector (2026-10-22), Ethereum mainnet on the EVM core: the core's EvmDexExchange with ETH's spec
(eth_constants). This module keeps what HMB and the chain's records need by name: EthExchange (HMB finds the connector as
eth_exchange.EthExchange), its config fields (eth_rpc_url, eth_wallet_address, eth_private_key: eth_utils.EthConfigMap), its
error classes (their names are in its log lines) and its data sources (their loggers named for eth). Everything else is the
core's: hummingbot/connector/utilities/evm_dex/ (wiki p2-dex-eth §code shape)."""
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange.eth import eth_constants as CONSTANTS
from hummingbot.connector.exchange.eth.eth_api_order_book_data_source import EthAPIOrderBookDataSource
from hummingbot.connector.exchange.eth.eth_api_user_stream_data_source import EthAPIUserStreamDataSource
from hummingbot.connector.utilities.evm_dex.evm_dex_exchange import (
    EvmDexExchange,
    EvmDexOrderRefused,
    EvmDexSendRefused,
    EvmDexTradingDisabled,
)
from hummingbot.connector.utilities.evm_dex.evm_dex_feed import sources_for

# the feed's code for eth's child, read as this module is imported (its spec just imported above): the code HMB runs
SOURCES = sources_for(CONSTANTS)


class EthTradingDisabled(EvmDexTradingDisabled):
    """an order on `eth` while eth_constants.TRADING_ENABLED is False (the kill switch)"""


class EthOrderRefused(EvmDexOrderRefused):
    """an order the connector can't turn into a swap: no fresh book, no route, no approval, no wallet key, another swap out"""


class EthSendRefused(EvmDexSendRefused, EthOrderRefused):
    """every builder refused the swap's bundle: nothing went out, and its nonce is free again (read anew by the feed)"""


class EthExchange(EvmDexExchange):
    """the `eth` connector: ETH's spec on the EVM core (EvmDexExchange holds the whole behaviour and its contract)"""
    C = CONSTANTS
    SOURCES = SOURCES
    ORDER_BOOK_DATA_SOURCE = EthAPIOrderBookDataSource
    USER_STREAM_DATA_SOURCE = EthAPIUserStreamDataSource
    TradingDisabled, OrderRefused, SendRefused = EthTradingDisabled, EthOrderRefused, EthSendRefused

    def __init__(self,
                 eth_rpc_url: str,
                 eth_wallet_address: str,
                 eth_private_key: str = "",
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(eth_rpc_url, eth_wallet_address, eth_private_key, balance_asset_limit, rate_limits_share_pct,
                         trading_pairs, trading_required, domain)
