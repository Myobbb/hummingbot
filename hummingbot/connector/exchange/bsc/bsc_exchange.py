"""P2-DEX: the `bsc` connector (2026-10-14), on the EVM core since 2026-10-22: the core's EvmDexExchange with BSC's spec
(bsc_constants). This module keeps what HMB and the chain's records need by name: BscExchange (HMB finds the connector as
bsc_exchange.BscExchange), its config fields (bsc_rpc_url, bsc_wallet_address, bsc_private_key: bsc_utils.BscConfigMap), its
error classes (their names are in its log lines) and its data sources (their loggers named for bsc). Everything else is the
core's: hummingbot/connector/utilities/evm_dex/ (wiki p2-dex-eth §code shape)."""
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
from hummingbot.connector.exchange.bsc.bsc_api_order_book_data_source import BscAPIOrderBookDataSource
from hummingbot.connector.exchange.bsc.bsc_api_user_stream_data_source import BscAPIUserStreamDataSource
from hummingbot.connector.utilities.evm_dex.evm_dex_exchange import (
    EvmDexExchange,
    EvmDexOrderRefused,
    EvmDexSendRefused,
    EvmDexTradingDisabled,
)
from hummingbot.connector.utilities.evm_dex.evm_dex_feed import sources_for

# the feed's code for bsc's child, read as this module is imported (its spec just imported above): the code HMB runs
SOURCES = sources_for(CONSTANTS)


class BscTradingDisabled(EvmDexTradingDisabled):
    """an order on `bsc` while bsc_constants.TRADING_ENABLED is False (the kill switch)"""


class BscOrderRefused(EvmDexOrderRefused):
    """an order the connector can't turn into a swap: no fresh book, no route, no approval, no wallet key, another swap out"""


class BscSendRefused(EvmDexSendRefused, BscOrderRefused):
    """every builder refused the swap's bundle: nothing went out, and its nonce is free again (read anew by the feed)"""


class BscExchange(EvmDexExchange):
    """the `bsc` connector: BSC's spec on the EVM core (EvmDexExchange holds the whole behaviour and its contract)"""
    C = CONSTANTS
    SOURCES = SOURCES
    ORDER_BOOK_DATA_SOURCE = BscAPIOrderBookDataSource
    USER_STREAM_DATA_SOURCE = BscAPIUserStreamDataSource
    TradingDisabled, OrderRefused, SendRefused = BscTradingDisabled, BscOrderRefused, BscSendRefused

    def __init__(self,
                 bsc_rpc_url: str,
                 bsc_wallet_address: str,
                 bsc_private_key: str = "",
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(bsc_rpc_url, bsc_wallet_address, bsc_private_key, balance_asset_limit, rate_limits_share_pct,
                         trading_pairs, trading_required, domain)
