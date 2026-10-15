from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# CoinW's spot API has no fee-rate route (docs, every spot page; the SDK JKorf/CoinW.Net likewise). 0.2 % is CoinW's
# standard spot rate; the first live fills' fee / quantity is audit-logged ([CW-AUDIT] fee-rate) against it. arb_l
# never reads connector fees (its profitability is gross); these feed TradeFee estimates only.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names a market and its two currencies.

    CoinW's per-market `state` (returnSymbol: 1 normal / 2 disabled) and `isFrozen` (returnTicker: 0 / 1) are NOT used
    to drop markets (non-negotiable 3: the venue decides what is tradable, and its answer to an order is logged). They
    drive what the tracker SEES: CoinwAPIOrderBookDataSource shows a market that is off as an EMPTY book, while its
    symbol and trading rule stay.
    """
    return all(symbol_info.get(key) not in (None, "") for key in ("currencyPair", "currencyBase", "currencyQuote"))


class CoinwConfigMap(BaseConnectorConfigMap):
    connector: str = "coinw"
    coinw_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinW API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coinw_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinW secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coinw")


KEYS = CoinwConfigMap.model_construct()
