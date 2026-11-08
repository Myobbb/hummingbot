from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# The symbol list prices each market itself (makerFee / takerFee as fractions): 287 of 376 markets at 0.002 / 0.002,
# 44 with a 0.000001 maker fee, BTC 0 / 0.002 (2026-11-08). CoinstoreExchange._update_trading_fees loads them per
# market; this is the fallback for a market not (yet) in that list.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names a symbol and its two currencies.

    `openTrade` is NOT used to drop a market (runbook non-negotiable 3; XT's B2-USDT KeyError, 2026-09-23): every
    listed market gets a symbol and a trading rule, and Coinstore's answer to an order decides; it is logged in full.
    The flag drives what the tracker SEES instead: CoinstoreAPIOrderBookDataSource shows a market that is not open (or
    no longer listed) as an empty book, the way a halt looks elsewhere.
    """
    return all(symbol_info.get(key) not in (None, "")
               for key in ("symbolCode", "tradeCurrencyCode", "quoteCurrencyCode"))


class CoinstoreConfigMap(BaseConnectorConfigMap):
    connector: str = "coinstore"
    coinstore_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Coinstore API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coinstore_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Coinstore secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coinstore")


KEYS = CoinstoreConfigMap.model_construct()
