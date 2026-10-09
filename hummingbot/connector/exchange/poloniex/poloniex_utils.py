from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# GET /feeinfo for the account (VIP0, 2026-10-09): makerRate 0.002, takerRate 0.002, specialFeeRates per symbol
# (WSTUSDT_USDT 0/0). The connector replaces this default with the account's rates once fees are fetched.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names a symbol and its two currencies.

    Poloniex's `state` (NORMAL / PAUSE / POST_ONLY) and `tradableStartTime` are NOT used to drop markets
    (non-negotiable 3: the venue decides what is tradable, and its answer to an order is logged). They drive what the
    tracker SEES: PoloniexAPIOrderBookDataSource shows a market that isn't NORMAL and started as an EMPTY book
    (5 of 40 PAUSE markets keep a frozen two-sided book), while its symbol and trading rule stay.
    """
    return all(symbol_info.get(key) not in (None, "") for key in ("symbol", "baseCurrencyName", "quoteCurrencyName"))


class PoloniexConfigMap(BaseConnectorConfigMap):
    connector: str = "poloniex"
    poloniex_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Poloniex API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    poloniex_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Poloniex secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="poloniex")


KEYS = PoloniexConfigMap.model_construct()
