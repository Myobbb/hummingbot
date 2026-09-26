from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# 1179 of 1185 markets list 0.2% maker / 0.2% taker (live /v4/public/symbol, 2026-09-23). Per-market
# rates from the same endpoint replace this default once fees are fetched (XtExchange._update_trading_fees).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names a symbol and its two currencies.

    XT's flags (`state`, `tradingEnabled`, `openapiEnabled`, `orderTypes`) are NOT used to drop
    markets. The docs do not say what they mean for an API order ("openapiEnabled: is OPENAPI enabled").
    A filter on them stopped the B2-USDT orders before they were sent (2026-09-23), so XT's answer was
    never seen. Every listed market gets a symbol and a trading rule, and XT's response to an order
    is what decides. It is logged in full.

    The flags do drive what the tracker SEES: XtAPIOrderBookDataSource shows a market XT has switched
    off as an empty book (XT keeps it two-sided; FUSD 2026-09-26), the way a halt looks elsewhere.
    """
    return all(symbol_info.get(key) not in (None, "") for key in ("symbol", "baseCurrency", "quoteCurrency"))


class XtConfigMap(BaseConnectorConfigMap):
    connector: str = "xt"
    xt_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your XT API key (appkey)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    xt_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your XT secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="xt")


KEYS = XtConfigMap.model_construct()
