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
    A market this connector can trade: ONLINE, trading enabled and **API trading enabled**.

    `openapiEnabled` is the gate that matters. Live on 2026-09-23 only 434 of 1096 USDT markets
    passed all three, and 213 of the 459 XT tickers in the P1 groups were openapiEnabled=false. An
    order on such a market fails with SYMBOL_005, so it is kept out of the symbol map altogether.

    Open question, recorded rather than guessed: 313 of the 434 list `orderTypes: []` (the rest list
    LIMIT/MARKET). An empty list is not treated as "no LIMIT", because that would drop most tradable
    markets (PEPE included) on an undocumented reading. The first live order on such a market settles
    it; the placement is audit-logged.
    """
    if not symbol_info.get("symbol"):
        return False
    if symbol_info.get("state") != "ONLINE":
        return False
    if symbol_info.get("tradingEnabled") is not True or symbol_info.get("openapiEnabled") is not True:
        return False
    order_types = symbol_info.get("orderTypes") or []
    if order_types and "LIMIT" not in order_types:
        return False
    return True


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
