from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# GroveX's API has no fee route (2026-10-09). 0.2% / 0.2% is an ESTIMATE for sizing only: the fee actually charged comes
# with every trade (order_info trade_list / all_trade `fee`, `feeCoin`) and is what gets booked. The first fills
# audit-log the rate seen ([GX-AUDIT] fee-rate).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(pair_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names its symbol and its two coins.

    GroveX's `isShow` (get_allticker) is NOT used to drop a market (runbook non-negotiable 3; XT's B2-USDT KeyError,
    2026-09-23): every listed market gets a symbol and a trading rule, and GroveX's answer to an order decides (logged in
    full). The switch drives what the tracker SEES instead: GrovexAPIOrderBookDataSource shows a hidden market as an
    empty book, the way a halt looks elsewhere.
    """
    return all(pair_info.get(key) not in (None, "") for key in ("symbol", "base_coin", "count_coin"))


class GrovexConfigMap(BaseConnectorConfigMap):
    connector: str = "grovex"
    grovex_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your GroveX API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    grovex_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your GroveX secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="grovex")


KEYS = GrovexConfigMap.model_construct()
