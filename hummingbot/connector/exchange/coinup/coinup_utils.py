from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# CoinUp has no fee route. 0.2% / 0.2% is an ESTIMATE for sizing only: the fee actually charged comes with every fill
# (`GET /sapi/v1/myTrades` -> `fee`, `feeCoin`), and that is what gets booked. The first live fills audit-log the rate.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(pair_info: Dict[str, Any]) -> bool:
    """Structural check only: the v1 symbols row names its wire symbol and its two coins. CoinUp's `isShow` (the site's
    live flag in get_allticker) is NOT used to drop a market (runbook non-negotiable 3): every listed market gets a
    symbol and a trading rule, and the venue's answer to an order decides. The switch drives what the tracker SEES —
    a hidden market is shown as an empty book by the data source, the way a halt looks elsewhere."""
    return all(pair_info.get(key) not in (None, "") for key in ("symbol", "base_coin", "count_coin"))


class CoinupConfigMap(BaseConnectorConfigMap):
    connector: str = "coinup"
    coinup_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinUp API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coinup_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinUp secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coinup")


KEYS = CoinupConfigMap.model_construct()