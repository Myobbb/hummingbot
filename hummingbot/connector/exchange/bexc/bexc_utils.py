from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# The account's tier rate, charged on the probe's fills (2026-11-13): 0.1% maker and taker, in the QUOTE on both sides
# (a buy pays notional + fee and gets the full base; a sell gets notional - fee), which is the schema's default model.
# /markets' maker_fee / taker_fee (0.001 / 0.002) are defaults, not what is charged; the rate per market comes from
# GET /fees/me and the fee actually charged with every fill.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(market: Dict[str, Any]) -> bool:
    """
    Structural check only: a market that names its symbol and its two assets.

    bexc's `status` (trading / halted) and `is_active` are NOT used to drop a market (runbook non-negotiable 3; XT's
    B2-USDT KeyError, 2026-09-23). Every listed market gets a symbol and a trading rule, and bexc's answer to an order
    decides (logged in full). The flags drive what the tracker SEES instead: BexcAPIOrderBookDataSource shows a market
    that is not trading as an empty book, the way a halt looks elsewhere.
    """
    return all(market.get(key) not in (None, "") for key in ("symbol", "base_asset", "quote_asset"))


class BexcConfigMap(BaseConnectorConfigMap):
    connector: str = "bexc"
    bexc_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your bexc API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bexc_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your bexc secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="bexc")


KEYS = BexcConfigMap.model_construct()
