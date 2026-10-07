from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# Bitunix's API has no fee endpoint and no fee route (probed 2026-10-07). 0.1% / 0.1% is an ESTIMATE for sizing only:
# the fee actually charged comes with every fill (order/deal/list `fee` + `feeCoin`) and is what gets booked. The first
# fills audit-log the rate seen ([BU-AUDIT] fee-rate).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(pair_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names its two currencies.

    Bitunix's `isOpen` is NOT used to drop a market (runbook non-negotiable 3; XT's B2-USDT KeyError, 2026-09-23).
    Every listed market gets a symbol and a trading rule, and Bitunix's answer to an order decides (logged in full).
    The switch drives what the tracker SEES instead: BitunixAPIOrderBookDataSource shows a market that is not open as
    an empty book, the way a halt looks elsewhere.
    """
    return all(pair_info.get(key) not in (None, "") for key in ("base", "quote"))


class BitunixConfigMap(BaseConnectorConfigMap):
    connector: str = "bitunix"
    bitunix_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitunix API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bitunix_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitunix secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="bitunix")


KEYS = BitunixConfigMap.model_construct()
