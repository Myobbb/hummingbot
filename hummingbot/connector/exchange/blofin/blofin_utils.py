from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# BloFin publishes no spot fee route (the API is futures-documented). 0.1% / 0.1% is an ESTIMATE for sizing only: the
# fee actually charged comes with every fill (spot fills-history `fee` + `feeCurrency`, documented under Tax) and is
# what gets booked. The first fills audit-log the rate seen ([BF-AUDIT] fee-rate).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(instrument: Dict[str, Any]) -> bool:
    """
    Structural check only: a SPOT instrument that names its two currencies.

    BloFin's `state` (live / suspend) is NOT used to drop a market (runbook non-negotiable 3; XT's B2-USDT KeyError,
    2026-09-23). Every listed market gets a symbol and a trading rule, and BloFin's answer to an order decides (logged
    in full). The state drives what the tracker SEES instead: BlofinAPIOrderBookDataSource shows a market that is not
    live as an empty book, the way a halt looks elsewhere.
    """
    return (str(instrument.get("instType") or "SPOT").upper() == "SPOT"
            and all(instrument.get(key) not in (None, "") for key in ("instId", "baseCurrency", "quoteCurrency")))


class BlofinConfigMap(BaseConnectorConfigMap):
    connector: str = "blofin"
    blofin_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your BloFin API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    blofin_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your BloFin secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    # Every signed request carries it (ACCESS-PASSPHRASE): without it nothing private works (152408).
    blofin_passphrase: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your BloFin API key passphrase",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="blofin")


KEYS = BlofinConfigMap.model_construct()
