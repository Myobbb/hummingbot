from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# The standard rate, 0.1% maker and taker, in the QUOTE on both sides (the report's tradeFee is "fee for trade in
# secondary currency"; REST trade rows' `fee` is in the quote too, with `alternateFee`/`alternateFeeAsset` when a
# platform token paid it). No fee route exists: the market list carries none and the docs expose none, so the rate is
# this default and the fee actually charged comes with every fill (push and REST row alike). The first live fill's
# rate is audit-logged and checked against this number (§6.3, the LBank fractions trap).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(market: Dict[str, Any]) -> bool:
    """
    Structural check only: a market that names its symbol and its base ticker.

    `isActive`, `isPaused`, `apiExcluded`, `pauseBuys`/`pauseSells` and `isSpecialTreatment` are NOT used to drop a
    market (runbook non-negotiable 3; XT's B2-USDT KeyError, 2026-09-23). Every listed market gets a symbol and a
    trading rule, and NonKYC's answer to an order decides -- logged in full. The flags drive what the tracker SEES
    instead: the data source shows a market the venue has paused as an empty book, the way a halt looks elsewhere.
    """
    return all(market.get(key) not in (None, "") for key in ("symbol", "primaryTicker"))


def is_market_tradable(market: Dict[str, Any]) -> bool:
    """The venue's own trading switch for a listed market (P1's rule: isActive, not isPaused). `apiExcluded` is NOT
    part of it here: P1 drops those markets upstream, and a connector never filters (non-negotiable 3)."""
    return bool(market.get("isActive")) and not bool(market.get("isPaused"))


class NonkycConfigMap(BaseConnectorConfigMap):
    connector: str = "nonkyc"
    nonkyc_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your NonKYC API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    nonkyc_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your NonKYC API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="nonkyc")


KEYS = NonkycConfigMap.model_construct()
