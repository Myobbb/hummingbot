from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# LBank's published spot rate is 0.1% maker / 0.1% taker (CCXT's default too). The account's own per-pair rates come
# from customer_trade_fee.do (LbankExchange._update_trading_fees) and replace this for every pair they cover.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    """
    Structural check only: the /v2/accuracy.do entry names a base_quote symbol and its two precisions.

    LBank has no per-market trading flag in its API; the pair list is the tradable set. Nothing here drops a market
    (runbook non-negotiable 3; XT's B2-USDT KeyError, 2026-09-23): every listed pair gets a symbol and a trading rule,
    and LBank's answer to an order decides, logged in full. A pair that leaves the list (a delisting) is shown as an
    EMPTY book by LbankAPIOrderBookDataSource.
    """
    symbol = str(rule.get("symbol") or "")
    return ("_" in symbol and all(rule.get(key) not in (None, "")
                                  for key in ("priceAccuracy", "quantityAccuracy")))


class LbankConfigMap(BaseConnectorConfigMap):
    connector: str = "lbank"
    lbank_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your LBank API key (an HmacSHA256 key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    lbank_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your LBank secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="lbank")


KEYS = LbankConfigMap.model_construct()
