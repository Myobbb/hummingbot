from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# Hotcoin has no fee endpoint in its API. Its site's public market list (www.hotcoin.com/hk-web/symbol_label/info,
# WEB tier) prices all 364 markets at buyFee = sellFee = 0.002 (2026-10-05), the VIP-0 rate. The fee actually paid
# comes with every fill (the order push's and detailById's `fees`), so this only feeds estimates.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    Structural check only: the entry names a symbol and its two currencies.

    Hotcoin's `state` (enable | disable) is NOT used to drop a market (runbook non-negotiable 3; XT's B2-USDT
    KeyError, 2026-09-23). Every listed market gets a symbol and a trading rule, and Hotcoin's answer to an order
    decides; it is logged in full. The flag drives what the tracker SEES instead: HotcoinAPIOrderBookDataSource
    shows a market that is not `enable` (or no longer listed) as an empty book, the way a halt looks elsewhere.
    """
    return all(symbol_info.get(key) not in (None, "") for key in ("symbol", "baseCurrency", "quoteCurrency"))


class HotcoinConfigMap(BaseConnectorConfigMap):
    connector: str = "hotcoin"
    hotcoin_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Hotcoin API key (Access Key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    hotcoin_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Hotcoin secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="hotcoin")


KEYS = HotcoinConfigMap.model_construct()
