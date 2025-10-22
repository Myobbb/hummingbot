import gzip
import io
import json
from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "AURA-USDT"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("status") == 1


def decompress_ws_message(message):
    """
    Robustly handle BingX WS frames which may be gzip-compressed bytes or plain JSON (bytes/str).
    Falls back gracefully if content is not gzip.
    """
    try:
        if isinstance(message, bytes):
            # First, try gzip
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb') as gz:
                    decompressed = gz.read()
                return json.loads(decompressed.decode('utf-8'))
            except Exception:
                # Not gzip or bad gzip; try plain UTF-8 JSON
                try:
                    return json.loads(message.decode('utf-8'))
                except Exception:
                    return {}
        elif isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return {}
        else:
            return message
    except Exception:
        return {}


class BingXConfigMap(BaseConnectorConfigMap):
    connector: str = "bing_x"
    bingx_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your BingX API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bingx_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your BingX API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bingx_orderbook_depth: str = Field(
        default="100",
        json_schema_extra={
            "prompt": "Enter orderbook depth level (5/10/20/50/100/incrDepth)",
            "prompt_on_new": False,
        }
    )
    model_config = ConfigDict(title="bing_x")


KEYS = BingXConfigMap.model_construct()