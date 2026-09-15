import zlib
from decimal import Decimal
from typing import Any, Dict, Union

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# CoinEx spot standard fee tier: 0.2% maker / 0.2% taker.
# https://www.coinex.com/en/fees  — per-market rates are also returned by GET /spot/market
# (maker_fee_rate / taker_fee_rate) and override this default once trading rules are fetched.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


def decompress_ws_message(message: Union[bytes, bytearray, str]) -> Union[str, bytes, bytearray]:
    """
    CoinEx pushes every WebSocket frame as gzip-compressed binary (magic 1f8b).

    The window-bits value MAX_WBITS|16 selects gzip framing specifically; this is the exact call
    proven in production by the P1 ws_book_checker adapter
    (Tracker_cex_cex/ws_book_checker/exchanges/coinex.py).

    Text frames are passed through untouched so the caller can handle them uniformly.
    """
    if not isinstance(message, (bytes, bytearray)):
        return message
    try:
        return zlib.decompress(message, zlib.MAX_WBITS | 16).decode("utf-8")
    except Exception:
        # Fall back to raw bytes rather than dropping the frame; the caller logs and skips
        # anything it cannot parse.
        try:
            return bytes(message).decode("utf-8")
        except Exception:
            return message


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Decide whether a market from GET /spot/market is tradable by this connector.

    Gates on the fields CoinEx documents in spot/market/http/list-market:
      - is_api_trading_available: the market rejects API orders when False
      - status: enum.md#market_status defines exactly three values — `bidding`, `counting_down`
        and `online`. Only `online` means the market is actually available for trading; the other
        two are pre-launch auction states.
      - delisted_at: non-zero means a delisting is already scheduled
    """
    if not exchange_info.get("market"):
        return False
    if exchange_info.get("is_api_trading_available") is False:
        return False
    if exchange_info.get("status") != "online":
        return False
    if exchange_info.get("delisted_at"):
        return False
    return True


class CoinexConfigMap(BaseConnectorConfigMap):
    connector: str = "coinex"
    coinex_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinEx API key (access_id)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coinex_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your CoinEx secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coinex")


KEYS = CoinexConfigMap.model_construct()
