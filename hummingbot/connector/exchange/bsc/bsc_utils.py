from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# An ordinary exchange-type connector (P2-DEX, 2026-10-14), not a Gateway one: no gateway code path is involved.
CENTRALIZED = True
EXAMPLE_PAIR = "CAKE-USDT"

# The pools' own fees are inside every quoted price (and the trade's tax, P1's tax_pct); a swap's gas is booked from its
# receipt (step 4).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
)


class BscConfigMap(BaseConnectorConfigMap):
    connector: str = "bsc"
    bsc_rpc_url: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the BSC RPC URL the books are quoted through (BlockPI, with its key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bsc_wallet_address: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the wallet address P2 trades from (0x...)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="bsc")


KEYS = BscConfigMap.model_construct()
