from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
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
            "prompt": "Enter the BSC RPC URL the books are quoted through (BlockRazor's free RPC: https://bsc.blockrazor.xyz)",
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
    # The wallet's credential, like a CEX API secret: stored encrypted with the Hummingbot password (the Ethereum v3
    # keyfile format, config_crypt). It is checked by the connector, never here: at login HMB validates this model while
    # the secret fields still hold their encrypted text, so a format check here would refuse every stored key and stop
    # HMB's login. The empty default keeps a bsc.yml written before this field loading (books and balances only).
    bsc_private_key: SecretStr = Field(
        default="",
        json_schema_extra={
            "prompt": "Enter the private key of that wallet (stored encrypted with your Hummingbot password)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="bsc")


KEYS = BscConfigMap.model_construct()

# Wallet 2 (Pavel 2026-11-10): this connector under its own name and keys file, by Hummingbot's other-domain mechanism (as
# binance_us is binance's), so `connect bsc2` stores conf/connectors/bsc2.yml and never touches bsc.yml. Its fields
# keep bsc's names: Hummingbot hands them to BscExchange as they are, with the domain `bsc2`, which makes that instance
# wallet 2 (bsc_constants.WALLET2_*: its name, its swap ledger). `control create` puts a new strategy's bsc leg here.
WALLET2 = CONSTANTS.WALLET2_NAME


class Bsc2ConfigMap(BscConfigMap):
    connector: str = WALLET2
    model_config = ConfigDict(title=WALLET2)


OTHER_DOMAINS = [WALLET2]
OTHER_DOMAINS_PARAMETER = {WALLET2: WALLET2}
OTHER_DOMAINS_EXAMPLE_PAIR = {WALLET2: EXAMPLE_PAIR}
OTHER_DOMAINS_DEFAULT_FEES = {WALLET2: DEFAULT_FEES}
OTHER_DOMAINS_KEYS = {WALLET2: Bsc2ConfigMap.model_construct()}
