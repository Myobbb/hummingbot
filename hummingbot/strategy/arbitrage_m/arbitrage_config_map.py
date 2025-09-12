from decimal import Decimal
from typing import Optional

import hummingbot.client.settings as settings
from hummingbot.client.config.config_helpers import parse_cvar_value
from hummingbot.client.config.config_validators import (
    validate_bool,
    validate_decimal,
    validate_exchange,
    validate_market_trading_pair,
)
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.settings import AllConnectorSettings, required_exchanges


def validate_primary_market_trading_pair(value: str) -> Optional[str]:
    primary_market = arbitrage_m_config_map.get("primary_market").value
    return validate_market_trading_pair(primary_market, value)


def validate_secondary_market_trading_pair(value: str) -> Optional[str]:
    secondary_market = arbitrage_m_config_map.get("secondary_market").value
    return validate_market_trading_pair(secondary_market, value)


def primary_trading_pair_prompt():
    primary_market = arbitrage_m_config_map.get("primary_market").value
    example = AllConnectorSettings.get_example_pairs().get(primary_market)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (primary_market, f" (e.g. {example})" if example else "")


def secondary_trading_pair_prompt():
    secondary_market = arbitrage_m_config_map.get("secondary_market").value
    example = AllConnectorSettings.get_example_pairs().get(secondary_market)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (secondary_market, f" (e.g. {example})" if example else "")


def additional_markets_prompt():
    return (
        "Optionally add more markets as a comma-separated list of connector:TRADING_PAIR entries\n"
        "For example: binance:BTC-USDT, kucoin:BTC-USDT (Leave blank to skip) >>> "
    )


def secondary_market_on_validated(value: str):
    required_exchanges.add(value)


def update_oracle_settings(value: str):
    """Update oracle settings based on configuration"""
    c_map = arbitrage_m_config_map
    
    # Ensure all required values are present
    if not (c_map["use_oracle_conversion_rate"].value is not None and
            c_map["primary_market_trading_pair"].value is not None and
            c_map["secondary_market_trading_pair"].value is not None):
        return
    
    use_oracle = parse_cvar_value(c_map["use_oracle_conversion_rate"], c_map["use_oracle_conversion_rate"].value)
    first_base, first_quote = c_map["primary_market_trading_pair"].value.split("-")
    second_base, second_quote = c_map["secondary_market_trading_pair"].value.split("-")
    
    # Check if assets differ
    assets_differ = first_base != second_base or first_quote != second_quote
    
    if use_oracle and assets_differ:
        settings.required_rate_oracle = True
        settings.rate_oracle_pairs = []
        if first_base != second_base:
            settings.rate_oracle_pairs.append(f"{second_base}-{first_base}")
        if first_quote != second_quote:
            settings.rate_oracle_pairs.append(f"{second_quote}-{first_quote}")
    else:
        # Either not using oracle or assets match - no oracle needed
        settings.required_rate_oracle = False
        settings.rate_oracle_pairs = []


def additional_markets_on_validated(value: str):
    """Add any connectors from additional_markets into required_exchanges."""
    if not value:
        return
    try:
        parts = [p.strip() for p in value.split(",") if p.strip()]
        for part in parts:
            if ":" in part:
                conn, _pair = part.split(":", 1)
                conn = conn.strip().lower()
                if conn:
                    required_exchanges.add(conn)
    except Exception:
        # best-effort; don't block config flow on parse issues
        pass

arbitrage_m_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="arbitrage_m"
    ),
    "primary_market": ConfigVar(
        key="primary_market",
        prompt="Enter your primary spot connector >>> ",
        prompt_on_new=True,
        validator=validate_exchange,
        on_validated=lambda value: required_exchanges.add(value),
    ),
    "secondary_market": ConfigVar(
        key="secondary_market",
        prompt="Enter your secondary spot connector >>> ",
        prompt_on_new=True,
        validator=validate_exchange,
        on_validated=secondary_market_on_validated,
    ),
    "primary_market_trading_pair": ConfigVar(
        key="primary_market_trading_pair",
        prompt=primary_trading_pair_prompt,
        prompt_on_new=True,
        validator=validate_primary_market_trading_pair,
        on_validated=update_oracle_settings,
    ),
    "secondary_market_trading_pair": ConfigVar(
        key="secondary_market_trading_pair",
        prompt=secondary_trading_pair_prompt,
        prompt_on_new=True,
        validator=validate_secondary_market_trading_pair,
        on_validated=update_oracle_settings,
    ),
    # Optional list, comma-separated entries like: binance:BTC-USDT, kucoin:BTC-USDT
    "additional_markets": ConfigVar(
        key="additional_markets",
        prompt=additional_markets_prompt,
        prompt_on_new=True,
        default="",
        type_str="str",
        on_validated=additional_markets_on_validated,
    ),
    "min_profitability": ConfigVar(
        key="min_profitability",
        prompt="What is the minimum profitability for you to make a trade? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.3"),
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal("100"), inclusive=True),
        type_str="decimal",
    ),
    "use_oracle_conversion_rate": ConfigVar(
        key="use_oracle_conversion_rate",
        type_str="bool",
        prompt="Do you want to use rate oracle on unmatched trading pairs? (Yes/No) >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_bool(v),
        on_validated=update_oracle_settings,
    ),
    "secondary_to_primary_base_conversion_rate": ConfigVar(
        key="secondary_to_primary_base_conversion_rate",
        prompt="Enter conversion rate for secondary base asset value to primary base asset value, e.g. "
               "if primary base asset is USD and the secondary is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
    ),
    "secondary_to_primary_quote_conversion_rate": ConfigVar(
        key="secondary_to_primary_quote_conversion_rate",
        prompt="Enter conversion rate for secondary quote asset value to primary quote asset value, e.g. "
               "if primary quote asset is USD and the secondary is DAI and 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
    ),
}
