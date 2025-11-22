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
    primary_market = arbitrage_l_config_map.get("primary_market").value
    return validate_market_trading_pair(primary_market, value)


def validate_secondary_market_trading_pair(value: str) -> Optional[str]:
    secondary_market = arbitrage_l_config_map.get("secondary_market").value
    return validate_market_trading_pair(secondary_market, value)


def primary_trading_pair_prompt():
    primary_market = arbitrage_l_config_map.get("primary_market").value
    example = AllConnectorSettings.get_example_pairs().get(primary_market)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (primary_market, f" (e.g. {example})" if example else "")


def secondary_trading_pair_prompt():
    secondary_market = arbitrage_l_config_map.get("secondary_market").value
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
    c_map = arbitrage_l_config_map
    
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
        
def validate_spread_value(value: str) -> Optional[str]:
    """
    Validate spread value - accepts either:
    - A decimal percentage (0-100)
    - The string 'min' for minimum tick spread
    """
    if isinstance(value, str) and value.lower().strip() == 'min':
        return None  # Valid
    # Otherwise validate as decimal
    return validate_decimal(value, Decimal(0), Decimal("100"), inclusive=True)
 
 

def _assets_differ() -> bool:
    c_map = arbitrage_l_config_map
    ptp = c_map.get("primary_market_trading_pair").value
    stp = c_map.get("secondary_market_trading_pair").value
    addl = c_map.get("additional_markets").value if c_map.get("additional_markets") is not None else None

    def _parse_pair(p: str):
        try:
            if isinstance(p, str) and "-" in p:
                b, q = p.split("-", 1)
                return b.strip(), q.strip()
        except Exception:
            pass
        return None, None

    def _iter_additional_pairs(val):
        # Accept list-like or comma-separated string of connector:PAIR
        if not val:
            return []
        try:
            if isinstance(val, list):
                iterable = val
            else:
                s = str(val).strip()
                if s.startswith("[") and s.endswith("]"):
                    s = s.strip("[]")
                    iterable = [x.strip().strip("'\"") for x in s.split(",") if x.strip()]
                else:
                    iterable = [x.strip() for x in s.split(",") if x.strip()]
            pairs = []
            for part in iterable:
                if ":" in part:
                    _conn, pair = part.split(":", 1)
                    pairs.append(pair.strip())
            return pairs
        except Exception:
            return []

    try:
        if not ptp or not stp:
            return False
        f_base, f_quote = _parse_pair(ptp)
        s_base, s_quote = _parse_pair(stp)
        if not f_base or not s_base:
            return False
        if f_base != s_base or f_quote != s_quote:
            return True
        # compare additional markets, if any
        for pair_str in _iter_additional_pairs(addl):
            a_base, a_quote = _parse_pair(pair_str)
            if a_base and (a_base != f_base or a_quote != f_quote):
                return True
        return False
    except Exception:
        return False

def additional_markets_on_validated(value: str):
    """Add any connectors from additional_markets into required_exchanges."""
    if not value:
        return
    try:
        # Accept list-like strings (e.g., "['mexc:SLF-USDT', 'htx:SLF-USDT']") or comma-separated strings
        if isinstance(value, list):
            iterable = value
        else:
            s = str(value).strip()
            if s.startswith("[") and s.endswith("]"):
                # naive split for list-like repr
                s = s.strip("[]")
                iterable = [x.strip().strip("'\"") for x in s.split(",") if x.strip()]
            else:
                iterable = [x.strip() for x in s.split(",") if x.strip()]
        for part in iterable:
            if ":" in part:
                conn, _pair = part.split(":", 1)
                conn = conn.strip().lower()
                if conn:
                    required_exchanges.add(conn)
    except Exception:
        # best-effort; don't block config flow on parse issues
        pass


def _is_buy_in_enabled() -> bool:
    """Helper to decide whether buy-in specific fields should be prompted."""
    try:
        enabled = arbitrage_l_config_map.get("buy_in_enabled").value
        return bool(enabled)
    except Exception:
        return False


def _is_sell_off_enabled() -> bool:
    """Helper to decide whether sell-off specific fields should be prompted."""
    try:
        enabled = arbitrage_l_config_map.get("sell_off_enabled").value
        return bool(enabled)
    except Exception:
        return False

arbitrage_l_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="arbitrage_l"
    ),
    # Position Balancer - Buy-in Configuration
    "buy_in_enabled": ConfigVar(
        key="buy_in_enabled",
        type_str="bool",
        prompt="Enable buy-in to acquire assets when below target? (Yes/No)",
        prompt_on_new=True,
        default=False,
        validator=lambda v: validate_bool(v),
    ),
    "buy_in_target_usdt": ConfigVar(
        key="buy_in_target_usdt",
        prompt="Target minimum base asset value (in quote units, e.g., USDT) >>> ",
        prompt_on_new=False,
        default=Decimal("1100"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
        required_if=_is_buy_in_enabled,
    ),
    "buy_in_spread_pct": ConfigVar(
        key="buy_in_spread_pct",
        prompt="Spread percentage below top bid for buy limit orders (e.g., 0.1 for 0.1%, or 'min' for minimum tick) >>> ",
        prompt_on_new=False,
        default="min",
        validator=validate_spread_value,
        type_str="str",
        required_if=_is_buy_in_enabled,
    ),
    # Position Balancer - Sell-off Configuration
    "sell_off_enabled": ConfigVar(
        key="sell_off_enabled",
        type_str="bool",
        prompt="Enable sell-off to reduce assets when above target? (Yes/No)",
        prompt_on_new=True,
        default=False,
        validator=lambda v: validate_bool(v),
    ),
    "sell_off_target_usd": ConfigVar(
        key="sell_off_target_usd",
        prompt="Target maximum base asset value (in quote units, e.g., USDT) >>> ",
        prompt_on_new=False,
        default=Decimal("3000"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
        required_if=_is_sell_off_enabled,
    ),
    "sell_off_spread_pct": ConfigVar(
        key="sell_off_spread_pct",
        prompt="Spread percentage above top ask for sell limit orders (e.g., 0.1 for 0.1%, or 'min' for minimum tick) >>> ",
        prompt_on_new=False,
        default="min",
        validator=validate_spread_value,
        type_str="str",
        required_if=_is_sell_off_enabled,
    ),
    # Position Balancer - Order Management
    "position_balancer_refresh_interval": ConfigVar(
        key="position_balancer_refresh_interval",
        prompt="How often to cancel and replace limit orders (seconds) >>> ",
        prompt_on_new=True,
        default=Decimal("60"),
        validator=lambda v: validate_decimal(v, Decimal(1), inclusive=True),
        type_str="decimal",
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
        # Optional: allow empty input; prompting handled by legacy create flow change
        required_if=lambda: False,
        default="",
        type_str="str",
        on_validated=additional_markets_on_validated,
    ),
    "min_profitability": ConfigVar(
        key="min_profitability",
        prompt="What is the minimum profitability for you to make a trade? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("1.9"),
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal("100"), inclusive=True),
        type_str="decimal",
    ),
    "filled_order_timeout": ConfigVar(
        key="filled_order_timeout",
        prompt="Timeout in seconds for limit orders that have received fills (default: 3600 = 1 hour) >>> ",
        prompt_on_new=True,
        default=Decimal("3600"),
        validator=lambda v: validate_decimal(v, Decimal(1), inclusive=True),
        type_str="decimal",
    ),
    "use_oracle_conversion_rate": ConfigVar(
        key="use_oracle_conversion_rate",
        type_str="bool",
        prompt="Do you want to use rate oracle on unmatched trading pairs? (Yes/No) >>> ",
        prompt_on_new=False,
        default=False,
        validator=lambda v: validate_bool(v),
        on_validated=update_oracle_settings,
        required_if=_assets_differ,
    ),
    "secondary_to_primary_base_conversion_rate": ConfigVar(
        key="secondary_to_primary_base_conversion_rate",
        prompt="Enter conversion rate for secondary base asset value to primary base asset value, e.g. "
               "if primary base asset is USD and the secondary is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
        required_if=lambda: (not arbitrage_l_config_map.get("use_oracle_conversion_rate").value) and _assets_differ(),
    ),
    "secondary_to_primary_quote_conversion_rate": ConfigVar(
        key="secondary_to_primary_quote_conversion_rate",
        prompt="Enter conversion rate for secondary quote asset value to primary quote asset value, e.g. "
               "if primary quote asset is USD and the secondary is DAI and 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal",
        required_if=lambda: (not arbitrage_l_config_map.get("use_oracle_conversion_rate").value) and _assets_differ(),
    ),
}