from hummingbot.client.config.config_validators import validate_decimal, validate_exchange
from hummingbot.client.config.config_validators import validate_market_trading_pair
from hummingbot.client.config.config_var import ConfigVar
from typing import Optional
from decimal import Decimal

def trading_pair_prompt() -> str:
    connector = order_book_alignment_config_map.get("connector").value
    return f'Enter the token trading pair on {connector} >>> '

def target_asset_amount_prompt():
    trading_pair = order_book_alignment_config_map.get("trading_pair").value
    base_token, _ = trading_pair.split("-")
    return f"What is the total amount of {base_token} to be traded? (Default is 1.0) >>> "

def asset_amount_per_trade_prompt():
    trading_pair = order_book_alignment_config_map.get("trading_pair").value
    base_token, _ = trading_pair.split("-")
    return f"What is the amount of {base_token} to be traded every order? (Default is 1.0) >>> "

def validate_trading_pair(value: str) -> Optional[str]:
    exchange = order_book_alignment_config_map.get("connector").value
    return validate_market_trading_pair(exchange, value)

def validate_trade_side(value: str) -> Optional[str]:
    if value in {"buy", "sell", ""}:
        return None
    return "Invalid operation type."

order_book_alignment_config_map ={
    "strategy":
        ConfigVar(key="strategy",
                  prompt=None,
                  default="order_book_alignment",
    ),
    "connector":
        ConfigVar(key="connector",
                  prompt="Enter the name of spot connector >>> ",
                  validator=validate_exchange,
                  prompt_on_new=True,
    ),
    "trading_pair":
        ConfigVar(key="trading_pair",
                  prompt=trading_pair_prompt,
                  validator=validate_trading_pair,
                  prompt_on_new=True,
    ),
    "is_buy":
        ConfigVar(key="is_buy",
                  prompt="What operation will be executed? (buy/sell) >>> ",
                  type_str="str",
                  validator=validate_trade_side,
                  default="buy",
                  prompt_on_new=True),
    "target_asset_amount":
        ConfigVar(key="target_asset_amount",
                  prompt=target_asset_amount_prompt,
                  default=1.0,
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
                  prompt_on_new=True),
    "asset_amount_per_trade":
        ConfigVar(key="asset_amount_per_trade",
                  prompt=asset_amount_per_trade_prompt,
                  default=1.0,
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
                  prompt_on_new=True),
    "price_limit":
        ConfigVar(key="price_limit",
                  prompt="What is the price limit for the orders? >>> ",
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
                  prompt_on_new=True),
    "spread":
        ConfigVar(key="spread",
                  prompt="How far away from the top orderbook price do you want to place the order?"
                         " (0 to min price step: auto.) >>> ",
                  type_str="decimal",
                  default=0,
                  validator=lambda v: validate_decimal(v),
                  prompt_on_new=True),
    "order_refresh_time":
        ConfigVar(key="order_refresh_time",
                  prompt="How often do you want to cancel and replace order in seconds?"
                         " (Default is 10 seconds. Enter 0 to skip refresh delay.) >>> ",
                  type_str="float",
                  default=10,
                  validator=lambda v: validate_decimal(v, 0, inclusive=True),
                  prompt_on_new=True),
    "place_after_fill_order_delay":
        ConfigVar(key="place_after_fill_order_delay",
                  prompt="How many seconds after filling an order do you want to wait before placing a new order?"
                         " (Default is 0 seconds.) >>> ",
                  type_str="float",
                  default=0,
                  validator=lambda v: validate_decimal(v, 0, inclusive=True),
                  prompt_on_new=True),
    "price_limit_retry_duration":
        ConfigVar(key="price_limit_retry_duration",
                  prompt="How many seconds after the price limit is reached should the request be repeated?"
                         "(Default is 300 seconds) >>> ",
                  type_str="float",
                  default=300,
                  validator=lambda v: validate_decimal(v, 1, inclusive=False),
                  prompt_on_new=True)
}