from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.order_book_alignment import OrderBookAlignment
from hummingbot.strategy.order_book_alignment.order_book_alignment_config_map import \
    order_book_alignment_config_map as c_map

from typing import (
    List,
    Tuple,
)

def start(self):
    try:
        connector = c_map.get("connector").value.lower()                # =Exchange name (bybit_testnet)
        trading_pair = c_map.get("trading_pair").value                  # =Trading pair (BTC-USDT)
        target_asset_amount = c_map.get("target_asset_amount").value
        asset_amount_per_trade = c_map.get("asset_amount_per_trade").value
        order_refresh_time = c_map.get("order_refresh_time").value
        place_after_fill_order_delay = c_map.get("place_after_fill_order_delay").value
        price_limit = c_map.get("price_limit").value
        trade_side = c_map.get("is_buy").value
        price_limit_retry_duration = c_map.get("price_limit_retry_duration").value
        spread = c_map.get("spread").value
        is_buy = trade_side == "buy"

        try:
            assets: Tuple[str, str] = self._initialize_market_assets(connector, [trading_pair])[0]
        except ValueError as e:
            self.notify(str(e))
            return

        market_names: List[Tuple[str, List[str]]] = [(connector, [trading_pair])]

        self._initialize_markets(market_names)
        market_info = [self.markets[connector], trading_pair] + list(assets)
        self.market_trading_pair_tuples = [MarketTradingPairTuple(*market_info)]

        self.strategy = OrderBookAlignment(
	    market_info=MarketTradingPairTuple(*market_info),
	    target_asset_amount=target_asset_amount,
	    asset_amount_per_trade=asset_amount_per_trade,
	    price_limit=price_limit,
	    spread=spread,
	    is_buy=is_buy,
	    price_limit_retry_duration=price_limit_retry_duration,
	    order_refresh_time=order_refresh_time,
	    place_after_fill_order_delay=place_after_fill_order_delay
	)

    except Exception as e:
        self.notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)
