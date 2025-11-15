from typing import List, Tuple
from decimal import Decimal

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage.arbitrage_market_pair import ArbitrageMarketPair
from hummingbot.strategy.arbitrage.arbitrage import ArbitrageStrategy
from hummingbot.strategy.arbitrage.arbitrage_config_map import arbitrage_config_map


def start(self):
    """Initialize and start the arbitrage strategy"""
    try:
        # Extract configuration values
        primary_market = arbitrage_config_map.get("primary_market").value.lower()
        secondary_market = arbitrage_config_map.get("secondary_market").value.lower()
        raw_primary_trading_pair = arbitrage_config_map.get("primary_market_trading_pair").value
        raw_secondary_trading_pair = arbitrage_config_map.get("secondary_market_trading_pair").value
        min_profitability = arbitrage_config_map.get("min_profitability").value / Decimal("100")
        use_oracle_conversion_rate = arbitrage_config_map.get("use_oracle_conversion_rate").value
        secondary_to_primary_base_conversion_rate = arbitrage_config_map["secondary_to_primary_base_conversion_rate"].value
        secondary_to_primary_quote_conversion_rate = arbitrage_config_map["secondary_to_primary_quote_conversion_rate"].value
        
        # Validate markets are different
        if primary_market == secondary_market:
            self.notify("Primary and secondary markets must be different exchanges.")
            return

        # Parse trading pairs
        primary_trading_pair: str = raw_primary_trading_pair
        secondary_trading_pair: str = raw_secondary_trading_pair
        
        if "-" not in primary_trading_pair or "-" not in secondary_trading_pair:
            self.notify("Invalid trading pair format. Use BASE-QUOTE format (e.g., BTC-USDT).")
            return
            
        primary_base, primary_quote = primary_trading_pair.split("-")
        secondary_base, secondary_quote = secondary_trading_pair.split("-")
        primary_assets: Tuple[str, str] = (primary_base, primary_quote)
        secondary_assets: Tuple[str, str] = (secondary_base, secondary_quote)

        # Initialize markets
        market_names: List[Tuple[str, List[str]]] = [
            (primary_market, [primary_trading_pair]),
            (secondary_market, [secondary_trading_pair])
        ]
        self.initialize_markets(market_names)

        # Create market trading pair tuples
        primary_data = [self.markets[primary_market], primary_trading_pair] + list(primary_assets)
        secondary_data = [self.markets[secondary_market], secondary_trading_pair] + list(secondary_assets)
        self.market_trading_pair_tuples = [
            MarketTradingPairTuple(*primary_data), 
            MarketTradingPairTuple(*secondary_data)
        ]
        self.market_pair = ArbitrageMarketPair(*self.market_trading_pair_tuples)
        
        # Initialize strategy
        self.strategy = ArbitrageStrategy()
        self.strategy.init_params(
            market_pairs=[self.market_pair],
            min_profitability=min_profitability,
            logging_options=(ArbitrageStrategy.OPTION_LOG_STATUS_REPORT |
                           ArbitrageStrategy.OPTION_LOG_ORDER_COMPLETED |
                           ArbitrageStrategy.OPTION_LOG_CREATE_ORDER),
            use_oracle_conversion_rate=use_oracle_conversion_rate,
            secondary_to_primary_base_conversion_rate=secondary_to_primary_base_conversion_rate,
            secondary_to_primary_quote_conversion_rate=secondary_to_primary_quote_conversion_rate,
            hb_app_notification=True
        )
        
        self.logger().info(f"Arbitrage strategy started: {primary_market} {primary_trading_pair} <-> "
                          f"{secondary_market} {secondary_trading_pair}")
        
    except Exception as e:
        self.notify(f"Error starting arbitrage strategy: {str(e)}")
        self.logger().error("Failed to start arbitrage strategy", exc_info=True)
