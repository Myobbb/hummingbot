from typing import List, Tuple
from decimal import Decimal

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage_m.arbitrage_market_pair import ArbitrageMMarketPair
from hummingbot.strategy.arbitrage_m.arbitrage import ArbitrageMStrategy
from hummingbot.strategy.arbitrage_m.arbitrage_config_map import arbitrage_m_config_map


def _parse_additional_markets(raw: str) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if not raw:
        return items
    for part in [p.strip() for p in raw.split(",") if p.strip()]:
        if ":" not in part:
            continue
        conn, pair = part.split(":", 1)
        conn = conn.strip().lower()
        pair = pair.strip()
        if conn and pair and "-" in pair:
            items.append((conn, pair))
    return items


def start(self):
    """Initialize and start the arbitrage_m strategy"""
    try:
        # Extract configuration values
        primary_market = arbitrage_m_config_map.get("primary_market").value.lower()
        secondary_market = arbitrage_m_config_map.get("secondary_market").value.lower()
        raw_primary_trading_pair = arbitrage_m_config_map.get("primary_market_trading_pair").value
        raw_secondary_trading_pair = arbitrage_m_config_map.get("secondary_market_trading_pair").value
        min_profitability = arbitrage_m_config_map.get("min_profitability").value / Decimal("100")
        use_oracle_conversion_rate = arbitrage_m_config_map.get("use_oracle_conversion_rate").value
        secondary_to_primary_base_conversion_rate = arbitrage_m_config_map["secondary_to_primary_base_conversion_rate"].value
        secondary_to_primary_quote_conversion_rate = arbitrage_m_config_map["secondary_to_primary_quote_conversion_rate"].value
        raw_additional = arbitrage_m_config_map.get("additional_markets").value or ""
        additional = _parse_additional_markets(raw_additional)
        
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

        # Build all unique connectors/pairs
        all_conns_pairs: List[Tuple[str, str]] = [
            (primary_market, primary_trading_pair),
            (secondary_market, secondary_trading_pair),
        ]
        # Add extra markets if any (ignore duplicates)
        seen = set(all_conns_pairs)
        for conn_pair in additional:
            if conn_pair not in seen:
                all_conns_pairs.append(conn_pair)
                seen.add(conn_pair)

        # Initialize markets (aggregate pairs per connector)
        conn_to_pairs = {}
        for conn, pair in all_conns_pairs:
            if conn not in conn_to_pairs:
                conn_to_pairs[conn] = []
            if pair not in conn_to_pairs[conn]:
                conn_to_pairs[conn].append(pair)
        market_names: List[Tuple[str, List[str]]] = [(conn, pairs) for conn, pairs in conn_to_pairs.items()]
        self.initialize_markets(market_names)

        # Create MarketTradingPairTuples for each connector/pair
        tuples: List[MarketTradingPairTuple] = []
        for conn, pair in all_conns_pairs:
            base, quote = pair.split("-")
            data = [self.markets[conn], pair, base, quote]
            tuples.append(MarketTradingPairTuple(*data))
        self.market_trading_pair_tuples = tuples

        # Build all ordered pairs (i != j)
        market_pairs: List[ArbitrageMMarketPair] = []
        n = len(tuples)
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                market_pairs.append(ArbitrageMMarketPair(tuples[i], tuples[j]))

        # Initialize strategy
        self.strategy = ArbitrageMStrategy()
        self.strategy.init_params(
            market_pairs=market_pairs,
            min_profitability=min_profitability,
            logging_options=(ArbitrageMStrategy.OPTION_LOG_STATUS_REPORT |
                           ArbitrageMStrategy.OPTION_LOG_ORDER_COMPLETED |
                           ArbitrageMStrategy.OPTION_LOG_CREATE_ORDER),
            use_oracle_conversion_rate=use_oracle_conversion_rate,
            secondary_to_primary_base_conversion_rate=secondary_to_primary_base_conversion_rate,
            secondary_to_primary_quote_conversion_rate=secondary_to_primary_quote_conversion_rate,
            hb_app_notification=True
        )
        
        self.logger().info(f"arbitrage_m started with {len(tuples)} markets and {len(market_pairs)} ordered pairs")
        
    except Exception as e:
        self.notify(f"Error starting arbitrage_m strategy: {str(e)}")
        self.logger().error("Failed to start arbitrage_m strategy", exc_info=True)
