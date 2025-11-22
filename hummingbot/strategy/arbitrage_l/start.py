from typing import List, Tuple
from decimal import Decimal

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.arbitrage_l.arbitrage_market_pair import ArbitrageLMarketPair
from hummingbot.strategy.arbitrage_l.arbitrage import ArbitrageLStrategy
from hummingbot.strategy.arbitrage_l.arbitrage_config_map import arbitrage_l_config_map


def _parse_additional_markets(raw) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if not raw:
        return items
    # Treat placeholder values as empty/skip
    try:
        s = str(raw).strip()
    except Exception:
        s = ""
    if s in {"-", ""}:
        return items
    if isinstance(raw, list):
        parts = [str(p).strip() for p in raw if str(p).strip() and str(p).strip() != "-"]
    else:
        parts = [p.strip() for p in str(raw).split(",") if p.strip() and p.strip() != "-"]
    for part in parts:
        if ":" not in part:
            continue
        conn, pair = part.split(":", 1)
        conn = conn.strip().lower()
        pair = pair.strip()
        if conn and pair and "-" in pair:
            items.append((conn, pair))
    return items


async def start(self):
    """Initialize and start the arbitrage_l strategy"""
    try:
        # Extract configuration values
        primary_market = arbitrage_l_config_map.get("primary_market").value.lower()
        secondary_market = arbitrage_l_config_map.get("secondary_market").value.lower()
        raw_primary_trading_pair = arbitrage_l_config_map.get("primary_market_trading_pair").value
        raw_secondary_trading_pair = arbitrage_l_config_map.get("secondary_market_trading_pair").value
        min_profitability = arbitrage_l_config_map.get("min_profitability").value / Decimal("100")
        use_oracle_conversion_rate = arbitrage_l_config_map.get("use_oracle_conversion_rate").value
        secondary_to_primary_base_conversion_rate = arbitrage_l_config_map["secondary_to_primary_base_conversion_rate"].value
        secondary_to_primary_quote_conversion_rate = arbitrage_l_config_map["secondary_to_primary_quote_conversion_rate"].value
        # Position balancer - buy-in configuration
        buy_in_enabled = arbitrage_l_config_map.get("buy_in_enabled").value
        buy_in_target_usdt = arbitrage_l_config_map.get("buy_in_target_usdt").value
        buy_in_spread_pct = arbitrage_l_config_map.get("buy_in_spread_pct").value
        # Position balancer - sell-off configuration
        sell_off_enabled = arbitrage_l_config_map.get("sell_off_enabled").value
        sell_off_target_usd = arbitrage_l_config_map.get("sell_off_target_usd").value
        sell_off_spread_pct = arbitrage_l_config_map.get("sell_off_spread_pct").value
        # Position balancer - order management
        position_balancer_refresh_interval = arbitrage_l_config_map.get("position_balancer_refresh_interval").value

        # Fallbacks when importing older/partial configs (use arbitrage.pyx defaults)
        if buy_in_enabled is None:
            buy_in_enabled = True
        if buy_in_target_usdt is None:
            buy_in_target_usdt = Decimal("100")
        if buy_in_spread_pct is None:
            buy_in_spread_pct = 0.1
        if sell_off_enabled is None:
            sell_off_enabled = False
        if sell_off_target_usd is None:
            sell_off_target_usd = 3000.0
        if sell_off_spread_pct is None:
            sell_off_spread_pct = 0.1
        if position_balancer_refresh_interval is None:
            position_balancer_refresh_interval = 60.0
        # Filled order timeout (defaults to 3600 seconds = 1 hour)
        filled_order_timeout_val = arbitrage_l_config_map.get("filled_order_timeout").value
        if filled_order_timeout_val is None:
            filled_order_timeout = 3600.0
        else:
            filled_order_timeout = float(filled_order_timeout_val)
        raw_additional = arbitrage_l_config_map.get("additional_markets").value or ""
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
        await self.initialize_markets(market_names)

        # Create MarketTradingPairTuples for each connector/pair
        tuples: List[MarketTradingPairTuple] = []
        for conn, pair in all_conns_pairs:
            base, quote = pair.split("-")
            data = [self.connector_manager.connectors[conn], pair, base, quote]
            tuples.append(MarketTradingPairTuple(*data))
        self.market_trading_pair_tuples = tuples

        # Build all ordered pairs (i != j)
        market_pairs: List[ArbitrageLMarketPair] = []
        n = len(tuples)
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                market_pairs.append(ArbitrageLMarketPair(tuples[i], tuples[j]))

        # Initialize strategy
        self.strategy = ArbitrageLStrategy()
        self.strategy.init_params(
            market_pairs=market_pairs,
            min_profitability=min_profitability,
            logging_options=(ArbitrageLStrategy.OPTION_LOG_STATUS_REPORT |
                           ArbitrageLStrategy.OPTION_LOG_ORDER_COMPLETED |
                           ArbitrageLStrategy.OPTION_LOG_CREATE_ORDER),
            filled_order_timeout=filled_order_timeout,
            use_oracle_conversion_rate=use_oracle_conversion_rate,
            secondary_to_primary_base_conversion_rate=secondary_to_primary_base_conversion_rate,
            secondary_to_primary_quote_conversion_rate=secondary_to_primary_quote_conversion_rate,
            hb_app_notification=True,
            # Position balancer - buy-in configuration
            buy_in_enabled=bool(buy_in_enabled),
            buy_in_target_usd=float(buy_in_target_usdt),
            buy_in_spread_pct=buy_in_spread_pct,  # Pass as-is (can be 'min' string or numeric)
            # Position balancer - sell-off configuration
            sell_off_enabled=bool(sell_off_enabled),
            sell_off_target_usd=float(sell_off_target_usd),
            sell_off_spread_pct=sell_off_spread_pct,  # Pass as-is (can be 'min' string or numeric)
            # Position balancer - order management
            position_balancer_refresh_interval=float(position_balancer_refresh_interval)
        )
        
        self.logger().info(f"arbitrage_l started with {len(tuples)} markets and {len(market_pairs)} ordered pairs")
        
    except Exception as e:
        self.notify(f"Error starting arbitrage_l strategy: {str(e)}")
        self.logger().error("Failed to start arbitrage_l strategy", exc_info=True)