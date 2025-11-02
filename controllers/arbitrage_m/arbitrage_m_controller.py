"""
Arbitrage M V2 Controller

Migrated from V1 Cython strategy (hummingbot/strategy/arbitrage_m/arbitrage.pyx)
to V2 framework while preserving performance-critical Cython logic.

Key differences from V1:
- Uses V2 ControllerBase instead of StrategyBase
- Returns ExecutorActions instead of placing orders directly
- Leverages V2 ArbitrageExecutor for order execution
- Supports live configuration updates
- Simplified without buy-in module (can be added later)
- Supports 2 market pairs (expandable to N pairs later)
"""

import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pydantic import Field, field_validator

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import MarketDict
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction

# Import Cython helpers for performance
try:
    from controllers.arbitrage_m.arbitrage_m_helpers import ArbitrageMHelpers
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    logging.warning("Arbitrage M Cython helpers not available. Performance will be degraded.")


class ArbitrageMConfig(ControllerConfigBase):
    """
    Configuration for Arbitrage M V2 Controller.

    Migrated from V1 arbitrage_config_map.py with simplifications:
    - No buy-in module (for now)
    - No additional_markets (for now, but designed for expansion)
    - Supports primary + secondary market pairs
    """
    controller_name: str = "arbitrage_m"
    controller_type: str = "arbitrage"
    candles_config: List[CandlesConfig] = []

    # Market configuration
    primary_market: ConnectorPair = Field(
        default=ConnectorPair(connector_name="binance", trading_pair="BTC-USDT"),
        json_schema_extra={
            "prompt": lambda mi: "Enter primary market (connector:pair, e.g., binance:BTC-USDT): ",
            "prompt_on_new": True
        }
    )

    secondary_market: ConnectorPair = Field(
        default=ConnectorPair(connector_name="kucoin", trading_pair="BTC-USDT"),
        json_schema_extra={
            "prompt": lambda mi: "Enter secondary market (connector:pair, e.g., kucoin:BTC-USDT): ",
            "prompt_on_new": True
        }
    )

    # Trading parameters
    min_profitability: Decimal = Field(
        default=Decimal("0.019"),  # 1.9% default from V1
        json_schema_extra={
            "prompt": lambda mi: "What is the minimum profitability for you to make a trade? (Enter 1.9 to indicate 1.9%): ",
            "prompt_on_new": True,
            "is_updatable": True
        }
    )

    order_amount: Decimal = Field(
        default=Decimal("100"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the order amount in quote currency (e.g., 100 USDT): ",
            "prompt_on_new": True,
            "is_updatable": True
        }
    )

    # Conversion settings
    use_oracle_conversion_rate: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Do you want to use rate oracle for unmatched trading pairs? (Yes/No): ",
            "prompt_on_new": False
        }
    )

    secondary_to_primary_base_conversion_rate: Decimal = Field(
        default=Decimal("1"),
        json_schema_extra={
            "prompt": lambda mi: "Enter conversion rate for secondary base asset to primary base asset (e.g., 1.0): ",
            "prompt_on_new": False
        }
    )

    secondary_to_primary_quote_conversion_rate: Decimal = Field(
        default=Decimal("1"),
        json_schema_extra={
            "prompt": lambda mi: "Enter conversion rate for secondary quote asset to primary quote asset (e.g., 1.0): ",
            "prompt_on_new": False
        }
    )

    # Timing parameters
    next_trade_delay_interval: float = Field(
        default=2.0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the delay between trades in seconds (e.g., 2.0): ",
            "prompt_on_new": False,
            "is_updatable": True
        }
    )

    order_timeout: float = Field(
        default=300.0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the order timeout in seconds (e.g., 300): ",
            "prompt_on_new": False
        }
    )

    # Thresholds
    min_order_usd: Decimal = Field(
        default=Decimal("15"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the minimum order size in USD (e.g., 15): ",
            "prompt_on_new": False,
            "is_updatable": True
        }
    )

    # Performance settings
    max_concurrent_arbitrages: int = Field(
        default=1,
        json_schema_extra={
            "prompt": lambda mi: "Enter the maximum number of concurrent arbitrage operations (e.g., 1): ",
            "prompt_on_new": False,
            "is_updatable": True
        }
    )

    @field_validator('primary_market', 'secondary_market', mode='before')
    @classmethod
    def parse_connector_pair(cls, v):
        """Parse connector:pair format to ConnectorPair"""
        if isinstance(v, str):
            if ':' in v:
                connector, pair = v.split(':', 1)
                return ConnectorPair(connector_name=connector.strip(), trading_pair=pair.strip())
            # Assume it's just a connector name, use default pair
            return ConnectorPair(connector_name=v.strip(), trading_pair="BTC-USDT")
        return v

    def update_markets(self, markets: MarketDict) -> MarketDict:
        """Update markets dict from config"""
        markets.add_or_update(self.primary_market.connector_name, self.primary_market.trading_pair)
        markets.add_or_update(self.secondary_market.connector_name, self.secondary_market.trading_pair)
        return markets


class ArbitrageMController(ControllerBase):
    """
    Arbitrage M V2 Controller

    Scans primary and secondary markets for arbitrage opportunities.
    Uses Cython helpers for performance-critical calculations.
    Leverages V2 ArbitrageExecutor for order execution.

    V1 mapping:
    - c_tick() → determine_executor_actions()
    - c_execute_arbitrage() → CreateExecutorAction(ArbitrageExecutorConfig)
    - c_find_best_profitable_amount() → Cython helper
    - c_calculate_profitability() → Cython helper
    """

    def __init__(self, config: ArbitrageMConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config

        # Initialize Cython helpers for performance
        if CYTHON_AVAILABLE:
            self._helpers = ArbitrageMHelpers(
                min_profitability=float(config.min_profitability) / 100.0,  # Convert % to decimal
                min_order_usd=float(config.min_order_usd)
            )
        else:
            self._helpers = None
            self.logger().warning("Cython helpers not available. Using slower Python fallback.")

        # State tracking (from V1)
        self._last_trade_timestamp = 0.0
        self._cached_base_rate = 1.0
        self._cached_quote_rate = 1.0
        self._last_rate_update = 0

        # Market pair tuples (for Cython compatibility)
        self._primary_tuple: Optional[MarketTradingPairTuple] = None
        self._secondary_tuple: Optional[MarketTradingPairTuple] = None

    def start(self):
        """Initialize controller and create market tuples"""
        super().start()

        # Create MarketTradingPairTuple objects for Cython helpers
        # These will be set when markets are available
        self._create_market_tuples()

    def _create_market_tuples(self):
        """Create MarketTradingPairTuple objects from config"""
        try:
            # Get connectors from market data provider
            primary_connector = self.market_data_provider.get_connector(
                self.config.primary_market.connector_name
            )
            secondary_connector = self.market_data_provider.get_connector(
                self.config.secondary_market.connector_name
            )

            if primary_connector and secondary_connector:
                # Create tuples compatible with Cython helpers
                self._primary_tuple = MarketTradingPairTuple(
                    primary_connector,
                    self.config.primary_market.trading_pair,
                    *self.config.primary_market.trading_pair.split("-")
                )
                self._secondary_tuple = MarketTradingPairTuple(
                    secondary_connector,
                    self.config.secondary_market.trading_pair,
                    *self.config.secondary_market.trading_pair.split("-")
                )
                self.logger().info("Market tuples created successfully")
        except Exception as e:
            self.logger().error(f"Error creating market tuples: {e}")

    async def update_processed_data(self):
        """
        Update conversion rates and market data.
        V1 equivalent: c_update_conversion_rates() (arbitrage.pyx:273)
        """
        current_time = self.market_data_provider.time()

        # Create market tuples if not yet done
        if self._primary_tuple is None or self._secondary_tuple is None:
            self._create_market_tuples()

        # Update conversion rates if using oracle
        if self.config.use_oracle_conversion_rate:
            if current_time - self._last_rate_update > 10.0:  # 10s cache
                self._update_conversion_rates()
                self._last_rate_update = current_time
        else:
            # Use fixed rates from config
            self._cached_base_rate = float(self.config.secondary_to_primary_base_conversion_rate)
            self._cached_quote_rate = float(self.config.secondary_to_primary_quote_conversion_rate)

    def _update_conversion_rates(self):
        """
        Update conversion rates from oracle.
        V1 equivalent: c_update_conversion_rates() (arbitrage.pyx:273-297)
        """
        try:
            primary_base, primary_quote = self.config.primary_market.trading_pair.split("-")
            secondary_base, secondary_quote = self.config.secondary_market.trading_pair.split("-")

            # Base asset conversion
            if primary_base != secondary_base:
                base_pair = f"{secondary_base}-{primary_base}"
                self._cached_base_rate = float(RateOracle.get_instance().get_pair_rate(base_pair))
            else:
                self._cached_base_rate = 1.0

            # Quote asset conversion
            if primary_quote != secondary_quote:
                quote_pair = f"{secondary_quote}-{primary_quote}"
                self._cached_quote_rate = float(RateOracle.get_instance().get_pair_rate(quote_pair))
            else:
                self._cached_quote_rate = 1.0

            self.logger().debug(
                f"Conversion rates updated: base={self._cached_base_rate:.6f}, "
                f"quote={self._cached_quote_rate:.6f}"
            )
        except Exception as e:
            self.logger().error(f"Error updating conversion rates: {e}")
            # Fallback to 1:1
            self._cached_base_rate = 1.0
            self._cached_quote_rate = 1.0

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Main arbitrage logic - scan markets and create executors.
        V1 equivalent: c_tick() (arbitrage.pyx:443)

        Flow:
        1. Check trade delay cooldown
        2. Check concurrent arbitrage limit
        3. Scan both directions for profitability
        4. Select best direction
        5. Create ArbitrageExecutor via ExecutorAction
        """
        actions = []
        current_time = self.market_data_provider.time()

        # Check if market tuples are ready
        if self._primary_tuple is None or self._secondary_tuple is None:
            return actions

        # Global trade delay (V1: arbitrage.pyx:764)
        if current_time - self._last_trade_timestamp < self.config.next_trade_delay_interval:
            return actions

        # Check concurrent arbitrage limit
        active_arbs = len([e for e in self.executors_info if e.status != RunnableStatus.TERMINATED])
        if active_arbs >= self.config.max_concurrent_arbitrages:
            return actions

        # Calculate market-to-market conversion rate
        conv_rate = self._get_conversion_rate()

        # Scan both directions for profitability
        # Direction 1: Buy primary, sell secondary
        # Direction 2: Buy secondary, sell primary
        best_direction = None
        best_profitability = 0.0
        best_buy_market = None
        best_sell_market = None

        # Try direction 1: Buy primary, sell secondary
        prof1 = self._calculate_profitability_direction(
            self._primary_tuple,  # buy
            self._secondary_tuple,  # sell
            conv_rate
        )

        if prof1 > best_profitability and prof1 >= float(self.config.min_profitability) / 100.0:
            best_profitability = prof1
            best_direction = 1
            best_buy_market = self._primary_tuple
            best_sell_market = self._secondary_tuple

        # Try direction 2: Buy secondary, sell primary
        prof2 = self._calculate_profitability_direction(
            self._secondary_tuple,  # buy
            self._primary_tuple,  # sell
            1.0 / conv_rate if conv_rate != 0 else 1.0
        )

        if prof2 > best_profitability and prof2 >= float(self.config.min_profitability) / 100.0:
            best_profitability = prof2
            best_direction = 2
            best_buy_market = self._secondary_tuple
            best_sell_market = self._primary_tuple

        # Create arbitrage executor for best direction
        if best_buy_market and best_sell_market:
            try:
                # Calculate order amount based on available balance
                order_amount = self._calculate_order_amount(
                    best_buy_market,
                    best_sell_market,
                    conv_rate if best_direction == 1 else (1.0 / conv_rate if conv_rate != 0 else 1.0)
                )

                if order_amount > 0:
                    # Create ArbitrageExecutorConfig
                    arb_config = ArbitrageExecutorConfig(
                        timestamp=current_time,
                        buying_market=ConnectorPair(
                            connector_name=best_buy_market.market.name,
                            trading_pair=best_buy_market.trading_pair
                        ),
                        selling_market=ConnectorPair(
                            connector_name=best_sell_market.market.name,
                            trading_pair=best_sell_market.trading_pair
                        ),
                        order_amount=Decimal(str(order_amount)),
                        min_profitability=self.config.min_profitability / 100,  # Convert % to decimal
                        controller_id=self.config.id
                    )

                    actions.append(CreateExecutorAction(
                        executor_config=arb_config,
                        controller_id=self.config.id
                    ))

                    self._last_trade_timestamp = current_time

                    self.logger().info(
                        f"Arbitrage opportunity: Buy {best_buy_market.market.name}:{best_buy_market.trading_pair} "
                        f"→ Sell {best_sell_market.market.name}:{best_sell_market.trading_pair} | "
                        f"Profit: {best_profitability*100:.3f}% | Amount: {order_amount:.6f}"
                    )
            except Exception as e:
                self.logger().error(f"Error creating arbitrage executor: {e}", exc_info=True)

        return actions

    def _calculate_profitability_direction(
        self,
        buy_market_tuple: MarketTradingPairTuple,
        sell_market_tuple: MarketTradingPairTuple,
        conversion_rate: float
    ) -> float:
        """
        Calculate profitability for a specific direction.
        Uses Cython helpers if available, otherwise Python fallback.
        """
        if self._helpers and CYTHON_AVAILABLE:
            # Use fast Cython implementation
            prof_tuple = self._helpers.calculate_profitability(
                buy_market_tuple,
                sell_market_tuple,
                conversion_rate
            )
            # prof_tuple = (prof_direction1, prof_direction2)
            # We want buy->sell profitability which is prof_direction2
            return prof_tuple[1] if len(prof_tuple) > 1 else 0.0
        else:
            # Python fallback
            return self._calculate_profitability_python(
                buy_market_tuple,
                sell_market_tuple,
                conversion_rate
            )

    def _calculate_profitability_python(
        self,
        buy_market_tuple: MarketTradingPairTuple,
        sell_market_tuple: MarketTradingPairTuple,
        conversion_rate: float
    ) -> float:
        """Python fallback for profitability calculation"""
        try:
            bid = float(sell_market_tuple.get_price(False))
            ask = float(buy_market_tuple.get_price(True))

            if bid <= 0 or ask <= 0:
                return 0.0

            bid_adj = bid * conversion_rate
            profitability = (bid_adj / ask - 1.0) if ask > 0 else 0.0

            return profitability
        except Exception as e:
            self.logger().error(f"Error in profitability calculation: {e}")
            return 0.0

    def _calculate_order_amount(
        self,
        buy_market_tuple: MarketTradingPairTuple,
        sell_market_tuple: MarketTradingPairTuple,
        conversion_rate: float
    ) -> float:
        """
        Calculate optimal order amount based on balances and order books.
        Uses Cython helpers if available for order book scanning.
        """
        try:
            # Get available balances
            buy_quote_balance = float(buy_market_tuple.market.get_available_balance(
                buy_market_tuple.quote_asset
            ))
            sell_base_balance = float(sell_market_tuple.market.get_available_balance(
                sell_market_tuple.base_asset
            ))

            # Use configured order amount as maximum
            max_order_quote = float(self.config.order_amount)

            # Limit by available balances
            if self._helpers and CYTHON_AVAILABLE:
                # Use Cython helper for precise calculation
                result = self._helpers.find_best_profitable_amount(
                    buy_market_tuple,
                    sell_market_tuple,
                    min(buy_quote_balance, max_order_quote),
                    sell_base_balance,
                    conversion_rate
                )
                # result = (amount, profitability, sell_price, buy_price)
                return result[0] if result[0] > 0 else 0.0
            else:
                # Simple fallback: use configured amount limited by balances
                approx_price = float(buy_market_tuple.get_price(True))
                if approx_price <= 0:
                    return 0.0

                max_base_from_quote = min(buy_quote_balance, max_order_quote) / approx_price
                return min(max_base_from_quote, sell_base_balance)

        except Exception as e:
            self.logger().error(f"Error calculating order amount: {e}")
            return 0.0

    def _get_conversion_rate(self) -> float:
        """
        Get conversion rate for sell→buy market.
        V1 equivalent: c_get_market_to_market_conversion_rate() (arbitrage.pyx:231)
        """
        primary_base, primary_quote = self.config.primary_market.trading_pair.split("-")
        secondary_base, secondary_quote = self.config.secondary_market.trading_pair.split("-")

        # Fast path: same assets
        if primary_base == secondary_base and primary_quote == secondary_quote:
            return 1.0

        # Use cached rates
        base_conv = self._cached_base_rate
        quote_conv = self._cached_quote_rate

        # Conversion formula: quote_conv / base_conv
        return quote_conv / base_conv if base_conv != 0 else 1.0

    def to_format_status(self) -> List[str]:
        """
        Format status for display.
        V1 equivalent: format_status() (arbitrage.pyx:338)
        """
        lines = []

        try:
            lines.append("\n" + "="*80)
            lines.append(f"  Arbitrage M Controller: {self.config.id}")
            lines.append("="*80)

            # Market configuration
            lines.append("\n  Markets:")
            lines.append(f"    Primary:   {self.config.primary_market.connector_name}:{self.config.primary_market.trading_pair}")
            lines.append(f"    Secondary: {self.config.secondary_market.connector_name}:{self.config.secondary_market.trading_pair}")

            # Trading parameters
            lines.append("\n  Parameters:")
            lines.append(f"    Min Profitability:     {self.config.min_profitability}%")
            lines.append(f"    Order Amount:          {self.config.order_amount}")
            lines.append(f"    Min Order (USD):       ${self.config.min_order_usd}")
            lines.append(f"    Max Concurrent Arbs:   {self.config.max_concurrent_arbitrages}")

            # Conversion rates (if not 1:1)
            if abs(self._cached_base_rate - 1.0) > 0.001 or abs(self._cached_quote_rate - 1.0) > 0.001:
                lines.append("\n  Conversion Rates:")
                lines.append(f"    Base Rate:   {self._cached_base_rate:.6f}")
                lines.append(f"    Quote Rate:  {self._cached_quote_rate:.6f}")

            # Current profitability snapshot
            if self._primary_tuple and self._secondary_tuple:
                lines.append("\n  Profitability Snapshot (without fees):")

                conv_rate = self._get_conversion_rate()

                # Direction 1: Buy primary, sell secondary
                prof1 = self._calculate_profitability_direction(
                    self._primary_tuple,
                    self._secondary_tuple,
                    conv_rate
                )
                lines.append(
                    f"    Buy-{self.config.primary_market.connector_name} "
                    f"Sell-{self.config.secondary_market.connector_name}: "
                    f"{prof1*100:+.4f}%"
                )

                # Direction 2: Buy secondary, sell primary
                prof2 = self._calculate_profitability_direction(
                    self._secondary_tuple,
                    self._primary_tuple,
                    1.0 / conv_rate if conv_rate != 0 else 1.0
                )
                lines.append(
                    f"    Buy-{self.config.secondary_market.connector_name} "
                    f"Sell-{self.config.primary_market.connector_name}: "
                    f"{prof2*100:+.4f}%"
                )

                # Highlight best
                if prof1 >= prof2:
                    lines.append(f"    Best: Direction 1 → {prof1*100:+.4f}%")
                else:
                    lines.append(f"    Best: Direction 2 → {prof2*100:+.4f}%")

            # Active executors
            active_count = len([e for e in self.executors_info if e.status != RunnableStatus.TERMINATED])
            completed_count = len([e for e in self.executors_info if e.status == RunnableStatus.TERMINATED])

            lines.append("\n  Executors:")
            lines.append(f"    Active:    {active_count}")
            lines.append(f"    Completed: {completed_count}")

            # Executor details if any
            if self.executors_info:
                executor_data = []
                for executor in self.executors_info:
                    executor_data.append({
                        "ID": executor.id[:8] + "...",
                        "Status": executor.status.name,
                        "Type": executor.type,
                        "Created": executor.timestamp
                    })

                if executor_data:
                    df = pd.DataFrame(executor_data)
                    lines.append("\n  Executor Details:")
                    lines.extend(["    " + line for line in format_df_for_printout(df, table_format="psql").split("\n")])

            lines.append("="*80)

        except Exception as e:
            lines.append(f"\n  Error formatting status: {e}")
            self.logger().error(f"Error in to_format_status: {e}", exc_info=True)

        return lines
