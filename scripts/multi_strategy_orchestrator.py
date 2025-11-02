"""
Multi-Strategy Orchestrator for V1 Strategies with Shared Websocket Connections

This script allows running multiple V1 strategies (like arbitrage_m) simultaneously
while sharing websocket connections to the same exchanges. This provides:

1. Resource Efficiency: One websocket connection per exchange instead of one per strategy
2. Rate Limit Optimization: All strategies share the same connection pool
3. Memory Savings: Order books and market data cached once and shared
4. Compatibility: Works with existing V1 Cython strategies without modification

Architecture:
-----------
MultiStrategyOrchestrator (ScriptStrategyBase)
├── connectors: Dict[str, ConnectorBase]     ← Shared connector pool
│   └── Each connector manages ONE websocket connection
├── strategies: List[V1StrategyInstance]     ← Multiple V1 strategy instances
│   ├── arbitrage_m instance 1
│   ├── arbitrage_m instance 2
│   └── ... (each adds event listeners to shared connectors)
└── on_tick() → tick all strategies independently

How Websocket Sharing Works:
---------------------------
1. ConnectorManager creates ONE connector instance per exchange
2. Multiple V1 strategies call c_add_markets() on the SAME connector
3. Each strategy registers its own event listeners
4. Connector broadcasts events to all registered listeners
5. Strategies operate independently, sharing the underlying connection

Critical Implementation Details:
-------------------------------
FIXED - Lifecycle Management:
- The orchestrator bypasses ScriptStrategyBase.__init__() to avoid double event listener registration
- Strategies are initialized in __init__() but started later in start() when clock is available
- Each strategy gets proper c_start(clock, timestamp) call with clock reference
- Strategies are stopped with c_stop(clock) using the SAME clock from start()

Event Listener Pattern:
- Orchestrator itself does NOT register event listeners (no add_markets call)
- Only V1 strategies register listeners via c_add_markets() during init_params()
- Multiple strategies can safely share connectors via observer pattern

Clock Management:
- Orchestrator is registered with clock (only orchestrator, not individual strategies)
- Orchestrator's start() is called by clock → starts all V1 strategies
- Orchestrator's on_tick() is called by clock → manually ticks all V1 strategies
- Orchestrator's on_stop() is called by clock → stops all V1 strategies

Example Usage:
-------------
See scripts/examples/conf_multi_arbitrage_m_*.yml for configurations

"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy.arbitrage_m.arbitrage import ArbitrageMStrategy
from hummingbot.strategy.arbitrage_m.arbitrage_market_pair import ArbitrageMMarketPair
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.script_strategy_base import ScriptConfigBase, ScriptStrategyBase
from hummingbot.strategy.strategy_base import StrategyBase


logger = None


@dataclass
class V1StrategyInstance:
    """Wrapper for a V1 strategy instance with metadata"""
    strategy: StrategyBase
    name: str
    config: Dict
    market_pairs: List[MarketTradingPairTuple]


class ArbitrageMInstanceConfig(BaseModel):
    """Configuration for a single arbitrage_m strategy instance"""
    name: str = Field(..., description="Unique name for this strategy instance")
    primary_market: str = Field(..., description="Primary exchange name (e.g., 'binance')")
    secondary_market: str = Field(..., description="Secondary exchange name (e.g., 'kucoin')")
    primary_trading_pair: str = Field(..., description="Primary trading pair (e.g., 'BTC-USDT')")
    secondary_trading_pair: str = Field(..., description="Secondary trading pair (e.g., 'BTC-USDT')")
    min_profitability: Decimal = Field(default=Decimal("0.5"), description="Minimum profitability percentage")

    # Advanced options
    use_oracle_conversion_rate: bool = Field(default=False)
    secondary_to_primary_base_conversion_rate: Decimal = Field(default=Decimal("1.0"))
    secondary_to_primary_quote_conversion_rate: Decimal = Field(default=Decimal("1.0"))

    # Buy-in configuration
    buy_in_enabled: bool = Field(default=False, description="Enable buy-in module")
    buy_in_target_usd: float = Field(default=100.0, description="Target USD value for buy-in")
    buy_in_min_profitability: float = Field(default=0.005, description="Min profitability for buy-in (0.5%)")

    # Timing
    status_report_interval: float = Field(default=60.0)
    next_trade_delay_interval: float = Field(default=2.0)
    order_timeout: float = Field(default=300.0)

    # Additional markets for cross-exchange opportunities
    additional_markets: List[str] = Field(default_factory=list, description="Additional markets as 'exchange:PAIR' (e.g., ['mexc:BTC-USDT'])")


class MultiStrategyOrchestratorConfig(ScriptConfigBase):
    """Configuration for multi-strategy orchestrator"""
    script_file_name: str = "multi_strategy_orchestrator.py"

    # All markets needed across all strategies
    markets: MarketDict = Field(
        default_factory=dict,
        description="All markets needed: {'binance': {'BTC-USDT', 'ETH-USDT'}, 'kucoin': {'BTC-USDT'}}"
    )

    # Strategy instances to run
    arbitrage_m_strategies: List[ArbitrageMInstanceConfig] = Field(
        default_factory=list,
        description="List of arbitrage_m strategy configurations to run"
    )


class MultiStrategyOrchestrator(ScriptStrategyBase):
    """
    Orchestrates multiple V1 strategies with shared websocket connections.

    This orchestrator creates a single pool of exchange connectors and allows
    multiple V1 strategy instances to share them. Each strategy operates
    independently but uses the same websocket connections for market data
    and order management.

    Key Implementation Details:
    --------------------------
    1. Connector Sharing:
       - ScriptStrategyBase.__init__() receives connectors dict from TradingCore
       - These connectors are already initialized with websocket connections
       - We pass the SAME connector references to all V1 strategies

    2. Event Listener Registration:
       - Each V1 strategy calls c_add_markets() in StrategyBase
       - This adds the strategy's event listeners to the connector
       - Connectors support multiple listeners per event (observer pattern)
       - When events fire, all registered strategies receive notifications

    3. Independent Operation:
       - Each strategy maintains its own state and logic
       - Strategies don't interfere with each other
       - Each has its own order tracking and profitability calculations

    4. Lifecycle Management:
       - on_tick(): Tick all strategies each cycle
       - on_stop(): Clean shutdown of all strategies
    """

    @classmethod
    def logger(cls):
        global logger
        if logger is None:
            logger = logging.getLogger(__name__)
        return logger

    @classmethod
    def init_markets(cls, config: MultiStrategyOrchestratorConfig):
        """Initialize markets from config"""
        cls.markets = config.markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: MultiStrategyOrchestratorConfig):
        """
        Initialize the multi-strategy orchestrator.

        Args:
            connectors: Shared connector pool from TradingCore
                       These connectors are SHARED across all strategies
            config: Orchestrator configuration
        """
        # FIX #1: Initialize base WITHOUT calling add_markets()
        # The orchestrator itself doesn't need event listeners - only the V1 strategies do
        # We manually set the required attributes instead of calling super().__init__()
        from hummingbot.strategy.strategy_py_base import StrategyPyBase
        StrategyPyBase.__init__(self)  # Initialize StrategyBase/TimeIterator

        self.connectors: Dict[str, ConnectorBase] = connectors
        self.config: MultiStrategyOrchestratorConfig = config
        self.ready_to_trade: bool = False

        # Storage for V1 strategy instances
        self.strategies: List[V1StrategyInstance] = []
        self._strategies_started: bool = False  # FIX #2: Track whether strategies have been started
        self._strategy_clock = None  # FIX #3: Store clock reference for strategies

        # Initialize all configured strategies (but don't start them yet - no clock available)
        self._initialize_arbitrage_m_strategies()

        self.logger().info(f"MultiStrategyOrchestrator initialized with {len(self.strategies)} strategies")
        self.logger().info(f"Shared connectors: {list(self.connectors.keys())}")

    def _initialize_arbitrage_m_strategies(self):
        """Initialize all arbitrage_m strategy instances"""
        for strategy_config in self.config.arbitrage_m_strategies:
            try:
                self._add_arbitrage_m_strategy(strategy_config)
            except Exception as e:
                self.logger().error(f"Failed to initialize strategy '{strategy_config.name}': {e}", exc_info=True)

    def _add_arbitrage_m_strategy(self, config: ArbitrageMInstanceConfig):
        """
        Add an arbitrage_m strategy instance.

        This method:
        1. Builds market pairs from the shared connector pool
        2. Creates an ArbitrageMStrategy instance
        3. Initializes it with the config
        4. The strategy's c_add_markets() call registers event listeners
        """
        self.logger().info(f"Adding arbitrage_m strategy: {config.name}")

        # Validate connectors exist
        if config.primary_market not in self.connectors:
            raise ValueError(f"Primary market '{config.primary_market}' not in connector pool")
        if config.secondary_market not in self.connectors:
            raise ValueError(f"Secondary market '{config.secondary_market}' not in connector pool")

        # Build market tuples from shared connectors
        market_tuples = []

        # Primary market tuple
        primary_base, primary_quote = config.primary_trading_pair.split("-")
        primary_tuple = MarketTradingPairTuple(
            market=self.connectors[config.primary_market],  # ← SHARED connector
            trading_pair=config.primary_trading_pair,
            base_asset=primary_base,
            quote_asset=primary_quote
        )
        market_tuples.append(primary_tuple)

        # Secondary market tuple
        secondary_base, secondary_quote = config.secondary_trading_pair.split("-")
        secondary_tuple = MarketTradingPairTuple(
            market=self.connectors[config.secondary_market],  # ← SHARED connector
            trading_pair=config.secondary_trading_pair,
            base_asset=secondary_base,
            quote_asset=secondary_quote
        )
        market_tuples.append(secondary_tuple)

        # Additional markets if specified
        for additional in config.additional_markets:
            if ":" not in additional:
                self.logger().warning(f"Invalid additional market format '{additional}', skipping")
                continue

            exchange, pair = additional.split(":", 1)
            exchange = exchange.lower()

            if exchange not in self.connectors:
                self.logger().warning(f"Additional market '{exchange}' not in connector pool, skipping")
                continue

            if "-" not in pair:
                self.logger().warning(f"Invalid trading pair format '{pair}', skipping")
                continue

            base, quote = pair.split("-")
            additional_tuple = MarketTradingPairTuple(
                market=self.connectors[exchange],  # ← SHARED connector
                trading_pair=pair,
                base_asset=base,
                quote_asset=quote
            )
            market_tuples.append(additional_tuple)

        # Build all arbitrage pairs (all permutations where i != j)
        market_pairs = []
        for i in range(len(market_tuples)):
            for j in range(len(market_tuples)):
                if i != j:
                    market_pairs.append(ArbitrageMMarketPair(
                        first=market_tuples[i],
                        second=market_tuples[j]
                    ))

        # Create strategy instance
        strategy = ArbitrageMStrategy()

        # Initialize strategy parameters
        # This will call c_add_markets() which registers event listeners on shared connectors
        strategy.init_params(
            market_pairs=market_pairs,
            min_profitability=config.min_profitability / Decimal("100"),  # Convert percentage to decimal
            logging_options=(
                ArbitrageMStrategy.OPTION_LOG_STATUS_REPORT |
                ArbitrageMStrategy.OPTION_LOG_ORDER_COMPLETED |
                ArbitrageMStrategy.OPTION_LOG_CREATE_ORDER
            ),
            status_report_interval=config.status_report_interval,
            next_trade_delay_interval=config.next_trade_delay_interval,
            order_timeout=config.order_timeout,
            use_oracle_conversion_rate=config.use_oracle_conversion_rate,
            secondary_to_primary_base_conversion_rate=config.secondary_to_primary_base_conversion_rate,
            secondary_to_primary_quote_conversion_rate=config.secondary_to_primary_quote_conversion_rate,
            hb_app_notification=True,
            buy_in_enabled=config.buy_in_enabled,
            buy_in_target_usd=config.buy_in_target_usd,
            buy_in_min_profitability=config.buy_in_min_profitability,
        )

        # Store strategy instance
        strategy_instance = V1StrategyInstance(
            strategy=strategy,
            name=config.name,
            config=config.dict(),
            market_pairs=market_tuples
        )
        self.strategies.append(strategy_instance)

        self.logger().info(
            f"Strategy '{config.name}' initialized: "
            f"{config.primary_market}/{config.primary_trading_pair} <-> "
            f"{config.secondary_market}/{config.secondary_trading_pair}, "
            f"{len(market_pairs)} arbitrage pairs, "
            f"min_profit={config.min_profitability}%"
        )

    def start(self, clock, timestamp: float):
        """
        FIX #2 & #4: Start the orchestrator and all V1 strategies with proper clock management.

        This is called by the Clock system after the orchestrator is registered.
        We use this opportunity to start all V1 strategies with the clock.

        Args:
            clock: The clock instance managing this orchestrator
            timestamp: Current timestamp
        """
        # Store clock reference for lifecycle management
        self._strategy_clock = clock
        self._last_timestamp = timestamp

        # Start all V1 strategies with the clock
        if not self._strategies_started:
            self.logger().info(f"Starting {len(self.strategies)} V1 strategies with clock...")

            for strategy_instance in self.strategies:
                try:
                    self.logger().info(f"Starting strategy: {strategy_instance.name}")
                    # Call c_start() which will:
                    # 1. Call StrategyBase.c_start() - initializes base state
                    # 2. Call strategy.start() - strategy-specific initialization
                    strategy_instance.strategy.c_start(clock, timestamp)
                except Exception as e:
                    self.logger().error(
                        f"Error starting strategy '{strategy_instance.name}': {e}",
                        exc_info=True
                    )

            self._strategies_started = True
            self.logger().info(f"All strategies started successfully")

    def on_tick(self):
        """
        Main tick function - tick all strategies.

        Each strategy's c_tick() is called independently. The strategies share
        the same connectors but maintain separate state and logic.
        """
        if not self.ready_to_trade:
            self.ready_to_trade = all(ex.ready for ex in self.connectors.values())
            if not self.ready_to_trade:
                for con in [c for c in self.connectors.values() if not c.ready]:
                    self.logger().warning(f"{con.name} is not ready. Please wait...")
                return

        current_timestamp = self.current_timestamp

        # Tick each strategy independently
        for strategy_instance in self.strategies:
            try:
                # Call the strategy's c_tick() method
                # This is a C-level Cython call for performance
                strategy_instance.strategy.c_tick(current_timestamp)
            except Exception as e:
                self.logger().error(
                    f"Error ticking strategy '{strategy_instance.name}': {e}",
                    exc_info=True
                )

    async def on_stop(self):
        """
        FIX #3: Clean shutdown of all strategies using the correct clock reference.

        Each strategy's stop() is called to clean up its event listeners
        and cancel any pending orders.
        """
        self.logger().info("Stopping MultiStrategyOrchestrator...")

        # Stop all V1 strategies with the SAME clock they were started with
        if self._strategies_started and self._strategy_clock is not None:
            for strategy_instance in self.strategies:
                try:
                    self.logger().info(f"Stopping strategy: {strategy_instance.name}")
                    # Call c_stop() with the correct clock reference
                    # This will:
                    # 1. Call StrategyBase.c_stop() - removes event listeners
                    # 2. Call strategy.stop() - strategy-specific cleanup
                    strategy_instance.strategy.c_stop(self._strategy_clock)
                except Exception as e:
                    self.logger().error(
                        f"Error stopping strategy '{strategy_instance.name}': {e}",
                        exc_info=True
                    )

            self._strategies_started = False
            self.logger().info(f"All strategies stopped successfully")
        else:
            self.logger().warning("Strategies were never started or clock not available")

        # Note: We don't call super().on_stop() because we bypassed super().__init__()
        # The orchestrator itself has minimal cleanup needs

        self.logger().info("MultiStrategyOrchestrator stopped")

    def format_status(self) -> str:
        """
        Format status output for all strategies.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        lines.append("\n" + "=" * 80)
        lines.append("MULTI-STRATEGY ORCHESTRATOR STATUS")
        lines.append(f"Running {len(self.strategies)} strategies with SHARED websocket connections")
        lines.append("=" * 80)

        # Show shared connectors
        lines.append("\nShared Connectors:")
        for connector_name, connector in self.connectors.items():
            status = "✓ READY" if connector.ready else "✗ NOT READY"
            lines.append(f"  {connector_name}: {status}")

        # Balance overview
        balance_df = self.get_balance_df()
        lines.extend(["\nBalances:"] + ["  " + line for line in balance_df.to_string(index=False).split("\n")])

        # Active orders across all strategies
        try:
            orders_df = self.active_orders_df()
            lines.extend(["\nActive Orders:"] + ["  " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.append("\nNo active orders")

        # Individual strategy status
        for i, strategy_instance in enumerate(self.strategies, 1):
            lines.append(f"\n{'-' * 80}")
            lines.append(f"Strategy {i}: {strategy_instance.name}")
            lines.append(f"{'-' * 80}")

            config = strategy_instance.config
            lines.append(f"  Type: arbitrage_m")
            lines.append(f"  Markets: {config['primary_market']}/{config['primary_trading_pair']} <-> "
                        f"{config['secondary_market']}/{config['secondary_trading_pair']}")
            lines.append(f"  Min Profitability: {config['min_profitability']}%")
            lines.append(f"  Buy-in Enabled: {config['buy_in_enabled']}")

            # Get strategy-specific stats if available
            try:
                strategy = strategy_instance.strategy
                if hasattr(strategy, 'tracked_limit_orders'):
                    active_orders = len([o for o in strategy.tracked_limit_orders if o[1].is_open])
                    lines.append(f"  Active Orders: {active_orders}")
            except Exception as e:
                self.logger().debug(f"Could not get strategy stats: {e}")

        lines.append("\n" + "=" * 80)

        return "\n".join(lines)
