"""
Multi-Strategy Orchestrator for V1 Strategies with Shared Websocket Connections

This script allows running multiple V1 strategies (like arbitrage_m) simultaneously
while sharing websocket connections to the same exchanges. This provides:

1. Resource Efficiency: One websocket connection per exchange instead of one per strategy
2. Rate Limit Optimization: All strategies share the same connection pool
3. Memory Savings: Order books and market data cached once and shared
4. Compatibility: Works with existing V1 Cython strategies without modification
5. Runtime Control: Pause/resume individual strategies without stopping the bot

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

Runtime Control:
---------------
From Hummingbot's Python console (>>>), import all functions with one line:

>>> from scripts.multi_strategy_orchestrator import *

Then use simple commands:

>>> pause("BSX")          # Pause by token symbol (easiest!)
>>> resume("BSX")         # Resume by token
>>> remove("BSX")         # Permanently remove strategy and update config
>>> list_arb()            # List all strategies with details
>>> pause_all()           # Pause everything
>>> resume_all()          # Resume everything
>>> help_arb()            # Show help and available strategies

Functions work by name or token:
>>> pause("arb_bsx_gate_bitmart")  # Full strategy name
>>> pause("BSX")                    # Or just the token symbol
>>> remove("BSX")                   # Remove by token
>>> remove("arb_bsx_gate_bitmart")  # Or by full name

The remove command will:
- Stop the strategy and cancel its orders
- Remove it from the running strategies
- Update the config YAML file
- Smartly clean up unused market pairs from config

The strategy automatically shows available commands when it starts.

"""

import logging
import os
import re
import yaml
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field
from hummingbot.client.config.config_data_types import BaseClientModel

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy.arbitrage_m.arbitrage import ArbitrageMStrategy
from hummingbot.strategy.arbitrage_m.arbitrage_market_pair import ArbitrageMMarketPair
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.script_strategy_base import ScriptConfigBase, ScriptStrategyBase
from hummingbot.strategy.strategy_base import StrategyBase


logger = None

# Export convenience functions for easy import
__all__ = ['pause', 'resume', 'remove', 'list_arb', 'pause_all', 'resume_all', 'help_arb', 'MultiStrategyOrchestrator']

# Global reference to orchestrator instance for convenience functions
_orchestrator_instance: Optional['MultiStrategyOrchestrator'] = None


def _get_orchestrator() -> 'MultiStrategyOrchestrator':
    """Get the orchestrator instance, with fallback to TradingCore."""
    if _orchestrator_instance is not None:
        return _orchestrator_instance

    # Fallback: try to get from HummingbotApplication
    try:
        from hummingbot.client.hummingbot_application import HummingbotApplication
        app = HummingbotApplication.main_application()
        if app and hasattr(app, 'trading_core') and app.trading_core.strategy:
            strategy = app.trading_core.strategy
            if isinstance(strategy, MultiStrategyOrchestrator):
                return strategy
    except Exception:
        pass

    raise RuntimeError("MultiStrategyOrchestrator not found. Is the strategy running?")


def pause(identifier: str) -> bool:
    """
    Pause a strategy by name or token symbol.

    Args:
        identifier: Full strategy name (e.g., 'arb_bsx_gate_bitmart') or token symbol (e.g., 'BSX')

    Returns:
        True if successful

    Examples:
        >>> pause("arb_bsx_gate_bitmart")  # By full name
        >>> pause("BSX")                    # By token symbol
    """
    orchestrator = _get_orchestrator()
    return orchestrator.pause_strategy_by_identifier(identifier)


def resume(identifier: str) -> bool:
    """
    Resume a strategy by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> resume("arb_bsx_gate_bitmart")
        >>> resume("BSX")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.resume_strategy_by_identifier(identifier)


def remove(identifier: str) -> bool:
    """
    Permanently remove a strategy by name or token symbol.

    This will:
    - Stop the strategy and cancel its orders
    - Remove it from the running strategies
    - Update the config YAML file
    - Clean up unused market pairs from config

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> remove("arb_bsx_gate_bitmart")  # By full name
        >>> remove("BSX")                    # By token symbol
    """
    orchestrator = _get_orchestrator()
    return orchestrator.remove_strategy_by_identifier(identifier)


def list_arb() -> Dict[str, Dict[str, Any]]:
    """
    List all arbitrage strategies with their status.

    Returns:
        Dictionary with strategy details

    Example:
        >>> list_arb()
    """
    orchestrator = _get_orchestrator()
    return orchestrator.list_strategies()


def pause_all() -> None:
    """
    Pause all strategies.

    Example:
        >>> pause_all()
    """
    orchestrator = _get_orchestrator()
    orchestrator.pause_all_strategies()


def resume_all() -> None:
    """
    Resume all strategies.

    Example:
        >>> resume_all()
    """
    orchestrator = _get_orchestrator()
    orchestrator.resume_all_strategies()


def help_arb() -> None:
    """
    Show runtime control help and available strategies.

    Example:
        >>> help_arb()
    """
    orchestrator = _get_orchestrator()
    orchestrator.show_help()


@dataclass
class V1StrategyInstance:
    """Wrapper for a V1 strategy instance with metadata"""
    strategy: StrategyBase
    name: str
    config: Dict
    market_pairs: List[MarketTradingPairTuple]
    paused: bool = False  # Runtime pause state


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


class MultiStrategyOrchestratorConfig(BaseClientModel):
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

        # Store config file path for runtime editing
        self._config_file_path: Optional[str] = None
        self._try_detect_config_file_path()

        # Storage for V1 strategy instances
        self.strategies: List[V1StrategyInstance] = []
        self._strategies_started: bool = False  # FIX #2: Track whether strategies have been started
        self._strategy_clock = None  # FIX #3: Store clock reference for strategies
        # Streamlined readiness logging and deferred start helpers
        self._last_not_ready_names: Set[str] = set()
        self._last_ready_log_time: float = 0.0
        self._ready_announce_done: bool = False

        # Initialize all configured strategies (but don't start them yet - no clock available)
        self._initialize_arbitrage_m_strategies()

        # Set global instance for convenience functions
        global _orchestrator_instance
        _orchestrator_instance = self

        self.logger().info(f"MultiStrategyOrchestrator initialized with {len(self.strategies)} strategies")
        self.logger().info(f"Shared connectors: {list(self.connectors.keys())}")
        self.logger().info("")
        self._show_runtime_help()

    def _show_runtime_help(self):
        """Display runtime control help message."""
        self.logger().info("=" * 70)
        self.logger().info("RUNTIME CONTROL COMMANDS")
        self.logger().info("=" * 70)
        self.logger().info("Available commands:")
        self.logger().info("  control list                # List all strategies")
        self.logger().info("  control pause <token>       # Pause strategy (e.g., control pause BSX)")
        self.logger().info("  control resume <token>      # Resume strategy")
        self.logger().info("  control remove <token>      # Permanently remove strategy and update config")
        self.logger().info("  control pause_all           # Pause all strategies")
        self.logger().info("  control resume_all          # Resume all strategies")
        self.logger().info("")
        self.logger().info(f"Loaded {len(self.strategies)} strateg{'y' if len(self.strategies) == 1 else 'ies'}")
        self.logger().info("=" * 70)
        self.logger().info("")

    def show_help(self):
        """Show runtime control help (can be called from console)."""
        self._show_runtime_help()

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
            buy_in_min_profitability=float(config.buy_in_min_profitability) / 100.0,
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

        # If connectors are already ready, start immediately; otherwise defer until tick detects readiness
        try:
            all_ready = all(getattr(c, 'ready', False) for c in self.connectors.values())
        except Exception:
            all_ready = False

        if all_ready:
            self._start_all_strategies_if_needed()
        else:
            self.logger().info(
                f"Deferring start of {len(self.strategies)} V1 strategies until connectors are ready..."
            )

    def tick(self, timestamp: float):
        """Override base tick to reduce startup log spam and start strategies only when ready.

        - Aggregates and throttles readiness logs to once every 2s or on change.
        - Starts V1 strategies only after all connectors are ready.
        """
        try:
            not_ready = [name for name, c in self.connectors.items() if not getattr(c, 'ready', False)]
        except Exception:
            not_ready = list(self.connectors.keys())

        if not_ready:
            # Throttle and aggregate logs
            names_set = set(not_ready)
            if names_set != self._last_not_ready_names or (timestamp - self._last_ready_log_time) >= 2.0:
                self.logger().info(
                    f"Waiting for connectors ({len(not_ready)}): {', '.join(not_ready)}"
                )
                self._last_not_ready_names = names_set
                self._last_ready_log_time = timestamp
            # Stay idle until connectors are ready
            return

        # All connectors ready
        if not self.ready_to_trade:
            self.ready_to_trade = True
            if not self._ready_announce_done:
                self.logger().info("All connectors ready. Starting strategies and entering trading loop.")
                self._ready_announce_done = True
            self._start_all_strategies_if_needed()

        # Delegate to base tick to preserve normal runtime cadence/behavior
        return super().tick(timestamp)

    def _start_all_strategies_if_needed(self):
        if self._strategies_started:
            return
        self.logger().info(f"Starting {len(self.strategies)} V1 strategies with clock...")
        for strategy_instance in self.strategies:
            try:
                self.logger().info(f"Starting strategy: {strategy_instance.name}")
                if hasattr(strategy_instance.strategy, "start"):
                    strategy_instance.strategy.start(self._strategy_clock)
            except Exception as e:
                self.logger().error(
                    f"Error starting strategy '{strategy_instance.name}': {e}",
                    exc_info=True
                )
        self._strategies_started = True
        self.logger().info("All strategies started successfully")

    def on_tick(self):
        """
        Main tick function - tick all strategies.

        Each strategy's c_tick() is called independently. The strategies share
        the same connectors but maintain separate state and logic.

        Note: ready_to_trade is already checked by the inherited tick() method
        from ScriptStrategyBase, so we don't need to check it again here.
        """
        # Get current timestamp from TimeIterator property
        # This was set by TimeIterator.c_tick() before tick() was called
        current_timestamp = self.current_timestamp

        # Tick each strategy independently (skip paused strategies)
        for strategy_instance in self.strategies:
            # Skip ticking if strategy is paused
            if strategy_instance.paused:
                continue

            try:
                # Call the Python-level tick() method
                strategy_instance.strategy.tick(current_timestamp)
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
                    if hasattr(strategy_instance.strategy, "stop"):
                        strategy_instance.strategy.stop(self._strategy_clock)
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

    def _find_strategy_by_token(self, token_symbol: str) -> Optional[V1StrategyInstance]:
        """
        Find a strategy by token symbol in its trading pairs.

        Args:
            token_symbol: Token symbol to search for (e.g., 'BSX', 'PHL')

        Returns:
            V1StrategyInstance if found, None otherwise
        """
        token_upper = token_symbol.upper()

        for strategy_instance in self.strategies:
            # Check if token appears in any trading pair
            for market_pair in strategy_instance.market_pairs:
                trading_pair = market_pair.trading_pair
                if '-' in trading_pair:
                    base, quote = trading_pair.split('-', 1)
                    if base.upper() == token_upper or quote.upper() == token_upper:
                        return strategy_instance

        return None

    def pause_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Pause a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match (silently)
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.pause_strategy(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.pause_strategy(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def resume_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Resume a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match (silently)
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.resume_strategy(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.resume_strategy(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def pause_strategy(self, strategy_name: str) -> bool:
        """
        Pause a specific arbitrage_m strategy by name.

        Args:
            strategy_name: The name of the strategy to pause (from config)

        Returns:
            True if successful, False otherwise
        """
        # Validate input
        if not strategy_name or not strategy_name.strip():
            self.logger().error("Strategy name cannot be empty")
            return False

        strategy_instance = next(
            (s for s in self.strategies if s.name == strategy_name),
            None
        )

        if not strategy_instance:
            self.logger().error(
                f"Strategy '{strategy_name}' not found. Available strategies: "
                f"{[s.name for s in self.strategies]}"
            )
            return False

        if strategy_instance.paused:
            self.logger().warning(f"Strategy '{strategy_name}' is already paused")
            return False

        self.logger().info(f"Pausing strategy: {strategy_name}")
        strategy_instance.paused = True

        # Cancel any open orders for this strategy
        try:
            if hasattr(strategy_instance.strategy, "cancel_all_orders"):
                strategy_instance.strategy.cancel_all_orders()
                self.logger().info(f"Cancelled all open orders for '{strategy_name}'")
        except Exception as e:
            self.logger().warning(f"Error cancelling orders for '{strategy_name}': {e}")

        self.logger().info(f"Strategy '{strategy_name}' paused successfully")
        return True

    def resume_strategy(self, strategy_name: str) -> bool:
        """
        Resume a paused arbitrage_m strategy by name.

        Args:
            strategy_name: The name of the strategy to resume

        Returns:
            True if successful, False otherwise
        """
        # Validate input
        if not strategy_name or not strategy_name.strip():
            self.logger().error("Strategy name cannot be empty")
            return False

        strategy_instance = next(
            (s for s in self.strategies if s.name == strategy_name),
            None
        )

        if not strategy_instance:
            self.logger().error(
                f"Strategy '{strategy_name}' not found. Available strategies: "
                f"{[s.name for s in self.strategies]}"
            )
            return False

        if not strategy_instance.paused:
            self.logger().warning(f"Strategy '{strategy_name}' is already running")
            return False

        self.logger().info(f"Resuming strategy: {strategy_name}")
        strategy_instance.paused = False
        self.logger().info(f"Strategy '{strategy_name}' resumed successfully")
        return True

    def pause_all_strategies(self) -> int:
        """Pause all running strategies.

        Returns:
            Number of strategies that were paused
        """
        self.logger().info("Pausing all strategies...")
        count = 0
        for strategy_instance in self.strategies:
            if not strategy_instance.paused:
                if self.pause_strategy(strategy_instance.name):
                    count += 1
        return count

    def resume_all_strategies(self) -> int:
        """Resume all paused strategies.

        Returns:
            Number of strategies that were resumed
        """
        self.logger().info("Resuming all strategies...")
        count = 0
        for strategy_instance in self.strategies:
            if strategy_instance.paused:
                if self.resume_strategy(strategy_instance.name):
                    count += 1
        return count

    def remove_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Permanently remove a strategy by full name or token symbol.

        This will:
        - Stop the strategy and cancel its orders
        - Remove it from the running strategies list
        - Update the config YAML file
        - Clean up unused market pairs from config

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match (silently)
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self._remove_strategy(strategy_instance.name)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self._remove_strategy(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def _remove_strategy(self, strategy_name: str) -> bool:
        """
        Internal method to permanently remove a strategy.

        Args:
            strategy_name: The name of the strategy to remove

        Returns:
            True if successful, False otherwise
        """
        # Validate input
        if not strategy_name or not strategy_name.strip():
            self.logger().error("Strategy name cannot be empty")
            return False

        strategy_instance = next(
            (s for s in self.strategies if s.name == strategy_name),
            None
        )

        if not strategy_instance:
            self.logger().error(
                f"Strategy '{strategy_name}' not found. Available strategies: "
                f"{[s.name for s in self.strategies]}"
            )
            return False

        self.logger().info(f"Removing strategy permanently: {strategy_name}")

        # Step 1: Cancel any open orders for this strategy
        try:
            if hasattr(strategy_instance.strategy, "cancel_all_orders"):
                strategy_instance.strategy.cancel_all_orders()
                self.logger().info(f"Cancelled all open orders for '{strategy_name}'")
        except Exception as e:
            self.logger().warning(f"Error cancelling orders for '{strategy_name}': {e}")

        # Step 2: Stop the strategy if it's running
        try:
            if hasattr(strategy_instance.strategy, "stop") and self._strategy_clock is not None:
                strategy_instance.strategy.stop(self._strategy_clock)
                self.logger().info(f"Stopped strategy '{strategy_name}'")
        except Exception as e:
            self.logger().warning(f"Error stopping strategy '{strategy_name}': {e}")

        # Step 3: Remove from strategies list
        self.strategies.remove(strategy_instance)
        self.logger().info(f"Removed '{strategy_name}' from runtime")

        # Step 4: Update config file
        if self._config_file_path and os.path.exists(self._config_file_path):
            try:
                self._update_config_file_remove_strategy(strategy_name)
                self.logger().info(f"Updated config file: {self._config_file_path}")
            except Exception as e:
                self.logger().error(f"Failed to update config file: {e}", exc_info=True)
                return False
        else:
            self.logger().warning(
                "Config file path not set or file doesn't exist. "
                "Strategy removed from runtime but config not updated."
            )

        self.logger().info(f"Strategy '{strategy_name}' removed successfully")
        return True

    def _update_config_file_remove_strategy(self, strategy_name: str):
        """
        Update the config YAML file to remove a strategy and clean up unused markets.

        Args:
            strategy_name: Name of the strategy to remove from config
        """
        # Load the current config file
        with open(self._config_file_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Find and remove the strategy from arbitrage_m_strategies
        strategies_list = config_data.get('arbitrage_m_strategies', [])
        removed_strategy = None

        for i, strategy in enumerate(strategies_list):
            if strategy.get('name') == strategy_name:
                removed_strategy = strategies_list.pop(i)
                break

        if removed_strategy is None:
            self.logger().warning(f"Strategy '{strategy_name}' not found in config file")
            return

        # Collect all markets used by the removed strategy
        removed_markets = self._collect_markets_from_strategy(removed_strategy)

        # Collect all markets still needed by remaining strategies
        remaining_markets = set()
        for strategy in strategies_list:
            remaining_markets.update(self._collect_markets_from_strategy(strategy))

        # Remove markets that are no longer needed
        markets_section = config_data.get('markets', {})
        for exchange, pair in removed_markets:
            if (exchange, pair) not in remaining_markets:
                if exchange in markets_section and pair in markets_section[exchange]:
                    markets_section[exchange].remove(pair)
                    self.logger().info(f"Removed unused market pair: {exchange}:{pair}")

                    # If exchange has no more pairs, remove the exchange
                    if not markets_section[exchange]:
                        del markets_section[exchange]
                        self.logger().info(f"Removed unused exchange: {exchange}")

        # Update the config data
        config_data['arbitrage_m_strategies'] = strategies_list
        config_data['markets'] = markets_section

        # Write back to file with formatting
        with open(self._config_file_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False, indent=2)

        self.logger().info(f"Config file updated: removed '{strategy_name}'")

    def _collect_markets_from_strategy(self, strategy_config: Dict) -> Set[Tuple[str, str]]:
        """
        Collect all (exchange, trading_pair) tuples used by a strategy.

        Args:
            strategy_config: Strategy configuration dict

        Returns:
            Set of (exchange, trading_pair) tuples
        """
        markets = set()

        # Add primary market
        primary_market = strategy_config.get('primary_market')
        primary_pair = strategy_config.get('primary_trading_pair')
        if primary_market and primary_pair:
            markets.add((primary_market, primary_pair))

        # Add secondary market
        secondary_market = strategy_config.get('secondary_market')
        secondary_pair = strategy_config.get('secondary_trading_pair')
        if secondary_market and secondary_pair:
            markets.add((secondary_market, secondary_pair))

        # Add additional markets
        additional_markets = strategy_config.get('additional_markets', [])
        for additional in additional_markets:
            if ':' in additional:
                exchange, pair = additional.split(':', 1)
                markets.add((exchange.lower(), pair))

        return markets

    def set_config_file_path(self, file_path: str):
        """
        Set the config file path for runtime editing.

        Args:
            file_path: Path to the config YAML file
        """
        self._config_file_path = file_path
        self.logger().info(f"Config file path set to: {file_path}")

    def _try_detect_config_file_path(self):
        """
        Try to auto-detect the config file path from the Hummingbot application.

        This attempts to find the config file that was used to start this strategy.
        """
        try:
            from hummingbot.client.hummingbot_application import HummingbotApplication
            from hummingbot.client import settings

            app = HummingbotApplication.main_application()
            if app and hasattr(app, 'strategy_file_name'):
                # The strategy file name is typically the config file name
                config_file_name = app.strategy_file_name

                # Check if it's in the scripts config directory
                scripts_conf_dir = os.path.join(settings.CONF_DIR_PATH, "scripts")
                potential_path = os.path.join(scripts_conf_dir, config_file_name)

                if os.path.exists(potential_path):
                    self._config_file_path = potential_path
                    self.logger().info(f"Auto-detected config file: {potential_path}")
                    return

                # Also try without the path (just the filename)
                potential_path = os.path.join(scripts_conf_dir, os.path.basename(config_file_name))
                if os.path.exists(potential_path):
                    self._config_file_path = potential_path
                    self.logger().info(f"Auto-detected config file: {potential_path}")
                    return
        except Exception as e:
            self.logger().debug(f"Could not auto-detect config file path: {e}")

        self.logger().warning(
            "Config file path not auto-detected. "
            "The 'remove' command will not be able to update the config file. "
            "You can manually set it with: orchestrator.set_config_file_path('/path/to/config.yml')"
        )

    def list_strategies(self) -> Dict[str, Dict[str, Any]]:
        """
        Get a summary of all strategies and their statuses.

        Returns:
            Dictionary mapping strategy_name to strategy info
        """
        strategy_summary = {}
        for strategy_instance in self.strategies:
            status = "PAUSED" if strategy_instance.paused else "RUNNING"

            # Try to get profitability info
            best_prof_str = "n/a"
            try:
                if hasattr(strategy_instance.strategy, 'format_status'):
                    status_blob = strategy_instance.strategy.format_status()
                    best_prof_str = self._parse_best_profitability(status_blob) or best_prof_str
            except Exception:
                pass

            strategy_summary[strategy_instance.name] = {
                "status": status,
                "paused": strategy_instance.paused,
                "primary_market": strategy_instance.config.get('primary_market', 'N/A'),
                "secondary_market": strategy_instance.config.get('secondary_market', 'N/A'),
                "primary_pair": strategy_instance.config.get('primary_trading_pair', 'N/A'),
                "secondary_pair": strategy_instance.config.get('secondary_trading_pair', 'N/A'),
                "min_profitability": strategy_instance.config.get('min_profitability', 'N/A'),
                "best_profitability": best_prof_str,
            }

        return strategy_summary

    def format_status(self) -> str:
        """
        Format status output for all strategies.
        """
        if not self.ready_to_trade:
            # Show only non-ready connectors if available
            not_ready = [name for name, c in self.connectors.items() if not getattr(c, 'ready', False)]
            if not_ready:
                return "\n".join(["Connectors not ready:"] + [f"  {n}" for n in not_ready])
            return "Market connectors are not ready."

        lines = []

        # Strategy Status Summary
        running_count = sum(1 for s in self.strategies if not s.paused)
        paused_count = sum(1 for s in self.strategies if s.paused)
        lines.append(f"\nStrategies: {running_count} active, {paused_count} paused")

        # Balances
        balance_df = self.get_balance_df()
        lines.extend(["\nBalances:"] + ["  " + line for line in balance_df.to_string(index=False).split("\n")])

        lines.append("")

        # One-line per strategy; collect optional sections for the bottom
        buyin_sections = []

        # Collect row data first to compute column widths for aligned output
        rows: List[Dict[str, str]] = []
        for strategy_instance in self.strategies:
            strategy_name = strategy_instance.name

            markets_str = self._format_markets_compact(strategy_instance.market_pairs)

            min_prof_value = strategy_instance.config.get('min_profitability')
            min_prof_str = self._format_min_percent(min_prof_value) if min_prof_value is not None else None

            status_blob = None
            best_prof_str = "n/a"

            # Show "PAUSED" instead of profitability if strategy is paused
            if strategy_instance.paused:
                best_prof_str = "PAUSED"
            else:
                try:
                    strategy = strategy_instance.strategy
                    if hasattr(strategy, 'format_status'):
                        status_blob = strategy.format_status()
                        best_prof_str = self._parse_best_profitability(status_blob) or best_prof_str
                except Exception as e:
                    self.logger().debug(f"Could not get strategy stats for '{strategy_instance.name}': {e}")

            rows.append({
                "markets": markets_str,
                "min": f"min {min_prof_str}" if min_prof_str is not None else "",
                "best": best_prof_str,
            })

            if status_blob is not None:
                bi_lines = self._extract_buyin_lines(status_blob)
                if bi_lines:
                    buyin_sections.append((strategy_name, bi_lines))

        if rows:
            # Compute widths for pair and exchange columns to align neatly
            def split_pair_ex(s: str) -> Tuple[str, str]:
                try:
                    pair, rest = s.split(" ", 1)
                    return pair, rest
                except Exception:
                    return s, ""

            pair_ex_rows: List[Tuple[str, str]] = [split_pair_ex(r["markets"]) for r in rows]
            pair_w = max((len(p) for p, _ in pair_ex_rows), default=0)
            ex_w = max((len(e) for _, e in pair_ex_rows), default=0)

            # Width for the entire "min X%" field; rows without min will be padded with spaces
            try:
                min_w = max((len(r["min"]) for r in rows), default=0)
            except Exception:
                min_w = 0

            for (pair_part, ex_part), r in zip(pair_ex_rows, rows):
                if ex_w > 0:
                    markets_fmt = f"{pair_part:<{pair_w}} {ex_part:<{ex_w}}"
                else:
                    markets_fmt = f"{pair_part:<{pair_w}}"

                if min_w > 0:
                    min_field = f"{r['min']:<{min_w}}" if r['min'] else (" " * min_w)
                    lines.append(f"{markets_fmt} | {min_field} | best {r['best']}")
                else:
                    lines.append(f"{markets_fmt} | best {r['best']}")

        if buyin_sections:
            lines.append("\nBuy-in active:")
            for name, blines in buyin_sections:
                lines.append(f"  {name}:")
                lines.extend([f"    {ln}" for ln in blines])

        # Only show connectors not ready; omit when all ready
        not_ready = [name for name, c in self.connectors.items() if not getattr(c, 'ready', False)]
        if not_ready:
            lines.append("\nConnectors not ready:")
            for n in not_ready:
                lines.append(f"  {n}")

        return "\n".join(lines)

    # --- compact status helpers ---
    def _exchange_priority(self) -> Dict[str, int]:
        # Lower index means higher priority
        order = ['bybit', 'kucoin', 'gate_io', 'mexc', 'htx', 'bitmart', 'bing_x', 'okx', 'bitget']
        return {name: idx for idx, name in enumerate(order)}

    def _display_exchange_name(self, name: str) -> str:
        """Trim everything after the first underscore for display (e.g., gate_io -> gate)."""
        try:
            if "_" in name:
                return name.split("_", 1)[0]
            return name
        except Exception:
            return name

    def _format_markets_compact(self, market_tuples: List[MarketTradingPairTuple]) -> str:
        """Return compact markets string for the instance.
        - All same base: "BASE-QUOTE ex1_ex2_ex3"
        - Mixed bases:   "BASE-QUOTE ex1_ex2(BASE2)_ex3(BASE3)"
        """
        if not market_tuples:
            return "-"

        triples: List[Tuple[str, str, str]] = []  # (exchange, base, quote)
        for t in market_tuples:
            try:
                triples.append((t.market.name, t.base_asset, t.quote_asset))
            except Exception:
                try:
                    base, quote = str(t.trading_pair).split("-")
                except Exception:
                    base, quote = getattr(t, 'base_asset', '?'), getattr(t, 'quote_asset', '?')
                triples.append((getattr(t.market, 'name', '?'), base, quote))

        base_counts = Counter([b for _, b, _ in triples])
        quote_counts = Counter([q for _, _, q in triples])

        # Pick most common base; tie-break using highest-priority exchange that lists it
        prio = self._exchange_priority()
        def best_prio_for_base(b: str) -> int:
            ranks = [prio.get(ex, 10_000) for ex, bb, _ in triples if bb == b]
            return min(ranks) if ranks else 10_000

        if base_counts:
            max_ct = max(base_counts.values())
            base_candidates = [b for b, c in base_counts.items() if c == max_ct]
            global_base = min(base_candidates, key=best_prio_for_base)
        else:
            global_base = triples[0][1]

        global_quote = quote_counts.most_common(1)[0][0] if quote_counts else triples[0][2]

        # Order exchanges by configured priority (same priority rule as tie-break for base)
        triples_sorted = sorted(triples, key=lambda x: prio.get(x[0], 10_000))
        parts: List[str] = []
        for ex, base, _q in triples_sorted:
            disp = self._display_exchange_name(ex)
            if base == global_base:
                parts.append(disp)
            else:
                parts.append(f"{disp}({base})")

        return f"{global_base}-{global_quote} {'_'.join(parts)}"

    def _extract_buyin_lines(self, status_blob: str) -> List[str]:
        """Extract the buy-in header and asset rows from a strategy status output.
        Returns [] when buy-in is not active (no section present)."""
        if not status_blob:
            return []
        raw_lines = status_blob.split("\n")
        out: List[str] = []
        collecting = False
        for raw in raw_lines:
            s = raw.strip()
            if s.startswith("Buy-in:"):
                # Reformat min_prof to one decimal if present
                try:
                    # Example: Buy-in: target=1000.000000 min_prof=2.50%
                    m = re.search(r"min_prof\s*=\s*([0-9]+(?:\.[0-9]+)?)%", s)
                    if m:
                        val = float(m.group(1))
                        s = re.sub(r"min_prof\s*=\s*([0-9]+(?:\.[0-9]+)?)%",
                                   f"min_prof={val:.1f}%", s)
                except Exception:
                    pass
                out.append(s)
                collecting = True
                continue
            if collecting:
                if raw.startswith("    ") and s:
                    out.append(s)
                    continue
                if out:
                    break
        return out

    def _parse_best_profitability(self, status_blob: str) -> Optional[str]:
        """Parse the 'best:' line and return only the trailing profitability piece after '->'."""
        if not status_blob:
            return None
        try:
            for ln in status_blob.split("\n"):
                stripped = ln.strip()
                if stripped.startswith("best:") and "->" in stripped:
                    try:
                        return stripped.split("->", 1)[1].strip()
                    except Exception:
                        return stripped
        except Exception:
            return None
        return None

    def _format_min_percent(self, value) -> str:
        """Format a percent value with one decimal, keeping input flexibility (str/float/Decimal)."""
        try:
            return f"{float(value):.1f}%"
        except Exception:
            return f"{value}%"