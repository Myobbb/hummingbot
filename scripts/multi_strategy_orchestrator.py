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
There are TWO ways to control strategies:

Method 1: CLI Commands (easiest, recommended)
----------------------------------------------
Use the 'control' command from the Hummingbot prompt:

control list                  # List all strategies with status
control pause BSX             # Pause by token symbol
control resume BSX            # Resume by token symbol
control pause_all             # Pause all strategies
control resume_all            # Resume all strategies
control remove BSX            # Remove strategy (edits config file!)

Commands work by name or token:
control pause arb_bsx_gate_bitmart   # Full strategy name
control pause BSX                     # Or just token symbol
control remove BSX                    # Same for remove

Method 2: Python Console Functions
-----------------------------------
From Hummingbot's Python console (>>>), import all functions:

>>> from scripts.multi_strategy_orchestrator import *

Then use Python functions:
>>> pause("BSX")          # Pause by token symbol
>>> resume("BSX")         # Resume by token
>>> remove("BSX")         # Remove strategy (edits config file!)
>>> list_arb()            # List all strategies with details
>>> pause_all()           # Pause everything
>>> resume_all()          # Resume everything

The strategy automatically shows available commands when it starts.

"""

import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
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
__all__ = ['pause', 'resume', 'list_arb', 'pause_all', 'resume_all', 'help_arb', 'remove', 'MultiStrategyOrchestrator']

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


def remove(identifier: str) -> bool:
    """
    Remove a strategy by name or token symbol, updating the config file.

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

    # Config file path (for runtime editing)
    config_file_path: Optional[str] = Field(
        default=None,
        description="Path to the YAML config file (set automatically on load)"
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
        self.logger().info("  control pause_all           # Pause all strategies")
        self.logger().info("  control resume_all          # Resume all strategies")
        self.logger().info("  control remove <token>      # Remove strategy (edits config file)")
        self.logger().info("  control add <file>          # Add strategy from conf/strategies/")
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

    def _add_arbitrage_m_strategy(self, config: ArbitrageMInstanceConfig, paused: bool = False):
        """
        Add an arbitrage_m strategy instance.

        This method:
        1. Builds market pairs from the shared connector pool
        2. Creates an ArbitrageMStrategy instance
        3. Initializes it with the config
        4. The strategy's c_add_markets() call registers event listeners

        Args:
            config: Strategy configuration
            paused: Whether to start the strategy in paused state (useful for runtime additions)
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
            market_pairs=market_tuples,
            paused=paused  # Set initial pause state
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
        Remove a strategy by full name or token symbol.

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
            return self.remove_strategy(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.remove_strategy(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def remove_strategy(self, strategy_name: str) -> bool:
        """
        Remove a specific arbitrage_m strategy by name.

        This will:
        1. Stop and remove the strategy from memory
        2. Update the config file to remove the strategy
        3. Remove markets that are no longer used by any remaining strategies

        Args:
            strategy_name: The name of the strategy to remove (from config)

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

        self.logger().info(f"Removing strategy: {strategy_name}")

        # Step 1: Stop the strategy if it's running
        try:
            if self._strategies_started and self._strategy_clock is not None:
                self.logger().info(f"Stopping strategy before removal: {strategy_name}")
                if hasattr(strategy_instance.strategy, "stop"):
                    strategy_instance.strategy.stop(self._strategy_clock)
                if hasattr(strategy_instance.strategy, "cancel_all_orders"):
                    strategy_instance.strategy.cancel_all_orders()
        except Exception as e:
            self.logger().warning(f"Error stopping strategy '{strategy_name}': {e}")

        # Step 2: Collect market info before removal
        removed_markets = self._get_strategy_markets(strategy_instance)

        # Step 3: Remove from in-memory list
        self.strategies = [s for s in self.strategies if s.name != strategy_name]
        self.logger().info(f"Strategy '{strategy_name}' removed from memory")

        # Step 4: Update the config file
        if self.config.config_file_path:
            try:
                self._update_config_file(strategy_name, removed_markets)
                self.logger().info(f"Config file updated: {self.config.config_file_path}")
            except Exception as e:
                self.logger().error(f"Failed to update config file: {e}", exc_info=True)
                return False
        else:
            self.logger().warning("Config file path not set, skipping file update")

        self.logger().info(f"Strategy '{strategy_name}' removed successfully")
        return True

    def _get_strategy_markets(self, strategy_instance: V1StrategyInstance) -> Dict[str, Set[str]]:
        """
        Get all markets used by a strategy.

        Returns:
            Dict mapping exchange name to set of trading pairs
        """
        markets = {}
        for market_pair in strategy_instance.market_pairs:
            exchange_name = market_pair.market.name
            trading_pair = market_pair.trading_pair

            if exchange_name not in markets:
                markets[exchange_name] = set()
            markets[exchange_name].add(trading_pair)

        return markets

    def _get_markets_still_in_use(self) -> Dict[str, Set[str]]:
        """
        Get all markets still used by remaining strategies.

        Returns:
            Dict mapping exchange name to set of trading pairs
        """
        markets_in_use = {}

        for strategy_instance in self.strategies:
            for market_pair in strategy_instance.market_pairs:
                exchange_name = market_pair.market.name
                trading_pair = market_pair.trading_pair

                if exchange_name not in markets_in_use:
                    markets_in_use[exchange_name] = set()
                markets_in_use[exchange_name].add(trading_pair)

        return markets_in_use

    def _update_config_file(self, strategy_name: str, removed_markets: Dict[str, Set[str]]):
        """
        Update the YAML config file to remove the strategy and unused markets.

        Args:
            strategy_name: Name of the strategy to remove
            removed_markets: Markets that were used by the removed strategy
        """
        config_path = Path(self.config.config_file_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read current config
        with open(config_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ValueError("Config file is empty or invalid")

        # Remove strategy from arbitrage_m_strategies
        if 'arbitrage_m_strategies' in yaml_data:
            original_count = len(yaml_data['arbitrage_m_strategies'])
            yaml_data['arbitrage_m_strategies'] = [
                s for s in yaml_data['arbitrage_m_strategies']
                if s.get('name') != strategy_name
            ]
            new_count = len(yaml_data['arbitrage_m_strategies'])
            self.logger().info(
                f"Removed strategy from config: {original_count} -> {new_count} strategies"
            )

        # Determine which markets are still in use
        markets_still_in_use = self._get_markets_still_in_use()

        # Remove markets that are no longer used
        if 'markets' in yaml_data:
            markets_to_remove = {}

            for exchange, pairs in removed_markets.items():
                still_used = markets_still_in_use.get(exchange, set())
                unused_pairs = pairs - still_used

                if unused_pairs:
                    markets_to_remove[exchange] = unused_pairs

            # Remove unused pairs from the YAML
            for exchange, unused_pairs in markets_to_remove.items():
                if exchange in yaml_data['markets']:
                    original_pairs = yaml_data['markets'][exchange]
                    if isinstance(original_pairs, list):
                        yaml_data['markets'][exchange] = [
                            p for p in original_pairs
                            if p not in unused_pairs
                        ]

                        # Remove exchange entirely if no pairs left
                        if not yaml_data['markets'][exchange]:
                            del yaml_data['markets'][exchange]
                            self.logger().info(f"Removed exchange '{exchange}' (no pairs remaining)")
                        else:
                            self.logger().info(
                                f"Removed pairs from '{exchange}': {unused_pairs}"
                            )

        # Write updated config back to file
        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)

    async def add_strategy_from_file(self, config_file: str) -> bool:
        """
        Add a strategy from a single-strategy config file.

        This will:
        1. Load the single-strategy config file from conf/strategies/
        2. Convert it to multi-strategy format
        3. Merge markets into the current config
        4. Add strategy to arbitrage_m_strategies list
        5. Save the updated config file
        6. Start the new strategy in the running orchestrator

        Args:
            config_file: Name of the config file (e.g., 'arb_ace_kucoin_gate.yml')

        Returns:
            True if successful, False otherwise
        """
        from hummingbot.client.settings import STRATEGIES_CONF_DIR_PATH

        try:
            # Normalize filename
            if not config_file.endswith('.yml') and not config_file.endswith('.yaml'):
                config_file = f"{config_file}.yml"

            # Load the single-strategy config file
            config_path = STRATEGIES_CONF_DIR_PATH / config_file
            if not config_path.exists():
                self.logger().error(f"Config file not found: {config_path}")
                return False

            with open(config_path, 'r') as f:
                single_config = yaml.safe_load(f)

            if not single_config:
                self.logger().error(f"Config file is empty or invalid: {config_path}")
                return False

            # Validate it's an arbitrage_m strategy
            if single_config.get('strategy') != 'arbitrage_m':
                self.logger().error(f"Only arbitrage_m strategies are supported. Found: {single_config.get('strategy')}")
                return False

            # Convert single-strategy config to multi-strategy format
            strategy_config = self._convert_single_to_multi_config(single_config, config_file)
            if not strategy_config:
                return False

            # Extract markets needed by this strategy
            new_markets = self._extract_markets_from_strategy(strategy_config)

            # Update the multi-strategy config file
            if not self.config.config_file_path:
                self.logger().error("Config file path not set")
                return False

            self._merge_strategy_into_config(strategy_config, new_markets)

            # Ensure connectors are updated/created for all new markets so order books subscribe properly
            from hummingbot.client.hummingbot_application import HummingbotApplication
            app = HummingbotApplication.main_application()
            trading_core = getattr(app, "trading_core", None)
            connector_manager = getattr(trading_core, "connector_manager", None) if trading_core else None

            if connector_manager is None:
                self.logger().error("ConnectorManager not available; cannot subscribe to new trading pairs at runtime.")
                return False

            # Determine which exchanges need updates or creation
            pairs_to_add_by_exchange: Dict[str, List[str]] = {}
            exchanges_to_create: Dict[str, List[str]] = {}
            for exchange, pairs in new_markets.items():
                if exchange in self.connectors:
                    existing_pairs = []
                    try:
                        existing_pairs = list(getattr(self.connectors[exchange], "trading_pairs", []))
                    except Exception:
                        existing_pairs = []
                    missing = [p for p in pairs if p not in existing_pairs]
                    if missing:
                        pairs_to_add_by_exchange[exchange] = missing
                else:
                    exchanges_to_create[exchange] = list(pairs)

            affected_exchanges = set(pairs_to_add_by_exchange.keys()) | set(exchanges_to_create.keys())

            # If we need to update/create connectors, do it and then reinitialize strategies to bind to new connector instances
            if affected_exchanges:
                self.logger().info(f"Updating connector subscriptions for exchanges: {sorted(list(affected_exchanges))}")

                # Remove affected existing connectors from the clock to let them stop cleanly
                for exchange in pairs_to_add_by_exchange.keys():
                    try:
                        if exchange in self.connectors and self._strategy_clock is not None:
                            self._strategy_clock.remove_iterator(self.connectors[exchange])
                    except Exception as e:
                        self.logger().warning(f"Could not remove {exchange} from clock before update: {e}")

                # Update existing connectors to include missing pairs (recreate under the hood)
                for exchange, add_pairs in pairs_to_add_by_exchange.items():
                    try:
                        self.logger().info(f"Adding trading pairs to {exchange}: {add_pairs}")
                        connector_manager.add_trading_pairs(exchange, add_pairs)
                    except Exception as e:
                        self.logger().error(f"Failed to update {exchange} trading pairs {add_pairs}: {e}", exc_info=True)
                        return False

                # Create entirely new connectors if needed
                for exchange, pairs in exchanges_to_create.items():
                    try:
                        self.logger().info(f"Creating connector {exchange} with pairs: {pairs}")
                        connector_manager.create_connector(exchange, pairs, trading_required=True)
                    except Exception as e:
                        self.logger().error(f"Failed to create connector {exchange}: {e}", exc_info=True)
                        return False

                # Fetch updated/new connector instances, add them to clock and markets recorder, and update our pool
                for exchange in affected_exchanges:
                    try:
                        new_connector = connector_manager.get_connector(exchange)
                        if new_connector is None:
                            self.logger().error(f"Connector {exchange} not found after update.")
                            return False
                        # Replace in our shared pool
                        self.connectors[exchange] = new_connector
                        # Add to clock so it starts ticking immediately
                        if self._strategy_clock is not None:
                            self._strategy_clock.add_iterator(new_connector)
                        # Add to markets recorder
                        if getattr(trading_core, "markets_recorder", None):
                            trading_core.markets_recorder.add_market(new_connector)
                    except Exception as e:
                        self.logger().error(f"Failed to finalize connector {exchange} update: {e}", exc_info=True)
                        return False

                # Rebuild ALL strategies (old + new) to bind to the updated connector objects
                try:
                    # Snapshot existing configs and pause state
                    existing = [(s.config, s.paused, s.name) for s in self.strategies]

                    # Stop and clean old strategy instances
                    for strategy_instance in self.strategies:
                        try:
                            if hasattr(strategy_instance.strategy, "stop") and self._strategy_clock is not None:
                                strategy_instance.strategy.stop(self._strategy_clock)
                            if hasattr(strategy_instance.strategy, "cancel_all_orders"):
                                strategy_instance.strategy.cancel_all_orders()
                        except Exception as e:
                            self.logger().warning(f"Error stopping strategy '{strategy_instance.name}': {e}")

                    # Compose the new full list (existing + the one being added now)
                    full_list: List[Tuple[Dict[str, Any], bool]] = []
                    for conf_dict, paused, _name in existing:
                        full_list.append((conf_dict, paused))
                    full_list.append((strategy_config, True))  # add the new strategy in PAUSED state

                    # Reset and rebuild
                    self.strategies = []
                    for conf_dict, paused in full_list:
                        self._add_arbitrage_m_strategy(ArbitrageMInstanceConfig(**conf_dict), paused=paused)

                    # If strategies were already started, start new instances with the current clock
                    if self._strategies_started and self._strategy_clock is not None:
                        for strategy_instance in self.strategies:
                            try:
                                if hasattr(strategy_instance.strategy, "start"):
                                    strategy_instance.strategy.start(self._strategy_clock)
                            except Exception as e:
                                self.logger().error(
                                    f"Error starting strategy '{strategy_instance.name}' after rebuild: {e}",
                                    exc_info=True
                                )
                        self.logger().info("All strategies rebound to updated connectors and started.")
                    else:
                        self.logger().info("Strategies rebuilt; will be started by orchestrator when ready.")
                except Exception as e:
                    self.logger().error(f"Failed to rebuild strategies after connector updates: {e}", exc_info=True)
                    return False
            else:
                # No connector changes needed; just add the new strategy in PAUSED state and start it if orchestrator already started
                self._add_arbitrage_m_strategy(ArbitrageMInstanceConfig(**strategy_config), paused=True)
                if self._strategies_started and self._strategy_clock is not None:
                    try:
                        self.strategies[-1].strategy.start(self._strategy_clock)
                    except Exception as e:
                        self.logger().warning(f"Failed to start newly added strategy immediately: {e}")

            # Schedule smart auto-resume that checks for order book readiness
            strategy_name = strategy_config['name']
            self.logger().info(
                f"Strategy '{strategy_name}' added in PAUSED state. "
                f"Will check order book availability and auto-resume when ready."
            )

            # Import asyncio for scheduling
            import asyncio

            async def smart_auto_resume_strategy():
                """Poll for order book availability and resume when ready."""
                max_wait_time = 60.0  # Max 60 seconds
                check_interval = 2.0   # Check every 2 seconds
                elapsed_time = 0.0

                while elapsed_time < max_wait_time:
                    await asyncio.sleep(check_interval)
                    elapsed_time += check_interval

                    # Check if all required order books exist
                    all_ready = True
                    missing_books = []

                    for exchange, pairs in new_markets.items():
                        if exchange in self.connectors:
                            connector = self.connectors[exchange]
                            for pair in pairs:
                                try:
                                    # Try to get the order book - will raise if not exists
                                    if hasattr(connector, 'get_order_book'):
                                        connector.get_order_book(pair)
                                except Exception:
                                    all_ready = False
                                    missing_books.append(f"{exchange}:{pair}")

                    if all_ready:
                        # All order books are ready, resume the strategy
                        success = self.resume_strategy(strategy_name)
                        if success:
                            self.logger().info(
                                f"Strategy '{strategy_name}' auto-resumed after {elapsed_time:.0f}s "
                                f"(order books ready)"
                            )
                        else:
                            self.logger().warning(f"Failed to auto-resume strategy '{strategy_name}'")
                        return

                    # Still waiting
                    if int(elapsed_time) % 10 == 0:  # Log every 10 seconds
                        self.logger().info(
                            f"Still waiting for order books ({elapsed_time:.0f}s): "
                            f"{', '.join(missing_books[:3])}{'...' if len(missing_books) > 3 else ''}"
                        )

                # Timeout reached - warn user
                self.logger().warning(
                    f"Timeout waiting for order books after {max_wait_time:.0f}s. "
                    f"Strategy '{strategy_name}' remains PAUSED. "
                    f"Missing: {', '.join(missing_books[:5])}. "
                    f"Use 'control resume {strategy_name}' to resume manually or restart the bot."
                )

            # Schedule the smart auto-resume coroutine
            asyncio.create_task(smart_auto_resume_strategy())

            self.logger().info(
                f"Strategy '{strategy_name}' added successfully. "
                f"Monitoring order books for auto-resume (max 60s wait)."
            )
            return True

        except Exception as e:
            self.logger().error(f"Failed to add strategy from {config_file}: {e}", exc_info=True)
            return False

    def _convert_single_to_multi_config(self, single_config: Dict[str, Any], source_file: str) -> Optional[Dict[str, Any]]:
        """Convert single-strategy config format to multi-strategy format."""
        try:
            # Generate strategy name from config if not provided
            name = single_config.get('name')
            if not name:
                # Extract name from filename (e.g., 'conf_arb_ace_kucoin_gate_strategy.yml' -> 'arb_ace_kucoin_gate')
                base_name = source_file.replace('.yml', '').replace('.yaml', '')
                base_name = base_name.replace('conf_', '').replace('_strategy', '')
                name = base_name

            # Parse additional_markets field (can be string or list)
            additional_markets = single_config.get('additional_markets', [])
            if isinstance(additional_markets, str):
                if additional_markets.strip():
                    # Parse comma-separated string: "mexc:SLF-USDT, htx:SLF-USDT"
                    additional_markets = [m.strip() for m in additional_markets.split(',') if m.strip()]
                else:
                    additional_markets = []

            # Build multi-strategy config
            multi_config = {
                'name': name,
                'primary_market': single_config['primary_market'],
                'secondary_market': single_config['secondary_market'],
                'primary_trading_pair': single_config['primary_market_trading_pair'],
                'secondary_trading_pair': single_config['secondary_market_trading_pair'],
                'min_profitability': single_config.get('min_profitability', 0.5),
                'use_oracle_conversion_rate': single_config.get('use_oracle_conversion_rate', False),
                'secondary_to_primary_base_conversion_rate': single_config.get('secondary_to_primary_base_conversion_rate', 1.0),
                'secondary_to_primary_quote_conversion_rate': single_config.get('secondary_to_primary_quote_conversion_rate', 1.0),
                'buy_in_enabled': single_config.get('buy_in_enabled', False),
                'buy_in_target_usd': single_config.get('buy_in_target_usdt', single_config.get('buy_in_target_usd', 100.0)),
                'buy_in_min_profitability': single_config.get('buy_in_min_profitability', 0.5),
                'status_report_interval': single_config.get('status_report_interval', 60.0),
                'next_trade_delay_interval': single_config.get('next_trade_delay_interval', 2.0),
                'order_timeout': single_config.get('order_timeout', 300.0),
                'additional_markets': additional_markets,
            }

            return multi_config

        except Exception as e:
            self.logger().error(f"Failed to convert config: {e}", exc_info=True)
            return None

    def _extract_markets_from_strategy(self, strategy_config: Dict[str, Any]) -> Dict[str, Set[str]]:
        """Extract all markets needed by a strategy config."""
        markets = {}

        # Primary market
        primary_exchange = strategy_config['primary_market']
        primary_pair = strategy_config['primary_trading_pair']
        if primary_exchange not in markets:
            markets[primary_exchange] = set()
        markets[primary_exchange].add(primary_pair)

        # Secondary market
        secondary_exchange = strategy_config['secondary_market']
        secondary_pair = strategy_config['secondary_trading_pair']
        if secondary_exchange not in markets:
            markets[secondary_exchange] = set()
        markets[secondary_exchange].add(secondary_pair)

        # Additional markets
        for additional in strategy_config.get('additional_markets', []):
            if ':' in additional:
                exchange, pair = additional.split(':', 1)
                if exchange not in markets:
                    markets[exchange] = set()
                markets[exchange].add(pair)

        return markets

    def _merge_strategy_into_config(self, strategy_config: Dict[str, Any], new_markets: Dict[str, Set[str]]):
        """Merge a new strategy and its markets into the config file."""
        config_path = Path(self.config.config_file_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read current config
        with open(config_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ValueError("Config file is empty or invalid")

        # Merge markets
        if 'markets' not in yaml_data:
            yaml_data['markets'] = {}

        for exchange, pairs in new_markets.items():
            if exchange not in yaml_data['markets']:
                yaml_data['markets'][exchange] = []

            # Add new pairs if not already present
            existing_pairs = set(yaml_data['markets'][exchange])
            for pair in pairs:
                if pair not in existing_pairs:
                    yaml_data['markets'][exchange].append(pair)
                    self.logger().info(f"Added market pair: {exchange}:{pair}")

        # Add strategy to arbitrage_m_strategies
        if 'arbitrage_m_strategies' not in yaml_data:
            yaml_data['arbitrage_m_strategies'] = []

        # Check if strategy with same name already exists
        existing_names = [s.get('name') for s in yaml_data['arbitrage_m_strategies']]
        if strategy_config['name'] in existing_names:
            self.logger().warning(f"Strategy '{strategy_config['name']}' already exists, replacing it")
            yaml_data['arbitrage_m_strategies'] = [
                s for s in yaml_data['arbitrage_m_strategies']
                if s.get('name') != strategy_config['name']
            ]

        yaml_data['arbitrage_m_strategies'].append(strategy_config)
        self.logger().info(f"Added strategy '{strategy_config['name']}' to config")

        # Write updated config back to file
        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)

        self.logger().info(f"Config file updated: {config_path}")

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