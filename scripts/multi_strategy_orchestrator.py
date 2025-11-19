"""
Multi-Strategy Orchestrator for V1 Strategies with Shared Websocket Connections

This script allows running multiple V1 strategies (like arbitrage_l and arbitrage_m) simultaneously
while sharing websocket connections to the same exchanges. This provides:

1. Resource Efficiency: One websocket connection per exchange instead of one per strategy
2. Rate Limit Optimization: All strategies share the same connection pool
3. Memory Savings: Order books and market data cached once and shared
4. Compatibility: Works with existing V1 Cython strategies without modification
5. Runtime Control: Pause/resume individual strategies without stopping the bot
6. Coordinated Initialization: Single readiness check for 40-50 strategies (vs. 40-50 individual checks)

Architecture:
-----------
MultiStrategyOrchestrator (ScriptStrategyBase)
├── connectors: Dict[str, ConnectorBase]     ← Shared connector pool
│   └── Each connector manages ONE websocket connection
├── strategies: List[V1StrategyInstance]     ← Multiple V1 strategy instances
│   ├── arbitrage_l/arbitrage_m instance 1
│   ├── arbitrage_l/arbitrage_m instance 2
│   └── ... (each adds event listeners to shared connectors)
└── on_tick() → tick all strategies independently

How Websocket Sharing Works:
---------------------------
1. ConnectorManager creates ONE connector instance per exchange
2. Multiple V1 strategies call c_add_markets() on the SAME connector
3. Each strategy registers its own event listeners
4. Connector broadcasts events to all registered listeners
5. Strategies operate independently, sharing the underlying connection

Websocket Subscription Limits:
-----------------------------
Different exchanges have limits on subscriptions per websocket connection:
- MEXC: 30 subscriptions per connection (2 per trading pair: trade + depth)
- BingX: 200 subscriptions per connection
- OKX: 240 subscriptions per hour

The orchestrator automatically handles these limits:
- MEXC: Uses MexcWebsocketSubscriptionManager for multiple connections when needed
- Other exchanges: Enhanced connectors respect their specific limits
- Automatic connection distribution when limits are exceeded
- Graceful degradation with warnings when limits cannot be accommodated

Critical Implementation Details:
-------------------------------
FIXED - Lifecycle Management:
- The orchestrator now properly calls ScriptStrategyBase.__init__() to register connectors with clock
- This is CRITICAL for network monitoring - without it, websocket disconnections are never detected
- Strategies are initialized in __init__() but started later in start() when clock is available
- Each strategy gets proper c_start(clock, timestamp) call with clock reference
- Strategies are stopped with c_stop(clock) using the SAME clock from start()

Event Listener Pattern:
- Orchestrator MUST call add_markets() to register connectors with clock for network monitoring
- This enables websocket disconnection detection via connector.network_status updates
- V1 strategies also register listeners via c_add_markets() during init_params()
- Multiple listeners per connector are supported via observer pattern

Clock Management:
- Orchestrator is registered with clock (only orchestrator, not individual strategies)
- Orchestrator's start() is called by clock → starts all V1 strategies
- Orchestrator's tick() implements custom per-strategy readiness checking
- Orchestrator's on_stop() is called by clock → stops all V1 strategies

FIXED - Websocket Reconnection Handling:
----------------------------------------
- CRITICAL FIX #1: Orchestrator must call add_markets() for event listeners (done in __init__)
- CRITICAL FIX #2: Orchestrator must register connectors with clock for network monitoring (done in start())
- Without clock registration, connector._check_network_loop() never starts and network_status never updates
- CRITICAL FIX #3: ScriptStrategyBase.tick() only calls on_tick() when ALL connectors are ready
- This breaks multi-exchange orchestrators where one exchange down stops all strategies
- Orchestrator now implements per-strategy readiness checking in tick() and on_tick()
- Each strategy only pauses when ITS specific connectors disconnect
- Unaffected strategies continue trading during partial disconnections
- Proper reconnection logging shows when individual strategies resume/pause

Optimizations for 40-50 Strategies (Reconnection Performance):
---------------------------------------------------------------
1. Coordinated Initialization:
   - Single coordinated initialization when ALL connectors first become ready
   - Coordinated re-initialization after full reconnection events
   - Prevents redundant startup logic across 40-50 strategies

2. Individual Strategy Position Balancing:
   - Each strategy handles its own position balancer logic in orchestrated mode
   - Position balancer check runs when strategy first detects markets are ready
   - No orchestrator-level coordination needed (strategies are self-contained)

3. Optimized Readiness Checking:
   - Individual strategies use orchestrated_mode for streamlined readiness checks
   - Reduces redundant logging while preserving connectivity detection
   - Maintains per-strategy readiness handling for partial disconnections

4. Framework Pattern Compliance:
   - Properly leverages ScriptStrategyBase.tick() readiness pattern
   - Follows hummingbot framework best practices for connector lifecycle
   - Minimal changes to existing arbitrage_m strategy

Strategy Types:
--------------
arbitrage_l (default): Uses limit orders for precision and better execution
arbitrage_m: Uses market orders for immediate execution

Example Usage:
-------------
See scripts/examples/conf_multi_arbitrage_m_*.yml for configurations

The orchestrator automatically selects the strategy type based on the 'strategy_type' field:
- If not specified: defaults to 'arbitrage_l' (limit order strategy)
- If set to 'arbitrage_m': uses market order strategy

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
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import yaml
from pydantic import BaseModel, Field
from hummingbot.client.config.config_data_types import BaseClientModel

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.arbitrage_m.arbitrage import ArbitrageMStrategy
from hummingbot.strategy.arbitrage_m.arbitrage_market_pair import ArbitrageMMarketPair
from hummingbot.strategy.arbitrage_l.arbitrage import ArbitrageLStrategy
from hummingbot.strategy.arbitrage_l.arbitrage_market_pair import ArbitrageLMarketPair
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.script_strategy_base import ScriptConfigBase, ScriptStrategyBase
from hummingbot.strategy.strategy_base import StrategyBase


logger = None

# Export convenience functions for easy import
__all__ = [
    'pause', 'resume', 'list_arb', 'pause_all', 'resume_all', 'help_arb', 'remove',
    'enable_buyin', 'disable_buyin', 'enable_selloff', 'disable_selloff',
    'MultiStrategyOrchestrator'
]

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


def enable_buyin(identifier: str) -> bool:
    """
    Enable buy-in mode for a strategy's position balancer by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> enable_buyin("arb_bsx_gate_bitmart")  # By full name
        >>> enable_buyin("BSX")                    # By token symbol
    """
    orchestrator = _get_orchestrator()
    return orchestrator.enable_buyin_by_identifier(identifier)


def disable_buyin(identifier: str) -> bool:
    """
    Disable buy-in mode for a strategy's position balancer by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> disable_buyin("arb_bsx_gate_bitmart")
        >>> disable_buyin("BSX")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.disable_buyin_by_identifier(identifier)


def enable_selloff(identifier: str) -> bool:
    """
    Enable sell-off mode for a strategy's position balancer by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> enable_selloff("arb_bsx_gate_bitmart")
        >>> enable_selloff("BSX")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.enable_selloff_by_identifier(identifier)


def disable_selloff(identifier: str) -> bool:
    """
    Disable sell-off mode for a strategy's position balancer by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> disable_selloff("arb_bsx_gate_bitmart")
        >>> disable_selloff("BSX")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.disable_selloff_by_identifier(identifier)


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
    _last_ready_state: bool = True  # Track ready transitions for reconnection logging


class ArbitrageMInstanceConfig(BaseModel):
    """
    Configuration for a single arbitrage strategy instance (supports both arbitrage_m and arbitrage_l).

    DEFAULT VALUES (automatically applied if not specified in config):
    ================================================================
    Strategy Type:
      - strategy_type: "arbitrage_l" (limit orders, default)

    Profitability:
      - min_profitability: 0.5 (%)

    Position Balancer (arbitrage_l uses this to manage inventory):
      Buy-in (acquire assets when below target):
        - buy_in_enabled: False
        - buy_in_target_usd: 1000.0 (USD, minimum asset value)
        - buy_in_spread_pct: 0.1 (%, spread below top bid for buy limit orders)

      Sell-off (reduce assets when above target):
        - sell_off_enabled: False
        - sell_off_target_usd: 2000.0 (USD, maximum asset value)
        - sell_off_spread_pct: 0.1 (%, spread above top ask for sell limit orders)

      Order Management:
        - position_balancer_refresh_interval: 10.0 (seconds, how often to refresh limit orders)
        - position_balancer_order_size_usd: 100.0 (USD, maximum order size per position balancer order)

    Timing (arbitrage_l defaults):
      - status_report_interval: 60.0 (seconds)
      - next_trade_delay_interval: 2.0 (seconds)
      - order_timeout: 180.0 (seconds)
      - filled_order_timeout: 3600.0 (seconds, arbitrage_l only)

    Conversion Rates:
      - use_oracle_conversion_rate: False
      - secondary_to_primary_base_conversion_rate: 1.0
      - secondary_to_primary_quote_conversion_rate: 1.0

    Additional Markets:
      - additional_markets: [] (empty list)
    """

    # Required fields
    name: str = Field(..., description="Unique name for this strategy instance")
    primary_market: str = Field(..., description="Primary exchange name (e.g., 'binance')")
    secondary_market: str = Field(..., description="Secondary exchange name (e.g., 'kucoin')")
    primary_trading_pair: str = Field(..., description="Primary trading pair (e.g., 'BTC-USDT')")
    secondary_trading_pair: str = Field(..., description="Secondary trading pair (e.g., 'BTC-USDT')")

    # Strategy type selection
    strategy_type: str = Field(
        default="arbitrage_l",
        description="Strategy type: 'arbitrage_l' (limit orders, default) or 'arbitrage_m' (market orders)"
    )

    # Profitability
    min_profitability: Decimal = Field(
        default=Decimal("0.5"),
        description="Minimum profitability percentage (e.g., 0.5 for 0.5%)"
    )

    # Advanced conversion options
    use_oracle_conversion_rate: bool = Field(
        default=False,
        description="Use oracle for conversion rates between trading pairs"
    )
    secondary_to_primary_base_conversion_rate: Decimal = Field(
        default=Decimal("1.0"),
        description="Conversion rate for base asset from secondary to primary"
    )
    secondary_to_primary_quote_conversion_rate: Decimal = Field(
        default=Decimal("1.0"),
        description="Conversion rate for quote asset from secondary to primary"
    )

    # Position balancer configuration - Buy-in
    buy_in_enabled: bool = Field(
        default=False,
        description="Enable buy-in to acquire assets when below target"
    )
    buy_in_target_usd: float = Field(
        default=1000.0,
        description="Target minimum USD value (buy when below this)"
    )
    buy_in_spread_pct: Union[float, str] = Field(
        default=0.1,
        description="Spread for buy limit orders: float (e.g., 0.1 = 0.1%) or 'min' (minimum tick)"
    )

    # Position balancer configuration - Sell-off
    sell_off_enabled: bool = Field(
        default=False,
        description="Enable sell-off to reduce assets when above target"
    )
    sell_off_target_usd: float = Field(
        default=2000.0,
        description="Target maximum USD value (sell when above this)"
    )
    sell_off_spread_pct: Union[float, str] = Field(
        default=0.1,
        description="Spread for sell limit orders: float (e.g., 0.1 = 0.1%) or 'min' (minimum tick)"
    )

    # Position balancer - Order management
    position_balancer_refresh_interval: float = Field(
        default=10.0,
        description="How often to cancel and replace limit orders (seconds)"
    )
    position_balancer_order_size_usd: float = Field(
        default=100.0,
        description="Maximum order size in USD per position balancer order (e.g., 100.0 = $100 max per order)"
    )

    # Timing parameters
    status_report_interval: float = Field(
        default=60.0,
        description="Interval in seconds between status reports"
    )
    next_trade_delay_interval: float = Field(
        default=2.0,
        description="Delay in seconds between trade executions"
    )
    order_timeout: float = Field(
        default=180.0,
        description="Timeout in seconds for unfilled orders"
    )
    filled_order_timeout: float = Field(
        default=3600.0,
        description="Timeout in seconds for orders with partial fills (arbitrage_l only)"
    )

    # Additional markets for cross-exchange opportunities
    additional_markets: List[str] = Field(
        default_factory=list,
        description="Additional markets as 'exchange:PAIR' (e.g., ['mexc:BTC-USDT'])"
    )


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
        # Initialize base class manually to control event listener registration
        from hummingbot.strategy.strategy_py_base import StrategyPyBase
        StrategyPyBase.__init__(self)
        
        # Set attributes that ScriptStrategyBase.__init__() would set
        self.connectors: Dict[str, ConnectorBase] = connectors
        self.config: MultiStrategyOrchestratorConfig = config
        self.ready_to_trade: bool = False
        
        # CRITICAL FIX: We must call add_markets() to register connectors with the clock
        # This starts the network monitoring loops that update connector.network_status
        # Without this, websocket disconnections are never detected!
        # Note: This will add event listeners to the orchestrator, but that's necessary for network monitoring
        self.add_markets(list(connectors.values()))

        # Storage for V1 strategy instances
        self.strategies: List[V1StrategyInstance] = []
        self._strategies_started: bool = False
        self._strategy_clock = None

        # Track initialization errors for diagnostics
        self._init_errors: List[Tuple[str, str]] = []  # (strategy_name, error_message)
        self._available_connectors: Set[str] = set(connectors.keys()) if connectors else set()

        # Orchestrator-level readiness coordination (optimization for 40-50 strategies)
        self._markets_ready_notified: bool = False

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
        self.logger().info("  control add <file>          # Add strategy from conf/strategies/ (staged; starts on restart)")
        self.logger().info("")
        self.logger().info(f"Loaded {len(self.strategies)} strateg{'y' if len(self.strategies) == 1 else 'ies'}")
        self.logger().info("=" * 70)
        self.logger().info("")

    def show_help(self):
        """Show runtime control help (can be called from console)."""
        self._show_runtime_help()

    def _validate_config_structure(self):
        """Validate configuration structure and return diagnostic information"""
        issues = []
        
        # Check if config has the required sections
        if not hasattr(self.config, 'arbitrage_m_strategies'):
            issues.append("Missing 'arbitrage_m_strategies' section in config")
        elif not self.config.arbitrage_m_strategies:
            issues.append("'arbitrage_m_strategies' section is empty")
        
        if not hasattr(self.config, 'markets'):
            issues.append("Missing 'markets' section in config")
        elif not self.config.markets:
            issues.append("'markets' section is empty")
        
        # Validate individual strategies
        if hasattr(self.config, 'arbitrage_m_strategies') and self.config.arbitrage_m_strategies:
            for i, strategy_config in enumerate(self.config.arbitrage_m_strategies):
                required_fields = ['name', 'primary_market', 'secondary_market', 
                                 'primary_trading_pair', 'secondary_trading_pair']
                
                missing_fields = []
                for field in required_fields:
                    if not hasattr(strategy_config, field) or not getattr(strategy_config, field):
                        missing_fields.append(field)
                
                if missing_fields:
                    strategy_name = getattr(strategy_config, 'name', f'strategy_{i}')
                    issues.append(f"Strategy '{strategy_name}' missing fields: {', '.join(missing_fields)}")
        
        return issues

    def _initialize_arbitrage_m_strategies(self):
        """Initialize all arbitrage_m strategy instances"""
        # First validate config structure
        config_issues = self._validate_config_structure()
        if config_issues:
            for issue in config_issues:
                self._init_errors.append(("CONFIG_VALIDATION", issue))
                self.logger().error(f"Configuration issue: {issue}")
            return
        
        for strategy_config in self.config.arbitrage_m_strategies:
            try:
                self._add_arbitrage_m_strategy(strategy_config)
            except Exception as e:
                error_msg = str(e)
                self._init_errors.append((strategy_config.name, error_msg))
                self.logger().error(f"Failed to initialize strategy '{strategy_config.name}': {e}", exc_info=True)

    def _add_arbitrage_m_strategy(self, config: ArbitrageMInstanceConfig, paused: bool = False):
        """
        Add an arbitrage strategy instance (supports both arbitrage_m and arbitrage_l).

        This method:
        1. Builds market pairs from the shared connector pool
        2. Creates an ArbitrageMStrategy or ArbitrageLStrategy instance based on config.strategy_type
        3. Initializes it with the config
        4. The strategy's c_add_markets() call registers event listeners

        Args:
            config: Strategy configuration
            paused: Whether to start the strategy in paused state (useful for runtime additions)
        """
        strategy_type = config.strategy_type.lower()
        self.logger().info(f"Adding {strategy_type} strategy: {config.name}")

        # Validate connectors exist with detailed error information
        missing_connectors = []
        if config.primary_market not in self.connectors:
            missing_connectors.append(config.primary_market)
        if config.secondary_market not in self.connectors:
            missing_connectors.append(config.secondary_market)
        
        # Check additional markets
        for additional in config.additional_markets:
            if ":" in additional:
                exchange = additional.split(":", 1)[0].lower()
                if exchange not in self.connectors:
                    missing_connectors.append(exchange)
        
        if missing_connectors:
            available = sorted(self.connectors.keys())
            raise ValueError(
                f"Missing connectors: {', '.join(set(missing_connectors))}. "
                f"Available: {', '.join(available)}"
            )

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
        # Use the appropriate market pair class based on strategy type
        if strategy_type == "arbitrage_l":
            MarketPairClass = ArbitrageLMarketPair
            StrategyClass = ArbitrageLStrategy
        elif strategy_type == "arbitrage_m":
            MarketPairClass = ArbitrageMMarketPair
            StrategyClass = ArbitrageMStrategy
        else:
            raise ValueError(f"Invalid strategy_type '{config.strategy_type}'. Must be 'arbitrage_l' or 'arbitrage_m'")

        market_pairs = []
        for i in range(len(market_tuples)):
            for j in range(len(market_tuples)):
                if i != j:
                    market_pairs.append(MarketPairClass(
                        first=market_tuples[i],
                        second=market_tuples[j]
                    ))

        # Create strategy instance
        strategy = StrategyClass()

        # Build init_params based on strategy type
        init_params = {
            "market_pairs": market_pairs,
            "min_profitability": config.min_profitability / Decimal("100"),  # Convert percentage to decimal
            "logging_options": (
                StrategyClass.OPTION_LOG_STATUS_REPORT |
                StrategyClass.OPTION_LOG_ORDER_COMPLETED |
                StrategyClass.OPTION_LOG_CREATE_ORDER
            ),
            "status_report_interval": config.status_report_interval,
            "next_trade_delay_interval": config.next_trade_delay_interval,
            "order_timeout": config.order_timeout,
            "use_oracle_conversion_rate": config.use_oracle_conversion_rate,
            "secondary_to_primary_base_conversion_rate": config.secondary_to_primary_base_conversion_rate,
            "secondary_to_primary_quote_conversion_rate": config.secondary_to_primary_quote_conversion_rate,
            "hb_app_notification": True,
            # Position balancer - buy-in configuration
            "buy_in_enabled": config.buy_in_enabled,
            "buy_in_target_usd": config.buy_in_target_usd,
            "buy_in_spread_pct": config.buy_in_spread_pct,
            # Position balancer - sell-off configuration
            "sell_off_enabled": config.sell_off_enabled,
            "sell_off_target_usd": config.sell_off_target_usd,
            "sell_off_spread_pct": config.sell_off_spread_pct,
            # Position balancer - order management
            "position_balancer_refresh_interval": config.position_balancer_refresh_interval,
            "position_balancer_order_size_usd": config.position_balancer_order_size_usd,
            "orchestrated_mode": True,  # Enable orchestrated mode for coordinated readiness checking
        }

        # Add filled_order_timeout for arbitrage_l only
        if strategy_type == "arbitrage_l":
            init_params["filled_order_timeout"] = config.filled_order_timeout

        # Initialize strategy parameters
        # This will call c_add_markets() which registers event listeners on shared connectors
        # CRITICAL: Set orchestrated_mode=True to skip redundant readiness checks
        strategy.init_params(**init_params)

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
        Start the orchestrator and all V1 strategies.
        
        CRITICAL: We must also register connectors with the clock to start their network monitoring.
        The orchestrator's add_markets() call only adds event listeners, not clock registration.
        """
        self._strategy_clock = clock
        
        # CRITICAL FIX: Register all connectors with the clock for network monitoring
        # Without this, connector._check_network_loop() never starts!
        registered_count = 0
        for connector_name, connector in self.connectors.items():
            try:
                # Check if connector is already registered with clock
                if connector not in clock.child_iterators:
                    self.logger().info(f"Registering connector {connector_name} with clock for network monitoring")
                    clock.add_iterator(connector)
                    registered_count += 1
                    
                    # Verify the network monitoring task was created
                    if hasattr(connector, '_check_network_task') and connector._check_network_task:
                        self.logger().debug(f"Network monitoring task created for {connector_name}")
                    else:
                        self.logger().warning(f"Network monitoring task not found for {connector_name}")
                else:
                    self.logger().debug(f"Connector {connector_name} already registered with clock")
            except Exception as e:
                self.logger().error(f"Failed to register connector {connector_name} with clock: {e}", exc_info=True)
        
        if registered_count > 0:
            self.logger().info(f"Registered {registered_count} connectors with clock - network monitoring started")
        else:
            self.logger().info("All connectors already registered with clock")
        
        self._start_all_strategies_if_needed()

    def tick(self, timestamp: float):
        """
        Custom tick implementation that handles per-strategy readiness instead of requiring ALL connectors to be ready.
        
        CRITICAL FIX: ScriptStrategyBase.tick() only calls on_tick() when ALL connectors are ready,
        which breaks multi-exchange orchestrators. We need per-strategy readiness checking.
        """
        # Check if ALL connectors are ready (for coordinated initialization only)
        all_connectors_ready = all(
            ex.ready and ex.network_status == NetworkStatus.CONNECTED
            for ex in self.connectors.values()
        )

        # Handle initial coordinated initialization
        if not self._markets_ready_notified and all_connectors_ready:
            # First time ALL connectors are ready - run coordinated init
            self._on_markets_ready(timestamp)
            self._markets_ready_notified = True

        # Track full disconnection/reconnection for coordinated re-init
        prev_global_ready = getattr(self, '_prev_all_ready', False)
        self._prev_all_ready = all_connectors_ready

        if all_connectors_ready and not prev_global_ready and self._markets_ready_notified:
            # Full reconnection after full disconnection
            self.logger().info("All connectors reconnected - running coordinated re-initialization")
            self._on_markets_ready(timestamp)

        # CRITICAL: Don't use ScriptStrategyBase.tick() - it requires ALL connectors to be ready
        # Instead, implement per-strategy readiness checking that allows partial operation
        
        # Update orchestrator-level ready_to_trade for status display
        self.ready_to_trade = any(ex.ready for ex in self.connectors.values())
        
        # Always call on_tick() - individual strategies will check their own connector readiness
        # This allows strategies with working connectors to continue during partial disconnections
        self.on_tick()

    def _on_markets_ready(self, timestamp: float):
        """
        Coordinated initialization when markets become ready.

        This performs ONE-TIME initialization for all strategies:
        1. Log market ready status (once for all strategies)
        2. Perform coordinated initialization (single wallet query)
        3. Notify strategies that markets are ready

        This replaces 40-50 individual strategy readiness checks and position balancer scans.
        """
        self.logger().info("=" * 70)
        self.logger().info("MARKETS READY - Coordinated Initialization")
        self.logger().info("=" * 70)

        # Show connector status
        for name, connector in self.connectors.items():
            status = connector.status_dict
            self.logger().info(f"{name}: {status}")

        # Position balancer coordination is handled by individual strategies in orchestrated mode
        # Each strategy calls its own position balancer check when it first detects markets are ready
        # arbitrage_l: calls self._position_balancer.c_scan_and_mark_completion()
        # No orchestrator-level coordination needed - strategies handle it internally

        self.logger().info("=" * 70)
        self.logger().info("Markets ready. Trading started.")
        self.logger().info("=" * 70)

    def _start_all_strategies_if_needed(self):
        """
        Start all V1 strategies with the clock.

        Note: Strategy init_params() already called c_add_markets(), so event listeners
        are registered. We just need to call start() to activate them.
        """
        if self._strategies_started:
            return

        self.logger().info(f"Starting {len(self.strategies)} V1 strategies...")
        for strategy_instance in self.strategies:
            try:
                self.logger().info(f"Starting strategy: {strategy_instance.name}")
                # Simply call start() - markets were already added in init_params()
                if hasattr(strategy_instance.strategy, "start"):
                    strategy_instance.strategy.start(self._strategy_clock)
            except Exception as e:
                self.logger().error(
                    f"Error starting strategy '{strategy_instance.name}': {e}",
                    exc_info=True
                )
        self._strategies_started = True
        self.logger().info("All strategies started successfully")


    def _get_strategy_connectors(self, strategy_instance: V1StrategyInstance) -> Set[ConnectorBase]:
        """Get the unique set of connectors used by a strategy."""
        connectors = set()
        for market_pair in strategy_instance.market_pairs:
            connectors.add(market_pair.market)
        return connectors

    def _is_strategy_ready(self, strategy_instance: V1StrategyInstance) -> bool:
        """
        Check if a strategy's specific connectors are ready.
        
        This enables partial disconnection handling - a strategy only pauses
        if ITS connectors are down, not if unrelated connectors disconnect.
        """
        strategy_connectors = self._get_strategy_connectors(strategy_instance)
        
        # Strategy is ready if ALL of its connectors are ready and connected
        return all(
            connector.ready and connector.network_status == NetworkStatus.CONNECTED
            for connector in strategy_connectors
        )

    def on_tick(self):
        """
        Main tick function with per-strategy readiness checking.
        
        CRITICAL: This implements the proper websocket reconnection handling that
        ScriptStrategyBase.tick() would do, but on a per-strategy basis instead of
        requiring ALL connectors to be ready.
        
        Each strategy is ticked independently and only pauses if ITS specific
        connectors are not ready. This allows unaffected strategies to continue
        trading during partial disconnections.
        """
        # Get current timestamp from TimeIterator property
        current_timestamp = self.current_timestamp

        # Tick each strategy independently with per-strategy readiness check
        for strategy_instance in self.strategies:
            # Skip if manually paused
            if strategy_instance.paused:
                continue

            # Check if THIS strategy's connectors are ready
            strategy_ready = self._is_strategy_ready(strategy_instance)
            
            # Track ready state transitions for reconnection logging
            prev_ready = getattr(strategy_instance, '_last_ready_state', True)
            strategy_instance._last_ready_state = strategy_ready
            
            # Log reconnection events
            if strategy_ready != prev_ready:
                if strategy_ready:
                    # Transition: not ready -> ready (reconnection)
                    self.logger().info(
                        f"Strategy '{strategy_instance.name}' connectors reconnected - resuming trading"
                    )
                else:
                    # Transition: ready -> not ready (disconnection)
                    affected_connectors = [
                        c.name for c in self._get_strategy_connectors(strategy_instance)
                        if not (c.ready and c.network_status == NetworkStatus.CONNECTED)
                    ]
                    self.logger().warning(
                        f"Strategy '{strategy_instance.name}' paused - connectors disconnected: "
                        f"{', '.join(affected_connectors)}"
                    )

            # Only tick if strategy's connectors are ready
            if not strategy_ready:
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
        Clean shutdown of all strategies.
        
        Note: We do NOT stop or restart connectors. Connectors manage their own
        lifecycle and reconnection independently. We only stop strategies and let
        them unregister their event listeners.
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
            self.logger().info("All strategies stopped")

        # Do NOT call any connector methods or cancel orders via TradingCore
        # This could interfere with connector reconnection logic
        # TradingCore will handle order cancellation if needed
        
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
        
        # Note: We do NOT cancel open orders on pause
        # Let the strategy's timeout logic handle any pending orders
        # Canceling orders could interfere with connector state during reconnection
        
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
        def _books_ready(si: V1StrategyInstance) -> Tuple[bool, List[str]]:
            missing: List[str] = []
            for mt in si.market_pairs:
                try:
                    mt.market.get_order_book(mt.trading_pair)
                except Exception:
                    try:
                        ex_name = getattr(mt.market, "name", "?")
                    except Exception:
                        ex_name = "?"
                    missing.append(f"{ex_name}:{mt.trading_pair}")
            return (len(missing) == 0), missing

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
        # Safety check: ensure order books exist before resuming to avoid runtime errors
        ready, missing = _books_ready(strategy_instance)
        if not ready:
            self.logger().warning(
                f"Cannot resume '{strategy_name}' yet. Missing order books: {', '.join(missing[:5])}"
                f"{'...' if len(missing) > 5 else ''}. Will remain PAUSED."
            )
            return False
        else:
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

    # Position Balancer Control Methods

    def enable_buyin_by_identifier(self, identifier: str) -> bool:
        """
        Enable buy-in mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.enable_buyin(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.enable_buyin(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def disable_buyin_by_identifier(self, identifier: str) -> bool:
        """
        Disable buy-in mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.disable_buyin(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.disable_buyin(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def enable_selloff_by_identifier(self, identifier: str) -> bool:
        """
        Enable sell-off mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.enable_selloff(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.enable_selloff(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def disable_selloff_by_identifier(self, identifier: str) -> bool:
        """
        Disable sell-off mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        # First try exact name match
        strategy_instance = next(
            (s for s in self.strategies if s.name == identifier),
            None
        )

        if strategy_instance:
            return self.disable_selloff(identifier)

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return self.disable_selloff(strategy_instance.name)

        # Not found by either method
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return False

    def enable_buyin(self, strategy_name: str) -> bool:
        """
        Enable buy-in mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

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

        # Check if strategy has position balancer
        has_attr = hasattr(strategy_instance, '_position_balancer')
        is_none = strategy_instance._position_balancer is None if has_attr else True
        self.logger().info(f"Position balancer check for '{strategy_name}': has_attr={has_attr}, is_none={is_none}")
        if not has_attr or is_none:
            self.logger().error(f"Strategy '{strategy_name}' does not have position balancer enabled")
            return False

        # Enable buy-in
        strategy_instance._position_balancer.enable_buy_in()
        return True

    def disable_buyin(self, strategy_name: str) -> bool:
        """
        Disable buy-in mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

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

        # Check if strategy has position balancer
        if not hasattr(strategy_instance, '_position_balancer') or strategy_instance._position_balancer is None:
            self.logger().error(f"Strategy '{strategy_name}' does not have position balancer enabled")
            return False

        # Disable buy-in
        strategy_instance._position_balancer.disable_buy_in()
        return True

    def enable_selloff(self, strategy_name: str) -> bool:
        """
        Enable sell-off mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

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

        # Check if strategy has position balancer
        if not hasattr(strategy_instance, '_position_balancer') or strategy_instance._position_balancer is None:
            self.logger().error(f"Strategy '{strategy_name}' does not have position balancer enabled")
            return False

        # Enable sell-off
        strategy_instance._position_balancer.enable_sell_off()
        return True

    def disable_selloff(self, strategy_name: str) -> bool:
        """
        Disable sell-off mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

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

        # Check if strategy has position balancer
        if not hasattr(strategy_instance, '_position_balancer') or strategy_instance._position_balancer is None:
            self.logger().error(f"Strategy '{strategy_name}' does not have position balancer enabled")
            return False

        # Disable sell-off
        strategy_instance._position_balancer.disable_sell_off()
        return True

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

        # Stop the strategy if it's running
        if self._strategies_started and self._strategy_clock is not None:
            try:
                self.logger().info(f"Stopping strategy before removal: {strategy_name}")
                if hasattr(strategy_instance.strategy, "stop"):
                    strategy_instance.strategy.stop(self._strategy_clock)
            except Exception as e:
                self.logger().warning(f"Error stopping strategy '{strategy_name}': {e}")
        
        # Note: Strategy.stop() will unregister event listeners from connectors
        # We don't cancel orders manually as this could interfere with connector state

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
        Add a strategy from a single-strategy config file (staged; no runtime activation).

        This will:
        1. Load the single-strategy config file from conf/strategies/
        2. Convert it to multi-strategy format
        3. Merge markets into the current config
        4. Add strategy to arbitrage_m_strategies list
        5. Save the updated config file

        Note: The new strategy is only written to the config and will start on the next orchestrator start.

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

            # Staged only: do not modify live connectors or strategies.
            strategy_name = strategy_config['name']
            self.logger().info(f"Strategy '{strategy_name}' staged in config. It will start on next orchestrator start.")
            self.logger().info("No runtime market subscriptions or activation attempted.")
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
                # Position balancer - buy-in configuration
                'buy_in_enabled': single_config.get('buy_in_enabled', False),
                'buy_in_target_usd': single_config.get('buy_in_target_usdt', single_config.get('buy_in_target_usd', 1000.0)),
                'buy_in_spread_pct': single_config.get('buy_in_spread_pct', 0.1),
                # Position balancer - sell-off configuration (new, defaults to disabled)
                'sell_off_enabled': single_config.get('sell_off_enabled', False),
                'sell_off_target_usd': single_config.get('sell_off_target_usd', 2000.0),
                'sell_off_spread_pct': single_config.get('sell_off_spread_pct', 0.1),
                # Position balancer - order management
                'position_balancer_refresh_interval': single_config.get('position_balancer_refresh_interval', 10.0),
                'position_balancer_order_size_usd': single_config.get('position_balancer_order_size_usd', 100.0),
                # Timing parameters
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

        Shows per-strategy status even during partial disconnections, since
        unaffected strategies may still be trading.
        """
        # Check if we've completed initial startup
        if not self._markets_ready_notified:
            # Initial startup - still waiting for first coordinated initialization
            not_ready = [name for name, c in self.connectors.items()
                        if not (c.ready and c.network_status == NetworkStatus.CONNECTED)]
            if not_ready:
                return "\n".join(["Waiting for connectors to initialize:"] + [f"  {n}" for n in not_ready])
            return "Market connectors are initializing..."

        lines = []

        # Connector Status Summary (show which connectors are down during partial disconnection)
        all_connectors_ready = all(
            c.ready and c.network_status == NetworkStatus.CONNECTED
            for c in self.connectors.values()
        )

        # Strategy Status Summary
        # During normal operation: simple count of non-paused strategies
        # During partial disconnection: show which connectors are down
        if not all_connectors_ready:
            # Partial disconnection - show detailed breakdown
            not_ready = [
                name for name, c in self.connectors.items()
                if not (c.ready and c.network_status == NetworkStatus.CONNECTED)
            ]
            lines.append(f"\n⚠ Partial Disconnection - Connectors down: {', '.join(not_ready)}")
            lines.append(f"  (Unaffected strategies continue trading)")

            # Count strategies by their actual readiness state during partial disconnection
            running_count = sum(1 for s in self.strategies
                               if not s.paused and self._is_strategy_ready(s))
            paused_by_disconnect = sum(1 for s in self.strategies
                                       if not s.paused and not self._is_strategy_ready(s))
            manually_paused = sum(1 for s in self.strategies if s.paused)

            lines.append(f"\nStrategies: {running_count} trading, "
                        f"{paused_by_disconnect} paused (connector down), "
                        f"{manually_paused} paused (manual)")
        else:
            # Normal operation - use original simple counting
            running_count = sum(1 for s in self.strategies if not s.paused)
            paused_count = sum(1 for s in self.strategies if s.paused)

            # Diagnostic: check if strategies list is empty
            if len(self.strategies) == 0:
                lines.append(f"\n⚠ No strategies loaded!")
                lines.append(f"Config file: {self.config.config_file_path if hasattr(self.config, 'config_file_path') else 'Unknown'}")
                
                # Show configuration diagnostics
                config_strategies = getattr(self.config, 'arbitrage_m_strategies', [])
                lines.append(f"Strategies in config: {len(config_strategies)}")
                
                if config_strategies and self._init_errors:
                    lines.append("\nInitialization errors:")
                    for strategy_name, error_msg in self._init_errors:
                        # Truncate long error messages
                        short_error = error_msg[:100] + "..." if len(error_msg) > 100 else error_msg
                        lines.append(f"  • {strategy_name}: {short_error}")
                
                # Show connector diagnostics
                if self._init_errors:
                    required_connectors = set()
                    for strategy_config in config_strategies:
                        required_connectors.add(getattr(strategy_config, 'primary_market', None))
                        required_connectors.add(getattr(strategy_config, 'secondary_market', None))
                    required_connectors.discard(None)
                    
                    missing_connectors = required_connectors - self._available_connectors
                    if missing_connectors:
                        lines.append(f"\nMissing connectors: {', '.join(sorted(missing_connectors))}")
                        lines.append(f"Available connectors: {', '.join(sorted(self._available_connectors))}")
                
                if not config_strategies:
                    lines.append("\nExpected format: 'arbitrage_m_strategies' list in YAML")
                elif not self._init_errors:
                    lines.append("\nNo initialization errors detected. Check logs for details.")
            else:
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

            # Determine strategy state
            if strategy_instance.paused:
                # Manually paused - always show this
                best_prof_str = "PAUSED"
            elif not all_connectors_ready and not self._is_strategy_ready(strategy_instance):
                # Partial disconnection AND this strategy's connectors are down
                affected_connectors = [
                    c.name for c in self._get_strategy_connectors(strategy_instance)
                    if not (c.ready and c.network_status == NetworkStatus.CONNECTED)
                ]
                best_prof_str = f"PAUSED ({', '.join(affected_connectors)} down)"
            else:
                # Normal operation or strategy's connectors are ready - show profitability
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
            lines.append("\nPosition Balancer active:")
            for name, blines in buyin_sections:
                lines.append(f"  {name}:")
                lines.extend([f"    {ln}" for ln in blines])

        # Pending Orders (aggregated across all strategies)
        pending_orders_info = self._get_pending_orders_summary()
        if pending_orders_info:
            lines.append("\nPending Orders:")
            lines.extend([f"  {line}" for line in pending_orders_info])

        # Only show connectors not ready; omit when all ready
        not_ready = [name for name, c in self.connectors.items() if not getattr(c, 'ready', False)]
        if not_ready:
            lines.append("\nConnectors not ready:")
            for n in not_ready:
                lines.append(f"  {n}")

        return "\n".join(lines)

    def _get_pending_orders_summary(self) -> List[str]:
        """
        Collect and format pending orders from all strategies.
        Returns a list of formatted lines showing pending orders by market.
        """
        from collections import defaultdict

        # Collect all pending orders from all strategies
        # Format: {(exchange, trading_pair, strategy_name): [(order_type, price, amount, order_id), ...]}
        orders_by_market = defaultdict(list)

        for strategy_instance in self.strategies:
            try:
                strategy = strategy_instance.strategy
                strategy_name = strategy_instance.name

                # Get limit orders (arbitrage_l primarily uses these)
                if hasattr(strategy, 'tracked_limit_orders'):
                    for market, limit_order in strategy.tracked_limit_orders:
                        try:
                            trading_pair = limit_order.trading_pair
                            exchange = market.name
                            order_type = "BUY" if limit_order.is_buy else "SELL"
                            price = float(limit_order.price)
                            amount = float(limit_order.quantity)
                            order_id = limit_order.client_order_id

                            orders_by_market[(exchange, trading_pair, strategy_name)].append(
                                (order_type, price, amount, order_id)
                            )
                        except Exception as e:
                            self.logger().debug(f"Error processing limit order: {e}")

                # Get market orders (arbitrage_m may use these)
                if hasattr(strategy, 'tracked_market_orders'):
                    for market, market_order in strategy.tracked_market_orders:
                        try:
                            trading_pair = market_order.trading_pair
                            exchange = market.name
                            order_type = "BUY" if market_order.is_buy else "SELL"
                            amount = float(market_order.amount)
                            order_id = market_order.order_id

                            orders_by_market[(exchange, trading_pair, strategy_name)].append(
                                (order_type, "MARKET", amount, order_id)
                            )
                        except Exception as e:
                            self.logger().debug(f"Error processing market order: {e}")

            except Exception as e:
                self.logger().debug(f"Error collecting orders from strategy {strategy_instance.name}: {e}")

        if not orders_by_market:
            return []

        # Format the output - show total count and breakdown by strategy
        total_orders = sum(len(orders) for orders in orders_by_market.values())
        lines = [f"Total: {total_orders} order(s)"]

        # Group by strategy for cleaner display
        by_strategy = defaultdict(list)
        for (exchange, trading_pair, strategy_name), orders in orders_by_market.items():
            by_strategy[strategy_name].append((exchange, trading_pair, orders))

        for strategy_name in sorted(by_strategy.keys()):
            markets = by_strategy[strategy_name]
            strategy_total = sum(len(orders) for _, _, orders in markets)
            lines.append(f"  {strategy_name}: {strategy_total} order(s)")

            for exchange, trading_pair, orders in sorted(markets):
                # Group buys and sells for compact display
                buys = [(p, a, oid) for ot, p, a, oid in orders if ot == "BUY"]
                sells = [(p, a, oid) for ot, p, a, oid in orders if ot == "SELL"]

                market_str = f"{trading_pair} ({exchange})"

                if buys and sells:
                    lines.append(f"    {market_str}: {len(buys)} BUY, {len(sells)} SELL")
                elif buys:
                    lines.append(f"    {market_str}: {len(buys)} BUY")
                elif sells:
                    lines.append(f"    {market_str}: {len(sells)} SELL")

                # Show individual order details
                for price, amount, order_id in buys:
                    short_id = order_id[-6:] if len(order_id) > 6 else order_id
                    if price == "MARKET":
                        lines.append(f"      BUY  {amount:>10.6f} @ MARKET     (#{short_id})")
                    else:
                        lines.append(f"      BUY  {amount:>10.6f} @ {price:<12.8f} (#{short_id})")

                for price, amount, order_id in sells:
                    short_id = order_id[-6:] if len(order_id) > 6 else order_id
                    if price == "MARKET":
                        lines.append(f"      SELL {amount:>10.6f} @ MARKET     (#{short_id})")
                    else:
                        lines.append(f"      SELL {amount:>10.6f} @ {price:<12.8f} (#{short_id})")

        return lines

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
        """Extract the position balancer header and asset rows from a strategy status output.
        Returns [] when position balancer is not active (no section present).
        Supports both old 'Buy-in:' and new 'Position Balancer:' formats."""
        if not status_blob:
            return []
        raw_lines = status_blob.split("\n")
        out: List[str] = []
        collecting = False
        for raw in raw_lines:
            s = raw.strip()
            # Support both old "Buy-in:" and new "Position Balancer:" formats
            if s.startswith("Buy-in:") or s.startswith("Position Balancer:"):
                # Reformat min_prof to one decimal if present (old format compatibility)
                try:
                    # Example old format: Buy-in: target=1000.000000 min_prof=2.50%
                    # Example new format: Position Balancer: buy_target=1000.000000 sell_target=2000.000000
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
        """Parse the 'best:' line and return direction + profitability (e.g., 'bybit->htx +2.1036%')."""
        if not status_blob:
            return None
        try:
            for ln in status_blob.split("\n"):
                stripped = ln.strip()
                if stripped.startswith("best:") and "->" in stripped:
                    try:
                        # Format: "best: buy-exchange1 sell-exchange2 -> +X.XXXX%"
                        # Extract: "exchange1->exchange2 +X.XXXX%"
                        parts = stripped.split("->", 1)
                        left = parts[0]  # "best: buy-exchange1 sell-exchange2 "
                        right = parts[1].strip()  # "+X.XXXX%"

                        # Extract exchanges from "buy-exchange1 sell-exchange2"
                        buy_match = re.search(r'buy-(\S+)', left)
                        sell_match = re.search(r'sell-(\S+)', left)

                        if buy_match and sell_match:
                            buy_ex = buy_match.group(1)
                            sell_ex = sell_match.group(1)
                            return f"{buy_ex}->{sell_ex} {right}"
                        else:
                            # Fallback: just return the percentage
                            return right
                    except Exception:
                        return stripped.split("->", 1)[1].strip()
        except Exception:
            return None
        return None

    def _format_min_percent(self, value) -> str:
        """Format a percent value with one decimal, keeping input flexibility (str/float/Decimal)."""
        try:
            return f"{float(value):.1f}%"
        except Exception:
            return f"{value}%"