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
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.script_strategy_base import ScriptConfigBase, ScriptStrategyBase
from hummingbot.strategy.strategy_base import StrategyBase


logger = None

# Export convenience functions for easy import
__all__ = [
    'pause', 'resume', 'list_arb', 'pause_all', 'resume_all', 'help_arb', 'remove',
    'enable_buyin', 'disable_buyin', 'enable_selloff', 'disable_selloff', 'clean',
    'set_min_profitability', 'add_market', 'remove_market', 'create',
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


def clean(identifier: str) -> bool:
    """
    Set sell-off target to 0 and enable sell-off (sell entire position).

    Args:
        identifier: Full strategy name or token symbol

    Returns:
        True if successful

    Examples:
        >>> clean("arb_bsx_gate_bitmart")
        >>> clean("BSX")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.clean_by_identifier(identifier)


def set_min_profitability(identifier: str, value: float) -> bool:
    """
    Set minimum profitability for a strategy by name or token symbol.

    Args:
        identifier: Full strategy name or token symbol
        value: New minimum profitability percentage (e.g., 1.5 for 1.5%)

    Returns:
        True if successful

    Examples:
        >>> set_min_profitability("arb_bsx_gate_bitmart", 1.5)
        >>> set_min_profitability("BSX", 0.5)
    """
    orchestrator = _get_orchestrator()
    return orchestrator.set_min_profitability_by_identifier(identifier, value)


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


def add_market(identifier: str, market_spec: str) -> bool:
    """
    Add a market to a strategy's additional_markets (runtime + config file).
    
    This function initiates the market addition asynchronously, including
    dynamic websocket subscription to the new trading pair.

    Args:
        identifier: Full strategy name or token symbol
        market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

    Returns:
        True if the task was scheduled successfully (actual result logs async)

    Examples:
        >>> add_market("arb_bsx_gate_bitmart", "mexc:BSX-USDT")
        >>> add_market("BSX", "htx:BSX-USDT")
    """
    import asyncio
    orchestrator = _get_orchestrator()
    
    # Schedule the async operation
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(orchestrator.add_market_by_identifier(identifier, market_spec))
        # Log result when complete
        def _log_result(future):
            try:
                result = future.result()
                if result:
                    orchestrator.logger().info(f"Market addition completed successfully: {market_spec}")
                else:
                    orchestrator.logger().warning(f"Market addition returned False: {market_spec}")
            except Exception as e:
                orchestrator.logger().error(f"Market addition failed: {e}")
        task.add_done_callback(_log_result)
        orchestrator.logger().info(f"Market addition initiated for {market_spec} (async)")
        return True
    except Exception as e:
        orchestrator.logger().error(f"Failed to schedule market addition: {e}")
        return False


def remove_market(identifier: str, market_spec: str) -> bool:
    """
    Remove a market from a strategy's additional_markets (runtime + config file).

    Note: Cannot remove primary or secondary markets, only additional_markets.

    Args:
        identifier: Full strategy name or token symbol
        market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

    Returns:
        True if successful

    Examples:
        >>> remove_market("arb_bsx_gate_bitmart", "mexc:BSX-USDT")
        >>> remove_market("BSX", "htx:BSX-USDT")
    """
    orchestrator = _get_orchestrator()
    return orchestrator.remove_market_by_identifier(identifier, market_spec)


def create(name: str, primary_spec: str, secondary_spec: str,
           min_profitability: float = 2.1,
           additional_markets: str = None) -> bool:
    """
    Create a new arbitrage strategy at runtime (always starts PAUSED).

    Use 'control resume <name>' to start trading after verifying settings.

    Args:
        name: Unique name for the strategy (e.g., 'arb_bsx_new')
        primary_spec: Primary market as 'exchange:PAIR' (e.g., 'gate:BSX-USDT')
        secondary_spec: Secondary market as 'exchange:PAIR' (e.g., 'kucoin:BSX-USDT')
        min_profitability: Minimum profitability percentage (default: 1.5%)
        additional_markets: Optional comma-separated list of additional markets

    Returns:
        True if the task was scheduled successfully (actual result logs async)

    Examples:
        >>> create("arb_bsx_new", "gate:BSX-USDT", "kucoin:BSX-USDT")
        >>> create("arb_bsx_new", "gate:BSX-USDT", "kucoin:BSX-USDT", min_profitability=2.0)
        >>> create("arb_bsx_3way", "gate:BSX-USDT", "kucoin:BSX-USDT", additional_markets="mexc:BSX-USDT")
    """
    import asyncio
    orchestrator = _get_orchestrator()

    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(orchestrator.create_strategy(
            name=name,
            primary_spec=primary_spec,
            secondary_spec=secondary_spec,
            min_profitability=min_profitability,
            additional_markets=additional_markets
        ))
        def _log_result(future):
            try:
                result = future.result()
                if result:
                    orchestrator.logger().info(f"Strategy creation completed: {name}")
                else:
                    orchestrator.logger().warning(f"Strategy creation returned False: {name}")
            except Exception as e:
                orchestrator.logger().error(f"Strategy creation failed: {e}")
        task.add_done_callback(_log_result)
        orchestrator.logger().info(f"Strategy creation initiated for {name} (async)")
        return True
    except Exception as e:
        orchestrator.logger().error(f"Failed to schedule strategy creation: {e}")
        return False

@dataclass
class V1StrategyInstance:
    """Wrapper for a V1 strategy instance with metadata"""
    strategy: StrategyBase
    name: str
    config: Dict
    market_pairs: List[MarketTradingPairTuple]
    paused: bool = False  # Runtime pause state
    _last_ready_state: bool = True  # Track ready transitions for reconnection logging
    _connectors: Optional[Set[ConnectorBase]] = None  # Cached connector set (structure only)
    
    # Health monitoring fields
    exception_count: int = 0
    last_exception_time: Optional[float] = None
    last_exception_msg: Optional[str] = None
    last_successful_tick: Optional[float] = None
    
    @property
    def connectors(self) -> Set[ConnectorBase]:
        """Get cached set of connectors used by this strategy (structure only, not state)"""
        if self._connectors is None:
            self._connectors = {mp.market for mp in self.market_pairs}
        return self._connectors


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
        - buy_in_target_usd: 1100.0 (USD, minimum asset value)
        - buy_in_spread_pct: min (%, spread below top bid for buy limit orders)

      Sell-off (reduce assets when above target):
        - sell_off_enabled: False
        - sell_off_target_usd: 3000.0 (USD, maximum asset value)
        - sell_off_spread_pct: min (%, spread above top ask for sell limit orders)

      Order Management:
        - position_balancer_refresh_interval: 60.0 (seconds, how often to refresh limit orders)
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
        default=Decimal("1.5"),
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
        default=1100.0,
        description="Target minimum USD value (buy when below this)"
    )
    buy_in_spread_pct: Union[float, str] = Field(
        default="min",
        description="Spread for buy limit orders: float (e.g., 0.1 = 0.1%) or 'min' (minimum tick)"
    )

    # Position balancer configuration - Sell-off
    sell_off_enabled: bool = Field(
        default=False,
        description="Enable sell-off to reduce assets when above target"
    )
    sell_off_target_usd: float = Field(
        default=3000.0,
        description="Target maximum USD value (sell when above this)"
    )
    sell_off_spread_pct: Union[float, str] = Field(
        default="min",
        description="Spread for sell limit orders: float (e.g., 0.1 = 0.1%) or 'min' (minimum tick)"
    )

    # Position balancer - Order management
    position_balancer_refresh_interval: float = Field(
        default=60.0,
        description="How often to cancel and replace limit orders (seconds)"
    )
    position_balancer_order_size_usd: float = Field(
        default=100.0,
        description="Maximum order size in USD per position balancer order (e.g., 100.0 = $100 max per order)"
    )

    # Hold-band guardrail — caps arb order size to keep total asset value near a target.
    # Disabled by default so existing configs load without change.
    hold_target_enabled: bool = Field(
        default=False,
        description="Enable hold-band guardrail (caps arb size to keep total holding near target)"
    )
    hold_target_usd: float = Field(
        default=1100.0,
        description="Centre of the acceptable holding band in USD (e.g. 1100 → aim to hold ~$1100 worth)"
    )
    hold_band_usd: float = Field(
        default=150.0,
        description="Half-width of the band in USD (target ± band = acceptable range, e.g. 150 → [950, 1250])"
    )

    # Runtime pause state — persisted so a paused strategy stays paused across restarts.
    # Disabled by default so existing configs load unpaused.
    paused: bool = Field(
        default=False,
        description="Whether this strategy starts paused (skips ticking until resumed)"
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

    # Exchanges to skip entirely at startup — no connector is built, so no WS connection,
    # no subscriptions and no balance polling for these. Strategies that reference a
    # disabled exchange fail-and-skip gracefully (logged to _init_errors). Use for a
    # temporary exchange outage/maintenance; clear the list to bring the exchange back.
    disabled_exchanges: List[str] = Field(
        default_factory=list,
        description="Exchange names to skip building connectors for, e.g. ['bybit']"
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
        """Initialize markets from config.

        Exchanges listed in `disabled_exchanges` are filtered out here so the framework
        (TradingCore) never builds a connector for them — no WS, no subscriptions, no
        balance polling. This is the single funnel TradingCore reads to decide which
        connectors to create, so filtering here cleanly disables an exchange end-to-end.
        """
        disabled = {e.lower() for e in (getattr(config, 'disabled_exchanges', None) or [])}
        if disabled:
            cls.markets = {ex: pairs for ex, pairs in config.markets.items()
                           if ex.lower() not in disabled}
            cls.logger().info(
                f"Disabled exchanges (no connector built): {sorted(disabled)}. "
                f"Active exchanges: {sorted(cls.markets.keys())}"
            )
        else:
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
        self._strategy_by_name: Dict[str, V1StrategyInstance] = {}  # O(1) lookup by name
        self._strategies_started: bool = False
        self._strategy_clock = None

        # Track initialization errors for diagnostics
        self._init_errors: List[Tuple[str, str]] = []  # (strategy_name, error_message)
        self._available_connectors: Set[str] = set(connectors.keys()) if connectors else set()

        # Orchestrator-level readiness coordination (optimization for 40-50 strategies)
        self._markets_ready_notified: bool = False

        # Connector readiness cache - refreshed once per tick for efficiency with 30+ strategies
        # Maps connector_name -> is_ready (bool)
        self._connector_ready_cache: Dict[str, bool] = {}

        # Strategies scheduled for automatic removal once their sell-off completes.
        # Populated by clean(); drained in on_tick() when sell is no longer active.
        self._pending_auto_remove: Set[str] = set()

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
        self.logger().info("  control add <file>          # Add strategy from conf/strategies/ (staged)")
        self.logger().info("  control create <name> <primary> <secondary>  # Create new strategy (paused)")
        self.logger().info("  control add_market <token> <exchange:PAIR>   # Add market to strategy")
        self.logger().info("  control remove_market <token> <exchange:PAIR> # Remove market from strategy")
        self.logger().info("  set_min_profitability <token> <val> # Set min profitability % (e.g., 1.5)")
        self.logger().info("")
        self.logger().info(f"Loaded {len(self.strategies)} strateg{'y' if len(self.strategies) == 1 else 'ies'}")
        self.logger().info("=" * 70)
        self.logger().info("")

    def show_help(self):
        """Show runtime control help (can be called from console)."""
        self._show_runtime_help()

    def _validate_config_structure(self):
        """Validate configuration structure and values, return diagnostic information"""
        issues = []
        
        # Check if config has the required sections
        if not hasattr(self.config, 'arbitrage_m_strategies'):
            issues.append("Missing 'arbitrage_m_strategies' section in config")
        elif not self.config.arbitrage_m_strategies:
            issues.append("'arbitrage_m_strategies' section is empty")
        
        if not hasattr(self.config, 'markets'):
            issues.append("Missing 'markets' section in config")
        elif not self.config.markets:
            issues.append("Removing 'markets' section is empty")
        
        # Validate individual strategies with enhanced checks
        if hasattr(self.config, 'arbitrage_m_strategies') and self.config.arbitrage_m_strategies:
            for i, strategy_config in enumerate(self.config.arbitrage_m_strategies):
                # Check required fields
                required_fields = ['name', 'primary_market', 'secondary_market', 
                                 'primary_trading_pair', 'secondary_trading_pair']
                
                missing_fields = []
                for field in required_fields:
                    if not hasattr(strategy_config, field) or not getattr(strategy_config, field):
                        missing_fields.append(field)
                
                if missing_fields:
                    strategy_name = getattr(strategy_config, 'name', f'strategy_{i}')
                    issues.append(f"Strategy '{strategy_name}' missing fields: {', '.join(missing_fields)}")
                    continue  # Skip value validation if required fields are missing
                
                # Enhanced value validation
                strategy_name = strategy_config.name
                
                # Validate min_profitability
                if hasattr(strategy_config, 'min_profitability'):
                    min_prof = strategy_config.min_profitability
                    try:
                        min_prof_float = float(min_prof)
                        if min_prof_float < -100 or min_prof_float > 1000:
                            issues.append(
                                f"Strategy '{strategy_name}': invalid min_profitability {min_prof}% "
                                f"(must be -100 to 1000)"
                            )
                    except (ValueError, TypeError):
                        issues.append(f"Strategy '{strategy_name}': min_profitability must be numeric")
                
                # Validate buy_in_target_usd
                if hasattr(strategy_config, 'buy_in_target_usd'):
                    try:
                        if float(strategy_config.buy_in_target_usd) < 0:
                            issues.append(f"Strategy '{strategy_name}': buy_in_target_usd cannot be negative")
                    except (ValueError, TypeError):
                        issues.append(f"Strategy '{strategy_name}': buy_in_target_usd must be numeric")
                
                # Validate sell_off_target_usd
                if hasattr(strategy_config, 'sell_off_target_usd'):
                    try:
                        if float(strategy_config.sell_off_target_usd) < 0:
                            issues.append(f"Strategy '{strategy_name}': sell_off_target_usd cannot be negative")
                    except (ValueError, TypeError):
                        issues.append(f"Strategy '{strategy_name}': sell_off_target_usd must be numeric")
                
                # Validate spread values
                for spread_field in ['buy_in_spread_pct', 'sell_off_spread_pct']:
                    if hasattr(strategy_config, spread_field):
                        spread_val = getattr(strategy_config, spread_field)
                        if spread_val != 'min':
                            try:
                                spread_float = float(spread_val)
                                if spread_float < 0 or spread_float > 100:
                                    issues.append(
                                        f"Strategy '{strategy_name}': {spread_field} must be 'min' or 0-100% "
                                        f"(got {spread_val})"
                                    )
                            except (ValueError, TypeError):
                                issues.append(
                                    f"Strategy '{strategy_name}': {spread_field} must be 'min' or numeric percentage"
                                )
                
                # Validate order_timeout
                if hasattr(strategy_config, 'order_timeout'):
                    try:
                        if float(strategy_config.order_timeout) <= 0:
                            issues.append(f"Strategy '{strategy_name}': order_timeout must be positive")
                    except (ValueError, TypeError):
                        issues.append(f"Strategy '{strategy_name}': order_timeout must be numeric")
                
                # Validate position_balancer_order_size_usd
                if hasattr(strategy_config, 'position_balancer_order_size_usd'):
                    try:
                        if float(strategy_config.position_balancer_order_size_usd) <= 0:
                            issues.append(
                                f"Strategy '{strategy_name}': position_balancer_order_size_usd must be positive"
                            )
                    except (ValueError, TypeError):
                        issues.append(
                            f"Strategy '{strategy_name}': position_balancer_order_size_usd must be numeric"
                        )
        
        return issues

    def _initialize_arbitrage_m_strategies(self):
        """Initialize all arbitrage_m strategy instances with retry logic"""
        # First validate config structure
        config_issues = self._validate_config_structure()
        if config_issues:
            for issue in config_issues:
                self._init_errors.append(("CONFIG_VALIDATION", issue))
                self.logger().error(f"Configuration issue: {issue}")
            return
        
        # Exchanges with no connector built (see init_markets). Strategies that need any
        # of these are skipped up-front with a single clean log line — no retry, no
        # sleep, no traceback — instead of letting them fail 3× through the retry loop.
        disabled = {e.lower() for e in (getattr(self.config, 'disabled_exchanges', None) or [])}
        skipped_disabled = []

        for strategy_config in self.config.arbitrage_m_strategies:
            if disabled:
                needed = {strategy_config.primary_market.lower(),
                          strategy_config.secondary_market.lower()}
                for am in (strategy_config.additional_markets or []):
                    if ":" in am:
                        needed.add(am.split(":", 1)[0].lower())
                blocked = needed & disabled
                if blocked:
                    skipped_disabled.append(strategy_config.name)
                    self._init_errors.append(
                        (strategy_config.name, f"skipped: requires disabled exchange(s) {sorted(blocked)}"))
                    continue

            max_retries = 3
            success = False

            for attempt in range(max_retries):
                try:
                    self._add_arbitrage_m_strategy(strategy_config, paused=strategy_config.paused)
                    success = True
                    break  # Success, move to next strategy
                except Exception as e:
                    error_msg = str(e)
                    
                    if attempt < max_retries - 1:
                        # Retry with exponential backoff
                        wait_time = 2 ** attempt  # 1s, 2s, 4s
                        self.logger().warning(
                            f"Failed to initialize strategy '{strategy_config.name}' (attempt {attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        import time
                        time.sleep(wait_time)
                    else:
                        # Final attempt failed
                        self._init_errors.append((strategy_config.name, error_msg))
                        self.logger().error(
                            f"Failed to initialize strategy '{strategy_config.name}' after {max_retries} attempts: {e}",
                            exc_info=True
                        )

        if skipped_disabled:
            self.logger().info(
                f"Skipped {len(skipped_disabled)} strateg{'y' if len(skipped_disabled) == 1 else 'ies'} "
                f"requiring disabled exchange(s) {sorted(disabled)}: {skipped_disabled}"
            )

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
            # Hold-band guardrail: pass target > 0 only when enabled; 0.0 disables the feature
            "hold_target_usd": config.hold_target_usd if config.hold_target_enabled else 0.0,
            "hold_band_usd": config.hold_band_usd,
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
        self._strategy_by_name[config.name] = strategy_instance  # Add to index

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
        # OPTIMIZATION: Refresh connector readiness cache ONCE per tick
        # This avoids redundant connector.ready and network_status checks across 30+ strategies
        self._refresh_connector_ready_cache()

        # Check if ALL connectors are ready using cached values (for coordinated initialization only)
        all_connectors_ready = all(self._connector_ready_cache.values()) if self._connector_ready_cache else False

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
        
        # Update orchestrator-level ready_to_trade for status display (using cached values)
        self.ready_to_trade = any(self._connector_ready_cache.values()) if self._connector_ready_cache else False
        
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

        # Warm hold-band cache for all strategies that have it enabled.
        # Must run after markets are ready so balances and order books are available.
        for si in self.strategies:
            try:
                strat = si.strategy
                if (hasattr(strat, '_hold_target_usd') and
                        float(strat._hold_target_usd) > 0.0 and
                        hasattr(strat, 'refresh_hold_cache')):
                    strat.refresh_hold_cache()
            except Exception as e:
                self.logger().warning(f"Hold-band cache warm-up failed for '{si.name}': {e}")

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
        """Get the unique set of connectors used by a strategy (cached)."""
        return strategy_instance.connectors

    def _refresh_connector_ready_cache(self):
        """
        Build/refresh the connector readiness cache.
        
        Call once at the start of tick() to avoid redundant connector status checks
        across 30+ strategies. The cache maps connector_name -> is_ready (bool).
        """
        self._connector_ready_cache = {
            name: (c.ready and c.network_status == NetworkStatus.CONNECTED)
            for name, c in self.connectors.items()
        }

    def _is_connector_ready_cached(self, connector_name: str) -> bool:
        """
        Check if a connector is ready using the cached value.
        
        Falls back to direct check if cache is empty (e.g., called outside tick cycle).
        """
        if self._connector_ready_cache:
            return self._connector_ready_cache.get(connector_name, False)
        # Fallback: direct check if cache not populated
        connector = self.connectors.get(connector_name)
        if connector is None:
            return False
        return connector.ready and connector.network_status == NetworkStatus.CONNECTED

    def _is_strategy_ready(self, strategy_instance: V1StrategyInstance) -> bool:
        """
        Check if a strategy's specific connectors are ready.
        
        Uses the connector readiness cache when available for efficiency.
        This enables partial disconnection handling - a strategy only pauses
        if ITS connectors are down, not if unrelated connectors disconnect.
        """
        # Use cached readiness if available (populated at start of tick)
        if self._connector_ready_cache:
            for connector in strategy_instance.connectors:
                if not self._connector_ready_cache.get(connector.name, False):
                    return False
            return True
        
        # Fallback to direct check if cache not populated (e.g., called outside tick cycle)
        return all(
            connector.ready and connector.network_status == NetworkStatus.CONNECTED
            for connector in strategy_instance.connectors
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
                    # Use cached readiness to find affected connectors
                    affected_connectors = [
                        c.name for c in strategy_instance.connectors
                        if not self._connector_ready_cache.get(c.name, False)
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
                strategy_instance.last_successful_tick = current_timestamp
                # Reset exception count on successful tick
                if strategy_instance.exception_count > 0:
                    strategy_instance.exception_count = 0
            except Exception as e:
                # Track exception for health monitoring
                strategy_instance.exception_count += 1
                strategy_instance.last_exception_time = current_timestamp
                strategy_instance.last_exception_msg = str(e)[:200]  # Truncate long messages
                
                # Auto-pause after 10 consecutive failures to prevent runaway issues
                if strategy_instance.exception_count >= 10 and not strategy_instance.paused:
                    self.logger().error(
                        f"Strategy '{strategy_instance.name}' auto-paused after {strategy_instance.exception_count} consecutive exceptions. "
                        f"Last error: {strategy_instance.last_exception_msg}"
                    )
                    strategy_instance.paused = True
                    strategy_instance.config['paused'] = True
                    self._persist_pause_state(strategy_instance.name, True)
                
                self.logger().error(
                    f"Error ticking strategy '{strategy_instance.name}' (failure #{strategy_instance.exception_count}): {e}",
                    exc_info=True
                )

        # Auto-remove strategies whose clean-triggered sell-off has finished.
        # Sell completing sets _sell_enabled=False via c_maybe_disable_sell; we detect that here.
        if self._pending_auto_remove:
            to_remove = []
            for strategy_name in list(self._pending_auto_remove):
                instance = self._strategy_by_name.get(strategy_name)
                if instance is None:
                    # Already removed by other means — just clean up the set
                    self._pending_auto_remove.discard(strategy_name)
                    continue
                pb = getattr(instance.strategy, '_position_balancer', None)
                if pb is None or not pb.is_sell_enabled:
                    self._pending_auto_remove.discard(strategy_name)
                    to_remove.append(strategy_name)
            for strategy_name in to_remove:
                self.logger().info(
                    f"Clean sell-off complete for '{strategy_name}' — auto-removing strategy"
                )
                self.remove_strategy(strategy_name)

    async def on_stop(self):
        """
        Clean shutdown of all strategies.

        Note: We do NOT stop or restart connectors. Connectors manage their own
        lifecycle and reconnection independently. We only stop strategies and let
        them unregister their event listeners.
        """
        self.logger().info("Stopping MultiStrategyOrchestrator...")

        # Try to get a valid clock reference
        clock = self._strategy_clock
        if clock is None and hasattr(self, "clock"):
            clock = self.clock

        # Stop all V1 strategies
        # We iterate strategies regardless of _strategies_started flag because listeners
        # are added in init_params (during __init__), so we must remove them even if
        # start() was never called.
        if self.strategies and clock is not None:
            for strategy_instance in self.strategies:
                try:
                    self.logger().info(f"Stopping strategy: {strategy_instance.name}")
                    if hasattr(strategy_instance.strategy, "stop"):
                        strategy_instance.strategy.stop(clock)
                except Exception as e:
                    self.logger().error(
                        f"Error stopping strategy '{strategy_instance.name}': {e}",
                        exc_info=True
                    )

            self._strategies_started = False
            self.logger().info("All strategies stopped")
        elif not clock:
            self.logger().warning("Cannot stop strategies: No clock available")

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

    def _resolve_identifier_to_name(self, identifier: str, log_token_match: bool = True) -> Optional[str]:
        """
        Resolve an identifier (name or token) to a strategy name.

        Args:
            identifier: Full strategy name or token symbol
            log_token_match: Whether to log when a token match is found

        Returns:
            Strategy name if found, None otherwise
        """
        # First try exact name match
        strategy_instance = next((s for s in self.strategies if s.name == identifier), None)
        if strategy_instance:
            return identifier

        # If not found by name, try token lookup
        strategy_instance = self._find_strategy_by_token(identifier)
        if strategy_instance:
            if log_token_match:
                self.logger().info(f"Found strategy by token '{identifier}': {strategy_instance.name}")
            return strategy_instance.name

        # Not found
        self.logger().error(
            f"No strategy found for '{identifier}'. "
            f"Available: {[s.name for s in self.strategies]}. "
            f"Use list_arb() for details."
        )
        return None

    def _get_strategy_instance(self, strategy_name: str) -> Optional[V1StrategyInstance]:
        """
        Get strategy instance by name with validation (O(1) lookup).

        Args:
            strategy_name: The name of the strategy

        Returns:
            V1StrategyInstance if found, None otherwise
        """
        if not strategy_name or not strategy_name.strip():
            self.logger().error("Strategy name cannot be empty")
            return None

        strategy_instance = self._strategy_by_name.get(strategy_name)
        if not strategy_instance:
            self.logger().error(
                f"Strategy '{strategy_name}' not found. Available strategies: "
                f"{list(self._strategy_by_name.keys())}"
            )
            return None

        return strategy_instance

    def _get_strategy_position_balancer(self, strategy_name: str):
        """
        Get position balancer for a strategy with validation.

        Args:
            strategy_name: The name of the strategy

        Returns:
            Position balancer instance if found, None otherwise
        """
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return None

        if not hasattr(strategy_instance.strategy, '_position_balancer') or \
           strategy_instance.strategy._position_balancer is None:
            self.logger().error(f"Strategy '{strategy_name}' does not have position balancer enabled")
            return None

        return strategy_instance.strategy._position_balancer

    def pause_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Pause a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.pause_strategy(strategy_name) if strategy_name else False

    def resume_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Resume a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.resume_strategy(strategy_name) if strategy_name else False

    def pause_strategy(self, strategy_name: str) -> bool:
        """
        Pause a specific arbitrage_m strategy by name.

        Args:
            strategy_name: The name of the strategy to pause (from config)

        Returns:
            True if successful, False otherwise
        """
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False

        if strategy_instance.paused:
            self.logger().warning(f"Strategy '{strategy_name}' is already paused")
            return False

        self.logger().info(f"Pausing strategy: {strategy_name}")
        strategy_instance.paused = True
        strategy_instance.config['paused'] = True
        self._persist_pause_state(strategy_name, True)

        # Note: We do NOT cancel open orders on pause
        # Let the strategy's timeout logic handle any pending orders
        # Canceling orders could interfere with connector state during reconnection

        self.logger().info(f"Strategy '{strategy_name}' paused successfully")
        return True

    def resume_strategy(self, strategy_name: str) -> bool:
        """
        Resume a paused arbitrage_m strategy by name.

        Enhanced with comprehensive health checks:
        - Order book existence check
        - Order book freshness check (stale data detection)
        - Subscription verification (ensure trading pairs are actively subscribed)

        Args:
            strategy_name: The name of the strategy to resume

        Returns:
            True if successful, False otherwise
        """
        import time
        STALE_THRESHOLD_SECONDS = 120.0  # Order book considered stale if no updates in 2 minutes
        
        def _books_healthy(si: V1StrategyInstance) -> Tuple[bool, List[str], List[str]]:
            """
            Check order book health comprehensively.
            Returns: (all_healthy, missing_books, stale_books)
            """
            missing: List[str] = []
            stale: List[str] = []
            now = time.perf_counter()
            
            for mt in si.market_pairs:
                try:
                    ex_name = getattr(mt.market, "name", "?")
                except Exception:
                    ex_name = "?"
                
                try:
                    ob = mt.market.get_order_book(mt.trading_pair)
                    
                    # Check if order book is receiving updates (freshness check)
                    last_diff = getattr(ob, 'last_applied_diff', -1000.0)
                    if last_diff > 0 and (now - last_diff) > STALE_THRESHOLD_SECONDS:
                        stale_secs = int(now - last_diff)
                        stale.append(f"{ex_name}:{mt.trading_pair} (stale {stale_secs}s)")
                    # Check if order book has any data (empty check)
                    elif hasattr(ob, 'snapshot'):
                        snapshot = ob.snapshot
                        if snapshot and (len(snapshot[0]) == 0 and len(snapshot[1]) == 0):
                            stale.append(f"{ex_name}:{mt.trading_pair} (empty)")
                except Exception:
                    missing.append(f"{ex_name}:{mt.trading_pair}")
            
            all_healthy = (len(missing) == 0 and len(stale) == 0)
            return all_healthy, missing, stale
        
        def _verify_subscriptions(si: V1StrategyInstance) -> Tuple[bool, List[str], List[str]]:
            """
            Verify trading pairs are in active subscriptions.
            Returns: (all_subscribed, missing_subscriptions, restored_subscriptions)
            """
            missing_subs: List[str] = []
            restored: List[str] = []
            
            for mt in si.market_pairs:
                connector = mt.market
                try:
                    ex_name = getattr(connector, "name", "?")
                except Exception:
                    ex_name = "?"
                
                # Check if connector has order book tracker with data source
                if hasattr(connector, 'order_book_tracker'):
                    tracker = connector.order_book_tracker
                    if hasattr(tracker, 'data_source'):
                        data_source = tracker.data_source
                        
                        # Check if trading pair is in active subscriptions
                        if hasattr(data_source, '_trading_pairs'):
                            if mt.trading_pair not in data_source._trading_pairs:
                                # Check if it was temporarily disabled (Bybit-specific)
                                if hasattr(data_source, '_temporarily_disabled_pairs'):
                                    if mt.trading_pair in data_source._temporarily_disabled_pairs:
                                        # Try to restore it
                                        if hasattr(data_source, '_original_trading_pairs'):
                                            if mt.trading_pair in data_source._original_trading_pairs:
                                                data_source._trading_pairs.append(mt.trading_pair)
                                                data_source._temporarily_disabled_pairs.discard(mt.trading_pair)
                                                restored.append(f"{ex_name}:{mt.trading_pair}")
                                                self.logger().info(f"Restored subscription for {ex_name}:{mt.trading_pair}")
                                                continue
                                missing_subs.append(f"{ex_name}:{mt.trading_pair}")
            
            all_subscribed = (len(missing_subs) == 0)
            return all_subscribed, missing_subs, restored

        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False

        if not strategy_instance.paused:
            self.logger().warning(f"Strategy '{strategy_name}' is already running")
            return False

        self.logger().info(f"Resuming strategy: {strategy_name}")
        
        # Step 1: Verify subscriptions and restore if needed
        subs_ok, missing_subs, restored = _verify_subscriptions(strategy_instance)
        if restored:
            self.logger().info(f"Restored {len(restored)} subscriptions: {', '.join(restored)}")
        
        if not subs_ok and missing_subs:
            self.logger().warning(
                f"Cannot resume '{strategy_name}' - missing subscriptions: {', '.join(missing_subs[:5])}"
                f"{'...' if len(missing_subs) > 5 else ''}. "
                f"Subscriptions may have been lost during pause. Will remain PAUSED."
            )
            return False
        
        # Step 2: Check order book health (existence and freshness)
        healthy, missing, stale = _books_healthy(strategy_instance)
        
        if missing:
            self.logger().warning(
                f"Cannot resume '{strategy_name}' - missing order books: {', '.join(missing[:5])}"
                f"{'...' if len(missing) > 5 else ''}. Will remain PAUSED."
            )
            return False
        
        if stale:
            # Stale books detected - actively trigger recovery rather than hoping WS recovers passively
            self.logger().warning(
                f"Resuming '{strategy_name}' with stale order books, triggering active recovery: {', '.join(stale[:3])}"
                f"{'...' if len(stale) > 3 else ''}"
            )
            # Actively request fresh snapshots for stale pairs
            self._trigger_stale_book_recovery(strategy_instance)
        
        # Reset exception tracking on resume
        strategy_instance.exception_count = 0
        strategy_instance.last_exception_time = None
        strategy_instance.last_exception_msg = None
        
        strategy_instance.paused = False
        strategy_instance.config['paused'] = False
        self._persist_pause_state(strategy_name, False)
        self.logger().info(f"Strategy '{strategy_name}' resumed successfully")
        return True

    def _trigger_stale_book_recovery(self, si: 'V1StrategyInstance'):
        """
        Actively trigger recovery for stale order books by requesting fresh snapshots.
        
        This is called on resume when stale books are detected, to avoid the scenario
        where a silently dropped subscription leaves the order book frozen forever.
        
        Uses fire-and-forget async tasks since this is called from sync context.
        """
        import asyncio
        import time as _time
        STALE_THRESHOLD_SECONDS = 120.0
        now = _time.perf_counter()
        
        for mt in si.market_pairs:
            try:
                connector = mt.market
                ex_name = getattr(connector, "name", "?")
                trading_pair = mt.trading_pair
                
                # Check if this specific order book is stale
                ob = connector.get_order_book(trading_pair)
                last_diff = getattr(ob, 'last_applied_diff', -1000.0)
                if last_diff > 0 and (now - last_diff) <= STALE_THRESHOLD_SECONDS:
                    continue  # This book is fresh, skip
                
                # Try to get the data source and trigger recovery
                if hasattr(connector, 'order_book_tracker'):
                    tracker = connector.order_book_tracker
                    if hasattr(tracker, 'data_source'):
                        ds = tracker.data_source
                        
                        # Method 1: BitMart-style per-pair refresh (preferred)
                        if hasattr(ds, '_refresh_snapshot_for_pair') and hasattr(ds, '_pair_to_symbol_cache'):
                            symbol = ds._pair_to_symbol_cache.get(trading_pair)
                            if symbol is None and hasattr(ds, '_connector'):
                                # Schedule async symbol lookup + refresh
                                async def _do_refresh(ds_ref, tp, conn):
                                    try:
                                        sym = await conn.exchange_symbol_associated_to_pair(trading_pair=tp)
                                        ds_ref._pair_to_symbol_cache[tp] = sym
                                        await ds_ref._refresh_snapshot_for_pair(tp, sym)
                                        self.logger().info(f"Recovery snapshot requested for {tp} on {ex_name}")
                                    except Exception as e:
                                        self.logger().warning(f"Failed to request recovery for {tp}: {e}")
                                asyncio.ensure_future(_do_refresh(ds, trading_pair, ds._connector))
                            elif symbol is not None:
                                async def _do_refresh_cached(ds_ref, tp, sym):
                                    try:
                                        await ds_ref._refresh_snapshot_for_pair(tp, sym)
                                        self.logger().info(f"Recovery snapshot requested for {tp} on {ex_name}")
                                    except Exception as e:
                                        self.logger().warning(f"Failed to request recovery for {tp}: {e}")
                                asyncio.ensure_future(_do_refresh_cached(ds, trading_pair, symbol))
                        
                        # Method 2: Generic - trigger reconnection to re-subscribe all channels
                        elif hasattr(ds, 'trigger_reconnection'):
                            self.logger().info(
                                f"Triggering WS reconnection for {ex_name} to recover stale {trading_pair}"
                            )
                            ds.trigger_reconnection()
                            
            except Exception as e:
                self.logger().warning(f"Error triggering recovery for market pair: {e}")

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

    # Subscription Verification and Restoration Methods

    def verify_subscriptions(self) -> Dict[str, Dict]:
        """
        Verify websocket subscriptions for all connectors.
        
        Returns a diagnostic report of subscription health per connector.
        Useful for debugging pause/resume issues.
        
        Returns:
            Dict mapping connector names to subscription status:
            {
                "connector_name": {
                    "total_pairs": int,
                    "active_subscriptions": int,
                    "disabled_pairs": List[str],
                    "missing_pairs": List[str],
                    "healthy": bool
                }
            }
        """
        report: Dict[str, Dict] = {}
        
        for connector_name, connector in self.connectors.items():
            connector_report = {
                "total_pairs": 0,
                "active_subscriptions": 0,
                "disabled_pairs": [],
                "missing_pairs": [],
                "healthy": True
            }
            
            try:
                # Get expected trading pairs from connector
                expected_pairs = set()
                if hasattr(connector, 'trading_pairs'):
                    expected_pairs = set(connector.trading_pairs or [])
                
                connector_report["total_pairs"] = len(expected_pairs)
                
                # Get data source subscription info
                if hasattr(connector, 'order_book_tracker'):
                    tracker = connector.order_book_tracker
                    if hasattr(tracker, 'data_source'):
                        ds = tracker.data_source
                        
                        # Get active subscriptions
                        if hasattr(ds, '_trading_pairs'):
                            active_pairs = set(ds._trading_pairs)
                            connector_report["active_subscriptions"] = len(active_pairs)
                            
                            # Check for missing pairs
                            missing = expected_pairs - active_pairs
                            connector_report["missing_pairs"] = list(missing)
                        
                        # Get temporarily disabled pairs (Bybit-specific)
                        if hasattr(ds, '_temporarily_disabled_pairs'):
                            connector_report["disabled_pairs"] = list(ds._temporarily_disabled_pairs)
                
                # Determine health
                connector_report["healthy"] = (
                    len(connector_report["missing_pairs"]) == 0 and
                    len(connector_report["disabled_pairs"]) == 0
                )
                
            except Exception as e:
                connector_report["error"] = str(e)
                connector_report["healthy"] = False
            
            report[connector_name] = connector_report
        
        # Log summary
        healthy_count = sum(1 for r in report.values() if r.get("healthy", False))
        self.logger().info(f"Subscription verification: {healthy_count}/{len(report)} connectors healthy")
        
        for name, r in report.items():
            if not r.get("healthy", False):
                self.logger().warning(
                    f"  {name}: {r.get('active_subscriptions', '?')}/{r.get('total_pairs', '?')} active, "
                    f"disabled={r.get('disabled_pairs', [])}, missing={r.get('missing_pairs', [])}"
                )
        
        return report

    def restore_all_subscriptions(self) -> Dict[str, List[str]]:
        """
        Attempt to restore all temporarily disabled subscriptions across all connectors.
        
        This is useful after pause/resume or network issues where some trading pairs
        may have been disabled due to subscription failures.
        
        Returns:
            Dict mapping connector names to lists of restored trading pairs
        """
        restored: Dict[str, List[str]] = {}
        
        for connector_name, connector in self.connectors.items():
            connector_restored = []
            
            try:
                if hasattr(connector, 'order_book_tracker'):
                    tracker = connector.order_book_tracker
                    if hasattr(tracker, 'data_source'):
                        ds = tracker.data_source
                        
                        # Check for temporarily disabled pairs (Bybit-specific)
                        if hasattr(ds, '_temporarily_disabled_pairs') and hasattr(ds, '_original_trading_pairs'):
                            disabled = list(ds._temporarily_disabled_pairs)
                            for pair in disabled:
                                if pair in ds._original_trading_pairs and pair not in ds._trading_pairs:
                                    ds._trading_pairs.append(pair)
                                    ds._temporarily_disabled_pairs.discard(pair)
                                    connector_restored.append(pair)
                                    self.logger().info(f"Restored subscription for {connector_name}:{pair}")
                            
                            # Reset failure counters to give restored pairs a fresh chance
                            if hasattr(ds, '_subscription_failure_count') and connector_restored:
                                ds._subscription_failure_count.clear()
                
            except Exception as e:
                self.logger().error(f"Error restoring subscriptions for {connector_name}: {e}")
            
            if connector_restored:
                restored[connector_name] = connector_restored
        
        if restored:
            total = sum(len(pairs) for pairs in restored.values())
            self.logger().info(f"Restored {total} subscriptions across {len(restored)} connectors")
        else:
            self.logger().info("No subscriptions needed restoration")
        
        return restored

    def get_order_book_health(self, identifier: Optional[str] = None) -> Dict[str, Dict]:
        """
        Get order book health status for strategies.
        
        Args:
            identifier: Optional strategy name or token. If None, checks all strategies.
            
        Returns:
            Dict mapping strategy names to order book health info
        """
        import time
        STALE_THRESHOLD = 120.0
        
        health: Dict[str, Dict] = {}
        now = time.perf_counter()
        
        strategies_to_check = []
        if identifier:
            name = self._resolve_identifier_to_name(identifier, log_token_match=False)
            if name:
                si = self._get_strategy_instance(name)
                if si:
                    strategies_to_check = [si]
        else:
            strategies_to_check = self.strategies
        
        for si in strategies_to_check:
            strategy_health = {
                "status": "PAUSED" if si.paused else "RUNNING",
                "order_books": {},
                "all_healthy": True
            }
            
            for mt in si.market_pairs:
                try:
                    ex_name = getattr(mt.market, "name", "?")
                except Exception:
                    ex_name = "?"
                
                key = f"{ex_name}:{mt.trading_pair}"
                ob_health = {"status": "unknown", "last_update": None, "stale_seconds": None}
                
                try:
                    ob = mt.market.get_order_book(mt.trading_pair)
                    last_diff = getattr(ob, 'last_applied_diff', -1000.0)
                    
                    if last_diff > 0:
                        stale_secs = now - last_diff
                        ob_health["last_update"] = int(stale_secs)
                        ob_health["stale_seconds"] = int(stale_secs)
                        
                        if stale_secs > STALE_THRESHOLD:
                            ob_health["status"] = "stale"
                            strategy_health["all_healthy"] = False
                        else:
                            ob_health["status"] = "healthy"
                    else:
                        ob_health["status"] = "no_data"
                        strategy_health["all_healthy"] = False
                        
                except Exception as e:
                    ob_health["status"] = "missing"
                    ob_health["error"] = str(e)
                    strategy_health["all_healthy"] = False
                
                strategy_health["order_books"][key] = ob_health
            
            health[si.name] = strategy_health
        
        return health

    # Position Balancer Control Methods

    def enable_buyin_by_identifier(self, identifier: str) -> bool:
        """
        Enable buy-in mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.enable_buyin(strategy_name) if strategy_name else False

    def disable_buyin_by_identifier(self, identifier: str) -> bool:
        """
        Disable buy-in mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.disable_buyin(strategy_name) if strategy_name else False

    def enable_selloff_by_identifier(self, identifier: str) -> bool:
        """
        Enable sell-off mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.enable_selloff(strategy_name) if strategy_name else False

    def disable_selloff_by_identifier(self, identifier: str) -> bool:
        """
        Disable sell-off mode for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.disable_selloff(strategy_name) if strategy_name else False

    def enable_buyin(self, strategy_name: str) -> bool:
        """
        Enable buy-in mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.enable_buy_in()
        return True

    def disable_buyin(self, strategy_name: str) -> bool:
        """
        Disable buy-in mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.disable_buy_in()
        return True

    def enable_selloff(self, strategy_name: str) -> bool:
        """
        Enable sell-off mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.enable_sell_off()
        return True

    def disable_selloff(self, strategy_name: str) -> bool:
        """
        Disable sell-off mode for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.disable_sell_off()
        return True

    def clean_by_identifier(self, identifier: str) -> bool:
        """
        Set sell-off target to 0 and enable sell-off for a strategy by full name or token symbol.
        Sells the entire position (target = 0 USD).

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        if not strategy_name:
            return False
        return self.clean(strategy_name)

    def clean(self, strategy_name: str) -> bool:
        """
        Set sell-off target to 0, enable sell-off, and schedule automatic removal of the
        strategy once the sell-off completes.

        Args:
            strategy_name: The name of the strategy

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_sell_target(0.0)
        position_balancer.enable_sell_off()
        self._pending_auto_remove.add(strategy_name)
        # Disable hold-band so it doesn't interfere with the sell-off
        self.disable_hold(strategy_name)
        self.logger().info(f"Strategy '{strategy_name}' scheduled for auto-removal after sell-off completes")
        return True

    def set_min_profitability_by_identifier(self, identifier: str, value: float) -> bool:
        """
        Set minimum profitability for a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            value: New minimum profitability percentage (e.g., 1.5 for 1.5%)

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_min_profitability(strategy_name, value) if strategy_name else False

    def set_min_profitability(self, strategy_name: str, value: float) -> bool:
        """
        Set minimum profitability for a strategy.

        Args:
            strategy_name: The name of the strategy
            value: New minimum profitability percentage (e.g., 1.5 for 1.5%)

        Returns:
            True if successful, False otherwise
        """
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False

        try:
            # Convert percentage to decimal (e.g. 1.5 -> 0.015)
            decimal_value = Decimal(str(value)) / Decimal("100")
            
            # Update the strategy
            # Note: This relies on the property setter we added to ArbitrageLStrategy
            if hasattr(strategy_instance.strategy, 'min_profitability'):
                strategy_instance.strategy.min_profitability = decimal_value
                
                # Also update the config dict so status display reflects the change
                if 'min_profitability' in strategy_instance.config:
                    strategy_instance.config['min_profitability'] = Decimal(str(value))
                
                # Persist to config file using the same pattern as _update_config_file
                if self.config.config_file_path:
                    try:
                        config_path = Path(self.config.config_file_path)
                        with open(config_path, 'r') as f:
                            yaml_data = yaml.safe_load(f)
                        
                        # Find and update the strategy
                        for strat_config in yaml_data.get('arbitrage_m_strategies', []):
                            if strat_config.get('name') == strategy_name:
                                strat_config['min_profitability'] = float(value)
                                break
                        
                        # Write back
                        with open(config_path, 'w') as f:
                            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)
                    except Exception as e:
                        self.logger().warning(f"Failed to update config file: {e}")
                
                self.logger().info(f"Updated min_profitability for '{strategy_name}' to {value}%")
                return True
            else:
                self.logger().error(f"Strategy '{strategy_name}' does not support dynamic min_profitability update")
                return False
        except Exception as e:
            self.logger().error(f"Error setting min profitability for '{strategy_name}': {e}")
            return False

    # ── Hold-band guardrail control ───────────────────────────────────────────

    def _persist_pause_state(self, strategy_name: str, paused: bool):
        """Write the runtime pause flag to the YAML config file so it survives restarts."""
        self._persist_hold_config(strategy_name, {'paused': paused})

    def _persist_hold_config(self, strategy_name: str, updates: dict):
        """Write hold-band field updates to the YAML config file."""
        if not self.config.config_file_path:
            return
        try:
            config_path = Path(self.config.config_file_path)
            with open(config_path, 'r') as f:
                yaml_data = yaml.safe_load(f)
            for strat_config in yaml_data.get('arbitrage_m_strategies', []):
                if strat_config.get('name') == strategy_name:
                    strat_config.update(updates)
                    break
            with open(config_path, 'w') as f:
                yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)
        except Exception as e:
            self.logger().warning(f"Failed to persist hold config for '{strategy_name}': {e}")

    def enable_hold_by_identifier(self, identifier: str) -> bool:
        """Enable hold-band guardrail for a strategy (restores last configured target)."""
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.enable_hold(strategy_name) if strategy_name else False

    def enable_hold(self, strategy_name: str) -> bool:
        """Enable hold-band guardrail for a strategy by full name."""
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False
        try:
            strategy = strategy_instance.strategy
            if not hasattr(strategy, '_hold_target_usd'):
                return False
            # Re-read the value stored in the instance config so the target is meaningful.
            # If it was already set to 0 (disabled), restore from the original YAML config.
            target = float(strategy_instance.config.get('hold_target_usd', 1100.0))
            if target <= 0.0:
                self.logger().warning(f"hold_target_usd is 0 for '{strategy_name}' — set it first with "
                                      f"'control set hold_target {strategy_name} <usd>'")
                return False
            strategy._hold_target_usd = target
            strategy_instance.config['hold_target_usd'] = target
            self._persist_hold_config(strategy_name, {'hold_target_usd': target, 'hold_target_enabled': True})
            # Immediately refresh the cache so _hold_correction_active is set without
            # waiting up to 60 s for the next c_cleanup_old_orders cycle.
            if hasattr(strategy, 'refresh_hold_cache'):
                strategy.refresh_hold_cache()
            self.logger().info(f"Hold-band enabled for '{strategy_name}': target={target:.0f} USD "
                               f"band=±{strategy._hold_band_usd:.0f}")
            return True
        except Exception as e:
            self.logger().error(f"Error enabling hold for '{strategy_name}': {e}")
            return False

    def disable_hold_by_identifier(self, identifier: str) -> bool:
        """Disable hold-band guardrail for a strategy (sets target to 0, disabling the clamp)."""
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.disable_hold(strategy_name) if strategy_name else False

    def disable_hold(self, strategy_name: str) -> bool:
        """Disable hold-band guardrail for a strategy by full name."""
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False
        try:
            strategy = strategy_instance.strategy
            if not hasattr(strategy, '_hold_target_usd'):
                return False
            strategy._hold_target_usd = 0.0
            if hasattr(strategy, '_hold_correction_active'):
                strategy._hold_correction_active = False
            if hasattr(strategy, '_hold_breach_count'):
                strategy._hold_breach_count = 0
            if hasattr(strategy, '_hold_low_balance_suspend'):
                strategy._hold_low_balance_suspend = False
            self._persist_hold_config(strategy_name, {'hold_target_enabled': False})
            self.logger().info(f"Hold-band disabled for '{strategy_name}'")
            return True
        except Exception as e:
            self.logger().error(f"Error disabling hold for '{strategy_name}': {e}")
            return False

    def set_hold_target_by_identifier(self, identifier: str, target_usd: float) -> bool:
        """Set hold-band target USD for a strategy (also enables the guardrail)."""
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_hold_target(strategy_name, target_usd) if strategy_name else False

    def set_hold_target(self, strategy_name: str, target_usd: float) -> bool:
        """Set hold-band target USD for a strategy by full name and enable the guardrail."""
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False
        try:
            strategy = strategy_instance.strategy
            if not hasattr(strategy, '_hold_target_usd'):
                return False
            strategy._hold_target_usd = target_usd
            # Mirror into instance config so enable_hold can restore it later
            strategy_instance.config['hold_target_usd'] = target_usd
            persist = {'hold_target_usd': target_usd}
            if target_usd > 0.0:
                persist['hold_target_enabled'] = True
            else:
                # Explicitly mark disabled so YAML stays consistent on reload
                persist['hold_target_enabled'] = False
            self._persist_hold_config(strategy_name, persist)
            # Immediately refresh so _hold_correction_active reflects new target.
            if target_usd > 0.0 and hasattr(strategy, 'refresh_hold_cache'):
                strategy.refresh_hold_cache()
            elif target_usd <= 0.0:
                if hasattr(strategy, '_hold_correction_active'):
                    strategy._hold_correction_active = False
                if hasattr(strategy, '_hold_breach_count'):
                    strategy._hold_breach_count = 0
                if hasattr(strategy, '_hold_low_balance_suspend'):
                    strategy._hold_low_balance_suspend = False
            enabled_str = "enabled" if target_usd > 0.0 else "disabled (target=0)"
            self.logger().info(f"Hold-band target set to {target_usd:.0f} USD for '{strategy_name}' ({enabled_str})")
            return True
        except Exception as e:
            self.logger().error(f"Error setting hold target for '{strategy_name}': {e}")
            return False

    def set_hold_band_by_identifier(self, identifier: str, band_usd: float) -> bool:
        """Set hold-band half-width USD for a strategy."""
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_hold_band(strategy_name, band_usd) if strategy_name else False

    def set_hold_band(self, strategy_name: str, band_usd: float) -> bool:
        """Set hold-band half-width USD for a strategy by full name."""
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            return False
        try:
            strategy = strategy_instance.strategy
            if not hasattr(strategy, '_hold_band_usd'):
                return False
            strategy._hold_band_usd = band_usd
            strategy_instance.config['hold_band_usd'] = band_usd
            self._persist_hold_config(strategy_name, {'hold_band_usd': band_usd})
            self.logger().info(f"Hold-band half-width set to ±{band_usd:.0f} USD for '{strategy_name}'")
            return True
        except Exception as e:
            self.logger().error(f"Error setting hold band for '{strategy_name}': {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────────

    def set_buy_target_by_identifier(self, identifier: str, target_usd: float) -> bool:
        """
        Set buy-in target for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            target_usd: New target minimum asset value in USD

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_buy_target(strategy_name, target_usd) if strategy_name else False

    def set_buy_target(self, strategy_name: str, target_usd: float) -> bool:
        """
        Set buy-in target for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            target_usd: New target minimum asset value in USD

        Returns:
            True if successful, False otherwise
        """
        if target_usd < 0:
            self.logger().error("Target USD must be non-negative")
            return False

        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_buy_target(target_usd)
        return True

    def set_sell_target_by_identifier(self, identifier: str, target_usd: float) -> bool:
        """
        Set sell-off target for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            target_usd: New target maximum asset value in USD

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_sell_target(strategy_name, target_usd) if strategy_name else False

    def set_sell_target(self, strategy_name: str, target_usd: float) -> bool:
        """
        Set sell-off target for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            target_usd: New target maximum asset value in USD

        Returns:
            True if successful, False otherwise
        """
        if target_usd < 0:
            self.logger().error("Target USD must be non-negative")
            return False

        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_sell_target(target_usd)
        return True

    def set_buy_spread_by_identifier(self, identifier: str, spread_pct) -> bool:
        """
        Set buy spread for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_buy_spread(strategy_name, spread_pct) if strategy_name else False

    def set_buy_spread(self, strategy_name: str, spread_pct) -> bool:
        """
        Set buy spread for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_buy_spread(spread_pct)
        return True

    def set_sell_spread_by_identifier(self, identifier: str, spread_pct) -> bool:
        """
        Set sell spread for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_sell_spread(strategy_name, spread_pct) if strategy_name else False

    def set_sell_spread(self, strategy_name: str, spread_pct) -> bool:
        """
        Set sell spread for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            spread_pct: Spread percentage (e.g., 0.1 for 0.1%) or 'min' for minimum tick

        Returns:
            True if successful, False otherwise
        """
        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_sell_spread(spread_pct)
        return True

    def set_order_size_by_identifier(self, identifier: str, order_size_usd: float) -> bool:
        """
        Set order size for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            order_size_usd: Maximum order size in USD per order

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_order_size(strategy_name, order_size_usd) if strategy_name else False

    def set_order_size(self, strategy_name: str, order_size_usd: float) -> bool:
        """
        Set order size for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            order_size_usd: Maximum order size in USD per order

        Returns:
            True if successful, False otherwise
        """
        if order_size_usd <= 0:
            self.logger().error("Order size must be positive")
            return False

        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_order_size(order_size_usd)
        return True

    def set_refresh_interval_by_identifier(self, identifier: str, refresh_interval: float) -> bool:
        """
        Set refresh interval for a strategy's position balancer by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            refresh_interval: How often to cancel and replace limit orders (seconds)

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.set_refresh_interval(strategy_name, refresh_interval) if strategy_name else False

    def set_refresh_interval(self, strategy_name: str, refresh_interval: float) -> bool:
        """
        Set refresh interval for a strategy's position balancer.

        Args:
            strategy_name: The name of the strategy
            refresh_interval: How often to cancel and replace limit orders (seconds)

        Returns:
            True if successful, False otherwise
        """
        if refresh_interval <= 0:
            self.logger().error("Refresh interval must be positive")
            return False

        position_balancer = self._get_strategy_position_balancer(strategy_name)
        if not position_balancer:
            return False

        position_balancer.set_refresh_interval(refresh_interval)
        return True

    def remove_strategy_by_identifier(self, identifier: str) -> bool:
        """
        Remove a strategy by full name or token symbol.

        Args:
            identifier: Full strategy name or token symbol

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        return self.remove_strategy(strategy_name) if strategy_name else False

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

        # Step 3: Remove from in-memory list and index
        self.strategies = [s for s in self.strategies if s.name != strategy_name]
        self._strategy_by_name.pop(strategy_name, None)  # Remove from index
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

        # Evict dead pairs from connector internal trading_pairs lists
        for exchange_name, pairs in removed_markets.items():
            for pair in pairs:
                self._maybe_remove_pair_from_connector(exchange_name, pair)

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

    # ====================================================================================
    # MARKET MANIPULATION (add/remove markets from strategies at runtime)
    # ====================================================================================

    async def _ensure_connector(self, exchange: str, pair: str) -> bool:
        """
        Ensure a connector exists for the given exchange, initializing it at runtime if needed.

        Used by create_strategy and add_market_to_strategy when the exchange
        is not yet in the connector pool.

        Returns:
            True if connector is available (existing or newly created), False on failure
        """
        if exchange in self.connectors:
            return True

        self.logger().info(f"Exchange '{exchange}' not in connector pool. Initializing at runtime...")

        try:
            from hummingbot.client.hummingbot_application import HummingbotApplication
            app = HummingbotApplication.main_application()
            if not app or not app.trading_core:
                self.logger().error("Could not access main application/trading core to initialize new connector")
                return False

            # Initialize the new connector via TradingCore
            # This handles creation, adding to clock, and markets recorder
            await app.trading_core.initialize_markets([(exchange, [pair])])

            # Retrieve the newly created connector
            new_connector = app.trading_core.connector_manager.connectors.get(exchange)

            if not new_connector:
                self.logger().error(f"Failed to initialize connector '{exchange}'")
                return False

            # Add to strategy's connector pool
            self.connectors[exchange] = new_connector

            # Reset strategy readiness to ensure we wait for the new connector to sync
            self.ready_to_trade = False

            # Register the new connector with the strategy's market registry
            # This populates self.markets which is required for get_assets/format_status
            self.add_markets([new_connector])

            if not new_connector.ready:
                self.logger().info(f"Connector '{exchange}' initialized but not yet ready. It will sync in background.")

            self.logger().info(f"Successfully initialized exchange '{exchange}'")
            return True

        except Exception as e:
            self.logger().error(f"Error initializing exchange '{exchange}': {e}", exc_info=True)
            return False

    async def add_market_by_identifier(self, identifier: str, market_spec: str) -> bool:
        """
        Add a market to a strategy's additional_markets by name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        if not strategy_name:
            self.logger().error(f"Could not resolve identifier '{identifier}' to a strategy name")
            return False
        return await self.add_market_to_strategy(strategy_name, market_spec)

    async def add_market_to_strategy(self, strategy_name: str, market_spec: str) -> bool:
        """
        Add a market to a strategy's additional_markets (runtime + config file).

        The new market will be:
        1. Added to the strategy's active market pairs (creates new arbitrage permutations)
        2. Added to the strategy's additional_markets config
        3. Added to the config file's markets section (if not already present)

        Args:
            strategy_name: Full strategy name
            market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

        Returns:
            True if successful, False otherwise
        """
        if not strategy_name:
            self.logger().error("Strategy name cannot be empty")
            return False

        # Validate market_spec format
        if ':' not in market_spec:
            self.logger().error(f"Invalid market spec '{market_spec}'. Expected format: 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')")
            return False

        exchange, pair = market_spec.split(':', 1)
        exchange = exchange.lower()

        if '-' not in pair:
            self.logger().error(f"Invalid trading pair '{pair}'. Expected format: 'BASE-QUOTE' (e.g., 'BSX-USDT')")
            return False

        # Ensure connector exists (initialize at runtime if missing)
        if not await self._ensure_connector(exchange, pair):
            return False

        # Get strategy instance
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            self.logger().error(f"Strategy '{strategy_name}' not found")
            return False

        # Check if market already exists
        existing_spec = f"{exchange}:{pair}"
        for mt in strategy_instance.market_pairs:
            mt_spec = f"{mt.market.name}:{mt.trading_pair}".lower()
            if mt_spec == existing_spec.lower():
                self.logger().warning(f"Market '{market_spec}' already exists in strategy '{strategy_name}'")
                return False

        # Update self.markets to ensure get_assets works for the new/updated connector
        if exchange not in self.markets:
            self.markets[exchange] = set()
        self.markets[exchange].add(pair)

        # Build new market tuple
        base, quote = pair.split('-', 1)
        new_tuple = MarketTradingPairTuple(
            market=self.connectors[exchange],
            trading_pair=pair,
            base_asset=base,
            quote_asset=quote
        )

        # DYNAMIC SUBSCRIPTION: Subscribe to the trading pair on the connector's websocket
        # This ensures order book data is available immediately without restart
        connector = self.connectors[exchange]
        subscription_success = False
        try:
            if hasattr(connector, 'add_trading_pair_subscription'):
                # Use the new unified subscription method
                subscription_success = await connector.add_trading_pair_subscription(pair)
                if subscription_success:
                    self.logger().info(
                        f"Dynamically subscribed to {pair} on {exchange}"
                    )
                else:
                    self.logger().warning(
                        f"Could not dynamically subscribe to {pair} on {exchange}. "
                        f"Will be subscribed on next connector reconnection."
                    )
            else:
                self.logger().info(
                    f"Exchange {exchange} does not support dynamic subscriptions. "
                    f"Trading pair will use existing data or require restart."
                )
        except Exception as e:
            self.logger().warning(
                f"Failed to dynamically subscribe to {pair} on {exchange}: {e}. "
                f"Market added but subscription may be delayed."
            )

        # Add to strategy's market_pairs list
        strategy_instance.market_pairs.append(new_tuple)

        # Keep the runtime config dict's additional_markets in sync with market_pairs.
        # Without this, a later primary/secondary remove_market reads a stale (empty)
        # additional_markets, finds nothing to promote, and refuses to remove a leg that
        # is actually live in market_pairs. _add_market_to_config only touches the YAML
        # file, not this in-memory dict, so the desync would otherwise persist until restart.
        config = strategy_instance.config
        existing_additional_lower = [m.lower() for m in config.get('additional_markets', [])]
        if market_spec.lower() not in existing_additional_lower:
            config.setdefault('additional_markets', []).append(market_spec)

        # Register the connector in the V1 strategy's _sb_markets set.
        # strategy.init_params() calls c_add_markets() at startup, but when a market is added
        # at runtime the connector may already exist in the pool (e.g. MEXC already running other
        # pairs), so _ensure_connector skips initialization and never calls c_add_markets() on
        # this strategy. Without this call, c_sell/buy_with_specific_market raises
        # "Market object for sell order is not in the whitelisted markets set."
        strategy_instance.strategy.add_markets([connector])

        # Invalidate cached connectors set (will be recomputed on next access)
        strategy_instance._connectors = None

        # Rebuild strategy's internal _market_pairs (ArbitrageLMarketPair permutations)
        self._rebuild_strategy_market_pairs(strategy_instance)

        self.logger().info(f"Added market '{market_spec}' to strategy '{strategy_name}' (runtime)")

        # Update config file
        if self.config.config_file_path:
            try:
                self._add_market_to_config(strategy_name, market_spec)
                self.logger().info(f"Config file updated: added '{market_spec}' to '{strategy_name}'")
            except Exception as e:
                self.logger().error(f"Failed to update config file: {e}", exc_info=True)
                return False
        else:
            self.logger().warning("Config file path not set, skipping file update")

        return True

    def remove_market_by_identifier(self, identifier: str, market_spec: str) -> bool:
        """
        Remove a market from a strategy's additional_markets by name or token symbol.

        Args:
            identifier: Full strategy name or token symbol
            market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

        Returns:
            True if successful, False otherwise
        """
        strategy_name = self._resolve_identifier_to_name(identifier)
        if not strategy_name:
            self.logger().error(f"Could not resolve identifier '{identifier}' to a strategy name")
            return False
        return self.remove_market_from_strategy(strategy_name, market_spec)

    def remove_market_from_strategy(self, strategy_name: str, market_spec: str) -> bool:
        """
        Remove a market from a strategy's additional_markets (runtime + config file).

        Note: Cannot remove primary or secondary markets, only additional_markets.

        Args:
            strategy_name: Full strategy name
            market_spec: Market specification as 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')

        Returns:
            True if successful, False otherwise
        """
        if not strategy_name:
            self.logger().error("Strategy name cannot be empty")
            return False

        # Validate market_spec format
        if ':' not in market_spec:
            self.logger().error(f"Invalid market spec '{market_spec}'. Expected format: 'exchange:PAIR' (e.g., 'mexc:BSX-USDT')")
            return False

        exchange, pair = market_spec.split(':', 1)
        exchange = exchange.lower()

        # Get strategy instance
        strategy_instance = self._get_strategy_instance(strategy_name)
        if not strategy_instance:
            self.logger().error(f"Strategy '{strategy_name}' not found")
            return False

        # Check if this is a primary or secondary market
        config = strategy_instance.config
        primary_spec = f"{config.get('primary_market', '')}:{config.get('primary_trading_pair', '')}".lower()
        secondary_spec = f"{config.get('secondary_market', '')}:{config.get('secondary_trading_pair', '')}".lower()
        target_spec = f"{exchange}:{pair}".lower()

        # Track if we need promotion (removing primary or secondary)
        is_removing_primary = (target_spec == primary_spec)
        is_removing_secondary = (target_spec == secondary_spec)
        promotion_info = None  # Will hold promotion details if needed

        if is_removing_primary or is_removing_secondary:
            # Need to promote another market to fill the gap
            additional_markets = config.get('additional_markets', [])

            if is_removing_primary:
                if additional_markets:
                    # Have additional markets: secondary becomes primary, first additional becomes secondary
                    promotion_info = {
                        'type': 'primary_removed',
                        'new_primary_market': config.get('secondary_market'),
                        'new_primary_pair': config.get('secondary_trading_pair'),
                        'new_secondary_from_additional': additional_markets[0]
                    }
                    self.logger().info(
                        f"Removing primary market '{market_spec}' - promoting secondary to primary"
                    )
                elif len(strategy_instance.market_pairs) == 2:
                    # Only 2 markets, no additional: duplicate remaining market into both slots
                    remaining_market = config.get('secondary_market')
                    remaining_pair = config.get('secondary_trading_pair')
                    promotion_info = {
                        'type': 'single_market_remaining',
                        'remaining_market': remaining_market,
                        'remaining_pair': remaining_pair,
                    }
                    self.logger().warning(
                        f"Removing primary market '{market_spec}' - strategy will have single market "
                        f"({remaining_market}:{remaining_pair}). Arbitrage disabled, position balancing only."
                    )
                else:
                    self.logger().error(
                        f"Cannot remove primary market '{market_spec}': no additional_markets to promote. "
                        f"Add another market first with 'control add_market'."
                    )
                    return False
            else:  # is_removing_secondary
                if additional_markets:
                    # First additional becomes new secondary
                    promotion_info = {
                        'type': 'secondary_removed',
                        'new_secondary_from_additional': additional_markets[0]
                    }
                    self.logger().info(
                        f"Removing secondary market '{market_spec}' - promoting from additional_markets"
                    )
                elif len(strategy_instance.market_pairs) == 2:
                    # Only 2 markets, no additional: duplicate remaining market into both slots
                    remaining_market = config.get('primary_market')
                    remaining_pair = config.get('primary_trading_pair')
                    promotion_info = {
                        'type': 'single_market_remaining',
                        'remaining_market': remaining_market,
                        'remaining_pair': remaining_pair,
                    }
                    self.logger().warning(
                        f"Removing secondary market '{market_spec}' - strategy will have single market "
                        f"({remaining_market}:{remaining_pair}). Arbitrage disabled, position balancing only."
                    )
                else:
                    self.logger().error(
                        f"Cannot remove secondary market '{market_spec}': no additional_markets to promote. "
                        f"Add another market first with 'control add_market'."
                    )
                    return False

        # Find and remove the market tuple
        found = False
        new_market_pairs = []
        for mt in strategy_instance.market_pairs:
            mt_spec = f"{mt.market.name}:{mt.trading_pair}".lower()
            if mt_spec == target_spec:
                found = True
                # Skip this one (effectively removing it)
            else:
                new_market_pairs.append(mt)

        if not found:
            self.logger().error(f"Market '{market_spec}' not found in strategy '{strategy_name}'")
            return False

        # Handle single-market case: duplicate remaining market into both slots
        if len(new_market_pairs) == 1 and promotion_info and promotion_info.get('type') == 'single_market_remaining':
            # Duplicate the remaining market tuple so the strategy has 2 "markets" (same exchange)
            remaining_mt = new_market_pairs[0]
            new_market_pairs.append(remaining_mt)
            self.logger().info(
                f"Duplicated remaining market {remaining_mt.market.name}:{remaining_mt.trading_pair} "
                f"into both primary and secondary slots"
            )
        elif len(new_market_pairs) < 2:
            self.logger().error(
                f"Cannot remove market '{market_spec}': strategy requires at least 2 markets for arbitrage. "
                f"Current market count: {len(strategy_instance.market_pairs)}"
            )
            return False

        # Apply promotion if needed (update runtime config dict)
        if promotion_info:
            if promotion_info['type'] == 'primary_removed':
                # Secondary becomes primary
                config['primary_market'] = config['secondary_market']
                config['primary_trading_pair'] = config['secondary_trading_pair']
                # First additional becomes secondary
                if promotion_info.get('new_secondary_from_additional'):
                    new_sec_spec = promotion_info['new_secondary_from_additional']
                    if ':' in new_sec_spec:
                        new_sec_exchange, new_sec_pair = new_sec_spec.split(':', 1)
                        config['secondary_market'] = new_sec_exchange.lower()
                        config['secondary_trading_pair'] = new_sec_pair
                        # Remove from additional_markets
                        config['additional_markets'] = [
                            m for m in config.get('additional_markets', [])
                            if m.lower() != new_sec_spec.lower()
                        ]
            elif promotion_info['type'] == 'secondary_removed':
                # First additional becomes secondary
                new_sec_spec = promotion_info['new_secondary_from_additional']
                if ':' in new_sec_spec:
                    new_sec_exchange, new_sec_pair = new_sec_spec.split(':', 1)
                    config['secondary_market'] = new_sec_exchange.lower()
                    config['secondary_trading_pair'] = new_sec_pair
                    # Remove from additional_markets
                    config['additional_markets'] = [
                        m for m in config.get('additional_markets', [])
                        if m.lower() != new_sec_spec.lower()
                    ]
            elif promotion_info['type'] == 'single_market_remaining':
                # Both primary and secondary point to the same remaining market
                remaining_market = promotion_info['remaining_market']
                remaining_pair = promotion_info['remaining_pair']
                config['primary_market'] = remaining_market
                config['primary_trading_pair'] = remaining_pair
                config['secondary_market'] = remaining_market
                config['secondary_trading_pair'] = remaining_pair
                # Clear conversion rates since same market on both sides
                config['use_oracle_conversion_rate'] = False
                config['secondary_to_primary_base_conversion_rate'] = 1.0
                config['secondary_to_primary_quote_conversion_rate'] = 1.0
                # Clear additional_markets
                config['additional_markets'] = []
                # NOTE: Strategy's Cython fields (_use_oracle_conversion_rate, _fixed_base_rate, etc.)
                # are cdef and cannot be set from Python. However, this is safe because:
                # - _conv_rate() fast-path returns 1.0 when both market tuples have identical
                #   base_asset and quote_asset (which they will since both slots are the same market)
                # - No oracle lookup is ever triggered for same-asset pairs
        else:
            # Plain additional-market removal (not primary/secondary): keep the runtime
            # config dict in sync with market_pairs. Without this, a later primary/secondary
            # removal reads a stale additional_markets, picks the promotion branch for a
            # market that no longer exists, and fails the "at least 2 markets" check.
            config['additional_markets'] = [
                m for m in config.get('additional_markets', [])
                if m.lower() != target_spec
            ]

        # Update strategy's market_pairs list
        strategy_instance.market_pairs = new_market_pairs

        # Invalidate cached connectors set (will be recomputed on next access)
        strategy_instance._connectors = None

        # Rebuild strategy's internal _market_pairs (ArbitrageLMarketPair permutations)
        self._rebuild_strategy_market_pairs(strategy_instance)

        # NOTE: Position balancer asset aliases (_asset_aliases, _canonical_asset) are cdef
        # fields and cannot be reset from Python. Stale aliases are harmless because
        # c_find_best_sell_market/c_find_best_buy_market only match against markets that
        # actually exist in _market_pairs. Aliases will be correctly rebuilt on next restart.

        self.logger().info(f"Removed market '{market_spec}' from strategy '{strategy_name}' (runtime)")

        # Update config file
        if self.config.config_file_path:
            try:
                self._remove_market_from_config(strategy_name, market_spec, promotion_info)
                self.logger().info(f"Config file updated: removed '{market_spec}' from '{strategy_name}'")
            except Exception as e:
                self.logger().error(f"Failed to update config file: {e}", exc_info=True)
                return False
        else:
            self.logger().warning("Config file path not set, skipping file update")

        # Evict dead pair from connector's internal trading_pairs list
        self._maybe_remove_pair_from_connector(exchange, pair)

        return True

    def _rebuild_strategy_market_pairs(self, strategy_instance: V1StrategyInstance):
        """
        Rebuild a strategy's internal _market_pairs from its market tuple list.

        This creates all permutations (i != j) of ArbitrageLMarketPair.
        """
        strategy = strategy_instance.strategy
        market_tuples = strategy_instance.market_pairs

        # Determine market pair class from strategy type
        config = strategy_instance.config
        strategy_type = config.get('strategy_type', 'arbitrage_l')

        if strategy_type == 'arbitrage_l':
            MarketPairClass = ArbitrageLMarketPair
        else:
            MarketPairClass = ArbitrageMMarketPair

        # Build all permutations
        new_market_pairs = []
        for i in range(len(market_tuples)):
            for j in range(len(market_tuples)):
                if i != j:
                    new_market_pairs.append(MarketPairClass(
                        first=market_tuples[i],
                        second=market_tuples[j]
                    ))

        # Update strategy's internal _market_pairs directly
        strategy._market_pairs = new_market_pairs

        self.logger().debug(
            f"Rebuilt market pairs for '{strategy_instance.name}': "
            f"{len(market_tuples)} markets -> {len(new_market_pairs)} pairs"
        )

    def _add_market_to_config(self, strategy_name: str, market_spec: str):
        """
        Add a market to the config file's strategy and markets sections.
        """
        config_path = Path(self.config.config_file_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read current config
        with open(config_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ValueError("Config file is empty or invalid")

        exchange, pair = market_spec.split(':', 1)
        exchange = exchange.lower()

        # 1. Add to strategy's additional_markets
        if 'arbitrage_m_strategies' in yaml_data:
            for strategy_config in yaml_data['arbitrage_m_strategies']:
                if strategy_config.get('name') == strategy_name:
                    if 'additional_markets' not in strategy_config:
                        strategy_config['additional_markets'] = []
                    # Case-insensitive check for duplicates
                    existing_lower = [m.lower() for m in strategy_config['additional_markets']]
                    if market_spec.lower() not in existing_lower:
                        strategy_config['additional_markets'].append(market_spec)
                    break

        # 2. Add to global markets section if needed
        if 'markets' not in yaml_data:
            yaml_data['markets'] = {}

        if exchange not in yaml_data['markets']:
            yaml_data['markets'][exchange] = []

        if isinstance(yaml_data['markets'][exchange], list):
            # Case-insensitive check for duplicates
            existing_pairs_lower = [p.lower() for p in yaml_data['markets'][exchange]]
            if pair.lower() not in existing_pairs_lower:
                yaml_data['markets'][exchange].append(pair)

        # Write updated config back to file
        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)

    # ====================================================================================
    # RUNTIME STRATEGY CREATION
    # ====================================================================================

    async def create_strategy(self, name: str, primary_spec: str, secondary_spec: str,
                              min_profitability: float = 2.1,
                              additional_markets: str = None) -> bool:
        """
        Create a new arbitrage strategy at runtime (always starts PAUSED).

        Handles dynamic WS subscriptions so order book data flows immediately.
        Use 'control resume <name>' to start trading after verifying settings.

        Args:
            name: Unique name for the strategy
            primary_spec: Primary market as 'exchange:PAIR' (e.g., 'gate:BSX-USDT')
            secondary_spec: Secondary market as 'exchange:PAIR' (e.g., 'kucoin:BSX-USDT')
            min_profitability: Minimum profitability percentage (default: 1.5)
            additional_markets: Optional comma-separated additional markets

        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate name uniqueness
            if name in self._strategy_by_name:
                self.logger().error(f"Strategy name '{name}' already exists")
                return False

            # Validate and parse primary market spec
            if ':' not in primary_spec:
                self.logger().error(f"Invalid primary market spec '{primary_spec}'. Expected format: 'exchange:PAIR'")
                return False
            primary_exchange, primary_pair = primary_spec.split(':', 1)
            primary_exchange = primary_exchange.lower()

            if '-' not in primary_pair:
                self.logger().error(f"Invalid primary trading pair '{primary_pair}'. Expected format: 'BASE-QUOTE'")
                return False

            # Validate and parse secondary market spec
            if ':' not in secondary_spec:
                self.logger().error(f"Invalid secondary market spec '{secondary_spec}'. Expected format: 'exchange:PAIR'")
                return False
            secondary_exchange, secondary_pair = secondary_spec.split(':', 1)
            secondary_exchange = secondary_exchange.lower()

            if '-' not in secondary_pair:
                self.logger().error(f"Invalid secondary trading pair '{secondary_pair}'. Expected format: 'BASE-QUOTE'")
                return False

            # Parse additional_markets
            additional_list = []
            if additional_markets:
                for am in additional_markets.split(','):
                    am = am.strip()
                    if am and ':' in am:
                        additional_list.append(am)

            # Collect all exchange:pair combos
            all_pairs = [
                (primary_exchange, primary_pair),
                (secondary_exchange, secondary_pair),
            ]
            for am in additional_list:
                if ':' in am:
                    ex, pr = am.split(':', 1)
                    all_pairs.append((ex.lower(), pr))

            # Ensure all required connectors exist (initialize missing ones at runtime)
            for exchange, pair in all_pairs:
                if exchange not in self.connectors:
                    ok = await self._ensure_connector(exchange, pair)
                    if not ok:
                        return False

            # Create config
            config = ArbitrageMInstanceConfig(
                name=name,
                primary_market=primary_exchange,
                secondary_market=secondary_exchange,
                primary_trading_pair=primary_pair,
                secondary_trading_pair=secondary_pair,
                min_profitability=Decimal(str(min_profitability)),
                buy_in_enabled=True,
                buy_in_target_usd=750.0,
                hold_target_enabled=True,
                hold_target_usd=750.0,
                hold_band_usd=150.0,
                additional_markets=additional_list
            )

            # Update self.markets so the framework tracks the new pairs
            for exchange, pair in all_pairs:
                if exchange not in self.markets:
                    self.markets[exchange] = set()
                self.markets[exchange].add(pair)

            # Dynamic WS subscriptions (same mechanism as add_market_to_strategy)
            for exchange, pair in all_pairs:
                connector = self.connectors.get(exchange)
                if connector is None:
                    continue
                try:
                    if hasattr(connector, 'add_trading_pair_subscription'):
                        ok = await connector.add_trading_pair_subscription(pair)
                        if ok:
                            self.logger().info(f"Subscribed to {pair} on {exchange}")
                        else:
                            self.logger().warning(
                                f"Could not subscribe to {pair} on {exchange}, "
                                f"will subscribe on next reconnection"
                            )
                except Exception as e:
                    self.logger().warning(
                        f"Failed to subscribe to {pair} on {exchange}: {e}, "
                        f"will subscribe on next reconnection"
                    )

            # Create the strategy instance (always paused)
            self._add_arbitrage_m_strategy(config, paused=True)

            # Start the new strategy on the clock so it participates in tick cycles
            if self._strategies_started and self._strategy_clock:
                strategy_instance = self._strategy_by_name.get(name)
                if strategy_instance and hasattr(strategy_instance.strategy, "start"):
                    try:
                        strategy_instance.strategy.start(self._strategy_clock)
                        self.logger().info(f"Strategy '{name}' registered with clock")
                    except Exception as e:
                        self.logger().error(f"Error starting strategy '{name}' on clock: {e}", exc_info=True)

            self.logger().info(f"Created strategy '{name}' (PAUSED): {primary_spec} <-> {secondary_spec}")

            # Persist to config file
            if self.config.config_file_path:
                try:
                    self._add_strategy_to_config(config)
                    self.logger().info(f"Config file updated: added strategy '{name}'")
                except Exception as e:
                    self.logger().error(f"Failed to update config file: {e}", exc_info=True)
                    self.logger().warning("Strategy created but NOT persisted to config file")

            return True

        except Exception as e:
            self.logger().error(f"Failed to create strategy '{name}': {e}", exc_info=True)
            return False

    def _add_strategy_to_config(self, config: ArbitrageMInstanceConfig):
        """
        Add a strategy to the config file's arbitrage_m_strategies list and update markets section.
        """
        config_path = Path(self.config.config_file_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read current config
        with open(config_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ValueError("Config file is empty or invalid")

        # 1. Add strategy to arbitrage_m_strategies list
        if 'arbitrage_m_strategies' not in yaml_data:
            yaml_data['arbitrage_m_strategies'] = []

        # Build strategy dict (only include non-default values for cleaner YAML)
        strategy_dict = {
            'name': config.name,
            'primary_market': config.primary_market,
            'secondary_market': config.secondary_market,
            'primary_trading_pair': config.primary_trading_pair,
            'secondary_trading_pair': config.secondary_trading_pair,
        }

        # Only add min_profitability if different from default
        if config.min_profitability != Decimal("1.5"):
            strategy_dict['min_profitability'] = float(config.min_profitability)

        # Always persist hold-band fields when enabled so they survive restart.
        # Without this, a restart would reload defaults (enabled=False, target=1100)
        # and the guardrail would be silently off even though create() sets it to 750/True.
        if config.hold_target_enabled and config.hold_target_usd > 0:
            strategy_dict['hold_target_enabled'] = True
            strategy_dict['hold_target_usd'] = float(config.hold_target_usd)
            strategy_dict['hold_band_usd'] = float(config.hold_band_usd)

        # Add additional_markets if any
        if config.additional_markets:
            strategy_dict['additional_markets'] = config.additional_markets

        yaml_data['arbitrage_m_strategies'].append(strategy_dict)

        # 2. Add markets to global markets section
        if 'markets' not in yaml_data:
            yaml_data['markets'] = {}

        # Add primary market pair
        if config.primary_market not in yaml_data['markets']:
            yaml_data['markets'][config.primary_market] = []
        if isinstance(yaml_data['markets'][config.primary_market], list):
            existing_lower = [p.lower() for p in yaml_data['markets'][config.primary_market]]
            if config.primary_trading_pair.lower() not in existing_lower:
                yaml_data['markets'][config.primary_market].append(config.primary_trading_pair)

        # Add secondary market pair
        if config.secondary_market not in yaml_data['markets']:
            yaml_data['markets'][config.secondary_market] = []
        if isinstance(yaml_data['markets'][config.secondary_market], list):
            existing_lower = [p.lower() for p in yaml_data['markets'][config.secondary_market]]
            if config.secondary_trading_pair.lower() not in existing_lower:
                yaml_data['markets'][config.secondary_market].append(config.secondary_trading_pair)

        # Add additional markets
        for am in config.additional_markets:
            if ':' in am:
                exchange, pair = am.split(':', 1)
                exchange = exchange.lower()
                if exchange not in yaml_data['markets']:
                    yaml_data['markets'][exchange] = []
                if isinstance(yaml_data['markets'][exchange], list):
                    existing_lower = [p.lower() for p in yaml_data['markets'][exchange]]
                    if pair.lower() not in existing_lower:
                        yaml_data['markets'][exchange].append(pair)

        # Write updated config back to file
        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, indent=2)

        self.logger().info(f"Strategy '{config.name}' added to config file")

    def _maybe_remove_pair_from_connector(self, exchange_name: str, trading_pair: str):
        """
        Evict trading_pair from connector._trading_pairs if no remaining strategy still needs it.
        Called after remove_market_from_strategy() and remove_strategy() to prevent the Binance
        (and potentially other) fill-polling loops from continuing to iterate dead pairs.
        """
        still_needed = any(
            any(mt.market.name == exchange_name and mt.trading_pair == trading_pair
                for mt in s.market_pairs)
            for s in self.strategies
        )
        if still_needed:
            return

        connector = self.connectors.get(exchange_name)
        if connector is None:
            return

        tp_list = getattr(connector, '_trading_pairs', None)
        if isinstance(tp_list, list) and trading_pair in tp_list:
            tp_list.remove(trading_pair)
            self.logger().info(
                f"Evicted {trading_pair} from {exchange_name} connector._trading_pairs (no longer used by any strategy)"
            )

    def _remove_market_from_config(self, strategy_name: str, market_spec: str, promotion_info: dict = None):
        """
        Remove a market from the config file's strategy additional_markets.
        If promotion_info is provided, also updates primary_market/secondary_market fields.
        Also removes from global markets section if no longer used by any strategy.
        """
        config_path = Path(self.config.config_file_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read current config
        with open(config_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ValueError("Config file is empty or invalid")

        exchange, pair = market_spec.split(':', 1)
        exchange = exchange.lower()

        # Find the strategy config entry
        target_strategy_config = None
        if 'arbitrage_m_strategies' in yaml_data:
            for strategy_config in yaml_data['arbitrage_m_strategies']:
                if strategy_config.get('name') == strategy_name:
                    target_strategy_config = strategy_config
                    break

        if target_strategy_config:
            # Handle promotion if needed
            if promotion_info:
                if promotion_info['type'] == 'primary_removed':
                    # Secondary becomes primary
                    target_strategy_config['primary_market'] = target_strategy_config['secondary_market']
                    target_strategy_config['primary_trading_pair'] = target_strategy_config['secondary_trading_pair']

                    # First additional becomes secondary
                    new_sec_spec = promotion_info.get('new_secondary_from_additional')
                    if new_sec_spec and ':' in new_sec_spec:
                        new_sec_exchange, new_sec_pair = new_sec_spec.split(':', 1)
                        target_strategy_config['secondary_market'] = new_sec_exchange.lower()
                        target_strategy_config['secondary_trading_pair'] = new_sec_pair
                        # Remove promoted market from additional_markets
                        if 'additional_markets' in target_strategy_config:
                            target_strategy_config['additional_markets'] = [
                                m for m in target_strategy_config['additional_markets']
                                if m.lower() != new_sec_spec.lower()
                            ]
                            if not target_strategy_config['additional_markets']:
                                del target_strategy_config['additional_markets']

                    self.logger().info(
                        f"Config: promoted secondary to primary, "
                        f"new secondary: {new_sec_spec or 'none'}"
                    )

                elif promotion_info['type'] == 'secondary_removed':
                    # First additional becomes secondary
                    new_sec_spec = promotion_info.get('new_secondary_from_additional')
                    if new_sec_spec and ':' in new_sec_spec:
                        new_sec_exchange, new_sec_pair = new_sec_spec.split(':', 1)
                        target_strategy_config['secondary_market'] = new_sec_exchange.lower()
                        target_strategy_config['secondary_trading_pair'] = new_sec_pair
                        # Remove promoted market from additional_markets
                        if 'additional_markets' in target_strategy_config:
                            target_strategy_config['additional_markets'] = [
                                m for m in target_strategy_config['additional_markets']
                                if m.lower() != new_sec_spec.lower()
                            ]
                            if not target_strategy_config['additional_markets']:
                                del target_strategy_config['additional_markets']

                    self.logger().info(f"Config: promoted {new_sec_spec} to secondary")

                elif promotion_info['type'] == 'single_market_remaining':
                    # Both primary and secondary point to the same remaining market
                    remaining_market = promotion_info['remaining_market']
                    remaining_pair = promotion_info['remaining_pair']
                    target_strategy_config['primary_market'] = remaining_market
                    target_strategy_config['primary_trading_pair'] = remaining_pair
                    target_strategy_config['secondary_market'] = remaining_market
                    target_strategy_config['secondary_trading_pair'] = remaining_pair
                    target_strategy_config['use_oracle_conversion_rate'] = False
                    target_strategy_config['secondary_to_primary_base_conversion_rate'] = 1.0
                    target_strategy_config['secondary_to_primary_quote_conversion_rate'] = 1.0
                    if 'additional_markets' in target_strategy_config:
                        del target_strategy_config['additional_markets']
                    self.logger().info(
                        f"Config: single market remaining ({remaining_market}:{remaining_pair})")

            else:
                # No promotion - just remove from additional_markets
                if 'additional_markets' in target_strategy_config:
                    # Remove matching market spec (case-insensitive comparison)
                    target_strategy_config['additional_markets'] = [
                        m for m in target_strategy_config['additional_markets']
                        if m.lower() != market_spec.lower()
                    ]
                    # Remove empty list
                    if not target_strategy_config['additional_markets']:
                        del target_strategy_config['additional_markets']

        # Check if pair is still used by any strategy
        pair_still_used = False
        for strategy_config in yaml_data.get('arbitrage_m_strategies', []):
            # Check primary/secondary
            if (strategy_config.get('primary_market', '').lower() == exchange and
                strategy_config.get('primary_trading_pair', '').lower() == pair.lower()):
                pair_still_used = True
                break
            if (strategy_config.get('secondary_market', '').lower() == exchange and
                strategy_config.get('secondary_trading_pair', '').lower() == pair.lower()):
                pair_still_used = True
                break
            # Check additional_markets
            for am in strategy_config.get('additional_markets', []):
                if am.lower() == market_spec.lower():
                    pair_still_used = True
                    break
            if pair_still_used:
                break

        # Remove from global markets section if no longer used
        if not pair_still_used and 'markets' in yaml_data:
            if exchange in yaml_data['markets']:
                if isinstance(yaml_data['markets'][exchange], list):
                    yaml_data['markets'][exchange] = [
                        p for p in yaml_data['markets'][exchange]
                        if p.lower() != pair.lower()
                    ]
                    # Remove exchange if no pairs left
                    if not yaml_data['markets'][exchange]:
                        del yaml_data['markets'][exchange]
                        self.logger().info(f"Removed exchange '{exchange}' from markets (no pairs remaining)")

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

            # Validate it's an arbitrage strategy (both arbitrage_l and arbitrage_m supported)
            strategy_type = single_config.get('strategy')
            if strategy_type not in ['arbitrage_l', 'arbitrage_m']:
                self.logger().error(
                    f"Unsupported strategy type: {strategy_type}. "
                    f"Only 'arbitrage_l' and 'arbitrage_m' are supported."
                )
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
        """
        Convert single-strategy config format to multi-strategy format.
        Only includes fields that are explicitly set in the source config or required.
        All other fields use defaults from ArbitrageMInstanceConfig.
        """
        try:
            # Generate strategy name from config if not provided
            name = single_config.get('name')
            if not name:
                # Extract name from filename (e.g., 'conf_arb_ace_kucoin_gate_strategy.yml' -> 'arb_ace_kucoin_gate')
                base_name = source_file.replace('.yml', '').replace('.yaml', '')
                base_name = base_name.replace('conf_', '').replace('_strategy', '')
                name = base_name

            # Start with required fields only
            multi_config = {
                'name': name,
                'primary_market': single_config['primary_market'],
                'secondary_market': single_config['secondary_market'],
                'primary_trading_pair': single_config['primary_market_trading_pair'],
                'secondary_trading_pair': single_config['secondary_market_trading_pair'],
            }

            # Map 'strategy' field to 'strategy_type' (single-strategy uses 'strategy', multi uses 'strategy_type')
            if 'strategy' in single_config and single_config['strategy']:
                multi_config['strategy_type'] = single_config['strategy']

            # Only add optional fields if they exist in source config
            optional_fields = [
                'strategy_type',
                'min_profitability',
                'use_oracle_conversion_rate',
                'secondary_to_primary_base_conversion_rate',
                'secondary_to_primary_quote_conversion_rate',
                'buy_in_enabled',
                'buy_in_target_usd',
                'buy_in_target_usdt',  # Legacy field
                'buy_in_spread_pct',
                'buy_in_min_profitability',
                'sell_off_enabled',
                'sell_off_target_usd',
                'sell_off_spread_pct',
                'position_balancer_refresh_interval',
                'position_balancer_order_size_usd',
                'status_report_interval',
                'next_trade_delay_interval',
                'order_timeout',
                'filled_order_timeout',
            ]

            for field in optional_fields:
                if field in single_config and single_config[field] is not None:
                    # Handle legacy buy_in_target_usdt -> buy_in_target_usd rename
                    if field == 'buy_in_target_usdt':
                        multi_config['buy_in_target_usd'] = single_config[field]
                    else:
                        multi_config[field] = single_config[field]

            # Handle additional_markets (can be string or list)
            if 'additional_markets' in single_config:
                additional_markets = single_config['additional_markets']
                if isinstance(additional_markets, str):
                    if additional_markets.strip():
                        # Parse comma-separated string: "mexc:SLF-USDT, htx:SLF-USDT"
                        multi_config['additional_markets'] = [m.strip() for m in additional_markets.split(',') if m.strip()]
                    # Empty string means no additional markets - use default
                elif isinstance(additional_markets, list) and additional_markets:
                    multi_config['additional_markets'] = additional_markets

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

    def _check_market_issues(self, strategy_instance: V1StrategyInstance) -> List[str]:
        """
        Check for market issues like low balance or not ready state.
        Returns a list of warning messages.
        """
        issues = []
        try:
            # Check 1: Market Readiness (Connectors and Order Books) - use cached values
            for market_pair in strategy_instance.market_pairs:
                connector = market_pair.market
                if not self._connector_ready_cache.get(connector.name, False):
                    issues.append(f"{connector.name} not ready")
                    continue
                
                # Check order book
                try:
                    ob = connector.get_order_book(market_pair.trading_pair)
                    if not ob.snapshot or (len(ob.snapshot[0]) == 0 and len(ob.snapshot[1]) == 0):
                         issues.append(f"{connector.name} empty order book")
                except Exception:
                    pass # Order book might not be ready yet

            # Check 2: Prices and Balances
            checked_assets = set()
            for market_pair in strategy_instance.market_pairs:
                connector = market_pair.market
                pair = market_pair.trading_pair
                base_asset = market_pair.base_asset
                quote_asset = market_pair.quote_asset

                # 2a. Check Market Price Availability
                price = None
                try:
                    price = connector.get_price(pair, is_buy=False)
                    if price is None or price <= 0:
                         price = connector.get_mid_price(pair)
                except Exception:
                    pass
                
                if price is None or price <= 0:
                    issues.append(f"{connector.name} {pair} (no price)")
                    # Can't value base asset reliably without price, but we can still check quote asset
                
                # 2b. Check Base Asset Balance (using market price)
                key_base = (connector.name, base_asset)
                if key_base not in checked_assets:
                    checked_assets.add(key_base)
                    try:
                        balance = connector.get_available_balance(base_asset)
                        if price is not None and price > 0:
                            # Value in Quote Asset
                            val_in_quote = float(balance) * float(price)
                            # Convert Quote to USD
                            quote_rate = RateOracle.get_instance().get_pair_rate(f"{quote_asset}-USDT")
                            if quote_rate is not None:
                                usd_value = val_in_quote * float(quote_rate)
                                if usd_value < 15.0:
                                    issues.append(f"{connector.name} (${usd_value:.0f})")
                            else:
                                issues.append(f"{connector.name} {quote_asset} (no rate)")
                        else:
                             # Fallback if no price: try direct Oracle conversion? 
                             # User requested market price, so maybe just skip or warn?
                             # We already warned about "no price" above.
                             pass
                    except Exception:
                        pass

                # 2c. Check Quote Asset Balance (direct Oracle)
                key_quote = (connector.name, quote_asset)
                if key_quote not in checked_assets:
                    checked_assets.add(key_quote)
                    try:
                        balance = connector.get_available_balance(quote_asset)
                        rate = RateOracle.get_instance().get_pair_rate(f"{quote_asset}-USDT")
                        if rate is not None:
                            usd_value = float(balance) * float(rate)
                            if usd_value < 15.0:
                                issues.append(f"{connector.name} (${usd_value:.0f})")
                        else:
                            issues.append(f"{connector.name} {quote_asset} (no rate)")
                    except Exception:
                        pass


        except Exception as e:
            self.logger().debug(f"Error checking market issues for {strategy_instance.name}: {e}")
            
        return issues


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

            # Hold-band guardrail live state (read from the Cython instance, not config)
            hold_target = None
            hold_band = None
            try:
                strat = strategy_instance.strategy
                if hasattr(strat, '_hold_target_usd') and float(strat._hold_target_usd) > 0.0:
                    hold_target = float(strat._hold_target_usd)
                    hold_band = float(strat._hold_band_usd)
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
                "hold_target": hold_target,   # None means disabled
                "hold_band": hold_band,
            }

        return strategy_summary

    def format_status(self) -> str:
        """
        Format status output for all strategies.

        Shows per-strategy status even during partial disconnections, since
        unaffected strategies may still be trading.
        """
        # Refresh connector readiness cache for accurate status display
        # (format_status may be called outside of tick cycle)
        self._refresh_connector_ready_cache()

        # Check if we've completed initial startup
        if not self._markets_ready_notified:
            # Initial startup - still waiting for first coordinated initialization
            not_ready = [name for name, ready in self._connector_ready_cache.items() if not ready]
            if not_ready:
                return "\n".join(["Waiting for connectors to initialize:"] + [f"  {n}" for n in not_ready])
            return "Market connectors are initializing..."

        lines = []

        # Connector Status Summary using cached values
        all_connectors_ready = all(self._connector_ready_cache.values()) if self._connector_ready_cache else False

        # Strategy Status Summary
        # During normal operation: simple count of non-paused strategies
        # During partial disconnection: show which connectors are down
        if not all_connectors_ready:
            # Partial disconnection - show detailed breakdown
            not_ready = [name for name, ready in self._connector_ready_cache.items() if not ready]
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
        hold_sections = []

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
                # Use cached readiness to find affected connectors
                affected_connectors = [
                    c.name for c in strategy_instance.connectors
                    if not self._connector_ready_cache.get(c.name, False)
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

            trade_count = 0
            try:
                if hasattr(strategy_instance.strategy, 'total_trades'):
                    trade_count = strategy_instance.strategy.total_trades
            except Exception:
                pass

            rows.append({
                "markets": markets_str,
                "trades": str(trade_count),
                "min": f"min {min_prof_str}" if min_prof_str is not None else "",
                "best": best_prof_str,
                "issues": self._check_market_issues(strategy_instance)
            })


            if status_blob is not None:
                bi_lines = self._extract_buyin_lines(status_blob)
                if bi_lines:
                    buyin_sections.append((strategy_name, bi_lines))
                hd_lines = self._extract_hold_lines(status_blob)
                if hd_lines:
                    hold_sections.append((strategy_name, hd_lines))

        if rows:
            # Calculate max widths
            max_markets_len = max([len(r["markets"]) for r in rows]) if rows else 0
            max_trades_len = max([len(r["trades"]) for r in rows]) if rows else 0
            max_min_len = max([len(r["min"]) for r in rows]) if rows else 0
            
            for r in rows:
                line = f"{r['markets']:<{max_markets_len}}  {r['trades']:<{max_trades_len}}  {r['min']:<{max_min_len}}  {r['best']}"
                if r['issues']:
                     line += f"  ⚠ {', '.join(r['issues'])}"
                lines.append(line)

        if buyin_sections:
            lines.append("\nPosition Balancer active:")
            # Truncate names to 6 chars for label column; pad to align all rows
            bi_labels = [name[:6] for name, _ in buyin_sections]
            bi_w = max(len(l) for l in bi_labels) if bi_labels else 6
            for (name, blines), label in zip(buyin_sections, bi_labels):
                header = re.sub(r"^Position Balancer:\s*", "", blines[0]).strip()
                rest = "  |  ".join(blines[1:]) if len(blines) > 1 else ""
                compact = f"  {label:<{bi_w}}  {header}"
                if rest:
                    compact += f"  |  {rest}"
                lines.append(compact)

        if hold_sections:
            lines.append("\nHold-band guardrail active:")
            hd_labels = [name[:6] for name, _ in hold_sections]
            hd_w = max(len(l) for l in hd_labels) if hd_labels else 6
            # Parse structured fields from each row so we can compute column widths
            # hlines[0]: "Hold-band guardrail:  target=$X  band=±$Y  range=[$A, $B]"
            # hlines[1]: "total=$T  base=...  bid=...  correcting → ..."
            hd_parsed = []
            for name, hlines in hold_sections:
                h0 = re.sub(r"^Hold-band guardrail:\s*", "", hlines[0]).strip()
                m_target = re.search(r"target=\$(\d+)", h0)
                m_band   = re.search(r"band=±\$(\d+)", h0)
                m_range  = re.search(r"range=(\[.*?\])", h0)
                h1 = hlines[1] if len(hlines) > 1 else ""
                m_total  = re.search(r"total=(\S+)", h1)
                # Everything after "base=" is not aligned — keep as raw suffix
                m_base   = re.search(r"(base=.*)", h1)
                hd_parsed.append({
                    "label":  name[:6],
                    "target": m_target.group(1) if m_target else "?",
                    "band":   m_band.group(1)   if m_band   else "?",
                    "range":  m_range.group(1)  if m_range  else "?",
                    "total":  m_total.group(1)  if m_total  else "?",
                    "suffix": m_base.group(1)   if m_base   else h1,
                })
            # Compute per-column max widths
            tw = max(len(r["target"]) for r in hd_parsed)
            rw = max(len(r["range"])  for r in hd_parsed)
            ow = max(len(r["total"])  for r in hd_parsed)
            # Extract direction tag (BUILDING/REDUCING ...) from suffix for alignment
            for r in hd_parsed:
                dm = re.search(r"(correcting\s*→\s*\S+.*)", r["suffix"])
                r["direction"] = dm.group(1).strip() if dm else ""
                # Trim suffix to just base= part (drop correcting onwards)
                r["base_part"] = re.sub(r"\s*correcting.*", "", r["suffix"]).strip()
            dw = max(len(r["direction"]) for r in hd_parsed) if hd_parsed else 0
            for r in hd_parsed:
                line = (
                    f"  {r['label']:<{hd_w}}"
                    f"  target=${r['target']:>{tw}}"
                    f"  band=±${r['band']}"
                    f"  range={r['range']:<{rw}}"
                    f"  |  total={r['total']:>{ow}}"
                    f"  {r['base_part']}"
                    f"  {r['direction']}"
                )
                lines.append(line)

        # Pending Orders (aggregated across all strategies)
        pending_orders_info = self._get_pending_orders_summary()
        if pending_orders_info:
            lines.append("\nPending Orders:")
            lines.extend([f"  {line}" for line in pending_orders_info])

        # Only show connectors not ready; omit when all ready (using cached values)
        not_ready = [name for name, ready in self._connector_ready_cache.items() if not ready]
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

        # Build a flat list of all orders for column-width calculation
        # Each entry: (strategy_name, order_type, trading_pair, exchange, amount, price, order_id)
        flat: list = []
        for (exchange, trading_pair, strategy_name), orders in orders_by_market.items():
            ex_short = exchange.split("_")[0]  # gate_io -> gate
            pair_short = trading_pair.split("-")[0][:6]  # FUNTOKEN-USDT -> FUNTOK
            market_str = f"{pair_short}@{ex_short}"
            for order_type, price, amount, order_id in orders:
                flat.append((strategy_name, order_type, market_str, amount, price, order_id))

        # Sort: strategy name, then market, then side
        flat.sort(key=lambda x: (x[0], x[2], x[1]))

        total_orders = len(flat)
        # Column widths
        name_w = min(max(len(r[0][:6]) for r in flat), 6) if flat else 6
        mkt_w  = max(len(r[2]) for r in flat) if flat else 12

        lines = [f"Total: {total_orders} order(s)"]
        for strategy_name, order_type, market_str, amount, price, order_id in flat:
            label = strategy_name[:6]
            short_id = f"#{order_id[-6:]}" if len(order_id) > 6 else f"#{order_id}"
            if price == "MARKET":
                price_str = "MARKET      "
            else:
                price_str = f"{price:<12.8g}"
            amt_str = f"{amount:>14.6g}"
            lines.append(f"  {label:<{name_w}}  {order_type:<4}  {market_str:<{mkt_w}}  {amt_str} @ {price_str}  {short_id}")

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

    def _extract_hold_lines(self, status_blob: str) -> List[str]:
        """Extract hold-band guardrail lines from a strategy's format_status output.
        Returns [] when hold-band is disabled or cache not yet warm."""
        if not status_blob:
            return []
        raw_lines = status_blob.split("\n")
        out: List[str] = []
        collecting = False
        for raw in raw_lines:
            s = raw.strip()
            if s.startswith("Hold-band guardrail:"):
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