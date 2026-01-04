"""
AMM Arbitrage Strategy V2 - CEX-DEX Arbitrage with Limit Orders on CEX

This is a modern POC arbitrage strategy that:
1. Uses LIMIT orders on CEX (maker side) for better execution
2. Uses MARKET orders on DEX/Gateway (taker side) for immediate fills
3. Is designed for CEX <-> DEX arbitrage via Gateway connectors
4. Has a simple but scalable structure (orchestrator-compatible)

Key Differences from older amm_arb:
- Limit orders on CEX side for precision (like arbitrage_l)
- Async price fetching from Gateway DEX
- Modern Pydantic configuration
- ScriptStrategyBase for simpler lifecycle management
- Rate oracle integration for cross-asset conversion
- Budget constraint checking before order placement
- VWAP-based pricing for CEX

Architecture:
------------
1. CEX Connector: Regular exchange connector (binance, okx, etc.)
   - Uses limit orders for better fill rates
   - VWAP-based pricing for accurate execution
   - Synchronous order book access

2. DEX Connector: Gateway connector (uniswap/amm, pancakeswap/amm, etc.)
   - Uses market orders (swaps) for immediate execution
   - Async quote fetching via Gateway API
   - Automatic slippage handling

Arbitrage Logic:
---------------
- Monitor both markets for price discrepancies
- When profitable: place limit buy on CEX, market sell on DEX (or vice versa)
- Account for trading fees on CEX, gas fees on DEX
- Slippage buffers for DEX execution
- Budget constraint checking to ensure sufficient balance

Example Usage:
-------------
1. Configure in conf/scripts/conf_amm_arb_v2_example.yml
2. Run: start --script amm_arb_v2.py --config conf_amm_arb_v2_example.yml

Flow: Config Creation -> Script Loading -> Connector Initialization -> Runtime Loop
--------------------------------------------------------------------------------------
1. User creates conf/scripts/conf_amm_arb_v2_example.yml with strategy parameters
2. Hummingbot loads the script and parses config via AmmArbV2Config (Pydantic)
3. init_markets(config) is called to define required connectors and trading pairs
4. Connectors are initialized by the framework (CEX via API, DEX via Gateway)
5. __init__(connectors, config) creates strategy instance with market references
6. tick() is called every second:
   - Check connector readiness
   - Fetch DEX quotes via Gateway async
   - Find profitable arbitrage opportunities
   - Execute trades via buy()/sell() methods
7. Orders are placed:
   - CEX: Via connector's buy/sell methods (limit orders)
   - DEX: Via Gateway's execute_swap (converted from limit to swap internally)
8. Events (did_fill_order, etc.) update trade tracking state

Future Orchestrator Integration:
-------------------------------
This strategy's structure is designed to be easily integrated with
multi_strategy_orchestrator.py by:
- Using MarketTradingPairTuple for market representation
- Having clear init_params pattern
- Supporting orchestrated_mode flag
- Tracking pending orders per market
"""

import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Optional, Set

import pandas as pd
from pydantic import ConfigDict, Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


# ============================================================================
# Configuration
# ============================================================================

class AmmArbV2Config(BaseClientModel):
    """
    Configuration for CEX-DEX AMM Arbitrage Strategy V2.
    
    This strategy arbitrages between a CEX (using limit orders) and 
    a DEX via Gateway (using market/swap orders).
    
    Configuration is loaded from YAML files in conf/scripts/
    
    Note: extra="ignore" allows trading_core to inject config_file_path
    without causing validation errors.
    """
    # Allow extra fields (trading_core injects config_file_path)
    model_config = ConfigDict(extra="ignore")
    
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    
    # CEX Configuration (maker side - limit orders)
    # Prompt contains "exchange name" to trigger connector autocomplete
    cex_connector: str = Field(
        "binance",
        json_schema_extra={
            "prompt": "Enter CEX exchange name (e.g., binance, okx, gate_io)",
            "prompt_on_new": True
        }
    )
    # Prompt contains "trading pair" to trigger trading pair autocomplete
    cex_trading_pair: str = Field(
        "ETH-USDT",
        json_schema_extra={
            "prompt": "Enter CEX trading pair (e.g., ETH-USDT)",
            "prompt_on_new": True
        }
    )
    
    # DEX Configuration (taker side - market/swap orders)
    # Prompt contains "exchange name" for connector autocomplete (includes Gateway)
    dex_connector: str = Field(
        "uniswap/amm",
        json_schema_extra={
            "prompt": "Enter DEX exchange name (Exchange/AMM) (e.g., uniswap/amm, pancakeswap/router)",
            "prompt_on_new": True
        }
    )
    # Prompt contains "trading pair" to trigger trading pair autocomplete
    dex_trading_pair: str = Field(
        "WETH-USDC",
        json_schema_extra={
            "prompt": "Enter DEX trading pair (e.g., WETH-USDC, WBNB-USDT)",
            "prompt_on_new": True
        }
    )
    
    # Order Configuration
    order_amount: Decimal = Field(
        Decimal("0.1"),
        json_schema_extra={"prompt": "Order amount in base asset", "prompt_on_new": True}
    )
    min_profitability: Decimal = Field(
        Decimal("0.5"),
        json_schema_extra={"prompt": "Minimum profitability percentage (e.g., 0.5 for 0.5%)", "prompt_on_new": True}
    )
    
    # Slippage Configuration
    cex_slippage_buffer: Decimal = Field(
        Decimal("0.1"),
        json_schema_extra={"prompt": "CEX slippage buffer percentage", "prompt_on_new": False}
    )
    dex_slippage_buffer: Decimal = Field(
        Decimal("1.0"),
        json_schema_extra={"prompt": "DEX slippage buffer percentage", "prompt_on_new": False}
    )
    
    # Timing Configuration
    order_refresh_interval: float = Field(
        60.0,
        json_schema_extra={"prompt": "Order refresh interval in seconds", "prompt_on_new": False}
    )
    quote_refresh_interval: float = Field(
        5.0,
        json_schema_extra={"prompt": "DEX quote refresh interval in seconds", "prompt_on_new": False}
    )
    next_trade_delay: float = Field(
        5.0,
        json_schema_extra={"prompt": "Cooldown delay between trades in seconds", "prompt_on_new": False}
    )
    
    # Conversion Rate Configuration
    use_oracle_conversion_rate: bool = Field(
        True,
        json_schema_extra={"prompt": "Use oracle for cross-asset conversion rates? (True/False)", "prompt_on_new": False}
    )
    quote_conversion_rate: Decimal = Field(
        Decimal("1.0"),
        json_schema_extra={"prompt": "Fixed quote conversion rate (if not using oracle)", "prompt_on_new": False}
    )
    
    # DEX Pool Configuration (optional - for explicit pool targeting)
    dex_pool_address: Optional[str] = Field(
        None,
        json_schema_extra={
            "prompt": "DEX pool address (optional, leave empty for auto-discovery)",
            "prompt_on_new": False
        }
    )
    

# ============================================================================
# Data Types
# ============================================================================

class ArbDirection(Enum):
    """Direction of arbitrage opportunity."""
    BUY_CEX_SELL_DEX = "buy_cex_sell_dex"  # Buy on CEX, sell on DEX
    BUY_DEX_SELL_CEX = "buy_dex_sell_cex"  # Buy on DEX, sell on CEX
    NONE = "none"


@dataclass
class ArbOpportunity:
    """
    Represents an arbitrage opportunity with calculated profitability.
    
    This is similar to ArbProposal in amm_arb but simplified for CEX-DEX.
    """
    direction: ArbDirection
    cex_price: Decimal
    dex_price: Decimal
    profitability_pct: Decimal  # After fees
    amount: Decimal
    cex_order_price: Decimal  # Adjusted for slippage
    dex_order_price: Decimal  # Adjusted for slippage
    estimated_gas_cost: Decimal = Decimal("0")
    cex_fee_pct: Decimal = Decimal("0")
    
    @property
    def is_profitable(self) -> bool:
        return self.profitability_pct > Decimal("0")
    
    def __str__(self) -> str:
        return (
            f"{self.direction.value}: CEX={self.cex_price:.6f}, DEX={self.dex_price:.6f}, "
            f"Profit={self.profitability_pct:.4f}% (after {self.cex_fee_pct:.3f}% CEX fee)"
        )


@dataclass
class PendingArbTrade:
    """
    Tracks a pending arbitrage trade across both legs.
    
    Similar to order tracking in arbitrage_l but simplified for two markets.
    """
    cex_order_id: Optional[str] = None
    dex_order_id: Optional[str] = None
    direction: ArbDirection = ArbDirection.NONE
    amount: Decimal = Decimal("0")
    cex_filled: bool = False
    dex_filled: bool = False
    cex_fill_price: Optional[Decimal] = None
    dex_fill_price: Optional[Decimal] = None
    created_at: float = 0.0
    
    @property
    def is_complete(self) -> bool:
        return self.cex_filled and self.dex_filled
    
    @property
    def is_partial(self) -> bool:
        return self.cex_filled != self.dex_filled


# ============================================================================
# Strategy Implementation
# ============================================================================

class AmmArbV2(ScriptStrategyBase):
    """
    CEX-DEX AMM Arbitrage Strategy V2.
    
    This strategy monitors price discrepancies between a CEX and DEX,
    placing limit orders on CEX and market orders on DEX when profitable.
    
    Follows patterns from:
    - arbitrage_l: Limit order placement, profitability calculation
    - amm_arb: Gateway integration, slippage handling
    - xrpl_arb_example: OrderCandidate pattern, budget checking
    """
    
    # Class-level markets definition (set by init_markets)
    markets: Dict[str, Set[str]] = {}
    
    @classmethod
    def init_markets(cls, config: AmmArbV2Config):
        """
        Initialize required market connectors.
        
        This is called by Hummingbot's start command before __init__.
        It tells the framework which connectors and trading pairs to initialize.
        
        For Gateway connectors (like uniswap/amm), this triggers:
        1. GatewayHttpClient connection to Gateway service
        2. Chain/network detection for the connector
        3. Wallet address loading from Gateway config
        4. Token data loading for amount quantization
        """
        cls.markets = {
            config.cex_connector: {config.cex_trading_pair},
            config.dex_connector: {config.dex_trading_pair}
        }
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: AmmArbV2Config):
        """
        Initialize the strategy with connectors and configuration.
        
        At this point:
        - CEX connector is connected to exchange API with order book data
        - DEX/Gateway connector is connected via Gateway HTTP client
        - Both have balance data available (after ready=True)
        
        Args:
            connectors: Dict mapping connector names to connector instances
            config: Strategy configuration (parsed from YAML)
        """
        super().__init__(connectors, config)
        self.config = config
        
        # Parse trading pairs
        self.cex_base, self.cex_quote = config.cex_trading_pair.split("-")
        self.dex_base, self.dex_quote = config.dex_trading_pair.split("-")
        
        # Validate connector types
        self._is_dex_gateway = self._check_is_gateway_connector(config.dex_connector)
        if not self._is_dex_gateway:
            self.logger().warning(
                f"{config.dex_connector} may not be a Gateway connector. "
                f"DEX orders require Gateway for swap execution."
            )
        
        # State tracking
        self._dex_buy_price: Optional[Decimal] = None
        self._dex_sell_price: Optional[Decimal] = None
        self._last_quote_fetch: float = 0
        self._quote_fetch_in_progress: bool = False
        
        # Order tracking
        self._pending_trade: Optional[PendingArbTrade] = None
        self._pending_cex_orders: Set[str] = set()
        self._pending_dex_orders: Set[str] = set()
        self._last_trade_timestamp: float = 0
        
        # Performance tracking
        self._arb_opportunities_found: int = 0
        self._trades_executed: int = 0
        self._total_profit: Decimal = Decimal("0")
        
        # Rate conversion source
        self._rate_source: Optional[RateOracle] = None
        if config.use_oracle_conversion_rate:
            self._rate_source = RateOracle.get_instance()
        
        self.logger().info(
            f"AmmArbV2 initialized:\n"
            f"  CEX: {config.cex_connector}/{config.cex_trading_pair}\n"
            f"  DEX: {config.dex_connector}/{config.dex_trading_pair} (Gateway={self._is_dex_gateway})\n"
            f"  Order Amount: {config.order_amount}\n"
            f"  Min Profitability: {config.min_profitability}%"
        )
    
    # ========================================================================
    # Core Trading Logic
    # ========================================================================
    
    def on_tick(self):
        """
        Main strategy tick - called every second by the clock.
        
        Flow:
        1. Check if all connectors are ready
        2. Refresh DEX quotes if needed (async via Gateway)
        3. Check for active pending trade
        4. Find arbitrage opportunity
        5. Execute if profitable
        """
        # Check readiness
        if not self._all_connectors_ready():
            return
        
        # Refresh DEX quotes periodically
        current_time = self.current_timestamp
        if current_time - self._last_quote_fetch >= self.config.quote_refresh_interval:
            if not self._quote_fetch_in_progress:
                self._quote_fetch_in_progress = True
                # Async fetch via Gateway API (see gateway_swap.py get_quote_price)
                safe_ensure_future(self._fetch_dex_quotes())
        
        # Skip if we have an active trade pending
        if self._pending_trade is not None and not self._pending_trade.is_complete:
            self._check_pending_trade_status()
            return
        
        # Check trade cooldown (like arbitrage_l's next_trade_delay)
        if current_time - self._last_trade_timestamp < self.config.next_trade_delay:
            return
        
        # Skip if we don't have DEX quotes yet
        if self._dex_buy_price is None or self._dex_sell_price is None:
            return
        
        # Find arbitrage opportunity
        opportunity = self._find_arbitrage_opportunity()
        if opportunity is not None and opportunity.is_profitable:
            self._arb_opportunities_found += 1
            
            if opportunity.profitability_pct >= self.config.min_profitability:
                self.logger().info(f"Profitable opportunity found: {opportunity}")
                safe_ensure_future(self._execute_arbitrage(opportunity))
    
    async def _fetch_dex_quotes(self):
        """
        Fetch buy and sell quotes from DEX via Gateway.
        
        This calls GatewaySwap.get_quote_price() which:
        1. Sends request to Gateway's /connectors/{connector}/quote-swap endpoint
        2. Gateway queries the DEX (e.g., Uniswap router) for swap quote
        3. Returns the price with slippage consideration
        
        The quotes are cached for quote_refresh_interval seconds.
        """
        try:
            dex_connector = self.connectors.get(self.config.dex_connector)
            if dex_connector is None:
                self.logger().warning(f"DEX connector {self.config.dex_connector} not found")
                return
            
            # Check if connector has get_quote_price method (Gateway connectors)
            if not hasattr(dex_connector, 'get_quote_price'):
                self.logger().warning(
                    f"{self.config.dex_connector} does not support get_quote_price. "
                    f"Using get_price fallback."
                )
                # Fallback for non-Gateway connectors
                buy_quote = dex_connector.get_price(self.config.dex_trading_pair, is_buy=True)
                sell_quote = dex_connector.get_price(self.config.dex_trading_pair, is_buy=False)
            else:
                # Gateway connectors: async quote with amount consideration
                # This accounts for price impact based on trade size
                # If dex_pool_address is specified, use that specific pool
                pool_address = self.config.dex_pool_address if self.config.dex_pool_address else None
                
                buy_quote = await dex_connector.get_quote_price(
                    self.config.dex_trading_pair,
                    is_buy=True,
                    amount=self.config.order_amount,
                    pool_address=pool_address
                )
                
                sell_quote = await dex_connector.get_quote_price(
                    self.config.dex_trading_pair,
                    is_buy=False,
                    amount=self.config.order_amount,
                    pool_address=pool_address
                )
            
            if buy_quote is not None and sell_quote is not None:
                buy_price = Decimal(str(buy_quote))
                sell_price = Decimal(str(sell_quote))
                
                # CRITICAL: Detect and correct inverted quotes
                # Gateway sometimes returns "how many base tokens per quote token" 
                # instead of "quote tokens per base token" for BUY side
                # 
                # Detection: if buy_price / sell_price > 1000, the buy is likely inverted
                # Example: WKC-WBNB
                #   Correct: buy=0.00000000077 (WBNB per WKC), sell=0.00000000076
                #   Inverted: buy=12999970041 (WKC per WBNB), sell=0.00000000076
                
                if buy_price > 0 and sell_price > 0:
                    price_ratio = buy_price / sell_price
                    
                    if price_ratio > Decimal("100"):
                        # Buy quote is likely inverted - it's returning WKC/WBNB instead of WBNB/WKC
                        self.logger().warning(
                            f"DEX buy quote appears inverted (ratio={price_ratio:.2f}). "
                            f"Correcting: 1/{buy_price} = {1/buy_price}"
                        )
                        buy_price = Decimal("1") / buy_price
                    
                    elif price_ratio < Decimal("0.01"):
                        # Sell quote might be inverted
                        self.logger().warning(
                            f"DEX sell quote appears inverted (ratio={price_ratio:.6f}). "
                            f"Correcting: 1/{sell_price} = {1/sell_price}"
                        )
                        sell_price = Decimal("1") / sell_price
                
                self._dex_buy_price = buy_price
                self._dex_sell_price = sell_price
                self._last_quote_fetch = self.current_timestamp
                
                # Log the corrected prices
                self.logger().info(
                    f"DEX quotes for {self.config.dex_trading_pair}: "
                    f"Buy={self._dex_buy_price} {self.dex_quote}/{self.dex_base}, "
                    f"Sell={self._dex_sell_price} {self.dex_quote}/{self.dex_base}"
                )
                
                # Final sanity check
                if self._dex_buy_price <= 0 or self._dex_sell_price <= 0:
                    self.logger().warning(
                        f"DEX quote has zero/negative price! Buy={self._dex_buy_price}, Sell={self._dex_sell_price}"
                    )
                
            else:
                self.logger().warning(
                    f"Failed to get DEX quotes: buy={buy_quote}, sell={sell_quote}"
                )
                
        except Exception as e:
            self.logger().error(f"Error fetching DEX quotes: {e}", exc_info=True)
        finally:
            self._quote_fetch_in_progress = False
    
    def _find_arbitrage_opportunity(self) -> Optional[ArbOpportunity]:
        """
        Analyze prices and find best arbitrage opportunity.
        
        This follows the pattern from arbitrage_l's c_calculate_profitability:
        1. Get prices from both markets
        2. Apply conversion rates if quote assets differ
        3. Calculate profitability for both directions
        4. Account for trading fees
        5. Return best profitable opportunity
        """
        cex_connector = self.connectors.get(self.config.cex_connector)
        if cex_connector is None:
            return None
        
        # Get CEX prices using VWAP if available (like xrpl_arb_example)
        # Fall back to top-of-book price
        try:
            if hasattr(cex_connector, 'get_vwap_for_volume'):
                # VWAP for more accurate execution price
                vwap_buy = cex_connector.get_vwap_for_volume(
                    self.config.cex_trading_pair, True, self.config.order_amount
                )
                vwap_sell = cex_connector.get_vwap_for_volume(
                    self.config.cex_trading_pair, False, self.config.order_amount
                )
                cex_ask = vwap_buy.result_price if vwap_buy else None
                cex_bid = vwap_sell.result_price if vwap_sell else None
            else:
                # Top-of-book fallback
                cex_bid = cex_connector.get_price(self.config.cex_trading_pair, is_buy=False)
                cex_ask = cex_connector.get_price(self.config.cex_trading_pair, is_buy=True)
        except Exception as e:
            self.logger().debug(f"Error getting CEX prices: {e}")
            cex_bid = cex_connector.get_price(self.config.cex_trading_pair, is_buy=False)
            cex_ask = cex_connector.get_price(self.config.cex_trading_pair, is_buy=True)
        
        if cex_bid is None or cex_ask is None:
            return None
        
        cex_bid = Decimal(str(cex_bid))
        cex_ask = Decimal(str(cex_ask))
        
        # Get CEX trading fee (like arbitrage_l)
        cex_fee_pct = self._get_cex_fee_percent()
        
        # Apply quote conversion if assets differ (like arbitrage_l's conversion rate)
        dex_buy_converted = self._convert_dex_price(self._dex_buy_price)
        dex_sell_converted = self._convert_dex_price(self._dex_sell_price)
        
        # Get estimated gas cost in quote currency
        gas_cost = self._get_estimated_gas_cost()
        
        # Calculate profitability for both directions (after fees)
        # Direction 1: Buy on CEX, Sell on DEX
        # Net proceeds = DEX_sell * (1 - dex_fee) - Gas
        # Cost = CEX_ask * (1 + cex_fee)
        # Profit = (Net proceeds / Cost - 1) * 100
        cex_buy_cost = cex_ask * (Decimal("1") + cex_fee_pct / Decimal("100"))
        dex_sell_proceeds = dex_sell_converted  # DEX fee is included in quote price
        profit_buy_cex_sell_dex = ((dex_sell_proceeds - gas_cost) / cex_buy_cost - Decimal("1")) * Decimal("100")
        
        # Direction 2: Buy on DEX, Sell on CEX
        # Net proceeds = CEX_bid * (1 - cex_fee)
        # Cost = DEX_buy + Gas
        cex_sell_proceeds = cex_bid * (Decimal("1") - cex_fee_pct / Decimal("100"))
        dex_buy_cost = dex_buy_converted + gas_cost
        profit_buy_dex_sell_cex = (cex_sell_proceeds / dex_buy_cost - Decimal("1")) * Decimal("100")
        
        # Choose better direction (like arbitrage_l's direction selection)
        if profit_buy_cex_sell_dex > profit_buy_dex_sell_cex and profit_buy_cex_sell_dex > Decimal("0"):
            # Apply slippage buffers for order placement
            cex_order_price = cex_ask * (Decimal("1") + self.config.cex_slippage_buffer / Decimal("100"))
            dex_order_price = self._dex_sell_price * (Decimal("1") - self.config.dex_slippage_buffer / Decimal("100"))
            
            return ArbOpportunity(
                direction=ArbDirection.BUY_CEX_SELL_DEX,
                cex_price=cex_ask,
                dex_price=self._dex_sell_price,
                profitability_pct=profit_buy_cex_sell_dex,
                amount=self.config.order_amount,
                cex_order_price=cex_order_price,
                dex_order_price=dex_order_price,
                estimated_gas_cost=gas_cost,
                cex_fee_pct=cex_fee_pct
            )
        
        elif profit_buy_dex_sell_cex > Decimal("0"):
            # Apply slippage buffers
            cex_order_price = cex_bid * (Decimal("1") - self.config.cex_slippage_buffer / Decimal("100"))
            dex_order_price = self._dex_buy_price * (Decimal("1") + self.config.dex_slippage_buffer / Decimal("100"))
            
            return ArbOpportunity(
                direction=ArbDirection.BUY_DEX_SELL_CEX,
                cex_price=cex_bid,
                dex_price=self._dex_buy_price,
                profitability_pct=profit_buy_dex_sell_cex,
                amount=self.config.order_amount,
                cex_order_price=cex_order_price,
                dex_order_price=dex_order_price,
                estimated_gas_cost=gas_cost,
                cex_fee_pct=cex_fee_pct
            )
        
        return None
    
    async def _execute_arbitrage(self, opportunity: ArbOpportunity):
        """
        Execute arbitrage trade on both legs.
        
        Order placement flow:
        1. Create OrderCandidate objects (like xrpl_arb_example)
        2. Adjust to budget constraints
        3. Place limit order on CEX via connector's buy/sell
        4. Place swap order on DEX via Gateway
        
        CEX Order: Uses ScriptStrategyBase.buy/sell -> buy_with_specific_market
                   -> connector.buy() which creates the order on exchange
        
        DEX Order: Uses ScriptStrategyBase.buy/sell -> GatewaySwap.buy/sell
                   -> GatewaySwap.place_order -> GatewaySwap._create_order
                   -> GatewayHttpClient.execute_swap (calls Gateway API)
        """
        self.logger().info(f"Executing arbitrage: {opportunity}")
        
        # Create order candidates (like xrpl_arb_example pattern)
        cex_candidate, dex_candidate = self._create_order_candidates(opportunity)
        
        # Adjust to budget constraints
        cex_candidate = self._adjust_to_budget(self.config.cex_connector, cex_candidate)
        dex_candidate = self._adjust_to_budget(self.config.dex_connector, dex_candidate)
        
        if cex_candidate.amount <= Decimal("0") or dex_candidate.amount <= Decimal("0"):
            self.logger().warning("Order amount reduced to zero by budget check - skipping")
            return
        
        # Initialize pending trade tracker
        self._pending_trade = PendingArbTrade(
            direction=opportunity.direction,
            amount=cex_candidate.amount,
            created_at=self.current_timestamp
        )
        
        try:
            # Place orders based on direction
            if opportunity.direction == ArbDirection.BUY_CEX_SELL_DEX:
                # Place limit buy on CEX first
                cex_order_id = self._place_order(self.config.cex_connector, cex_candidate)
                self._pending_trade.cex_order_id = cex_order_id
                self._pending_cex_orders.add(cex_order_id)
                
                # Place swap sell on DEX
                dex_order_id = self._place_order(self.config.dex_connector, dex_candidate)
                self._pending_trade.dex_order_id = dex_order_id
                self._pending_dex_orders.add(dex_order_id)
                
            else:  # BUY_DEX_SELL_CEX
                # Place swap buy on DEX first
                dex_order_id = self._place_order(self.config.dex_connector, dex_candidate)
                self._pending_trade.dex_order_id = dex_order_id
                self._pending_dex_orders.add(dex_order_id)
                
                # Place limit sell on CEX
                cex_order_id = self._place_order(self.config.cex_connector, cex_candidate)
                self._pending_trade.cex_order_id = cex_order_id
                self._pending_cex_orders.add(cex_order_id)
            
            self._trades_executed += 1
            self._last_trade_timestamp = self.current_timestamp
            
            self.logger().info(
                f"Arbitrage orders placed:\n"
                f"  CEX: {self._pending_trade.cex_order_id}\n"
                f"  DEX: {self._pending_trade.dex_order_id}"
            )
            
        except Exception as e:
            self.logger().error(f"Error executing arbitrage: {e}", exc_info=True)
            self._pending_trade = None
    
    def _create_order_candidates(self, opportunity: ArbOpportunity) -> tuple:
        """
        Create OrderCandidate objects for both legs.
        
        Uses the OrderCandidate pattern from xrpl_arb_example.
        This allows budget checking before order placement.
        """
        if opportunity.direction == ArbDirection.BUY_CEX_SELL_DEX:
            cex_candidate = OrderCandidate(
                trading_pair=self.config.cex_trading_pair,
                is_maker=True,  # Limit order
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY,
                amount=opportunity.amount,
                price=opportunity.cex_order_price,
            )
            dex_candidate = OrderCandidate(
                trading_pair=self.config.dex_trading_pair,
                is_maker=False,  # Market/swap order
                order_type=OrderType.LIMIT,  # Gateway converts to swap
                order_side=TradeType.SELL,
                amount=opportunity.amount,
                price=opportunity.dex_order_price,
            )
        else:  # BUY_DEX_SELL_CEX
            cex_candidate = OrderCandidate(
                trading_pair=self.config.cex_trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=opportunity.amount,
                price=opportunity.cex_order_price,
            )
            dex_candidate = OrderCandidate(
                trading_pair=self.config.dex_trading_pair,
                is_maker=False,
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY,
                amount=opportunity.amount,
                price=opportunity.dex_order_price,
            )
        
        return cex_candidate, dex_candidate
    
    def _adjust_to_budget(self, connector_name: str, candidate: OrderCandidate) -> OrderCandidate:
        """
        Adjust order candidate to available budget.
        
        Uses the budget_checker pattern from xrpl_arb_example.
        """
        connector = self.connectors.get(connector_name)
        if connector is None:
            return candidate
        
        if hasattr(connector, 'budget_checker'):
            return connector.budget_checker.adjust_candidate(candidate, all_or_none=False)
        
        return candidate
    
    def _place_order(self, connector_name: str, order: OrderCandidate) -> str:
        """
        Place an order using ScriptStrategyBase's buy/sell methods.
        
        For CEX: This calls ExchangePyBase.buy/sell -> creates order via REST API
        For DEX/Gateway: This calls GatewaySwap.buy/sell -> execute_swap via Gateway
        
        The Gateway connector (GatewaySwap) handles:
        1. Quantization of amount/price
        2. Order tracking via GatewayInFlightOrder
        3. Transaction monitoring for confirmation
        """
        connector = self.connectors[connector_name]
        
        # Quantize for CEX orders
        quantized_amount = connector.quantize_order_amount(
            order.trading_pair, order.amount
        )
        quantized_price = connector.quantize_order_price(
            order.trading_pair, order.price
        )
        
        if quantized_amount <= Decimal("0"):
            raise ValueError(f"Quantized amount is zero for {order.trading_pair}")
        
        if order.order_side == TradeType.BUY:
            order_id = self.buy(
                connector_name=connector_name,
                trading_pair=order.trading_pair,
                amount=quantized_amount,
                order_type=order.order_type,
                price=quantized_price
            )
        else:
            order_id = self.sell(
                connector_name=connector_name,
                trading_pair=order.trading_pair,
                amount=quantized_amount,
                order_type=order.order_type,
                price=quantized_price
            )
        
        side_str = "BUY" if order.order_side == TradeType.BUY else "SELL"
        self.logger().info(
            f"{connector_name} {side_str} order placed: "
            f"{quantized_amount} @ {quantized_price}, order_id={order_id}"
        )
        
        return order_id
    
    # ========================================================================
    # Helper Methods
    # ========================================================================
    
    def _all_connectors_ready(self) -> bool:
        """
        Check if all required connectors are ready.
        
        For CEX: Checks API connection and order book data
        For DEX/Gateway: Checks Gateway connection, chain info, and balances
        """
        for connector_name in [self.config.cex_connector, self.config.dex_connector]:
            connector = self.connectors.get(connector_name)
            if connector is None:
                return False
            if not connector.ready:
                return False
        return True
    
    @staticmethod
    @lru_cache(maxsize=10)
    def _check_is_gateway_connector(connector_name: str) -> bool:
        """
        Check if a connector is a Gateway connector.
        
        Gateway connectors are identified by being in GATEWAY_CONNECTORS list
        or having the format "protocol/type" (e.g., "uniswap/amm").
        """
        # Check by format
        if "/" in connector_name:
            return True
        
        # Check against registered Gateway connectors
        try:
            gateway_connectors = AllConnectorSettings.get_gateway_amm_connector_names()
            return connector_name in gateway_connectors
        except Exception:
            return "/" in connector_name
    
    def _convert_dex_price(self, dex_price: Decimal) -> Decimal:
        """
        Convert DEX price to CEX quote currency if needed.
        
        Uses RateOracle like arbitrage_l for cross-asset conversion.
        Examples:
        - CEX: ETH-USDT, DEX: WETH-USDC -> Convert USDC to USDT
        - CEX: WKC-USDT, DEX: WKC-WBNB -> Convert WBNB to USDT
        
        Handles wrapped token symbols (WBNB->BNB, WETH->ETH) since
        oracle typically uses unwrapped symbols.
        """
        if self.cex_quote == self.dex_quote:
            return dex_price
        
        if self._rate_source is not None:
            rate = self._get_conversion_rate(self.dex_quote, self.cex_quote)
            if rate is not None and rate > 0:
                return dex_price * rate
        
        # Use fixed rate as fallback
        return dex_price * self.config.quote_conversion_rate
    
    def _get_conversion_rate(self, from_token: str, to_token: str) -> Optional[Decimal]:
        """
        Get conversion rate between two tokens using RateOracle.
        
        Tries multiple pair variants to handle wrapped tokens:
        - Direct: WBNB-USDT
        - Unwrapped: BNB-USDT
        - Via intermediary: WBNB-BNB-USDT
        """
        if self._rate_source is None:
            return None
        
        # Unwrap token symbols (WBNB->BNB, WETH->ETH)
        from_unwrapped = self._unwrap_token(from_token)
        to_unwrapped = self._unwrap_token(to_token)
        
        # Try different pair variants
        pairs_to_try = [
            f"{from_token}-{to_token}",           # WBNB-USDT
            f"{from_unwrapped}-{to_token}",       # BNB-USDT  
            f"{from_token}-{to_unwrapped}",       # WBNB-USD
            f"{from_unwrapped}-{to_unwrapped}",   # BNB-USD
        ]
        
        for pair in pairs_to_try:
            rate = self._rate_source.get_pair_rate(pair)
            if rate is not None and rate > 0:
                self.logger().debug(f"Found oracle rate for {pair}: {rate}")
                return Decimal(str(rate))
        
        # If from_token is wrapped, try assuming 1:1 with unwrapped
        if from_token != from_unwrapped:
            # WBNB = BNB, so try BNB-USDT
            rate = self._rate_source.get_pair_rate(f"{from_unwrapped}-{to_token}")
            if rate is not None and rate > 0:
                self.logger().debug(f"Using {from_unwrapped}-{to_token} rate for {from_token}: {rate}")
                return Decimal(str(rate))
        
        self.logger().warning(
            f"Could not find oracle rate for {from_token}->{to_token}. "
            f"Tried: {pairs_to_try}"
        )
        return None
    
    def _unwrap_token(self, token: str) -> str:
        """
        Unwrap token symbol for oracle lookup.
        
        Examples:
        - WBNB -> BNB
        - WETH -> ETH  
        - WBTC -> BTC
        - WMATIC -> MATIC
        - USDT -> USDT (unchanged)
        """
        # Common wrapped token patterns
        wrapped_mapping = {
            'WBNB': 'BNB',
            'WETH': 'ETH',
            'WBTC': 'BTC',
            'WMATIC': 'MATIC',
            'WAVAX': 'AVAX',
            'WSOL': 'SOL',
            'WFTM': 'FTM',
            'WPOL': 'POL',
        }
        
        if token in wrapped_mapping:
            return wrapped_mapping[token]
        
        # Generic W-prefix pattern for other tokens
        if token.startswith('W') and len(token) > 1:
            # Check if removing W gives a valid-looking symbol (at least 2 chars)
            potential = token[1:]
            if len(potential) >= 2 and potential.isupper():
                return potential
        
        return token
    
    def _get_cex_fee_percent(self) -> Decimal:
        """
        Get CEX trading fee percentage.
        
        Uses connector's get_fee method like arbitrage_l.
        """
        try:
            cex_connector = self.connectors.get(self.config.cex_connector)
            if cex_connector is None:
                return Decimal("0.1")  # Default 0.1%
            
            fee = cex_connector.get_fee(
                base_currency=self.cex_base,
                quote_currency=self.cex_quote,
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY,
                amount=self.config.order_amount,
                price=Decimal("1"),  # Price doesn't affect percent fee
                is_maker=True
            )
            return fee.percent * Decimal("100")
        except Exception:
            return Decimal("0.1")  # Default 0.1%
    
    def _get_estimated_gas_cost(self) -> Decimal:
        """
        Get estimated gas cost from DEX connector in quote currency.
        
        Gateway connectors have network_transaction_fee property
        that contains the estimated gas cost.
        """
        dex_connector = self.connectors.get(self.config.dex_connector)
        if dex_connector is not None and hasattr(dex_connector, 'network_transaction_fee'):
            fee: TokenAmount = dex_connector.network_transaction_fee
            if fee is not None and fee.amount > Decimal("0"):
                # Convert fee to quote currency using rate oracle
                if self._rate_source is not None:
                    rate = self._rate_source.get_pair_rate(f"{fee.token}-{self.cex_quote}")
                    if rate is not None:
                        return Decimal(str(fee.amount)) * Decimal(str(rate))
                return Decimal(str(fee.amount))
        return Decimal("0")
    
    def _check_pending_trade_status(self):
        """Check and log status of pending trade."""
        if self._pending_trade is None:
            return
        
        trade = self._pending_trade
        age = self.current_timestamp - trade.created_at
        
        if trade.is_complete:
            # Calculate realized profit
            if trade.cex_fill_price and trade.dex_fill_price:
                if trade.direction == ArbDirection.BUY_CEX_SELL_DEX:
                    profit = (trade.dex_fill_price - trade.cex_fill_price) * trade.amount
                else:
                    profit = (trade.cex_fill_price - trade.dex_fill_price) * trade.amount
                self._total_profit += profit
            
            self.logger().info(
                f"Arbitrage trade complete:\n"
                f"  CEX filled @ {trade.cex_fill_price}\n"
                f"  DEX filled @ {trade.dex_fill_price}\n"
                f"  Total P&L: {self._total_profit:.4f}"
            )
            self._pending_trade = None
            
        elif trade.is_partial and age > 300:  # 5 minute timeout for partial fills
            self.logger().warning(
                f"Partial arbitrage trade timed out after {age:.0f}s:\n"
                f"  CEX filled: {trade.cex_filled}\n"
                f"  DEX filled: {trade.dex_filled}\n"
                f"  Manual intervention may be required!"
            )
            self._pending_trade = None
    
    # ========================================================================
    # Event Handlers  
    # ========================================================================
    
    def did_fill_order(self, event: OrderFilledEvent):
        """
        Handle order fill events.
        
        Called by the framework when an order is filled (partial or complete).
        Updates pending trade tracking.
        """
        order_id = event.order_id
        
        if self._pending_trade is not None:
            if order_id == self._pending_trade.cex_order_id:
                self._pending_trade.cex_filled = True
                self._pending_trade.cex_fill_price = Decimal(str(event.price))
                self._pending_cex_orders.discard(order_id)
                self.logger().info(
                    f"CEX order filled: {event.amount} @ {event.price} "
                    f"({event.trade_type.name})"
                )
            
            elif order_id == self._pending_trade.dex_order_id:
                self._pending_trade.dex_filled = True
                self._pending_trade.dex_fill_price = Decimal(str(event.price))
                self._pending_dex_orders.discard(order_id)
                self.logger().info(
                    f"DEX order filled: {event.amount} @ {event.price} "
                    f"({event.trade_type.name})"
                )
    
    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """Handle buy order completion."""
        self.logger().info(
            f"Buy order completed: {event.base_asset_amount} {event.base_asset} "
            f"on {event.exchange}"
        )
    
    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """Handle sell order completion."""
        self.logger().info(
            f"Sell order completed: {event.base_asset_amount} {event.base_asset} "
            f"on {event.exchange}"
        )
    
    def did_fail_order(self, event: MarketOrderFailureEvent):
        """
        Handle order failure.
        
        For Gateway/DEX orders, this might indicate:
        - Transaction reverted
        - Insufficient gas
        - Slippage exceeded
        """
        order_id = event.order_id
        self.logger().error(f"Order failed: {order_id}")
        
        # Clean up tracking
        self._pending_cex_orders.discard(order_id)
        self._pending_dex_orders.discard(order_id)
        
        if self._pending_trade is not None:
            if order_id in [self._pending_trade.cex_order_id, self._pending_trade.dex_order_id]:
                self.logger().error(
                    f"Arbitrage leg failed - partial fill state:\n"
                    f"  CEX filled: {self._pending_trade.cex_filled}\n"
                    f"  DEX filled: {self._pending_trade.dex_filled}\n"
                    f"  Manual intervention may be required!"
                )
                self._pending_trade = None
    
    def did_cancel_order(self, event: OrderCancelledEvent):
        """Handle order cancellation."""
        order_id = event.order_id
        self.logger().info(f"Order cancelled: {order_id}")
        
        self._pending_cex_orders.discard(order_id)
        self._pending_dex_orders.discard(order_id)
    
    # ========================================================================
    # Status Display
    # ========================================================================
    
    def format_status(self) -> str:
        """
        Format strategy status for display.
        
        Called when user runs 'status' command.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        lines = []
        
        # Header
        lines.append("=" * 60)
        lines.append("  AMM Arbitrage V2 - CEX-DEX Strategy")
        lines.append("=" * 60)
        
        # Configuration
        lines.append("")
        lines.append("Configuration:")
        lines.append(f"  CEX: {self.config.cex_connector} / {self.config.cex_trading_pair}")
        lines.append(f"  DEX: {self.config.dex_connector} / {self.config.dex_trading_pair}")
        lines.append(f"       (Gateway: {self._is_dex_gateway})")
        lines.append(f"  Order Amount: {self.config.order_amount}")
        lines.append(f"  Min Profitability: {self.config.min_profitability}%")
        
        # Balances
        lines.append("")
        lines.append("Balances:")
        balance_df = self.get_balance_df()
        for line in balance_df.to_string(index=False).split("\n"):
            lines.append(f"  {line}")
        
        # Current Prices - use dynamic formatting for micro-priced tokens
        lines.append("")
        lines.append("Current Prices:")
        
        cex_connector = self.connectors.get(self.config.cex_connector)
        cex_bid = None
        cex_ask = None
        if cex_connector:
            cex_bid = cex_connector.get_price(self.config.cex_trading_pair, is_buy=False)
            cex_ask = cex_connector.get_price(self.config.cex_trading_pair, is_buy=True)
            if cex_bid and cex_ask:
                # Use dynamic formatting for very small prices
                lines.append(f"  CEX: Bid={self._format_price(cex_bid)}  Ask={self._format_price(cex_ask)}")
            else:
                lines.append(f"  CEX: Prices not available (bid={cex_bid}, ask={cex_ask})")
        
        if self._dex_buy_price and self._dex_sell_price:
            lines.append(f"  DEX (raw {self.dex_quote}):")
            lines.append(f"       Buy={self._format_price(self._dex_buy_price)}  Sell={self._format_price(self._dex_sell_price)}")
            quote_age = self.current_timestamp - self._last_quote_fetch
            lines.append(f"       (Quote age: {quote_age:.1f}s)")
            
            # Show converted prices if quote currencies differ
            if self.cex_quote != self.dex_quote:
                dex_buy_converted = self._convert_dex_price(self._dex_buy_price)
                dex_sell_converted = self._convert_dex_price(self._dex_sell_price)
                lines.append(f"  DEX (converted to {self.cex_quote}):")
                lines.append(f"       Buy={self._format_price(dex_buy_converted)}  Sell={self._format_price(dex_sell_converted)}")
        else:
            lines.append("  DEX: Fetching quotes...")
        
        # Oracle/Conversion Rates
        lines.append("")
        lines.append("Conversion Rates:")
        if self.cex_quote == self.dex_quote:
            lines.append(f"  Same quote currency ({self.cex_quote}) - no conversion needed")
        else:
            if self._rate_source:
                # Use the new conversion method that handles wrapped tokens
                rate = self._get_conversion_rate(self.dex_quote, self.cex_quote)
                unwrapped = self._unwrap_token(self.dex_quote)
                if rate:
                    if unwrapped != self.dex_quote:
                        lines.append(f"  Oracle {unwrapped}-{self.cex_quote}: {self._format_price(rate)} (via {self.dex_quote})")
                    else:
                        lines.append(f"  Oracle {self.dex_quote}-{self.cex_quote}: {self._format_price(rate)}")
                else:
                    lines.append(f"  Oracle {self.dex_quote}-{self.cex_quote}: NOT AVAILABLE ⚠️")
                    lines.append(f"  Tried unwrapped: {unwrapped}-{self.cex_quote}")
                    lines.append(f"  Using fixed rate: {self.config.quote_conversion_rate}")
            else:
                lines.append(f"  Oracle disabled, using fixed rate: {self.config.quote_conversion_rate}")
        
        # Gas estimate
        gas_cost = self._get_estimated_gas_cost()
        if gas_cost > Decimal("0"):
            lines.append(f"  Gas Est: {self._format_price(gas_cost)} {self.cex_quote}")
        
        # Profitability Analysis
        lines.append("")
        lines.append("Profitability Analysis:")
        
        # Only analyze if we have valid prices
        if cex_bid and cex_ask and self._dex_buy_price and self._dex_sell_price:
            opportunity = self._find_arbitrage_opportunity()
            if opportunity:
                lines.append(f"  Direction: {opportunity.direction.value}")
                lines.append(f"  Raw Profit: {opportunity.profitability_pct:.4f}%")
                lines.append(f"  CEX Fee: {opportunity.cex_fee_pct:.3f}%")
                lines.append(f"  Gas Cost: {self._format_price(opportunity.estimated_gas_cost)} {self.cex_quote}")
                lines.append(f"  Min Required: {self.config.min_profitability}%")
                
                if opportunity.profitability_pct >= self.config.min_profitability:
                    lines.append("  Status: ✅ PROFITABLE - Will execute")
                else:
                    lines.append("  Status: ⏳ Below threshold")
            else:
                # Show why no opportunity (detailed analysis)
                cex_bid_d = Decimal(str(cex_bid))
                cex_ask_d = Decimal(str(cex_ask))
                dex_buy_conv = self._convert_dex_price(self._dex_buy_price)
                dex_sell_conv = self._convert_dex_price(self._dex_sell_price)
                
                # Calculate raw spreads
                if cex_ask_d > 0:
                    spread1 = ((dex_sell_conv / cex_ask_d) - 1) * 100
                    lines.append(f"  Buy CEX→Sell DEX: {spread1:.4f}%")
                if dex_buy_conv > 0:
                    spread2 = ((cex_bid_d / dex_buy_conv) - 1) * 100
                    lines.append(f"  Buy DEX→Sell CEX: {spread2:.4f}%")
                
                lines.append("  Status: No profitable opportunity")
        else:
            lines.append("  Waiting for complete price data...")
            if not cex_bid or not cex_ask:
                lines.append("    - CEX prices missing")
            if not self._dex_buy_price or not self._dex_sell_price:
                lines.append("    - DEX quotes missing")
        
        # Statistics
        lines.append("")
        lines.append("Statistics:")
        lines.append(f"  Opportunities Found: {self._arb_opportunities_found}")
        lines.append(f"  Trades Executed: {self._trades_executed}")
        lines.append(f"  Total P&L: {self._format_price(self._total_profit)} {self.cex_quote}")
        
        # Pending Trade
        if self._pending_trade:
            lines.append("")
            lines.append("Active Trade:")
            lines.append(f"  Direction: {self._pending_trade.direction.value}")
            lines.append(f"  Amount: {self._pending_trade.amount}")
            lines.append(f"  CEX Order: {self._pending_trade.cex_order_id}")
            lines.append(f"    Filled: {self._pending_trade.cex_filled}")
            lines.append(f"  DEX Order: {self._pending_trade.dex_order_id}")
            lines.append(f"    Filled: {self._pending_trade.dex_filled}")
            age = self.current_timestamp - self._pending_trade.created_at
            lines.append(f"  Age: {age:.0f}s")
        
        return "\n".join(lines)
    
    def _format_price(self, price: Decimal) -> str:
        """
        Format price for display, handling both very small and very large values.
        
        For micro-priced tokens like WKC (0.00000007), standard formatting
        would show 0.000000 which is not useful.
        """
        if price is None:
            return "N/A"
        
        price = Decimal(str(price))
        
        if price == 0:
            return "0"
        
        abs_price = abs(price)
        
        # Very small prices (< 0.0001) - use scientific or more decimals
        if abs_price < Decimal("0.0001"):
            # Count leading zeros to determine precision
            price_str = f"{abs_price:.18f}".rstrip('0')
            # Find first non-zero digit position
            decimal_pos = price_str.find('.')
            first_sig = -1
            for i, c in enumerate(price_str[decimal_pos+1:]):
                if c != '0':
                    first_sig = i
                    break
            
            if first_sig >= 0:
                # Show 4 significant figures after first non-zero
                precision = first_sig + 5
                if precision > 18:
                    precision = 18
                formatted = f"{price:.{precision}f}"
                return formatted.rstrip('0').rstrip('.')
            return f"{price:.8e}"  # Fallback to scientific
        
        # Normal prices (0.0001 to 1000) - use 6 decimals
        elif abs_price < Decimal("1000"):
            formatted = f"{price:.6f}"
            return formatted.rstrip('0').rstrip('.')
        
        # Large prices (> 1000) - use 2 decimals
        elif abs_price < Decimal("1000000000"):
            return f"{price:,.2f}"
        
        # Very large prices - use scientific notation
        else:
            return f"{price:.4e}"
