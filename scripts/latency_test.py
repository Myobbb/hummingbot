"""
Multi-Exchange Latency Test Script

Measures order creation and cancellation latencies across multiple cryptocurrency exchanges.
Places unfillable limit orders (buy BTC at $66,420) and measures round-trip times.

Inspired by: https://github.com/supervik/crypto-exchanges-latencies-test

Usage:
    1. Configure API keys for target exchanges in Hummingbot
    2. Update EXCHANGES_TO_TEST list below
    3. Run: start --script latency_test.py
"""

import csv
import logging
import os
import time
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class OrderState(Enum):
    """Order lifecycle states for latency tracking."""
    PENDING_CREATE = "PENDING_CREATE"
    CREATED = "CREATED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELED = "CANCELED"


class MultiExchangeLatencyTest(ScriptStrategyBase):
    """
    Multi-exchange latency test script.
    
    Places unfillable limit buy orders at a very low price across multiple exchanges,
    then cancels them to measure creation and cancellation latencies.
    
    Results are saved to CSV files for analysis.
    """
    
    # ========== CONFIGURATION ==========
    
    # Exchanges to test - comment out any you don't have configured
    EXCHANGES_TO_TEST: List[str] = [
        "bybit",
        "kucoin",
        "gate_io",
        "mexc",
        "htx",
        "bitmart",
        "bing_x",
        "okx",
        "bitget",
    ]
    
    # Trading pair (same for all exchanges)
    TRADING_PAIR = "BTC-USDT"
    
    # Order parameters - unfillable buy at $66,420 (way below market)
    ORDER_PRICE = Decimal("69420")
    ORDER_QUOTE_AMOUNT = Decimal("25")  # $25 worth of BTC
    
    # Timing configuration
    TEST_INTERVAL_SECONDS = 60  # How often to run a test cycle
    CANCEL_DELAY_SECONDS = 5    # Wait before canceling (to ensure order is created)
    
    # Output configuration
    OUTPUT_DIR = "data/latency"
    CSV_FILE_ID = "latency_test"
    
    # ========== END CONFIGURATION ==========
    
    # Build markets dict from config
    markets = {exchange: {TRADING_PAIR} for exchange in EXCHANGES_TO_TEST}
    
    def __init__(self, connectors):
        super().__init__(connectors)
        
        # State tracking
        self._last_test_timestamp: float = 0
        self._current_exchange_index: int = 0
        self._pending_orders: Dict[str, Dict] = {}  # order_id -> {exchange, created_at, ...}
        self._orders_to_cancel: List[Dict] = []
        self._cancel_scheduled_at: float = 0
        
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        
        self.logger().info(f"Latency test initialized for exchanges: {self.EXCHANGES_TO_TEST}")
    
    @property
    def timestamp_ms(self) -> int:
        """Current timestamp in milliseconds."""
        return int(time.time() * 1000)
    
    def get_csv_filename(self, exchange: str) -> str:
        """Generate CSV filename for an exchange."""
        return os.path.join(self.OUTPUT_DIR, f"{exchange}_{self.CSV_FILE_ID}.csv")
    
    def on_tick(self):
        """Main tick handler - orchestrates test cycles."""
        current_time = self.current_timestamp
        
        # Check if we have pending cancellations
        if self._orders_to_cancel and current_time >= self._cancel_scheduled_at:
            self._execute_pending_cancels()
            return
        
        # Check if it's time for a new test cycle
        if current_time >= self._last_test_timestamp + self.TEST_INTERVAL_SECONDS:
            self._run_test_cycle()
            self._last_test_timestamp = current_time
    
    def _run_test_cycle(self):
        """Run a test cycle: place orders on all configured exchanges."""
        self.logger().info("=" * 50)
        self.logger().info("Starting latency test cycle")
        
        for exchange in self.EXCHANGES_TO_TEST:
            if exchange not in self.connectors:
                self.logger().warning(f"Exchange {exchange} not configured, skipping")
                continue
            
            try:
                self._place_test_order(exchange)
            except Exception as e:
                self.logger().error(f"Error placing order on {exchange}: {e}")
        
        # Schedule cancellation
        self._cancel_scheduled_at = self.current_timestamp + self.CANCEL_DELAY_SECONDS
    
    def _place_test_order(self, exchange: str):
        """Place a single test order on an exchange."""
        connector = self.connectors[exchange]
        
        # Calculate amount in base currency (BTC)
        amount = self.ORDER_QUOTE_AMOUNT / self.ORDER_PRICE
        
        # Quantize to exchange's requirements
        amount = connector.quantize_order_amount(self.TRADING_PAIR, amount)
        
        if amount <= 0:
            self.logger().warning(f"{exchange}: Order amount too small after quantization")
            return
        
        # Record pre-send timestamp
        pre_send_ts = self.timestamp_ms
        
        # Place limit buy order
        order_id = self.buy(
            connector_name=exchange,
            trading_pair=self.TRADING_PAIR,
            amount=amount,
            order_type=OrderType.LIMIT,
            price=self.ORDER_PRICE
        )
        
        # Track the order
        self._pending_orders[order_id] = {
            "exchange": exchange,
            "trading_pair": self.TRADING_PAIR,
            "pre_send_ts": pre_send_ts,
            "amount": amount,
            "price": self.ORDER_PRICE,
        }
        
        # Log to CSV
        self._save_to_csv(exchange, pre_send_ts, order_id, OrderState.PENDING_CREATE.value)
        
        self.logger().info(f"{exchange}: Placed test order {order_id} (amount={amount})")
    
    def _execute_pending_cancels(self):
        """Cancel all pending test orders."""
        for exchange in self.EXCHANGES_TO_TEST:
            if exchange not in self.connectors:
                continue
            
            active_orders = self.get_active_orders(exchange)
            for order in active_orders:
                if order.client_order_id in self._pending_orders:
                    pre_cancel_ts = self.timestamp_ms
                    
                    # Record pre-cancel timestamp
                    self._save_to_csv(
                        exchange, 
                        pre_cancel_ts, 
                        order.client_order_id, 
                        OrderState.PENDING_CANCEL.value
                    )
                    
                    # Cancel the order
                    self.cancel(exchange, order.trading_pair, order.client_order_id)
                    
                    self.logger().info(f"{exchange}: Canceling order {order.client_order_id}")
        
        self._orders_to_cancel = []
    
    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """Handle buy order creation confirmation."""
        created_ts = self.timestamp_ms
        
        if event.order_id in self._pending_orders:
            order_info = self._pending_orders[event.order_id]
            exchange = order_info["exchange"]
            
            # Log creation timestamp
            self._save_to_csv(exchange, created_ts, event.order_id, OrderState.CREATED.value)
            
            # Calculate and log latency
            latency_ms = created_ts - order_info["pre_send_ts"]
            self.logger().info(f"{exchange}: Order {event.order_id} CREATED in {latency_ms}ms")
    
    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """Handle sell order creation confirmation (not used but required for completeness)."""
        pass
    
    def did_cancel_order(self, event: OrderCancelledEvent):
        """Handle order cancellation confirmation."""
        canceled_ts = self.timestamp_ms
        
        if event.order_id in self._pending_orders:
            order_info = self._pending_orders[event.order_id]
            exchange = order_info["exchange"]
            
            # Log cancellation timestamp
            self._save_to_csv(exchange, canceled_ts, event.order_id, OrderState.CANCELED.value)
            
            self.logger().info(f"{exchange}: Order {event.order_id} CANCELED at {canceled_ts}")
            
            # Clean up
            del self._pending_orders[event.order_id]
    
    def did_fail_order(self, event: MarketOrderFailureEvent):
        """Handle order failure."""
        if event.order_id in self._pending_orders:
            order_info = self._pending_orders[event.order_id]
            exchange = order_info["exchange"]
            self.logger().error(f"{exchange}: Order {event.order_id} FAILED")
            del self._pending_orders[event.order_id]
    
    def _save_to_csv(self, exchange: str, timestamp: int, order_id: str, status: str):
        """Append a latency measurement to the exchange's CSV file."""
        filename = self.get_csv_filename(exchange)
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Order_ID', 'Exchange', 'Trading_Pair', 'Status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'Timestamp': timestamp,
                'Order_ID': order_id,
                'Exchange': exchange,
                'Trading_Pair': self.TRADING_PAIR,
                'Status': status,
            })
    
    def format_status(self) -> str:
        """Return status for display in Hummingbot UI."""
        lines = []
        lines.append("=" * 50)
        lines.append("Multi-Exchange Latency Test")
        lines.append("=" * 50)
        lines.append(f"Trading Pair: {self.TRADING_PAIR}")
        lines.append(f"Order Price: ${self.ORDER_PRICE}")
        lines.append(f"Order Size: ${self.ORDER_QUOTE_AMOUNT}")
        lines.append(f"Test Interval: {self.TEST_INTERVAL_SECONDS}s")
        lines.append("")
        lines.append("Configured Exchanges:")
        for exchange in self.EXCHANGES_TO_TEST:
            status = "✓ Connected" if exchange in self.connectors else "✗ Not configured"
            lines.append(f"  - {exchange}: {status}")
        lines.append("")
        lines.append(f"Pending Orders: {len(self._pending_orders)}")
        lines.append(f"Output Dir: {self.OUTPUT_DIR}")
        
        # Show recent results if available
        lines.append("")
        lines.append("Recent Latency Results (last test cycle):")
        # Would need to track this separately for display
        
        return "\n".join(lines)
