"""
Multi-Exchange Latency Test Script

Measures order creation and cancellation latencies across multiple cryptocurrency exchanges.
Places unfillable limit orders (buy BTC at $69,420) and measures round-trip times.

Inspired by: https://github.com/supervik/crypto-exchanges-latencies-test

Usage:
    1. Configure API keys for target exchanges in Hummingbot
    2. Comment out exchanges you don't have configured in the markets dict
    3. Run: start --script latency_test.py
"""

import csv
import logging
import os
import time
from decimal import Decimal
from enum import Enum

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    OrderCancelledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class OrderState(Enum):
    PENDING_CREATE = 0
    CREATED = 1
    PENDING_CANCEL = 2
    CANCELED = 3


class LatencyTest(ScriptStrategyBase):
    """
    Multi-exchange latency test script.
    
    Places unfillable limit buy orders at a very low price across multiple exchanges,
    then cancels them to measure creation and cancellation latencies.
    
    Results are saved to CSV files for analysis.
    """
    
    # Configuration - class-level variables
    trading_pair: str = "BTC-USDT"
    order_price: Decimal = Decimal("69420")  # Unfillable price (way below market)
    order_amount: Decimal = Decimal("0.0004")  # ~$25 worth at ~$100k BTC
    create_interval: int = 60  # Time interval (in seconds) between test cycles
    delay: int = 5  # Time delay (in seconds) before canceling orders
    csv_file_id: str = "latency_test"  # Identifier for CSV filenames
    
    # Exchanges to test - comment out any you don't have configured
    markets = {
        "bybit": {trading_pair},
        "kucoin": {trading_pair},
        "gate_io": {trading_pair},
        "mexc": {trading_pair},
        "htx": {trading_pair},
        "bitmart": {trading_pair},
        "bing_x": {trading_pair},
        "okx": {trading_pair},
        "bitget": {trading_pair},
    }
    
    # Runtime state
    create_timestamp: float = 0
    delay_timestamp: float = 0
    pending_orders: dict = {}  # order_id -> {exchange, pre_send_ts}
    
    # Batch latency tracking
    current_batch_latencies: dict = {}  # exchange -> latency_ms
    batch_count: int = 0
    
    @property
    def timestamp_now(self):
        """Returns the current timestamp in milliseconds."""
        return int(time.time() * 1e3)
    
    def get_filename(self, exchange):
        """Generates the filename for the CSV based on the exchange name."""
        os.makedirs("data/latency", exist_ok=True)
        return f"data/latency/{exchange}_{self.csv_file_id}.csv"
    
    def on_tick(self):
        """Called regularly to check for order creation and cancellation conditions."""
        # Prevent further actions if within the delay period
        if self.current_timestamp < self.delay_timestamp:
            return
        
        # Cancel any active orders after the delay has passed
        has_active = False
        for connector_name in self.connectors:
            active_orders = self.get_active_orders(connector_name)
            if active_orders:
                has_active = True
                self.cancel_all_orders(connector_name)
        
        if has_active:
            self.delay_timestamp = self.current_timestamp + self.delay
            return
        
        # Place limit orders if conditions are met
        if self.current_timestamp > self.create_timestamp:
            self.delay_timestamp = self.current_timestamp + self.delay
            self.create_timestamp = self.current_timestamp + self.create_interval
            self.place_orders_all_exchanges()
    
    def cancel_all_orders(self, connector_name):
        """Cancels all active orders on an exchange and logs the pre-transmission timestamp."""
        for order in self.get_active_orders(connector_name):
            self.save_to_csv(connector_name, self.timestamp_now, order.client_order_id, OrderState.PENDING_CANCEL.name)
            self.cancel(connector_name, order.trading_pair, order.client_order_id)
    
    def place_orders_all_exchanges(self):
        """Places test orders on all configured exchanges."""
        self.batch_count += 1
        self.current_batch_latencies = {}  # Reset for new batch
        
        self.logger().info(f"{'='*60}")
        self.logger().info(f"BATCH #{self.batch_count} - Starting latency test")
        
        for connector_name in self.connectors:
            try:
                self.place_order(connector_name)
            except Exception as e:
                self.logger().error(f"Error placing order on {connector_name}: {e}")
    
    def place_order(self, connector_name):
        """Places a single test order on an exchange."""
        connector = self.connectors[connector_name]
        
        # Quantize amount to exchange's requirements
        amount = connector.quantize_order_amount(self.trading_pair, self.order_amount)
        price = connector.quantize_order_price(self.trading_pair, self.order_price)
        
        if amount <= 0:
            self.logger().warning(f"{connector_name}: Order amount too small after quantization")
            return
        
        # Record pre-send timestamp
        time_before_order_sent = self.timestamp_now
        
        # Place limit buy order
        order_id = self.buy(connector_name, self.trading_pair, amount, OrderType.LIMIT, price)
        
        # Track the order
        self.pending_orders[order_id] = {
            "exchange": connector_name,
            "pre_send_ts": time_before_order_sent,
        }
        
        # Log to CSV
        self.save_to_csv(connector_name, time_before_order_sent, order_id, OrderState.PENDING_CREATE.name)
    
    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """Logs the post-transmission timestamp when a buy order is created."""
        created_ts = self.timestamp_now
        
        if event.order_id in self.pending_orders:
            order_info = self.pending_orders[event.order_id]
            exchange = order_info["exchange"]
            latency_ms = created_ts - order_info["pre_send_ts"]
            
            # Track for batch summary
            self.current_batch_latencies[exchange] = latency_ms
            
            self.save_to_csv(exchange, created_ts, event.order_id, OrderState.CREATED.name)
            
            # Check if all orders for this batch have been confirmed
            expected_count = len(self.connectors)
            if len(self.current_batch_latencies) == expected_count:
                self._log_batch_summary()
    
    def _log_batch_summary(self):
        """Log a one-line summary of all latencies sorted by speed."""
        if not self.current_batch_latencies:
            return
        
        # Sort by latency (fastest first)
        sorted_latencies = sorted(self.current_batch_latencies.items(), key=lambda x: x[1])
        
        # Format as: "exchange:XXms"
        summary_parts = [f"{ex}:{lat}ms" for ex, lat in sorted_latencies]
        summary = " | ".join(summary_parts)
        
        # Calculate stats
        latencies = list(self.current_batch_latencies.values())
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        self.logger().info(f"{'='*60}")
        self.logger().info(f"BATCH #{self.batch_count} RESULTS (sorted by speed):")
        self.logger().info(f"  {summary}")
        self.logger().info(f"  Min: {min_latency}ms | Avg: {avg_latency:.0f}ms | Max: {max_latency}ms")
        self.logger().info(f"{'='*60}")
    
    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """Logs the post-transmission timestamp when a sell order is created."""
        pass
    
    def did_cancel_order(self, event: OrderCancelledEvent):
        """Logs the post-transmission timestamp when an order is cancelled."""
        canceled_ts = self.timestamp_now
        
        if event.order_id in self.pending_orders:
            order_info = self.pending_orders[event.order_id]
            exchange = order_info["exchange"]
            
            self.save_to_csv(exchange, canceled_ts, event.order_id, OrderState.CANCELED.name)
            
            # Clean up
            del self.pending_orders[event.order_id]
    
    def save_to_csv(self, exchange, timestamp, order_id, status):
        """Appends the provided data to the CSV file. If the file doesn't exist, it creates one."""
        filename = self.get_filename(exchange)
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Order_ID', 'Status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({'Timestamp': timestamp, 'Order_ID': order_id, 'Status': status})
