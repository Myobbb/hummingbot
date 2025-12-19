"""
Multi-Exchange Latency Test Script

Measures order creation and cancellation latencies across multiple cryptocurrency exchanges.
Places unfillable limit orders (buy BTC at $69,420) and measures round-trip times.

Inspired by: https://github.com/supervik/crypto-exchanges-latencies-test

Usage:
    1. Configure API keys for target exchanges in Hummingbot
    2. Comment out exchanges you don't have configured in EXCHANGES below
    3. Run: start --script latency_test.py
"""

import csv
import math
import os
import time
from decimal import Decimal
from enum import Enum

from hummingbot.core.data_type.common import OrderType
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
    
    # ========== CONFIGURATION ==========
    # Trading pair (must be available on all exchanges)
    trading_pair = "BTC-USDT"
    
    # Order parameters - unfillable buy price
    order_price = Decimal("69420")
    order_amount = Decimal("0.0004")  # ~$25 at ~$100k BTC
    
    # Timing
    create_interval = 60  # seconds between test cycles
    delay = 5  # seconds before canceling orders
    ws_wait_timeout = 1.0  # seconds to wait for late WS updates (for BingX etc)
    
    # Exchanges to test - comment out any you don't have configured
    EXCHANGES = [
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
    # ========== END CONFIGURATION ==========
    
    # Build markets dict from exchanges list
    # Note: MEXC uses SOL-USDT because BTC-USDT has isSpotTradingAllowed=False in their API
    markets = {
        "bybit": {"BTC-USDT"},
        "kucoin": {"BTC-USDT"},
        "gate_io": {"BTC-USDT"},
        "mexc": {"SOL-USDT"},
        "htx": {"BTC-USDT"},
        "bitmart": {"BTC-USDT"},
        "bing_x": {"BTC-USDT"},
        "okx": {"BTC-USDT"},
        "bitget": {"BTC-USDT"},
    }
    
    def __init__(self, connectors):
        super().__init__(connectors)
        
        # Runtime state
        self.create_timestamp = 0
        self.delay_timestamp = 0
        self.pending_orders = {}  # order_id -> {exchange, pre_send_ts}
        
        # Batch latency tracking
        self.current_batch_latencies = {}  # exchange -> latency_ms
        self.batch_count = 0
        
        self.csv_file_id = "latency_test"
        
        self.logger().info(f"LatencyTest initialized with {len(connectors)} exchanges")
    
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
        # Inject WS hooks if not active
        if not getattr(self, "hooks_injected", False):
            self.inject_hooks()
            self.hooks_injected = True

        # Prevent further actions if within the delay period
        if self.current_timestamp < self.delay_timestamp:
            return
        
        # Cancel any active orders after the delay has passed
        has_active = False
        # Check if we have active orders to cancel
        active_counts = {c: len(self.get_active_orders(c)) for c in self.connectors}
        
        if any(active_counts.values()):
            has_active = True
            
            # Wait for all WS updates before logging summary
            # Check if we have WS updates for all exchanges, or timeout has passed
            ws_received_count = len(getattr(self, 'current_batch_one_way', {}))
            all_ws_received = ws_received_count >= len(self.connectors)
            
            # Check if ws_wait_timeout has passed since batch started
            batch_start = getattr(self, 'batch_start_timestamp', 0)
            ws_timeout_passed = (self.current_timestamp - batch_start) >= (self.delay + self.ws_wait_timeout)
            
            # Log batch summary (with all collected WS updates) before cancelling
            if not getattr(self, "summary_logged", False):
                if all_ws_received or ws_timeout_passed:
                    self._log_batch_summary()
                    self.summary_logged = True
                else:
                    # Still waiting for WS updates - log which ones are pending
                    ws_received = set(getattr(self, 'current_batch_one_way', {}).keys())
                    ws_pending = set(self.connectors.keys()) - ws_received
                    if not getattr(self, '_ws_wait_logged', False):
                        self.logger().info(f"Waiting for WS updates from: {', '.join(sorted(ws_pending))}")
                        self._ws_wait_logged = True
                    return
            
            for connector_name in self.connectors:
                if active_counts[connector_name] > 0:
                     self.cancel_all_orders(connector_name)
        
        if has_active:
            self.delay_timestamp = self.current_timestamp + self.delay
            return
        
        # Place limit orders if conditions are met
        if self.current_timestamp > self.create_timestamp:
            self.delay_timestamp = self.current_timestamp + self.delay
            self.create_timestamp = self.current_timestamp + self.create_interval
            self.place_orders_all_exchanges()

    def inject_hooks(self):
        """Monkey-patches the ClientOrderTracker to intercept WS updates before they are filtered."""
        for connector_name, connector in self.connectors.items():
            tracker = getattr(connector, "_client_order_tracker", None)
            if not tracker:
                tracker = getattr(connector, "_order_tracker", None)
                
            if not tracker:
                self.logger().warning(f"Could not find order tracker for {connector_name} - skipping WS hooks")
                continue
            
            # Avoid double wrapping if possible (wrapper name usually 'wrapper' or 'make_wrapper')
            if getattr(tracker.process_order_update, "__name__", "") == "wrapper":
                continue 
                
            original_method = tracker.process_order_update
            
            def make_wrapper(orig, name):
                def wrapper(order_update):
                    # Intercept WS update
                    try:
                        self.process_ws_update(name, order_update)
                    except Exception as e:
                        self.logger().error(f"Error in WS hook: {e}")
                    return orig(order_update)
                return wrapper
            
            tracker.process_order_update = make_wrapper(original_method, connector_name)
            self.logger().info(f"Injected WS hook for {connector_name}")

    def process_ws_update(self, connector_name, order_update):
        client_order_id = order_update.client_order_id
        if not client_order_id:
            # Sometimes updates only have exchange_order_id, need to find mapping
            # But for simplicity, we focus on updates that carry client_id or are matched
            return
            
        if not hasattr(self, 'pending_orders') or client_order_id not in self.pending_orders:
            return
            
        # We found a WS update for a pending/active order!
        new_ts = order_update.update_timestamp
        
        if not hasattr(self, 'ws_updates_found'):
             self.ws_updates_found = set()
             
        if client_order_id in self.ws_updates_found:
            return

        order_info = self.pending_orders[client_order_id]
        pre_send_ts = order_info["pre_send_ts"]
        
        ws_ts_ms = new_ts * 1000
        # Calculate One-Way (MS - MS)
        one_way = ws_ts_ms - pre_send_ts
        
        if not hasattr(self, 'current_batch_one_way'):
            self.current_batch_one_way = {}
            
        self.current_batch_one_way[connector_name] = one_way
        
        # Update for CSV (Store as MS)
        order_info["exchange_ts"] = ws_ts_ms
        
        self.ws_updates_found.add(client_order_id)
        
        self.logger().info(f"WS UPDATE {connector_name}: Captured! P={one_way:.0f}ms (Exch={new_ts:.3f} - Send={pre_send_ts/1000:.3f})")
    
    def cancel_all_orders(self, connector_name):
        """Cancels all active orders on an exchange."""
        for order in self.get_active_orders(connector_name):
            # Save the deferred "CREATED" row now (with best timestamp)
            if order.client_order_id in self.pending_orders:
                info = self.pending_orders[order.client_order_id]
                if "created_ts" in info:
                    # Use pre_send_ts (Seconds) for Timestamp column to allow (Exchange - Send) calculation
                    ts_to_write = info["pre_send_ts"]
                    # Use exchange_ts (Seconds)
                    exchange_ts = info.get("exchange_ts", 0)
                    self.save_to_csv(connector_name, ts_to_write, order.client_order_id, OrderState.CREATED.name, exchange_ts)

            self.save_to_csv(connector_name, self.timestamp_now, order.client_order_id, OrderState.PENDING_CANCEL.name)
            self.cancel(connector_name, order.trading_pair, order.client_order_id)
    
    def place_orders_all_exchanges(self):
        """Places test orders on all configured exchanges."""
        self.batch_count += 1
        self.current_batch_latencies = {}
        self.current_batch_one_way = {}
        self.ws_updates_found = set()
        self.summary_logged = False
        self._ws_wait_logged = False
        
        self.logger().info(f"{'='*60}")
        self.logger().info(f"BATCH #{self.batch_count} - Starting latency test")
        
        # Track when batch started for WS timeout calculation
        self.batch_start_timestamp = self.current_timestamp
        
        # Log start of dispatch
        dispatch_start = time.perf_counter()
        
        for connector_name in self.connectors:
            try:
                self.place_order(connector_name)
            except Exception as e:
                self.logger().error(f"Error placing order on {connector_name}: {e}")
        
        # Log end of dispatch to prove parallel scheduling
        dispatch_duration = (time.perf_counter() - dispatch_start) * 1000
        self.logger().info(f"Batch dispatched to {len(self.connectors)} exchanges in {dispatch_duration:.2f}ms")
        self.logger().info(f"(Orders are executed asynchronously/parallel - dispatch time is purely local scheduling overhead)")
    
    def place_order(self, connector_name):
        """Places a single test order on an exchange."""
        connector = self.connectors[connector_name]
        
        # Get the trading pair for this exchange
        trading_pair = list(self.markets.get(connector_name, {self.trading_pair}))[0]
        
        # Get appropriate price for the pair
        if trading_pair == "SOL-USDT":
            order_price = Decimal("100")  # Unfillable low price for SOL
            order_amount = Decimal("0.1")  # ~$20 worth
        else:
            order_price = self.order_price
            order_amount = self.order_amount
        
        # Quantize amount to exchange's requirements
        amount = connector.quantize_order_amount(trading_pair, order_amount)
        price = connector.quantize_order_price(trading_pair, order_price)
        
        if amount <= 0:
            self.logger().warning(f"{connector_name}: Order amount too small after quantization")
            return
        
        # Record pre-send timestamp
        # Metric: End-to-End Latency (Bot Decision -> Exchange ACK -> Bot Confirmation)
        time_before_order_sent = self.timestamp_now
        
        # Place limit buy order
        # Note: self.buy() is non-blocking (returns immediately after scheduling task)
        order_id = self.buy(connector_name, trading_pair, amount, OrderType.LIMIT, price)
        
        # Track the order
        self.pending_orders[order_id] = {
            "exchange": connector_name,
            "pre_send_ts": time_before_order_sent,
        }
        
        # Log to CSV
        self.save_to_csv(connector_name, time_before_order_sent, order_id, OrderState.PENDING_CREATE.name)
    
    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """Logs the post-transmission timestamp when a buy order is created."""
        # Capture time immediately upon receiving event (Exchange ACK)
        created_ts = self.timestamp_now
        
        if event.order_id in self.pending_orders:
            order_info = self.pending_orders[event.order_id]
            exchange = order_info["exchange"]
            
            # Latency = (Time Event Received) - (Time Order Sent)
            # This captures:
            # 1. Internal Scheduling
            # 2. Network Request (RTT)
            # 3. Exchange Processing
            # 4. Network Response (RTT)
            latency_ms = created_ts - order_info["pre_send_ts"]
            
            # One-Way Latency (Approximate due to Clock Skew)
            # = Exchange TS - Local Send TS
            exchange_ts = event.creation_timestamp * 1e3 # Convert to ms if needed, check format
            # Note: hummingbot event timestamps are usually floats in seconds. 
            # Scripts usually use ms for logging.
            # event.creation_timestamp comes from connector which did * 1e-3. So it is strictly seconds.
            # self.timestamp_now is milliseconds.
            # So:
            exchange_ts_ms = event.creation_timestamp * 1000
            one_way_ms = exchange_ts_ms - order_info["pre_send_ts"]
            
            # VALIDATION: Detect second-precision timestamps (lack of millisecond precision)
            # If fractional milliseconds are < 1, the exchange timestamp was likely in seconds-only
            fractional_ms = exchange_ts_ms % 1000
            has_ms_precision = fractional_ms >= 1  # True if there's actual ms precision
            
            if not has_ms_precision:
                # Log warning once per exchange about low-precision timestamps
                if not hasattr(self, '_low_precision_warned'):
                    self._low_precision_warned = set()
                if exchange not in self._low_precision_warned:
                    self.logger().warning(
                        f"{exchange}: Low-precision timestamp detected (seconds only). "
                        f"One-way latency may be unreliable."
                    )
                    self._low_precision_warned.add(exchange)
                # Mark as approximate with special value (use NaN for unreliable)
                one_way_ms = float('nan')  # Indicates unreliable measurement
            
            # Track for batch summary
            self.current_batch_latencies[exchange] = latency_ms
            if exchange not in self.current_batch_one_way:
                self.current_batch_one_way[exchange] = one_way_ms
            
            # Store timestamps for delayed CSV writing (allows capturing better WS timestamps)
            if event.order_id in self.pending_orders:
                self.pending_orders[event.order_id]["created_ts"] = created_ts
                # Only set exchange_ts if not already set by WS (rare race condition)
                if "exchange_ts" not in self.pending_orders[event.order_id]:
                    # Store as MS
                    self.pending_orders[event.order_id]["exchange_ts"] = event.creation_timestamp * 1000
    
    def _log_batch_summary(self):
        """Log a one-line summary of all latencies sorted by speed."""
        if not self.current_batch_latencies:
            return
        
        # Sort by latency (fastest first)
        sorted_latencies = sorted(self.current_batch_latencies.items(), key=lambda x: x[1])
        
        # Format as: "exchange:RTT(OneWay)"
        summary_parts = []
        for ex, lat in sorted_latencies:
            one_way = self.current_batch_one_way.get(ex, 0)
            # Handle NaN (unreliable) one-way measurements
            if isinstance(one_way, float) and math.isnan(one_way):
                summary_parts.append(f"{ex}:{lat}ms(N/A)")
            else:
                summary_parts.append(f"{ex}:{lat}ms({one_way:.0f})")
            
        summary = " | ".join(summary_parts)
        
        # Calculate stats
        latencies = list(self.current_batch_latencies.values())
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        self.logger().info(f"{'='*60}")
        self.logger().info(f"BATCH #{self.batch_count} RESULTS (RTT | One-Way):")
        self.logger().info(f"  {summary}")
        self.logger().info(f"  Min: {min_latency}ms | Avg: {avg_latency:.0f}ms | Max: {max_latency}ms")
        self.logger().info(f"{'='*60}")
    
    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        pass
    
    def did_cancel_order(self, event: OrderCancelledEvent):
        """Logs the post-transmission timestamp when an order is cancelled."""
        canceled_ts = self.timestamp_now
        
        if event.order_id in self.pending_orders:
            order_info = self.pending_orders[event.order_id]
            exchange = order_info["exchange"]
            
            self.save_to_csv(exchange, canceled_ts, event.order_id, OrderState.CANCELED.name)
            del self.pending_orders[event.order_id]
    
    def save_to_csv(self, exchange, timestamp, order_id, status, exchange_timestamp=0):
        """Appends the provided data to the CSV file."""
        filename = self.get_filename(exchange)
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Order_ID', 'Status', 'Exchange_Timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'Timestamp': timestamp, 
                'Order_ID': order_id, 
                'Status': status,
                'Exchange_Timestamp': exchange_timestamp
            })
