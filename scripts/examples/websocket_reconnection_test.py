#!/usr/bin/env python3
"""
Websocket Reconnection Test Script

This script helps test and monitor websocket reconnection behavior
in the multi-strategy orchestrator by simulating network issues.

Usage:
1. Start the orchestrator with multiple strategies
2. Run this script to monitor connector status
3. Manually disconnect network or restart exchange services
4. Observe reconnection logs and per-strategy behavior

Expected Logs During Reconnection:
- "Strategy 'X' connectors disconnected: exchange_name"
- "Strategy 'X' connectors reconnected - resuming trading"
- Individual strategies should pause/resume independently
"""

import asyncio
import time
from typing import Dict, Any

def monitor_orchestrator_health():
    """Monitor orchestrator and connector health"""
    try:
        from scripts.multi_strategy_orchestrator import _get_orchestrator
        orchestrator = _get_orchestrator()
        
        print("=" * 70)
        print("WEBSOCKET RECONNECTION MONITORING")
        print("=" * 70)
        
        # Show connector status
        print("\nConnector Status:")
        for name, connector in orchestrator.connectors.items():
            ready = getattr(connector, 'ready', False)
            network_status = getattr(connector, 'network_status', 'UNKNOWN')
            status_dict = getattr(connector, 'status_dict', {})
            
            print(f"  {name}: ready={ready}, network={network_status}")
            if status_dict:
                status_items = [f"{k}={v}" for k, v in status_dict.items()]
                print(f"    Status: {', '.join(status_items)}")
        
        # Show strategy readiness
        print(f"\nStrategy Readiness:")
        for strategy_instance in orchestrator.strategies:
            strategy_ready = orchestrator._is_strategy_ready(strategy_instance)
            strategy_connectors = orchestrator._get_strategy_connectors(strategy_instance)
            connector_names = [c.name for c in strategy_connectors]
            
            status = "READY" if strategy_ready else "NOT READY"
            if strategy_instance.paused:
                status = "PAUSED (manual)"
            
            print(f"  {strategy_instance.name}: {status}")
            print(f"    Connectors: {', '.join(connector_names)}")
            
            # Show which specific connectors are down
            if not strategy_ready and not strategy_instance.paused:
                down_connectors = [
                    c.name for c in strategy_connectors
                    if not (c.ready and hasattr(c, 'network_status') and 
                           c.network_status.name == 'CONNECTED')
                ]
                if down_connectors:
                    print(f"    Down: {', '.join(down_connectors)}")
        
        print("\n" + "=" * 70)
        print("Monitor this output during network issues to verify reconnection behavior")
        print("Expected: Individual strategies pause/resume based on their specific connectors")
        print("=" * 70)
        
    except Exception as e:
        print(f"Error monitoring orchestrator: {e}")
        print("Make sure the orchestrator is running first")

def test_connector_transitions():
    """Test connector ready state transitions"""
    try:
        from scripts.multi_strategy_orchestrator import _get_orchestrator
        orchestrator = _get_orchestrator()
        
        print("Testing connector ready state transitions...")
        print("This will monitor for 60 seconds and log any state changes")
        
        # Track previous states
        prev_states = {}
        for name, connector in orchestrator.connectors.items():
            prev_states[name] = {
                'ready': getattr(connector, 'ready', False),
                'network_status': getattr(connector, 'network_status', None)
            }
        
        start_time = time.time()
        while time.time() - start_time < 60:  # Monitor for 1 minute
            time.sleep(1)
            
            # Check for state changes
            for name, connector in orchestrator.connectors.items():
                current_ready = getattr(connector, 'ready', False)
                current_network = getattr(connector, 'network_status', None)
                
                prev_ready = prev_states[name]['ready']
                prev_network = prev_states[name]['network_status']
                
                if current_ready != prev_ready or current_network != prev_network:
                    print(f"[{time.strftime('%H:%M:%S')}] {name}: "
                          f"ready {prev_ready}->{current_ready}, "
                          f"network {prev_network}->{current_network}")
                    
                    prev_states[name]['ready'] = current_ready
                    prev_states[name]['network_status'] = current_network
        
        print("Monitoring complete.")
        
    except Exception as e:
        print(f"Error testing transitions: {e}")

if __name__ == "__main__":
    print("Websocket Reconnection Test Script")
    print("1. Monitor current status")
    print("2. Test connector transitions (60s)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        monitor_orchestrator_health()
    elif choice == "2":
        test_connector_transitions()
    else:
        print("Invalid choice")
