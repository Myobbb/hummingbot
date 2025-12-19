#!/usr/bin/env python3
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict

def analyze_latency_results():
    # Define paths - Hardcoded as requested to match latency_test.py
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir, "data", "latency")
    file_pattern = os.path.join(data_dir, "*_latency_test.csv")
    
    print(f"Searching for data files in: {data_dir}")
    
    files = glob.glob(file_pattern)
    if not files:
        print(f"No latency data files found matching: {file_pattern}")
        print("Make sure you run this script from the hummingbot root directory.")
        return

    print(f"Found {len(files)} files. Analyzing...\n")
    
    # Store results for final table
    exchange_stats = []
    
    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        # filename format: {exchange}_latency_test.csv
        exchange_name = filename.replace("_latency_test.csv", "")
        
        latencies = []
        one_way_latencies = []
        orders = defaultdict(dict)
        
        try:
            with open(filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    order_id = row['Order_ID']
                    status = row['Status']
                    timestamp = float(row['Timestamp'])
                    
                    # Handle new direct latency columns (preferred)
                    rtt_direct = None
                    one_way_direct = None
                    if 'RTT_Latency' in row and row['RTT_Latency']:
                        try:
                            rtt_direct = float(row['RTT_Latency'])
                        except ValueError:
                            pass
                    if 'One_Way_Latency' in row and row['One_Way_Latency']:
                        try:
                            one_way_direct = float(row['One_Way_Latency'])
                        except ValueError:
                            pass
                    
                    # Handle Exchange Timestamp for backward compatibility
                    exchange_ts = 0.0
                    if 'Exchange_Timestamp' in row and row['Exchange_Timestamp']:
                        try:
                            exchange_ts = float(row['Exchange_Timestamp'])
                        except ValueError:
                            exchange_ts = 0.0
                    
                    if status == "PENDING_CREATE":
                        orders[order_id]['start'] = timestamp
                    elif status == "CREATED":
                        orders[order_id]['end'] = timestamp
                        if exchange_ts > 0:
                            orders[order_id]['exchange_ts'] = exchange_ts
                        # Store direct values from CSV (preferred)
                        if rtt_direct is not None and rtt_direct > 0:
                            orders[order_id]['rtt_direct'] = rtt_direct
                        if one_way_direct is not None:
                            orders[order_id]['one_way_direct'] = one_way_direct
            
            # Calculate latencies
            for order_id, times in orders.items():
                # Prefer direct RTT from CSV, otherwise calculate
                if 'rtt_direct' in times:
                    latency = times['rtt_direct']
                elif 'start' in times and 'end' in times:
                    latency = times['end'] - times['start']
                else:
                    continue
                
                # Prefer direct one-way from CSV, otherwise calculate
                if 'one_way_direct' in times:
                    one_way = times['one_way_direct']
                elif 'exchange_ts' in times and 'start' in times:
                    one_way = times['exchange_ts'] - times['start']
                else:
                    one_way = None
                
                # Filter out obviously bad data (e.g. negative latency)
                if latency >= 0:
                    latencies.append(latency)
                    if one_way is not None:
                        one_way_latencies.append(one_way)
            
            if not latencies:
                print(f"Warning: No completed orders found for {exchange_name}")
                continue
                
            # Calculate Statistics
            # P50 (Median): The "typical" latency. 50% of orders were faster than this.
            # P90: The "slow" tail. 90% of orders were faster than this. Good for identifying lag spikes.
            # P99: The "worst case". 99% of orders were faster than this. Only 1 in 100 was slower.
            
            # Filter out NaN and extreme negative one-way values (likely clock skew artifacts)
            valid_one_way = [x for x in one_way_latencies if x is not None and x > -1000 and x < 10000]
            
            stats = {
                "Exchange": exchange_name,
                "Count": len(latencies),
                "Min": min(latencies),
                "Max": max(latencies),
                "Avg": statistics.mean(latencies),
                "P50": statistics.median(latencies),
                "P90": statistics.quantiles(latencies, n=10)[8] if len(latencies) >= 10 else max(latencies), # Approx P90
                "Avg One-Way": statistics.mean(valid_one_way) if valid_one_way else "N/A",
                "P50 One-Way": statistics.median(valid_one_way) if valid_one_way else "N/A",
                "One-Way Count": len(valid_one_way),
            }
            exchange_stats.append(stats)
            
        except Exception as e:
            print(f"Error analyzing {exchange_name}: {e}")

    if not exchange_stats:
        print("No valid statistics could be calculated.")
        return

    # Sort results by One-Way Latency (fastest first), falling back to RTT if no one-way data
    exchange_stats.sort(key=lambda x: x["Avg One-Way"] if isinstance(x["Avg One-Way"], (int, float)) else 9999)
    
    # Print unified table - user-friendly format
    print("\n" + "=" * 90)
    print("EXCHANGE LATENCY ANALYSIS")
    print("One-Way = time from bot decision to order on exchange book")
    print("RTT = round-trip time (full order creation cycle)")
    print("=" * 90)
    
    # Header
    print(f"\n{'Exchange':<12} {'Avg One-Way':<14} {'P50 One-Way':<14} {'Samples':<10} {'Notes':<25}")
    print("-" * 85)
    
    for s in exchange_stats:
        # Format one-way with RTT in parentheses
        if isinstance(s['Avg One-Way'], (int, float)):
            avg_str = f"{s['Avg One-Way']:.0f}ms ({s['Avg']:.0f}ms RTT)"
        else:
            avg_str = f"N/A ({s['Avg']:.0f}ms RTT)"
        
        if isinstance(s['P50 One-Way'], (int, float)):
            p50_str = f"{s['P50 One-Way']:.0f}ms ({s['P50']:.0f}ms RTT)"
        else:
            p50_str = f"N/A ({s['P50']:.0f}ms RTT)"
        
        # Sample count with filtering note
        samples_str = f"{s['One-Way Count']}/{s['Count']}"
        
        # Notes for issues
        notes = ""
        if s["One-Way Count"] == 0:
            notes = "No valid timestamps"
        elif s["One-Way Count"] < s["Count"]:
            filtered = s['Count'] - s['One-Way Count']
            notes = f"{filtered} filtered (clock skew)"
        
        print(f"{s['Exchange']:<12} {avg_str:<14} {p50_str:<14} {samples_str:<10} {notes:<25}")
    
    print("-" * 85)
    print(f"\nTotal orders analyzed: {sum(s['Count'] for s in exchange_stats)}")
    print(f"Exchanges: {len(exchange_stats)}")

if __name__ == "__main__":
    analyze_latency_results()
