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
                    timestamp = float(row['Timestamp']) # Changed to float to handle potential decimals
                    
                    # Handle Exchange Timestamp if present
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
            
            # Calculate latencies
            for order_id, times in orders.items():
                if 'start' in times and 'end' in times:
                    latency = times['end'] - times['start']
                    
                    one_way = None
                    if 'exchange_ts' in times:
                        one_way = times['exchange_ts'] - times['start']
                    
                    # Filter out obviously bad data (e.g. negative latency)
                    if latency >= 0:
                        latencies.append(latency)
                        if one_way is not None:
                            # Keep one-way even if negative (due to clock skew), 
                            # but filtering huge negative outliers might be good.
                            # For now, let's keep it raw to see the skew.
                            one_way_latencies.append(one_way)
            
            if not latencies:
                print(f"Warning: No completed orders found for {exchange_name}")
                continue
                
            # Calculate Statistics
            # P50 (Median): The "typical" latency. 50% of orders were faster than this.
            # P90: The "slow" tail. 90% of orders were faster than this. Good for identifying lag spikes.
            # P99: The "worst case". 99% of orders were faster than this. Only 1 in 100 was slower.
            stats = {
                "Exchange": exchange_name,
                "Count": len(latencies),
                "Min": min(latencies),
                "Max": max(latencies),
                "Avg": statistics.mean(latencies),
                "P50": statistics.median(latencies),
                "P90": statistics.quantiles(latencies, n=10)[8] if len(latencies) >= 10 else max(latencies), # Approx P90
                "Avg One-Way": statistics.mean(one_way_latencies) if one_way_latencies else "N/A"
            }
            exchange_stats.append(stats)
            
        except Exception as e:
            print(f"Error analyzing {exchange_name}: {e}")

    if not exchange_stats:
        print("No valid statistics could be calculated.")
        return

    # Sort results by Average Latency
    exchange_stats.sort(key=lambda x: x["Avg"])
    
    # Print Table
    headers = ["Exchange", "Count", "Min (ms)", "Avg (ms)", "P50 (ms)", "P90 (ms)", "Max (ms)", "One-Way(Avg)"]
    row_format = "{:<15} {:<8} {:<10} {:<10} {:<10} {:<10} {:<10} {:<12}"
    
    print("-" * 95)
    print(row_format.format(*headers))
    print("-" * 95)
    
    for s in exchange_stats:
        one_way_str = f"{s['Avg One-Way']:.2f}" if isinstance(s['Avg One-Way'], (int, float)) else "N/A"
        print(row_format.format(
            s["Exchange"],
            s["Count"],
            f"{s['Min']:.1f}",
            f"{s['Avg']:.1f}",
            f"{s['P50']:.1f}",
            f"{s['P90']:.1f}",
            f"{s['Max']:.1f}",
            one_way_str
        ))
    print("-" * 95)

if __name__ == "__main__":
    analyze_latency_results()
