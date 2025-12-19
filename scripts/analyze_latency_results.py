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
        orders = defaultdict(dict)
        
        try:
            with open(filepath, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    order_id = row['Order_ID']
                    status = row['Status']
                    timestamp = int(row['Timestamp'])
                    
                    if status == "PENDING_CREATE":
                        orders[order_id]['start'] = timestamp
                    elif status == "CREATED":
                        orders[order_id]['end'] = timestamp
            
            # Calculate latencies
            for order_id, times in orders.items():
                if 'start' in times and 'end' in times:
                    latency = times['end'] - times['start']
                    # Filter out obviously bad data (e.g. negative latency)
                    if latency >= 0:
                        latencies.append(latency)
            
            if not latencies:
                print(f"Warning: No completed orders found for {exchange_name}")
                continue
                
            # Calculate Statistics
            stats = {
                "Exchange": exchange_name,
                "Count": len(latencies),
                "Min": min(latencies),
                "Max": max(latencies),
                "Avg": statistics.mean(latencies),
                "P50": statistics.median(latencies),
                "P90": statistics.quantiles(latencies, n=10)[8] if len(latencies) >= 10 else max(latencies), # Approx P90
                "P99": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies) # Approx P99
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
    headers = ["Exchange", "Count", "Min (ms)", "Avg (ms)", "P50 (ms)", "P90 (ms)", "Max (ms)"]
    row_format = "{:<15} {:<8} {:<10} {:<10} {:<10} {:<10} {:<10}"
    
    print("-" * 80)
    print(row_format.format(*headers))
    print("-" * 80)
    
    for s in exchange_stats:
        print(row_format.format(
            s["Exchange"],
            s["Count"],
            f"{s['Min']}",
            f"{s['Avg']:.2f}",
            f"{s['P50']:.1f}",
            f"{s['P90']:.1f}",
            f"{s['Max']}"
        ))
    print("-" * 80)

if __name__ == "__main__":
    analyze_latency_results()
