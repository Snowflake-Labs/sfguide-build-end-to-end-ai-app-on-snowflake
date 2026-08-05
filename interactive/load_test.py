import os
import time
import random
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.connector

CONNECTION_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME", "hol")
NUM_CONCURRENT = 200
NUM_QUERIES_PER_THREAD = 5

SAMPLE_ORDER_IDS = []

def get_sample_ids():
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute("SELECT order_id FROM DASH_AUTOMATED_INTELLIGENCE_DB.INTERACTIVE.ORDER_LOOKUP SAMPLE (1000 ROWS)")
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids

def run_queries(warehouse, order_ids):
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    
    latencies = []
    for _ in range(NUM_QUERIES_PER_THREAD):
        oid = random.choice(order_ids)
        start = time.perf_counter()
        cur.execute(f"SELECT * FROM DASH_AUTOMATED_INTELLIGENCE_DB.INTERACTIVE.ORDER_LOOKUP WHERE order_id = '{oid}'")
        cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        latencies.append(elapsed_ms)
    
    cur.close()
    conn.close()
    return latencies

def run_load_test(warehouse, order_ids):
    print(f"\n{'='*60}")
    print(f"  Load Test: {warehouse} ({NUM_CONCURRENT} concurrent sessions, {NUM_QUERIES_PER_THREAD} queries each)")
    print(f"{'='*60}")
    
    print("  Warming up (10 queries)...")
    warm_conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    warm_cur = warm_conn.cursor()
    warm_cur.execute(f"USE WAREHOUSE {warehouse}")
    warm_cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    for oid in random.sample(order_ids, min(10, len(order_ids))):
        warm_cur.execute(f"SELECT * FROM DASH_AUTOMATED_INTELLIGENCE_DB.INTERACTIVE.ORDER_LOOKUP WHERE order_id = '{oid}'")
        warm_cur.fetchall()
    warm_cur.close()
    warm_conn.close()
    print("  Warm-up complete. Starting concurrent test...")
    
    all_latencies = []
    start = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=NUM_CONCURRENT) as executor:
        futures = [executor.submit(run_queries, warehouse, order_ids) for _ in range(NUM_CONCURRENT)]
        for future in as_completed(futures):
            all_latencies.extend(future.result())
    
    total_time = time.perf_counter() - start
    
    all_latencies.sort()
    p50 = statistics.median(all_latencies)
    p90 = all_latencies[int(len(all_latencies) * 0.90)]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]
    
    print(f"  Total queries: {len(all_latencies)}")
    print(f"  Total time:    {total_time:.1f}s")
    print(f"  Throughput:    {len(all_latencies)/total_time:.0f} queries/sec")
    print(f"  P50 latency:   {p50:.0f} ms")
    print(f"  P90 latency:   {p90:.0f} ms")
    print(f"  P99 latency:   {p99:.0f} ms")
    print(f"  Min:           {min(all_latencies):.0f} ms")
    print(f"  Max:           {max(all_latencies):.0f} ms")
    
    return {"p50": p50, "p90": p90, "p99": p99, "throughput": len(all_latencies)/total_time}

if __name__ == "__main__":
    print("Fetching sample order IDs...")
    order_ids = get_sample_ids()
    print(f"Got {len(order_ids)} sample IDs")
    
    interactive_results = run_load_test("HOL_INTERACTIVE_WH", order_ids)
    standard_results = run_load_test("HOL_WH", order_ids)
    
    print(f"\n{'='*60}")
    print("  COMPARISON")
    print(f"{'='*60}")
    print(f"  {'Metric':<15} {'Interactive':<15} {'Standard':<15} {'Speedup':<10}")
    print(f"  {'-'*55}")
    for metric in ["p50", "p90", "p99"]:
        i = interactive_results[metric]
        s = standard_results[metric]
        speedup = s / i if i > 0 else 0
        print(f"  {metric.upper():<15} {i:<15.0f} {s:<15.0f} {speedup:.1f}x")
    print(f"  {'Throughput':<15} {interactive_results['throughput']:<15.0f} {standard_results['throughput']:<15.0f}")
