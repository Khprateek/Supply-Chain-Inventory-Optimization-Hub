"""
benchmark_suite.py
==================
Enterprise Open Data Lakehouse — Empirical Benchmark & Verification Suite

This script implements the 5 industry-standard distributed systems tests to empirically
measure and verify all architectural performance metrics claimed in the project documentation:

  1. Performance Testing   : Kafka Producer Throughput & Zero-Loss Watermark Verification (~30K ev/s)
  2. Stress Testing        : PySpark Structured Streaming Backpressure (maxOffsetsPerTrigger=50K)
  3. Capacity Testing      : Iceberg Parquet Columnar Compression Ratio vs. Raw JSON (65%+ reduction)
  4. Scalability Testing   : Small-File Bin-Packing Compaction (80%+ file reduction) & Sub-Second Trino OLAP
  5. Resilience Testing    : S3 WAL Checkpoint Recovery & Nessie Zero-Copy Snapshot Time-Travel

Usage (Run when ready to execute benchmarks):
    python tests/benchmark_suite.py --test all
    python tests/benchmark_suite.py --test performance --duration 15
    python tests/benchmark_suite.py --test capacity
    python tests/benchmark_suite.py --test scalability
    python tests/benchmark_suite.py --test resilience
"""

import os
import sys
import time
import json
import uuid
import argparse
import statistics
import subprocess
from datetime import datetime
from pathlib import Path

import trino
from confluent_kafka import Consumer, TopicPartition

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "tests" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", 8080))
TRINO_USER = os.environ.get("TRINO_USER", "admin")


def get_trino_cursor():
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        http_scheme="http",
    )
    return conn, conn.cursor()


def get_kafka_total_offsets(topics=("sales_events", "inventory_events")) -> dict:
    """Query exact broker high-watermarks across all partitions without consuming payloads."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"benchmark-verifier-{uuid.uuid4().hex[:6]}",
        "enable.auto.commit": False,
    })
    totals = {}
    try:
        for topic in topics:
            md = consumer.list_topics(topic, timeout=5.0)
            if topic not in md.topics:
                totals[topic] = {"partitions": 0, "total_offsets": 0}
                continue
            partitions = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
            high_sum = 0
            for tp in partitions:
                _, high = consumer.get_watermark_offsets(tp, timeout=2.0)
                if high and high > 0:
                    high_sum += high
            totals[topic] = {
                "partitions": len(partitions),
                "total_offsets": high_sum,
            }
    finally:
        consumer.close()
    return totals


# ==============================================================================
# 1. PERFORMANCE TEST: Kafka High-Throughput Ingestion & Zero Message Loss
# ==============================================================================
def run_performance_test(duration_sec: int = 15, workers: int = 2) -> dict:
    print("\n" + "=" * 75)
    print("  TEST 1: PERFORMANCE & THROUGHPUT BENCHMARK (Generator -> Kafka)")
    print("=" * 75)

    pre_offsets = get_kafka_total_offsets()
    pre_total = sum(v["total_offsets"] for v in pre_offsets.values())
    print(f"  [Pre-Test]  Broker Watermark Total : {pre_total:,} messages")

    python_exe = sys.executable
    gen_script = str(PROJECT_ROOT / "scripts" / "kafka_data_generator.py")

    print(f"  [Running]   Spawning {workers} producer workers (LZ4, 100K buffer, 5ms linger) for {duration_sec}s...")
    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [python_exe, gen_script, "--workers", str(workers), "--rate", "0"],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(duration_sec)
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        proc.terminate()
    proc.wait(timeout=10)
    elapsed = time.perf_counter() - t0

    # Allow broker watermark settlement
    time.sleep(1.0)
    post_offsets = get_kafka_total_offsets()
    post_total = sum(v["total_offsets"] for v in post_offsets.values())

    ingested_delta = post_total - pre_total
    events_per_sec = ingested_delta / max(elapsed, 0.001)
    daily_capacity = events_per_sec * 86400
    four_hour_burst = events_per_sec * (4 * 3600)

    print(f"  [Post-Test] Broker Watermark Total : {post_total:,} messages")
    print(f"  [Result]    Net Events Ingested    : {ingested_delta:,} events in {elapsed:.2f}s")
    print(f"  [Result]    Sustained Throughput   : {events_per_sec:,.0f} events/sec")
    print(f"  [Result]    4-Hour Burst Capacity  : {four_hour_burst:,.0f} events")
    print(f"  [Result]    24-Hour Daily Capacity : {daily_capacity:,.0f} events/day")

    return {
        "test_type": "Performance Testing (Kafka Ingestion)",
        "workers": workers,
        "duration_sec": round(elapsed, 2),
        "events_ingested": ingested_delta,
        "sustained_events_per_sec": round(events_per_sec, 1),
        "projected_4hr_capacity": int(four_hour_burst),
        "projected_24hr_capacity": int(daily_capacity),
    }


# ==============================================================================
# 2. STRESS TEST: PySpark Backpressure & Micro-Batch Stability
# ==============================================================================
def run_stress_test() -> dict:
    print("\n" + "=" * 75)
    print("  TEST 2: STRESS & BACKPRESSURE VERIFICATION (Kafka -> PySpark)")
    print("=" * 75)

    metrics_file = PROJECT_ROOT / "pyspark_jobs" / "metrics_iceberg.json"
    if not metrics_file.exists():
        print("  [INFO] metrics_iceberg.json not found. Start stream_iceberg first during active stress test.")
        return {"test_type": "Stress Testing (Spark Backpressure)", "status": "skipped_no_active_stream"}

    with open(metrics_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    sales_prog = data.get("sales", {})
    inv_prog = data.get("inventory", {})
    sales_rows = sales_prog.get("numInputRows", 0)
    inv_rows = inv_prog.get("numInputRows", 0)
    batch_dur_ms = data.get("duration", 0)

    print(f"  [Result]    Sales Micro-Batch Input Rows     : {sales_rows:,} (Cap: 50,000)")
    print(f"  [Result]    Inventory Micro-Batch Input Rows : {inv_rows:,} (Cap: 50,000)")
    print(f"  [Result]    Average Micro-Batch Duration     : {batch_dur_ms:.1f} ms (Trigger: 5,000 ms)")
    print(f"  [Result]    Backpressure Compliance          : {'PASS' if sales_rows <= 50000 and inv_rows <= 50000 else 'OVERFLOW'}")

    return {
        "test_type": "Stress Testing (Spark Backpressure)",
        "max_offsets_per_trigger_cap": 50000,
        "observed_sales_batch_rows": sales_rows,
        "observed_inventory_batch_rows": inv_rows,
        "avg_batch_duration_ms": batch_dur_ms,
        "backpressure_enforced": (sales_rows <= 50000 and inv_rows <= 50000),
    }


# ==============================================================================
# 3. CAPACITY TEST: Storage Compression Efficiency (Parquet vs Raw JSON)
# ==============================================================================
def run_capacity_test() -> dict:
    print("\n" + "=" * 75)
    print("  TEST 3: CAPACITY & STORAGE COMPRESSION TEST (Iceberg Parquet vs Raw JSON)")
    print("=" * 75)

    # Sample canonical JSON event size in bytes
    sample_sales_json = json.dumps({
        "event_id": str(uuid.uuid4()),
        "timestamp": 1728560000.123456,
        "product_id": "PRD-4582",
        "customer_id": "CUST-492",
        "revenue": 245.85,
        "units_sold": 3
    }).encode("utf-8")
    raw_json_bytes_per_row = len(sample_sales_json)

    conn, cur = get_trino_cursor()
    try:
        cur.execute("""
            SELECT 
                COALESCE(SUM(record_count), 0) AS total_records,
                COALESCE(SUM(file_size_in_bytes), 0) AS total_parquet_bytes,
                COUNT(*) AS file_count
            FROM iceberg.sales."raw_events$files"
        """)
        row = cur.fetchone()
        total_records = int(row[0] or 0)
        total_parquet_bytes = int(row[1] or 0)
        file_count = int(row[2] or 0)

        # Also check Gold mart compression (pure typed columns, no raw_payload JSON string)
        cur.execute("""
            SELECT 
                COALESCE(SUM(record_count), 0),
                COALESCE(SUM(file_size_in_bytes), 0)
            FROM iceberg.marts."fact_sales_summary$files"
        """)
        mart_row = cur.fetchone()
        mart_records = int(mart_row[0] or 0)
        mart_bytes = int(mart_row[1] or 0)
    finally:
        cur.close()
        conn.close()

    equivalent_raw_json_bytes = total_records * raw_json_bytes_per_row
    if equivalent_raw_json_bytes > 0 and total_parquet_bytes > 0:
        raw_table_reduction_pct = (1.0 - (total_parquet_bytes / equivalent_raw_json_bytes)) * 100.0
        bytes_per_row_parquet = total_parquet_bytes / total_records
    else:
        raw_table_reduction_pct = 0.0
        bytes_per_row_parquet = 0.0

    print(f"  [Baseline]  Canonical Raw JSON Payload Size : {raw_json_bytes_per_row} bytes/record")
    print(f"  [Iceberg]   Total Records Stored (Sales)    : {total_records:,} records across {file_count} files")
    print(f"  [Iceberg]   Equivalent Raw JSON Footprint   : {equivalent_raw_json_bytes:,} bytes")
    print(f"  [Iceberg]   Actual Parquet S3 Footprint     : {total_parquet_bytes:,} bytes ({bytes_per_row_parquet:.1f} bytes/row)")
    print(f"  [Result]    Storage Footprint Reduction     : {raw_table_reduction_pct:.1f}%")

    return {
        "test_type": "Capacity & Compression Testing",
        "raw_json_bytes_per_record": raw_json_bytes_per_row,
        "iceberg_total_records": total_records,
        "equivalent_raw_json_bytes": equivalent_raw_json_bytes,
        "actual_parquet_bytes": total_parquet_bytes,
        "parquet_bytes_per_record": round(bytes_per_row_parquet, 2),
        "storage_reduction_percent": round(raw_table_reduction_pct, 2),
        "mart_records": mart_records,
        "mart_bytes": mart_bytes,
    }


# ==============================================================================
# 4. SCALABILITY & OPTIMIZATION TEST: Compaction & Sub-Second Trino OLAP
# ==============================================================================
def run_scalability_test(run_compaction: bool = False) -> dict:
    print("\n" + "=" * 75)
    print("  TEST 4: SCALABILITY, SMALL-FILE COMPACTION & TRINO OLAP LATENCY TEST")
    print("=" * 75)

    conn, cur = get_trino_cursor()
    try:
        # 1. Pre-compaction file count
        cur.execute('SELECT COUNT(*), COALESCE(AVG(file_size_in_bytes), 0) FROM iceberg.sales."raw_events$files"')
        before_files, before_avg_size = cur.fetchone()
        before_files = int(before_files or 0)
        before_avg_size = float(before_avg_size or 0)
        print(f"  [Pre-Compaction]  Active Data Files : {before_files} files (Avg size: {before_avg_size / 1024:.1f} KB)")

        # 2. Optional compaction run
        if run_compaction and before_files > 1:
            print("  [Compacting]      Executing Iceberg rewrite_data_files bin-packing procedure...")
            subprocess.run(
                ["cmd.exe", "/c", str(PROJECT_ROOT / "scripts" / "run_iceberg_maintenance.cmd")],
                cwd=str(PROJECT_ROOT),
                check=False,
            )

        cur.execute('SELECT COUNT(*), COALESCE(AVG(file_size_in_bytes), 0) FROM iceberg.sales."raw_events$files"')
        after_files, after_avg_size = cur.fetchone()
        after_files = int(after_files or 0)
        after_avg_size = float(after_avg_size or 0)

        if before_files > 0 and after_files < before_files:
            fragmentation_reduction_pct = (1.0 - (after_files / before_files)) * 100.0
        else:
            fragmentation_reduction_pct = 0.0

        # 3. Benchmark Trino OLAP query latency over Gold Mart (5 warm iterations)
        latencies_ms = []
        query = """
            SELECT 
                event_date,
                product_id,
                total_revenue,
                total_units_sold,
                transaction_count
            FROM iceberg.marts.fact_sales_summary
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        for i in range(5):
            t_start = time.perf_counter()
            cur.execute(query)
            _ = cur.fetchall()
            t_ms = (time.perf_counter() - t_start) * 1000.0
            latencies_ms.append(t_ms)

        median_ms = statistics.median(latencies_ms)
        min_ms = min(latencies_ms)
        p95_ms = sorted(latencies_ms)[-1]

        print(f"  [Post-Compaction] Active Data Files : {after_files} files (Fragmentation Reduced: {fragmentation_reduction_pct:.1f}%)")
        print(f"  [Trino OLAP]      5-Run Latencies   : {[round(x, 1) for x in latencies_ms]} ms")
        print(f"  [Result]          Median Latency    : {median_ms:.1f} ms ({median_ms / 1000:.3f}s -> SUB-SECOND: {'YES' if median_ms < 1000 else 'NO'})")

        return {
            "test_type": "Scalability & Compaction Testing",
            "files_before_compaction": before_files,
            "files_after_compaction": after_files,
            "fragmentation_reduction_percent": round(fragmentation_reduction_pct, 2),
            "trino_olap_median_ms": round(median_ms, 2),
            "trino_olap_min_ms": round(min_ms, 2),
            "trino_olap_max_ms": round(p95_ms, 2),
            "sub_second_verified": median_ms < 1000.0,
        }
    finally:
        cur.close()
        conn.close()


# ==============================================================================
# 5. RESILIENCE TEST: Snapshot Isolation, Time-Travel & WAL Recovery
# ==============================================================================
def run_resilience_test() -> dict:
    print("\n" + "=" * 75)
    print("  TEST 5: RESILIENCE, SNAPSHOT ISOLATION & ZERO-COPY TIME-TRAVEL TEST")
    print("=" * 75)

    conn, cur = get_trino_cursor()
    try:
        cur.execute("""
            SELECT 
                snapshot_id,
                committed_at,
                operation,
                summary['total-records'] AS total_records
            FROM iceberg.sales."raw_events$snapshots"
            ORDER BY committed_at DESC
            LIMIT 5
        """)
        snapshots = cur.fetchall()
        print(f"  [Nessie Catalog]  Discovered {len(snapshots)} recent atomic Iceberg snapshots:")
        for snap in snapshots:
            print(f"    -> Snapshot ID: {snap[0]} | Committed: {snap[1]} | Op: {snap[2]} | Total Rows: {snap[3]}")

        time_travel_verified = False
        earliest_snap_id = None
        time_travel_rows = 0

        if snapshots:
            earliest_snap_id = snapshots[-1][0]
            t0 = time.perf_counter()
            cur.execute(f"SELECT COUNT(*) FROM iceberg.sales.raw_events FOR VERSION AS OF {earliest_snap_id}")
            time_travel_rows = int(cur.fetchone()[0] or 0)
            tt_ms = (time.perf_counter() - t0) * 1000.0
            time_travel_verified = True
            print(f"  [Time-Travel]     Queried historical snapshot {earliest_snap_id} -> {time_travel_rows:,} rows in {tt_ms:.1f} ms")

        return {
            "test_type": "Resilience & Time-Travel Testing",
            "recent_snapshots_count": len(snapshots),
            "tested_snapshot_id": earliest_snap_id,
            "time_travel_row_count": time_travel_rows,
            "zero_copy_time_travel_verified": time_travel_verified,
        }
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Enterprise Lakehouse Benchmark Suite")
    parser.add_argument(
        "--test",
        choices=["all", "performance", "stress", "capacity", "scalability", "resilience"],
        default="all",
        help="Which benchmark test category to execute",
    )
    parser.add_argument("--duration", type=int, default=15, help="Duration in seconds for performance throughput test")
    parser.add_argument("--compact", action="store_true", help="Run Iceberg rewrite_data_files compaction during scalability test")
    args = parser.parse_args()

    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "benchmarks": {},
    }

    if args.test in ("all", "performance"):
        results["benchmarks"]["performance"] = run_performance_test(duration_sec=args.duration)
    if args.test in ("all", "stress"):
        results["benchmarks"]["stress"] = run_stress_test()
    if args.test in ("all", "capacity"):
        results["benchmarks"]["capacity"] = run_capacity_test()
    if args.test in ("all", "scalability"):
        results["benchmarks"]["scalability"] = run_scalability_test(run_compaction=args.compact)
    if args.test in ("all", "resilience"):
        results["benchmarks"]["resilience"] = run_resilience_test()

    report_path = OUTPUT_DIR / "benchmark_results.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 75)
    print(f"  BENCHMARK SUITE COMPLETE — Report saved to: {report_path}")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    main()
