"""
05_bigquery_stream.py

Architecture A: Streaming-First (Spark -> BigQuery)
Consumes from Kafka topics ('inventory_events' and 'sales_events') and
streams raw JSON records into BigQuery using the Python google-cloud-bigquery
client inside foreachBatch — avoiding the broken spark-bigquery JVM connector
entirely, which has a known javax.inject shading bug on Java 11+/Spark 4.0.
"""

import os
import sys
import platform
from pathlib import Path
from pyspark.sql import SparkSession

# ── Project root & path setup ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT / "pyspark_jobs") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "pyspark_jobs"))

# ── GCP Auth: use service account key from credentials/ ───────────────────────
# This works without gcloud CLI being installed on the machine.
SA_KEY_PATH = PROJECT_ROOT / "credentials" / "sa_dbt.json"
if SA_KEY_PATH.exists() and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_KEY_PATH)
    print(f"[ARCH-A] Using SA key: {SA_KEY_PATH}")

# Set GCP project from env or fall back to what's in the key file
if not os.environ.get("GCP_PROJECT_ID"):
    import json
    with open(SA_KEY_PATH) as f:
        os.environ["GCP_PROJECT_ID"] = json.load(f).get("project_id", "smart-supply-and-inventory")

# ── Windows: set HADOOP_HOME so Spark can use winutils ────────────────────────
if platform.system() == "Windows":
    import ctypes

    def _short(p: str) -> str:
        buf = ctypes.create_unicode_buffer(256)
        ctypes.windll.kernel32.GetShortPathNameW(p, buf, 256)
        return buf.value or p

    if not os.environ.get("HADOOP_HOME"):
        wdir = PROJECT_ROOT / "conf" / "spark" / "winutils" / "hadoop-3.3.6"
        short = _short(str(wdir))
        os.environ["HADOOP_HOME"] = short
        os.environ["PATH"] = short + os.sep + "bin" + os.pathsep + os.environ.get("PATH", "")

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:29092"
GCP_PROJECT  = os.environ.get("GCP_PROJECT_ID", "smart-supply-and-inventory")
BQ_DATASET   = os.environ.get("BQ_DATASET_RAW", "raw_supply_chain")

# Module-level BigQuery client singleton
_BQ_CLIENT = None

# Checkpoint base — Windows-safe path
CHECKPOINT_BASE = str(PROJECT_ROOT / "pyspark_jobs" / "streaming" / "checkpoints")


# ── Spark session ──────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Only the Kafka connector is loaded via Ivy.
    BigQuery writes are handled by the Python google-cloud-bigquery client,
    so we deliberately exclude the broken spark-bigquery JVM jar.
    """
    return (
        SparkSession.builder
        .appName("KafkaToBigQueryStream")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",   # Scala 2.13 / Spark 4.0
        )
        .config("spark.jars.ivy", "D:/tmp/spark_ivy")
        .getOrCreate()
    )


# ── Per-topic streaming function ───────────────────────────────────────────────
def stream_topic_to_bq(spark: SparkSession, topic: str, bq_table: str):
    """
    Read a Kafka topic and write each micro-batch to BigQuery via the
    Python client (pandas-based). This sidesteps the JVM connector entirely.
    """
    print(f"[ARCH-A] Subscribing to '{topic}' -> BQ table '{GCP_PROJECT}.{BQ_DATASET}.{bq_table}'")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS raw_payload",
            "timestamp                AS kafka_timestamp",
        )
    )

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Module-level singleton to avoid OAuth/HTTP overhead on every micro-batch
        global _BQ_CLIENT
        if '_BQ_CLIENT' not in globals() or _BQ_CLIENT is None:
            from google.cloud import bigquery as bq
            _BQ_CLIENT = bq.Client(project=GCP_PROJECT)

        client = _BQ_CLIENT
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{bq_table}"

        try:
            # Convert micro-batch to Pandas
            pdf = batch_df.toPandas()
            
            # Cast datetime columns to string for JSON serialization
            for col in pdf.select_dtypes(include=['datetime64', 'datetimetz', '<M8[ns]']).columns:
                pdf[col] = pdf[col].astype(str)
                
            records = pdf.to_dict(orient="records")

            # Use low-latency streaming inserts (~10ms) instead of BQ Load Jobs (15s latency)
            errors = client.insert_rows_json(table_ref, records)

            if not errors:
                print(f"[ARCH-A] batch {batch_id} -> {len(records):,} rows written to {bq_table}")
            else:
                print(f"[ARCH-A][ERROR] batch {batch_id} failed with {len(errors)} row errors. Sample: {errors[:2]}")

        except Exception as exc:
            print(f"[ARCH-A][ERROR] batch {batch_id} crashed: {exc}")

    chk = os.path.join(CHECKPOINT_BASE, f"v2_bq_{bq_table}")

    query = (
        raw.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", chk)
        .start()
    )

    return query


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    inv_q   = stream_topic_to_bq(spark, "inventory_events", "raw_inventory_events")
    sales_q = stream_topic_to_bq(spark, "sales_events",     "raw_sales_events")

    print("\n[ARCH-A] Both streams running. Press Ctrl+C to stop.\n")

    try:
        import time
        while True:
            for q in [inv_q, sales_q]:
                if q.exception():
                    print(f"\n[ARCH-A][CRITICAL] Stream failed: {q.exception()}")
                    inv_q.stop()
                    sales_q.stop()
                    raise q.exception()
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n[ARCH-A] Shutting down streams gracefully...")
        inv_q.stop()
        sales_q.stop()
