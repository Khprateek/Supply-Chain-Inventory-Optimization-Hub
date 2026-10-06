import sys
import os
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Setup Spark with Iceberg and Nessie configurations
# Packages are provided via spark-submit (--packages), avoiding duplicate Ivy resolution
spark = SparkSession.builder \
    .appName("Iceberg-Streaming-Pipeline") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse") \
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://s3:4566") \
    .config("spark.sql.catalog.nessie.client.region", "us-east-1") \
    .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
    .config("spark.sql.catalog.nessie.s3.access-key-id", "test") \
    .config("spark.sql.catalog.nessie.s3.secret-access-key", "test") \
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://s3:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session Created. Iceberg Nessie Catalog Initialized.")

# ── Schemas ───────────────────────────────────────────────────────────────────
inventory_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity_change", LongType(), True),
    StructField("event_type", StringType(), True)
])

sales_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("units_sold", LongType(), True)
])

# ── Initialization ────────────────────────────────────────────────────────────
print("Initializing Iceberg Namespaces and Tables...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.inventory")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.inventory.raw_events (
    kafka_key STRING,
    raw_payload STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.sales.raw_events (
    kafka_key STRING,
    raw_payload STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP
) USING iceberg
""")

def start_stream(topic, table_name):
    print(f"Listening to Kafka Topic: {topic} -> nessie.{table_name}...")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 50000) \
        .option("minPartitions", 4) \
        .load()

    raw_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at")
    )

    return raw_df.coalesce(4).writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", f"nessie.{table_name}") \
        .option("checkpointLocation", f"/opt/spark/work-dir/streaming/checkpoints/v2_iceberg_{table_name.replace('.','_')}_chkpt") \
        .start()

# ── Start Streams ─────────────────────────────────────────────────────────────
inv_query = start_stream("inventory_events", "inventory.raw_events")
sales_query = start_stream("sales_events", "sales.raw_events")

print("Both streams are running! Metrics reporting active.")

metrics_file = "/opt/spark/work-dir/metrics_iceberg.json"

def write_atomic_metrics(data):
    try:
        temp_file = f"{metrics_file}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(temp_file, metrics_file)
    except Exception:
        pass

# Immediately register active queries in metrics file
write_atomic_metrics({
    "inventory": {"id": str(inv_query.id), "name": "inventory", "inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0, "batchDuration": 0, "batchId": 0},
    "sales": {"id": str(sales_query.id), "name": "sales", "inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0, "batchDuration": 0, "batchId": 0},
    "inputRate": 0.0,
    "procRate": 0.0,
    "duration": 0.0
})

try:
    while True:
        metrics = {}
        for q, name in [(inv_query, "inventory"), (sales_query, "sales")]:
            if q.exception():
                print(f"[CRITICAL] Stream failed: {q.exception()}")
                inv_query.stop()
                sales_query.stop()
                raise q.exception()
            if q.lastProgress:
                prog = dict(q.lastProgress)
                duration_ms = 0
                if "durationMs" in prog and isinstance(prog["durationMs"], dict):
                    duration_ms = prog["durationMs"].get("triggerExecution", 0)
                elif "batchDuration" in prog:
                    duration_ms = prog["batchDuration"]
                prog["batchDuration"] = duration_ms
                metrics[name] = prog
            else:
                metrics[name] = {
                    "id": str(q.id),
                    "name": name,
                    "inputRowsPerSecond": 0.0,
                    "processedRowsPerSecond": 0.0,
                    "batchDuration": 0,
                    "batchId": 0
                }
        
        total_in = sum(v.get("inputRowsPerSecond", 0.0) for v in metrics.values() if isinstance(v, dict))
        total_proc = sum(v.get("processedRowsPerSecond", 0.0) for v in metrics.values() if isinstance(v, dict))
        avg_dur = sum(v.get("batchDuration", 0.0) for v in metrics.values() if isinstance(v, dict)) / max(len(metrics), 1)
        metrics["inputRate"] = round(total_in, 2)
        metrics["procRate"] = round(total_proc, 2)
        metrics["duration"] = round(avg_dur, 2)

        write_atomic_metrics(metrics)
        time.sleep(2)
except (KeyboardInterrupt, SystemExit):
    print("Stopping streams gracefully...")
    try:
        inv_query.stop()
        sales_query.stop()
    except Exception:
        pass
    write_atomic_metrics({
        "inventory": {"inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0, "batchDuration": 0},
        "sales": {"inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0, "batchDuration": 0},
        "inputRate": 0.0,
        "procRate": 0.0,
        "duration": 0.0
    })
