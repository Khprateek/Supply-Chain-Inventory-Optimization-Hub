import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# Setup Spark with Iceberg, Nessie, and Kafka packages
spark = SparkSession.builder \
    .appName("Iceberg-Streaming-Pipeline") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "software.amazon.awssdk:bundle:2.20.18,"
            "software.amazon.awssdk:url-connection-client:2.20.18") \
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
print("✅ Spark Session Created. Iceberg Nessie Catalog Initialized.")

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
print("📦 Initializing Iceberg Namespaces and Tables...")
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
PARTITIONED BY (days(ingested_at))
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
PARTITIONED BY (days(ingested_at))
""")

def start_stream(topic, table_name):
    print(f"🎧 Listening to Kafka Topic: {topic} -> nessie.{table_name}...")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "true") \
        .option("maxOffsetsPerTrigger", 50000) \
        .option("minPartitions", 16) \
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
        .trigger(processingTime="1 minute") \
        .option("path", f"nessie.{table_name}") \
        .option("checkpointLocation", f"/opt/spark/work-dir/streaming/checkpoints/v2_iceberg_{table_name.replace('.','_')}_chkpt") \
        .start()

# ── Start Streams ─────────────────────────────────────────────────────────────
inv_query = start_stream("inventory_events", "inventory.raw_events")
sales_query = start_stream("sales_events", "sales.raw_events")

print("✅ Both streams are running! Press Ctrl+C to stop.")

try:
    import json
    import time
    metrics_file = "/opt/spark/work-dir/metrics_iceberg.json"
    while True:
        metrics = {}
        for q, name in [(inv_query, "inventory"), (sales_query, "sales")]:
            if q.exception():
                print(f"[CRITICAL] Stream failed: {q.exception()}")
                inv_query.stop()
                sales_query.stop()
                raise q.exception()
            if q.lastProgress:
                metrics[name] = q.lastProgress
        if metrics:
            try:
                with open(metrics_file, "w") as f:
                    json.dump(metrics, f)
            except Exception:
                pass
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopping streams gracefully...")
    inv_query.stop()
    sales_query.stop()
