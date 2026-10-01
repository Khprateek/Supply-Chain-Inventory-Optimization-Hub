import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Force AWS SDK to see these variables globally
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

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
CREATE TABLE IF NOT EXISTS nessie.inventory.streaming_events (
    event_id STRING,
    timestamp DOUBLE,
    product_id STRING,
    warehouse_id STRING,
    quantity_change BIGINT,
    event_type STRING,
    ingested_at TIMESTAMP
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.sales.streaming_events (
    event_id STRING,
    timestamp DOUBLE,
    product_id STRING,
    customer_id STRING,
    revenue DOUBLE,
    units_sold BIGINT,
    ingested_at TIMESTAMP
) USING iceberg
""")

def start_stream(topic, schema, table_name):
    print(f"🎧 Listening to Kafka Topic: {topic} -> nessie.{table_name}...")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    enriched_df = parsed_df.withColumn("ingested_at", current_timestamp())

    return enriched_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", f"nessie.{table_name}") \
        .option("checkpointLocation", f"/opt/spark/work-dir/streaming/checkpoints/v2_iceberg_{table_name.replace('.','_')}_chkpt") \
        .start()

# ── Start Streams ─────────────────────────────────────────────────────────────
inv_query = start_stream("inventory_events", inventory_schema, "inventory.streaming_events")
sales_query = start_stream("sales_events", sales_schema, "sales.streaming_events")

print("✅ Both streams are running! Press Ctrl+C to stop.")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streams...")
    inv_query.stop()
    sales_query.stop()
