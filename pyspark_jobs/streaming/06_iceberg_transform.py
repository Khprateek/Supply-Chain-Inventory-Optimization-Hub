import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("Iceberg-Transform-Pipeline") \
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.inventory")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales")

spark.sql('''
CREATE TABLE IF NOT EXISTS nessie.inventory.structured_events (
    event_id STRING,
    timestamp TIMESTAMP,
    product_id STRING,
    warehouse_id STRING,
    quantity_change BIGINT,
    event_type STRING,
    ingested_at TIMESTAMP,
    transformed_at TIMESTAMP
) USING iceberg
''')

spark.sql('''
CREATE TABLE IF NOT EXISTS nessie.sales.structured_events (
    event_id STRING,
    timestamp TIMESTAMP,
    product_id STRING,
    customer_id STRING,
    revenue DOUBLE,
    units_sold BIGINT,
    ingested_at TIMESTAMP,
    transformed_at TIMESTAMP
) USING iceberg
''')

def start_transform(schema, source_table, target_table):
    print(f"Reading from {source_table} -> {target_table}...")
    
    df = spark.readStream \
        .format("iceberg") \
        .load(f"nessie.{source_table}")

    parsed_df = df.select(
        from_json(col("raw_payload"), schema).alias("data"),
        col("ingested_at")
    ).select("data.*", "ingested_at")

    enriched_df = parsed_df \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("transformed_at", current_timestamp())

    return enriched_df.coalesce(4).writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("path", f"nessie.{target_table}") \
        .option("checkpointLocation", f"/opt/spark/work-dir/streaming/checkpoints/v2_transform_{target_table.replace('.','_')}_chkpt") \
        .start()

inv_query = start_transform(inventory_schema, "inventory.raw_events", "inventory.structured_events")
sales_query = start_transform(sales_schema, "sales.raw_events", "sales.structured_events")

print("Both transforms running! Press Ctrl+C to stop.")

try:
    while True:
        for q in [inv_query, sales_query]:
            if q.exception():
                print(f"[CRITICAL] Transform failed: {q.exception()}")
                inv_query.stop()
                sales_query.stop()
                raise q.exception()
        time.sleep(5)
except (KeyboardInterrupt, SystemExit):
    print("Stopping transforms gracefully...")
    try:
        inv_query.stop()
        sales_query.stop()
    except Exception:
        pass
