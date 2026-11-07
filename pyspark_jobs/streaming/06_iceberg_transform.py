import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder.appName("Iceberg-Transform-Pipeline")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse")
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://s3:4566")
    .config("spark.sql.catalog.nessie.client.region", "us-east-1")
    .config("spark.sql.catalog.nessie.s3.region", "us-east-1")
    .config("spark.sql.catalog.nessie.s3.access-key-id", "test")
    .config("spark.sql.catalog.nessie.s3.secret-access-key", "test")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.endpoint", "http://s3:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.parquet.compression.codec", "zstd")
    .config("spark.sql.catalog.nessie.table-default.write.parquet.compression-codec", "zstd")
    .config("spark.sql.catalog.nessie.table-default.write.parquet.compression-level", "7")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

inventory_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("product_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("quantity_change", LongType(), True),
        StructField("event_type", StringType(), True),
    ]
)

sales_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("product_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("units_sold", LongType(), True),
    ]
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.inventory")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales")

spark.sql("""
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
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.compression-level' = '7'
)
""")

spark.sql("""
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
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.compression-level' = '7'
)
""")

# ── Dead-Letter Queue (DLQ) Quarantine Tables ────────────────────────────────
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.inventory.dlq_events (
    kafka_key STRING,
    raw_payload STRING,
    error_reason STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP,
    quarantined_at TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.compression-level' = '7'
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.sales.dlq_events (
    kafka_key STRING,
    raw_payload STRING,
    error_reason STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP,
    quarantined_at TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.compression-level' = '7'
)
""")


def start_transform(schema, source_table, target_table):
    dlq_table = target_table.replace("structured_events", "dlq_events")
    print(f"Routing Stream: {source_table} -> Valid: {target_table} | DLQ: {dlq_table}...")

    # Augment schema with corrupt record handler for malformed JSON strings
    schema_with_corrupt = StructType(
        schema.fields + [StructField("_corrupt_record", StringType(), True)]
    )

    df = spark.readStream.format("iceberg").load(f"nessie.{source_table}")

    def process_micro_batch(batch_df, batch_id):
        # Micro-batch empty check (fast-path skip)
        if batch_df.rdd.isEmpty():
            return

        # 1. Parse JSON with PERMISSIVE mode capturing syntax errors in _corrupt_record
        parsed = batch_df.select(
            col("kafka_key"),
            col("raw_payload"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            from_json(
                col("raw_payload"),
                schema_with_corrupt,
                {"columnNameOfCorruptRecord": "_corrupt_record", "mode": "PERMISSIVE"},
            ).alias("data"),
        )

        # 2. Evaluate error classification rules
        error_expr = (
            when(col("data._corrupt_record").isNotNull(), lit("MALFORMED_JSON"))
            .when(col("data.event_id").isNull(), lit("MISSING_EVENT_ID"))
            .when(col("data.product_id").isNull(), lit("MISSING_PRODUCT_ID"))
        )
        if "sales" in target_table:
            error_expr = error_expr.when(
                col("data.revenue").isNull() | (col("data.revenue") < 0),
                lit("INVALID_REVENUE"),
            )
        elif "inventory" in target_table:
            error_expr = error_expr.when(
                col("data.warehouse_id").isNull(),
                lit("MISSING_WAREHOUSE_ID"),
            )
        error_expr = error_expr.otherwise(lit(None))

        classified_df = parsed.withColumn("error_reason", error_expr)

        # 3. Cache micro-batch in memory so valid and DLQ branches do NOT re-parse JSON
        classified_df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            valid_df = classified_df.filter(col("error_reason").isNull())
            dlq_df = classified_df.filter(col("error_reason").isNotNull())

            # 4. Write valid records to Silver structured table
            if not valid_df.rdd.isEmpty():
                field_cols = [
                    col(f"data.{f.name}").cast("timestamp")
                    if f.name == "timestamp"
                    else col(f"data.{f.name}")
                    for f in schema.fields
                ]
                valid_output = valid_df.select(
                    *field_cols,
                    col("ingested_at"),
                    current_timestamp().alias("transformed_at"),
                )
                valid_output.coalesce(4).write.format("iceberg").mode("append").save(
                    f"nessie.{target_table}"
                )

            # 5. Write poisoned / corrupt records to DLQ table (Zero overhead when 100% data is clean)
            if not dlq_df.rdd.isEmpty():
                dlq_output = dlq_df.select(
                    col("kafka_key"),
                    col("raw_payload"),
                    col("error_reason"),
                    col("topic"),
                    col("partition"),
                    col("offset"),
                    col("kafka_timestamp"),
                    col("ingested_at"),
                    current_timestamp().alias("quarantined_at"),
                )
                print(
                    f"  [DLQ ALERT] Batch {batch_id}: Quarantining {dlq_output.count()} bad record(s) -> nessie.{dlq_table}"
                )
                dlq_output.coalesce(1).write.format("iceberg").mode("append").save(
                    f"nessie.{dlq_table}"
                )

        finally:
            classified_df.unpersist()

    chkpt_base = os.environ.get(
        "CHECKPOINT_BASE_DIR", "/opt/spark/work-dir/streaming/checkpoints"
    ).rstrip("/")
    chkpt_path = f"{chkpt_base}/v2_transform_{target_table.replace('.','_')}_chkpt"

    return (
        df.writeStream.trigger(processingTime="10 seconds")
        .option("checkpointLocation", chkpt_path)
        .foreachBatch(process_micro_batch)
        .start()
    )


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
