import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, window, date_trunc

# Setup Spark with Iceberg and Nessie
spark = SparkSession.builder \
    .appName("Iceberg-Batch-Transform") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
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
print("✅ Spark Session Created for Iceberg Batch Transforms.")


def run_transforms():
    # 1. Check source tables
    source_table = None
    try:
        if spark.table("nessie.sales.structured_events").count() > 0:
            source_table = "nessie.sales.structured_events"
        elif spark.table("nessie.sales.raw_events").count() > 0:
            source_table = "nessie.sales.raw_events"
    except Exception:
        pass

    if not source_table:
        for candidate in ["nessie.sales.structured_events", "nessie.sales.raw_events"]:
            try:
                spark.table(candidate)
                source_table = candidate
                break
            except Exception:
                continue

    if not source_table:
        print("❌ Source sales tables do not exist yet. Please run run_iceberg_stream.cmd first.")
        return

    print(f"📥 Using source table for transform: {source_table}")

    # 2. Ensure schema & target table exist
    print("⚙️ Ensuring nessie.marts namespace and fact_sales_summary table exist...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.marts")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.marts.fact_sales_summary (
        event_date DATE,
        product_id STRING,
        total_revenue DOUBLE,
        total_units_sold BIGINT,
        transaction_count BIGINT,
        avg_order_value DOUBLE
    ) USING iceberg
    PARTITIONED BY (event_date)
    """)

    # 3. Perform deduplication & MERGE INTO
    if "structured_events" in source_table:
        spark.sql("""
        MERGE INTO nessie.marts.fact_sales_summary t
        USING (
            SELECT 
                CAST(timestamp AS DATE) AS event_date,
                product_id, 
                ROUND(SUM(revenue), 2) AS total_revenue,
                SUM(units_sold) AS total_units_sold,
                COUNT(event_id) AS transaction_count,
                ROUND(AVG(revenue / NULLIF(units_sold, 0)), 2) AS avg_order_value
            FROM (
                SELECT *, row_number() over(partition by event_id order by timestamp desc) as rn
                FROM nessie.sales.structured_events
            )
            WHERE rn = 1
            GROUP BY 1, 2
        ) s ON t.product_id = s.product_id AND t.event_date = s.event_date
        WHEN MATCHED THEN UPDATE SET 
            total_revenue = s.total_revenue,
            total_units_sold = s.total_units_sold,
            transaction_count = s.transaction_count,
            avg_order_value = s.avg_order_value
        WHEN NOT MATCHED THEN INSERT (event_date, product_id, total_revenue, total_units_sold, transaction_count, avg_order_value)
        VALUES (s.event_date, s.product_id, s.total_revenue, s.total_units_sold, s.transaction_count, s.avg_order_value)
        """)
    else:
        # Fallback to direct raw JSON extraction
        from pyspark.sql.functions import from_json
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        raw_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", DoubleType(), True),
            StructField("product_id", StringType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("units_sold", LongType(), True)
        ])
        df = spark.table("nessie.sales.raw_events")
        parsed = df.select(from_json(col("raw_payload"), raw_schema).alias("d")).select("d.*")
        parsed.createOrReplaceTempView("temp_parsed_sales")

        spark.sql("""
        MERGE INTO nessie.marts.fact_sales_summary t
        USING (
            SELECT 
                CAST(FROM_UNIXTIME(timestamp) AS DATE) AS event_date,
                product_id, 
                ROUND(SUM(revenue), 2) AS total_revenue,
                SUM(units_sold) AS total_units_sold,
                COUNT(event_id) AS transaction_count,
                ROUND(AVG(revenue / NULLIF(units_sold, 0)), 2) AS avg_order_value
            FROM (
                SELECT *, row_number() over(partition by event_id order by timestamp desc) as rn
                FROM temp_parsed_sales
            )
            WHERE rn = 1
            GROUP BY 1, 2
        ) s ON t.product_id = s.product_id AND t.event_date = s.event_date
        WHEN MATCHED THEN UPDATE SET 
            total_revenue = s.total_revenue,
            total_units_sold = s.total_units_sold,
            transaction_count = s.transaction_count,
            avg_order_value = s.avg_order_value
        WHEN NOT MATCHED THEN INSERT (event_date, product_id, total_revenue, total_units_sold, transaction_count, avg_order_value)
        VALUES (s.event_date, s.product_id, s.total_revenue, s.total_units_sold, s.transaction_count, s.avg_order_value)
        """)

    print("✅ Successfully wrote nessie.marts.fact_sales_summary!")
    spark.sql("SELECT * FROM nessie.marts.fact_sales_summary LIMIT 5").show()


if __name__ == "__main__":
    run_transforms()
    print("pipeline complete!")
