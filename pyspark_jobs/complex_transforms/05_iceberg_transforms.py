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
    # 1. Read the raw streaming tables
    print("📥 Reading raw streaming Iceberg tables...")
    try:
        sales_df = spark.table("nessie.sales.streaming_events")
        inv_df = spark.table("nessie.inventory.streaming_events")
    except Exception as e:
        print(f"❌ Error reading tables (has the stream run yet?): {e}")
        return

    # 2. Perform transformations and write using Spark SQL
    # This avoids the JVM crash associated with the PySpark v2 write API on Java 11.
    print("⚙️ Processing transformations and writing to nessie.marts.fact_sales_summary...")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.marts")
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.marts.fact_sales_summary (
        product_id STRING,
        sales_date DATE,
        total_revenue DOUBLE,
        total_units_sold BIGINT,
        transaction_count BIGINT
    ) USING iceberg
    PARTITIONED BY (sales_date)
    """)
    
    spark.sql("""
    MERGE INTO nessie.marts.fact_sales_summary t
    USING (
        SELECT 
            product_id, 
            CAST(ingested_at AS DATE) AS sales_date,
            SUM(revenue) AS total_revenue,
            SUM(units_sold) AS total_units_sold,
            COUNT(event_id) AS transaction_count
        FROM (
            SELECT *, row_number() over(partition by event_id order by timestamp desc) as rn
            FROM nessie.sales.streaming_events
            WHERE CAST(ingested_at AS DATE) = CURRENT_DATE
        )
        WHERE rn = 1
        GROUP BY 1, 2
    ) s ON t.product_id = s.product_id AND t.sales_date = s.sales_date
    WHEN MATCHED THEN UPDATE SET 
        total_revenue = s.total_revenue,
        total_units_sold = s.total_units_sold,
        transaction_count = s.transaction_count
    WHEN NOT MATCHED THEN INSERT (product_id, sales_date, total_revenue, total_units_sold, transaction_count)
    VALUES (s.product_id, s.sales_date, s.total_revenue, s.total_units_sold, s.transaction_count)
    """)
    
    print("✅ Successfully wrote nessie.marts.fact_sales_summary!")
    
    # Show a preview
    spark.sql("SELECT * FROM nessie.marts.fact_sales_summary LIMIT 5").show()

if __name__ == "__main__":
    run_transforms()
    print("pipeline complete!")
