import os
import sys
from pathlib import Path

from pyspark.sql.functions import col

# Centralized SparkSession factory resolution (supports host local and Docker container environments)
try:
    from pyspark_jobs.utils.spark_session import get_spark_session
except ImportError:
    current_dir = Path(__file__).resolve().parent
    work_dir = current_dir.parent
    if str(work_dir) not in sys.path:
        sys.path.insert(0, str(work_dir))
    from utils.spark_session import get_spark_session

env = os.getenv("SPARK_ENV", "cluster" if os.path.exists("/.dockerenv") else "local")
spark = get_spark_session(env=env, app_name="Iceberg-Batch-Transform")
spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark Session Initialized via Shared Factory (env={env}). Iceberg & ZSTD Enabled.")


def run_sales_transforms():
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
        print("⚠️ Source sales tables do not exist yet. Skipping sales transform.")
        return

    print(f"📥 Using source table for sales transform: {source_table}")

    # 2. Ensure schema & target table exist with Iceberg ZSTD compression
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
    TBLPROPERTIES (
        'write.parquet.compression-codec' = 'zstd',
        'write.parquet.compression-level' = '7'
    )
    """)

    # 3. Perform windowed deduplication & MERGE INTO
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
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        raw_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", DoubleType(), True),
                StructField("product_id", StringType(), True),
                StructField("revenue", DoubleType(), True),
                StructField("units_sold", LongType(), True),
            ]
        )
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


def run_inventory_health_transforms():
    """Build Gold-tier Inventory Health & Optimization Mart."""
    source_table = None
    for candidate in ["nessie.inventory.structured_events", "nessie.inventory.raw_events"]:
        try:
            spark.table(candidate)
            source_table = candidate
            break
        except Exception:
            continue

    if not source_table:
        print("⚠️ Source inventory tables do not exist yet. Skipping inventory transform.")
        return

    print(f"📥 Using source table for inventory health transform: {source_table}")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.marts.fact_inventory_health (
        snapshot_date DATE,
        warehouse_id STRING,
        product_id STRING,
        current_on_hand BIGINT,
        total_receipts BIGINT,
        total_picks BIGINT,
        daily_turnover_rate DOUBLE,
        days_of_supply DOUBLE,
        stockout_risk_status STRING
    ) USING iceberg
    PARTITIONED BY (snapshot_date)
    TBLPROPERTIES (
        'write.parquet.compression-codec' = 'zstd',
        'write.parquet.compression-level' = '7'
    )
    """)

    if "structured_events" in source_table:
        spark.sql("""
        MERGE INTO nessie.marts.fact_inventory_health t
        USING (
            SELECT
                CAST(timestamp AS DATE) AS snapshot_date,
                warehouse_id,
                product_id,
                COALESCE(SUM(quantity_change), 0) AS current_on_hand,
                COALESCE(SUM(CASE WHEN event_type = 'RECEIPT' THEN quantity_change ELSE 0 END), 0) AS total_receipts,
                COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) AS total_picks,
                ROUND(COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) /
                      NULLIF(GREATEST(COALESCE(SUM(quantity_change), 0), 1), 0), 4) AS daily_turnover_rate,
                ROUND(COALESCE(SUM(quantity_change), 0) /
                      NULLIF(COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) / 7.0, 0), 1) AS days_of_supply,
                CASE
                    WHEN COALESCE(SUM(quantity_change), 0) <= 10 THEN 'CRITICAL_STOCKOUT'
                    WHEN COALESCE(SUM(quantity_change), 0) <= 50 THEN 'LOW_STOCK_WARNING'
                    ELSE 'OPTIMAL'
                END AS stockout_risk_status
            FROM (
                SELECT *, row_number() over(partition by event_id order by timestamp desc) as rn
                FROM nessie.inventory.structured_events
            )
            WHERE rn = 1
            GROUP BY 1, 2, 3
        ) s ON t.warehouse_id = s.warehouse_id AND t.product_id = s.product_id AND t.snapshot_date = s.snapshot_date
        WHEN MATCHED THEN UPDATE SET
            current_on_hand = s.current_on_hand,
            total_receipts = s.total_receipts,
            total_picks = s.total_picks,
            daily_turnover_rate = s.daily_turnover_rate,
            days_of_supply = s.days_of_supply,
            stockout_risk_status = s.stockout_risk_status
        WHEN NOT MATCHED THEN INSERT (snapshot_date, warehouse_id, product_id, current_on_hand, total_receipts, total_picks, daily_turnover_rate, days_of_supply, stockout_risk_status)
        VALUES (s.snapshot_date, s.warehouse_id, s.product_id, s.current_on_hand, s.total_receipts, s.total_picks, s.daily_turnover_rate, s.days_of_supply, s.stockout_risk_status)
        """)
    else:
        from pyspark.sql.functions import from_json
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

        raw_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", DoubleType(), True),
                StructField("product_id", StringType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("quantity_change", LongType(), True),
                StructField("event_type", StringType(), True),
            ]
        )
        df = spark.table("nessie.inventory.raw_events")
        parsed = df.select(from_json(col("raw_payload"), raw_schema).alias("d")).select("d.*")
        parsed.createOrReplaceTempView("temp_parsed_inv")

        spark.sql("""
        MERGE INTO nessie.marts.fact_inventory_health t
        USING (
            SELECT
                CAST(FROM_UNIXTIME(timestamp) AS DATE) AS snapshot_date,
                warehouse_id,
                product_id,
                COALESCE(SUM(quantity_change), 0) AS current_on_hand,
                COALESCE(SUM(CASE WHEN event_type = 'RECEIPT' THEN quantity_change ELSE 0 END), 0) AS total_receipts,
                COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) AS total_picks,
                ROUND(COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) /
                      NULLIF(GREATEST(COALESCE(SUM(quantity_change), 0), 1), 0), 4) AS daily_turnover_rate,
                ROUND(COALESCE(SUM(quantity_change), 0) /
                      NULLIF(COALESCE(SUM(CASE WHEN event_type = 'PICK' THEN ABS(quantity_change) ELSE 0 END), 0) / 7.0, 0), 1) AS days_of_supply,
                CASE
                    WHEN COALESCE(SUM(quantity_change), 0) <= 10 THEN 'CRITICAL_STOCKOUT'
                    WHEN COALESCE(SUM(quantity_change), 0) <= 50 THEN 'LOW_STOCK_WARNING'
                    ELSE 'OPTIMAL'
                END AS stockout_risk_status
            FROM (
                SELECT *, row_number() over(partition by event_id order by timestamp desc) as rn
                FROM temp_parsed_inv
            )
            WHERE rn = 1
            GROUP BY 1, 2, 3
        ) s ON t.warehouse_id = s.warehouse_id AND t.product_id = s.product_id AND t.snapshot_date = s.snapshot_date
        WHEN MATCHED THEN UPDATE SET
            current_on_hand = s.current_on_hand,
            total_receipts = s.total_receipts,
            total_picks = s.total_picks,
            daily_turnover_rate = s.daily_turnover_rate,
            days_of_supply = s.days_of_supply,
            stockout_risk_status = s.stockout_risk_status
        WHEN NOT MATCHED THEN INSERT (snapshot_date, warehouse_id, product_id, current_on_hand, total_receipts, total_picks, daily_turnover_rate, days_of_supply, stockout_risk_status)
        VALUES (s.snapshot_date, s.warehouse_id, s.product_id, s.current_on_hand, s.total_receipts, s.total_picks, s.daily_turnover_rate, s.days_of_supply, s.stockout_risk_status)
        """)

    print("✅ Successfully wrote nessie.marts.fact_inventory_health!")


def run_transforms():
    run_sales_transforms()
    run_inventory_health_transforms()
    print("🎉 Lakehouse Batch Transforms Complete.")


if __name__ == "__main__":
    run_transforms()
