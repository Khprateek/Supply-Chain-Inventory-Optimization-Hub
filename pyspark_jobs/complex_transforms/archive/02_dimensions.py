"""
02_dimensions.py

Staging -> Kimball dimensions.

Responsibilities:
    1. Read staged Parquet datasets
    2. Build conformed dimensions
    3. Generate surrogate keys
    4. Apply dimensional business rules
    5. Maintain SCD Type 2 history for dim_product
    6. Validate dimension uniqueness
    7. Write dimensional Parquet datasets
"""

from __future__ import annotations

import os
import platform
import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Windows Environment Setup
# ---------------------------------------------------------------------------
if platform.system() == "Windows":
    import ctypes
    
    def get_short_path(long_path: str) -> str:
        buffer = ctypes.create_unicode_buffer(256)
        ctypes.windll.kernel32.GetShortPathNameW(long_path, buffer, 256)
        return buffer.value or long_path
        
    if not os.environ.get("HADOOP_HOME"):
        winutils_dir = PROJECT_ROOT / "conf" / "spark" / "winutils" / "hadoop-3.3.6"
        short_winutils = get_short_path(str(winutils_dir))
        os.environ["HADOOP_HOME"] = short_winutils
        os.environ["PATH"] = short_winutils + os.sep + "bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGING_DIR = PROJECT_ROOT / "data" / "staging"
DIMENSION_DIR = PROJECT_ROOT / "data" / "dimensions"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("supply_chain_dimensions")

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    try:
        from utils.spark_session import get_spark_session
        return get_spark_session(env="local", app_name="SupplyChain-Dimensions")
    except (ImportError, AttributeError):
        return (
            SparkSession.builder
            .appName("SupplyChain-Dimensions")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

# ---------------------------------------------------------------------------
# I/O Helpers
# ---------------------------------------------------------------------------

def read_staged(spark: SparkSession, dataset_name: str) -> DataFrame:
    """Read a dataset from the staging layer."""
    path = STAGING_DIR / f"stg_{dataset_name}"

    if not path.exists():
        raise FileNotFoundError(f"Staged dataset not found: {path}")

    return spark.read.parquet(str(path))


def write_dimension(df: DataFrame, dimension_name: str) -> None:
    """Write a dimension dataset to the dimension layer."""
    output_path = DIMENSION_DIR / dimension_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )
    logger.info(f"Wrote dimension {dimension_name} to {output_path} ({df.count():,} rows)")

# ---------------------------------------------------------------------------
# Dimension Builders
# ---------------------------------------------------------------------------

def build_dim_date(df: DataFrame) -> DataFrame:
    """Build the Date dimension."""
    # dim_date is usually pre-generated and static. We just add the SK.
    return (
        df
        .withColumn("date_sk", F.col("date_key"))
        .dropDuplicates(["date_sk"])
    )

def build_dim_region(df: DataFrame) -> DataFrame:
    """Build the Region dimension."""
    return (
        df
        .select(
            "region_key",
            "region_code",
            "region_name",
            "theater",
            "primary_country",
            "currency_code",
            "regional_director"
        )
        .dropDuplicates(["region_key"])
        .withColumn("region_sk", F.xxhash64("region_key"))
    )

def build_dim_supplier(df: DataFrame) -> DataFrame:
    """Build the Supplier dimension."""
    return (
        df
        .select(
            "supplier_key",
            "supplier_code",
            "supplier_name",
            "country",
            "city",
            "region_zone",
            "supplier_tier",
            "contract_lead_time_days",
            "lead_time_tolerance_days",
            "payment_terms_days",
            "minimum_order_quantity",
            "preferred_status_flag",
            "vendor_risk_score"
        )
        .dropDuplicates(["supplier_key"])
        .withColumn("supplier_sk", F.xxhash64("supplier_key"))
    )

def build_dim_warehouse(df: DataFrame) -> DataFrame:
    """Build the Warehouse dimension."""
    return (
        df
        .select(
            "warehouse_key",
            "warehouse_code",
            "warehouse_name",
            "region_key",
            "facility_type",
            "storage_capacity_pallets",
            "total_area_sq_meters",
            "refrigerated_capacity_pallets",
            "operating_hours_per_week",
            "active_flag"
        )
        .dropDuplicates(["warehouse_key"])
        .withColumn("warehouse_sk", F.xxhash64("warehouse_key"))
    )

def build_dim_customer_channel(df: DataFrame) -> DataFrame:
    """Build the Customer Channel dimension."""
    return (
        df
        .select(
            "customer_channel_key",
            "customer_channel_code",
            "channel_name",
            "customer_account_name",
            "customer_segment",
            "credit_terms_days",
            "delivery_priority_tier"
        )
        .dropDuplicates(["customer_channel_key"])
        .withColumn("customer_channel_sk", F.xxhash64("customer_channel_key"))
    )

def build_dim_scenario(df: DataFrame) -> DataFrame:
    """Build the Scenario dimension."""
    return (
        df
        .select(
            "scenario_key",
            "scenario_code",
            "scenario_name",
            "description",
            "demand_multiplier",
            "lead_time_shock_days",
            "service_level_target_pct",
            "annual_carrying_cost_rate_pct"
        )
        .dropDuplicates(["scenario_key"])
        .withColumn("scenario_sk", F.xxhash64("scenario_key"))
    )

def build_dim_employee_planner(df: DataFrame) -> DataFrame:
    """Build the Employee Planner dimension."""
    return (
        df
        .select(
            "planner_key",
            "employee_number",
            "planner_name",
            "email_address",
            "job_role",
            "department",
            "assigned_region_key",
            "assigned_category_group"
        )
        .dropDuplicates(["planner_key"])
        .withColumn("planner_sk", F.xxhash64("planner_key"))
    )

def build_security_user(df: DataFrame) -> DataFrame:
    """Build the Security User dimension."""
    return (
        df
        .dropDuplicates(["user_key"])
        .withColumn("user_sk", F.xxhash64("user_key"))
    )

# ---------------------------------------------------------------------------
# SCD Type 2 Logic for Product Dimension
# ---------------------------------------------------------------------------

def build_dim_product_scd2(df: DataFrame) -> DataFrame:
    """
    Build the Product dimension implementing SCD Type 2 logic.
    Identifies changes based on the tracked columns and versioning.
    """
    business_key = "product_sku"
    
    tracked_columns = [
        "product_name",
        "brand_name",
        "category_name",
        "subcategory_name",
        "department_name",
        "unit_standard_cost",
        "unit_list_price",
        "handling_profile",
        "storage_class",
        "weight_kg",
        "volume_cubic_meters",
        "primary_supplier_key",
        "abcclassification",
        "xyzclassification"
    ]
    
    event_timestamp_col = "effective_from"
    
    # 1. Base selection
    base_cols = [business_key, event_timestamp_col] + tracked_columns
    
    # Filter rows with missing business keys
    clean_df = df.select(*base_cols).filter(F.col(business_key).isNotNull())
    
    # 2. Hash tracked columns to easily detect changes
    hash_expr = F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_columns])
    clean_df = clean_df.withColumn("row_hash", hash_expr)
    
    # 3. Window by business key to order versions over time
    window_spec = Window.partitionBy(business_key).orderBy(event_timestamp_col)
    
    # 4. Compare current row hash to previous row hash
    df_with_prev = clean_df.withColumn("prev_hash", F.lag("row_hash").over(window_spec))
    
    # Keep row if it's the first record or if an attribute actually changed
    changed_df = df_with_prev.filter(
        F.col("prev_hash").isNull() | (F.col("row_hash") != F.col("prev_hash"))
    )
    
    # 5. Calculate SCD2 validity range and current flag
    change_window = Window.partitionBy(business_key).orderBy(event_timestamp_col)
    
    final_scd2 = (
        changed_df
        .withColumn(
            "effective_to", 
            F.lead(event_timestamp_col).over(change_window)
        )
        .withColumn(
            "is_current",
            F.col("effective_to").isNull()
        )
    )
    
    # Close open records with '9999-12-31'
    final_scd2 = final_scd2.withColumn(
        "effective_to",
        F.coalesce("effective_to", F.to_date(F.lit("9999-12-31")))
    )
    
    # 6. Generate Surrogate Key representing this specific SCD2 version
    final_scd2 = final_scd2.withColumn(
        "product_sk", 
        F.xxhash64(business_key, event_timestamp_col)
    )
    
    final_columns = ["product_sk", business_key] + tracked_columns + [
        event_timestamp_col, "effective_to", "is_current"
    ]
    
    return final_scd2.select(*final_columns)


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Starting 02_dimensions pipeline...")
    spark = get_spark()
    
    try:
        dimensions_to_build = [
            ("dim_date", build_dim_date),
            ("dim_region", build_dim_region),
            ("dim_supplier", build_dim_supplier),
            ("dim_warehouse", build_dim_warehouse),
            ("dim_customer_channel", build_dim_customer_channel),
            ("dim_scenario", build_dim_scenario),
            ("dim_employee_planner", build_dim_employee_planner),
        ]
        
        for dim_name, build_func in dimensions_to_build:
            logger.info(f"Building {dim_name}...")
            try:
                stg_df = read_staged(spark, dim_name)
                dim_df = build_func(stg_df)
                write_dimension(dim_df, dim_name)
            except FileNotFoundError:
                logger.warning(f"Skipping {dim_name}: Staging data not found.")
            except Exception as e:
                logger.error(f"Failed to build {dim_name}: {e}")

        # Build security_user
        try:
            logger.info("Building security_user...")
            stg_security_df = read_staged(spark, "security_user")
            dim_security_df = build_security_user(stg_security_df)
            write_dimension(dim_security_df, "security_user")
        except FileNotFoundError:
            logger.warning("Skipping security_user: Staging data not found.")
        except Exception as e:
            logger.error(f"Failed to build security_user: {e}")

        # Build dim_product SCD Type 2
        logger.info("Building dim_product (SCD Type 2)...")
        try:
            stg_product_df = read_staged(spark, "dim_product")
            dim_product_scd2_df = build_dim_product_scd2(stg_product_df)
            write_dimension(dim_product_scd2_df, "dim_product")
        except FileNotFoundError:
            logger.warning("Skipping dim_product: Staging data not found.")
        except Exception as e:
            logger.error(f"Failed to build dim_product: {e}")

        logger.info("02_dimensions pipeline completed.")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
