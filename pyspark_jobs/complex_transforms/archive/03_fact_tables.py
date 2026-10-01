"""
03_fact_tables.py

Staging -> Dimensional Fact Tables.

Responsibilities:
    1. Read staged fact tables.
    2. Read dimensional tables (SCD2 and standard).
    3. Perform dimensional lookups to resolve surrogate keys.
    4. Handle SCD2 temporal joins.
    5. Validate row counts and foreign key integrity.
    6. Write partitioned fact Parquet datasets.
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import sys
import tempfile
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Environment Setup (Must happen before pyspark import on Windows)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
pyspark_jobs_dir = PROJECT_ROOT / "pyspark_jobs"
if str(pyspark_jobs_dir) not in sys.path:
    sys.path.insert(0, str(pyspark_jobs_dir))

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
        
    if " " in sys.executable or "&" in sys.executable:
        os.environ["PYSPARK_PYTHON"] = "python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_STAGING_DIR = PROJECT_ROOT / "data" / "staging"
DEFAULT_DIMENSIONS_DIR = PROJECT_ROOT / "data" / "dimensions"
DEFAULT_FACTS_DIR = PROJECT_ROOT / "data" / "facts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class FactResult:
    fact_name: str
    input_rows: int
    output_rows: int
    status: str
    error: Optional[str] = None
    missing_fks: Dict[str, int] = None


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    try:
        from utils.spark_session import get_spark_session
        spark = get_spark_session(
            env="local",
            app_name="SupplyChain-Facts"
        )
        logger.info("Using project SparkSession utility.")
        return spark
    except (ImportError, AttributeError):
        logger.warning("Could not load utils.spark_session.get_spark_session(). Using local fallback.")
        return (
            SparkSession.builder
            .appName("SupplyChain-Facts")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )


# ---------------------------------------------------------------------------
# Dimension Loaders
# ---------------------------------------------------------------------------
def load_dimensions(spark: SparkSession, dims_dir: Path) -> Dict[str, DataFrame]:
    """Load dimensional datasets to be broadcasted/joined against facts."""
    dims = {}
    expected_dims = [
        "dim_product", "dim_warehouse", "dim_customer_channel", "dim_date",
        "dim_supplier", "dim_region", "dim_scenario", "dim_employee_planner"
    ]
    
    for dim_name in expected_dims:
        path = dims_dir / dim_name
        if path.exists():
            dims[dim_name] = spark.read.parquet(str(path))
            logger.info("Loaded dimension: %s", dim_name)
        else:
            logger.warning("Dimension not found: %s", dim_name)
            
    return dims


# ---------------------------------------------------------------------------
# Validation Helpers
# ---------------------------------------------------------------------------
def validate_fact(
    df: DataFrame,
    input_count: int,
    fact_name: str,
    fk_columns: List[str]
) -> FactResult:
    """Validate referential integrity and row count conservation."""
    
    output_count = df.count()
    if output_count != input_count:
        return FactResult(
            fact_name=fact_name,
            input_rows=input_count,
            output_rows=output_count,
            status="FAIL",
            error=f"Row count changed unexpectedly: {input_count:,} -> {output_count:,}"
        )
        
    missing_fks = {}
    for col in fk_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        missing_fks[col] = null_count
        
    total_missing = sum(missing_fks.values())
    
    if total_missing > 0:
        logger.error("[%s] Missing Foreign Keys:", fact_name)
        for col, count in missing_fks.items():
            if count > 0:
                logger.error("  - %s: %d nulls", col, count)
        return FactResult(
            fact_name=fact_name,
            input_rows=input_count,
            output_rows=output_count,
            status="FAIL",
            error=f"Referential integrity failure: {total_missing} total null FKs.",
            missing_fks=missing_fks
        )
        
    logger.info("[%s] Validation PASS. %d rows. %d FKs intact.", fact_name, output_count, len(fk_columns))
    return FactResult(
        fact_name=fact_name,
        input_rows=input_count,
        output_rows=output_count,
        status="PASS",
        missing_fks=missing_fks
    )


# ---------------------------------------------------------------------------
# Output Writers
# ---------------------------------------------------------------------------
def write_fact(
    df: DataFrame,
    output_dir: Path,
    fact_name: str,
    partition_column: str = None
) -> str:
    output_path = output_dir / fact_name
    
    writer = df.write.mode("overwrite")
    if partition_column:
        writer = writer.partitionBy(partition_column)
        
    writer.parquet(str(output_path))
    return str(output_path)


# ---------------------------------------------------------------------------
# Fact Builders
# ---------------------------------------------------------------------------
def build_fact_sales(
    spark: SparkSession,
    staging_dir: Path,
    dims: Dict[str, DataFrame]
) -> DataFrame:
    """
    Build FactSales:
      1. Joins Date (order_date_key)
      2. Joins Warehouse (warehouse_key)
      3. Joins Customer Channel (customer_channel_key)
      4. Joins Product (product_sku + temporal order_date)
    """
    logger.info("Building FactSales...")
    stg_path = staging_dir / "stg_fact_sales"
    fact = spark.read.parquet(str(stg_path))
    
    input_count = fact.count()
    logger.info("[FactSales] Input rows: %d", input_count)

    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_customer_channel = F.broadcast(dims["dim_customer_channel"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_product = dims["dim_product"] 
    
    fact = fact.join(
        dim_warehouse.select("warehouse_sk", "warehouse_key"),
        on="warehouse_key",
        how="left"
    )
    
    fact = fact.join(
        dim_customer_channel.select("customer_channel_sk", "customer_channel_key"),
        on="customer_channel_key",
        how="left"
    )
    
    fact = fact.join(
        dim_date.select(
            F.col("date_sk").alias("order_date_sk"),
            F.col("date_key").alias("order_date_key")
        ),
        on="order_date_key",
        how="left"
    )
    
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & 
        (fact["order_date"] >= dim_product["effective_from"]) & 
        (fact["order_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"]) 
    
    fact = fact.withColumn(
        "gross_margin", 
        F.col("gross_sales_amount") - F.col("cost_of_goods_sold")
    )
    
    final_cols = [
        "sales_line_key",
        "sales_order_id",
        "sales_order_line_number",
        "order_date_sk",
        "order_date_key", 
        "product_sk",
        "warehouse_sk",
        "customer_channel_sk",
        "ship_date_key",
        "delivery_date_key",
        "ordered_quantity",
        "shipped_quantity",
        "cancelled_quantity",
        "unit_price",
        "unit_standard_cost",
        "gross_sales_amount",
        "discount_amount",
        "net_sales_amount",
        "cost_of_goods_sold",
        "gross_margin",
        "order_line_cycle_time_days",
        "on_time_in_full_flag"
    ]
    
    fact_final = fact.select(*final_cols)
    
    validation = validate_fact(
        fact_final, 
        input_count, 
        "FactSales", 
        ["order_date_sk", "product_sk", "warehouse_sk", "customer_channel_sk"]
    )
    
    if validation.status == "FAIL":
        raise ValueError(f"FactSales validation failed: {validation.error}")
        
    return fact_final


def build_fact_inventory_snapshot(
    spark: SparkSession,
    staging_dir: Path,
    dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactInventorySnapshot...")
    stg_path = staging_dir / "stg_fact_inventory_snapshot"
    fact = spark.read.parquet(str(stg_path))
    
    input_count = fact.count()
    logger.info("[FactInventorySnapshot] Input rows: %d", input_count)

    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_product = dims["dim_product"]
    
    # 1. Repartition for performance on massive dataset
    fact = fact.repartition("snapshot_date_key")

    # 2. Joins
    fact = fact.join(
        dim_warehouse.select("warehouse_sk", "warehouse_key"),
        on="warehouse_key",
        how="left"
    )
    
    fact = fact.join(
        dim_date.select(
            F.col("date_sk").alias("snapshot_date_sk"),
            F.col("date_key").alias("snapshot_date_key")
        ),
        on="snapshot_date_key",
        how="left"
    )
    
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & 
        (fact["snapshot_date"] >= dim_product["effective_from"]) & 
        (fact["snapshot_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = [
        "snapshot_key",
        "snapshot_date_sk",
        "snapshot_date_key", # Keep for partitioning
        "product_sk",
        "warehouse_sk",
        "on_hand_quantity",
        "reserved_quantity",
        "available_quantity",
        "in_transit_inbound_quantity",
        "unit_landed_cost",
        "inventory_valuation",
        "days_since_last_movement",
        "is_stockout_flag",
        "is_dead_stock_flag"
    ]
    
    fact_final = fact.select(*final_cols)
    
    validation = validate_fact(
        fact_final, 
        input_count, 
        "FactInventorySnapshot", 
        ["snapshot_date_sk", "product_sk", "warehouse_sk"]
    )
    
    if validation.status == "FAIL":
        raise ValueError(f"FactInventorySnapshot validation failed: {validation.error}")
        
    return fact_final


def build_fact_inventory_movement(
    spark: SparkSession,
    staging_dir: Path,
    dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactInventoryMovement...")
    stg_path = staging_dir / "stg_fact_inventory_movement"
    fact = spark.read.parquet(str(stg_path))
    
    input_count = fact.count()
    logger.info("[FactInventoryMovement] Input rows: %d", input_count)

    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_employee = F.broadcast(dims["dim_employee_planner"])
    dim_product = dims["dim_product"]
    
    # 1. Date lookup
    fact = fact.withColumn("movement_date_key", F.date_format(F.col("movement_date"), "yyyyMMdd").cast("int"))
    fact = fact.join(
        dim_date.select(
            F.col("date_sk").alias("movement_date_sk"),
            F.col("date_key").alias("movement_date_key")
        ),
        on="movement_date_key",
        how="left"
    )
    
    # 2. Origin Warehouse lookup
    fact = fact.join(
        dim_warehouse.select(F.col("warehouse_sk").alias("origin_warehouse_sk"), F.col("warehouse_key").alias("origin_warehouse_key")),
        on="origin_warehouse_key",
        how="left"
    )
    
    # 3. Destination Warehouse lookup
    fact = fact.join(
        dim_warehouse.select(F.col("warehouse_sk").alias("destination_warehouse_sk"), F.col("warehouse_key").alias("destination_warehouse_key")),
        on="destination_warehouse_key",
        how="left"
    )
    
    # 4. Employee lookup
    fact = fact.join(
        dim_employee.select(F.col("planner_sk").alias("responsible_employee_sk"), F.col("planner_key").alias("responsible_employee_key")),
        on="responsible_employee_key",
        how="left"
    )
    
    # 5. Product lookup (SCD2 temporal join based on movement_date)
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & 
        (fact["movement_date"] >= dim_product["effective_from"]) & 
        (fact["movement_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = [
        "movement_key",
        "movement_transaction_id",
        "movement_date_sk",
        "movement_date_key",
        "product_sk",
        "origin_warehouse_sk",
        "destination_warehouse_sk",
        "responsible_employee_sk",
        "movement_type",
        "movement_quantity",
        "movement_value",
        "transfer_freight_cost",
        "transfer_transit_days"
    ]
    
    fact_final = fact.select(*final_cols)
    
    validation = validate_fact(
        fact_final, 
        input_count, 
        "FactInventoryMovement", 
        ["movement_date_sk", "product_sk", "origin_warehouse_sk"] # Only check non-nullable FKs
    )
    
    if validation.status == "FAIL":
        raise ValueError(f"FactInventoryMovement validation failed: {validation.error}")
        
    return fact_final


def build_fact_customer_returns(
    spark: SparkSession,
    staging_dir: Path,
    dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactCustomerReturns...")
    stg_path = staging_dir / "stg_fact_customer_returns"
    fact = spark.read.parquet(str(stg_path))
    
    input_count = fact.count()
    logger.info("[FactCustomerReturns] Input rows: %d", input_count)

    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_customer_channel = F.broadcast(dims["dim_customer_channel"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_product = dims["dim_product"]
    
    # Date lookups (Return date & Original order date)
    fact = fact.join(
        dim_date.select(F.col("date_sk").alias("return_date_sk"), F.col("date_key").alias("return_date_key")),
        on="return_date_key",
        how="left"
    )
    fact = fact.join(
        dim_date.select(F.col("date_sk").alias("original_order_date_sk"), F.col("date_key").alias("original_order_date_key")),
        on="original_order_date_key",
        how="left"
    )
    
    # Warehouse lookup
    fact = fact.join(
        dim_warehouse.select(F.col("warehouse_sk").alias("receiving_warehouse_sk"), F.col("warehouse_key").alias("receiving_warehouse_key")),
        on="receiving_warehouse_key",
        how="left"
    )
    
    # Customer Channel lookup
    fact = fact.join(
        dim_customer_channel.select("customer_channel_sk", "customer_channel_key"),
        on="customer_channel_key",
        how="left"
    )
    
    # Product lookup (SCD2 temporal join based on return_date)
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & 
        (fact["return_date"] >= dim_product["effective_from"]) & 
        (fact["return_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = [
        "return_line_key",
        "return_id",
        "return_date_sk",
        "original_order_date_sk",
        "product_sk",
        "receiving_warehouse_sk",
        "customer_channel_sk",
        "returned_quantity",
        "restocked_quantity",
        "scrapped_quantity",
        "refund_amount",
        "return_reason_category",
        "disposition_status"
    ]
    
    fact_final = fact.select(*final_cols)
    
    validation = validate_fact(
        fact_final, 
        input_count, 
        "FactCustomerReturns", 
        ["return_date_sk", "product_sk", "receiving_warehouse_sk", "customer_channel_sk"]
    )
    
    if validation.status == "FAIL":
        raise ValueError(f"FactCustomerReturns validation failed: {validation.error}")
        
    return fact_final


def build_fact_purchase_order(
    spark: SparkSession,
    staging_dir: Path,
    dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactPurchaseOrder...")
    stg_path = staging_dir / "stg_fact_purchase_order"
    fact = spark.read.parquet(str(stg_path))
    
    input_count = fact.count()
    logger.info("[FactPurchaseOrder] Input rows: %d", input_count)

    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_supplier = F.broadcast(dims["dim_supplier"])
    dim_employee = F.broadcast(dims["dim_employee_planner"])
    dim_product = dims["dim_product"]
    
    # 1. Date lookups
    fact = fact.join(
        dim_date.select(F.col("date_sk").alias("pocreation_date_sk"), F.col("date_key").alias("pocreation_date_key")),
        on="pocreation_date_key", how="left"
    )
    fact = fact.join(
        dim_date.select(F.col("date_sk").alias("promised_delivery_date_sk"), F.col("date_key").alias("promised_delivery_date_key")),
        on="promised_delivery_date_key", how="left"
    )
    # cast actual_dock_receipt_date_key from double to int before join
    fact = fact.withColumn("actual_dock_receipt_date_key_int", F.col("actual_dock_receipt_date_key").cast("int"))
    fact = fact.join(
        dim_date.select(F.col("date_sk").alias("actual_dock_receipt_date_sk"), F.col("date_key").alias("actual_dock_receipt_date_key_int")),
        on="actual_dock_receipt_date_key_int", how="left"
    ).drop("actual_dock_receipt_date_key_int")
    
    # 2. Supplier
    fact = fact.join(
        dim_supplier.select("supplier_sk", "supplier_key"),
        on="supplier_key", how="left"
    )
    
    # 3. Warehouse
    fact = fact.join(
        dim_warehouse.select(F.col("warehouse_sk").alias("receiving_warehouse_sk"), F.col("warehouse_key").alias("receiving_warehouse_key")),
        on="receiving_warehouse_key", how="left"
    )
    
    # 4. Employee
    fact = fact.join(
        dim_employee.select(F.col("planner_sk").alias("buyer_employee_sk"), F.col("planner_key").alias("buyer_employee_key")),
        on="buyer_employee_key", how="left"
    )
    
    # 5. Product (SCD2 based on pocreation_date)
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & 
        (fact["pocreation_date"] >= dim_product["effective_from"]) & 
        (fact["pocreation_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = [
        "poline_key", "purchase_order_id", "poline_number",
        "pocreation_date_sk", "pocreation_date_key", 
        "promised_delivery_date_sk", "actual_dock_receipt_date_sk",
        "supplier_sk", "product_sk", "receiving_warehouse_sk", "buyer_employee_sk",
        "ordered_quantity", "received_quantity", "accepted_quantity", "rejected_quantity",
        "unit_purchase_price", "extended_poamount", 
        "promised_lead_time_days", "actual_lead_time_days", "lead_time_variance_days",
        "is_delivered_on_time_flag", "is_delivered_in_full_flag", "is_supplier_otifflag", "postatus"
    ]
    
    fact_final = fact.select(*final_cols)
    
    validation = validate_fact(fact_final, input_count, "FactPurchaseOrder", 
                               ["pocreation_date_sk", "supplier_sk", "product_sk", "receiving_warehouse_sk", "buyer_employee_sk"])
    if validation.status == "FAIL": raise ValueError(f"FactPurchaseOrder validation failed: {validation.error}")
    return fact_final


def build_fact_demand_forecast(
    spark: SparkSession, staging_dir: Path, dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactDemandForecast...")
    stg_path = staging_dir / "stg_fact_demand_forecast"
    fact = spark.read.parquet(str(stg_path))
    input_count = fact.count()
    
    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_scenario = F.broadcast(dims["dim_scenario"])
    dim_product = dims["dim_product"]
    
    fact = fact.join(dim_date.select(F.col("date_sk").alias("target_period_date_sk"), F.col("date_key").alias("target_period_date_key")), on="target_period_date_key", how="left")
    fact = fact.join(dim_date.select(F.col("date_sk").alias("forecast_generated_date_sk"), F.col("date_key").alias("forecast_generated_date_key")), on="forecast_generated_date_key", how="left")
    fact = fact.join(dim_warehouse.select("warehouse_sk", "warehouse_key"), on="warehouse_key", how="left")
    fact = fact.join(dim_scenario.select("scenario_sk", "scenario_key"), on="scenario_key", how="left")
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & (fact["forecast_generated_date"] >= dim_product["effective_from"]) & (fact["forecast_generated_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = ["forecast_key", "target_period_date_sk", "forecast_generated_date_sk", "target_period_date_key", 
                  "product_sk", "warehouse_sk", "scenario_sk", "forecasted_quantity", "baseline_statistical_quantity", 
                  "planner_adjustment_quantity", "forecast_value", "forecast_model_version"]
    fact_final = fact.select(*final_cols)
    validate_fact(fact_final, input_count, "FactDemandForecast", ["target_period_date_sk", "product_sk", "warehouse_sk", "scenario_sk"])
    return fact_final


def build_fact_stockout(
    spark: SparkSession, staging_dir: Path, dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactStockout...")
    stg_path = staging_dir / "stg_fact_stockout"
    fact = spark.read.parquet(str(stg_path))
    input_count = fact.count()
    
    dim_warehouse = F.broadcast(dims["dim_warehouse"])
    dim_date = F.broadcast(dims["dim_date"])
    dim_product = dims["dim_product"]
    
    fact = fact.join(dim_date.select(F.col("date_sk").alias("start_date_sk"), F.col("date_key").alias("start_date_key")), on="start_date_key", how="left")
    fact = fact.join(dim_date.select(F.col("date_sk").alias("end_date_sk"), F.col("date_key").alias("end_date_key")), on="end_date_key", how="left")
    fact = fact.join(dim_warehouse.select("warehouse_sk", "warehouse_key"), on="warehouse_key", how="left")
    fact = fact.join(
        dim_product.select("product_sk", "product_sku", "effective_from", "effective_to"),
        (fact["product_sku"] == dim_product["product_sku"]) & (fact["start_date"] >= dim_product["effective_from"]) & (fact["start_date"] < dim_product["effective_to"]),
        how="left"
    ).drop(dim_product["product_sku"])
    
    final_cols = ["stockout_event_key", "start_date_sk", "end_date_sk", "start_date_key", "product_sk", "warehouse_sk", 
                  "stockout_duration_days", "estimated_lost_demand_units", "estimated_lost_revenue_amount", "stockout_attributed_reason", "severity_tier"]
    fact_final = fact.select(*final_cols)
    validate_fact(fact_final, input_count, "FactStockout", ["start_date_sk", "product_sk", "warehouse_sk"])
    return fact_final


def build_fact_supplier_monthly_performance(
    spark: SparkSession, staging_dir: Path, dims: Dict[str, DataFrame]
) -> DataFrame:
    logger.info("Building FactSupplierMonthlyPerformance...")
    stg_path = staging_dir / "stg_fact_supplier_monthly_performance"
    fact = spark.read.parquet(str(stg_path))
    input_count = fact.count()
    
    dim_supplier = F.broadcast(dims["dim_supplier"])
    dim_date = F.broadcast(dims["dim_date"])
    
    fact = fact.join(dim_date.select(F.col("date_sk").alias("year_month_date_sk"), F.col("date_key").alias("year_month_date_key")), on="year_month_date_key", how="left")
    fact = fact.join(dim_supplier.select("supplier_sk", "supplier_key"), on="supplier_key", how="left")
    
    final_cols = ["supplier_monthly_key", "year_month_date_sk", "year_month_date_key", "supplier_sk", "year_month", "total_pocount", "total_polines", 
                  "total_ordered_quantity", "total_received_quantity", "total_rejected_quantity", "total_spend_amount", "on_time_pocount", 
                  "in_full_pocount", "otifline_count", "otifrate_pct", "average_lead_time_days", "lead_time_std_dev_days", 
                  "late_delivery_count", "line_fill_rate_pct", "supplier_monthly_risk_rating"]
    fact_final = fact.select(*final_cols)
    validate_fact(fact_final, input_count, "FactSupplierMonthlyPerformance", ["year_month_date_sk", "supplier_sk"])
    return fact_final
# ---------------------------------------------------------------------------
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enterprise Supply Chain Fact Builder")
    parser.add_argument("--staging-dir", type=Path, default=DEFAULT_STAGING_DIR)
    parser.add_argument("--dimensions-dir", type=Path, default=DEFAULT_DIMENSIONS_DIR)
    parser.add_argument("--facts-dir", type=Path, default=DEFAULT_FACTS_DIR)
    parser.add_argument("--fact", type=str, default=None, help="Specific fact to build (e.g. FactSales)")
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    logger.info("Starting Fact Tables Build (Stage 3)")
    
    args.facts_dir.mkdir(parents=True, exist_ok=True)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        dims = load_dimensions(spark, args.dimensions_dir)
        
        target_facts = [
            "FactSales",
            "FactInventorySnapshot",
            "FactInventoryMovement",
            "FactCustomerReturns",
            "FactPurchaseOrder",
            "FactDemandForecast",
            "FactStockout",
            "FactSupplierMonthlyPerformance"
        ]
        
        if args.fact:
            target_facts = [args.fact]
            
        for fact_name in target_facts:
            if fact_name == "FactSales":
                df_fact = build_fact_sales(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="order_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactInventorySnapshot":
                df_fact = build_fact_inventory_snapshot(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="snapshot_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactInventoryMovement":
                df_fact = build_fact_inventory_movement(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="movement_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactCustomerReturns":
                df_fact = build_fact_customer_returns(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name)
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactPurchaseOrder":
                df_fact = build_fact_purchase_order(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="pocreation_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactDemandForecast":
                df_fact = build_fact_demand_forecast(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="target_period_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactStockout":
                df_fact = build_fact_stockout(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="start_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            elif fact_name == "FactSupplierMonthlyPerformance":
                df_fact = build_fact_supplier_monthly_performance(spark, args.staging_dir, dims)
                output_path = write_fact(df_fact, args.facts_dir, fact_name, partition_column="year_month_date_key")
                logger.info("Successfully wrote %s to %s", fact_name, output_path)
            else:
                logger.warning("Fact table %s is not yet implemented.", fact_name)
                
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
