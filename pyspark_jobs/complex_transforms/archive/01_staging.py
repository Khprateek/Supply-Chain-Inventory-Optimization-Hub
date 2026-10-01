"""
01_staging.py

Raw -> Staging PySpark pipeline for the Enterprise Supply Chain project.

Responsibilities:
    1. Discover raw Parquet datasets
    2. Read Parquet into Spark DataFrames
    3. Normalize column names
    4. Apply safe type normalization
    5. Add audit/lineage metadata
    6. Validate required technical properties
    7. Deduplicate using configured business keys where known
    8. Write staged Parquet datasets
    9. Produce a staging quality summary

Important:
    This module intentionally does NOT implement business transformations,
    surrogate keys, SCD Type 2, fact construction, or dimensional joins.
    Those belong to later pipeline stages.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import os
import platform

# ---------------------------------------------------------------------------
# Environment Setup (Must happen before pyspark import on Windows)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
pyspark_jobs_dir = PROJECT_ROOT / "pyspark_jobs"
if str(pyspark_jobs_dir) not in sys.path:
    sys.path.insert(0, str(pyspark_jobs_dir))

# Automatically set HADOOP_HOME for Windows local runs
if platform.system() == "Windows":
    import sys
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
        
    # Prevent PySpark crash when project path has spaces or ampersands (&)
    # The & character breaks cmd.exe which PySpark uses internally on Windows.
    if " " in sys.executable or "&" in sys.executable:
        os.environ["PYSPARK_PYTHON"] = "python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_STAGING_DIR = PROJECT_ROOT / "data" / "staging"


# Dataset-specific natural/business keys.
#
# IMPORTANT:
# These should be confirmed against the actual schemas in data/raw.
# If a dataset isn't listed here, we do NOT perform key-based deduplication.
#
# You should update this mapping after inspecting the actual Parquet schemas.
DATASET_KEYS: Dict[str, Sequence[str]] = {
    "dim_date": ["date_key"],
    "dim_warehouse": ["warehouse_key"],
    "dim_product": ["product_sku", "effective_from"],
    "dim_supplier": ["supplier_key"],
    "dim_customer_channel": ["customer_channel_key"],
    "dim_region": ["region_key"],
    "dim_scenario": ["scenario_key"],
    "dim_employee_planner": ["planner_key"],
    "security_user": ["user_key"],
    "bridge_product_supplier": ["product_sku", "supplier_key"],
    "fact_demand_forecast": ["forecast_key"],
    "fact_purchase_order": ["poline_key"],
    "fact_sales": ["sales_line_key"],
    "fact_inventory_snapshot": ["snapshot_key"],
    "fact_inventory_movement": ["movement_key"],
    "fact_supplier_monthly_performance": ["supplier_monthly_key"],
    "fact_stockout": ["stockout_event_key"],
    "fact_customer_returns": ["return_line_key"],
}


# Columns which are expected to contain dates.
#
# This is deliberately conservative. We only cast columns that actually
# exist in the DataFrame.
DATE_COLUMN_PATTERNS = (
    r".*_date$",
    r".*_dt$",
)


# Columns which are expected to contain timestamps.
TIMESTAMP_COLUMN_PATTERNS = (
    r".*_timestamp$",
    r".*_ts$",
    r"created_at$",
    r"updated_at$",
)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

logger = logging.getLogger("supply_chain_staging")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DatasetResult:
    """Result of processing one dataset."""

    dataset_name: str
    input_path: str
    output_path: str
    input_rows: int
    output_rows: int
    duplicate_rows_removed: int
    null_primary_key_rows: int
    status: str
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create the SparkSession.
    """

    try:
        from utils.spark_session import get_spark_session

        spark = get_spark_session(
            env="local",
            app_name="SupplyChain-Staging"
        )

        logger.info("Using project SparkSession utility.")
        return spark

    except (ImportError, AttributeError):
        logger.warning(
            "Could not load utils.spark_session.get_spark_session(). "
            "Using local SparkSession fallback."
        )

        return (
            SparkSession.builder
            .appName("SupplyChain-Staging")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )


# ---------------------------------------------------------------------------
# Dataset discovery
# ---------------------------------------------------------------------------

def discover_parquet_files(raw_dir: Path) -> List[Path]:
    """
    Discover Parquet datasets recursively.

    Supports:
        data/raw/file.parquet
        data/raw/dataset_name/*.parquet
    """

    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Raw directory does not exist: {raw_dir}"
        )

    files = sorted(raw_dir.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No Parquet files found under: {raw_dir}"
        )

    return files


def dataset_name_from_path(path: Path, raw_dir: Path) -> str:
    """
    Derive a logical dataset name in snake_case.

    Examples:
        data/raw/FactSales.parquet
            -> fact_sales
    """

    relative = path.relative_to(raw_dir)

    if len(relative.parts) == 1:
        name = relative.stem
    else:
        name = relative.parts[0]

    # Use the column normalizer to ensure it becomes snake_case
    return normalize_column_name(name)


def group_files_by_dataset(
    parquet_files: Sequence[Path],
    raw_dir: Path,
) -> Dict[str, List[Path]]:
    """Group Parquet files into logical datasets."""

    datasets: Dict[str, List[Path]] = {}

    for path in parquet_files:
        name = dataset_name_from_path(path, raw_dir)

        datasets.setdefault(name, []).append(path)

    return datasets


# ---------------------------------------------------------------------------
# Column handling
# ---------------------------------------------------------------------------

def normalize_column_name(name: str) -> str:
    """
    Convert a column name to snake_case.

    Examples:
        Product SKU -> product_sku
        ProductSKU  -> productsku
        Order-Date   -> order_date
    """

    normalized = name.strip()

    normalized = re.sub(
        r"([a-z0-9])([A-Z])",
        r"\1_\2",
        normalized,
    )

    normalized = normalized.lower()

    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)

    normalized = re.sub(r"_+", "_", normalized)

    normalized = normalized.strip("_")

    return normalized


def normalize_columns(df: DataFrame) -> DataFrame:
    """Normalize all DataFrame column names."""

    renamed = df

    seen = set()

    for original_name in df.columns:
        new_name = normalize_column_name(original_name)

        if not new_name:
            raise ValueError(
                f"Column '{original_name}' becomes an empty column name."
            )

        if new_name in seen:
            raise ValueError(
                f"Column name collision after normalization: "
                f"'{original_name}' -> '{new_name}'"
            )

        seen.add(new_name)

        if original_name != new_name:
            renamed = renamed.withColumnRenamed(
                original_name,
                new_name,
            )

    return renamed


# ---------------------------------------------------------------------------
# Type normalization
# ---------------------------------------------------------------------------

def _matches_any_pattern(
    column_name: str,
    patterns: Sequence[str],
) -> bool:
    return any(
        re.match(pattern, column_name)
        for pattern in patterns
    )


def normalize_date_columns(df: DataFrame) -> DataFrame:
    """
    Convert obvious date columns to Spark DateType.

    We only touch columns whose names strongly indicate they are dates.
    """

    result = df

    for column_name, data_type in df.dtypes:
        if _matches_any_pattern(
            column_name,
            DATE_COLUMN_PATTERNS,
        ):
            if data_type == "string":
                result = result.withColumn(
                    column_name,
                    F.to_date(F.col(column_name)),
                )

    return result


def normalize_timestamp_columns(df: DataFrame) -> DataFrame:
    """Convert obvious timestamp columns to Spark TimestampType."""

    result = df

    for column_name, data_type in df.dtypes:
        if _matches_any_pattern(
            column_name,
            TIMESTAMP_COLUMN_PATTERNS,
        ):
            if data_type == "string":
                result = result.withColumn(
                    column_name,
                    F.to_timestamp(F.col(column_name)),
                )

    return result


def normalize_types(df: DataFrame) -> DataFrame:
    """
    Apply conservative type normalization.

    We deliberately do not guess numeric types based purely on column names.
    Parquet's existing schema remains authoritative unless an obvious date
    or timestamp conversion is required.
    """

    result = normalize_date_columns(df)
    result = normalize_timestamp_columns(result)

    return result


# ---------------------------------------------------------------------------
# Audit metadata
# ---------------------------------------------------------------------------

def add_audit_columns(
    df: DataFrame,
    dataset_name: str,
    batch_id: str,
) -> DataFrame:
    """
    Add technical lineage metadata.

    These columns are useful for tracing records through the pipeline.
    """

    return (
        df
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_source",
            F.lit(dataset_name),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_duplicate_column_names(df: DataFrame) -> None:
    """Ensure the DataFrame doesn't contain duplicate column names."""

    normalized = [
        column.lower()
        for column in df.columns
    ]

    duplicates = {
        column
        for column in normalized
        if normalized.count(column) > 1
    }

    if duplicates:
        raise ValueError(
            f"Duplicate column names detected: {sorted(duplicates)}"
        )


def validate_non_empty_dataset(
    df: DataFrame,
    dataset_name: str,
) -> int:
    """Ensure a dataset contains at least one row."""

    row_count = df.count()

    if row_count == 0:
        raise ValueError(
            f"Dataset '{dataset_name}' contains zero rows."
        )

    return row_count


def validate_key_columns_exist(
    df: DataFrame,
    dataset_name: str,
) -> Optional[Sequence[str]]:
    """
    Validate configured natural/business key columns.

    Returns the configured key if present, otherwise None.
    """

    keys = DATASET_KEYS.get(dataset_name)

    if not keys:
        return None

    missing = [
        key
        for key in keys
        if key not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Dataset '{dataset_name}' is configured with key columns "
            f"that don't exist: {missing}"
        )

    return keys


def count_null_key_rows(
    df: DataFrame,
    keys: Optional[Sequence[str]],
) -> int:
    """Count records where any configured key column is NULL."""

    if not keys:
        return 0

    condition = None

    for key in keys:
        current = F.col(key).isNull()

        condition = (
            current
            if condition is None
            else condition | current
        )

    return df.filter(condition).count()


def validate_null_keys(
    df: DataFrame,
    dataset_name: str,
    keys: Optional[Sequence[str]],
) -> int:
    """Validate that configured primary/business keys are not NULL."""

    null_count = count_null_key_rows(df, keys)

    if null_count > 0:
        raise ValueError(
            f"Dataset '{dataset_name}' contains "
            f"{null_count:,} rows with NULL key values. "
            f"Keys: {list(keys or [])}"
        )

    return null_count


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(
    df: DataFrame,
    dataset_name: str,
    keys: Optional[Sequence[str]],
) -> Tuple[DataFrame, int]:
    """
    Deduplicate using explicitly configured business keys.

    We intentionally refuse to guess a primary key.
    """

    if not keys:
        logger.warning(
            "[%s] No deduplication key configured. "
            "Keeping all records.",
            dataset_name,
        )

        return df, 0

    before = df.count()

    deduplicated = df.dropDuplicates(list(keys))

    after = deduplicated.count()

    removed = before - after

    logger.info(
        "[%s] Deduplication: %s rows removed.",
        dataset_name,
        f"{removed:,}",
    )

    return deduplicated, removed


# ---------------------------------------------------------------------------
# Basic business-neutral validation
# ---------------------------------------------------------------------------

def validate_no_invalid_dates(
    df: DataFrame,
    dataset_name: str,
) -> None:
    """
    Detect invalid/null dates in columns that look like dates.

    This does not impose a project-specific date range.
    """

    date_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "date"
    ]

    for column_name in date_columns:
        invalid_count = df.filter(
            F.col(column_name).isNull()
        ).count()

        logger.info(
            "[%s] %s NULL date values: %s",
            dataset_name,
            column_name,
            f"{invalid_count:,}",
        )


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def write_staged_dataset(
    df: DataFrame,
    output_dir: Path,
    dataset_name: str,
    overwrite: bool = True,
) -> str:
    """Write one staged dataset as Parquet, partitioned or coalesced for optimization."""

    output_path = output_dir / f"stg_{dataset_name}"

    # Optimization: Repartition based on table scale
    if dataset_name.startswith("dim_") or dataset_name in ["security_user", "bridge_product_supplier"]:
        # Dimensions are relatively small, avoid many tiny files
        df_to_write = df.coalesce(1)
        writer = df_to_write.write
    elif dataset_name == "fact_inventory_snapshot":
        # Massive fact table: partition by date for downstream pruning
        writer = df.write.partitionBy("snapshot_date_key")
    elif dataset_name == "fact_sales":
        # Partition by order date
        writer = df.write.partitionBy("order_date_key")
    elif dataset_name == "fact_inventory_movement":
        writer = df.write.partitionBy("movement_date_key")
    else:
        # Default behavior: use spark's default partitioning
        writer = df.write

    writer = writer.mode("overwrite" if overwrite else "errorifexists")
    writer.parquet(str(output_path))

    return str(output_path)


# ---------------------------------------------------------------------------
# Dataset processing
# ---------------------------------------------------------------------------

def read_dataset(
    spark: SparkSession,
    files: Sequence[Path],
) -> DataFrame:
    """Read one logical dataset."""

    paths = [str(path) for path in files]

    return spark.read.parquet(*paths)


def process_dataset(
    spark: SparkSession,
    dataset_name: str,
    files: Sequence[Path],
    staging_dir: Path,
    batch_id: str,
) -> DatasetResult:
    """Run the complete staging flow for one dataset."""

    input_path = ", ".join(
        str(path)
        for path in files
    )

    logger.info("=" * 70)
    logger.info("Processing dataset: %s", dataset_name)
    logger.info("=" * 70)

    try:
        # ---------------------------------------------------------------
        # 1. Read
        # ---------------------------------------------------------------

        logger.info("[%s] Reading Parquet...", dataset_name)

        df = read_dataset(
            spark,
            files,
        )

        input_rows = validate_non_empty_dataset(
            df,
            dataset_name,
        )

        logger.info(
            "[%s] Input rows: %s",
            dataset_name,
            f"{input_rows:,}",
        )

        # ---------------------------------------------------------------
        # 2. Schema validation
        # ---------------------------------------------------------------

        validate_duplicate_column_names(df)

        logger.info(
            "[%s] Input schema:",
            dataset_name,
        )

        df.printSchema()

        # ---------------------------------------------------------------
        # 3. Column normalization
        # ---------------------------------------------------------------

        df = normalize_columns(df)

        # ---------------------------------------------------------------
        # 4. Type normalization
        # ---------------------------------------------------------------

        df = normalize_types(df)

        # ---------------------------------------------------------------
        # 5. Business key configuration
        # ---------------------------------------------------------------

        keys = validate_key_columns_exist(
            df,
            dataset_name,
        )

        if keys:
            logger.info(
                "[%s] Business key: %s",
                dataset_name,
                ", ".join(keys),
            )

        # ---------------------------------------------------------------
        # 6. Key null validation
        # ---------------------------------------------------------------

        null_primary_key_rows = validate_null_keys(
            df,
            dataset_name,
            keys,
        )

        # ---------------------------------------------------------------
        # 7. Dataset-specific deduplication
        # ---------------------------------------------------------------

        df, duplicate_rows_removed = deduplicate(
            df,
            dataset_name,
            keys,
        )

        # ---------------------------------------------------------------
        # 8. Basic validation
        # ---------------------------------------------------------------

        validate_no_invalid_dates(
            df,
            dataset_name,
        )

        # ---------------------------------------------------------------
        # 9. Add lineage
        # ---------------------------------------------------------------

        df = add_audit_columns(
            df,
            dataset_name,
            batch_id,
        )

        # ---------------------------------------------------------------
        # 10. Write
        # ---------------------------------------------------------------

        logger.info(
            "[%s] Writing staged Parquet...",
            dataset_name,
        )

        output_path = write_staged_dataset(
            df,
            staging_dir,
            dataset_name,
        )

        # ---------------------------------------------------------------
        # 11. Final count
        # ---------------------------------------------------------------

        output_rows = df.count()

        logger.info(
            "[%s] Output rows: %s",
            dataset_name,
            f"{output_rows:,}",
        )

        logger.info(
            "[%s] Output: %s",
            dataset_name,
            output_path,
        )

        return DatasetResult(
            dataset_name=dataset_name,
            input_path=input_path,
            output_path=output_path,
            input_rows=input_rows,
            output_rows=output_rows,
            duplicate_rows_removed=duplicate_rows_removed,
            null_primary_key_rows=null_primary_key_rows,
            status="PASS",
        )

    except Exception as exc:
        logger.exception(
            "[%s] FAILED",
            dataset_name,
        )

        return DatasetResult(
            dataset_name=dataset_name,
            input_path=input_path,
            output_path="",
            input_rows=0,
            output_rows=0,
            duplicate_rows_removed=0,
            null_primary_key_rows=0,
            status="FAIL",
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(results: Sequence[DatasetResult]) -> None:
    """Print final staging quality summary."""

    print()
    print("=" * 90)
    print("SUPPLY CHAIN PYSPARK STAGING SUMMARY")
    print("=" * 90)

    print(
        f"{'DATASET':<32}"
        f"{'INPUT':>15}"
        f"{'OUTPUT':>15}"
        f"{'DUP REMOVED':>15}"
        f"{'STATUS':>10}"
    )

    print("-" * 90)

    for result in results:
        print(
            f"{result.dataset_name:<32}"
            f"{result.input_rows:>15,}"
            f"{result.output_rows:>15,}"
            f"{result.duplicate_rows_removed:>15,}"
            f"{result.status:>10}"
        )

    print("-" * 90)

    passed = sum(
        result.status == "PASS"
        for result in results
    )

    failed = sum(
        result.status == "FAIL"
        for result in results
    )

    total_input = sum(
        result.input_rows
        for result in results
    )

    total_output = sum(
        result.output_rows
        for result in results
    )

    total_duplicates = sum(
        result.duplicate_rows_removed
        for result in results
    )

    print(f"Datasets processed : {len(results)}")
    print(f"Datasets passed    : {passed}")
    print(f"Datasets failed    : {failed}")
    print(f"Input rows         : {total_input:,}")
    print(f"Output rows        : {total_output:,}")
    print(f"Duplicates removed : {total_duplicates:,}")

    print()

    if failed:
        print("FAILED DATASETS")
        print("-" * 90)

        for result in results:
            if result.status == "FAIL":
                print(f"\n{result.dataset_name}")
                print(f"Error: {result.error}")

    print()
    print(
        f"FINAL STATUS: {'PASS' if failed == 0 else 'FAIL'}"
    )

    print("=" * 90)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Enterprise Supply Chain PySpark "
            "raw-to-staging pipeline"
        )
    )

    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help="Raw Parquet directory.",
    )

    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help="Staging output directory.",
    )

    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help=(
            "Process only one dataset. "
            "Example: --dataset sales"
        ),
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Optional pipeline batch identifier.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    batch_id = (
        args.batch_id
        or datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )

    logger.info("Starting Supply Chain PySpark Staging")
    logger.info("Project root : %s", PROJECT_ROOT)
    logger.info("Raw directory: %s", args.raw_dir)
    logger.info("Stage output : %s", args.staging_dir)
    logger.info("Batch ID     : %s", batch_id)

    args.staging_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------------------------------------------------------------
        # Discover datasets
        # ---------------------------------------------------------------

        parquet_files = discover_parquet_files(
            args.raw_dir
        )

        datasets = group_files_by_dataset(
            parquet_files,
            args.raw_dir,
        )

        if args.dataset:
            if args.dataset not in datasets:
                logger.error(
                    "Dataset '%s' was not found.",
                    args.dataset,
                )

                logger.info(
                    "Available datasets: %s",
                    ", ".join(sorted(datasets)),
                )

                return 1

            datasets = {
                args.dataset: datasets[args.dataset]
            }

        logger.info(
            "Discovered %d dataset(s): %s",
            len(datasets),
            ", ".join(sorted(datasets)),
        )

        # ---------------------------------------------------------------
        # Process datasets
        # ---------------------------------------------------------------

        results: List[DatasetResult] = []

        for dataset_name in sorted(datasets):
            result = process_dataset(
                spark=spark,
                dataset_name=dataset_name,
                files=datasets[dataset_name],
                staging_dir=args.staging_dir,
                batch_id=batch_id,
            )

            results.append(result)

        # ---------------------------------------------------------------
        # Final report
        # ---------------------------------------------------------------

        print_summary(results)

        failed = any(
            result.status == "FAIL"
            for result in results
        )

        return 1 if failed else 0

    finally:
        logger.info("Stopping SparkSession.")

        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
