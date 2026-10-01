from __future__ import annotations

import sys
from pathlib import Path

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

from pyspark.sql import SparkSession

STAGING_DIR = PROJECT_ROOT / "data" / "staging"


DIMENSION_DATASETS = [
    "dim_date",
    "dim_product",
    "dim_supplier",
    "dim_warehouse",
    "dim_region",
    "dim_customer_channel",
    "dim_scenario",
    "dim_employee_planner",
    "security_user",
]


def main() -> None:
    try:
        from utils.spark_session import get_spark_session
        spark = get_spark_session(env="local", app_name="Inspect-Dimension-Schemas")
    except (ImportError, AttributeError):
        spark = (
            SparkSession.builder
            .appName("Inspect-Dimension-Schemas")
            .master("local[*]")
            .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")

    for dataset in DIMENSION_DATASETS:
        path = STAGING_DIR / f"stg_{dataset}"

        print("\n" + "=" * 80)
        print(f"DATASET: {dataset}")
        print(f"PATH:    {path}")
        print("=" * 80)

        if not path.exists():
            print(f"[SKIP] Dataset does not exist: {path}")
            continue

        df = spark.read.parquet(str(path))

        print(f"Rows: {df.count():,}")
        print("\nSchema:")
        df.printSchema()

        print("\nColumns:")
        for column in df.columns:
            print(f"  - {column}")

        print("\nSample:")
        df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()