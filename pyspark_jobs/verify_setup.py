"""
verify_setup.py
===============
Quick smoke-test script to verify environment and lakehouse prerequisites.

Run this to verify that:
  1. PySpark is correctly installed.
  2. The SparkSession factory works in local mode.
  3. Basic Spark operations (DataFrame creation, SQL, Parquet write) work.
  4. Windows winutils / HADOOP_HOME is properly configured.
  5. Lakehouse services (Kafka, Nessie, S3, Trino) are reachable.

Usage (PowerShell):
    python pyspark_jobs/verify_setup.py
"""

import os
import sys
import tempfile
import platform
import subprocess
import socket

# Make sure we import from the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"
WARN = "[WARN]"
SEP  = "-" * 70

all_passed = True


def check(label: str, condition: bool, details: str = "") -> None:
    global all_passed
    suffix = f" -- {details}" if details else ""
    if condition:
        print(f"  {PASS} {label}{suffix}")
    else:
        print(f"  {FAIL} {label}{suffix}")
        all_passed = False


def section(title: str) -> None:
    print(f"\n-- {title} {'-' * (65 - len(title))}")


# ── Windows winutils / HADOOP_HOME ──────────────────────────────────────────
if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
    winutils_dir = os.path.normpath(
        os.path.join(PROJECT_ROOT, "conf", "spark", "winutils", "hadoop-3.3.6")
    )
    if os.path.isdir(winutils_dir):
        os.environ["HADOOP_HOME"] = winutils_dir
        print(f"  {INFO} Auto-set HADOOP_HOME = {winutils_dir}")

_python_exe = sys.executable
if platform.system() == "Windows" and " " in _python_exe:
    try:
        import ctypes
        buf = ctypes.create_unicode_buffer(512)
        if ctypes.windll.kernel32.GetShortPathNameW(_python_exe, buf, 512):
            if buf.value and os.path.exists(buf.value):
                _python_exe = buf.value
    except Exception:
        pass

os.environ["PYSPARK_PYTHON"] = _python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = _python_exe


# ===========================================================================
# 1. PySpark installation
# ===========================================================================
section("1. Checking PySpark installation")
try:
    import pyspark
    check("PySpark installed", True, f"version {pyspark.__version__}")
except ImportError as e:
    check("PySpark installed", False, str(e))
    print("\n  Fix: pip install pyspark")
    sys.exit(1)


# ===========================================================================
# 2. SparkSession in local mode
# ===========================================================================
section("2. Creating SparkSession in local mode")
spark = None
try:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("VerifyLakehouseSetup")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    check("SparkSession created", spark is not None, f"master={spark.sparkContext.master}")
    check("local mode active", "local" in spark.sparkContext.master, spark.sparkContext.master)
except Exception as e:
    check("SparkSession created", False, str(e))


# ===========================================================================
# 3. Basic DataFrame operations
# ===========================================================================
section("3. Testing DataFrame operations")
df = None
if spark:
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

        schema = StructType([
            StructField("sku_id",       StringType(),  nullable=False),
            StructField("warehouse_id", StringType(),  nullable=False),
            StructField("quantity",     IntegerType(), nullable=True),
            StructField("unit_cost",    DoubleType(),  nullable=True),
        ])

        data = [
            ("SKU-001", "WH-01", 100, 12.50),
            ("SKU-002", "WH-01", 250,  8.99),
            ("SKU-003", "WH-02",  75, 45.00),
            ("SKU-001", "WH-02", 300, 12.50),
            ("SKU-004", "WH-03",   0, 99.99),
        ]

        df = spark.createDataFrame(data, schema=schema)

        # Aggregation
        agg = df.groupBy("sku_id").agg(
            F.sum("quantity").alias("total_qty"),
            F.sum(F.col("quantity") * F.col("unit_cost")).alias("total_value"),
        )
        rows = agg.count()
        check("DataFrame aggregation", rows == 4, f"{rows} SKU groups")

        # Window function
        from pyspark.sql.window import Window
        w = Window.partitionBy("warehouse_id").orderBy(F.desc("quantity"))
        ranked = df.withColumn("rank_in_wh", F.rank().over(w))
        check("Window function (rank)", ranked.count() == 5)

        # Spark SQL
        df.createOrReplaceTempView("inventory")
        sql_df = spark.sql(
            "SELECT warehouse_id, COUNT(*) AS sku_count "
            "FROM inventory GROUP BY warehouse_id"
        )
        check("Spark SQL query", sql_df.count() == 3, f"{sql_df.count()} warehouses")

    except Exception as e:
        check("DataFrame operations", False, str(e))


# ===========================================================================
# 4. Parquet read/write round-trip
# ===========================================================================
section("4. Testing Parquet read/write")
if spark and df is not None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmp:
            parquet_path = os.path.join(tmp, "test_sc_data.parquet")
            pandas_df = df.toPandas()
            table = pa.Table.from_pandas(pandas_df)
            pq.write_table(table, parquet_path, compression="snappy")
            check("Parquet write (PyArrow)", True, f"{len(pandas_df)} rows written")

            df_back = spark.read.parquet(parquet_path)
            rc = df_back.count()
            check("Parquet read-back via Spark", rc == 5, f"{rc} rows")

    except Exception as e:
        check("Parquet round-trip", False, str(e))
else:
    print(f"  {INFO} Skipped (SparkSession not available)")


# ===========================================================================
# 5. Lakehouse Service Connectivity
# ===========================================================================
section("5. Checking Lakehouse Services")


def check_port(host: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.5)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


services = [
    ("Kafka Broker", "localhost", 29092),
    ("LocalStack S3", "localhost", 4566),
    ("Project Nessie Catalog", "localhost", 19120),
    ("Trino SQL Engine", "localhost", 8080),
]

for svc_name, host, port in services:
    is_up = check_port(host, port)
    if is_up:
        print(f"  {PASS} {svc_name} reachable at {host}:{port}")
    else:
        print(f"  {WARN} {svc_name} offline at {host}:{port} (Start via: docker-compose up -d)")


# ===========================================================================
# Result
# ===========================================================================
print(f"\n{SEP}")
if all_passed:
    print("  Lakehouse local environment verification PASSED!")
else:
    print("  Some checks FAILED -- review errors above.")
print(f"{SEP}\n")

if spark:
    spark.stop()

sys.exit(0 if all_passed else 1)
