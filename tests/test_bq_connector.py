"""
test_bq_connector.py
Tests the Spark-BigQuery connector jar and write capability.
"""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SA_KEY_PATH = PROJECT_ROOT / "credentials" / "sa_dbt.json"

spark = SparkSession.builder \
    .appName("Test-BQ-Connector") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

data = [("test_id", "Test event")]
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True)
])

df = spark.createDataFrame(data, schema)

print("Attempting to write to BigQuery...")
if not SA_KEY_PATH.exists():
    print(f"SKIPPED: Credentials not found at {SA_KEY_PATH}")
else:
    try:
        df.write \
            .format("bigquery") \
            .option("credentialsFile", str(SA_KEY_PATH)) \
            .option("table", "smart-supply-and-inventory.raw_supply_chain.test_table") \
            .option("temporaryGcsBucket", "dummy-bucket-not-used") \
            .mode("append") \
            .save()
        print("SUCCESS!")
    except Exception as e:
        print(f"FAILED: {e}")

spark.stop()
