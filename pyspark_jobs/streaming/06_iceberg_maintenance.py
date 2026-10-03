import sys
import os
import time
from pyspark.sql import SparkSession

def main():
    print("🚀 Starting Iceberg Maintenance Job...")
    
    # Setup Spark with Iceberg and Nessie packages
    spark = SparkSession.builder \
        .appName("Iceberg-Maintenance-Job") \
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
    print("✅ Spark Session Created. Iceberg Nessie Catalog Initialized.")

    tables = [
        "nessie.inventory.streaming_events",
        "nessie.sales.streaming_events"
    ]
    
    for table in tables:
        print(f"🔄 Running maintenance for {table}...")
        
        try:
            # 1. Compaction: Rewrite Data Files
            print(f"  -> Rewriting data files (Compacting small files)...")
            spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{table}')")
            
            # 2. Expire old snapshots
            print(f"  -> Expiring old snapshots...")
            # Using retain_last to keep a reasonable amount of history for time travel, while clearing old ones
            spark.sql(f"CALL nessie.system.expire_snapshots(table => '{table}', retain_last => 10)")
            
            # 3. Rewrite manifests
            print(f"  -> Rewriting manifest files...")
            spark.sql(f"CALL nessie.system.rewrite_manifests(table => '{table}')")
            
            print(f"✅ Maintenance complete for {table}.")
        except Exception as e:
            print(f"❌ Error during maintenance for {table}: {e}")
        
    print("🎉 All maintenance operations completed.")
    spark.stop()

if __name__ == "__main__":
    main()
