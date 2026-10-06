"""
spark_session.py
================
Centralised SparkSession factory for the Enterprise Open Data Lakehouse.
Configures Apache Iceberg, Project Nessie catalog, and S3A / S3FileIO storage.

Usage:
    from pyspark_jobs.utils.spark_session import get_spark_session

    spark = get_spark_session(env="local")   # local development
    spark = get_spark_session(env="cluster") # Docker Spark cluster
"""

import os
import platform
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf

logger = logging.getLogger(__name__)


def get_spark_session(
    env: str = "local",
    app_name: str = "EnterpriseLakehouseHub",
    extra_conf: dict | None = None,
) -> SparkSession:
    """
    Build and return a configured SparkSession for Apache Iceberg and Nessie.

    Args:
        env:        Execution environment: "local" (host machine) or "cluster" (containerized).
        app_name:   Application name shown in Spark UI.
        extra_conf: Additional Spark config key-value pairs.

    Returns:
        A fully-configured SparkSession ready for Iceberg ACID table operations.
    """
    conf = SparkConf()
    conf.setAppName(app_name)

    # ── Host / Network Endpoint Resolution ─────────────────────────────────────
    # If running inside Docker, services resolve to 'nessie' and 's3'.
    # If running on the host machine, services resolve to 'localhost'.
    is_docker = env == "cluster" or os.path.exists("/.dockerenv")
    default_nessie_host = "nessie" if is_docker else "localhost"
    default_s3_host = "s3" if is_docker else "localhost"

    nessie_uri = os.getenv("NESSIE_URI", f"http://{default_nessie_host}:19120/api/v1")
    s3_endpoint = os.getenv("S3_ENDPOINT", f"http://{default_s3_host}:4566")
    s3_warehouse = os.getenv("S3_WAREHOUSE", "s3://warehouse")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    # ── Master & Resource Settings ─────────────────────────────────────────────
    if env == "local":
        conf.setMaster("local[*]")
        conf.set("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        conf.set("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
        conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Auto-detect winutils HADOOP_HOME on Windows
        if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
            project_root = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
            winutils_dir = os.path.join(project_root, "conf", "spark", "winutils", "hadoop-3.3.6")
            if os.path.isdir(winutils_dir):
                os.environ["HADOOP_HOME"] = winutils_dir
                logger.info("Auto-set HADOOP_HOME = %s", winutils_dir)

    elif env == "cluster":
        conf.setMaster(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        conf.set("spark.driver.memory", "2g")
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        conf.set("spark.sql.adaptive.enabled", "true")

    # ── Apache Iceberg & Nessie Catalog Configurations ─────────────────────────
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    conf.set("spark.sql.catalog.nessie.uri", nessie_uri)
    conf.set("spark.sql.catalog.nessie.ref", os.getenv("NESSIE_REF", "main"))
    conf.set("spark.sql.catalog.nessie.warehouse", s3_warehouse)
    conf.set("spark.sql.catalog.nessie.s3.endpoint", s3_endpoint)
    conf.set("spark.sql.catalog.nessie.client.region", "us-east-1")
    conf.set("spark.sql.catalog.nessie.s3.region", "us-east-1")
    conf.set("spark.sql.catalog.nessie.s3.access-key-id", aws_access_key)
    conf.set("spark.sql.catalog.nessie.s3.secret-access-key", aws_secret_key)
    conf.set("spark.sql.catalog.nessie.s3.path-style-access", "true")
    conf.set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    # S3A FileSystem for standard Spark S3 access
    conf.set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # ── User-supplied Overrides ───────────────────────────────────────────────
    if extra_conf:
        for key, value in extra_conf.items():
            conf.set(key, value)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger.info("SparkSession created for Lakehouse | env=%s | appName=%s", env, app_name)
    return spark
