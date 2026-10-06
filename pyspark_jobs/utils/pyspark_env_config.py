"""
pyspark_env_config.py
=====================
Centralised environment configuration for the Open Data Lakehouse platform.
Reads environment variables (or falls back to defaults) for Kafka, Spark,
Iceberg, Nessie, MinIO/S3, and Trino.

Usage:
    from pyspark_jobs.utils.pyspark_env_config import LakehouseEnvConfig, get_lakehouse_config

    config = get_lakehouse_config()
    print(config.nessie_uri)
    print(config.s3_endpoint)
"""

import os
from dataclasses import dataclass, field


@dataclass
class LakehouseEnvConfig:
    """Configuration settings for the Enterprise Open Data Lakehouse."""

    # ── Execution Environment ─────────────────────────────────────────────────
    env: str = "local"
    """Spark execution environment: 'local' (host) | 'cluster' (Docker Spark)"""

    # ── Kafka Streaming Broker ────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:29092"
    """Kafka bootstrap servers for streaming producer and consumer."""
    kafka_sales_topic: str = "sales_events"
    kafka_inventory_topic: str = "inventory_events"

    # ── Catalog & Metadata (Project Nessie) ───────────────────────────────────
    nessie_uri: str = "http://localhost:19120/api/v1"
    """Nessie REST API endpoint for Iceberg catalog metadata."""
    nessie_ref: str = "main"
    """Nessie branch reference (e.g., 'main')."""

    # ── Storage Layer (S3 / MinIO / LocalStack) ───────────────────────────────
    s3_endpoint: str = "http://localhost:4566"
    """S3-compatible object storage endpoint."""
    s3_warehouse: str = "s3://warehouse"
    """Root warehouse directory for Iceberg table Parquet files."""
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    aws_region: str = "us-east-1"

    # ── Trino Distributed SQL Engine ──────────────────────────────────────────
    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "admin"
    trino_catalog: str = "iceberg"

    # ── PySpark Tuning ────────────────────────────────────────────────────────
    spark_driver_memory: str = "4g"
    spark_executor_memory: str = "4g"
    spark_shuffle_partitions: int = 8
    extra_spark_conf: dict = field(default_factory=dict)


def get_lakehouse_config() -> LakehouseEnvConfig:
    """
    Build a LakehouseEnvConfig from environment variables.
    Precedence: environment variable → default value.
    """
    return LakehouseEnvConfig(
        env=os.getenv("SPARK_ENV", "local"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_sales_topic=os.getenv("KAFKA_SALES_TOPIC", "sales_events"),
        kafka_inventory_topic=os.getenv("KAFKA_INVENTORY_TOPIC", "inventory_events"),
        nessie_uri=os.getenv("NESSIE_URI", "http://localhost:19120/api/v1"),
        nessie_ref=os.getenv("NESSIE_REF", "main"),
        s3_endpoint=os.getenv("S3_ENDPOINT", "http://localhost:4566"),
        s3_warehouse=os.getenv("S3_WAREHOUSE", "s3://warehouse"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        trino_host=os.getenv("TRINO_HOST", "localhost"),
        trino_port=int(os.getenv("TRINO_PORT", "8080")),
        trino_user=os.getenv("TRINO_USER", "admin"),
        trino_catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        spark_executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        spark_shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")),
    )


# Backward-compatibility alias
PySparkEnvConfig = LakehouseEnvConfig
get_pyspark_config = get_lakehouse_config
