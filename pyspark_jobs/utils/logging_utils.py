"""
logging_utils.py
================
Structured logging setup for PySpark jobs in the Open Data Lakehouse.

Sets up Python logging + adjusts Spark/Py4J verbosity so the console stays
readable during development.

Usage:
    from pyspark_jobs.utils.logging_utils import configure_logging, get_logger

    configure_logging(level="INFO")
    logger = get_logger(__name__)
    logger.info("Job started | Lakehouse pipeline initialized")
"""

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """
    Configure root logger and quieten noisy Spark/Py4J/HTTP libraries.

    Args:
        level: Python log level string: "DEBUG" | "INFO" | "WARNING" | "ERROR".
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

    # Quieten verbose libraries so pipeline operational messages remain clear
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Always call after configure_logging()."""
    return logging.getLogger(name)


def log_spark_config(spark) -> None:
    """
    Print the most relevant active Spark configuration values.
    Useful for verifying lakehouse catalog, memory, and S3 settings at job start.
    """
    logger = get_logger("spark_config")
    interesting_keys = [
        "spark.master",
        "spark.app.name",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.extensions",
        "spark.sql.catalog.nessie",
        "spark.sql.catalog.nessie.uri",
        "spark.sql.catalog.nessie.warehouse",
        "spark.sql.catalog.nessie.s3.endpoint",
        "spark.hadoop.fs.s3a.endpoint",
    ]
    logger.info("─── Active Lakehouse Spark Configuration ────────────────────")
    conf = spark.sparkContext.getConf()
    for key in interesting_keys:
        value = conf.get(key, "<not set>")
        logger.info("  %-42s = %s", key, value)
    logger.info("─────────────────────────────────────────────────────────────")
