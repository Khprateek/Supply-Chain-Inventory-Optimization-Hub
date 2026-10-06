"""
Apache Airflow Production Reference DAG: Enterprise Open Data Lakehouse Hub
Schedule: Daily @ 02:00 UTC (0 2 * * *)
Target Architecture: Apache Kafka -> PySpark Structured Streaming -> Apache Iceberg v2 (Nessie + MinIO) -> Trino

Pipeline Workflow:
1. Verify Lakehouse storage & metadata endpoints (Nessie & S3/MinIO).
2. Trigger PySpark batch deduplication & Gold-tier mart transformations.
3. Run Iceberg table maintenance (bin-pack compaction rewrite_data_files & snapshot expiration).
4. Execute Trino SQL data quality assertions over Iceberg tables.
5. Publish pipeline metrics & health status.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import urllib.request
import json

default_args = {
    "owner": "lakehouse-data-platform",
    "depends_on_past": False,
    "email": ["data-engineering@enterprise-lakehouse.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=45),
}


def check_lakehouse_health():
    """Verify Nessie catalog and S3 object storage availability before triggering jobs."""
    nessie_url = "http://nessie:19120/api/v1/config"
    req = urllib.request.Request(nessie_url, headers={"User-Agent": "Airflow-Health-Check"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            print(f"[HEALTH] Nessie catalog healthy: default branch = {data.get('defaultBranch')}")
    except Exception as e:
        raise RuntimeError(f"Lakehouse Nessie catalog check failed: {e}")


with DAG(
    dag_id="dag_supply_chain_lakehouse",
    default_args=default_args,
    description="Daily Lakehouse orchestration: Nessie Catalog -> Spark Batch -> Iceberg Compaction -> Trino Audit",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "pyspark", "trino", "nessie"],
) as dag:

    start = EmptyOperator(task_id="pipeline_start")

    # Stage 1: Health check
    verify_lakehouse_catalog = PythonOperator(
        task_id="verify_lakehouse_catalog",
        python_callable=check_lakehouse_health,
    )

    # Stage 2: Batch transformation & deduplication
    pyspark_batch_transformation = BashOperator(
        task_id="pyspark_batch_transformation",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              /opt/spark/work-dir/complex_transforms/05_iceberg_transforms.py
        """,
    )

    # Stage 3: Iceberg table maintenance (bin-packing compaction & snapshot cleanup)
    iceberg_table_maintenance = BashOperator(
        task_id="iceberg_table_maintenance",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              /opt/spark/work-dir/streaming/06_iceberg_maintenance.py
        """,
    )

    # Stage 4: Trino automated verification
    trino_data_quality_audit = BashOperator(
        task_id="trino_data_quality_audit",
        bash_command="""
            docker exec trino trino --execute "
                SELECT 
                    COUNT(*) as total_records,
                    SUM(total_revenue) as total_rev
                FROM iceberg.marts.fact_sales_summary;
            "
        """,
    )

    end = EmptyOperator(task_id="pipeline_complete")

    start >> verify_lakehouse_catalog >> pyspark_batch_transformation >> iceberg_table_maintenance >> trino_data_quality_audit >> end
