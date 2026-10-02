import os
import time
import threading
from fastapi import APIRouter
from google.cloud import bigquery
import trino

router = APIRouter()

# Ensure GCP Auth
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sa_key = os.path.join(PROJECT_ROOT, "credentials", "sa_dbt.json")
if os.path.exists(sa_key):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_key

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "smart-supply-and-inventory")
BQ_DATASET = "sc_dev"
BQ_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.fct_stream_sales_summary"

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA_MART = "marts"
TRINO_TABLE_MART = "fact_sales_summary"
TRINO_SCHEMA_RAW = "sales"
TRINO_TABLE_RAW = "streaming_events"

# Keep track of previous counts for speed calculation
speed_metrics = {
    "bq_rows": 0,
    "bq_timestamp": 0,
    "iceberg_rows": 0,
    "iceberg_timestamp": 0
}
_speed_metrics_lock = threading.Lock()

@router.get("/metrics/showdown")
def get_showdown_metrics():
    result = {"bq": {}, "iceberg": {}}
    
    # 1. BigQuery
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        query = f"""
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_units_sold) as total_units_sold,
            SUM(total_transactions) as total_transactions
        FROM `{BQ_TABLE}`
        """
        rows = list(client.query(query))
        if rows:
            result["bq"] = dict(rows[0].items())
            # Replace None with 0
            for k in result["bq"]:
                if result["bq"][k] is None:
                    result["bq"][k] = 0
    except Exception as e:
        result["bq"] = {"error": str(e)}

    # 2. Iceberg (Trino)
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG, schema=TRINO_SCHEMA_MART,
        )
        query = f"""
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_units_sold) as total_units_sold,
            SUM(transaction_count) as total_transactions
        FROM {TRINO_TABLE_MART}
        """
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            result["iceberg"] = {
                "total_revenue": row[0] or 0,
                "total_units_sold": row[1] or 0,
                "total_transactions": row[2] or 0
            }
    except Exception as e:
        result["iceberg"] = {"error": str(e)}
        
    return result

@router.get("/metrics/live")
def get_live_metrics():
    global speed_metrics
    result = {
        "bq": {"total_rows": 0, "rows_per_sec": 0},
        "iceberg": {"total_rows": 0, "rows_per_sec": 0}
    }
    
    # BigQuery Raw Rows (Metadata fast query)
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        query = f"""
        SELECT sum(row_count) as total_rows 
        FROM `{GCP_PROJECT}.raw_supply_chain.__TABLES__`
        WHERE table_id IN ('raw_sales_events', 'raw_inventory_events')
        """
        rows = list(client.query(query))
        if rows and rows[0].total_rows is not None:
            current_bq_rows = int(rows[0].total_rows)
            result["bq"]["total_rows"] = current_bq_rows
            
            with _speed_metrics_lock:
                current_time = time.time()
                dt = current_time - speed_metrics["bq_timestamp"]
                if dt > 0 and speed_metrics["bq_timestamp"] > 0:
                    result["bq"]["rows_per_sec"] = max(0, int((current_bq_rows - speed_metrics["bq_rows"]) / dt))
                    
                speed_metrics["bq_rows"] = current_bq_rows
                speed_metrics["bq_timestamp"] = current_time
    except Exception as e:
        print("BQ Error:", e)

    # Iceberg Raw Rows (Trino COUNT(*) is metadata optimized)
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG, schema=TRINO_SCHEMA_RAW,
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_TABLE_RAW}")
        row = cursor.fetchone()
        if row:
            current_ice_rows = int(row[0])
            result["iceberg"]["total_rows"] = current_ice_rows
            
            with _speed_metrics_lock:
                current_time = time.time()
                dt = current_time - speed_metrics["iceberg_timestamp"]
                if dt > 0 and speed_metrics["iceberg_timestamp"] > 0:
                    result["iceberg"]["rows_per_sec"] = max(0, int((current_ice_rows - speed_metrics["iceberg_rows"]) / dt))
                    
                speed_metrics["iceberg_rows"] = current_ice_rows
                speed_metrics["iceberg_timestamp"] = current_time
    except Exception as e:
        print("Trino Error:", e)

    return result
