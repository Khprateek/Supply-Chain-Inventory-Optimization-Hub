import os
import time
import json
import threading
from fastapi import APIRouter
from google.cloud import bigquery
import trino
from confluent_kafka import Consumer, TopicPartition

router = APIRouter()

# Paths & GCP Configuration
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sa_key = os.path.join(PROJECT_ROOT, "credentials", "sa_dbt.json")
if os.path.exists(sa_key) and "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
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

# Cached BigQuery client singleton
_bq_client_lock = threading.Lock()
_bq_client = None

def get_bq_client():
    global _bq_client
    with _bq_client_lock:
        if _bq_client is None:
            try:
                _bq_client = bigquery.Client(project=GCP_PROJECT)
            except Exception:
                return None
        return _bq_client

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
        client = get_bq_client()
        if client:
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
                for k in result["bq"]:
                    if result["bq"][k] is None:
                        result["bq"][k] = 0
    except Exception as e:
        result["bq"] = {"error": str(e)}

    # 2. Iceberg (Trino) with explicit connection lifecycle
    conn = None
    cursor = None
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG, schema=TRINO_SCHEMA_MART,
            http_scheme='http'
        )
        cursor = conn.cursor()
        query = f"""
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_units_sold) as total_units_sold,
            SUM(transaction_count) as total_transactions
        FROM {TRINO_TABLE_MART}
        """
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
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass
        
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
        client = get_bq_client()
        if client:
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
        pass

    # Iceberg Raw Rows (Query BOTH sales and inventory raw event tables for a balanced comparison)
    conn = None
    cursor = None
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG,
            http_scheme='http'
        )
        cursor = conn.cursor()
        total_ice_rows = 0
        for table_path in ["sales.raw_events", "inventory.raw_events"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_path}")
                r = cursor.fetchone()
                if r and r[0] is not None:
                    total_ice_rows += int(r[0])
            except Exception:
                pass

        result["iceberg"]["total_rows"] = total_ice_rows
        
        with _speed_metrics_lock:
            current_time = time.time()
            dt = current_time - speed_metrics["iceberg_timestamp"]
            if dt > 0 and speed_metrics["iceberg_timestamp"] > 0:
                result["iceberg"]["rows_per_sec"] = max(0, int((total_ice_rows - speed_metrics["iceberg_rows"]) / dt))
                
            speed_metrics["iceberg_rows"] = total_ice_rows
            speed_metrics["iceberg_timestamp"] = current_time
    except Exception as e:
        pass
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

    return result


kafka_metrics_state = {
    "topics": {},
    "timestamp": 0
}

@router.get("/metrics/kafka")
def get_kafka_metrics():
    global kafka_metrics_state
    
    current_time = time.time()
    dt = current_time - kafka_metrics_state["timestamp"]
    
    result = {"topics": {}, "global_events_sec": 0, "healthy": True}
    consumer = None
    
    try:
        consumer = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": "dashboard-metric-reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "socket.timeout.ms": 2000
        })
        total_rate = 0
        for topic in ["sales_events", "inventory_events"]:
            md = consumer.list_topics(topic, timeout=1.5)
            if md and topic in md.topics:
                t = md.topics[topic]
                result["topics"][topic] = {"partitions": len(t.partitions), "events_sec": 0, "total_events": 0}
                
                # Retrieve partition high watermarks without reading payload
                partitions = [TopicPartition(topic, p) for p in t.partitions]
                total_high = 0
                for p in partitions:
                    try:
                        low, high = consumer.get_watermark_offsets(p, timeout=0.8)
                        if high is not None and high > 0:
                            total_high += high
                    except Exception:
                        pass
                    
                result["topics"][topic]["total_events"] = total_high
                
                if dt > 0 and topic in kafka_metrics_state["topics"]:
                    prev_total = kafka_metrics_state["topics"][topic]["total_events"]
                    rate = max(0, int((total_high - prev_total) / dt))
                    result["topics"][topic]["events_sec"] = rate
                    total_rate += rate
                    
        result["global_events_sec"] = total_rate
        
        for t, data in result["topics"].items():
            if t not in kafka_metrics_state["topics"]:
                kafka_metrics_state["topics"][t] = {}
            kafka_metrics_state["topics"][t]["total_events"] = data["total_events"]
        kafka_metrics_state["timestamp"] = current_time
    except Exception as e:
        result["healthy"] = False
        result["error"] = str(e)
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        
    return result


@router.get("/metrics/spark")
def get_spark_metrics():
    result = {"bq": None, "iceberg": None}
    
    bq_path = os.path.join(PROJECT_ROOT, "dashboard", "metrics_bq.json")
    if os.path.exists(bq_path):
        try:
            with open(bq_path, "r", encoding="utf-8") as f:
                result["bq"] = json.load(f)
        except Exception:
            pass
            
    ice_path = os.path.join(PROJECT_ROOT, "pyspark_jobs", "metrics_iceberg.json")
    if os.path.exists(ice_path):
        try:
            with open(ice_path, "r", encoding="utf-8") as f:
                result["iceberg"] = json.load(f)
        except Exception:
            pass
            
    return result
