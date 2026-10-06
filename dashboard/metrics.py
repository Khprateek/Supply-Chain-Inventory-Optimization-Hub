import os
import time
import json
import threading
from fastapi import APIRouter
import trino
from confluent_kafka import Consumer, TopicPartition

router = APIRouter()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", 8080))
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA_MART = "marts"
TRINO_TABLE_MART = "fact_sales_summary"

# Keep track of previous counts for ingestion speed calculation
speed_metrics = {
    "iceberg_rows": 0,
    "iceberg_timestamp": 0
}
_speed_metrics_lock = threading.Lock()


@router.get("/metrics/showdown")
@router.get("/metrics/marts")
def get_mart_metrics():
    """Retrieve Gold-tier analytical summary metrics from Trino over Apache Iceberg."""
    result = {
        "iceberg": {
            "total_revenue": 0,
            "total_units_sold": 0,
            "total_transactions": 0,
        }
    }

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
                "total_revenue": round(float(row[0] or 0), 2),
                "total_units_sold": int(row[1] or 0),
                "total_transactions": int(row[2] or 0)
            }
    except Exception as e:
        result["iceberg"]["error"] = str(e)
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
    """Retrieve live row counts and real-time ingestion rates for Iceberg tables."""
    global speed_metrics
    result = {
        "iceberg": {
            "total_rows": 0,
            "rows_per_sec": 0,
            "sales_rows": 0,
            "inventory_rows": 0
        }
    }

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

        # Query sales raw events
        try:
            cursor.execute("SELECT COUNT(*) FROM sales.raw_events")
            r = cursor.fetchone()
            if r and r[0] is not None:
                sales_count = int(r[0])
                result["iceberg"]["sales_rows"] = sales_count
                total_ice_rows += sales_count
        except Exception:
            pass

        # Query inventory raw events
        try:
            cursor.execute("SELECT COUNT(*) FROM inventory.raw_events")
            r = cursor.fetchone()
            if r and r[0] is not None:
                inv_count = int(r[0])
                result["iceberg"]["inventory_rows"] = inv_count
                total_ice_rows += inv_count
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
        result["iceberg"]["error"] = str(e)
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
    """Retrieve real-time Kafka partition offsets, message counts, and production rates."""
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
    """Retrieve Spark Structured Streaming micro-batch progress metrics."""
    result = {"iceberg": None}

    ice_path = os.path.join(PROJECT_ROOT, "pyspark_jobs", "metrics_iceberg.json")
    if os.path.exists(ice_path):
        try:
            with open(ice_path, "r", encoding="utf-8") as f:
                result["iceberg"] = json.load(f)
        except Exception:
            pass

    return result
