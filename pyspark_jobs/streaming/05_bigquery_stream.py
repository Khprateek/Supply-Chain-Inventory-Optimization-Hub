import os
import sys
import platform
import json
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
SA_KEY_PATH = PROJECT_ROOT / "credentials" / "sa_dbt.json"
CHECKPOINT_BASE = str(PROJECT_ROOT / "pyspark_jobs" / "streaming" / "checkpoints")
KAFKA_BROKER = "localhost:29092"

if os.name == 'nt':
    import ctypes
    def _short(p):
        buf = ctypes.create_unicode_buffer(256)
        ctypes.windll.kernel32.GetShortPathNameW(p, buf, 256)
        return buf.value or p
    
    if not os.environ.get("HADOOP_HOME"):
        wdir = PROJECT_ROOT / "conf" / "spark" / "winutils" / "hadoop-3.3.6"
        short = _short(str(wdir))
        os.environ["HADOOP_HOME"] = short
        os.environ["PATH"] = short + os.sep + "bin" + os.pathsep + os.environ.get("PATH", "")

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "smart-supply-and-inventory")
if SA_KEY_PATH.exists():
    try:
        with open(SA_KEY_PATH, "r", encoding="utf-8") as f:
            GCP_PROJECT = json.load(f).get("project_id", GCP_PROJECT)
    except Exception:
        pass
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_KEY_PATH)
BQ_DATASET = "raw_supply_chain"


from pyspark.sql.streaming import StreamingQueryListener
import json

class BQMetricsListener(StreamingQueryListener):
    def __init__(self):
        self.queries = {}
        self.metrics_path = PROJECT_ROOT / "dashboard" / "metrics_bq.json"
        
    def onQueryStarted(self, event):
        self.queries[str(event.id)] = {"name": event.name or str(event.id)}

    def onQueryProgress(self, event):
        try:
            qid = str(event.progress.id)
            qname = event.progress.name or qid
            self.queries[qname] = {
                "id": qid,
                "name": qname,
                "inputRowsPerSecond": event.progress.inputRowsPerSecond,
                "processedRowsPerSecond": event.progress.processedRowsPerSecond,
                "batchDuration": getattr(event.progress, "batchDurationMs", 0) or 0,
                "batchId": event.progress.batchId
            }
            
            total_in = sum(v.get("inputRowsPerSecond", 0) for v in self.queries.values() if isinstance(v, dict))
            total_proc = sum(v.get("processedRowsPerSecond", 0) for v in self.queries.values() if isinstance(v, dict))
            
            output = dict(self.queries)
            output["inputRate"] = total_in
            output["procRate"] = total_proc
            
            temp_path = str(self.metrics_path) + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(output, f)
            os.replace(temp_path, str(self.metrics_path))
        except Exception:
            pass

    def onQueryTerminated(self, event):
        try:
            qid = str(event.id)
            for k in list(self.queries.keys()):
                if self.queries[k].get("id") == qid or k == qid:
                    del self.queries[k]
        except Exception:
            pass

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("KafkaToBigQueryStream")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.streams.addListener(BQMetricsListener())
    return spark


def stream_topic_to_bq(spark: SparkSession, topic: str, bq_table: str, schema: StructType):
    print(f"[ARCH-A] Subscribing to '{topic}' -> BQ table '{GCP_PROJECT}.{BQ_DATASET}.{bq_table}'")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    parsed_df = raw.withColumn("data", from_json(col("value").cast("string"), schema)) \
        .select(
            "data.*",
            col("timestamp").alias("kafka_timestamp")
        )
    
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        def process_partition(iterator):
            from google.cloud import bigquery as bq
            client = (
                bq.Client.from_service_account_json(str(SA_KEY_PATH), project=GCP_PROJECT)
                if SA_KEY_PATH.exists()
                else bq.Client(project=GCP_PROJECT)
            )
            table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{bq_table}"
            
            records = []
            for row in iterator:
                row_dict = row.asDict()
                # Cast datetime columns to string for JSON serialization
                for k, v in row_dict.items():
                    if hasattr(v, 'isoformat'):
                        row_dict[k] = str(v)
                records.append(row_dict)
                
            if records:
                try:
                    job_config = bq.LoadJobConfig(
                        write_disposition=bq.WriteDisposition.WRITE_APPEND,
                        autodetect=True
                    )
                    job = client.load_table_from_json(records, table_ref, job_config=job_config)
                    job.result() # Wait for completion
                    print(f"[ARCH-A] Worker loaded {len(records)} rows to {bq_table}")
                except Exception as e:
                    print(f"[ARCH-A][ERROR] Worker load failed: {e}")

        try:
            # Distributed write avoids Driver OOM from pulling 2.7M rows via toPandas
            batch_df.rdd.foreachPartition(process_partition)
        except Exception as exc:
            print(f"[ARCH-A][ERROR] batch {batch_id} crashed: {exc}")

    chk = f"{CHECKPOINT_BASE}/native_{bq_table}"
    
    return (
        parsed_df.writeStream
        .queryName(f"bq_{topic}")
        .foreachBatch(write_batch)
        .option("checkpointLocation", chk)
        .trigger(processingTime="1 minute") # 1 minute trigger = 1440 jobs/day (Under the 1500 limit!)
        .start()
    )

if __name__ == "__main__":
    inventory_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("product_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("quantity_change", LongType(), True),
        StructField("event_type", StringType(), True)
    ])

    sales_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity_sold", LongType(), True),
        StructField("revenue", DoubleType(), True)
    ])

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    inv_q = stream_topic_to_bq(spark, "inventory_events", "raw_inventory_events", inventory_schema)
    sales_q = stream_topic_to_bq(spark, "sales_events", "raw_sales_events", sales_schema)

    print("\n[ARCH-A] Both streams running (1 min micro-batches). Press Ctrl+C to stop.\n")
    spark.streams.awaitAnyTermination()
