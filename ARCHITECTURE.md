# Enterprise Supply Chain & Inventory Optimization Hub
## Dual-Architecture Streaming Pipeline

This project implements a high-throughput streaming data pipeline to process supply chain and sales events. To evaluate modern data platform designs, the system is built using a **Dual-Architecture pattern**, running two separate data paradigms side-by-side:

1. **Architecture A (Cloud Data Warehouse):** PySpark + Google BigQuery + dbt
2. **Architecture B (Open Data Lakehouse):** PySpark + Apache Iceberg + Project Nessie + Trino

---

## 🏗️ High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Event Generation
        G[Python Data Generator] -->|Produces JSON Events| K1[(Kafka Topic: sales_events)]
        G -->|Produces JSON Events| K2[(Kafka Topic: inventory_events)]
    end

    subgraph Architecture A: Cloud Data Warehouse
        S1[PySpark Stream\n(foreachBatch + BQ Python Client)]
        K1 --> S1
        K2 --> S1
        S1 -->|Inserts| BQ_RAW[(BigQuery:\nraw_supply_chain)]
        DBT[dbt Transformations] -->|Selects & Aggregates| BQ_RAW
        DBT -->|Materializes| BQ_MART[(BigQuery:\nsc_dev.fct_stream_sales_summary)]
    end

    subgraph Architecture B: Open Data Lakehouse
        S2[PySpark Stream\n(Iceberg DataFrame v2 API)]
        K1 --> S2
        K2 --> S2
        S2 -->|Appends| ICE_RAW[(MinIO/S3 Object Store:\niceberg.sales.streaming_events)]
        NESSIE[Project Nessie\n(Iceberg Catalog)] -.->|Tracks Metadata| ICE_RAW
        TRINO[Trino Query Engine] -->|Reads & Aggregates| ICE_RAW
    end

    subgraph Reconciliation
        COMPARE[Reconciliation Script\n(compare_architectures.py)]
        COMPARE -->|Queries| BQ_MART
        COMPARE -->|Queries| TRINO
    end
```

---

## ⚙️ Core Components

### 1. Data Generation & Ingestion
- **Event Generator** (`scripts/kafka_data_generator.py`): Simulates continuous, high-volume transactions and inventory movements, publishing to Kafka topics.
- **Message Broker** (`Docker: Kafka + Zookeeper`): Buffers incoming events, allowing independent downstream consumers to process data at their own pace.

### 2. Architecture A: BigQuery & dbt
This architecture represents the modern cloud ELT standard.
- **Ingestion** (`pyspark_jobs/streaming/05_bigquery_stream.py`): A PySpark structured streaming job reads from Kafka. Due to known JVM shading bugs with the official Spark BigQuery connector on Java 11/17, this job uses `foreachBatch` to convert micro-batches to Pandas DataFrames and loads them into BigQuery using the native Python `google-cloud-bigquery` client.
- **Transformation** (`dbt/`): dbt models query the raw tables, apply business logic, and aggregate the data into a persistent `fct_stream_sales_summary` table. 

### 3. Architecture B: Iceberg, Nessie, & Trino
This architecture represents the Open Data Lakehouse paradigm, decoupling storage, cataloging, and compute.
- **Storage** (`Docker: Localstack S3`): All data is stored locally in S3-compatible storage in the open Apache Parquet format.
- **Catalog** (`Docker: Project Nessie`): Acts as the catalog for Apache Iceberg, enabling Git-like branching, tagging, and atomic commits for the data lake.
- **Ingestion** (`pyspark_jobs/streaming/04_iceberg_stream.py`): A PySpark structured streaming job runs inside a Docker Spark cluster. It reads from Kafka and uses the Iceberg Spark extensions to perform transactional streaming appends directly into S3. 
- **Compute / Transformation**: Originally intended to be a PySpark batch job, JVM HotSpot compiler bugs (`signature.cpp:53` segmentation faults on Java 11) required a shift in architecture. We lean completely into the Lakehouse paradigm by using **Trino** (`Docker: Trino`) to dynamically query and aggregate the raw Iceberg tables on-the-fly at read time, completely sidestepping JVM compute limitations.

### 4. Reconciliation
- **Comparison Engine** (`scripts/compare_architectures.py`): A Python script that simultaneously connects to Google BigQuery and Trino, executes aggregation queries across both architectures, and renders a side-by-side terminal showdown of the metrics (Total Revenue, Units Sold, Transactions).

---

## 🛠️ Tech Stack & Infrastructure

- **Compute & Orchestration:** Docker Compose, PySpark 3.5
- **Message Broker:** Apache Kafka 3.4
- **Data Warehouse:** Google BigQuery
- **Data Transformation:** dbt-core 1.8.0, dbt-bigquery
- **Data Lakehouse:** Apache Iceberg 1.5.0
- **Data Catalog:** Project Nessie 0.77.1
- **Object Storage:** LocalStack (S3)
- **Query Engine:** Trino
- **Languages:** Python 3.12, SQL

---

## 🚀 How to Run

1. **Start the Infrastructure**
   ```powershell
   docker-compose up -d
   ```
2. **Start the Data Generator**
   ```powershell
   python scripts/kafka_data_generator.py
   ```
3. **Run Architecture A (BigQuery)**
   ```powershell
   python pyspark_jobs/streaming/05_bigquery_stream.py
   .\dbt.cmd run --project-dir dbt --select streaming
   ```
4. **Run Architecture B (Iceberg)**
   ```powershell
   .\run_iceberg_stream.cmd
   ```
5. **Run the Showdown**
   ```powershell
   python scripts/compare_architectures.py
   ```
