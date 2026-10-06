# Enterprise Open Data Lakehouse Platform
## Comprehensive Architecture Specification

This document provides an in-depth technical specification of the **Enterprise Open Data Lakehouse Platform**. The system is built entirely on open-source, vendor-neutral technologies—decoupling compute, catalog, and storage to process high-throughput streaming supply chain telemetry and serve sub-second analytical queries.

---

## 🏗️ 1. High-Level System Architecture

```mermaid
flowchart TD
    subgraph 1. Event Generation & Streaming Ingestion
        G[Python Telemetry Generator\n(Multiprocessing Event Engine)] -->|JSON Telemetry| K1[(Kafka: sales_events\n16 Partitions)]
        G -->|JSON Telemetry| K2[(Kafka: inventory_events\n16 Partitions)]
        ZK[Zookeeper] -.->|Coordination| K1
        ZK -.->|Coordination| K2
    end

    subgraph 2. Distributed Stream Processing
        SPARK_STREAM[PySpark Structured Streaming\n5s Micro-Batch · S3 WAL Checkpoints]
        K1 --> SPARK_STREAM
        K2 --> SPARK_STREAM
    end

    subgraph 3. Open Lakehouse Storage & Catalog Tier
        SPARK_STREAM -->|S3FileIO / Parquet| ICE_RAW[(MinIO / S3 Object Storage\niceberg.sales / iceberg.inventory)]
        NESSIE[Project Nessie Catalog\nGit-like Transactional Branching\nBranch: main] -.->|Metadata & Snapshot Commits| ICE_RAW
    end

    subgraph 4. Continuous Transformation & Table Maintenance
        SPARK_TRANS[PySpark Transformations\nDeduplication · MERGE INTO] -->|Calculates| ICE_GOLD[(Iceberg Gold Marts\nmarts.fact_sales_summary)]
        SPARK_MAINT[Iceberg Table Maintenance\nBin-Pack Compaction · Snapshot Expiration] -->|Optimizes| ICE_RAW
        SPARK_MAINT -->|Optimizes| ICE_GOLD
    end

    subgraph 5. Analytical Serving & Observability
        TRINO[Trino Distributed SQL Engine\nMetadata Zero-Copy Optimization] -->|Federated Queries| ICE_GOLD
        TRINO -->|Ad-Hoc Inspection| ICE_RAW
        FASTAPI[FastAPI Control Tower\nProcess Management & Telemetry API] -->|Subprocess Control| G
        FASTAPI -->|Docker Exec| SPARK_STREAM
        FASTAPI -->|Docker Exec| SPARK_TRANS
        UI[Interactive Web Control Tower\nTailwind CSS · Real-Time Websockets/Polling] -->|REST / Status| FASTAPI
    end
```

---

## 📡 2. Ingestion Layer: Event Engine & Apache Kafka

The ingestion layer simulates a global omnichannel retail and supply chain network, emitting concurrent sales orders and warehouse inventory stock changes.

### Kafka Topic Configurations
* **`sales_events`** (16 Partitions):
  * **Payload Schema**: `event_id` (UUID), `timestamp` (Unix epoch), `product_id` (SKU), `customer_id` (String), `revenue` (Float), `units_sold` (Integer).
  * **Ingestion Characteristics**: High burst capability, sub-millisecond producer latency, LZ4 compression.
* **`inventory_events`** (16 Partitions):
  * **Payload Schema**: `event_id` (UUID), `timestamp` (Unix epoch), `product_id` (SKU), `warehouse_id` (Facility Code), `quantity_change` (Signed Integer), `event_type` (`RECEIPT`, `PICK`, `ADJUSTMENT`).
  * **Ingestion Characteristics**: Partitioned to preserve inventory order per facility and SKU.

---

## ⚡ 3. Real-Time Stream Processing: PySpark Structured Streaming

The stream processing engine (`04_iceberg_stream.py`) runs PySpark 3.5 on Scala 2.12 within containerized Spark clusters.

### Core Streaming Guarantees:
1. **Dynamic Backpressure**: Configured with `spark.streaming.kafka.maxRatePerPartition` and `maxOffsetsPerTrigger=50000` to prevent memory thrashing and executor OOMs during traffic spikes.
2. **Exactly-Once Semantics (EOS)**: Achieved via S3 write-ahead log (WAL) checkpointing combined with Apache Iceberg's transactional atomic table commits.
3. **Trigger Interval**: Configured at a 5-second micro-batch trigger cadence, balancing latency against Parquet file size creation.

---

## 🧊 4. Storage & Catalog Tier: Apache Iceberg v2 & Project Nessie

The lakehouse tier replaces proprietary cloud data warehouses with an open, portable storage architecture.

### Decoupled Components:
* **Table Format**: **Apache Iceberg v2** format with native row-level deletion support, hidden partitioning, and schema evolution.
* **Metadata & Catalog**: **Project Nessie** acts as a transactional Git-like catalog. Every streaming commit is recorded as an atomic snapshot on the `main` branch, enabling zero-copy branch creation, tag isolation, and instant rollbacks.
* **Storage Protocol**: Direct **S3FileIO** (`org.apache.iceberg.aws.s3.S3FileIO`) over MinIO / LocalStack S3, bypassing S3A Hadoop listing bottlenecks and achieving cloud-native multi-part upload throughput.
* **Compression**: Snappy/ZSTD-compressed columnar Parquet files with built-in column min/max statistics for partition and data file pruning.

---

## 🔄 5. Processing & Serving Tier: Deduplication & Gold Marts

### Transformation Workflow (`06_iceberg_transform.py` / `05_iceberg_transforms.py`)
1. **Windowed Deduplication**: Resolves duplicate streaming telemetry via window functions:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp DESC)
   ```
2. **Idempotent Upsert (MERGE INTO)**: Merges raw micro-batches into dimensionally aggregated Gold-tier tables (`nessie.marts.fact_sales_summary`) partitioned by date.
3. **Serving Engine**: **Trino 440+** directly queries Iceberg metadata without interacting with Spark, serving sub-second interactive BI queries and analytics dashboards.

---

## 🧹 6. Lakehouse Table Maintenance & Compaction

Streaming ingestion frequently results in small-file fragmentation ("small-files problem"). To guarantee sustained analytical query performance, an automated maintenance worker (`06_iceberg_maintenance.py`) performs:

1. **Bin-Packing Compaction**: Executes Iceberg's `rewrite_data_files` procedure to merge small Parquet files into optimal 128 MB blocks.
2. **Snapshot Expiration**: Automatically expires commits older than 10 snapshots using `expire_snapshots`, pruning unreferenced physical files from S3 and reclaiming storage.
3. **Manifest Rewriting**: Optimizes Iceberg manifest trees to reduce metadata listing overhead during Trino query planning.

---

## 🖥️ 7. Control Tower & Observability Dashboard

The operations layer (`dashboard/`) provides unified pipeline observability:
* **FastAPI Backend**: Manages background worker processes, collects health status from Kafka, Spark, Nessie, and Trino, and exposes telemetry APIs.
* **Interactive UI**: Real-time visualization with live Kafka broker lag/watermarks, Iceberg ingestion rates, cumulative revenues, and one-click operational controls.
