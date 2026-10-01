# Enterprise Supply Chain & Inventory Optimization Hub
## The Dual-Architecture Streaming Showdown

<div align="center">
  
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-Streaming-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![BigQuery](https://img.shields.io/badge/Google_BigQuery-Cloud_DW-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Iceberg](https://img.shields.io/badge/Apache_Iceberg-Lakehouse-008282?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-SQL_Engine-DD00A1?logo=trino&logoColor=white)](https://trino.io/)
[![dbt](https://img.shields.io/badge/dbt_Core-1.8.0-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Dashboard-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)

**A high-throughput, real-time supply chain data platform comparing a traditional Cloud Data Warehouse against a modern Open Data Lakehouse.**
</div>

---

## 🚀 Project Overview

This project simulates a massive global supply chain network generating thousands of events per second. Instead of building just one data platform, we built **two** side-by-side to evaluate their performance, complexity, and capabilities in real-time.

* **Architecture A (Cloud Data Warehouse):** PySpark + Google BigQuery + dbt
* **Architecture B (Open Data Lakehouse):** PySpark + Apache Iceberg + Project Nessie + Trino

We also built a **Real-Time Interactive Dashboard** using FastAPI and Vanilla JS to control the entire pipeline, visualize the architecture, and monitor the showdown metrics live!

---

## 🏗️ The Architecture

Both pipelines ingest the exact same Kafka topics simultaneously.

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
```

---

## 🎮 How to Run the Project & Dashboard

Everything is containerized and orchestrated through Python and Docker.

### 1. Start the Infrastructure (Docker)
Start the Kafka broker, Spark Cluster, Nessie Catalog, MinIO S3, and Trino query engine.
```powershell
docker-compose up -d
```

### 2. Launch the Control Dashboard
Start the FastAPI backend which serves our interactive UI.
```powershell
# Activate your virtual environment first
.\venv\Scripts\activate

# Start the dashboard server
.\dashboard\run_server.cmd
```
👉 **Open your browser to: http://localhost:8000**

From the dashboard, you can click **START** on the Data Generator and the streaming jobs. The dashboard will automatically track row counts and ingestion speed.

---

## 🛑 How to Stop / Close Docker & Reclaim Memory

When you are done working, it is important to shut down Docker to free up your computer's RAM and CPU. Open your terminal in the project folder and run ONE of the following commands:

1. **Stop the containers safely** *(Recommended - Keeps your data safe for next time)*:
   ```powershell
   docker-compose stop
   ```
2. **Tear down the containers** *(Removes them from Docker Desktop, but keeps volume data intact)*:
   ```powershell
   docker-compose down
   ```
3. **Nuke everything** *(Deletes containers AND wipes all databases/volumes clean. Good for a fresh start)*:
   ```powershell
   docker-compose down -v
   ```

### ⚠️ Memory Still High? (The VmmemWSL Quirks)
If you close Docker and still see `VmmemWSL` consuming a massive amount of memory in your Windows Task Manager, this is expected! Windows Subsystem for Linux (WSL2), which Docker uses in the background, does not immediately release RAM back to Windows after containers stop; it caches it. 

To force Windows to immediately shut down the background Linux VM and release the memory, run this in PowerShell:
```powershell
wsl --shutdown
```
*(The next time you open Docker Desktop, it will safely restart WSL automatically.)*

---

## 🧠 Engineering Highlights & Workarounds

Building this on a local Windows Docker environment required solving several massive engineering hurdles:
- **PySpark BigQuery Connectors:** Official Java connectors suffer from Guice shading bugs on modern JDKs. We bypassed this by utilizing `foreachBatch` to convert Spark micro-batches to Pandas and writing via the native Google Cloud Python client.
- **Iceberg JVM Crashes:** PySpark `writeTo()` and `CREATE TABLE AS SELECT` triggered fatal HotSpot JVM C2 compiler segmentation faults (`signature.cpp:53`) on Java 11. We solved this by bypassing Spark for batch transformations and shifting compute directly to **Trino**, embracing the true decoupled nature of a Data Lakehouse!
- **Zombie Processes:** Windows file locks caused `rm -rf` inside WSL containers to silently fail, leaving zombie streams. We implemented `taskkill /T` process tree management in our FastAPI dashboard to guarantee clean shutdowns.

---
