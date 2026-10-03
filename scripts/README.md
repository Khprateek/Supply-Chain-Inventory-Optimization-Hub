# 🛠️ Scripts & Operational Runners

This directory houses all administrative, operational, data ingestion, and containerized compute runner scripts for the **Enterprise Supply Chain & Inventory Optimization Hub**.

---

## 📂 Script Catalog

### 🔄 Streaming & Data Ingestion
- **`kafka_data_generator.py`**: Multi-worker, throttled Kafka event producer simulating real-time inventory adjustments, receipts, picks, and omnichannel sales orders.
  ```powershell
  python scripts/kafka_data_generator.py --workers 1
  ```
- **`run_iceberg_stream.cmd`**: Submits the PySpark streaming ingestion job (`04_iceberg_stream.py`) to the containerized Spark cluster, ingesting Kafka topics into Nessie/Iceberg tables.
  ```powershell
  .\scripts\run_iceberg_stream.cmd
  ```

### ⚙️ Transformation & Lakehouse Compute
- **`run_iceberg_transform.cmd`**: Executes continuous real-time structured Iceberg transformations (`06_iceberg_transform.py`) in Docker.
  ```powershell
  .\scripts\run_iceberg_transform.cmd
  ```
- **`run_iceberg_batch_transform.cmd`**: Runs batch aggregation transformations (`05_iceberg_transforms.py`) in Docker, creating summary marts in Iceberg.
  ```powershell
  .\scripts\run_iceberg_batch_transform.cmd
  ```
- **`run_iceberg_maintenance.cmd`**: Runs table maintenance (`06_iceberg_maintenance.py`), performing file compaction, snapshot expiration, and manifest rewrites to eliminate small-file lakehouse degradation.
  ```powershell
  .\scripts\run_iceberg_maintenance.cmd
  ```

### 📊 ELT & Analytics Engineering
- **`dbt.cmd`**: Isolated dbt Core execution wrapper configured with proper virtual environment paths and profile resolution.
  ```powershell
  .\scripts\dbt.cmd run --project-dir dbt --select streaming
  ```

### 🔬 Architecture Benchmarking & Verification
- **`compare_architectures.py`**: Dual-query validation script comparing query performance and data consistency between **Architecture A** (BigQuery) and **Architecture B** (Iceberg Lakehouse via Trino).
  ```powershell
  python scripts/compare_architectures.py
  ```

### ☁️ Infrastructure & Environment Setup
- **`create_dataproc_cluster.py`**: Automation script to provision or tear down the Google Cloud Dataproc cluster based on `conf/gcp/dataproc_cluster.yaml`.
  ```powershell
  python scripts/create_dataproc_cluster.py --action create
  python scripts/create_dataproc_cluster.py --action delete
  ```
- **`download_spark_jars.py`**: Downloads required BigQuery connector JARs into `conf/spark/jars/` for offline PySpark execution.
  ```powershell
  python scripts/download_spark_jars.py
  ```
- **`setup_winutils.py`**: Windows-specific setup tool downloading `winutils.exe` and `hadoop.dll` for Hadoop 3.3.6.
  ```powershell
  python scripts/setup_winutils.py
  ```
