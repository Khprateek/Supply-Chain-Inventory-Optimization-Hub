# 🛠️ Scripts & Operational Runners

This directory houses all operational, data ingestion, table maintenance, and containerized compute runner scripts for the **Enterprise Open Data Lakehouse Platform**.

---

## 📂 Script Catalog

### 🔄 Streaming & Data Ingestion
- **`kafka_data_generator.py`**: Multi-worker, throttled Kafka event producer simulating real-time omnichannel sales orders and warehouse inventory adjustments.
  ```powershell
  python scripts/kafka_data_generator.py --workers 1
  ```
- **`run_iceberg_stream.cmd`**: Submits the PySpark structured streaming ingestion job (`04_iceberg_stream.py`) to the containerized Spark cluster, streaming Kafka topics into Iceberg tables.
  ```powershell
  .\scripts\run_iceberg_stream.cmd
  ```

### ⚙️ Transformation & Lakehouse Compute
- **`run_iceberg_transform.cmd`**: Executes continuous real-time structured Iceberg transformations (`06_iceberg_transform.py`) in Docker, deduplicating and merging records.
  ```powershell
  .\scripts\run_iceberg_transform.cmd
  ```
- **`run_iceberg_batch_transform.cmd`**: Runs batch aggregation transformations (`05_iceberg_transforms.py`) in Docker, creating summary marts in Iceberg.
  ```powershell
  .\scripts\run_iceberg_batch_transform.cmd
  ```
- **`run_iceberg_maintenance.cmd`**: Runs table maintenance (`06_iceberg_maintenance.py`), performing bin-packing compaction, snapshot expiration, and manifest rewrites to prevent small-file degradation.
  ```powershell
  .\scripts\run_iceberg_maintenance.cmd
  ```

### 💻 Infrastructure & Environment Setup
- **`setup_winutils.py`**: Windows-specific setup tool downloading `winutils.exe` and `hadoop.dll` for Hadoop 3.3.6.
  ```powershell
  python scripts/setup_winutils.py
  ```
