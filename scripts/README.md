# 🛠️ Operational Scripts & Pipeline Runners

This directory houses all automation, operational runners, streaming submitters, table maintenance workers, and developer bootstrap utilities for the **Enterprise Open Data Lakehouse Platform**.

Scripts are provided in cross-platform formats:
* **Windows**: Native Batch (`.cmd`) scripts optimized for Windows Command Prompt and PowerShell.
* **Linux / macOS**: POSIX Shell (`.sh`) scripts with execution permissions.
* **Makefile**: Universal automation via `make <command>` at the repository root.

---

## 📂 Runner Catalog

### 🚀 Bootstrap & Orchestration
- **`start_environment.cmd`** / **`start_environment.sh`**: One-command platform bootstrap. Verifies Windows `winutils.exe` (auto-downloads if missing), starts all Docker containers, and launches the FastAPI Control Tower dashboard.
  ```bash
  # Windows
  .\scripts\start_environment.cmd

  # Linux / macOS
  ./scripts/start_environment.sh
  ```

---

### 🔄 Streaming & Data Ingestion
- **`kafka_data_generator.py`**: High-throughput multi-worker event engine generating continuous sales telemetry and inventory movements to Kafka topics (`sales_events`, `inventory_events`) with LZ4 compression.
  ```bash
  # High-throughput benchmark mode (~30K events/sec across 2 workers)
  python scripts/kafka_data_generator.py --workers 2

  # Throttled steady-state mode for local monitoring (1,000 events/sec)
  python scripts/kafka_data_generator.py --workers 1 --rate 1000

  # Fixed-batch mode (e.g. 50,000 events per worker)
  python scripts/kafka_data_generator.py --workers 2 --max-events 50000
  ```

- **`run_iceberg_stream.cmd`** / **`run_iceberg_stream.sh`**: Submits PySpark Structured Streaming ingestion (`04_iceberg_stream.py`) to the containerized Spark cluster, streaming Kafka topics into Iceberg ACID tables via Project Nessie and MinIO S3.
  ```bash
  # Windows
  .\scripts\run_iceberg_stream.cmd

  # Linux / macOS / WSL
  ./scripts/run_iceberg_stream.sh

  # Or via Make
  make stream
  ```

---

### ⚙️ Transformation & Lakehouse Compute
- **`run_iceberg_transform.cmd`** / **`run_iceberg_transform.sh`**: Executes continuous real-time structured Iceberg transformations (`06_iceberg_transform.py`) in Docker, performing in-flight deduplication and upserts.
  ```bash
  # Windows
  .\scripts\run_iceberg_transform.cmd

  # Linux / macOS
  ./scripts/run_iceberg_transform.sh
  ```

- **`run_iceberg_batch_transform.cmd`** / **`run_iceberg_batch_transform.sh`**: Runs batch aggregation transformations (`05_iceberg_transforms.py`) in Docker, materializing Gold-tier dimensional summary marts.
  ```bash
  # Windows
  .\scripts\run_iceberg_batch_transform.cmd

  # Linux / macOS
  ./scripts/run_iceberg_batch_transform.sh

  # Or via Make
  make transform
  ```

- **`run_iceberg_maintenance.cmd`** / **`run_iceberg_maintenance.sh`**: Executes Iceberg table maintenance (`06_iceberg_maintenance.py`), performing bin-packing compaction (`rewrite_data_files`), snapshot expiration, and manifest tree pruning.
  ```bash
  # Windows
  .\scripts\run_iceberg_maintenance.cmd

  # Linux / macOS
  ./scripts/run_iceberg_maintenance.sh

  # Or via Make
  make maintenance
  ```

---

### 💻 Infrastructure & Environment Setup
- **`setup_winutils.py`**: Windows-specific setup tool automatically downloading `winutils.exe` and `hadoop.dll` for Hadoop 3.3.6 from the community repository into `conf/spark/winutils/hadoop-3.3.6/bin/`.
  ```bash
  python scripts/setup_winutils.py
  ```
