# Implementation Plan & Milestones
## Supply Chain & Inventory Optimization Hub Upgrade

### Phase 1: Infrastructure Containerization (Completed)
- [x] Design the target "Exceptional" streaming architecture.
- [x] Migrate away from local Polars scripts to a distributed Docker Compose stack.
- [x] Configure Kafka (Broker) & Zookeeper for data ingestion.
- [x] Configure LocalStack S3 for mock object storage.
- [x] Configure Nessie as the Iceberg Catalog.
- [x] Configure Trino for distributed SQL querying over S3/Nessie.
- [x] Configure Spark Master & Worker nodes for distributed streaming execution.

### Phase 2: High-Throughput Ingestion (Completed)
- [x] Develop a Python-based Kafka Producer (`kafka_data_generator.py`).
- [x] Bypass Python GIL bottlenecks using the `multiprocessing` library to max out CPU cores.
- [x] Pre-generate payload templates to eliminate JSON serialization overhead during the benchmark.
- [x] Achieve generation capability of 150,000+ events per second.

### Phase 3: Stream Processing & Data Lakehouse (Completed)
- [x] Develop PySpark Structured Streaming application (`04_iceberg_stream.py`).
- [x] Connect PySpark to Kafka using the `spark-sql-kafka` package.
- [x] Apply explicit schema definitions and JSON parsing functions.
- [x] Configure Iceberg Nessie Catalog and AWS SDK V2 (`S3FileIO`) integrations.
- [x] Fix AWS SDK Region chain issues by passing explicit `-Daws.region=us-east-1` Java properties.
- [x] Fix Hadoop S3 Checkpointing dependencies by moving checkpoints to local Docker volumes.
- [x] Implement `.option("startingOffsets", "earliest")` to ensure benchmark compatibility.

### Phase 4: Final Validation & Benchmarking (Completed)
- [x] Execute `kafka_data_generator.py` to stream continuous events to the Kafka `inventory_events` topic.
- [x] Submit the Spark Streaming job to the Spark Master node.
- [x] Verify exactly-once delivery and continuous Iceberg writes.
- [x] Removed partition transform barriers to optimize for continuous high-throughput loads.

### Phase 5: Downstream BI Integration (Future)
- [ ] Connect a BI tool (Power BI, Tableau, or Apache Superset) to Trino via JDBC.
- [ ] Build a Real-Time Inventory Tracking Dashboard.
- [ ] Implement Iceberg Maintenance Jobs (e.g., `OPTIMIZE`, `EXPIRE SNAPSHOTS`) to periodically compact small streaming files into larger blocks.
