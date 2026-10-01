# Technical Requirements Document (TRD)
## Supply Chain & Inventory Optimization Hub - Streaming Lakehouse

### 1. System Architecture Overview
The platform utilizes a **Distributed Streaming Lakehouse** architecture entirely containerized using Docker Compose for local validation and horizontal scaling capabilities. 

### 2. Technology Stack
* **Message Broker / Ingestion:** Apache Kafka (Confluent CP-Kafka v7.4.0) & Zookeeper
* **Stream Processing Engine:** Apache Spark Structured Streaming (Spark v3.5.0)
* **Table Format / Lakehouse:** Apache Iceberg
* **Catalog Service:** Project Nessie (Iceberg Catalog for branching/commits)
* **Object Storage Backend:** LocalStack S3 (AWS S3 Mock via v3.5.0)
* **Distributed Query Engine:** Trino (MPP SQL Engine)

### 3. Non-Functional Requirements (NFRs)
* **Fault Tolerance:** Spark Checkpointing to local/S3 storage guarantees recovery and exactly-once processing semantics.
* **ACID Transactions:** Apache Iceberg ensures atomic commits. Trino queries will always read a consistent snapshot, eliminating dirty reads.
* **Scalability:** Spark Workers can be horizontally scaled in Docker to increase partition processing throughput.
* **Performance:** Parquet files optimized via Iceberg metadata; Trino utilizes vectorized query execution.

### 4. Component Interactions
1. **Event Generator (Python):** Bypasses CPU bottlenecks via `multiprocessing`. Generates and pushes JSON events directly into Kafka (`inventory_events` topic).
2. **Apache Kafka:** Acts as a persistent, distributed log. Buffers events to ensure no data loss during traffic spikes.
3. **Apache Spark Master/Worker:** Runs `04_iceberg_stream.py`. Spark Structured Streaming consumes Kafka topics, parses JSON using a rigid schema, and streams micro-batches every 5 seconds.
4. **Apache Iceberg / Nessie / S3:** Spark writes data via Iceberg `S3FileIO` directly to LocalStack S3 (`s3://warehouse`). Nessie acts as the catalog API to track Iceberg snapshots and table metadata.
5. **Trino:** Connects to Nessie and S3 via `iceberg.properties`. Trino serves as the JDBC/ODBC endpoint for end-user tools to query the Iceberg warehouse at MPP speeds.

### 5. Deployment & Configuration Requirements
* **Docker Memory:** Ensure Docker Desktop / WSL2 is allocated at least 8GB+ RAM.
* **AWS SDK Overrides:** Because LocalStack S3 is used, AWS SDK requires explicitly passing dummy `AWS_REGION=us-east-1` and `AWS_ACCESS_KEY_ID` to the JVM via `spark-submit` Java Properties and environment variables.
* **Dependency Management:** Spark packages for Iceberg, Nessie, Kafka, and AWS SDK must be passed via `--packages` on application boot.
