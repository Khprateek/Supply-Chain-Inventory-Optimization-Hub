# 🧪 Automated Testing & Verification Suite

This directory contains automated test suites, engineering benchmarks, quality assertions, and integration tests for the **Enterprise Open Data Lakehouse Platform**.

---

## 📋 Test Catalog

- **`benchmark_suite.py`**: Comprehensive 5-stage engineering benchmark harness executing **Performance Testing**, **Stress Testing**, **Resilience Testing**, **Capacity Testing**, and **Scalability Testing** across Kafka, PySpark, Iceberg, Nessie, and Trino.
  - 📄 See **[`docs/BENCHMARK_AND_TESTING_SUITE.md`](../docs/BENCHMARK_AND_TESTING_SUITE.md)** for full mathematical derivations and test specifications.
  ```powershell
  # Run all 5 benchmark tests and generate JSON report
  python tests/benchmark_suite.py --test all
  ```

- **`validate_dataset.py`**: Automated data quality, nullability, referential integrity, and business logic verification suite for Parquet datasets.
  ```powershell
  python tests/validate_dataset.py
  ```

- **`verify_setup.py` (in `pyspark_jobs/`)**: Comprehensive pre-flight verification script checking PySpark, local DataFrame engines, Windows winutils, and connectivity to Lakehouse services (Kafka, Nessie, LocalStack S3, Trino).
  ```powershell
  python pyspark_jobs/verify_setup.py
  ```

---

## 🔍 Lakehouse Data Quality & SQL Audits
Trino SQL quality assertions can be executed directly against Iceberg tables using the queries in `sql/trino_lakehouse_queries.sql` or via the Docker container:
```powershell
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.sales.raw_events;"
```
