# 🧪 Automated Testing & Verification Suite

This directory contains automated test suites, quality assertions, and integration tests for the **Enterprise Open Data Lakehouse Platform**.

---

## 📋 Test Catalog

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
