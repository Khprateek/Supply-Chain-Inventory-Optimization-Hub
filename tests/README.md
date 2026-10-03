# 🧪 Automated Testing Suite

This directory contains automated test suites, quality assertions, and integration tests for the **Enterprise Supply Chain & Inventory Optimization Hub**.

---

## 📋 Test Catalog

- **`validate_dataset.py`**: Automated data quality, nullability, referential integrity, and business logic verification suite for landing/staging Parquet datasets.
  ```powershell
  python tests/validate_dataset.py
  ```
- **`test_bq.py`**: Integration check validating GCP authentication, BigQuery client initialization, dataset enumeration, and raw layer table schemas.
  ```powershell
  python tests/test_bq.py
  ```
- **`test_bq_connector.py`**: PySpark integration test validating Spark-BigQuery connector jar functionality and direct streaming writes.
  ```powershell
  python tests/test_bq_connector.py
  ```

---

## 🔍 dbt Data Quality Tests
In addition to the Python unit and integration tests above, the repository contains 170+ automated dbt data assertions in `dbt/models/` and `dbt/tests/`. Run them via:
```powershell
.\dbt.cmd test --project-dir dbt
```
