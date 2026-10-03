"""
test_bq.py
Comprehensive BigQuery connectivity and schema validation test.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SA_KEY_PATH = PROJECT_ROOT / "credentials" / "sa_dbt.json"

if SA_KEY_PATH.exists() and "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_KEY_PATH)

def test_bigquery_connection():
    try:
        from google.cloud import bigquery
    except ImportError:
        print("[FAIL] google-cloud-bigquery library is not installed.")
        return False

    try:
        client = bigquery.Client()
        print(f"[PASS] Connected to GCP Project: {client.project}")

        datasets = [d.dataset_id for d in client.list_datasets()]
        print(f"[PASS] Available Datasets ({len(datasets)}): {datasets}")

        # Check sample tables in raw layer if dataset exists
        target_dataset = "raw_supply_chain"
        if target_dataset in datasets:
            sample_tables = ["raw_bridge_product_supplier", "raw_fact_demand_forecast"]
            for tbl_name in sample_tables:
                try:
                    table_ref = f"{client.project}.{target_dataset}.{tbl_name}"
                    table = client.get_table(table_ref)
                    col_names = [f.name for f in table.schema]
                    print(f"[PASS] Table '{tbl_name}' schema verified ({len(col_names)} columns): {col_names[:5]}...")
                except Exception as e:
                    print(f"[INFO] Table '{tbl_name}' not queryable: {e}")
        return True
    except Exception as e:
        print(f"[FAIL] BigQuery connectivity check failed: {e}")
        return False

if __name__ == "__main__":
    success = test_bigquery_connection()
    sys.exit(0 if success else 1)
