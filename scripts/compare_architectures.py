import os
import pandas as pd
from google.cloud import bigquery
import trino

# Ensure GCP Auth is set
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sa_key = os.path.join(PROJECT_ROOT, "credentials", "sa_dbt.json")
if os.path.exists(sa_key):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_key

# Configuration
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "smart-supply-and-inventory")
BQ_DATASET = "sc_dev"
BQ_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.fct_stream_sales_summary"

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "marts"
TRINO_TABLE = "fact_sales_summary"

def query_bigquery():
    print(f"[>] Querying Architecture A (BigQuery: {BQ_TABLE})...")
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
    SELECT 
        SUM(total_revenue) as total_revenue,
        SUM(total_units_sold) as total_units_sold,
        SUM(total_transactions) as total_transactions
    FROM `{BQ_TABLE}`
    """
    try:
        df = client.query(query).to_dataframe()
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"[X] BigQuery Error: {e}")
        return None

def query_iceberg():
    print(f"[>] Querying Architecture B (Iceberg via Trino: {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE})...")
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )
        query = f"""
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_units_sold) as total_units_sold,
            SUM(transaction_count) as total_transactions
        FROM {TRINO_TABLE}
        """
        df = pd.read_sql(query, conn)
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"[X] Trino/Iceberg Error: {e}")
        return None

def print_comparison(bq_data, iceberg_data):
    print("\n" + "="*60)
    print("[*] ARCHITECTURE SHOWDOWN: BIGQUERY vs. ICEBERG")
    print("="*60)
    
    metrics = ["total_revenue", "total_units_sold", "total_transactions"]
    
    print(f"{'Metric':<25} | {'Architecture A (BQ)':<20} | {'Architecture B (Iceberg)':<20}")
    print("-" * 60)
    
    for metric in metrics:
        bq_val = bq_data.get(metric, 0) if bq_data else 0
        ice_val = iceberg_data.get(metric, 0) if iceberg_data else 0
        
        # Format numbers
        bq_str = f"{bq_val:,.2f}" if isinstance(bq_val, float) else f"{bq_val:,}"
        ice_str = f"{ice_val:,.2f}" if isinstance(ice_val, float) else f"{ice_val:,}"
        
        # Handle None values gracefully
        if pd.isna(bq_val): bq_str = "0"
        if pd.isna(ice_val): ice_str = "0"
        
        match = "[MATCH]" if bq_str == ice_str and bq_val != 0 else "[DIFF]"
        print(f"{metric:<25} | {bq_str:<20} | {ice_str:<18} {match}")
        
    print("="*60)
    print("* Note: If the numbers don't match exactly, it's likely because the")
    print("  streaming ingestion scripts were running for different lengths of time,")
    print("  or dbt/Spark batch transforms were run at different offsets.")

if __name__ == "__main__":
    bq_results = query_bigquery()
    iceberg_results = query_iceberg()
    
    print_comparison(bq_results, iceberg_results)
