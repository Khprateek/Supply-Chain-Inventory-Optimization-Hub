import os
from google.cloud import bigquery

client = bigquery.Client()

datasets = [
    os.getenv('BQ_DATASET_RAW', 'raw_supply_chain'),
    os.getenv('BQ_DATASET_STAGING', 'supply_chain_staging'),
    os.getenv('BQ_DATASET_MARTS', 'supply_chain_marts'),
    os.getenv('BQ_DATASET_ML', 'supply_chain_ml'),
    os.getenv('BQ_DATASET_SPARK_TEMP', 'spark_temp'),
    os.getenv('GCP_DATASET_DEV', 'sc_dev') # from dbt profile
]

for ds_id in datasets:
    dataset_ref = f"{client.project}.{ds_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {ds_id} created or already exists.")
    except Exception as e:
        print(f"Error creating {ds_id}: {e}")
