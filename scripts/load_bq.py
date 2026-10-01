import os
import glob
import re
from google.cloud import bigquery

client = bigquery.Client()
project = client.project
dataset_id = os.getenv('BQ_DATASET_RAW', 'raw_supply_chain')

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

files = glob.glob('data/raw/*.parquet')
for f in files:
    basename = os.path.basename(f)
    name_without_ext = os.path.splitext(basename)[0]
    
    # special case for SecurityUser if needed, but we can just prepend raw_ and snake case
    snake_name = camel_to_snake(name_without_ext)
    table_id = f"raw_{snake_name}"
    
    table_ref = f"{project}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    
    print(f"Loading {f} to {table_ref}...")
    with open(f, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
    try:
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_ref)  # Make an API request.
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    except Exception as e:
        print(f"Error loading {f}: {e}")
        if hasattr(job, 'errors') and job.errors:
            for err in job.errors:
                print(err)
