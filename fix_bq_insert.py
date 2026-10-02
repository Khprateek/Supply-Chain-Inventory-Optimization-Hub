import re

path = 'pyspark_jobs/streaming/05_bigquery_stream.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace insert_rows_json with load_table_from_json
old_code = '''                if len(records) >= 5000:
                    errors = client.insert_rows_json(table_ref, records)
                    if errors:
                        print(f"[ARCH-A] BQ Insert Errors: {errors}")
                    records = []
            
            if records:
                errors = client.insert_rows_json(table_ref, records)
                if errors:
                    print(f"[ARCH-A] BQ Insert Errors: {errors}")'''

new_code = '''                if len(records) >= 5000:
                    job = client.load_table_from_json(records, table_ref)
                    job.result()  # wait for completion
                    records = []
            
            if records:
                job = client.load_table_from_json(records, table_ref)
                job.result()'''

content = content.replace(old_code, new_code)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
