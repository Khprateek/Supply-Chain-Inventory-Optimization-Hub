import re

path = 'pyspark_jobs/streaming/05_bigquery_stream.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# We'll use a simpler replace
content = content.replace('errors = client.insert_rows_json(table_ref, records)', 'job = client.load_table_from_json(records, table_ref)\n                    job.result()')
content = content.replace('''                    if errors:
                        print(f"[ARCH-A] BQ Insert Errors: {errors}")''', '')
content = content.replace('''                if errors:
                    print(f"[ARCH-A] BQ Insert Errors: {errors}")''', '')

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated 05_bigquery_stream.py")
