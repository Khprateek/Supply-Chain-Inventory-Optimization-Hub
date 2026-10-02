import re

path = 'pyspark_jobs/streaming/05_bigquery_stream.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

content = re.sub(r'inv_q\s*=\s*stream_topic_to_bq\(spark,\s*"inventory_events",\s*"raw_inventory_events"\)', 'inv_q = stream_topic_to_bq(spark, "inventory_events", "raw_inventory_events", inventory_schema)', content)

content = re.sub(r'sales_q\s*=\s*stream_topic_to_bq\(spark,\s*"sales_events",\s*"raw_sales_events"\)', 'sales_q = stream_topic_to_bq(spark, "sales_events", "raw_sales_events", sales_schema)', content)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed BQ Stream")
