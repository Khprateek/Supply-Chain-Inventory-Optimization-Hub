import re

path = 'run_iceberg_stream.cmd'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('--conf spark.jars.ivy=/tmp/.ivy2', '--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2')

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated run_iceberg_stream.cmd")
