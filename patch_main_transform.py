import re

path = 'dashboard/main.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '[VENV_PYTHON, os.path.join(PROJECT_ROOT, "pyspark_jobs", "streaming", "06_iceberg_transform.py")]',
    '[os.path.join(PROJECT_ROOT, "run_iceberg_transform.cmd")]'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
