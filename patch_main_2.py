import re

path = 'dashboard/main.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Add transform_iceberg to COMMANDS
if '"transform_iceberg"' not in content:
    content = content.replace(
        '"dbt_run":',
        '"transform_iceberg": [VENV_PYTHON, os.path.join(PROJECT_ROOT, "pyspark_jobs", "streaming", "06_iceberg_transform.py")],\n    "dbt_run":'
    )

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated main.py")
