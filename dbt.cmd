@echo off
REM dbt wrapper — bypasses the broken .exe launcher that points to the old venv.
REM Forces use of the project venv's Python, isolated from system Python packages.
REM Usage (from project root):
REM   .\dbt.cmd run --project-dir dbt --select streaming
REM   .\dbt.cmd debug --project-dir dbt

set PYTHONNOUSERSITE=1
set PYTHONPATH=
set DBT_PROFILES_DIR=dbt
"d:\JOB\1.Buid_Skill\Work\Supply Chain and Inventory Optimization Hub\.venv\Scripts\python.exe" -c "from dbt.cli.main import cli; cli()" %*
