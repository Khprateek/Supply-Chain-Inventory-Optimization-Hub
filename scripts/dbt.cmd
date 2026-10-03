@echo off
REM Enterprise dbt wrapper — runs dbt via the virtual environment CLI
REM Portable across all developer machines and CI environments.

setlocal
set PYTHONNOUSERSITE=1
set PYTHONPATH=
set "PROJECT_ROOT=%~dp0.."
set "DBT_PROFILES_DIR=%PROJECT_ROOT%\dbt"

"%PROJECT_ROOT%\.venv\Scripts\python.exe" -c "from dbt.cli.main import cli; cli()" %*
endlocal
