@echo off
REM Enterprise Open Data Lakehouse Platform - Environment Bootstrap
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"

echo =======================================================
echo   Starting Enterprise Open Data Lakehouse Platform
echo =======================================================
echo.

REM 1. Verify WinUtils for Windows PySpark local operations
if not exist "conf\spark\winutils\hadoop-3.3.6\bin\winutils.exe" (
    echo [Setup] Windows Hadoop binaries (winutils.exe) not found.
    echo [Setup] Running automatic setup script...
    python scripts\setup_winutils.py
)

REM 2. Start Docker containers
echo.
echo [1/3] Starting Docker containers (Kafka, Zookeeper, Spark, Nessie, S3, Trino)...
docker compose up -d
if errorlevel 1 (
    echo [ERROR] Failed to start Docker containers. Make sure Docker Desktop is running.
    exit /b 1
)

REM 3. Wait for services initialization
echo.
echo [2/3] Waiting for distributed services to initialize (5s)...
timeout /t 5 /nobreak > nul

REM 4. Detect Python runtime
set "PYTHON_CMD=python"
if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
)

REM 5. Launch FastAPI Observability Control Tower
echo.
echo [3/3] Starting FastAPI Dashboard Server on http://localhost:8000 ...
echo Press Ctrl+C to terminate the dashboard server.
echo.
"%PYTHON_CMD%" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload

endlocal
