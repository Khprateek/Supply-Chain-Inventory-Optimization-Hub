@echo off
REM Enterprise Supply Chain & Inventory Optimization Hub - Environment Bootstrap
setlocal
cd /d "%~dp0"

echo =======================================================
echo Starting Enterprise Supply Chain Hub Environment...
echo =======================================================

echo.
echo [1/3] Starting Docker containers (Kafka, Spark, Trino, Nessie, LocalStack)...
docker-compose up -d

echo.
echo [2/3] Waiting for services to initialize (5s)...
timeout /t 5 /nobreak > nul

echo.
echo [3/3] Starting FastAPI Dashboard Server on http://localhost:8000 ...
"%~dp0.venv\Scripts\python.exe" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload

endlocal
