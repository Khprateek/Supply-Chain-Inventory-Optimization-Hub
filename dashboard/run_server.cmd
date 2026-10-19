@echo off
setlocal enabledelayedexpansion
set "PROJECT_ROOT=%~dp0.."
cd /d "%PROJECT_ROOT%"

set "PYTHON_CMD=python"
if exist "%PROJECT_ROOT%\.venv\Scripts\python.exe" (
    set "PYTHON_CMD=%PROJECT_ROOT%\.venv\Scripts\python.exe"
)

echo Starting Supply Chain Dashboard API Server on http://localhost:8000 ...
"%PYTHON_CMD%" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload

endlocal
