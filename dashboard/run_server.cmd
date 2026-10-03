@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
cd /d "%PROJECT_ROOT%"

echo Starting Supply Chain Dashboard API Server on http://localhost:8000 ...
"%PROJECT_ROOT%\.venv\Scripts\python.exe" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload

endlocal
