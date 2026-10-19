#!/usr/bin/env bash
# Start FastAPI Control Tower Observability Dashboard
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON_CMD="python3"
if [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
fi

echo "Starting FastAPI Dashboard Server on http://localhost:8000 ..."
exec "$PYTHON_CMD" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload
