#!/usr/bin/env bash
# =============================================================================
# Enterprise Open Data Lakehouse Platform - Environment Bootstrap (Linux/macOS)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "======================================================="
echo "  Starting Enterprise Open Data Lakehouse Platform"
echo "======================================================="
echo ""

# 1. Start Docker containers
echo "[1/3] Starting Docker containers (Kafka, Zookeeper, Spark, Nessie, S3, Trino)..."
docker compose up -d

# 2. Wait for services to initialize
echo ""
echo "[2/3] Waiting for distributed services to initialize (5s)..."
sleep 5

# 3. Detect Python runtime
PYTHON_CMD="python3"
if [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
fi

# 4. Launch FastAPI Observability Control Tower
echo ""
echo "[3/3] Starting FastAPI Dashboard Server on http://localhost:8000 ..."
echo "Press Ctrl+C to terminate the dashboard server."
echo ""
exec "$PYTHON_CMD" -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload
