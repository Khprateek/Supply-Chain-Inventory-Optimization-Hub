import os
import subprocess
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from typing import Dict, Optional
import threading
import signal
import socket
import requests
from contextlib import asynccontextmanager
from confluent_kafka.admin import AdminClient

# Paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe")
LOGS_DIR = os.path.join(PROJECT_ROOT, "dashboard", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Process management
processes: Dict[str, subprocess.Popen] = {}
_proc_lock = threading.Lock()

COMMANDS = {
    "generator": [VENV_PYTHON, os.path.join(PROJECT_ROOT, "scripts", "kafka_data_generator.py"), "--workers", "1"],
    "stream_iceberg": [os.path.join(PROJECT_ROOT, "scripts", "run_iceberg_stream.cmd")],
    "transform_iceberg": [os.path.join(PROJECT_ROOT, "scripts", "run_iceberg_transform.cmd")],
    "maintenance_iceberg": [os.path.join(PROJECT_ROOT, "scripts", "run_iceberg_maintenance.cmd")],
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Gracefully terminate all spawned processes on server shutdown
    with _proc_lock:
        for pid, proc in list(processes.items()):
            if proc and proc.poll() is None:
                try:
                    if os.name == 'nt':
                        subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    else:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except Exception:
                    pass


app = FastAPI(title="Open Lakehouse Control Tower", lifespan=lifespan)

# Mount static files and templates
STATIC_DIR = os.path.join(PROJECT_ROOT, "dashboard", "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=os.path.join(PROJECT_ROOT, "dashboard", "templates"))


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.post("/api/process/{process_id}/{action}")
def manage_process(process_id: str, action: str):
    if process_id not in COMMANDS:
        raise HTTPException(status_code=404, detail="Process not found")

    with _proc_lock:
        proc = processes.get(process_id)

        if action == "start":
            if proc and proc.poll() is None:
                return {"status": "already running", "process_id": process_id, "pid": proc.pid}

            cmd = COMMANDS[process_id]
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0

            # Capture combined stdout and stderr to log file
            log_path = os.path.join(LOGS_DIR, f"{process_id}.log")
            err_file = open(log_path, "a", encoding="utf-8")
            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=PROJECT_ROOT,
                    stdout=err_file,
                    stderr=subprocess.STDOUT,
                    creationflags=creationflags
                )
            finally:
                err_file.close()

            processes[process_id] = proc
            return {"status": "started", "process_id": process_id, "pid": proc.pid}

        elif action == "stop":
            if not proc or proc.poll() is not None:
                return {"status": "already stopped", "process_id": process_id}

            if os.name == 'nt':
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except Exception:
                    pass

            if "iceberg" in process_id:
                try:
                    if "stream" in process_id:
                        target_script = "04_iceberg_stream.py"
                    elif "maintenance" in process_id:
                        target_script = "06_iceberg_maintenance.py"
                    else:
                        target_script = "06_iceberg_transform.py"

                    subprocess.run(["docker", "exec", "spark-master", "pkill", "-f", target_script],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass

            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

            return {"status": "stopped", "process_id": process_id}


@app.get("/api/status")
def get_status():
    with _proc_lock:
        status_dict = {}
        for pid in COMMANDS.keys():
            proc = processes.get(pid)
            if proc and proc.poll() is None:
                status_dict[pid] = {"state": "running", "pid": proc.pid}
            else:
                error_msg = None
                log_path = os.path.join(LOGS_DIR, f"{pid}.log")
                if not os.path.exists(log_path):
                    log_path = os.path.join(LOGS_DIR, f"{pid}.err")
                if os.path.exists(log_path):
                    try:
                        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                            lines = f.readlines()
                            if lines:
                                error_msg = "".join(lines[-15:])
                    except Exception:
                        pass
                status_dict[pid] = {
                    "state": "stopped",
                    "exit_code": proc.poll() if proc else None,
                    "error": error_msg if (proc and proc.poll() not in (None, 0)) else None
                }

        return status_dict


@app.get("/api/logs/{process_id}")
def get_logs(process_id: str, lines: int = 100):
    """Retrieve recent console output logs for a background process."""
    if process_id not in COMMANDS:
        raise HTTPException(status_code=404, detail="Process not found")

    for ext in (".log", ".err"):
        log_path = os.path.join(LOGS_DIR, f"{process_id}{ext}")
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    all_lines = f.readlines()
                    return {
                        "process_id": process_id,
                        "lines": "".join(all_lines[-lines:])
                    }
            except Exception as e:
                return {"process_id": process_id, "lines": f"Error reading log: {e}"}

    return {"process_id": process_id, "lines": "No log output recorded yet."}


@app.get("/api/health")
def health_check():
    health = {}

    # 1. Kafka (external listener on host is port 29092)
    try:
        admin = AdminClient({'bootstrap.servers': 'localhost:29092', 'socket.timeout.ms': 2000})
        topics = admin.list_topics(timeout=2).topics
        health["kafka"] = {
            "status": "ok" if "sales_events" in topics or "inventory_events" in topics else "warning",
            "details": f"Connected ({len(topics)} topics discovered)"
        }
    except Exception as e:
        health["kafka"] = {"status": "error", "details": str(e)}

    # 2. Nessie (Iceberg catalog)
    try:
        r = requests.get('http://localhost:19120/api/v1/config', timeout=2)
        health["nessie"] = {
            "status": "ok" if r.status_code == 200 else "error",
            "details": f"HTTP {r.status_code} ({r.json().get('defaultBranch', 'main')})"
        }
    except Exception as e:
        health["nessie"] = {"status": "error", "details": str(e)}

    # 3. Trino (Distributed query engine)
    try:
        r = requests.get('http://localhost:8080/v1/info', timeout=2)
        if r.status_code == 200:
            info = r.json()
            uptime = info.get("uptime", "online")
            health["trino"] = {"status": "ok", "details": f"Online (uptime: {uptime})"}
        else:
            health["trino"] = {"status": "error", "details": f"HTTP {r.status_code}"}
    except Exception as e:
        health["trino"] = {"status": "error", "details": str(e)}

    # 4. Spark Master
    try:
        r = requests.get('http://localhost:8081', timeout=2)
        health["spark"] = {
            "status": "ok" if r.status_code == 200 else "warning",
            "details": f"Master WebUI Active (port 8081)"
        }
    except Exception as e:
        health["spark"] = {"status": "error", "details": "Spark Master WebUI unreachable"}

    return health


# Import metrics routes
from dashboard.metrics import router as metrics_router
app.include_router(metrics_router, prefix="/api")
