import os
import subprocess
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from typing import Dict
from .metrics import router as metrics_router

app = FastAPI(title="Supply Chain Dashboard API")
app.include_router(metrics_router, prefix="/api")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store running processes
processes: Dict[str, subprocess.Popen] = {}

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe")

COMMANDS = {
    "generator": [VENV_PYTHON, os.path.join(PROJECT_ROOT, "scripts", "kafka_data_generator.py")],
    "stream_bq": [VENV_PYTHON, os.path.join(PROJECT_ROOT, "pyspark_jobs", "streaming", "05_bigquery_stream.py")],
    "stream_iceberg": [os.path.join(PROJECT_ROOT, "run_iceberg_stream.cmd")],
    "dbt_run": [os.path.join(PROJECT_ROOT, "dbt.cmd"), "run", "--project-dir", os.path.join(PROJECT_ROOT, "dbt"), "--select", "streaming"]
}

def kill_process_tree(pid: int):
    """Kills a process and all its children on Windows."""
    if os.name == 'nt':
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        os.system(f"pkill -TERM -P {pid}")

@app.post("/api/process/{process_id}/start")
def start_process(process_id: str):
    if process_id not in COMMANDS:
        raise HTTPException(status_code=404, detail="Unknown process ID")
    
    # Check if already running
    if process_id in processes:
        proc = processes[process_id]
        if proc.poll() is None:
            return {"status": "already_running", "process_id": process_id, "pid": proc.pid}
            
    try:
        cmd = COMMANDS[process_id]
        
        # Start the process. We use CREATE_NEW_CONSOLE or CREATE_NEW_PROCESS_GROUP
        # to ensure it gets its own process tree which we can cleanly taskkill later.
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        
        proc = subprocess.Popen(
            cmd, 
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL, # In production we'd stream this to a file
            stderr=subprocess.DEVNULL,
            creationflags=creationflags
        )
        processes[process_id] = proc
        return {"status": "started", "process_id": process_id, "pid": proc.pid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process/{process_id}/stop")
def stop_process(process_id: str):
    if process_id not in processes:
        return {"status": "not_running"}
        
    proc = processes[process_id]
    if proc.poll() is None:
        try:
            kill_process_tree(proc.pid)
            return {"status": "stopped", "process_id": process_id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        return {"status": "already_stopped"}

@app.get("/api/status")
def get_status():
    status_dict = {}
    for pid, cmd in COMMANDS.items():
        if pid in processes and processes[pid].poll() is None:
            status_dict[pid] = "running"
        else:
            status_dict[pid] = "stopped"
    return status_dict
