"""Argon API server: runs workflows, streams their events live, serves
benchmark results, and drives the failure lab.

Crash scenarios deliberately run in a subprocess (python -m argon.lab).
A simulated process death cancels asyncio tasks as it unwinds, and that
must never be able to take the API server down with it. Everything else
(transient failures, uncertain actions, delays) runs in process.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .agents import Planner
from .benchmark import generate_tasks
from .lab import FAMILIES
from .orchestrator import Runner

ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "runs" / "server"

runner = Runner(RUNS_DIR / "journal.db", RUNS_DIR / "ws")
run_lock = asyncio.Lock()
tasks_by_run: dict[str, dict[str, Any]] = {}
reports: dict[str, dict[str, Any]] = {}
ws_queues: set[asyncio.Queue] = set()


async def _broadcast(kind: str, payload: dict[str, Any]) -> None:
    message = {"kind": kind, "payload": payload}
    for queue in list(ws_queues):
        queue.put_nowait(message)


runner.listeners.append(_broadcast)

app = FastAPI(title="Argon", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv(
        "CORS_ORIGINS", "http://localhost:5173,http://localhost:8080"
    ).split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


def example_tasks() -> list[dict[str, Any]]:
    by_id = {t["id"]: t for t in generate_tasks()}
    picked = [by_id[k] for k in
              ("transform_00", "fanout_00", "ledger_00", "report_00",
               "chain_00")]
    picked.append({
        "id": "payment_demo",
        "title": "Log a payment and issue a receipt",
        "steps": [
            {"id": "log", "tool": "append_line",
             "args": {"path": "ledger.txt", "line": "paid 50"}},
            {"id": "receipt", "tool": "write_file",
             "args": {"path": "receipt.txt", "content": "paid 50"},
             "depends_on": ["log"]},
        ],
        "acceptance": [
            {"type": "file_line_count", "path": "ledger.txt", "count": 1},
            {"type": "file_equals", "path": "receipt.txt",
             "content": "paid 50"},
        ],
    })
    return picked


class RunRequest(BaseModel):
    task: dict[str, Any]
    faults: Optional[dict[str, Any]] = None


def _workspace_for(run_id: str) -> Path:
    return RUNS_DIR / "ws" / run_id


async def _execute(run_id: str, task: dict[str, Any],
                   faults: Optional[dict[str, Any]], resume: bool) -> None:
    async with run_lock:
        if resume:
            report = await runner.resume_task(
                task, run_id, workspace=_workspace_for(run_id)
            )
        else:
            report = await runner.run_task(
                task, faults=faults, run_id=run_id,
                workspace=_workspace_for(run_id),
            )
        reports[run_id] = report
        await _broadcast("report_ready", {"run_id": run_id, **{
            k: report.get(k) for k in ("status", "verified", "duration_s")
        }})


@app.get("/api/examples")
async def examples() -> list[dict[str, Any]]:
    return example_tasks()


@app.post("/api/runs")
async def create_run(req: RunRequest) -> dict[str, Any]:
    if req.faults and "crash_on" in req.faults:
        raise HTTPException(
            400,
            "crash faults run through the failure lab, not the live runner",
        )
    workflow = await Planner(runner.model).plan(req.task)
    run_id = await runner.journal.start_run(workflow)
    tasks_by_run[run_id] = req.task
    asyncio.create_task(_execute(run_id, req.task, req.faults, resume=False))
    return {"run_id": run_id}


@app.post("/api/runs/{run_id}/resume")
async def resume_run(run_id: str) -> dict[str, Any]:
    task = tasks_by_run.get(run_id)
    if task is None:
        raise HTTPException(404, "unknown run")
    asyncio.create_task(_execute(run_id, task, None, resume=True))
    return {"run_id": run_id, "resuming": True}


@app.get("/api/runs")
async def list_runs() -> list[dict[str, Any]]:
    return await runner.journal.list_runs()


@app.get("/api/runs/{run_id}")
async def inspect(run_id: str) -> dict[str, Any]:
    data = await runner.inspect_run(run_id)
    if not data["states"]:
        raise HTTPException(404, "unknown run")
    data["report"] = reports.get(run_id)
    data["task"] = tasks_by_run.get(run_id)
    return data


@app.get("/api/benchmarks")
async def benchmarks() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in ("results", "failure-lab-results"):
        path = ROOT / "benchmarks" / f"{name}.json"
        if path.exists():
            data = json.loads(path.read_text())
            data.pop("results", None)  # summaries only; detail stays on disk
            out[name] = data
    return out


@app.post("/api/lab/{family}")
async def run_lab(family: str, index: int = 0) -> dict[str, Any]:
    if family not in FAMILIES:
        raise HTTPException(404, f"unknown scenario family: {family}")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "argon.lab", family, str(index % 10),
        cwd=ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise HTTPException(
            500, f"lab scenario failed: {stderr.decode()[-500:]}"
        )
    return json.loads(stdout.decode().strip().splitlines()[-1])


@app.websocket("/ws")
async def events_socket(ws: WebSocket) -> None:
    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue()
    ws_queues.add(queue)
    try:
        await ws.send_json({"kind": "hello", "payload": {"runs": [
            r["run_id"] for r in await runner.journal.list_runs(10)
        ]}})
        while True:
            await ws.send_json(await queue.get())
    except WebSocketDisconnect:
        pass
    finally:
        ws_queues.discard(queue)


_dist = ROOT / "dashboard" / "dist"
if _dist.exists():
    app.mount("/", StaticFiles(directory=_dist, html=True), name="dashboard")


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
