"""API tests: examples, a full run over HTTP, crash guard, the lab
endpoint, and benchmark summaries."""

import time

import pytest
from fastapi.testclient import TestClient

from argon.server import app

client = TestClient(app)


def payment_task():
    tasks = client.get("/api/examples").json()
    return next(t for t in tasks if t["id"] == "payment_demo")


def wait_for_report(run_id: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f"/api/runs/{run_id}").json()
        if data.get("report"):
            return data
        time.sleep(0.2)
    raise AssertionError("run did not finish in time")


def test_examples_include_runnable_tasks():
    tasks = client.get("/api/examples").json()
    assert len(tasks) >= 5
    assert all("steps" in t and "acceptance" in t for t in tasks)


def test_run_completes_and_verifies_over_http():
    resp = client.post("/api/runs", json={"task": payment_task()})
    assert resp.status_code == 200
    data = wait_for_report(resp.json()["run_id"])
    assert data["report"]["status"] == "succeeded"
    assert data["report"]["verified"] is True
    tools = {a["tool"] for a in data["actions"]}
    assert tools == {"append_line", "write_file"}


def test_uncertain_run_then_resume_over_http():
    resp = client.post("/api/runs", json={
        "task": payment_task(),
        "faults": {"uncertain_after": {"append_line": 1}},
    })
    run_id = resp.json()["run_id"]
    data = wait_for_report(run_id)
    assert data["report"]["status"] == "failed"

    resp = client.post(f"/api/runs/{run_id}/resume")
    assert resp.status_code == 200
    data = wait_for_report(run_id)
    assert data["report"]["status"] == "succeeded"
    assert data["report"]["verified"] is True


def test_crash_faults_are_refused_on_the_live_runner():
    resp = client.post("/api/runs", json={
        "task": payment_task(),
        "faults": {"crash_on": {"append_line": 1}},
    })
    assert resp.status_code == 400


def test_lab_endpoint_runs_a_scenario_in_isolation():
    resp = client.post("/api/lab/transient_storm", params={"index": 0})
    assert resp.status_code == 200
    body = resp.json()
    assert body["family"] == "transient_storm"
    assert body["passed"] is True


def test_lab_endpoint_rejects_unknown_family():
    resp = client.post("/api/lab/not_a_family")
    assert resp.status_code == 404


def test_benchmarks_endpoint_serves_summaries():
    body = client.get("/api/benchmarks").json()
    assert body["results"]["argon"]["completed"] == 100
    assert body["failure-lab-results"]["passed"] == 50
    assert "results" not in body["results"]  # per task detail stays on disk
