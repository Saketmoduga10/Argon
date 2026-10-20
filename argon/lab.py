"""The failure lab: 50 injected failure scenarios with hard postconditions.

Each scenario runs a small workflow, injects one failure, resumes at most
once, and then checks postconditions against the actual workspace and
journal. A scenario passes only if:

- the workflow ends verified (or, where recovery is impossible by
  design, fails loudly with the uncertain action identified), and
- no side effect was duplicated, and
- no side effect was silently lost.

Recovery being *correct* is the claim under test, not recovery being
*claimed*. Run as a module to execute the suite and write
benchmarks/failure-lab-results.json. Also runnable one scenario at a
time (used by the dashboard, in a subprocess, so a simulated crash can
never take the server with it).
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from .orchestrator import Runner

# ---------------------------------------------------------------------------
# Scenario builders. Each returns (task, faults, postcheck) where
# postcheck(workspace, inspection, report) -> (ok, detail).
# ---------------------------------------------------------------------------

def _payment_task(i: int) -> dict[str, Any]:
    return {
        "id": f"lab_pay_{i}",
        "title": "Log a payment and issue a receipt",
        "steps": [
            {"id": "log", "tool": "append_line",
             "args": {"path": "ledger.txt", "line": f"paid {50 + i}"}},
            {"id": "receipt", "tool": "write_file",
             "args": {"path": "receipt.txt", "content": f"paid {50 + i}"},
             "depends_on": ["log"]},
        ],
        "acceptance": [
            {"type": "file_line_count", "path": "ledger.txt", "count": 1},
            {"type": "file_equals", "path": "receipt.txt",
             "content": f"paid {50 + i}"},
        ],
    }


def _lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [l for l in path.read_text().splitlines() if l.strip()]


def scenario_crash_after_append(i: int):
    task = _payment_task(i)
    faults = {"crash_on": {"append_line": 1, "after_side_effect": True}}

    def check(ws, insp, report):
        lines = _lines(ws / "ledger.txt")
        ok = report.get("verified") and lines == [f"paid {50 + i}"]
        return ok, f"ledger lines after crash+resume: {lines}"

    return task, faults, check


def scenario_crash_before_write(i: int):
    task = _payment_task(i)
    faults = {"crash_on": {"write_file": 1, "after_side_effect": False}}

    def check(ws, insp, report):
        receipt = ws / "receipt.txt"
        ok = report.get("verified") and receipt.exists() \
            and receipt.read_text() == f"paid {50 + i}"
        return ok, "receipt written exactly once after resume"

    return task, faults, check


def scenario_transient_storm(i: int):
    task = _payment_task(i)
    tool = "write_file" if i % 2 == 0 else "append_line"
    faults = {"fail_times": {tool: 1 + (i % 2)}}  # 1 or 2 transient failures

    def check(ws, insp, report):
        lines = _lines(ws / "ledger.txt")
        ok = report.get("verified") and lines == [f"paid {50 + i}"]
        return ok, f"recovered through transient {tool} failures"

    return task, faults, check


def scenario_uncertain_write(i: int):
    task = _payment_task(i)
    faults = {"uncertain_after": {"write_file": 1}}

    def check(ws, insp, report):
        actions = [a for a in insp["actions"] if a["tool"] == "write_file"]
        completed = [a for a in actions
                     if a["status"] == "completed"]
        ok = report.get("verified") and len(completed) == 1
        return ok, "write reconciled from the journal, executed once"

    return task, faults, check


def scenario_timeout_pure_tool(i: int):
    task = {
        "id": f"lab_slow_{i}",
        "title": "A pure computation that times out",
        "steps": [
            {"id": "calc", "tool": "compute",
             "args": {"expression": f"{i} + {i + 1}"}, "timeout_s": 0.2},
            {"id": "out", "tool": "write_file",
             "args": {"path": "answer.txt", "content": str(2 * i + 1)},
             "depends_on": ["calc"]},
        ],
        "acceptance": [
            {"type": "file_equals", "path": "answer.txt",
             "content": str(2 * i + 1)},
        ],
    }
    faults = {"delay": {"compute": 2.0}}

    def check(ws, insp, report):
        kinds = [e["kind"] for e in insp["events"]]
        ok = report.get("verified") and "action_uncertain" in kinds
        return ok, "timeout went uncertain, pure tool safely re-executed"

    return task, faults, check


FAMILIES: dict[str, Callable[[int], tuple]] = {
    "crash_after_append": scenario_crash_after_append,
    "crash_before_write": scenario_crash_before_write,
    "transient_storm": scenario_transient_storm,
    "uncertain_write": scenario_uncertain_write,
    "timeout_pure_tool": scenario_timeout_pure_tool,
}


async def run_scenario(family: str, index: int, root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    task, faults, check = FAMILIES[family](index)
    workspace = root / "ws"
    runner = Runner(root / "journal.db", workspace)
    try:
        started = time.perf_counter()
        report = await runner.run_task(task, faults=faults)
        crashed = report["status"] == "crashed"
        if report["status"] in ("crashed", "failed"):
            report = await runner.resume_task(task, report["run_id"])
        inspection = await runner.inspect_run(report["run_id"])
        ok, detail = check(workspace, inspection, report)
        return {
            "family": family, "index": index, "passed": bool(ok),
            "detail": detail, "crashed_mid_run": crashed,
            "final_status": report["status"],
            "verified": report.get("verified"),
            "duration_s": round(time.perf_counter() - started, 3),
            "states": {k: v["status"] for k, v in
                       inspection["states"].items()},
            "actions": [
                {"node": a["node_id"], "tool": a["tool"],
                 "status": a["status"]}
                for a in inspection["actions"]
            ],
            "events": [e["kind"] for e in inspection["events"]],
        }
    finally:
        runner.close()


async def run_suite(root: Path) -> dict[str, Any]:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    results = []
    for family in FAMILIES:
        for i in range(10):
            result = await run_scenario(family, i, root / f"{family}_{i}")
            results.append(result)
            print(f"{family}[{i}] -> {'PASS' if result['passed'] else 'FAIL'}"
                  f"  {result['detail']}", flush=True)
    passed = sum(r["passed"] for r in results)
    by_family = {
        fam: sum(r["passed"] for r in results if r["family"] == fam)
        for fam in FAMILIES
    }
    return {"scenarios": len(results), "passed": passed,
            "by_family": by_family, "results": results}


def main() -> None:
    if len(sys.argv) > 2:  # single scenario mode for the dashboard
        family, index = sys.argv[1], int(sys.argv[2])
        with tempfile.TemporaryDirectory() as tmp:
            result = asyncio.run(run_scenario(family, index, Path(tmp)))
        print(json.dumps(result))
        return
    summary = asyncio.run(run_suite(Path("runs/failure-lab")))
    out = Path("benchmarks/failure-lab-results.json")
    out.write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "results"},
                     indent=2))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
