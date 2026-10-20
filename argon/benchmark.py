"""Fair benchmark: single agent loop versus Argon, same everything.

Both systems get the same 100 tasks, the same tools, the same acceptance
checks, and the same seeded fault plan per task. The only difference is
the runtime structure:

- Baseline: one agent loop running steps sequentially in memory. It
  retries a failed call once. If its process dies, the work is lost,
  which is what happens to a typical in memory agent loop.
- Argon: the DAG scheduler with the journal, role scoped gateway, and
  verifier. After a crash or an uncertain action it gets ONE resume,
  because durable resume is the feature being measured.

A task counts as completed only when the harness's own verifier confirms
every acceptance criterion in the final workspace. Offline runs use the
stub model, so this measures orchestration, not model quality.
"""

from __future__ import annotations

import asyncio
import json
import random
import shutil
import statistics
import time
from pathlib import Path
from typing import Any, Optional

from .agents import Verifier
from .core import ToolFailed
from .orchestrator import Runner
from .tools import build_registry

# ---------------------------------------------------------------------------
# Task suite: 5 templates x 20 variants. Every template moves real data
# through files, so acceptance is checked against computed ground truth.
# ---------------------------------------------------------------------------

def generate_tasks() -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for i in range(20):
        a, b = 3 + i, 5 + 2 * i
        tasks.append({
            "id": f"transform_{i:02d}",
            "title": "Add two inputs and summarise",
            "steps": [
                {"id": "w_a", "tool": "write_file",
                 "args": {"path": "in_a.txt", "content": str(a)}},
                {"id": "w_b", "tool": "write_file",
                 "args": {"path": "in_b.txt", "content": str(b)}},
                {"id": "tf", "tool": "run_python",
                 "args": {"code": "a=int(open('in_a.txt').read());"
                                  "b=int(open('in_b.txt').read());"
                                  "open('result.txt','w').write(str(a+b))"},
                 "depends_on": ["w_a", "w_b"]},
                {"id": "sum", "tool": "write_file",
                 "args": {"path": "summary.txt", "content": f"total={a+b}"},
                 "depends_on": ["tf"]},
            ],
            "acceptance": [
                {"type": "file_equals", "path": "result.txt",
                 "content": str(a + b)},
                {"type": "file_contains", "path": "summary.txt",
                 "text": f"total={a+b}"},
            ],
        })

        x, y = 10 + i, 7 + i
        tasks.append({
            "id": f"ledger_{i:02d}",
            "title": "Append two entries and total them",
            "steps": [
                {"id": "e1", "tool": "append_line",
                 "args": {"path": "log.txt", "line": f"entry {x}"}},
                {"id": "e2", "tool": "append_line",
                 "args": {"path": "log.txt", "line": f"entry {y}"},
                 "depends_on": ["e1"]},
                {"id": "tot", "tool": "run_python",
                 "args": {"code": "s=sum(int(l.split()[1]) for l in "
                                  "open('log.txt') if l.strip());"
                                  "open('total.txt','w').write(str(s))"},
                 "depends_on": ["e2"]},
            ],
            "acceptance": [
                {"type": "file_line_count", "path": "log.txt", "count": 2},
                {"type": "file_equals", "path": "total.txt",
                 "content": str(x + y)},
            ],
        })

        parts = [i + 1, 2 * i + 3, 5 + i, 11]
        steps = [
            {"id": f"w{k}", "tool": "write_file",
             "args": {"path": f"part{k}.txt", "content": str(v)}}
            for k, v in enumerate(parts)
        ]
        steps.append({
            "id": "agg", "tool": "run_python",
            "args": {"code": "s=sum(int(open(f'part{k}.txt').read()) "
                             "for k in range(4));"
                             "open('combined.txt','w').write(str(s))"},
            "depends_on": [f"w{k}" for k in range(4)],
        })
        tasks.append({
            "id": f"fanout_{i:02d}",
            "title": "Write four parts in parallel and combine",
            "steps": steps,
            "acceptance": [
                {"type": "file_equals", "path": "combined.txt",
                 "content": str(sum(parts))},
            ],
        })

        cats = {"books": 2 + i, "games": 3 + i}
        data = "\n".join(f"{c}:{n}" for c, n in cats.items())
        tasks.append({
            "id": f"report_{i:02d}",
            "title": "Aggregate a small sales file and audit it",
            "steps": [
                {"id": "w", "tool": "write_file",
                 "args": {"path": "data.txt", "content": data}},
                {"id": "agg", "tool": "run_python",
                 "args": {"code": "d={};"
                                  "[d.__setitem__(l.split(':')[0], "
                                  "d.get(l.split(':')[0],0)+int(l.split(':')[1])) "
                                  "for l in open('data.txt') if l.strip()];"
                                  "open('report.txt','w').write("
                                  "'\\n'.join(f'{k} {v}' for k,v in d.items()))"},
                 "depends_on": ["w"]},
                {"id": "foot", "tool": "append_line",
                 "args": {"path": "audit.log", "line": "report generated"},
                 "depends_on": ["agg"]},
            ],
            "acceptance": [
                {"type": "file_contains", "path": "report.txt",
                 "text": f"books {cats['books']}"},
                {"type": "file_line_count", "path": "audit.log", "count": 1},
            ],
        })

        n = 4 + i
        tasks.append({
            "id": f"chain_{i:02d}",
            "title": "Two stage computation chain",
            "steps": [
                {"id": "s1", "tool": "write_file",
                 "args": {"path": "start.txt", "content": str(n)}},
                {"id": "s2", "tool": "run_python",
                 "args": {"code": "v=int(open('start.txt').read());"
                                  "open('mid.txt','w').write(str(v*2))"},
                 "depends_on": ["s1"]},
                {"id": "s3", "tool": "run_python",
                 "args": {"code": "v=int(open('mid.txt').read());"
                                  "open('final.txt','w').write(str(v+1))"},
                 "depends_on": ["s2"]},
            ],
            "acceptance": [
                {"type": "file_equals", "path": "mid.txt",
                 "content": str(2 * n)},
                {"type": "file_equals", "path": "final.txt",
                 "content": str(2 * n + 1)},
            ],
        })
    return tasks


def fault_plan_for(task: dict[str, Any], index: int) -> Optional[dict[str, Any]]:
    """One seeded fault plan per task, shared by both systems."""
    rng = random.Random(10_000 + index)
    tools = {s["tool"] for s in task["steps"]}
    side_effects = [t for t in ("append_line", "write_file") if t in tools]
    roll = rng.random()
    if roll < 0.40:
        return None
    if roll < 0.70:
        tool = rng.choice(sorted(tools))
        return {"kind": "transient",
                "plan": {"fail_times": {tool: rng.choice([1, 2])}}}
    if roll < 0.88:
        tool = rng.choice(side_effects)
        return {"kind": "uncertain", "plan": {"uncertain_after": {tool: 1}}}
    tool = rng.choice(side_effects)
    return {"kind": "crash",
            "plan": {"crash_on": {tool: 1,
                                  "after_side_effect": rng.random() < 0.5}}}


# ---------------------------------------------------------------------------
# Baseline: a single agent loop, sequential, in memory
# ---------------------------------------------------------------------------

class _BaselineCrash(Exception):
    pass


class _BaselineUncertain(Exception):
    pass


def _topo_order(task: dict[str, Any]) -> list[dict[str, Any]]:
    steps = {s["id"]: s for s in task["steps"]}
    order, seen = [], set()

    def visit(step):
        if step["id"] in seen:
            return
        for dep in step.get("depends_on", []):
            visit(steps[dep])
        seen.add(step["id"])
        order.append(step)

    for s in task["steps"]:
        visit(s)
    return order


async def run_baseline(
    task: dict[str, Any], workspace: Path, fault: Optional[dict[str, Any]]
) -> bool:
    registry = build_registry(workspace)
    plan = (fault or {}).get("plan") or {}
    fail_times = plan.get("fail_times") or {}
    uncertain_after = plan.get("uncertain_after") or {}
    crash_on = plan.get("crash_on") or {}
    seen: dict[str, int] = {}

    async def call(tool: str, args: dict[str, Any]) -> Any:
        seen[tool] = seen.get(tool, 0) + 1
        n = seen[tool]
        if n <= fail_times.get(tool, 0):
            raise ToolFailed(f"injected transient failure for {tool}")
        if crash_on.get(tool) == n:
            if crash_on.get("after_side_effect"):
                await registry.call("executor", tool, args)
            raise _BaselineCrash()
        if n <= uncertain_after.get(tool, 0):
            await registry.call("executor", tool, args)
            raise _BaselineUncertain()
        return await registry.call("executor", tool, args)

    try:
        for step in _topo_order(task):
            attempts = 0
            while True:
                attempts += 1
                try:
                    await call(step["tool"], step.get("args", {}))
                    break
                except ToolFailed:
                    if attempts >= 2:  # one retry, then the step fails
                        raise
                except _BaselineUncertain:
                    if attempts >= 2:
                        raise
                    # A typical loop cannot tell whether the call landed,
                    # so it just tries again, duplicates included.
    except (_BaselineCrash, _BaselineUncertain, ToolFailed, Exception):
        return False
    ok, _ = await Verifier(registry).verify(task["acceptance"])
    return ok


# ---------------------------------------------------------------------------
# Argon side
# ---------------------------------------------------------------------------

async def run_argon(
    task: dict[str, Any], workspace: Path, journal_path: Path,
    fault: Optional[dict[str, Any]],
) -> tuple[bool, bool]:
    """Returns (completed, needed_resume)."""
    runner = Runner(journal_path, workspace)
    try:
        report = await runner.run_task(
            task, faults=(fault or {}).get("plan")
        )
        needed_resume = report["status"] in ("crashed", "failed")
        if needed_resume:
            report = await runner.resume_task(task, report["run_id"])
        registry = build_registry(workspace)
        ok, _ = await Verifier(registry).verify(task["acceptance"])
        return ok and report["status"] == "succeeded", needed_resume
    finally:
        runner.close()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

async def run_benchmark(root: Path) -> dict[str, Any]:
    if root.exists():
        shutil.rmtree(root)
    tasks = generate_tasks()
    results = []
    argon_times, baseline_times = [], []
    argon_done = baseline_done = resumes = 0
    kinds: dict[str, int] = {}

    for index, task in enumerate(tasks):
        fault = fault_plan_for(task, index)
        kinds[(fault or {}).get("kind", "none")] = \
            kinds.get((fault or {}).get("kind", "none"), 0) + 1

        ws_b = root / "baseline" / task["id"]
        start = time.perf_counter()
        b_ok = await run_baseline(task, ws_b, fault)
        b_s = time.perf_counter() - start

        ws_a = root / "argon" / task["id"]
        start = time.perf_counter()
        a_ok, resumed = await run_argon(
            task, ws_a, root / "argon-journal.db", fault
        )
        a_s = time.perf_counter() - start

        argon_done += a_ok
        baseline_done += b_ok
        resumes += resumed
        if a_ok:
            argon_times.append(a_s)
        if b_ok:
            baseline_times.append(b_s)
        results.append({
            "task": task["id"], "fault": (fault or {}).get("kind", "none"),
            "argon": a_ok, "baseline": b_ok,
            "argon_s": round(a_s, 3), "baseline_s": round(b_s, 3),
            "resumed": resumed,
        })
        print(f"[{index + 1:3d}/100] {task['id']:<14s} "
              f"fault={results[-1]['fault']:<9s} "
              f"argon={'OK ' if a_ok else 'FAIL'} "
              f"baseline={'OK ' if b_ok else 'FAIL'}", flush=True)

    summary = {
        "tasks": len(tasks),
        "model": "stub (offline, deterministic)",
        "fault_mix": kinds,
        "argon": {
            "completed": argon_done,
            "median_latency_s": round(statistics.median(argon_times), 3),
            "mean_latency_s": round(statistics.mean(argon_times), 3),
            "runs_needing_resume": resumes,
        },
        "baseline": {
            "completed": baseline_done,
            "median_latency_s": round(statistics.median(baseline_times), 3),
            "mean_latency_s": round(statistics.mean(baseline_times), 3),
        },
        "results": results,
    }
    return summary


def main() -> None:
    root = Path("runs/benchmark")
    summary = asyncio.run(run_benchmark(root))
    out = Path("benchmarks/results.json")
    out.write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "results"},
                     indent=2))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
