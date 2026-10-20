"""Argon command line.

    python -m argon serve                 API server and dashboard
    python -m argon benchmark             the 100 task benchmark
    python -m argon lab                   all 50 failure lab scenarios
    python -m argon lab crash_after_append 0
                                          one scenario (JSON to stdout)
    python -m argon run task.json [faults.json]
                                          run a task, resume once if it
                                          does not succeed, print a report
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

USAGE = __doc__


async def _run_file(rest: list[str]) -> None:
    from .orchestrator import Runner

    if not rest:
        raise SystemExit("usage: python -m argon run TASK.json [FAULTS.json]")
    task = json.loads(Path(rest[0]).read_text())
    faults = json.loads(Path(rest[1]).read_text()) if len(rest) > 1 else None
    runner = Runner("runs/cli/journal.db", "runs/cli/ws")
    report = await runner.run_task(task, faults=faults)
    if report["status"] != "succeeded":
        report = await runner.resume_task(task, report["run_id"])
    print(json.dumps(
        {k: report[k] for k in ("run_id", "status", "verified", "duration_s")},
        indent=2,
    ))
    runner.close()


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print(USAGE)
        return
    cmd, rest = args[0], args[1:]
    if cmd == "serve":
        from .server import main as serve

        serve()
    elif cmd == "benchmark":
        sys.argv = ["argon.benchmark", *rest]
        from .benchmark import main as bench

        bench()
    elif cmd == "lab":
        sys.argv = ["argon.lab", *rest]
        from .lab import main as lab

        lab()
    elif cmd == "run":
        asyncio.run(_run_file(rest))
    else:
        raise SystemExit(f"unknown command: {cmd}\n{USAGE}")


if __name__ == "__main__":
    main()
