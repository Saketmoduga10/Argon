"""Orchestrator: plans a task, runs it on the scheduler, verifies the
result, and (for the failure lab) injects faults along the way.

Failure injection is how Argon's reliability claims get tested instead of
asserted. A fault plan can:

- fail_times: make the first N calls to a tool raise ToolFailed
  (a transient error: nothing ran, a retry is safe);
- uncertain_after: let a call's side effect land, then report the outcome
  as unknown (the classic crash-after-commit window);
- delay: stall a tool call (paired with a node timeout, this produces a
  genuine uncertain action);
- crash_on: kill the whole run mid action, the way a process death
  would. The journal is left exactly as a real crash would leave it:
  in flight actions still STARTED, nodes still RUNNING. Resuming the run
  reconciles from the journal alone.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .agents import Planner, Verifier, default_model
from .core import Journal, Node, Scheduler, ToolCall, ToolFailed, ToolUncertain
from .tools import build_registry, make_executor, make_reconciler

ExecuteFn = Callable[[Node, ToolCall], Any]


class Injector:
    """Wraps a real ExecuteFn and injects the faults in the plan.
    Counts are per tool name and only fire while the plan says so, which
    keeps every scenario deterministic and repeatable."""

    def __init__(self, inner: ExecuteFn, plan: dict[str, Any]):
        self.inner = inner
        self.plan = plan or {}
        self.seen: dict[str, int] = {}
        self.on_crash: Optional[Callable[[], None]] = None
        self.crashed = False

    async def __call__(self, node: Node, call: ToolCall) -> Any:
        tool = call.tool
        self.seen[tool] = self.seen.get(tool, 0) + 1
        n = self.seen[tool]

        delay = (self.plan.get("delay") or {}).get(tool)
        if delay:
            await asyncio.sleep(delay)

        fail_times = (self.plan.get("fail_times") or {}).get(tool, 0)
        if n <= fail_times:
            raise ToolFailed(f"injected transient failure #{n} for {tool}")

        crash = self.plan.get("crash_on") or {}
        if crash.get(tool) == n and not self.crashed:
            self.crashed = True
            if crash.get("after_side_effect"):
                await self.inner(node, call)  # the work lands...
            if self.on_crash:
                self.on_crash()               # ...then the process "dies"
            await asyncio.sleep(3600)          # cancelled by the crash

        uncertain = (self.plan.get("uncertain_after") or {}).get(tool, 0)
        if n <= uncertain:
            await self.inner(node, call)       # side effect lands
            raise ToolUncertain(f"injected: outcome of {tool} call #{n} unknown")

        return await self.inner(node, call)


class Runner:
    """One Argon deployment: a journal, a workspace, a model."""

    def __init__(
        self,
        journal_path: str | Path,
        workspace: str | Path,
        model: Optional[Any] = None,
    ):
        self.journal = Journal(journal_path)
        self.workspace = Path(workspace)
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.model = model or default_model()
        self.listeners: list[Callable[[str, dict], Any]] = []

    def close(self) -> None:
        self.journal.close()

    async def _fanout(self, kind: str, payload: dict[str, Any]) -> None:
        for listener in self.listeners:
            await listener(kind, payload)

    def _scheduler(self, registry, faults) -> tuple[Scheduler, Optional[Injector]]:
        execute = make_executor(registry)
        injector = None
        if faults:
            injector = Injector(execute, faults)
            execute = injector
        scheduler = Scheduler(
            self.journal,
            execute,
            reconcile=make_reconciler(registry),
            on_event=self._fanout,
        )
        return scheduler, injector

    async def run_task(
        self,
        task: dict[str, Any],
        faults: Optional[dict[str, Any]] = None,
        run_id: Optional[str] = None,
        workspace: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Plan, execute, verify. With a crash fault this returns as soon
        as the 'process' dies, with status 'crashed'; call resume_task
        with the same run_id to continue from the journal. Each run gets
        its own workspace (default: a per run directory) so runs never
        collide in the same files; a resume must pass the same workspace
        as the original run."""
        registry = build_registry(workspace or self.workspace)
        workflow = await Planner(self.model).plan(task)
        if run_id is None:
            run_id = await self.journal.start_run(workflow)
        scheduler, injector = self._scheduler(registry, faults)
        if injector is not None:
            injector.on_crash = lambda: current.cancel()  # noqa: F821

        started = time.perf_counter()
        current = asyncio.ensure_future(scheduler.run(workflow, run_id))
        try:
            result = await current
        except asyncio.CancelledError:
            # The injected crash killed the run mid flight. Cancel any
            # sibling node tasks the way an OS reaping a process would,
            # then report the journal's honest mid crash state.
            for other in asyncio.all_tasks():
                if other is not asyncio.current_task() and other is not current:
                    other.cancel()
            await asyncio.gather(
                *[t for t in asyncio.all_tasks()
                  if t is not asyncio.current_task()],
                return_exceptions=True,
            )
            states = await self.journal.node_states(run_id)
            return {
                "run_id": run_id, "status": "crashed",
                "duration_s": time.perf_counter() - started,
                "states": states, "verified": None, "checks": [],
            }

        verified, checks = await Verifier(registry).verify(
            task.get("acceptance", [])
        )
        return {
            "run_id": run_id,
            "status": result["status"],
            "duration_s": time.perf_counter() - started,
            "states": result["states"],
            "verified": verified,
            "checks": [
                {"ok": c.ok, "criterion": c.criterion, "evidence": c.evidence}
                for c in checks
            ],
        }

    async def resume_task(
        self, task: dict[str, Any], run_id: str,
        workspace: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Resume a crashed or interrupted run from its journal. No faults
        are injected on a resume: this is the recovery being inspected."""
        return await self.run_task(
            task, faults=None, run_id=run_id, workspace=workspace
        )

    async def inspect_run(self, run_id: str) -> dict[str, Any]:
        """Everything the failure lab shows: node states, the full action
        journal (completed, failed, uncertain), and the event timeline."""
        return {
            "run_id": run_id,
            "states": await self.journal.node_states(run_id),
            "actions": await self.journal.actions(run_id),
            "events": await self.journal.events(run_id),
        }
