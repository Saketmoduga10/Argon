"""Argon core: durable DAG scheduling and the tool action journal.

Two ideas carry the whole system:

1. Every tool call an agent makes is written to a journal BEFORE it runs,
   with an idempotency key. The journal distinguishes three outcomes:
   completed (the side effect definitely happened), failed (it definitely
   did not), and uncertain (the process died or timed out mid call, so the
   side effect may or may not have landed). Recovery never blindly repeats
   an uncertain action; it reconciles first.

2. The scheduler checkpoints every node transition to SQLite, so a workflow
   run survives a process crash. Resuming a run skips finished nodes and
   reconciles interrupted ones instead of starting over.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional


# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------

class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNCERTAIN = "uncertain"
    SKIPPED = "skipped"


class ActionStatus(str, Enum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


@dataclass
class ToolCall:
    tool: str
    args: dict[str, Any] = field(default_factory=dict)
    # Side effects are made idempotent by keying them: the same node, tool,
    # and arguments always map to the same key, so a replayed call is
    # recognised instead of executed twice.
    idempotency_key: str = ""

    def __post_init__(self) -> None:
        if not self.idempotency_key:
            raw = self.tool + "|" + json.dumps(self.args, sort_keys=True, default=str)
            self.idempotency_key = hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass
class Node:
    id: str
    title: str
    role: str  # which agent role owns this node; drives tool permissions
    calls: list[ToolCall] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    timeout_s: float = 30.0
    max_retries: int = 2  # retries apply to FAILED actions only, never UNCERTAIN
    acceptance: list[str] = field(default_factory=list)  # verifier criteria


@dataclass
class Workflow:
    id: str
    title: str
    nodes: list[Node]

    def validate(self) -> None:
        ids = [n.id for n in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node id")
        known = set(ids)
        for n in self.nodes:
            for dep in n.depends_on:
                if dep not in known:
                    raise ValueError(f"node {n.id} depends on unknown node {dep}")
        # Cycle check via DFS colouring.
        color: dict[str, int] = {i: 0 for i in ids}
        by_id = {n.id: n for n in self.nodes}

        def visit(nid: str) -> None:
            color[nid] = 1
            for dep in by_id[nid].depends_on:
                if color[dep] == 1:
                    raise ValueError(f"dependency cycle at {dep}")
                if color[dep] == 0:
                    visit(dep)
            color[nid] = 2

        for i in ids:
            if color[i] == 0:
                visit(i)

    def by_id(self) -> dict[str, Node]:
        return {n.id: n for n in self.nodes}


# --------------------------------------------------------------------------
# Journal: the durable record of runs, node states, and tool actions
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    finished_at REAL
);
CREATE TABLE IF NOT EXISTS node_states (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (run_id, node_id)
);
CREATE TABLE IF NOT EXISTS actions (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    tool TEXT NOT NULL,
    args_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    error TEXT,
    started_at REAL,
    finished_at REAL,
    PRIMARY KEY (run_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL,
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    at REAL NOT NULL
);
"""


class Journal:
    """SQLite backed journal. One connection, short transactions, so a
    crash can only ever lose the action that was in flight, and that action
    is exactly the one marked started (later reconciled as uncertain)."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # WAL with NORMAL sync: commits stop paying a full fsync each,
        # which dominates end to end latency when every action and event
        # is its own transaction. A process crash (what the failure lab
        # injects) loses nothing; only an OS or power failure can drop
        # the last few transactions, the standard SQLite tradeoff.
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self.db.commit()
        self._lock = asyncio.Lock()

    def close(self) -> None:
        self.db.close()

    # -- runs -------------------------------------------------------------
    async def start_run(self, workflow: Workflow) -> str:
        run_id = uuid.uuid4().hex[:12]
        async with self._lock:
            self.db.execute(
                "INSERT INTO runs VALUES (?,?,?,?,?,?)",
                (run_id, workflow.id, workflow.title, "running", time.time(), None),
            )
            for n in workflow.nodes:
                self.db.execute(
                    "INSERT INTO node_states VALUES (?,?,?,0,NULL,?)",
                    (run_id, n.id, NodeStatus.PENDING.value, time.time()),
                )
            self.db.commit()
        return run_id

    async def finish_run(self, run_id: str, status: str) -> None:
        async with self._lock:
            self.db.execute(
                "UPDATE runs SET status=?, finished_at=? WHERE run_id=?",
                (status, time.time(), run_id),
            )
            self.db.commit()

    async def set_node_state(
        self, run_id: str, node_id: str, status: NodeStatus,
        error: Optional[str] = None, bump_attempt: bool = False,
    ) -> None:
        async with self._lock:
            self.db.execute(
                """UPDATE node_states
                   SET status=?, error=?, updated_at=?,
                       attempts = attempts + ?
                   WHERE run_id=? AND node_id=?""",
                (status.value, error, time.time(), 1 if bump_attempt else 0, run_id, node_id),
            )
            self.db.commit()

    async def node_states(self, run_id: str) -> dict[str, dict[str, Any]]:
        async with self._lock:
            rows = self.db.execute(
                "SELECT * FROM node_states WHERE run_id=?", (run_id,)
            ).fetchall()
        return {r["node_id"]: dict(r) for r in rows}

    # -- actions ----------------------------------------------------------
    async def begin_action(
        self, run_id: str, node_id: str, seq: int, call: ToolCall,
        skey: str = "",
    ) -> tuple[Optional[dict[str, Any]], bool]:
        """Register an action before it executes.

        Returns (existing_record, should_execute). If the journal already
        holds this idempotency key as COMPLETED, the caller must NOT execute
        again; it replays the stored result instead. If it holds it as
        STARTED from a previous life, the action is UNCERTAIN and the caller
        must reconcile rather than re-execute.
        """
        async with self._lock:
            row = self.db.execute(
                "SELECT * FROM actions WHERE run_id=? AND idempotency_key=?",
                (run_id, skey),
            ).fetchone()
            if row is not None:
                rec = dict(row)
                if rec["status"] == ActionStatus.STARTED.value:
                    self.db.execute(
                        "UPDATE actions SET status=? WHERE run_id=? AND idempotency_key=?",
                        (ActionStatus.UNCERTAIN.value, run_id, skey),
                    )
                    self.db.commit()
                    rec["status"] = ActionStatus.UNCERTAIN.value
                return rec, False
            self.db.execute(
                """INSERT INTO actions
                   (run_id, node_id, seq, tool, args_json, idempotency_key,
                    status, result_json, error, started_at, finished_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, node_id, seq, call.tool,
                    json.dumps(call.args, default=str), skey,
                    ActionStatus.STARTED.value, None, None, time.time(), None,
                ),
            )
            self.db.commit()
            return None, True

    async def complete_action(
        self, run_id: str, key: str, result: Any
    ) -> None:
        async with self._lock:
            self.db.execute(
                """UPDATE actions SET status=?, result_json=?, finished_at=?
                   WHERE run_id=? AND idempotency_key=?""",
                (ActionStatus.COMPLETED.value, json.dumps(result, default=str),
                 time.time(), run_id, key),
            )
            self.db.commit()

    async def fail_action(self, run_id: str, key: str, error: str, uncertain: bool) -> None:
        async with self._lock:
            self.db.execute(
                """UPDATE actions SET status=?, error=?, finished_at=?
                   WHERE run_id=? AND idempotency_key=?""",
                (
                    ActionStatus.UNCERTAIN.value if uncertain else ActionStatus.FAILED.value,
                    error, time.time(), run_id, key,
                ),
            )
            self.db.commit()

    async def actions(self, run_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self.db.execute(
                "SELECT * FROM actions WHERE run_id=? ORDER BY started_at, seq",
                (run_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # -- events ------------------------------------------------------------
    async def emit(self, run_id: str, kind: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            self.db.execute(
                "INSERT INTO events (run_id, kind, payload_json, at) VALUES (?,?,?,?)",
                (run_id, kind, json.dumps(payload, default=str), time.time()),
            )
            self.db.commit()

    async def events(self, run_id: str, after_seq: int = 0) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self.db.execute(
                "SELECT * FROM events WHERE run_id=? AND seq>? ORDER BY seq",
                (run_id, after_seq),
            ).fetchall()
        return [
            {"seq": r["seq"], "kind": r["kind"],
             "payload": json.loads(r["payload_json"]), "at": r["at"]}
            for r in rows
        ]


# --------------------------------------------------------------------------
# Scheduler
# --------------------------------------------------------------------------

# An executor performs ONE tool call for a node and returns its result.
# It may raise ToolFailed (side effect definitely did not happen) or
# ToolUncertain (the call timed out or the process was interrupted, so the
# side effect is unknown). Any other exception is treated as UNCERTAIN,
# because once a call is in flight a crash tells us nothing about whether
# it landed.
class ToolFailed(Exception):
    pass


class ToolUncertain(Exception):
    pass


ExecuteFn = Callable[[Node, ToolCall], Awaitable[Any]]
# A reconciler inspects an UNCERTAIN action and reports whether its side
# effect landed: returns the action's result if it did, raises ToolFailed
# if it definitely did not, raises ToolUncertain if still unknowable.
ReconcileFn = Callable[[Node, ToolCall, dict[str, Any]], Awaitable[Any]]
EventFn = Callable[[str, dict[str, Any]], Awaitable[None]]


class Scheduler:
    def __init__(
        self,
        journal: Journal,
        execute: ExecuteFn,
        reconcile: Optional[ReconcileFn] = None,
        on_event: Optional[EventFn] = None,
        max_parallel: int = 8,
    ):
        self.journal = journal
        self.execute = execute
        self.reconcile = reconcile
        self.on_event = on_event
        self.sem = asyncio.Semaphore(max_parallel)

    async def _event(self, run_id: str, kind: str, payload: dict[str, Any]) -> None:
        await self.journal.emit(run_id, kind, payload)
        if self.on_event:
            await self.on_event(kind, {"run_id": run_id, **payload})

    async def run(self, workflow: Workflow, run_id: Optional[str] = None) -> dict[str, Any]:
        """Run (or resume) a workflow. Safe to call again after a crash:
        succeeded nodes are skipped, interrupted nodes are reconciled."""
        workflow.validate()
        if run_id is None:
            run_id = await self.journal.start_run(workflow)
            await self._event(run_id, "run_started", {"title": workflow.title})
        else:
            prior = await self.journal.events(run_id)
            kind = "run_resumed" if prior else "run_started"
            await self._event(run_id, kind, {"title": workflow.title})

        by_id = workflow.by_id()
        states = await self.journal.node_states(run_id)
        results: dict[str, list[Any]] = {}
        pending = {
            nid for nid, s in states.items()
            if s["status"] not in (NodeStatus.SUCCEEDED.value,)
        }
        # Nodes left RUNNING by a dead process are interrupted, not done.
        for nid in list(pending):
            if states[nid]["status"] == NodeStatus.RUNNING.value:
                await self.journal.set_node_state(run_id, nid, NodeStatus.UNCERTAIN)
            # A node SKIPPED in a previous life was cut off because a
            # dependency failed then. This life may repair the dependency,
            # so the node is revived and judged on this run's outcome.
            if states[nid]["status"] == NodeStatus.SKIPPED.value:
                await self.journal.set_node_state(run_id, nid, NodeStatus.PENDING)

        running: dict[str, asyncio.Task] = {}
        while pending or running:
            progress = False
            for nid in list(pending):
                node = by_id[nid]
                deps = node.depends_on
                dep_states = await self.journal.node_states(run_id)
                if all(dep_states[d]["status"] == NodeStatus.SUCCEEDED.value for d in deps):
                    pending.discard(nid)
                    running[nid] = asyncio.create_task(
                        self._run_node(run_id, node, results)
                    )
                    progress = True
                elif any(
                    dep_states[d]["status"] in (
                        NodeStatus.FAILED.value, NodeStatus.UNCERTAIN.value,
                        NodeStatus.SKIPPED.value,
                    )
                    and d not in pending and d not in running
                    for d in deps
                ):
                    # A dependency settled badly in THIS run, so this node
                    # can never run. A dependency still pending or running
                    # (for example, being reconciled on a resume) does not
                    # settle anything yet.
                    pending.discard(nid)
                    await self.journal.set_node_state(run_id, nid, NodeStatus.SKIPPED)
                    await self._event(run_id, "node_skipped", {"node": nid})
                    progress = True
            if not running:
                if not pending:
                    break
                if not progress:
                    # A full pass started and settled nothing while nodes
                    # remain: they can genuinely never run. (A pass that
                    # skipped a node must loop again, because that skip can
                    # settle a dependent evaluated earlier in the pass.)
                    raise RuntimeError("deadlock: pending nodes can never run")
                continue
            done, _ = await asyncio.wait(
                running.values(), return_when=asyncio.FIRST_COMPLETED
            )
            for nid in [n for n, t in running.items() if t in done]:
                running.pop(nid)

        final = await self.journal.node_states(run_id)
        bad = [
            nid for nid, s in final.items()
            if s["status"] in (NodeStatus.FAILED.value, NodeStatus.UNCERTAIN.value)
        ]
        status = "succeeded" if not bad else "failed"
        await self.journal.finish_run(run_id, status)
        await self._event(run_id, "run_finished", {"status": status})
        return {"run_id": run_id, "status": status, "states": final, "results": results}

    async def _run_node(
        self, run_id: str, node: Node, results: dict[str, list[Any]]
    ) -> None:
        async with self.sem:
            await self.journal.set_node_state(
                run_id, node.id, NodeStatus.RUNNING, bump_attempt=True
            )
            await self._event(run_id, "node_started", {"node": node.id, "role": node.role})
            node_results: list[Any] = []
            try:
                for seq, call in enumerate(node.calls):
                    value = await self._run_action(run_id, node, seq, call)
                    node_results.append(value)
            except _NodeUncertain as exc:
                await self.journal.set_node_state(
                    run_id, node.id, NodeStatus.UNCERTAIN, error=str(exc)
                )
                await self._event(
                    run_id, "node_uncertain", {"node": node.id, "error": str(exc)}
                )
                return
            except Exception as exc:  # exhausted retries on a FAILED action
                await self.journal.set_node_state(
                    run_id, node.id, NodeStatus.FAILED, error=str(exc)
                )
                await self._event(
                    run_id, "node_failed", {"node": node.id, "error": str(exc)}
                )
                return
            results[node.id] = node_results
            await self.journal.set_node_state(run_id, node.id, NodeStatus.SUCCEEDED)
            await self._event(run_id, "node_succeeded", {"node": node.id})

    async def _run_action(
        self, run_id: str, node: Node, seq: int, call: ToolCall
    ) -> Any:
        # The journal key scopes the call's content key to this node and
        # position, so two nodes making the same call stay distinct while
        # a replay of THIS node's call is still recognised.
        skey = f"{node.id}:{seq}:{call.idempotency_key}"
        existing, should_execute = await self.journal.begin_action(
            run_id, node.id, seq, call, skey
        )
        if not should_execute:
            assert existing is not None
            if existing["status"] == ActionStatus.COMPLETED.value:
                await self._event(run_id, "action_replayed", {
                    "node": node.id, "tool": call.tool,
                    "key": call.idempotency_key,
                })
                return json.loads(existing["result_json"]) if existing["result_json"] else None
            # UNCERTAIN: never re-execute blindly. Reconcile if we can.
            if self.reconcile is None:
                raise _NodeUncertain(
                    f"action {call.tool} ({call.idempotency_key}) is uncertain "
                    "and no reconciler is configured"
                )
            try:
                value = await self.reconcile(node, call, existing)
            except ToolFailed:
                # The reconciler proved the side effect never landed, so
                # executing now is safe. Fall through to real execution.
                await self._event(run_id, "action_reconcile_missed", {
                    "node": node.id, "tool": call.tool,
                    "key": call.idempotency_key,
                })
            else:
                await self.journal.complete_action(run_id, skey, value)
                await self._event(run_id, "action_reconciled", {
                    "node": node.id, "tool": call.tool,
                    "key": call.idempotency_key,
                })
                return value

        await self._event(run_id, "action_started", {
            "node": node.id, "tool": call.tool, "key": call.idempotency_key,
        })
        attempts_left = node.max_retries
        while True:
            try:
                value = await asyncio.wait_for(
                    self.execute(node, call), timeout=node.timeout_s
                )
            except ToolFailed as exc:
                if attempts_left > 0:
                    attempts_left -= 1
                    await self._event(run_id, "action_retry", {
                        "node": node.id, "tool": call.tool, "error": str(exc),
                    })
                    continue
                await self.journal.fail_action(
                    run_id, skey, str(exc), uncertain=False
                )
                raise
            except (ToolUncertain, asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
                # Once a call is in flight, a timeout or crash means the
                # side effect is unknowable. Mark UNCERTAIN, do not retry.
                await self.journal.fail_action(
                    run_id, skey, repr(exc), uncertain=True
                )
                await self._event(run_id, "action_uncertain", {
                    "node": node.id, "tool": call.tool, "error": repr(exc),
                })
                raise _NodeUncertain(
                    f"action {call.tool} outcome uncertain: {exc!r}"
                ) from exc
            await self.journal.complete_action(run_id, skey, value)
            await self._event(run_id, "action_completed", {
                "node": node.id, "tool": call.tool, "key": call.idempotency_key,
            })
            return value


class _NodeUncertain(Exception):
    pass
