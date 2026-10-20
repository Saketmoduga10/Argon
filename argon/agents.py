"""Agents: a planner that turns a task into a DAG, executor agents that
run nodes through the tool gateway, and a verifier that checks evidence.

The model is pluggable. Two clients ship:

- StubModel: a deterministic offline model. It plans from the task's
  structured spec, which is what the benchmark harness generates. Offline
  runs therefore measure the orchestration (scheduling, retries,
  recovery, parallelism), not language understanding.
- OpenAICompatModel: any OpenAI compatible chat endpoint. Set
  ARGON_MODEL_BASE_URL, ARGON_MODEL, and ARGON_API_KEY to run the same
  planner and benchmark against a real model.

The verifier never trusts an agent's word. Every acceptance criterion is
checked with a tool call made under the verifier role (which cannot write
or execute), and the tool result is kept as evidence.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from .core import Node, ToolCall, Workflow
from .tools import ToolRegistry


class ModelClient(Protocol):
    async def plan(self, task: dict[str, Any]) -> dict[str, Any]:
        """Return a plan: {"nodes": [{id, title, role, calls, depends_on}]}"""
        ...


# --------------------------------------------------------------------------
# Deterministic offline model
# --------------------------------------------------------------------------

class StubModel:
    """Plans directly from the task's structured step list. Steps keep
    their declared dependencies, so independent steps become sibling
    nodes the scheduler can run in parallel."""

    async def plan(self, task: dict[str, Any]) -> dict[str, Any]:
        nodes = []
        for step in task["steps"]:
            nodes.append({
                "id": step["id"],
                "title": step.get("title", step["id"]),
                "role": "executor",
                "calls": [{"tool": step["tool"], "args": step.get("args", {})}],
                "depends_on": step.get("depends_on", []),
                "timeout_s": step.get("timeout_s", 30.0),
                "max_retries": step.get("max_retries", 2),
            })
        return {"nodes": nodes}


# --------------------------------------------------------------------------
# OpenAI compatible model
# --------------------------------------------------------------------------

PLAN_PROMPT = """You are the planner in a multi-agent orchestration system.
Break the task into a DAG of tool calls. Reply with JSON only:
{"nodes": [{"id": "...", "title": "...", "role": "executor",
            "calls": [{"tool": "...", "args": {...}}],
            "depends_on": ["..."]}]}
Available tools depend on role; executor tools: write_file, append_line,
read_file, list_files, check_file, compute, run_python.
Task: {task}
"""


class OpenAICompatModel:
    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.base_url = base_url or os.environ["ARGON_MODEL_BASE_URL"]
        self.model = model or os.environ["ARGON_MODEL"]
        self.api_key = api_key or os.environ.get("ARGON_API_KEY", "")

    async def plan(self, task: dict[str, Any]) -> dict[str, Any]:
        import httpx

        prompt = PLAN_PROMPT.replace("{task}", json.dumps(task))
        # Model APIs shed load with 429s and 503s, especially on free
        # tiers. A planner that cannot survive a transient error makes
        # the whole framework look flaky, so retry those with backoff.
        # Client errors (a bad key, a bad model name) fail at once.
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    resp = await client.post(
                        f"{self.base_url.rstrip('/')}/chat/completions",
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={
                            "model": self.model,
                            "messages": [{"role": "user", "content": prompt}],
                            "response_format": {"type": "json_object"},
                        },
                    )
                resp.raise_for_status()
                break
            except httpx.HTTPStatusError as exc:
                retryable = exc.response.status_code in (
                    429, 500, 502, 503, 504)
                if not retryable or attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
            except httpx.TransportError:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)


def default_model() -> ModelClient:
    if os.environ.get("ARGON_MODEL_BASE_URL"):
        return OpenAICompatModel()
    return StubModel()


# --------------------------------------------------------------------------
# Planner
# --------------------------------------------------------------------------

class Planner:
    """Turns a model plan into a validated Workflow and appends the
    verifier node, which always runs last and only reads evidence."""

    def __init__(self, model: ModelClient):
        self.model = model

    async def plan(self, task: dict[str, Any]) -> Workflow:
        raw = await self.model.plan(task)
        nodes: list[Node] = []
        for n in raw["nodes"]:
            nodes.append(Node(
                id=n["id"],
                title=n.get("title", n["id"]),
                role=n.get("role", "executor"),
                calls=[ToolCall(c["tool"], c.get("args", {}))
                       for c in n.get("calls", [])],
                depends_on=n.get("depends_on", []),
                timeout_s=n.get("timeout_s", 30.0),
                max_retries=n.get("max_retries", 2),
                acceptance=[],
            ))
        workflow = Workflow(id=task["id"], title=task["title"], nodes=nodes)
        workflow.validate()
        return workflow


# --------------------------------------------------------------------------
# Verifier
# --------------------------------------------------------------------------

@dataclass
class CheckResult:
    criterion: dict[str, Any]
    ok: bool
    evidence: Any


class Verifier:
    """Evidence based verification. Each criterion names a check the
    verifier performs itself through the gateway under the verifier
    role, so the evidence is a tool result, not an agent's claim."""

    def __init__(self, registry: ToolRegistry):
        self.registry = registry

    async def verify(
        self, acceptance: list[dict[str, Any]]
    ) -> tuple[bool, list[CheckResult]]:
        results: list[CheckResult] = []
        for crit in acceptance:
            kind = crit["type"]
            if kind == "file_exists":
                ev = await self.registry.call(
                    "verifier", "check_file", {"path": crit["path"]})
                ok = bool(ev["exists"])
            elif kind == "file_contains":
                ev = await self.registry.call(
                    "verifier", "check_file",
                    {"path": crit["path"], "expect_contains": crit["text"]})
                ok = bool(ev.get("contains"))
            elif kind == "file_equals":
                ev = await self.registry.call(
                    "verifier", "check_file",
                    {"path": crit["path"], "expect_content": crit["content"]})
                ok = bool(ev.get("content_matches"))
            elif kind == "file_line_count":
                ev = await self.registry.call(
                    "verifier", "read_file", {"path": crit["path"]})
                lines = [l for l in ev["content"].splitlines() if l.strip()]
                ok = len(lines) == crit["count"]
                ev = {"path": crit["path"], "lines": len(lines),
                      "expected": crit["count"]}
            elif kind == "value_equals":
                ev = await self.registry.call(
                    "verifier", "compute", {"expression": crit["expression"]})
                ok = ev["value"] == crit["expected"]
                ev = {**ev, "expected": crit["expected"]}
            else:
                ev = {"error": f"unknown criterion type {kind}"}
                ok = False
            results.append(CheckResult(criterion=crit, ok=ok, evidence=ev))
        return all(r.ok for r in results), results
