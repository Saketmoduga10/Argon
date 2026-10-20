"""Tool layer: an MCP style tool gateway with role scoped permissions.

Agents never call tools directly. They call through the gateway, which:

- exposes a per role manifest (like MCP tools/list): a role only sees the
  tools it is allowed to use;
- enforces the permission on every call (like MCP tools/call with a scoped
  session): a planner cannot write files, a verifier cannot execute code,
  no matter what the model asks for;
- knows which tools have side effects and how to verify them, which is
  what the scheduler's reconciler uses after an uncertain action.

Roles: planner (read only), executor (read, write, compute, run code in
the sandbox), verifier (read and check only).
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .core import Node, ToolCall, ToolFailed

Handler = Callable[[dict[str, Any]], Awaitable[Any]]
Verify = Callable[[dict[str, Any]], Awaitable[Optional[Any]]]


class PermissionDenied(ToolFailed):
    """A role tried to use a tool outside its scope. A definite failure:
    nothing ran, so nothing needs reconciling."""


@dataclass
class ToolSpec:
    name: str
    description: str
    roles: set[str]
    handler: Handler
    side_effect: bool = False
    verify: Optional[Verify] = None  # for side effect tools: did it land?


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        self._tools[spec.name] = spec

    def manifest(self, role: str) -> list[dict[str, Any]]:
        """The tools/list view for one role: only what it may use."""
        return [
            {"name": t.name, "description": t.description,
             "side_effect": t.side_effect}
            for t in self._tools.values() if role in t.roles
        ]

    def spec(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError:
            raise ToolFailed(f"unknown tool: {name}") from None

    async def call(self, role: str, tool: str, args: dict[str, Any]) -> Any:
        spec = self.spec(tool)
        if role not in spec.roles:
            raise PermissionDenied(
                f"role '{role}' may not call tool '{tool}' "
                f"(allowed: {sorted(spec.roles)})"
            )
        return await spec.handler(args)


# --------------------------------------------------------------------------
# Sandbox for code execution
# --------------------------------------------------------------------------

class Sandbox:
    """Runs agent written Python with no network and tight limits.

    Prefers Docker when it is available (full isolation: no network, capped
    memory and CPU, workspace mounted read/write at /work). Falls back to a
    subprocess with resource limits and a scrubbed environment, which is
    what CI machines without Docker use. The mode in use is reported in
    every result so nothing pretends to be more isolated than it is.
    """

    def __init__(self, workspace: Path, timeout_s: float = 10.0):
        self.workspace = Path(workspace)
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.timeout_s = timeout_s
        self.docker = shutil.which("docker") is not None

    @property
    def mode(self) -> str:
        return "docker" if self.docker else "subprocess"

    def run_python(self, code: str) -> dict[str, Any]:
        if self.docker:
            return self._run_docker(code)
        return self._run_subprocess(code)

    def _run_docker(self, code: str) -> dict[str, Any]:
        cmd = [
            "docker", "run", "--rm", "--network", "none",
            "--memory", "256m", "--cpus", "0.5",
            "-v", f"{self.workspace}:/work", "-w", "/work",
            "python:3.12-alpine", "python", "-c", code,
        ]
        return self._finish(cmd, cwd=None)

    def _run_subprocess(self, code: str) -> dict[str, Any]:
        def limits() -> None:
            resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
            resource.setrlimit(
                resource.RLIMIT_AS, (512 * 1024 * 1024, 512 * 1024 * 1024)
            )

        cmd = [sys.executable, "-I", "-c", code]
        env = {"PATH": os.environ.get("PATH", ""), "PYTHONHASHSEED": "0"}
        return self._finish(cmd, cwd=self.workspace, env=env, preexec=limits)

    def _finish(self, cmd, cwd, env=None, preexec=None) -> dict[str, Any]:
        try:
            proc = subprocess.run(
                cmd, cwd=cwd, env=env, preexec_fn=preexec,
                capture_output=True, text=True, timeout=self.timeout_s,
            )
            return {
                "mode": self.mode, "returncode": proc.returncode,
                "stdout": proc.stdout[-4000:], "stderr": proc.stderr[-2000:],
                "timed_out": False,
            }
        except subprocess.TimeoutExpired:
            return {
                "mode": self.mode, "returncode": None, "stdout": "",
                "stderr": f"exceeded {self.timeout_s}s limit",
                "timed_out": True,
            }


# --------------------------------------------------------------------------
# Built in tools, all rooted at one workspace directory
# --------------------------------------------------------------------------

def _safe_path(root: Path, rel: str) -> Path:
    path = (root / rel).resolve()
    if root.resolve() not in path.parents and path != root.resolve():
        raise ToolFailed(f"path escapes the workspace: {rel}")
    return path


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def build_registry(workspace: Path, sandbox: Optional[Sandbox] = None) -> ToolRegistry:
    root = Path(workspace)
    root.mkdir(parents=True, exist_ok=True)
    sandbox = sandbox or Sandbox(root)
    reg = ToolRegistry()

    async def write_file(args: dict[str, Any]) -> Any:
        path = _safe_path(root, args["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(args["content"])
        return {"path": args["path"], "sha256": _sha(args["content"]),
                "bytes": len(args["content"])}

    async def verify_write(args: dict[str, Any]) -> Optional[Any]:
        path = _safe_path(root, args["path"])
        if path.exists() and path.read_text() == args["content"]:
            return {"path": args["path"], "sha256": _sha(args["content"]),
                    "bytes": len(args["content"]), "reconciled": True}
        return None

    async def read_file(args: dict[str, Any]) -> Any:
        path = _safe_path(root, args["path"])
        if not path.exists():
            raise ToolFailed(f"no such file: {args['path']}")
        return {"path": args["path"], "content": path.read_text()}

    async def list_files(args: dict[str, Any]) -> Any:
        files = sorted(
            str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()
        )
        return {"files": files}

    async def check_file(args: dict[str, Any]) -> Any:
        path = _safe_path(root, args["path"])
        if not path.exists():
            return {"path": args["path"], "exists": False}
        content = path.read_text()
        out: dict[str, Any] = {
            "path": args["path"], "exists": True, "sha256": _sha(content),
            "bytes": len(content),
        }
        if "expect_content" in args:
            out["content_matches"] = content == args["expect_content"]
        if "expect_contains" in args:
            out["contains"] = args["expect_contains"] in content
        return out

    async def compute(args: dict[str, Any]) -> Any:
        return {"expression": args["expression"],
                "value": _eval_expr(args["expression"])}

    async def run_python(args: dict[str, Any]) -> Any:
        return sandbox.run_python(args["code"])

    reg.register(ToolSpec(
        "write_file", "Write text to a file in the workspace.",
        {"executor"}, write_file, side_effect=True, verify=verify_write,
    ))
    reg.register(ToolSpec(
        "read_file", "Read a file from the workspace.",
        {"planner", "executor", "verifier"}, read_file,
    ))
    reg.register(ToolSpec(
        "list_files", "List files in the workspace.",
        {"planner", "executor", "verifier"}, list_files,
    ))
    reg.register(ToolSpec(
        "check_file", "Check a file exists and optionally match its content.",
        {"verifier", "executor"}, check_file,
    ))
    reg.register(ToolSpec(
        "compute", "Evaluate an arithmetic expression exactly.",
        {"planner", "executor", "verifier"}, compute,
    ))
    reg.register(ToolSpec(
        "run_python", "Run Python in the sandbox (no network).",
        {"executor"}, run_python, side_effect=True,
    ))
    return reg


# Safe arithmetic: walk the AST, allow numbers and a small operator set.
_BINOPS = {
    ast.Add: lambda a, b: a + b, ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b, ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b, ast.Mod: lambda a, b: a % b,
    ast.Pow: lambda a, b: a ** b,
}
_FUNCS = {"min": min, "max": max, "abs": abs, "round": round}


def _eval_expr(expr: str) -> float:
    tree = ast.parse(expr, mode="eval")

    def walk(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return walk(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINOPS:
            return _BINOPS[type(node.op)](walk(node.left), walk(node.right))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -walk(node.operand)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in _FUNCS:
            return _FUNCS[node.func.id](*[walk(a) for a in node.args])
        raise ToolFailed(f"expression not allowed: {expr}")

    return walk(tree)


# --------------------------------------------------------------------------
# Glue between the scheduler and the gateway
# --------------------------------------------------------------------------

def make_executor(registry: ToolRegistry):
    """An ExecuteFn for the Scheduler: routes each call through the
    gateway under the node's role, so permissions are always enforced."""

    async def execute(node: Node, call: ToolCall) -> Any:
        return await registry.call(node.role, call.tool, call.args)

    return execute


def make_reconciler(registry: ToolRegistry):
    """A ReconcileFn: for tools with a verify hook, ask whether the side
    effect landed. Landed returns the reconstructed result; not landed
    raises ToolFailed (safe to execute); no hook raises ToolUncertain
    from core, imported lazily to avoid a cycle."""

    from .core import ToolUncertain

    async def reconcile(node: Node, call: ToolCall, record: dict[str, Any]) -> Any:
        spec = registry.spec(call.tool)
        if spec.verify is None:
            raise ToolUncertain(
                f"tool {call.tool} has no verify hook; cannot reconcile"
            )
        args = json.loads(record["args_json"])
        result = await spec.verify(args)
        if result is None:
            raise ToolFailed(f"verified: {call.tool} side effect did not land")
        return result

    return reconcile
