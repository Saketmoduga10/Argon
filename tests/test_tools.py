"""Tool gateway tests: permissions, manifests, sandbox, verify hooks."""

import asyncio

import pytest

from argon.core import ToolFailed
from argon.tools import PermissionDenied, build_registry


@pytest.fixture()
def registry(tmp_path):
    return build_registry(tmp_path / "ws")


def run(coro):
    return asyncio.run(coro)


def test_manifest_is_scoped_by_role(registry):
    planner = {t["name"] for t in registry.manifest("planner")}
    executor = {t["name"] for t in registry.manifest("executor")}
    verifier = {t["name"] for t in registry.manifest("verifier")}
    assert "write_file" not in planner and "write_file" in executor
    assert "run_python" not in verifier and "run_python" in executor
    assert "check_file" in verifier


def test_permission_denied_for_out_of_scope_call(registry):
    with pytest.raises(PermissionDenied):
        run(registry.call("planner", "write_file",
                          {"path": "x.txt", "content": "nope"}))
    with pytest.raises(PermissionDenied):
        run(registry.call("verifier", "run_python", {"code": "print(1)"}))


def test_unknown_tool_is_a_clean_failure(registry):
    with pytest.raises(ToolFailed, match="unknown tool"):
        run(registry.call("executor", "teleport", {}))


def test_write_read_check_round_trip(registry):
    async def main():
        out = await registry.call(
            "executor", "write_file", {"path": "a/b.txt", "content": "hello"}
        )
        assert out["bytes"] == 5
        read = await registry.call("verifier", "read_file", {"path": "a/b.txt"})
        assert read["content"] == "hello"
        check = await registry.call(
            "verifier", "check_file",
            {"path": "a/b.txt", "expect_content": "hello"},
        )
        assert check["exists"] and check["content_matches"]

    run(main())


def test_path_escape_is_refused(registry):
    with pytest.raises(ToolFailed, match="escapes"):
        run(registry.call("executor", "write_file",
                          {"path": "../evil.txt", "content": "x"}))


def test_compute_evaluates_safely(registry):
    out = run(registry.call("planner", "compute",
                            {"expression": "2 * (3 + 4) ** 2"}))
    assert out["value"] == 98
    with pytest.raises(ToolFailed):
        run(registry.call("planner", "compute",
                          {"expression": "__import__('os').system('ls')"}))


def test_sandbox_runs_python_and_reports_mode(registry):
    out = run(registry.call("executor", "run_python",
                            {"code": "print(sum(range(10)))"}))
    assert out["returncode"] == 0
    assert out["stdout"].strip() == "45"
    assert out["mode"] in ("docker", "subprocess")


def test_sandbox_write_is_visible_to_check_file(registry):
    async def main():
        await registry.call(
            "executor", "run_python",
            {"code": "open('made.txt','w').write('from sandbox')"},
        )
        check = await registry.call(
            "verifier", "check_file",
            {"path": "made.txt", "expect_contains": "sandbox"},
        )
        assert check["exists"] and check["contains"]

    run(main())


def test_verify_hook_detects_landed_and_missing_writes(registry):
    async def main():
        spec = registry.spec("write_file")
        landed = await spec.verify({"path": "v.txt", "content": "data"})
        assert landed is None  # nothing written yet
        await registry.call("executor", "write_file",
                            {"path": "v.txt", "content": "data"})
        landed = await spec.verify({"path": "v.txt", "content": "data"})
        assert landed is not None and landed["reconciled"]

    run(main())


def test_append_line_and_its_verify_hook(registry):
    async def main():
        spec = registry.spec("append_line")
        assert await spec.verify({"path": "log.txt", "line": "paid"}) is None
        out = await registry.call(
            "executor", "append_line", {"path": "log.txt", "line": "paid"}
        )
        assert out["lines"] == 1
        landed = await spec.verify({"path": "log.txt", "line": "paid"})
        assert landed is not None and landed["reconciled"]
        read = await registry.call("verifier", "read_file", {"path": "log.txt"})
        assert read["content"] == "paid\n"

    run(main())
