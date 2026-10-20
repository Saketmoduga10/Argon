"""Agent tests: stub planning into a valid DAG, and evidence based
verification that catches a wrong output."""

import asyncio

import pytest

from argon.agents import OpenAICompatModel, Planner, StubModel, Verifier
from argon.tools import build_registry

TASK = {
    "id": "t1",
    "title": "Write inputs, transform, summarise",
    "steps": [
        {"id": "seed_a", "tool": "write_file",
         "args": {"path": "a.txt", "content": "3"}},
        {"id": "seed_b", "tool": "write_file",
         "args": {"path": "b.txt", "content": "4"}},
        {"id": "sum", "tool": "run_python",
         "args": {"code": "print(3 + 4)"},
         "depends_on": ["seed_a", "seed_b"]},
    ],
    "acceptance": [
        {"type": "file_equals", "path": "a.txt", "content": "3"},
        {"type": "value_equals", "expression": "3 + 4", "expected": 7},
    ],
}


def run(coro):
    return asyncio.run(coro)


def test_stub_planner_builds_dag_with_dependencies():
    async def main():
        workflow = await Planner(StubModel()).plan(TASK)
        by_id = workflow.by_id()
        assert by_id["sum"].depends_on == ["seed_a", "seed_b"]
        assert by_id["seed_a"].calls[0].tool == "write_file"
        assert all(n.role == "executor" for n in workflow.nodes)

    run(main())


def test_planner_rejects_cyclic_model_output():
    class BadModel:
        async def plan(self, task):
            return {"nodes": [
                {"id": "x", "calls": [], "depends_on": ["y"]},
                {"id": "y", "calls": [], "depends_on": ["x"]},
            ]}

    with pytest.raises(ValueError, match="cycle"):
        run(Planner(BadModel()).plan(TASK))


def test_verifier_passes_on_correct_evidence(tmp_path):
    async def main():
        registry = build_registry(tmp_path / "ws")
        await registry.call("executor", "write_file",
                            {"path": "a.txt", "content": "3"})
        ok, results = await Verifier(registry).verify(TASK["acceptance"])
        assert ok
        assert all(r.evidence for r in results)

    run(main())


def test_verifier_fails_on_wrong_or_missing_evidence(tmp_path):
    async def main():
        registry = build_registry(tmp_path / "ws")
        await registry.call("executor", "write_file",
                            {"path": "a.txt", "content": "WRONG"})
        ok, results = await Verifier(registry).verify(TASK["acceptance"])
        assert not ok
        assert results[0].ok is False and results[0].evidence["exists"]

    run(main())


def test_verifier_line_count_catches_duplicate_appends(tmp_path):
    async def main():
        registry = build_registry(tmp_path / "ws")
        for _ in range(2):
            await registry.call("executor", "append_line",
                                {"path": "log.txt", "line": "paid"})
        ok, results = await Verifier(registry).verify([
            {"type": "file_line_count", "path": "log.txt", "count": 1},
        ])
        assert not ok  # a duplicated side effect is visible in evidence
        assert results[0].evidence["lines"] == 2

    run(main())


class _FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx
            raise httpx.HTTPStatusError(
                "err", request=httpx.Request("POST", "http://x"),
                response=httpx.Response(self.status_code))

    def json(self):
        return self._payload


def _fake_client(statuses, monkeypatch):
    import httpx
    calls = {"n": 0}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            status = statuses[calls["n"]]
            calls["n"] += 1
            payload = {"choices": [{"message": {
                "content": '{"nodes": []}'}}]}
            return _FakeResponse(status, payload)

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    return calls


def test_model_client_retries_transient_errors(monkeypatch):
    calls = _fake_client([503, 429, 200], monkeypatch)
    model = OpenAICompatModel("http://x", "m", "k")
    plan = asyncio.run(model.plan({"id": "t", "title": "T", "steps": []}))
    assert plan == {"nodes": []}
    assert calls["n"] == 3


def test_model_client_does_not_retry_client_errors(monkeypatch):
    import httpx
    import pytest
    calls = _fake_client([404], monkeypatch)
    model = OpenAICompatModel("http://x", "m", "k")
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(model.plan({"id": "t", "title": "T", "steps": []}))
    assert calls["n"] == 1
