"""Core scheduler and journal tests: dependencies, parallelism, retries,
uncertain actions, idempotent replay, and crash resume."""

import asyncio
import time

import pytest

from argon.core import (
    Journal, Node, Scheduler, ToolCall, ToolFailed, ToolUncertain, Workflow,
)


def wf(nodes):
    return Workflow(id="w1", title="test", nodes=nodes)


@pytest.fixture()
def journal(tmp_path):
    j = Journal(tmp_path / "journal.db")
    yield j
    j.close()


def run(coro):
    return asyncio.run(coro)


def test_dependencies_gate_execution(journal):
    order = []

    async def execute(node, call):
        order.append(node.id)
        return node.id

    async def main():
        sched = Scheduler(journal, execute)
        return await sched.run(wf([
            Node(id="a", title="A", role="executor", calls=[ToolCall("noop")]),
            Node(id="b", title="B", role="executor", calls=[ToolCall("noop")],
                 depends_on=["a"]),
            Node(id="c", title="C", role="executor", calls=[ToolCall("noop")],
                 depends_on=["b"]),
        ]))

    out = run(main())
    assert out["status"] == "succeeded"
    assert order == ["a", "b", "c"]


def test_independent_nodes_run_in_parallel(journal):
    async def execute(node, call):
        await asyncio.sleep(0.25)
        return node.id

    async def main():
        sched = Scheduler(journal, execute)
        start = time.perf_counter()
        out = await sched.run(wf([
            Node(id=n, title=n, role="executor", calls=[ToolCall("noop")])
            for n in ("a", "b", "c", "d")
        ]))
        return out, time.perf_counter() - start

    out, elapsed = run(main())
    assert out["status"] == "succeeded"
    assert elapsed < 0.6  # 4 x 0.25s serially would take 1.0s


def test_failed_action_is_retried_then_succeeds(journal):
    calls = {"n": 0}

    async def execute(node, call):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ToolFailed("tool not ready")
        return "ok"

    async def main():
        sched = Scheduler(journal, execute)
        return await sched.run(wf([
            Node(id="a", title="A", role="executor",
                 calls=[ToolCall("flaky")], max_retries=2),
        ]))

    out = run(main())
    assert out["status"] == "succeeded"
    assert calls["n"] == 3


def test_failed_action_exhausts_retries(journal):
    async def execute(node, call):
        raise ToolFailed("always broken")

    async def main():
        sched = Scheduler(journal, execute)
        return await sched.run(wf([
            Node(id="a", title="A", role="executor",
                 calls=[ToolCall("broken")], max_retries=1),
            Node(id="b", title="B", role="executor",
                 calls=[ToolCall("noop")], depends_on=["a"]),
        ]))

    out = run(main())
    assert out["status"] == "failed"
    assert out["states"]["a"]["status"] == "failed"
    assert out["states"]["b"]["status"] == "skipped"


def test_timeout_marks_action_uncertain_and_never_retries(journal):
    calls = {"n": 0}

    async def execute(node, call):
        calls["n"] += 1
        await asyncio.sleep(5)
        return "too late"

    async def main():
        sched = Scheduler(journal, execute)
        return await sched.run(wf([
            Node(id="a", title="A", role="executor",
                 calls=[ToolCall("slow")], timeout_s=0.1, max_retries=3),
        ]))

    out = run(main())
    assert out["status"] == "failed"
    assert out["states"]["a"]["status"] == "uncertain"
    assert calls["n"] == 1  # uncertain actions are not retried


def test_completed_action_is_replayed_not_reexecuted(journal):
    calls = {"n": 0}

    async def execute(node, call):
        calls["n"] += 1
        return {"value": 42}

    async def main():
        sched = Scheduler(journal, execute)
        workflow = wf([
            Node(id="a", title="A", role="executor", calls=[ToolCall("charge")]),
        ])
        first = await sched.run(workflow)
        second = await sched.run(workflow, run_id=first["run_id"])
        return first, second

    first, second = run(main())
    assert first["status"] == second["status"] == "succeeded"
    assert calls["n"] == 1  # second run replayed the journal result


def test_uncertain_action_is_reconciled_on_resume(journal):
    state = {"landed": False, "executions": 0}

    async def execute(node, call):
        state["executions"] += 1
        state["landed"] = True  # side effect lands, then the "crash" hits
        raise ToolUncertain("process died after the call")

    async def reconcile(node, call, record):
        if state["landed"]:
            return {"value": "recovered"}
        raise ToolFailed("side effect never landed")

    async def main():
        sched = Scheduler(journal, execute, reconcile=reconcile)
        workflow = wf([
            Node(id="a", title="A", role="executor", calls=[ToolCall("charge")]),
        ])
        first = await sched.run(workflow)
        assert first["states"]["a"]["status"] == "uncertain"
        second = await sched.run(workflow, run_id=first["run_id"])
        return second

    out = run(main())
    assert out["status"] == "succeeded"
    assert state["executions"] == 1  # the charge was never executed twice


def test_uncertain_without_reconciler_stays_uncertain(journal):
    async def execute(node, call):
        raise ToolUncertain("boom")

    async def main():
        sched = Scheduler(journal, execute)
        workflow = wf([
            Node(id="a", title="A", role="executor", calls=[ToolCall("x")]),
        ])
        first = await sched.run(workflow)
        return await sched.run(workflow, run_id=first["run_id"])

    out = run(main())
    assert out["states"]["a"]["status"] == "uncertain"


def test_cycle_is_rejected(journal):
    async def execute(node, call):
        return None

    async def main():
        sched = Scheduler(journal, execute)
        return await sched.run(wf([
            Node(id="a", title="A", role="r", depends_on=["b"]),
            Node(id="b", title="B", role="r", depends_on=["a"]),
        ]))

    with pytest.raises(ValueError, match="cycle"):
        run(main())


def test_reconcile_proving_no_side_effect_allows_execution(journal):
    state = {"executions": 0}

    async def execute(node, call):
        state["executions"] += 1
        if state["executions"] == 1:
            raise ToolUncertain("died before doing anything")
        return "done"

    async def reconcile(node, call, record):
        raise ToolFailed("checked: the side effect is not there")

    async def main():
        sched = Scheduler(journal, execute, reconcile=reconcile)
        workflow = wf([
            Node(id="a", title="A", role="executor", calls=[ToolCall("charge")]),
        ])
        first = await sched.run(workflow)
        assert first["states"]["a"]["status"] == "uncertain"
        return await sched.run(workflow, run_id=first["run_id"])

    out = run(main())
    assert out["status"] == "succeeded"
    assert state["executions"] == 2  # one uncertain attempt + one real run
