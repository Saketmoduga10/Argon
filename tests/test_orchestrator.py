"""Orchestrator and failure lab tests: the recovery stories Argon exists
to tell, exercised end to end."""

import asyncio
from pathlib import Path

from argon.orchestrator import Runner

TASK = {
    "id": "report1",
    "title": "Log a payment and write a receipt",
    "steps": [
        {"id": "log", "tool": "append_line",
         "args": {"path": "ledger.txt", "line": "paid 50"}},
        {"id": "receipt", "tool": "write_file",
         "args": {"path": "receipt.txt", "content": "paid 50"},
         "depends_on": ["log"]},
    ],
    "acceptance": [
        {"type": "file_line_count", "path": "ledger.txt", "count": 1},
        {"type": "file_equals", "path": "receipt.txt", "content": "paid 50"},
    ],
}


def make_runner(tmp_path):
    return Runner(tmp_path / "journal.db", tmp_path / "ws")


def run_task(runner, task, faults=None):
    return asyncio.run(runner.run_task(task, faults=faults))


def test_clean_run_succeeds_and_verifies(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(runner, TASK)
    assert out["status"] == "succeeded"
    assert out["verified"] is True
    runner.close()


def test_transient_failures_recover_via_retries(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(runner, TASK, faults={"fail_times": {"write_file": 2}})
    assert out["status"] == "succeeded" and out["verified"]
    runner.close()


def test_uncertain_append_is_reconciled_not_duplicated(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(runner, TASK, faults={"uncertain_after": {"append_line": 1}})
    assert out["status"] == "failed"
    assert out["states"]["log"]["status"] == "uncertain"

    resumed = asyncio.run(runner.resume_task(TASK, out["run_id"]))
    assert resumed["status"] == "succeeded" and resumed["verified"]
    ledger = Path(tmp_path / "ws" / "ledger.txt").read_text()
    assert ledger == "paid 50\n"  # exactly once: no blind replay
    runner.close()


def test_crash_after_side_effect_resumes_without_duplicate(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(
        runner, TASK,
        faults={"crash_on": {"tool": "append_line", "append_line": 1,
                             "after_side_effect": True}},
    )
    assert out["status"] == "crashed"

    inspection = asyncio.run(runner.inspect_run(out["run_id"]))
    tools = {a["tool"]: a["status"] for a in inspection["actions"]}
    assert tools["append_line"] in ("started", "uncertain")

    resumed = asyncio.run(runner.resume_task(TASK, out["run_id"]))
    assert resumed["status"] == "succeeded" and resumed["verified"]
    ledger = Path(tmp_path / "ws" / "ledger.txt").read_text()
    assert ledger == "paid 50\n"
    runner.close()


def test_crash_before_side_effect_executes_on_resume(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(
        runner, TASK,
        faults={"crash_on": {"tool": "append_line", "append_line": 1,
                             "after_side_effect": False}},
    )
    assert out["status"] == "crashed"
    assert not Path(tmp_path / "ws" / "ledger.txt").exists()

    resumed = asyncio.run(runner.resume_task(TASK, out["run_id"]))
    assert resumed["status"] == "succeeded" and resumed["verified"]
    assert Path(tmp_path / "ws" / "ledger.txt").read_text() == "paid 50\n"
    runner.close()


TIMEOUT_TASK = {
    "id": "slow1",
    "title": "One slow step",
    "steps": [
        {"id": "slow", "tool": "compute",
         "args": {"expression": "1 + 1"}, "timeout_s": 0.2},
    ],
    "acceptance": [{"type": "value_equals", "expression": "1 + 1", "expected": 2}],
}


def test_timeout_leaves_node_uncertain_and_identifiable(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(runner, TIMEOUT_TASK, faults={"delay": {"compute": 2.0}})
    assert out["status"] == "failed"
    assert out["states"]["slow"]["status"] == "uncertain"
    inspection = asyncio.run(runner.inspect_run(out["run_id"]))
    kinds = [e["kind"] for e in inspection["events"]]
    assert "action_uncertain" in kinds
    runner.close()


def test_timed_out_pure_tool_reexecutes_on_resume(tmp_path):
    runner = make_runner(tmp_path)
    out = run_task(runner, TIMEOUT_TASK, faults={"delay": {"compute": 2.0}})
    assert out["states"]["slow"]["status"] == "uncertain"
    resumed = asyncio.run(runner.resume_task(TIMEOUT_TASK, out["run_id"]))
    assert resumed["status"] == "succeeded" and resumed["verified"]
    runner.close()


def test_journal_stores_the_plan_with_the_run(tmp_path):
    async def main():
        runner = Runner(tmp_path / "j.db", tmp_path / "ws")
        report = await runner.run_task(TASK)
        plan = await runner.journal.get_plan(report["run_id"])
        runner.close()
        return plan

    plan = asyncio.run(main())
    assert plan is not None
    assert [n.id for n in plan.nodes] == ["log", "receipt"]
    assert plan.nodes[0].calls[0].tool == "append_line"


def test_resume_executes_the_stored_plan_not_a_rederived_one(tmp_path):
    async def main():
        runner = Runner(tmp_path / "j.db", tmp_path / "ws")
        report = await runner.run_task(
            TASK,
            faults={"crash_on": {"append_line": 1, "after_side_effect": True}},
        )
        assert report["status"] == "crashed"
        # Same run, but the caller now hands resume a DIFFERENT task body.
        # If resume re-planned, the ledger would gain a "paid 999" line.
        mutated = {
            **TASK,
            "steps": [
                {"id": "log", "tool": "append_line",
                 "args": {"path": "ledger.txt", "line": "paid 999"}},
                {"id": "receipt", "tool": "write_file",
                 "args": {"path": "receipt.txt", "content": "paid 999"},
                 "depends_on": ["log"]},
            ],
        }
        resumed = await runner.resume_task(mutated, report["run_id"])
        runner.close()
        return resumed

    report = asyncio.run(main())
    assert report["status"] == "succeeded"
    # Verification ran against the ORIGINAL acceptance criteria (paid 50),
    # and it passed: the stored plan, not the mutated one, was executed.
    assert report["verified"] is True
