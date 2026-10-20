# Argon engineering notes

Design decisions, invariants, and the reasoning behind them. Written for a reader who wants to know why the system is shaped the way it is, including where the shape is wrong.

## The central invariant

**A side effect is executed at most once, and the system can always explain what happened to it.**

Everything else in Argon is downstream of that sentence. The journal exists to make it true; the reconciler exists to repair it when a crash makes it ambiguous; the verifier exists to check it from the outside.

## Action states and why there are three

Most agent frameworks treat a tool call as done or not done. Real calls have a third outcome: the request went out and the answer never came back. The side effect is *uncertain*. Collapsing uncertain into failed causes duplicate payments and duplicate writes on retry. Collapsing it into completed causes silently lost work. Argon keeps the third state as a first class citizen:

- The scheduler retries `failed` actions (they provably did nothing).
- It replays `completed` actions from the journal (they provably did something, and the result is stored).
- It sends `uncertain` actions to the reconciler, which must produce evidence: for a write, the file's current content; for an append, the presence of the exact line; for a pure tool, the mathematical fact that re-execution cannot duplicate anything. Only evidence moves an action out of uncertain.

Tools declare whether they have side effects and how to verify them. A side effecting tool with no verify hook cannot be reconciled, and Argon says so instead of guessing: the node stays uncertain and the run fails loudly. That is a deliberate, visible gap, and adding verify hooks is the intended extension point.

## Idempotency keys are scoped, not global

An action's content key is a hash of its tool and arguments. Two different nodes legitimately making the same call (write the same content to the same file in two branches) must not collide, or the second would replay the first's result without running. The journal therefore keys actions by `node:position:contentkey`. A replay of *this* node's call is recognised; another node's identical call is its own action. This was a real bug in the first version, caught by the parallel execution test, where four nodes calling the same tool raced on one journal row and three of them were wrongly told their action was already in flight.

## Scheduler notes

- One worker process is the authority for a run. Node states live in the journal; the in memory loop only tracks what is currently executing.
- On resume, nodes left `running` by a dead process become `uncertain`, and nodes `skipped` in a previous life are revived: a skip was a judgement about a failure that this new life may repair. Only dependencies that settle badly *in the current run* settle a dependent's fate.
- The scheduler loop once declared deadlock too eagerly: a pass that skipped a node can change whether a dependent evaluated earlier in the same pass is stuck, so the loop must make a full no progress pass before concluding deadlock. Found by the benchmark after journal writes got faster (below) changed the interleaving, which is a good reminder that timing dependent tests passing proves little.

## Journal performance and the WAL tradeoff

The first benchmark run was dominated by the journal: Argon's median task time was 3.468s against the baseline's 0.031s, because every action and event was its own transaction and every transaction paid a full fsync. Switching the journal to WAL mode with `synchronous=NORMAL` cut the median to 0.139s. The tradeoff is standard SQLite: a *process* crash loses nothing (all failure lab scenarios still pass), while an OS or power failure can drop the last few committed transactions. For an orchestration journal that can reconcile uncertain actions anyway, that is the right trade, and it is declared in the code rather than hidden.

## Benchmark methodology

- One suite of 100 generated tasks (five templates, twenty variants each), every task moving real data through files with acceptance criteria computed from ground truth.
- One seeded fault plan per task, generated before either system runs, and given to both. Mix: 49 clean, 28 transient, 14 uncertain, 9 crashes.
- The baseline is a competent single agent loop: sequential topological execution, one retry per step. It is not a strawman; it loses exactly where an in memory loop should lose: process death, repeated transient failure beyond its retry budget, and unknowable appends.
- The Argon side is allowed one resume per task, because durable resume is the feature under test. Denying it would be like benchmarking a database with crashes but forbidding recovery.
- Completion is judged by the harness running the verifier against the final workspace, for both systems alike. Neither system grades itself.
- Offline runs use the stub model. Numbers say something about scheduling, journaling, and recovery, and nothing about model quality. Any future real model numbers should be published with the model named and the same suite re-run.

## Failure lab postconditions

A scenario passes only if all of these hold after at most one resume:

1. The run verifies against its acceptance criteria (or, where recovery is impossible by design, fails with the uncertain action identified).
2. No side effect exists twice (checked as exact file contents and line counts).
3. No side effect is missing.

This is stricter than "the run eventually reported success", and it is why the lab is the part of this project I would show first.

## Known limitations and next steps

- Single authority scheduler; multi writer journals need a consensus or locking story the current format does not attempt.
- Plans are re-derived on resume rather than persisted. Persisting the plan with the run is the obvious fix and removes a real (if stub masked) inconsistency for live models.
- The sandbox's subprocess mode limits CPU and memory but shares the host network namespace model of a plain subprocess: it cannot promise network isolation. Docker mode can, and is preferred automatically when Docker exists.
- The verifier's criteria are structural (files, values). Semantic criteria (does this summary actually answer the question) need a model based judge, which reintroduces the trust problem the verifier exists to avoid. A hybrid, structural first and judged second with the judgement labelled as such, is the direction I would take.
