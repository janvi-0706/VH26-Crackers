# PULSE — adaptive event pipeline (30h hackathon)

## What this is
An event pipeline that survives a 20x traffic spike by triaging, not by scaling.
Every event carries a business value and an SLA deadline. Worker capacity is
scarce and fixed. The pipeline maximises value delivered per unit of capacity,
subject to a hard constraint: critical events are never silently dropped, and
we can prove it.

## Judged on (four jury rounds)
1. Progress made — measured as a delta at each round, not an absolute
2. Code originality — hand-written logic, not glued-together libraries
3. Data design — schemas, identity model, indexes, growth bounds
4. Architecture — module boundaries, control loops, documented decisions

Optimise for these four. Demo polish is NOT scored.

## Hard rules — do not violate
1. NO Kafka, NO Redis, NO Docker Compose, NO Celery. Single Python process,
   asyncio, in-memory heaps. Spike load is only ~333 events/sec. Writing the
   scheduling logic ourselves IS the originality score.
2. Worker service time is SIMULATED via a cost model so the capacity ceiling
   is deterministic on any machine. Intentional. Must be documented.
3. P0 (orders, payments) is NEVER batched, deferred, sampled, or shed. Under
   pressure we throttle the source instead. Asserted in code and in tests.
4. Every phase must end in a RUNNABLE state. Never leave the repo broken.
5. Never modify files outside your lane.

## Stack
Python 3.11, asyncio, FastAPI, uvicorn, pydantic. SQLite (stdlib) for
deferred buffer, sink, audit ledger, rollups. Dashboard: React + Vite +
Recharts + Tailwind, served as static from FastAPI. One WebSocket, 4 Hz.
pytest for invariant tests.

## Event taxonomy (config/tiers.yaml)
| type      | tier | value | sla   | cost |
|-----------|------|-------|-------|------|
| payment   | P0   | 120   | 200ms | 3.5u |
| order     | P0   | 100   | 500ms | 3.0u |
| inventory | P1   | 40    | 5s    | 2.0u |
| click     | P2   | 5     | 30s   | 0.5u |
| log       | P2   | 1     | 60s   | 0.3u |

Mix: 5% payment, 5% order, 10% inventory, 50% click, 30% log.
Worker capacity: 25 work-units/sec each, 6 workers = 150 u/s total.

Calibration that MUST hold:
- P0 demand at spike ~108 u/s, comfortably under 150 u/s capacity
- Total demand at spike ~288 u/s, ~1.9x capacity, so triage is forced
- Total demand at baseline ~14 u/s, so everything streams individually
If you change any cost or mix number, re-verify all three.

## Data identity model — five distinct fields, not one
| field           | identifies              | assigned by | on retry |
|-----------------|-------------------------|-------------|----------|
| event_id        | one emission            | generator   | NEW      |
| dedup_key       | business identity       | generator   | SAME     |
| seq             | pipeline order          | classifier  | NEW      |
| partition_key   | ordering domain (cust)  | generator   | SAME     |
| idempotency_key | sink upsert target      | classifier  | SAME     |
Collapsing these into one `id` is why most pipelines' dedup breaks their retry.

## Lane ownership — do not edit files outside your lane
Lane A: src/triage/queue.py, decision.py, worker.py, codel.py, ladder.py
Lane B: src/triage/generator.py, classifier.py, admission.py, dedup.py,
        sink.py, deferral.py, chaos.py, costmodel.py, ordering.py
Lane C: dashboard/**  (nothing else, ever)
Lane D: src/triage/contracts.py, metrics.py, ledger.py, app.py,
        config/**, bench/**, docs/**, Makefile, Dockerfile, README.md

contracts.py and config/tiers.yaml are FROZEN after Phase 0. Changing them
requires all four people to agree. If you need a field that isn't there,
STOP and ask — do not add it yourself.

Branches: lane-a, lane-b, lane-c, lane-d. Lane D merges to main at each
phase gate. Never merge your own branch to main.

## Working style
- State your lane at the start of every session.
- One phase at a time. Stop when the acceptance criteria pass.
- After each task: run it, show me the output, update PROGRESS.md, git commit.
- Do NOT start the next phase until I say go.
- If a task needs a file outside your lane, say so and stop.
- Keep commit messages legible. Our git history is progress evidence.
