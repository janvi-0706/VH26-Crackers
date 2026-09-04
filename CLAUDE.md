# PULSE — adaptive event pipeline (hackathon)

## What this is
An event pipeline that survives a 20x traffic spike by triaging, not scaling.
Every event carries a business value and an SLA deadline. Worker capacity is
scarce and fixed. The pipeline maximises value delivered per unit of capacity,
subject to a hard constraint: critical events are never silently dropped, and
we can prove it.

## Judged on (four jury rounds)
1. Progress made — a delta at each round, not an absolute
2. Code originality — hand-written logic, not glued-together libraries
3. Data design — schemas, identity model, indexes, growth bounds
4. Architecture — module boundaries, control loops, documented decisions
Demo polish is NOT scored. Do not spend time on it.

## Hard rules
1. NO Kafka, NO Redis, NO Docker Compose, NO Celery. Single Python process,
   asyncio, in-memory heaps. Spike load is only ~333 events/sec. Writing the
   scheduling logic ourselves IS the originality score.
2. Worker service time is SIMULATED via a cost model so the capacity ceiling
   is deterministic on any machine. Intentional. Must be documented.
3. P0 (orders, payments) is NEVER batched, deferred, sampled, or shed. Under
   pressure we throttle the source instead. Asserted in code and tests.
4. Every prompt must end in a RUNNABLE state. Never leave the repo broken.

## Working style — single machine, sequential
- ONE prompt at a time. Do exactly what it asks, nothing more.
- Never build ahead. If you think a later feature is needed now, SAY SO and
  wait for me. Do not implement it.
- After every prompt: run it, show me the output, update PROGRESS.md,
  then `git commit -m "PN: <what you did>"`.
- Keep commit messages legible. Our git history is progress evidence.
- If PROGRESS.md and my instructions disagree, PROGRESS.md is the truth about
  what exists; ask me about the difference.

## Stack
Python 3.11, asyncio, FastAPI, uvicorn, pydantic. SQLite (stdlib) for sink,
deferred buffer, audit ledger, rollups. Dashboard: React + Vite + Recharts +
Tailwind, served as static from FastAPI. One WebSocket at 4 Hz. pytest.

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

Calibration that MUST hold — re-verify if you change any number:
- P0 demand at spike ~108 u/s, comfortably under 150 u/s capacity
- Total demand at spike ~288 u/s, ~1.9x capacity, so triage is forced
- Total demand at baseline ~14 u/s, so everything streams individually

## Data identity model — five distinct fields, never one
| field           | identifies             | assigned by | on retry |
|-----------------|------------------------|-------------|----------|
| event_id        | one emission           | generator   | NEW      |
| dedup_key       | business identity      | generator   | SAME     |
| seq             | pipeline order         | classifier  | NEW      |
| partition_key   | ordering domain (cust) | generator   | SAME     |
| idempotency_key | sink upsert target     | classifier  | SAME     |
Collapsing these into one `id` is why most pipelines' dedup breaks retry.

## Frozen after Stage A
contracts.py and config/tiers.yaml. If you need a field that isn't there,
STOP and tell me — do not add it silently.