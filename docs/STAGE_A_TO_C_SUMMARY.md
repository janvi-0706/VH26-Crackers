# PULSE — Work Completed Through Stage C

## Scope

This document summarizes the repository state through Stage C of the PULSE
hackathon plan. The implementation runs as one Python process on one laptop,
using `asyncio`, in-memory scheduling structures, SQLite, FastAPI, and a static
dashboard. The traffic spike is simulated; worker service time is deliberately
simulated from the configured work-unit cost so the capacity ceiling is
deterministic.

Stage D adaptive decisions and Stage E backpressure are not included yet.

## Stage A — Contract lock

Stage A established the interfaces that later stages must use.

- `contracts.py` defines the frozen `Event` envelope with five separate
  identity fields: `event_id`, `dedup_key`, `seq`, `partition_key`, and
  `idempotency_key`.
- `Decision` defines the five routing outcomes: `STREAM_NOW`, `MICRO_BATCH`,
  `DEFER`, `SAMPLE_ROLLUP`, and `SHED`.
- `MetricsFrame` contains the complete dashboard schema, including per-tier
  queue and latency metrics, rates, pressure, worker gauges, conservation
  counters, sampling counters, SLA counters, correctness counters, and recent
  decision/shed records. Fields default to zero or empty collections so the
  dashboard contract remains stable.
- `metrics.py` provides the module-level observation points for ingest,
  dequeue, completion, and decisions. Latency percentiles and the currently
  applicable counters are implemented; future-stage gauges remain explicit
  zero-valued stubs.
- `ledger.py` provides the bounded in-memory `record(...)` stub. Decisions are
  recorded through the metrics decision observation point; ingestion and
  classification do not pretend that a routing decision has already happened.
- `config/tiers.yaml` and `config.py` externalize the event taxonomy, values,
  SLAs, costs, mix, worker count, capacity, baseline rate, and spike
  multiplier. The loader checks the calibration invariants:
  - baseline demand is approximately 14.4 work-units/sec;
  - 20x spike demand is approximately 288 work-units/sec;
  - P0 spike demand is approximately 108.2 work-units/sec, below the fixed
    150 work-units/sec worker capacity.
- `fake_metrics.py` produces valid plausible frames at 4 Hz for dashboard work
  before the real adaptive engine exists.
- `docs/DATA_MODEL.md` documents the identity model, envelope boundary, SQLite
  schemas and indexes, rollup accounting, ledger hash chain, and ER diagram.

## Stage B — Vertical slice

Stage B made the pipeline runnable end to end before triage policy existed.

### P4: ingress and sink

- `generator.py` emits the configured five-type mix at a configurable rate.
  Payload sizes vary by type, and `partition_key` is selected from a pool of
  500 customers.
- The generator distinguishes a new physical emission from a retry: a retry
  gets a new `event_id` but retains its `dedup_key` and `partition_key`.
- `classifier.py` loads tier metadata from YAML and assigns `tier`, `value`,
  `cost`, absolute `deadline_ts`, a contiguous monotonic `seq`, and a stable
  sink `idempotency_key`.
- `sink.py` creates the documented `events_sink` table and indexes, serializes
  the event envelope, and upserts by `idempotency_key` while counting attempts.

### P5: runnable backend

- The pipeline is wired as generator → classifier → queue → worker pool →
  SQLite sink in one asynchronous process.
- The queue/workers path initially provided a single FIFO queue and six
  workers. Each worker simulates service with `event.cost / 25.0` seconds,
  documenting the fixed 25 work-units/sec per-worker ceiling.
- `app.py` provides `GET /health`, `POST /control/rate`, and `WS /ws` with
  metrics frames at 4 Hz. It serves the built dashboard when available and
  retains a backend fallback when no build is present.
- No Kafka, Redis, Docker Compose, Celery, or external integration is used.

### P6: dashboard scaffold

- The React/Vite dashboard uses one WebSocket connection and a rolling metrics
  history.
- The connection indicator, throughput panel, per-tier latency panel, P0 SLA
  scoreboard, and stacked per-tier queue-depth panel are implemented.
- The TypeScript metrics mirror covers the frozen backend `MetricsFrame`,
  `DecisionTrace`, and `ShedRecord` contracts.
- The dashboard build and backend acceptance path were verified after Node was
  available. The calibrated spike is controlled through the backend rate
  endpoint at this point.

## Stage C — First demoable scheduler

Stage C replaced the FIFO scheduling behavior with a hand-written priority
queue while preserving the worker-facing `put`, `get`, `task_done`, and `join`
interfaces.

- `queue.py` maintains three in-memory heaps:
  - P0 uses earliest-deadline-first ordering, with `seq` as the tie-breaker.
  - P1 and P2 use arrival order by `seq`.
  - Adaptive selection prefers the highest-priority non-empty tier.
  - A bounded P2 aging guard prevents an individual old P2 event from waiting
    forever.
  - Naive mode selects globally by `seq`, providing the FIFO control arm for
    comparison.
- The queue is async-safe for multiple consumers and preserves the queue
  accounting behavior expected by the existing workers.
- The dashboard includes a stacked queue-depth chart for P0, P1, and P2.
- The queue acceptance tests cover EDF ordering, aging, naive/adaptive mode
  switching, multiple waiters, task completion, and join behavior.

At the calibrated 20x spike, the first demonstration showed P0 p99 around
192 ms during the initial observation window while P2 backlog and latency
grew. During a longer sustained spike, all six non-preemptive workers became
busy; P0 queue depth remained zero, but P0 p99 eventually rose above its SLA
because a P0 event can still arrive while every worker is finishing work that
already started. This is an honest Stage C boundary: priority ordering protects
queued P0 work, but it cannot reclaim service capacity from in-flight work.

## Verification through Stage C

The progress log records the following checks:

```text
python -m pytest -q
83 passed

npm run typecheck
clean

npm run build
clean; dashboard/dist rebuilt
```

The repository is therefore runnable and demoable through the Stage C priority
scheduler, subject to the remaining Stage C checklist items below.

## Remaining before Stage C can be called fully closed

- Wire a visible dashboard control for switching the generator between the
  baseline rate and the calibrated 20x spike. The backend rate endpoint exists;
  the remaining item is the live UI control/acceptance check.
- Add the Round 1 notes under `docs/rounds/`.

## What starts after Stage C

Stage D adds the real adaptive decision function and per-event reasons. Stage E
adds CoDel-style sojourn control, the degradation ladder, credit-based source
admission/throttling, deferral, and the durable hash-chained ledger. Those are
the components that will turn Stage C queue pressure into actual backpressure;
they are intentionally not claimed as complete here.
