# PULSE — adaptive event pipeline

An event pipeline that survives a 20x traffic spike by **triaging, not
scaling**. Every event carries a business value and an SLA deadline;
worker capacity is scarce and fixed. The pipeline maximises value
delivered per unit of capacity, subject to one hard constraint: critical
events (payments, orders) are never silently dropped, and the ledger can
prove it.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the component and
control-loop diagrams, [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) for the
schemas, and [`docs/adr/`](docs/adr/) for why the system is built this
way rather than the more obvious alternatives.

## Setup

Requires Python 3.11 (not 3.12+ — pinned in `pyproject.toml`) and Node
for the dashboard build.

```bash
python3.11 -m venv .venv
.venv/Scripts/pip install -e ".[dev]"      # Windows
# .venv/bin/pip install -e ".[dev]"        # macOS / Linux

cd dashboard && npm install && npm run build && cd ..
```

`make` auto-detects `.venv` (Windows or POSIX layout) and exports
`PYTHONPATH=src`, so every target below works with a bare `make <target>`
once the venv exists — no manual activation required.

```bash
make dev     # the real pipeline: generator -> classifier -> queue ->
             # workers -> sink, FastAPI serving /health, /control/*, /ws
             # on :8000. Open http://localhost:8000 for the dashboard.
make fake    # same dashboard, no engine — /ws streams triage.fake_metrics
             # instead. Lets the dashboard be exercised without the engine
             # running (this is how it was built in Stage A/B).
make config  # print the tier table and re-check the three calibration
             # invariants (P0 demand, total demand, baseline demand)
make test    # the full pytest suite (invariant + contract tests)
make bench   # headless benchmark: 4 configs x 90s + a 5x/10x/20x/40x
             # sensitivity sweep (~10.5 real minutes). Writes
             # bench/report.md and bench/report.html.
```

`make dev` needs `dashboard/dist` built first (the `npm run build` step
above) — without it, `/` returns a plain JSON placeholder rather than
500ing, and `/health` and `/ws` still work on their own.

## What is real vs. simulated

Every number this system reports is real — the metrics, the queueing
behaviour, the pressure signal, the admission decisions, the audit
ledger, the SLA outcomes. **Exactly one thing is simulated, on purpose,
and disclosed everywhere it matters:** how long a worker takes to do the
work.

| | Real | Simulated |
|---|---|---|
| Event generation, timing, arrival rate | ✅ real `asyncio`, real wall clock | |
| Classification into type/tier | ✅ | |
| Queue ordering (EDF-style score) | ✅ | |
| Pressure signal (utilization, arrival/service ratio, p95÷SLA, worker util) | ✅ computed from real EWMA counters | |
| Admission control (AIMD credits) | ✅ real credit buckets, real deny counts | |
| CoDel sojourn tracking | ✅ real observed sojourn times | |
| Ladder escalation (batch / defer / sample / shed) | ✅ real routing decisions | |
| Reservoir sampling and rollup weights | ✅ | |
| SQLite sink, deferred buffer, audit ledger | ✅ real SQLite, real hash chain | |
| **Worker "doing the work"** | | ⚠️ **simulated**: a worker sleeps for `cost / capacity_per_worker` seconds rather than performing `cost` units of real CPU/IO |

**Why simulate this one thing:** every claim PULSE makes ("P0 stays under
200ms," "we survive a 1.9x overload") is a claim about capacity versus
demand, and that claim must hold identically on a judge's laptop, a demo
machine, and CI — none of which have comparable hardware. Real work per
event would make throughput a function of whatever machine happens to run
it; simulated service time via a fixed, disclosed cost model
(`config/tiers.yaml`: 25 work-units/sec/worker, 6 workers, 150 u/s total)
makes the capacity ceiling a documented constant instead of a benchmark
result. Full rationale: [ADR 0002](docs/adr/0002-simulated-service-cost.md).

This is said out loud in the demo, not glossed over. If a judge asks "is
this real," the honest answer is one sentence: everything is real except
how long a worker takes to finish one event, which is a fixed, disclosed
number from a config file, chosen so the 150 u/s capacity ceiling is
identical and reproducible on any machine.

## Hard rules this system is built to prove

From `CLAUDE.md`, unchanged since Stage A:

1. No Kafka, Redis, Docker Compose, or Celery — one Python process,
   `asyncio`, in-memory heaps. See [ADR 0001](docs/adr/0001-in-process-asyncio-over-kafka.md).
2. Worker service time is simulated via a disclosed cost model (above).
3. P0 (orders, payments) is never batched, deferred, sampled, or shed —
   under pressure the *source* is throttled instead
   (`admission.py`'s AIMD credits). Asserted in code
   (`metrics.critical_failure_count()`, never cleared by a normal reset)
   and proved in tests (`test_p0_is_never_batched_deferred_sampled_or_shed_at_any_pressure`).
4. Every prompt in this project's build ends in a runnable state — see
   `PROGRESS.md` for the stage-by-stage record.
