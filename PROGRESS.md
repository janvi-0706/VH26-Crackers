# PROGRESS

Running log. Newest at the bottom.

---

## Stage A — contract lock (Lane D)

**Done**

- `src/triage/contracts.py` — Event (five separate identity fields), Decision
  (5 outcomes), Mode, Tier, EventType, DecisionTrace, ShedRecord, and
  MetricsFrame with 39 fields, every one defaulted.
- `src/triage/metrics.py` — module-level registry. Latency and queue-wait
  percentiles are real (hand-written, bounded 4096-sample windows per tier).
  Counters, SLA attainment and value delivered/shed are real. Rates, pressure,
  ladder, worker gauges, cost and dedup counters report 0 until the stage that
  owns them lands.
- `src/triage/ledger.py` — `record(seq, decision, reason, pressure, tier)`
  stub, bounded in-memory deque. Signature frozen, body is not. Called from
  `metrics.observe_decision()`, which is the single choke point every decision
  passes through, so no decision path can skip the audit row.
- `config/tiers.yaml` + `src/triage/config.py` — tier table as data, with a
  loader that re-derives the three calibration invariants on every load and
  refuses to start if they have drifted.
- `src/triage/fake_metrics.py` — 4 Hz feed of plausible frames. Counters
  conserve exactly, P0 is never degraded, the 20x spike shows up.
- `tests/` — 43 tests: contract round-trips + frozen-field guard, calibration,
  percentiles + observation points, fake-feed invariants.

**Verified**

```
$ python -m triage.config
capacity: 150.0 u/s (6 workers x 25 u/s)
baseline: 16.65 eps, spike: 333.00 eps (20x)
weighted cost/event: 0.865 u
[ok ] P0 demand at spike:        expected 108.20 u/s, actual 108.23 u/s
[ok ] total demand at spike:     expected 288.00 u/s, actual 288.05 u/s
[ok ] total demand at baseline:  expected  14.40 u/s, actual  14.40 u/s

$ python -m triage.fake_metrics --seconds 2 --seed 7
8 frames at 4.0 Hz, all valid

$ python -m pytest -q
43 passed
```

**Environment note.** The machine's default `python` is 3.13 via Anaconda, and
CLAUDE.md pins 3.11. There is a 3.11 venv at `Code/.venv` (gitignored):

```
py -3.11 -m venv .venv
.venv/Scripts/python.exe -m pip install -e ".[dev]"
```

Everything runs as `.venv/Scripts/python.exe -m triage.<module>` with
`PYTHONPATH=src`, or `make <target> PY=./.venv/Scripts/python.exe`.

**Open / next**

- Contract is not frozen until all four have read it and the team review
  happens. Raise missing fields now; a field added here is free.
- Stage D will need a publish path for the stubbed gauges (pressure, ladder
  rung, worker counts). Decide then whether that is a setter on `metrics` or a
  callback the engine registers - not decided yet, deliberately.

---

## Data-model documentation (Lane D)

**Done**

- Added `docs/DATA_MODEL.md`: five-field identity lifecycle, event-envelope
  boundary and versioning, YAML tier rationale, complete planned SQLite DDL
  with query-specific indexes and growth bounds, rollup accounting, ledger
  hash-chain limits, and a Mermaid ER diagram.
- Performed a direct contract audit against `contracts.py`; all existing Event,
  Decision, MetricsFrame, DecisionTrace, and ShedRecord fields described in
  the document match the code.

**Review finding — do not freeze yet**

- `Event` does not have a `payload` field, only `payload_size`; the documented
  envelope cannot presently serialize its type-specific body.
- No Pydantic contracts yet represent rollups, durable ledger entries, or the
  persistence-only deferred/sink/trace fields. The document lists the exact
  missing fields and distinguishes the SQL-only `event_type` alias from
  existing `Event.type`.

No production code or frozen configuration changed in this prompt.

## Stage B — P4 ingress and sink (Lane B)

**Done**

- `src/triage/generator.py` — seeded/configurable async source, configured type
  mix, variable per-type payload sizes, 500-customer partition pool, and
  explicit retry identity behavior.
- `src/triage/classifier.py` — YAML-derived tier/value/cost/SLA enrichment,
  absolute deadline, contiguous monotonic `seq`, and stable sink
  `idempotency_key`. Classification does not record a routing decision because
  no routing decision exists until a later stage.
- `src/triage/sink.py` — SQLite `events_sink` table and the exact documented
  indexes, full `Event` JSON round-trip, and idempotent upsert with attempt
  counting.
- `tests/test_ingress.py` — mix, retry identity, async source, sequence,
  classification, and sink tests.

**Verified**

```
$ python -m pytest -q
48 passed in 1.07s

mix= {'inventory': 1068, 'click': 5016, 'log': 2929,
      'payment': 517, 'order': 470}
seq= 1 10000 strict= True
sink_round_trip= True rows= 1
```

P4 is complete. Queue, worker, FastAPI, dashboard, and Makefile wiring remain
for P5 and later; no P5 implementation was started in this prompt.

## Stage B — P5 queue, workers, app (Lane A/D)

**Done**

- `src/triage/queue.py` — `EventQueue` wraps one `asyncio.Queue`. `put`/
  `put_nowait` call `metrics.observe_ingest`; `get` calls
  `metrics.observe_dequeue`. No priority, no tiers — a single FIFO, as
  scoped. Internals only; Stage C replaces them with three heaps without
  touching `worker.py`'s call sites.
- `src/triage/worker.py` — `WorkerPool` of `config.worker_count` (6) asyncio
  tasks. Each loop is `queue.get()` -> `asyncio.sleep(event.cost /
  worker_capacity_ups)` -> `metrics.observe_complete` -> `sink.write`, per
  the cost model in `config/tiers.yaml` (25 u/s/worker, never hardcoded
  twice). One bad event logs and does not kill the worker.
- `src/triage/app.py` — FastAPI factory `create_app(fake=..., seed=...)`.
  `GET /health`, `POST /control/rate` (422 on negative, 409 in `--fake`
  mode), `WS /ws` pushing a frame at 4 Hz (`metrics.snapshot()` in real
  mode, `FakeSource.tick()` in `--fake`). Real mode's `lifespan` starts an
  `Engine` (generator -> classifier -> queue -> workers) as background
  tasks on the app's own event loop and stores it on `app.state.engine`;
  `--fake` starts no engine at all. `dashboard/dist` is mounted as static
  when it exists; otherwise `/` returns a small JSON notice instead of
  500ing — `dashboard/` has no build yet, so this is the live path today.
- `Makefile` — `dev` (real engine), `fake` (`--fake`), `test` (pytest),
  `config` (calibration printout), `bench` (still a stub; Stage G).
- `tests/test_engine.py`, `tests/test_app.py` — 21 new tests: queue
  instrumentation and FIFO order; worker cost-model timing and sink
  wiring; the 150 u/s ceiling within 5%; `/health`, `/control/rate`,
  `/ws` in both modes; a real-mode test that the whole pipeline actually
  moves an event (not just imports cleanly).

**Windows-specific finding, fixed, not worked around.** The first run of the
throughput test measured ~128-140 u/s instead of 150 (7-15% low). Traced it
to Windows' default ~7ms per-call overhead on `asyncio.sleep()` under
`ProactorEventLoop` (confirmed directly: a bare loop of `asyncio.sleep(0.04)`
overshoots by ~7ms/call regardless of concurrency). Since the whole cost
model *is* `asyncio.sleep(cost/capacity)`, that overhead would have quietly
inflated every latency number the demo shows on a Windows judge's machine.
Fixed at the source in `worker.py` via `winmm.timeBeginPeriod(1)` (the
standard fix for this, used by games/audio engines) — process-wide,
reversible, a documented no-op on non-Windows. After the fix, the same
measurement lands within 1% of 150 u/s.

**Verified**

```
$ python -m triage.app --fake --port 8091   (then curl)
GET  /health          {"status":"ok","mode":"fake","uptime_s":null}
GET  /                {"info":"dashboard/dist not built yet; try /health or /ws"}
POST /control/rate    409 {"error":"rate control has no effect in --fake mode"}

$ python -m triage.app --port 8092 --seed 5   (then curl)
GET  /health                          {"status":"ok","mode":"real","uptime_s":1.7}
POST /control/rate {"rate":200}       200 {"rate":200.0}
GET  /health (2s later)               {"status":"ok","mode":"real","uptime_s":4.0}

$ python -m pytest -q
64 passed
```

`make`/`make test`/`make dev` could not be exercised directly — this machine
has no `make` binary (Windows, no build tools installed) — but the Makefile
is plain GNU-make syntax and every recipe was run and verified via its
underlying `python -m ...` command above.

**Open / next**

- `dashboard/` is still empty; Stage B's own acceptance line ("Vite + React
  dashboard, real UI") is not yet built — next prompt, per the runbook.
- Contract freeze (Stage A's last open item) is still pending team review.
- `Event.payload` and the rollup/ledger/deferred/sink-only contracts flagged
  in the data-model review are still outstanding — unrelated to this prompt,
  carried forward.

## Stage B — P6 dashboard scaffold (Lane C)

**Done** (`dashboard/` only; `src/` untouched)

- Vite + React 18 + TypeScript + Tailwind + Recharts, dark theme
  (`tailwind.config.js` custom palette: `surface`/`ink`/`good`/`bad`/`warn`/
  `tier`).
- `src/components/Panel.tsx` — the layout system, built first as asked.
  `PanelGrid` is a 12-column CSS grid with fixed-height rows; `Panel` claims
  a fixed span via `size` (`sm|md|lg|wide|tall|full`). New panels only ever
  add a grid item, so the next ~7 panels over the coming stages drop in
  without moving the ones already placed.
- `src/hooks/useMetricsSocket.ts` — one WebSocket to `ws://localhost:8000/ws`
  for the app's lifetime, capped exponential backoff (0.5s → 8s) on any
  drop, exposes `status` + a 240-frame (60s at 4 Hz) rolling history.
- `src/components/ConnectionIndicator.tsx` — always visible in the header;
  green/live, amber/reconnecting, red/disconnected.
- Three panels: `ThroughputPanel` (events/sec line), `LatencyByTierPanel`
  (p99, one line per tier — meaningful now because the classifier already
  tags every event's tier, ahead of Stage C's scheduler), `P0ScoreboardPanel`
  (large p99 vs the 200ms target, green/red, "waiting for data" before the
  first event lands).
- `src/types/metrics.ts` — hand-kept TypeScript mirror of `contracts.py`'s
  `MetricsFrame`/`DecisionTrace`/`ShedRecord`; no fields omitted.
- `dashboard/README.md` — layout system, panel list, run instructions for
  both the Vite dev server and the FastAPI-served build.

**Not done — no Node.js/npm on this machine**

`npm install`, `npm run build`, and `npm run dev` could not be run here, so
the acceptance line (`make dev` one process; charts moving at 1000
events/min; `POST /control/rate 20000` visibly climbing latency) has **not**
been visually verified — only checked without a JS runtime: `package.json`
and both `tsconfig*.json` parse as valid JSON, every `.ts`/`.tsx` file has
balanced brackets, and every import/export was cross-checked by hand. Until
`npm run build` has been run once, `dashboard/dist` does not exist and
`app.py` serves its existing JSON fallback at `/` instead (verified in P5).
The backend half of the acceptance line — rate control, throughput, latency
climbing under load — is already covered by `tests/test_engine.py` and
`tests/test_app.py`.

**Next action, before Stage C:** on a machine with Node, run
`cd dashboard && npm install && npm run build`, then `make dev` and confirm
the acceptance line directly. Flagging this now rather than silently
declaring P6 acceptance-tested.
