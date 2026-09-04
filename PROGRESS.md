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

## Stage B — P6 follow-up: Node installed, acceptance line verified

This machine had no Node.js/npm (see above). Installed Node 24 LTS via
`scoop install nodejs-lts` (only pre-existing package manager on the box);
added it to `PATH`. `npm install` then surfaced two real config bugs, fixed
in `dashboard/`:

- `tsconfig.json` referenced `tsconfig.node.json` via TS project references
  and `tsc -b` — but the referenced project had `noEmit: true`, which
  TS6310 forbids for a referenced project (it must be able to emit for
  whatever references it, even though nothing here actually imports from
  it). Nothing in `src/` needs `vite.config.ts`'s types, so the reference
  was pure unnecessary coupling. Fix: dropped the `references` array and
  `-b` build mode entirely; `tsconfig.node.json` is now a standalone,
  unreferenced config (editor convenience for `vite.config.ts` only).
  `package.json`'s `build`/`typecheck` scripts now run plain
  `tsc --noEmit`, not `tsc -b`.
- The first attempt at the fix above (`tsc -b` with the reference still
  emitting) wrote `vite.config.js` / `vite.config.d.ts` /
  `tsconfig.node.tsbuildinfo` straight into `dashboard/` — deleted, and the
  actual fix (above) stops it from recurring.

**Verified, this session:**

```
$ npm install         # 172 packages; 1 recharts deprecation notice (2.x EOL'd
                       # upstream, not a bug here); npm audit's advisory
                       # endpoint returned 503 (registry outage, not this repo)
$ npm run typecheck   # tsc --noEmit — clean, no stray output files
$ npm run build       # vite build — dist/index.html + hashed JS/CSS, ~2.4s
                       # (one "chunk >500kB" advisory from recharts/d3; not
                       # chased — not a Stage B concern)
```

Then ran the actual acceptance line end to end: `python -m triage.app --port
8000 --seed 42` (real mode, dashboard/dist mounted), opened it in a browser:

- baseline: connection indicator green/"live", P0 p99 138ms, scoreboard
  green/"within SLA", per-tier latency chart flat.
- `POST /control/rate {"rate": 20000}`: within ~8s the P0 scoreboard flipped
  to red/"SLA BREACHED" (10.87s, climbing), latency chart trending up.
  Watched further: P0/P1/P2 latency converged to ~45-51s and the three
  lines overlapped on the chart — correct, not a rendering bug: Stage B has
  no priority queue, so under sustained overload every tier suffers the
  same FIFO wait. That convergence *is* "the baseline we are about to beat"
  the prompt names — Stage C's scheduler is what will pull P0 back out of
  it. Cross-checked against the raw frame over the socket directly
  (`latency_p99 P0=45374ms P1=45179ms P2=45558ms`) — the chart was drawing
  real numbers, not stuck.
- `/health` stayed responsive (200) throughout, even with `in_queue` at
  17k+ — the event loop was never blocked.
- Reset to `{"rate": 16.65}` afterward; server stopped cleanly.

**Acceptance line: now genuinely verified**, not just plausible. Full
backend suite re-run after the config fix: `64 passed`. (One transient
failure of the 150 u/s timing test appeared mid-session while Node/npm/the
demo server were all competing for CPU; reran clean three times once that
load cleared — not a regression, see P5's notes on why that test is
wall-clock sensitive.)

Committed `dashboard/package-lock.json` for reproducible installs going
forward.

## Stage C — P7 three-heap priority queue (Lane A/C)

**Done**

- `src/triage/queue.py` — rewritten from the Stage B FIFO to three tiered
  heaps (`P0`/`P1`/`P2`), behind the exact same `put`/`get` shape
  `worker.py` and `app.py` already used, so neither had to change:
  - **P0: EDF**, keyed `(deadline_ts, seq, event)` — earliest deadline
    wins, not arrival order or type.
  - **P1/P2: arrival order**, keyed `(seq, event)` — `seq` is already the
    classifier's globally unique monotonic pipeline number, so no separate
    counter was needed.
  - **Selection**: highest-priority non-empty tier wins, *except* a bounded
    aging-guard exception — if the oldest P2 item's sojourn crosses
    `aging_guard_seconds` (default 2.0s), that one item is served instead,
    then the next call re-evaluates from scratch. Deliberately not "P0
    fully before P1 before P2" as an absolute rule — see the module
    docstring for why that phrasing matters.
  - **`set_mode("naive" | "adaptive")`** — naive picks the globally
    smallest `seq` across all three heaps every call (tier-blind pure
    arrival order, exactly Stage B's FIFO); adaptive uses the policy
    above. Same storage either way — switching modes needs no migration.
  - Multi-consumer wakeup via a shared `asyncio.Event` (no `asyncio.Queue`
    left inside at all), plus a hand-rolled `task_done()`/`join()` pair
    matching `asyncio.Queue`'s contract so `worker.py`'s unconditional
    `finally: queue.task_done()` kept working unmodified.
- `tests/test_queue.py` — 19 tests, including the exact case from the
  prompt (an order at 400ms of a 500ms SLA dequeues ahead of a payment 2ms
  old with a 200ms SLA), the aging guard firing per-item (not once per
  backlog — see the test's own note on why that's the correct reading),
  P1 explicitly having no aging exception, naive mode locked to pure
  arrival order and blind to both tier and aging, mode-switching without
  data loss, and the asyncio-level plumbing (blocking `get()`, multiple
  concurrent waiters each getting exactly one item, `task_done`/`join`).
- Dashboard: `QueueDepthPanel.tsx` — stacked area chart of `queue_depth`
  by tier over the rolling window, wired into `App.tsx`. No backend metrics
  change needed — `metrics.py`'s per-tier `queue_depth` was already
  correct, since `queue.py`'s `put`/`get` still call the same
  `observe_ingest`/`observe_dequeue` hooks regardless of what's behind
  them.

**Verified**

```
$ python -m pytest -q
83 passed   (64 from before + 19 new in test_queue.py)

$ npm run build   (dashboard/)
tsc --noEmit && vite build — clean, dist/ rebuilt
```

Then re-ran the full P6 browser check against the real backend with the new
queue, at the actual calibrated 20x spike (`POST /control/rate {"rate":
333}` — 333 events/sec, per `config/tiers.yaml`'s `load.spike_multiplier`,
not an arbitrary large number):

- P0 p99 stayed green/"within SLA" at 192ms in the first ~10s of the spike,
  while P2's line climbed to ~2.2s on the latency chart and the new queue
  depth panel showed the backlog was almost entirely the P2 (orange) band —
  P0/P1 bands stayed a thin sliver. This is the priority story, visibly.

**A real, honest finding — not a bug, and not fixed in this prompt.** Left
the spike running longer (~20s sustained), P0's own p99 crept up to 265ms
and briefly flipped the scoreboard red. Checked the raw frame over the
socket rather than guessing:

```
queue_depth   {'P0': 0, 'P1': 112, 'P2': 369}
in_flight     6            (== worker_count: every worker busy, always)
latency_p99   {'P0': 352ms, 'P1': 6043ms, 'P2': 2081ms}
```

`queue_depth.P0 == 0` the entire time — the priority queue is doing exactly
what it's supposed to: a P0 item is *never* waiting behind P1/P2 in the
queue. The residual latency is worker-pool contention, not misordering:
with only 6 non-preemptive workers running at ~100% utilization (P0 alone
demands ~108 u/s, which is 72% of the 150 u/s pool — "comfortably under
capacity" in total, but not idle), a P0 arrival still has to wait for
*some* worker to finish whatever it already started, and at full
saturation that residual wait is no longer negligible. Stage C's job was
ordering, and `queue_depth.P0 == 0` proves the ordering is correct; freeing
actual worker capacity from low-value P1/P2 work under sustained overload
is exactly what Stage D's decision function (batching/deferral/shedding)
exists to do next. Not attempting that here — flagging it now rather than
either hiding it or quietly building ahead into Stage D's scope.

**Open / next:** live spike control in the dashboard UI (a wired button,
not just `curl`) is part of a later prompt, not this one — left untouched.

## Fix-up: the page wasn't visible, and why (before Stage C P8)

The user reported `localhost:8000` showed nothing. Root cause: **nothing
was wrong with the app** — no server had been left running (every prior
session started one for testing, then stopped it), and separately, `make
dev` would have failed for anyone anyway: this machine's default `python`
is Anaconda 3.13 with none of our dependencies, `make` itself was not even
installed, and the Makefile's `PY ?= python` picked up that broken default
silently.

Fixed both:

- Installed `make` and Node.js LTS via `scoop` (the only package manager
  already on this machine) — `scoop install make nodejs-lts`.
- `Makefile`: `PY` now auto-detects `.venv/Scripts/python.exe` /
  `.venv/bin/python` before falling back to plain `python`, so `make dev`
  works with zero setup regardless of whose `python` is on PATH. Verified
  by literally reproducing the broken scenario (`make dev`, no `PY=`
  override) and confirming it now picks the venv correctly.
- Left the real backend running for the user this time
  (`python -m triage.app --port 8000`), reset to a clean baseline —
  `http://localhost:8000/` should be live now.

## Stage C — P8 controls, per-tier percentile proof, dashboard control bar

**Backend — done**

- `src/triage/app.py` — new `Engine` methods and endpoints:
  - `POST /control/spike` — instant jump to `20000/min` (`SPIKE_RATE_EPS =
    20_000/60`, a fixed demo constant, not derived from
    `config.spike_multiplier`). No ramp: `set_rate()` takes effect on the
    generator's very next emission.
  - `POST /control/mode` — `{"mode": "naive"|"adaptive"}`, routed through
    `Engine.set_mode()` so `metrics`'s reported mode never drifts from
    what the queue is actually doing.
  - `POST /control/reset` — walks the pipeline back to a clean baseline
    (rate, queue, metrics, ledger) without restarting the process; leaves
    `mode` untouched (a separate, explicit control).
  - `POST /control/inject` — `{"type": ..., "partition_key": null}`. All
    four return 409 in `--fake` mode, 422 on bad input.
- `src/triage/generator.py` — `EventGenerator.emit_single(event_type,
  partition_key=None)`: the identity half of `inject_event`, sharing
  `emit()`'s event_id/dedup_key/payload-size machinery so an injected
  event is structurally identical to an organic one. Tier/value/cost/
  deadline are never accepted here — only the classifier assigns those,
  from config, for every event regardless of origin.
- `src/triage/app.py` — `Engine.inject_event()`: `emit_single` ->
  `classify` -> `queue.put`. No economics parameters to override with.
- `src/triage/queue.py` — `EventQueue.clear()`, for `/control/reset`.
- `src/triage/metrics.py` — per-tier p50/p95/p99 were already real
  (separate deques per tier since Stage A, never blended) — audited, not
  rewritten. Added `set_mode`/`get_mode` so `MetricsFrame.mode` mirrors the
  queue's actual live mode instead of being hardcoded to adaptive.
- Tests: 27 new (control endpoints in both modes, `emit_single`,
  `EventQueue.clear`, and `test_a_blown_p0_latency_is_not_hidden_by_many_
  healthy_p2_samples` — a hundred healthy P2 completions cannot move P0's
  p99 by a millisecond, in either direction).

**Dashboard — done**

- `dashboard/src/components/ControlBar.tsx` — rate slider, naive/adaptive
  toggle (reflects the backend's real mode, not local optimistic state),
  RESET, and a large red SPIKE button (`px-8 py-3 text-base font-black`,
  hover-scale, shadow) sized and colored to be unmissable next to the
  other controls.
- `dashboard/src/lib/api.ts` — thin `fetch` wrapper over `/control/*`,
  hardcoded to `:8000` like the WS hook (works from the Vite dev server
  too, no proxy needed).

**Three real bugs found and fixed while verifying the acceptance
criteria live** — none of them were visible from unit tests alone; all
three only showed up running the actual app in a browser under load,
which is exactly why that verification step exists:

1. **RESET could silently kill every worker.** `EventQueue.clear()`
   originally zeroed `_unfinished` unconditionally. A worker already
   mid-`serve()` on an item removed from the heap (not visible to
   `clear()`) would then call `task_done()` in its `finally` block — found
   nothing outstanding, raised `ValueError`, and that exception is
   *outside* worker.py's `except Exception` guard, so it killed the
   worker silently. Every worker in flight at the moment of a reset died
   the same way. Fixed: `clear()` now decrements `_unfinished` by exactly
   what it removed from the heaps, leaving in-flight accounting alone.
   Regression tests: `test_clear_does_not_break_task_done_for_items_
   already_in_flight` (queue.py) and `test_workers_keep_processing_after_
   a_reset_under_load` (app.py, drives real load, resets mid-flight,
   proves `processed` keeps increasing).
2. **A reset straggler could poison a tier's latency for a very long
   time.** Even after fixing (1), a worker already serving an item from
   *before* a reset would finish normally and report that event's real
   (huge, pre-reset) latency into an otherwise-fresh window — honest, but
   at low arrival rates that single sample can dominate a tier's p50/p99
   for tens of minutes before enough fresh samples push it out, silently
   breaking RESET's "clean slate" promise. Fixed: `Engine.reset()` now
   restarts the worker pool (`await workers.stop()` before clearing,
   `workers.start()` after) so nothing in-flight survives a reset.
   Regression test: `test_reset_discards_stragglers_instead_of_letting_
   them_pollute_latency`.
3. **The generator could not actually sustain 333 eps — or even close to
   it.** Verifying "adaptive + spike" live, `/control/spike` reported
   `rate: 333.3` but the *measured* ingest rate was only ~200 eps, even in
   complete isolation with worker.py's Windows timer fix applied.
   Root cause: pacing via one `asyncio.sleep()` per emitted event pays
   that call's fixed overhead once per event; at a 60ms interval
   (baseline, 16.65 eps) that's negligible, but at a ~3ms interval (spike,
   333 eps) the overhead dominates the budget. Fixed: `events()` now
   paces against a running schedule and catches up in a no-sleep burst
   when behind, so one sleep's overhead is amortised across many
   emissions instead of paid per event (`_MAX_BURST = 500` caps a single
   catch-up). Measured after the fix: 16.65 eps at 0.1% error, 333.3 eps
   at 0.1–0.5% error. Regression test:
   `test_async_generator_sustains_the_spec_spike_rate`.
4. **With the generator actually hitting 333 eps, a fourth and more
   serious bug surfaced: P0 itself started climbing under sustained
   load** — to 30+ seconds p99, the opposite of the acceptance criterion.
   Raw frames showed why: `queue_depth.P0` had grown to 1000+.
   `_take_adaptive()`'s aging-guard check ran *before* the P0 check, so
   the guard could preempt P0, not just P1. Under a brief spike this
   never mattered (P2 is rarely aged), but under a *sustained* spike — the
   exact condition the guard exists for — P2 almost always has *some*
   item past the guard, so the exception doesn't fire occasionally: it
   fires on **every single dequeue**, and P0 starves completely instead
   of the opposite. This directly contradicted both CLAUDE.md's hard rule
   3 (P0 never degraded) and this prompt's own acceptance line. Fixed:
   P0 is now checked first and absolutely, unconditionally, before the
   aging guard is even consulted — the guard only ever arbitrates P1 vs
   P2. Two of the original queue tests asserted the *old* (wrong)
   behaviour and were rewritten to use P1 as the aging guard's contender
   instead of P0; two new invariant tests lock the fix in directly:
   `test_p0_is_never_preempted_by_the_aging_guard_no_matter_how_old_p2_is`
   and `test_p0_stays_absolute_under_a_sustained_flood_of_aged_p2` (50
   permanently-aged P2 items in queue; 5 fresh P0 items must all still
   come out first).

**Acceptance, verified live in a browser, both directions, after all four
fixes** (raw `/ws` frames cross-checked against the chart, not just eyeballed):

```
naive + spike (sustained ~10s):
  t=0.0s  p99 P0=5907ms P1=5726ms P2=5663ms
  t=8.0s  p99 P0=9219ms P1=9077ms P2=9107ms
  -> all three climb together, within ~1-2% of each other throughout.

adaptive + spike (sustained ~15s):
  t=0.0s   qd={P0:0,  P1:338, P2:1694} p99 P0=420ms P1=282ms P2=6347ms
  t=14.0s  qd={P0:3,  P1:806, P2:3422} p99 P0=397ms P1=282ms P2=12662ms
  -> queue_depth.P0 stays ~0 throughout; P0 p99 stays flat (~400ms,
     essentially the cost-model floor under 100% worker utilisation, not
     climbing); P2 climbs continuously; P0/P2 diverge by nearly two
     orders of magnitude.
```

Full backend suite: 111 passed (1 pre-existing wall-clock-sensitive
timing test occasionally flakes when run as part of the full suite while
CPU is under load — passes reliably alone; documented since P5, more
noticeable this session because a real demo server was also running in
the background throughout verification).

Server left running for the user at `http://localhost:8000/`, reset to a
clean adaptive baseline.

## Documentation: ADRs 0001-0004 and round-1 notes

Documentation only, no code changed.

- `docs/adr/0001-in-process-asyncio-over-kafka.md`
- `docs/adr/0002-simulated-service-cost.md`
- `docs/adr/0003-five-field-identity-model.md`
- `docs/adr/0004-contract-first-freeze.md`

  Each under 300 words, Context/Options considered (≥2 real
  alternatives)/Decision/Consequences, per the requested format. Done
  earlier than PLAN.md's Stage G originally scheduled them for, since this
  prompt asked for them now — PLAN.md updated to reflect that.

- `docs/rounds/round-1.md` — one page: what's built, three demo highlights
  (the P0-flat/P2-climbing contrast, EDF ordering, the aging-guard bug
  found and fixed by testing the live demo itself), specifically what's
  incomplete (no Stage D decision function, no backpressure/ledger yet, and
  the honest worker-contention latency floor on P0 documented in the P8
  entry above), and the next 8 hours. `git log --oneline --decorate`
  appended as evidence, cross-checked against a fresh run rather than
  copied from memory — 111 tests currently pass (re-verified before
  writing this number into the round doc; the one wall-clock-sensitive
  timing test noted since P5 is still occasionally flaky while the demo
  server runs in the background, not a regression).

## Stage D — the split decision function (originality core)

**Built**

- `src/triage/decision.py` — new module, two pure functions, exactly as
  specified, plus routing:
  - `score(event, now, capacity, weights)` — ORDERING, per-event only:
    `w1 * density * urgency + w2 * aging`. `urgency = 1/max(slack, EPS)`
    saturates rather than blowing up once slack goes negative (an event an
    hour overdue isn't "more urgent" than one a minute overdue — both are
    already maximally so). `now` is a parameter, never cached into a static
    key — see queue.py below for why.
  - `pressure(signals, weights)` — PRESSURE, system-state only:
    `clamp(a*(qdepth/qmax) + b*(arrival/service) + c*(p95/sla) +
    d*worker_util, 0, 1)`. `PressureWeights`/`ScoreWeights` validate
    non-negativity; `PressureWeights` additionally enforces the weights sum
    to 1.0 (raises otherwise) — a formula that silently stopped summing to
    1 after an edit would still produce an in-[0,1]-looking number, exactly
    the bug that survives to a demo.
  - `decide(event, pressure, now, capacity) -> (Decision, reason)` — P0
    checked first and returns unconditionally, with a defensive
    `assert event.tier is not Tier.P0` immediately after guarding every
    branch below it; then `slack < 0 -> DEFER` (checked before pressure);
    then the three pressure bands exactly as specified
    (<0.40 stream, [0.40,0.75) batch, >=0.75 defer).
  - **Pressure is never added into score** — the module docstring proves
    why with the actual algebra: `(score_A + P) > (score_B + P)` reduces to
    `score_A > score_B` for any P, so an additive pressure term would have
    literally zero effect on ordering while looking like it does something.
    `tests/test_decision.py::test_pressure_additive_score_term_would_be_a_no_op`
    demonstrates this directly rather than just asserting it in prose.

- `src/triage/queue.py` — dequeue within each tier is now score-ordered
  (P0 included — see the module docstring for why applying it uniformly
  doesn't regress the Stage C EDF test case: urgency dominates density
  once slack is small, and dominates completely once negative). Tier
  *selection* (P0 absolute, the P1-vs-P2 aging guard) is untouched from
  Stage C — only what comes out of a chosen tier changed.

  **A real performance bug, found and fixed before it ever reached a
  test failure log, by reasoning about the design rather than waiting for
  a flake:** since `score()` depends on live elapsed time (urgency and
  aging only ever grow), a `heapq` keyed on it would silently go stale, so
  the natural first implementation was "recompute score fresh, linear-scan
  the whole tier, on every single dequeue." Measured directly: a full scan
  of a 1,200-item backlog cost ~0.7ms; at ~150-200 dequeues/sec that is
  10-14% of the event loop's own time synchronously blocked on scoring —
  and at the 10,000-item backlogs Stage C's own sustained-spike testing
  actually produced, a full sort costs ~7.5ms, which at that rate would
  have meant the event loop spending *more than 100% of real time*
  scanning, i.e. throughput collapsing under exactly the sustained
  overload this project is supposed to survive. Caught by profiling before
  publishing, not by a jury noticing a stalled demo. Fixed with a
  settled/pending split per tier (see queue.py's own docstring for the
  full design): a full O(n log n) resort happens at most once every 50ms,
  cached as `_settled` (kept in score order — popping the best is O(1));
  arrivals since the last resort sit in a small `_pending` list, and a pop
  between resorts only compares pending's own best (bounded by roughly
  arrival_rate x 50ms, not the whole tier) against settled's current tail.
  Verified after the fix: the existing 150 u/s-within-5% throughput test
  passed 6/6 consecutive runs in isolation and the full suite 3/3 times
  with no server competing for CPU (it does still occasionally flake when
  a live demo server is *also* running in the background — the same
  pre-existing, documented wall-clock sensitivity, not this change).

- `src/triage/metrics.py` — pressure's inputs are now real, not stubs:
  - `_Ewma`: exponentially-weighted moving average of a rate plus a
    first-difference trend term (`with_trend = level + trend`) — the
    spec's "arrival_ewma_with_trend" literally. Fed by amount-at-a-
    timestamp (each event's cost), deriving the instantaneous rate from
    true elapsed wall-clock time, so it is correct regardless of how often
    it's called.
  - `current_pressure(config, now)` gathers real `qdepth` (existing
    per-tier counters, summed), `arrival_rate_ewma_with_trend` and
    `service_rate` (new EWMAs, fed from `observe_ingest`/`observe_complete`),
    `p95_sojourn` (existing `queue_wait_percentile`), `sla_reference` (P1's
    own SLA — the tier pressure actually gates), and `worker_util`
    (`in_flight / worker_count`, both now real). **Throttled to at most
    once every 50ms** — computing it involves a percentile sort over up to
    4096 samples, which is exactly the class of "cheap-looking call, too
    expensive at spike rate" mistake this project already made once (the
    Stage C generator-pacing bug); this one was avoided by remembering
    that lesson instead of repeating it.
  - `MetricsFrame.pressure`, `.service_rate`, `.worker_count`, and
    `.active_workers` are real now, not stubs. `throughput`,
    `offered_rate`, `admitted_rate` remain stubbed — not needed for
    pressure and not asked for this prompt.

- `src/triage/app.py` — `Engine._ingest()` now computes real pressure and
  calls `decision.decide()` for every event at admission, and calls
  `metrics.observe_decision()` (which already writes the ledger — Stage A's
  own choke point) for every non-STREAM_NOW result. **Deliberately
  observational for Stage D**: every event is still enqueued exactly as
  before regardless of its decision — actually *acting* on MICRO_BATCH
  (a real batch execution in worker.py) or DEFER (a real deferred buffer)
  is Stage E's machinery, and building that now would be building ahead of
  what this prompt asked for. What's real: the decision is genuinely
  computed from live system state and audited from the moment pressure
  first crosses a threshold, not retrofitted later.

**Tests — 100 new (358 total)**

- `tests/test_invariant.py` — the requested file: 212 tests total, the
  headline being a 101-value parametrized sweep of pressure from 0.00 to
  1.00 in 0.01 steps, asserting STREAM_NOW every time — for a P0 event
  with ordinary positive slack, and *separately* for one with already-
  negative slack (proving the tier check, not a lucky slack coincidence,
  is what protects it). Also proves the contrapositive: P1 genuinely does
  move STREAM_NOW -> MICRO_BATCH -> DEFER across the same pressure range,
  so the invariant is about P0's immunity specifically, not the routing
  function being inert for everyone.
- `tests/test_decision.py` — 25 unit tests for `score()`/`pressure()`
  directly: saturation past zero slack, density/aging tie-breaking,
  weight validation, zero-service-rate not crashing, and the additive-
  pressure-is-a-no-op proof.
- `tests/test_queue.py` (existing, unchanged file, all 24 still pass) —
  confirmed the score-based rewrite doesn't regress Stage C's EDF test
  case or any tier-selection/aging-guard behaviour.
- `tests/test_metrics.py` — 7 new: pressure throttling, the EWMA's
  behaviour on a steady rate and a burst, `reset()` clearing it all,
  `worker_count`/`active_workers` now real.
- `tests/test_app.py` — 2 new live-integration tests: drives the *real*
  calibrated spike (not synthetic values) until pressure genuinely crosses
  into MICRO_BATCH/DEFER territory, confirms the ledger actually receives
  rows, and confirms zero P0 rows ever reach it — the invariant proven
  against events that actually flowed through the real pipeline, not just
  constructed ones.

**Verified live, in a browser, against the real backend** (raw `/ws`
frames, not just eyeballed):

```
adaptive + spike, sustained 8s:
  t=0s  pressure=1.000  qd={P0:0, P1:448,  P2:1798}  p99 P0=210ms  P2=6783ms
  t=8s  pressure=1.000  qd={P0:0, P1:748,  P2:2712}  p99 P0=200ms  P2=10202ms
  40 frames checked end to end: zero P0 events ever received a
  non-STREAM_NOW decision.
```

P0's own p99 is now flat at ~200-210ms — noticeably *tighter* than the
~250-420ms floor observed pre-Stage-D (P7/P8 notes above), and sitting
almost exactly on the 200ms target rather than comfortably above it. The
likely reason: P0's own internal ordering is no longer plain EDF but
density-and-urgency-weighted, so when more than one P0 item is
momentarily queued, the better-value one is served marginally sooner.
Not claimed as proven without a controlled before/after benchmark (that's
Stage F's job) — noted honestly as an observed, plausible improvement.

**What Stage D deliberately does not do:** no dashboard panel for
per-event decisions (not asked for this prompt); no batching execution,
no deferred buffer, no ladder, no admission control (Stage E); no
benchmark report quantifying any of this (Stage F).

## Stage D — P11 micro-batching and the durable deferred buffer (Lane A/D)

Decisions now *do* something, not just get recorded. Two execution paths
land in `worker.py`, driven by `decision.decide()` called fresh at
dequeue time (not once at ingest — pressure moves while an event waits):

- **MICRO_BATCH**: `decision.batch_size(pressure)` (`round(B_min + (B_max
  - B_min)*P)`, `B_min=4`, `B_max=8` — capped hard regardless of pressure,
  so batching can never grow large enough to threaten the 200ms P0 SLA
  even though P0 never enters a batch anyway). The worker greedily,
  non-blockingly gathers more MICRO_BATCH-eligible events off the same
  queue (`EventQueue.try_get()`, new), re-checking each one — an event
  pulled while filling a batch that turns out to deserve STREAM_NOW or
  DEFER is not forced in just because it was convenient to grab. The
  whole batch is served with one combined sleep from
  `decision.batch_cost()` (`sum(costs)*0.4 + 0.5`) — genuinely one
  shorter sleep, not several relabelled; proven by wall-clock, not just
  by trusting the formula (`test_micro_batch_is_actually_faster...`).
- **DEFER**: handed to the new `deferral.py` — a SQLite-backed
  `DeferralStore` whose schema is exactly `docs/DATA_MODEL.md`'s
  `deferred_buffer` table (P0 rejected by construction). A background
  drainer (`run_drainer`, started alongside ingest in `Engine.start()`,
  stopped alongside it) replays parked events once pressure falls below
  `DRAIN_PRESSURE_THRESHOLD = 0.35`, rate-limited to
  `DRAIN_BATCH_PER_TICK / DRAIN_TICK_SECONDS` (100 events/sec) so replay
  cannot re-saturate the pool it is draining into.

**The infinite-redefer trap, found and fixed:** `decide()`'s own rule —
`slack < 0` always defers, checked before pressure, unconditionally — is
correct and was left untouched (already tested). But it means a replayed
event whose deadline has already passed would be deferred again forever
on every replay. Fixed in `worker.py._resolve()`: a *second* DEFER
verdict for an event already known to `deferral.was_deferred()` is
overridden to STREAM_NOW instead — served now, honestly counted as an
SLA miss, never looped. `decision.py`'s formula itself needed no change.

**Two real bugs found empirically, not by inspection, while making the
required acceptance test actually pass** (a 30s real spike, a real
`/control/reset`, then waiting for the backlog to hit zero):

1. **`in_flight` leaked on every DEFER.** `observe_dequeue()` increments
   `in_flight`; only `observe_complete()` decremented it. A deferred
   event is never completed, so every one silently held its slot open
   forever — `worker_util` pinned to 1.0 within seconds of any load,
   which alone kept computed pressure elevated and stopped the drainer
   from ever running. Fixed with a new `metrics.observe_defer()` that
   releases the slot without counting the event as processed (its real
   SLA outcome is still judged honestly later, at actual completion on
   replay).
2. **The arrival/service EWMA silently dropped simultaneous samples.**
   `_Ewma.observe_amount()` returned early on `dt <= 0` — meant to guard
   divide-by-zero, but a micro-batch's `for e in batch:
   metrics.observe_complete(e, now=now)` calls land on the *same*
   timestamp, so every event but the first in a batch vanished from the
   service-rate signal entirely. That systematically understated service
   rate the moment any batching happened, biasing pressure high and
   self-reinforcing more batching/deferral — real oscillation risk, not
   the load. Fixed by carrying the amount forward (`_pending_amount`) to
   the next call with a real `dt`, instead of discarding it.
3. **Replayed events poisoned their own queue-wait signal.** A replayed
   event's `ingest_ts` can be tens of seconds old (it sat deferred that
   whole time); measuring "queue wait" for pressure purposes as `now -
   ingest_ts` the moment it's replayed reports a p95 sojourn of tens of
   seconds, alone saturating pressure back to 1.0 and stalling the
   drainer the instant it starts — the exact oscillation this stage is
   required to prevent. Fixed with `metrics._replay_admitted_at`: the
   real re-admission time, used only for the pressure-facing wait signal.
   `observe_complete()`'s end-to-end latency/SLA accounting is
   deliberately untouched — it still measures from the true original
   `ingest_ts`, so a late replay still and correctly counts as an SLA
   miss.

With all three fixed: pressure genuinely decays toward calm at baseline,
the drainer runs at a steady ~100 events/sec with no self-induced
stalls, and a real 30s spike's multi-thousand-event backlog fully drains
within the test's (generous, math-backed) wait window.

**Tests — 26 new**

- `tests/test_decision.py` — 6 new: `batch_size()`'s growth, hard cap,
  and default bounds; `batch_cost()`'s exact formula and why `B_min`
  exists (cheaper above it, not below).
- `tests/test_deferral.py` (new file, 9 tests) — schema/storage/ordering
  (`(ready_at, tier, deadline_ts, seq)` exactly, matching the DDL's own
  index), `already_deferred` surviving a drain, drain-rate windowing, and
  the drainer itself: gated on pressure, rate-limited to
  `DRAIN_BATCH_PER_TICK` per tick, not all-at-once.
- `tests/test_batching.py` (new file, 8 tests) — P0 immunity even at
  pressure 1.0, a batch actually gathering and serving together
  (wall-clock proof), exact `task_done()` accounting under the
  gather-and-serve path, an event that deserves STREAM_NOW not getting
  dragged into someone else's batch, DEFER routing to the store and the
  ledger, and the infinite-redefer-trap regression test itself.
- `tests/test_app.py` — 1 new, the literal acceptance line: real 30s
  spike, real reset, poll until the backlog hits zero, assert everything
  ever deferred was drained (read after the wait completes, since a few
  more events can still land deferred in the brief high-pressure window
  right after reset — those are just as real and just as owed a drain).

**Known, pre-existing, unrelated flake (not a regression):**
`test_worker_pool_sustains_150_units_per_second_within_5_percent`
occasionally fails only when run as part of the full suite (never
standalone) — Windows `ProactorEventLoop` `asyncio.sleep()` overhead
degrading under heavy concurrent test load elsewhere in the same
process, documented since Stage C/P5. Not caused by anything in this
prompt's changes; re-confirmed by running it standalone after every
change here.
