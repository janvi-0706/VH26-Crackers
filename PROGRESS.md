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
