# PULSE — plan

Nine stages, A through I, across 30 hours. A stage is done when every box under
it is ticked and the repo still runs. Jury rounds are hard deadlines; the repo
must be demoable at each one.

| Stage | Hours | Outcome | Jury |
|---|---|---|---|
| A | 0–1 | Contract frozen | |
| B | 1–4 | Vertical slice runs | |
| C | 4–7 | First demoable state | Round 1 (~H8) |
| D | 7–10 | Adaptive engine live | |
| E | 10–12.5 | Backpressure, ladder, ledger | |
| F | 12.5–14 | Benchmark + report | |
| G | 14–16 | Polish, freeze, rehearse | Round 2 (~H16) |
| H | 16–23 | Fault tolerance, chaos, dedup | Round 3 (~H23) |
| I | 23–30 | Scaling, ordering, cost learner | Round 4 (~H30) |

---

## Stage A — Contract lock (H0–1)
- [x] `contracts.py`: Event (five identity fields), Decision enum, MetricsFrame with every future field defaulted
- [x] `metrics.py`: observe_ingest / observe_dequeue / observe_complete / observe_decision / snapshot; real latency percentiles
- [x] `ledger.py`: `record(...)` stub with call sites in place
- [x] `config/tiers.yaml`: tier table, mix, worker capacity + loader
- [x] `fake_metrics.py`: plausible frames at 4 Hz for the dashboard to build against
- [x] `tests/test_contracts.py`: round-trip serialisation
- [x] `docs/DATA_MODEL.md`: identity model, envelope, SQLite DDL, rollups, hash chain, ER diagram
- [ ] Team review, then **freeze** `contracts.py` and `config/tiers.yaml`

## Stage B — Vertical slice (H1–4)
- [x] `queue.py`: single FIFO, instrumented
- [x] `worker.py`: pool with simulated service time from the cost model
- [x] `generator.py`: mix-correct event stream, baseline rate
- [x] `classifier.py`: assigns seq, idempotency_key, cost, deadline
- [x] `sink.py`: terminal write
- [x] `app.py`: FastAPI + WebSocket metrics at 4 Hz + static dashboard mount
- [x] `dashboard/`: real UI against the frozen MetricsFrame
- [x] `make dev` runs the whole slice end to end

## Stage C — First demoable state (H4–7) — Round 1
- [x] `queue.py`: three heaps, per-tier depth (EDF within P0, bounded P2
      aging guard, naive/adaptive mode switch)
- [x] Generator: live spike control (baseline ↔ 20x) — `/control/spike`,
      `/control/mode`, `/control/reset`, `inject_event`
- [x] Dashboard: per-tier stacked queue depth panel
- [x] Dashboard: control bar (rate slider, SPIKE, RESET, naive/adaptive)
- [x] Round 1 notes in `docs/rounds/`
- [x] ADRs 0001-0004 (in-process asyncio, simulated cost, five-field
      identity, contract-first freeze) — done early, ahead of Stage G

## Stage D — Adaptive engine (H7–10)
- [x] `decision.py`: split decision function (score/pressure, never combined),
      real pressure signal wired from live EWMAs, per-event reason string
- [x] queue.py: score-ordered dequeue within each tier (replaces EDF/arrival)
- [x] P0-never-shed (non-STREAM_NOW) asserted in tests — 212-step invariant
      sweep plus live-spike integration tests
- [ ] Decisions visible per event in the *dashboard* — backend records them
      (ledger + recent_decisions), no frontend panel yet; not asked for in
      this prompt, left for the next one that asks for it
- [x] `worker.py`: MICRO_BATCH actually executed — greedy non-blocking
      gather up to `decision.batch_size(pressure)` (capped B_max=8),
      served with one combined `decision.batch_cost()` sleep, proven
      genuinely cheaper by wall-clock

## Stage E — Backpressure, ladder, ledger (H10–12.5)
- [ ] `codel.py`: queue-latency controller
- [ ] `ladder.py`: degradation rungs with hysteresis
- [ ] `admission.py`: credit-based admission, source throttling
- [x] `deferral.py`: park and drain — SQLite-backed store matching
      `docs/DATA_MODEL.md`'s `deferred_buffer` schema, pressure-gated
      background drainer, rate-limited to ~100 events/sec; done ahead of
      its planned slot because the driving prompt (P11) asked for it
      alongside micro-batching under Stage D — see PROGRESS.md
- [ ] `ledger.py`: real hash-chained audit ledger

## Stage F — Benchmark + report (H12.5–14)
- [ ] `bench/run.py`: headless adaptive vs naive runs
- [ ] Conservation equation balances (ingested = processed + in-flight + deferred + sampled + shed)
      — the deferred/drained corner of this is already proven under real
      spike load in `tests/test_app.py` (P11); the full equation across
      every bucket at once is still this stage's job
- [ ] `docs/BENCHMARK.md` with numbers

## Stage G — Polish, freeze, rehearse (H14–16) — Round 2
- [ ] Dashboard final layout
- [ ] `docs/ARCHITECTURE.md` (ADRs 0001-0004 already done, see Stage C)
- [ ] Code freeze, demo rehearsal
- [ ] Round 2 notes in `docs/rounds/`

## Stage H — Fault tolerance, chaos, dedup (H16–23) — Round 3
- [ ] Idempotent retry path
- [ ] `dedup.py`: dedup_key-based duplicate suppression
- [ ] `chaos.py`: injectable faults wired into the demo
- [ ] Round 3 notes in `docs/rounds/`

## Stage I — Scaling, ordering, cost learner (H23–30) — Round 4
- [ ] `ordering.py`: per-partition ordering guarantees
- [ ] Worker scaling under load
- [ ] Cost learner / adaptive cost model
- [ ] Final report + Round 4 notes in `docs/rounds/`
