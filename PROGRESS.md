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
