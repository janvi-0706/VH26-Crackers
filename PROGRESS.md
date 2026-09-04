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

- `docs/DATA_MODEL.md` not written yet — next prompt.
- Contract is not frozen until all four have read it and the team review
  happens. Raise missing fields now; a field added here is free.
- Stage D will need a publish path for the stubbed gauges (pressure, ladder
  rung, worker counts). Decide then whether that is a setter on `metrics` or a
  callback the engine registers — not decided yet, deliberately.
