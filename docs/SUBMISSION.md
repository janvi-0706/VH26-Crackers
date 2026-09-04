# PULSE — submission

**Repository**: https://github.com/janvi-0706/VH26-Crackers

An event pipeline that survives a 20x traffic spike by triaging, not
scaling — one Python process, no Kafka/Redis/Celery, a hand-written
scheduler in front of a fixed 6-worker pool. `README.md` (at the repo
root) is the one-minute version: setup, and an explicit "what is real vs.
simulated" table. This page is the index everything else hangs off.

## Architecture

- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — two diagrams, not one: a
  **component graph** (what depends on what, and why `contracts.py` and
  `codel.py`/`dedup.py`/`checkpoint.py` sit at the bottom with zero
  project imports), and a **control loop** (pressure → admission →
  ladder, drawn as a feedback system, plus the Stage I addition — a third,
  deliberately slower loop for the learned cost model). A "why the module
  boundaries are where they are" section answers, in writing, the
  question a judge would otherwise have to ask live.
- [`docs/DATA_MODEL.md`](DATA_MODEL.md) — the five-field identity model
  (`event_id`/`dedup_key`/`seq`/`partition_key`/`idempotency_key`), every
  schema, every index, and the conservation equation this whole system is
  built to keep true.

## Decision record

[`docs/adr/`](adr/) — eleven ADRs, each Context / Options considered /
Decision / Consequences, dated in build order:

| # | Decision |
|---|---|
| [0001](adr/0001-in-process-asyncio-over-kafka.md) | In-process asyncio over Kafka |
| [0002](adr/0002-simulated-service-cost.md) | Simulated service cost for deterministic capacity |
| [0003](adr/0003-five-field-identity-model.md) | Five-field identity model instead of one id |
| [0004](adr/0004-contract-first-freeze.md) | Contract-first: freeze schemas before implementation |
| [0005](adr/0005-split-ordering-and-pressure-functions.md) | Split ordering and pressure functions instead of one additive score |
| [0006](adr/0006-sojourn-aqm-over-queue-length.md) | Sojourn-time AQM (CoDel) instead of queue-length thresholds |
| [0007](adr/0007-sample-with-weight-instead-of-drop.md) | Sample-with-weight instead of drop |
| [0008](adr/0008-hash-chained-audit-ledger.md) | Hash-chained audit ledger |
| [0009](adr/0009-write-ahead-checkpoint-over-full-transaction-log.md) | Write-ahead checkpoint over a full transaction log |
| [0010](adr/0010-bloom-lru-over-persistent-dedup-store.md) | Bloom filter + LRU over a persistent dedup store |
| [0011](adr/0011-online-cost-learning-over-static-or-bandit.md) | Online cost learning over static constants, and over a bandit |

## Proof, not narration

- [`bench/report.md`](../bench/report.md) / [`bench/report.html`](../bench/report.html)
  — `make bench`'s own headless output. Six configs (the original
  naive/adaptive × baseline/spike four, plus two Stage I chaos variants —
  a real worker killed mid-spike, a real 1000-event duplicate flood
  mid-spike), a 5x/10x/20x/40x sensitivity sweep showing exactly where
  the system stops holding, and `exactly_once_violations` as a column
  that reads 0 in every row, chaos rows included.
- `make test` — 1,000+ tests, organised by claim as much as by module
  (`tests/test_stage_g_claims.py` names each one after the exact sentence
  it proves). `docs/QA.md` cites the specific test or ADR behind every
  answer a jury is likely to ask for.
- [`docs/DEMO.md`](DEMO.md) — the rehearsed 5-minute script, minute by
  minute, ending on the one honest sentence about what's simulated.
- [`docs/QA.md`](QA.md) — seven jury questions, answered in under 100
  words each, every answer citing a real file or number.

## Progress, round by round

[`docs/rounds/`](rounds/) — one document per jury round, each stating
what was built, what's being shown, and what's honestly still
incomplete, closing with the exact `git log` range that round covers:

- [Round 1](rounds/round-1.md) — end of Stage C: the priority queue exists, the decision function doesn't yet.
- [Round 2](rounds/round-2.md) — end of Stage H (`v1-jury`): the full adaptive control loop, the audit ledger, the benchmark harness, the dashboard's final layout.
- [Round 3](rounds/round-3.md) — end of Stage I's engineering: exactly-once worker recovery, ingest-time dedup, the learned cost model.
- [Round 4](rounds/round-4.md) — this round, final (`v2-final`): chaos proven headlessly in the benchmark, the complete ADR set, this page.

## Tags

`stage-b`, `stage-c`, `v1-jury` (end of the core build), `v2-final`
(this submission) — `git checkout <tag>` reproduces exactly what that
round's own document describes.
