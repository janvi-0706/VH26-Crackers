# PULSE benchmark report

Six configs (the original naive/adaptive x baseline/spike four, plus two Stage-I chaos variants — adaptive-spike with a real worker killed mid-run, and adaptive-spike with a real 1000-event duplicate flood mid-run), 90s each, headless — `bench/run.py`, driven directly against `Engine`, no HTTP involved.

## Target check

**TARGET(S) NOT MET — see CLAUDE.md's own instruction: this means a calibration problem, not a reporting problem.**

| Target | Result | Met? |
|---|---|---|
| naive-at-spike P0 p99 in the seconds | 842ms | ❌ NOT MET |
| adaptive-at-spike P0 p99 under 200ms | 418ms | ❌ NOT MET |
| zero critical (P0) events lost, any config | 0 lost across all 6 configs | ✅ |

## Six-config matrix

Latency and SLA-attainment columns are `P0/P1/P2`, in that order, joined by `/`. The last two rows fire a real chaos action (a genuine worker `task.cancel()`, or a genuine 1000-event duplicate flood — the same mechanisms `POST /chaos/kill-worker` and `POST /chaos/duplicate-flood` use, called directly against `Engine`) at the run's own midpoint, under the same 20x spike load as `adaptive-spike` — `exactly_once_violations` is the column this stage's own prompt asks for, and it reads 0 in every row, chaos rows included, not just the four undisturbed ones. See `report.html` for the same data with a chart.

| Config | Rate (eps) | Throughput (eps) | p50 (P0/P1/P2) | p95 (P0/P1/P2) | p99 (P0/P1/P2) | SLA attainment (P0/P1/P2) | Deferred | Batched | Sampled | Shed | Value delivered | Value shed | P0 lost | Chain OK | Exactly-once violations |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| naive-baseline | 16.6 | 16.6 | 117ms/81ms/18ms | 189ms/129ms/34ms | 209ms/134ms/37ms | 97.1%/100.0%/100.0% | 0 | 130 | 0 | 0 | 24538 | 0 | 0 | yes | 0 |
| naive-spike | 333.0 | 178.3 | 650ms/701ms/609ms | 775ms/1.27s/924ms | 842ms/1.57s/1.32s | 6.6%/100.0%/100.0% | 1751 | 13118 | 11055 | 166 | 154279 | 558 | 0 | yes | 0 |
| adaptive-baseline | 16.6 | 16.6 | 143ms/79ms/31ms | 206ms/130ms/48ms | 221ms/2.07s/49ms | 93.8%/100.0%/100.0% | 4 | 171 | 0 | 2 | 23147 | 6 | 0 | yes | 0 |
| adaptive-spike | 333.0 | 163.9 | 144ms/154ms/123ms | 267ms/422ms/592ms | 418ms/625ms/971ms | 89.4%/100.0%/100.0% | 11755 | 11717 | 233 | 149 | 418909 | 493 | 0 | yes | 0 |
| adaptive-spike-worker-kill | 333.0 | 175.8 | 141ms/153ms/126ms | 249ms/390ms/656ms | 359ms/500ms/1.28s | 89.6%/100.0%/100.0% | 12585 | 12848 | 18 | 103 | 425520 | 323 | 0 | yes | 0 |
| adaptive-spike-duplicate-flood | 333.0 | 175.7 | 142ms/155ms/124ms | 249ms/376ms/611ms | 328ms/499ms/951ms | 89.2%/100.0%/100.0% | 13234 | 12860 | 32 | 25 | 421493 | 81 | 0 | yes | 0 |

## Cost model

`actual_worker_seconds = worker_count * duration` — our fixed 6-worker pool, paid for regardless of load. `naive_scaled_worker_seconds = (offered work-units/sec * duration) / worker_capacity_ups` — workers needed, continuously scaled, to stream 100% of that same offered load with zero triage. Both converted to USD at a stated, illustrative $0.36/worker-hour (not tied to any specific vendor's real pricing — the ratio is the argument, not the absolute figure).

| Config | Actual worker-s | Naive-scaled worker-s | Actual $ | Naive-scaled $ | Ratio |
|---|---|---|---|---|---|
| naive-baseline | 540 | 52 | $0.0540 | $0.0052 | 0.10x |
| naive-spike | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |
| adaptive-baseline | 540 | 52 | $0.0540 | $0.0052 | 0.10x |
| adaptive-spike | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |
| adaptive-spike-worker-kill | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |
| adaptive-spike-duplicate-flood | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |

## Sensitivity sweep — adaptive only, per-tier SLA attainment

Where the system actually breaks, not just that it survives the one spike level (20x) it is calibrated for. 20x's row is the matrix's own adaptive-spike result, not a separate run.

| Multiplier | Rate (eps) | P0 SLA | P1 SLA | P2 SLA | P0 p99 | P1 p99 | P2 p99 | P0 lost |
|---|---|---|---|---|---|---|---|---|
| 5x | 83.2 | 94.0% | 99.4% | 100.0% | 220ms | 133ms | 47ms | 0 |
| 10x | 166.5 | 94.1% | 100.0% | 100.0% | 217ms | 156ms | 123ms | 0 |
| 20x | 333.0 | 89.4% | 100.0% | 100.0% | 418ms | 625ms | 971ms | 0 |
| 40x | 666.0 | 4.1% | 100.0% | 100.0% | 45.88s | 107ms | 30ms | 0 |
