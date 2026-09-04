# PULSE benchmark report

Four configs (naive/adaptive x baseline/spike), 90s each, headless — `bench/run.py`, driven directly against `Engine`, no HTTP involved.

## Target check

**TARGET(S) NOT MET — see CLAUDE.md's own instruction: this means a calibration problem, not a reporting problem.**

| Target | Result | Met? |
|---|---|---|
| naive-at-spike P0 p99 in the seconds | 767ms | ❌ NOT MET |
| adaptive-at-spike P0 p99 under 200ms | 290ms | ❌ NOT MET |
| zero critical (P0) events lost, any config | 0 lost across all 4 configs | ✅ |

## Four-config matrix

Latency and SLA-attainment columns are `P0/P1/P2`, in that order, joined by `/`. See `report.html` for the same data with a chart.

| Config | Rate (eps) | Throughput (eps) | p50 (P0/P1/P2) | p95 (P0/P1/P2) | p99 (P0/P1/P2) | SLA attainment (P0/P1/P2) | Deferred | Batched | Sampled | Shed | Value delivered | Value shed | P0 lost | Chain OK |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| naive-baseline | 16.6 | 16.2 | 120ms/79ms/21ms | 140ms/346ms/30ms | 150ms/7.50s/7.02s | 100.0%/95.0%/100.0% | 24 | 156 | 0 | 24 | 24950 | 72 | 0 | yes |
| naive-spike | 333.0 | 194.1 | 656ms/719ms/624ms | 732ms/1.17s/889ms | 767ms/1.48s/1.22s | 1.8%/100.0%/100.0% | 71 | 14520 | 12140 | 16 | 160866 | 68 | 0 | yes |
| adaptive-baseline | 16.6 | 16.6 | 120ms/80ms/21ms | 140ms/92ms/26ms | 149ms/97ms/29ms | 100.0%/100.0%/100.0% | 1 | 123 | 0 | 0 | 24113 | 0 | 0 | yes |
| adaptive-spike | 333.0 | 172.2 | 127ms/170ms/187ms | 204ms/407ms/1.86s | 290ms/531ms/2.11s | 99.5%/100.0%/100.0% | 12035 | 12441 | 214 | 88 | 469341 | 292 | 0 | yes |

## Cost model

`actual_worker_seconds = worker_count * duration` — our fixed 6-worker pool, paid for regardless of load. `naive_scaled_worker_seconds = (offered work-units/sec * duration) / worker_capacity_ups` — workers needed, continuously scaled, to stream 100% of that same offered load with zero triage. Both converted to USD at a stated, illustrative $0.36/worker-hour (not tied to any specific vendor's real pricing — the ratio is the argument, not the absolute figure).

| Config | Actual worker-s | Naive-scaled worker-s | Actual $ | Naive-scaled $ | Ratio |
|---|---|---|---|---|---|
| naive-baseline | 540 | 52 | $0.0540 | $0.0052 | 0.10x |
| naive-spike | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |
| adaptive-baseline | 540 | 52 | $0.0540 | $0.0052 | 0.10x |
| adaptive-spike | 540 | 1037 | $0.0540 | $0.1037 | 1.92x |

## Sensitivity sweep — adaptive only, per-tier SLA attainment

Where the system actually breaks, not just that it survives the one spike level (20x) it is calibrated for. 20x's row is the four-config matrix's own adaptive-spike result, not a separate run.

| Multiplier | Rate (eps) | P0 SLA | P1 SLA | P2 SLA | P0 p99 | P1 p99 | P2 p99 | P0 lost |
|---|---|---|---|---|---|---|---|---|
| 5x | 83.2 | 100.0% | 99.9% | 100.0% | 134ms | 87ms | 47ms | 0 |
| 10x | 166.5 | 100.0% | 100.0% | 100.0% | 139ms | 124ms | 96ms | 0 |
| 20x | 333.0 | 99.5% | 100.0% | 100.0% | 290ms | 531ms | 2.11s | 0 |
| 40x | 666.0 | 4.1% | 100.0% | 100.0% | 41.39s | 79ms | 31ms | 0 |
