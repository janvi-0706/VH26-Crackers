# PULSE contention report — before Phase J (single process)

Phase J0: measurement only. `bench/contention.py`, 90s at 20x spike, adaptive mode, driven directly against `Engine` the same way `bench/run.py` is — no HTTP involved, `src/` untouched. This is the "before" evidence for the P0/P1-P2 process split Phase J proposes.

**3068 P0 events observed.**

## 1. P0 head-of-line blocking (waited for a worker busy with P1/P2)

For each P0 event, the portion of its own queue wait that overlapped a LOWER-tier (P1/P2, individual or MICRO_BATCH) interval on the specific worker that ended up serving it — the exact cost a process split removes.

| Metric | Value |
|---|---|
| p50 | 0.00ms |
| p95 | 31.56ms |
| p99 | 63.04ms |
| max | 218.41ms |
| P0 events with ANY such wait | 512 / 3068 (16.7%) |

## 2. P0 queue wait, decomposed

Total P0 queue wait, split into: waited behind another P0 event on the same worker; waited behind P1/P2 work on the same worker (row 1's own numbers, repeated here for direct comparison); and unattributed (real scheduling noise, or a worker this run had no prior record for yet).

| Component | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Total queue wait | 15.21ms | 108.55ms | 187.73ms | 390.08ms |
| ...behind other P0 | 0.00ms | 94.12ms | 187.41ms | 390.08ms |
| ...behind P1/P2 (head-of-line) | 0.00ms | 31.56ms | 63.04ms | 218.41ms |
| ...unattributed | 0.00ms | 0.00ms | 0.00ms | 1.06ms |

## 3. Largest single blocking event observed

**218.41ms** — P0 event `evt-00019547` waited behind a batch of 7 on tier(s) `P1` (batch_size=7).

## 4. Event-loop scheduling delay (proxy for GIL/loop contention)

How much longer `await asyncio.sleep(0)` — yield to the loop, resume on its next turn — actually took than the microseconds it should, sampled continuously throughout the run (every loop turn probed; only 1-in-500 recorded, to keep the sample list bounded — see `LOOP_PROBE_SAMPLE_STRIDE`'s own docstring). A property of the loop itself under real load, not of any one P0 event's own path — see `_loop_lag_prober`'s own docstring for two real, measured problems with pacing this any other way, and why probing every turn but recording only a stride of them is what survived.

**43127 recorded samples**, out of 21563919 loop turns probed.

| Metric | Value |
|---|---|
| p50 | 4.7us |
| p95 | 5.6us |
| p99 | 8.3us |
| max | 2.00ms |

## What this does and does not show

This measures the CURRENT single-process build under exactly the load Phase J is meant to survive better. It does not simulate the split itself — a real two-process build could still have its own new costs (IPC, serialization, a second audit-ledger-consistency problem) this report says nothing about. It is evidence for whether the specific contention Phase J targets is real and large enough to be worth that cost, not a promise of what Phase J will achieve.

Section 4's own prober is itself an observer effect worth naming: it yields to the loop on every single turn for the whole run (hundreds of thousands of times per second, per the isolated test in `_loop_lag_prober`'s own docstring), which is itself additional loop activity, not a free window into it. Its own per-call cost is small, but at that frequency it is a real, if likely minor, contributor to whatever contention section 4 reports — not purely a passive measurement.
