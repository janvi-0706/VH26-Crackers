# server1 contention — after the split (Phase J8)

The "after" half of `bench/contention-before.md`'s own before/after comparison — same two measurements, same calibrated 20x P0-only spike rate (108.2 u/s, 33.3 events/sec), run for 90s directly against server1's own real process (`triage.server1.create_server1_app`), traffic submitted straight into its own `P0Queue` — bypassing HTTP, matching `contention.py`'s own choice to call straight into the pipeline it measures, for the same reason: this measures the PROCESSING loop's own contention, not the transport layer's (which Phase J3-J7 already measure separately — see `GET /control/transport-latency`).

Run as **two separate passes**, not one — a real, measured methodology finding from this phase, not the original plan: a first, single-pass version ran the loop-lag prober (section 2) CONCURRENTLY with the throughput/latency measurement (section 1) and produced tens of SECONDS of queue wait, even though an isolated burst test against the identical app (no prober at all) confirmed 6 workers genuinely drain 60 P0 events in ~1.6s, matching the ~1.56s the math predicts. Traced directly, not assumed: `contention-before.md`'s own section 4 already measured the prober at ~774,000 loop turns/sec in isolation and called that "a real, if likely minor, contributor" for the MONOLITH's 150 u/s pool — for server1's own smaller, standalone 135 u/s pool sharing one event loop with that same prober, it is not minor at all: it starves the real timer callbacks a worker's own `asyncio.sleep(cost / rate)` needs to fire on schedule. Section 1 below is measured with NO prober running; section 2 is measured in a separate pass with the prober running (that is the whole point of it — loop responsiveness UNDER load) but that pass's own throughput numbers are discarded rather than reported.

## 1. Head-of-line blocking behind a lower-tier batch

**Zero, by construction — not merely observed as zero this run.** `server1.py`'s own worker loop has exactly one path (dequeue -> `asyncio.sleep(cost / per_worker_rate)` -> ack); there is no `MICRO_BATCH`/`DEFER`/`SAMPLE_ROLLUP`/`SHED` branch anywhere in the file (confirmed by direct inspection, not assumed), and `/ingest` 422s any non-P0 event before it ever reaches the queue — a P0 event on this process cannot structurally wait behind a lower-tier interval, because a lower-tier interval cannot exist on this process at all. Live confirmation from this run: every one of the **2571** events submitted, and every one of the **2571** this run's own workers served before the measurement window closed, carried `tier=['P0']` — P0 and only P0.

The OTHER component `contention-before.md`'s own section 2 named separately — a P0 event queueing behind ANOTHER P0 event on the same worker — is not eliminated by the split (splitting P0 into its own process does not stop P0's own load from queueing behind itself if it exceeds P0's own worker capacity), and is measured honestly below as server1's own real end-to-end latency and queue wait under this run's own load.

| Metric | p50 | p95 | p99 |
|---|---|---|---|
| Queue wait (ingest -> dequeue) | 0.000ms | 0.000ms | 0.505ms |
| End-to-end latency (ingest -> complete) | 140.143ms | 169.008ms | 172.845ms |

(6 worker(s) at 22.50 u/s each, derived from `config/servers.yaml`'s own `server1.capacity_us` — `servers_config.ServerSpec.workers()`, unchanged since Phase J2/J4.)

## 2. Event-loop scheduling delay (proxy for GIL/loop contention)

Identical technique to `contention-before.md`'s own section 4: how much longer `await asyncio.sleep(0)` actually took than the microseconds it should, sampled continuously (every loop turn probed; only 1-in-500 recorded) throughout the run, concurrently with server1's own real worker(s) processing the same 20x P0-only load.

**77381 recorded samples**, out of 38690953 loop turns probed.

| Metric | Value |
|---|---|
| p50 | 1.8us |
| p95 | 5.4us |
| p99 | 5.8us |
| max | 279.8us |

## What this does and does not show

This measures server1's own real process, standalone, under the identical calibrated load `contention-before.md` used for the single-process build's P0 traffic share — it does NOT include server2, ingress, or the real transport hop between them (those are measured separately: `GET /control/transport-latency`, `tests/test_server1.py`'s own load test, `bench/phase-j-stress.md`'s own live sustained-spike run). It confirms the one claim this bench file exists to check — P0 head-of-line blocking behind a lower tier is structurally impossible post-split, not merely rare — and reports the loop-lag number honestly rather than assuming a smaller process trivially implies a quieter loop: server1 still runs its own worker tasks on its own one event loop, and this is that loop's own real, measured responsiveness under real load, not an assumption.
