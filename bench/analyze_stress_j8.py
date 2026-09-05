"""One-off analysis of bench/stress_j8_log.jsonl — not part of the phase
deliverable itself, just the tool used to pull real numbers out of the
real run for bench/phase-j-stress.md. Prints a summary per phase."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG = REPO_ROOT / "bench" / "stress_j8_log.jsonl"


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def main():
    records = [json.loads(line) for line in LOG.read_text(encoding="utf-8").splitlines() if line.strip()]
    samples = [r for r in records if r.get("event") == "sample"]
    phases = {}
    for s in samples:
        phases.setdefault(s["phase"], []).append(s)

    for phase, rows in phases.items():
        print(f"\n=== phase: {phase} ({len(rows)} samples) ===")
        s1_p99 = [r["server1_metrics"]["latency_ms"]["p99"] for r in rows if r.get("server1_metrics") and "latency_ms" in r["server1_metrics"]]
        s1_processed = [r["server1_metrics"]["processed"] for r in rows if r.get("server1_metrics") and "processed" in r["server1_metrics"]]
        s2_shed_crit = [r["server2_metrics"]["shed_critical"] for r in rows if r.get("server2_metrics") and "shed_critical" in r["server2_metrics"]]
        transport_p99 = [r["transport_latency"]["p99"] for r in rows if r.get("transport_latency") and "p99" in r.get("transport_latency", {})]
        outstanding = [r["topology"]["outstanding_dispatch"] for r in rows if r.get("topology") and "outstanding_dispatch" in r["topology"]]
        conservation_shed_crit = [r["conservation"]["shed_critical"] for r in rows if r.get("conservation") and "shed_critical" in r["conservation"]]
        deferred_pending = [r["conservation"]["deferred_pending"] for r in rows if r.get("conservation") and "deferred_pending" in r["conservation"]]
        redispatch = [r["topology"]["redispatch_count"] for r in rows if r.get("topology") and "redispatch_count" in r["topology"]]
        dispatch_ident = [
            (r["conservation"]["dispatch"]["dispatched"], r["conservation"]["dispatch"]["resolved"], r["conservation"]["dispatch"]["outstanding"])
            for r in rows if r.get("conservation") and "dispatch" in r["conservation"]
        ]
        s1_healthz = [r.get("server1_healthz", {}).get("status") for r in rows]
        s2_healthz = [r.get("server2_healthz", {}).get("status") for r in rows]
        s1_readyz = [r.get("server1_readyz") for r in rows]
        s2_readyz = [r.get("server2_readyz") for r in rows]

        if s1_p99:
            print(f"server1 p99 latency: min={min(s1_p99):.1f} max={max(s1_p99):.1f} last={s1_p99[-1]:.1f} (ms)")
        if s1_processed:
            print(f"server1 processed: first={s1_processed[0]} last={s1_processed[-1]} delta={s1_processed[-1]-s1_processed[0]}")
        if s2_shed_crit:
            print(f"server2 shed_critical: max={max(s2_shed_crit)}")
        if conservation_shed_crit:
            print(f"conservation shed_critical: max={max(conservation_shed_crit)}")
        if transport_p99:
            print(f"transport p99: min={min(transport_p99):.1f} max={max(transport_p99):.1f} last={transport_p99[-1]:.1f} (ms)")
        if outstanding:
            print(f"outstanding_dispatch: min={min(outstanding)} max={max(outstanding)} last={outstanding[-1]}")
        if redispatch:
            print(f"redispatch_count: first={redispatch[0]} last={redispatch[-1]} delta={redispatch[-1]-redispatch[0]}")
        if dispatch_ident:
            d0 = dispatch_ident[0]; d1 = dispatch_ident[-1]
            print(f"dispatch identity (dispatched,resolved,outstanding): first={d0} last={d1}")
            mismatches = [d for d in dispatch_ident if d[0] != d[1] + d[2]]
            print(f"identity mismatches (dispatched != resolved+outstanding): {len(mismatches)} / {len(dispatch_ident)}")
        if deferred_pending:
            print(f"deferred_pending: min={min(deferred_pending)} max={max(deferred_pending)} last={deferred_pending[-1]}")
        print(f"server1_healthz statuses: {set(s1_healthz)}")
        print(f"server2_healthz statuses: {set(s2_healthz)}")
        print(f"server1_readyz samples (first 3, last 3): {s1_readyz[:3]} ... {s1_readyz[-3:]}")
        print(f"server2_readyz samples (first 3, last 3): {s2_readyz[:3]} ... {s2_readyz[-3:]}")


if __name__ == "__main__":
    main()
