import { useEffect, useRef, useState } from "react";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { COST_PER_WORKER_SECOND_USD, WORKER_CAPACITY_UPS } from "../../types/metrics";

/**
 * A running total, accumulated client-side frame by frame from real
 * fields already on the wire — `bench/run.py`'s own cost model (Stage G),
 * live: `actual worker-seconds += worker_count * dt` (the fixed 6-worker
 * pool, paid for every second regardless of load) vs. `naive-scaled
 * worker-seconds += (offered_rate / WORKER_CAPACITY_UPS) * dt` (workers
 * needed, continuously scaled, to stream 100% of the currently offered
 * load with zero triage). `dt` is measured between successive frames'
 * own server timestamps (`latest.ts`), not client `Date.now()` — the same
 * clock-skew-avoiding trick `toChartPoints` already uses.
 *
 * Deliberately NOT reset by the dashboard's own Reset button — this is a
 * running total across the whole session on stage, the same way the
 * backend's own `metrics.critical_failure_count()` is deliberately not
 * cleared by a reset either: the point of "running total" is that it
 * keeps counting through every phase of the demo (baseline, spike, reset,
 * spike again), not that it restarts every time the presenter clicks a
 * button. It resets only on an actual page reload.
 */
export function CostComparisonPanel({ latest }: { latest: MetricsFrame | null }) {
  const actualWorkerSecondsRef = useRef(0);
  const naiveScaledWorkerSecondsRef = useRef(0);
  const lastTsRef = useRef<number | null>(null);
  const [, tick] = useState(0);

  useEffect(() => {
    if (!latest) return;
    const now = latest.ts;
    if (lastTsRef.current !== null) {
      const dt = Math.max(0, Math.min(now - lastTsRef.current, 5)); // clamp a stale/huge gap
      actualWorkerSecondsRef.current += latest.worker_count * dt;
      naiveScaledWorkerSecondsRef.current += (latest.offered_rate / WORKER_CAPACITY_UPS) * dt;
    }
    lastTsRef.current = now;
    tick((n) => n + 1);
  }, [latest]);

  const actualUsd = actualWorkerSecondsRef.current * COST_PER_WORKER_SECOND_USD;
  const naiveUsd = naiveScaledWorkerSecondsRef.current * COST_PER_WORKER_SECOND_USD;
  const ratio = actualWorkerSecondsRef.current > 0
    ? naiveScaledWorkerSecondsRef.current / actualWorkerSecondsRef.current
    : 0;

  return (
    <Panel
      title="Cost: fixed pool vs naive-scaled"
      cols={3}
      headline={ratio > 0 ? `${ratio.toFixed(1)}x` : "—"}
    >
      <div className="flex h-full flex-col items-center justify-center gap-2">
        <div className="w-full text-center">
          <div className="font-mono text-2xl font-black tabular-nums text-good">
            ${actualUsd.toFixed(4)}
          </div>
          <div className="text-[10px] font-medium uppercase tracking-wide text-ink-muted">
            our fixed 6 workers
          </div>
        </div>
        <div className="w-full text-center">
          <div className="font-mono text-2xl font-black tabular-nums text-bad">
            ${naiveUsd.toFixed(4)}
          </div>
          <div className="text-[10px] font-medium uppercase tracking-wide text-ink-muted">
            naive, linearly scaled
          </div>
        </div>
      </div>
    </Panel>
  );
}
