import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";

/**
 * `worker_count` and `active_workers` (both real since Stage D) as a grid
 * of cells rather than a number — a projector-legible "is the pool busy"
 * signal a judge reads without doing arithmetic. Workers aren't
 * individually identified anywhere in this codebase (WorkerPool is a
 * fixed-size pool, not N named workers), so "which" cells light up is
 * arbitrary — the first `active_workers` of them, left to right — only
 * the COUNT lit is ever meaningful, which is also the only thing this
 * panel claims.
 *
 * `active_workers` is wired to metrics.py's own `in_flight` counter, not
 * to a bounded count of the fixed pool — under a real spike in_flight
 * (queued work claimed but not yet returned) climbs well past
 * `worker_count` (observed 30 in flight against a 6-worker pool live).
 * That is correct backend behaviour, not a bug this dashboard-only stage
 * can or should change — but showing it verbatim as "30/6 busy" is
 * exactly the kind of number that needs explaining, which Stage H's own
 * brief rules out. Clamp what this panel DISPLAYS to the pool size: every
 * cell lit plus a "6/6 (+24 waiting)" headline says the true thing (pool
 * saturated, more work queued behind it) without implying a 6-worker pool
 * somehow ran 30 workers at once.
 */
export function WorkerPoolGridPanel({ latest }: { latest: MetricsFrame | null }) {
  const total = latest?.worker_count ?? 6;
  const rawActive = latest?.active_workers ?? 0;
  const active = Math.min(rawActive, total);
  const waiting = Math.max(rawActive - total, 0);

  return (
    <Panel
      title="Worker pool"
      cols={4}
      headline={waiting > 0 ? `${active}/${total} (+${waiting} waiting)` : `${active}/${total} busy`}
    >
      <div className="flex h-full items-center justify-center gap-3">
        {Array.from({ length: total }, (_, i) => {
          const lit = i < active;
          return (
            <div
              key={i}
              className={`aspect-square flex-1 max-w-16 rounded-xl border-2 transition-colors duration-150 ${
                lit
                  ? "border-tier-p0 bg-tier-p0/30 shadow-[0_0_16px_rgba(96,165,250,0.6)]"
                  : "border-surface-border bg-surface"
              }`}
            />
          );
        })}
      </div>
    </Panel>
  );
}
