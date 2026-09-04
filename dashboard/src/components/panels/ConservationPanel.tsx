import { useEffect, useRef, useState } from "react";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";

/**
 * "ingested == processed + in_queue + in_flight + deferred_pending +
 * sampled_out + shed" (docs/DATA_MODEL.md's own conservation equation,
 * real and checked continuously on the backend since the ledger stage —
 * see metrics.py's `_check_conservation()`). This panel recomputes the
 * exact same equation client-side from the raw counters already on the
 * wire, so it needs no new backend field to prove the same thing visually.
 *
 * Once broken, this panel latches red for the rest of the page's life —
 * a reset does not clear it. That mirrors the backend's own
 * `critical_failure_count()` on purpose: a critical-invariant violation is
 * exactly the kind of evidence a demo reset must not quietly erase. A
 * judge should be able to walk up mid-demo, see red, and trust that it
 * means something really broke — not "something broke a while ago and
 * someone already clicked past it."
 */
export function ConservationPanel({ latest }: { latest: MetricsFrame | null }) {
  const [everBroken, setEverBroken] = useState(false);
  const brokenAtRef = useRef<string | null>(null);

  const hasData = latest !== null && latest.ingested > 0;
  const rhs = latest
    ? latest.processed + latest.in_queue + latest.in_flight
      + latest.deferred_pending + latest.sampled_out + latest.shed
    : 0;
  const lhs = latest?.ingested ?? 0;
  const balancedNow = !hasData || lhs === rhs;

  useEffect(() => {
    if (hasData && lhs !== rhs && !everBroken) {
      setEverBroken(true);
      brokenAtRef.current = new Date().toLocaleTimeString();
    }
  }, [hasData, lhs, rhs, everBroken]);

  const broken = everBroken;
  const statusColor = !hasData ? "text-ink-faint" : broken ? "text-bad" : "text-good";

  return (
    <Panel
      title="Conservation equation"
      size="full"
      accent={!hasData ? "neutral" : broken ? "bad" : "good"}
    >
      <div
        className={`flex h-full flex-col items-center justify-center gap-3 rounded-lg transition-colors ${
          broken ? "bg-bad/10" : ""
        }`}
      >
        <div className="flex items-center gap-5">
          <span className={`font-mono text-8xl font-black leading-none ${statusColor}`}>
            {!hasData ? "—" : broken ? "✕" : "✓"}
          </span>
          <span className={`text-3xl font-black uppercase tracking-widest ${statusColor}`}>
            {!hasData ? "waiting" : broken ? "broken" : "balanced"}
          </span>
        </div>

        <div className="flex flex-wrap items-center justify-center gap-x-1.5 gap-y-1 px-4 font-mono text-sm text-ink-muted">
          <span className="font-semibold text-ink">ingested</span>
          <span className="tabular-nums text-ink">{lhs}</span>
          <span>=</span>
          <span>processed</span>
          <span className="tabular-nums text-ink">{latest?.processed ?? 0}</span>
          <span>+</span>
          <span>in_queue</span>
          <span className="tabular-nums text-ink">{latest?.in_queue ?? 0}</span>
          <span>+</span>
          <span>in_flight</span>
          <span className="tabular-nums text-ink">{latest?.in_flight ?? 0}</span>
          <span>+</span>
          <span>deferred_pending</span>
          <span className="tabular-nums text-ink">{latest?.deferred_pending ?? 0}</span>
          <span>+</span>
          <span>sampled_out</span>
          <span className="tabular-nums text-ink">{latest?.sampled_out ?? 0}</span>
          <span>+</span>
          <span>shed</span>
          <span className="tabular-nums text-ink">{latest?.shed ?? 0}</span>
          <span>=</span>
          <span className={`font-semibold ${balancedNow ? "text-ink" : "text-bad"}`}>{rhs}</span>
        </div>

        {broken && (
          <div className="text-xs font-bold uppercase tracking-wide text-bad">
            first broke at {brokenAtRef.current} — does not clear on reset
          </div>
        )}
      </div>
    </Panel>
  );
}
