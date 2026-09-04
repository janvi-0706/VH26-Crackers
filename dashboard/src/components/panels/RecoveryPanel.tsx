import { useEffect, useState } from "react";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";

interface RecoveryPanelProps {
  latest: MetricsFrame | null;
  /** Only place this number exists — see ChaosControlPanel's own note. */
  workersKilled: number;
}

/**
 * Four numbers a judge should be able to read in one glance right after
 * the kill button: what got broken, and what proves it was fixed
 * automatically rather than by a presenter quietly clicking reset.
 *
 * `exactly_once_violations` latches red client-side the same way
 * ConservationPanel does, and for the same reason: the live field itself
 * resets with everything else on /control/reset (metrics.py's own
 * `_counters`, Stage I), but a judge walking up mid-demo should be able to
 * trust that red here means something really broke at some point during
 * this run, not "broke a while ago and someone already clicked past it."
 */
export function RecoveryPanel({ latest, workersKilled }: RecoveryPanelProps) {
  const [everViolated, setEverViolated] = useState(false);
  const violations = latest?.exactly_once_violations ?? 0;

  useEffect(() => {
    if (violations > 0) setEverViolated(true);
  }, [violations]);

  const tiles: Array<{ label: string; value: number; accent: "good" | "bad" | "neutral" }> = [
    { label: "workers killed", value: workersKilled, accent: "neutral" },
    { label: "events retried", value: latest?.retries ?? 0, accent: "neutral" },
    { label: "duplicates suppressed", value: latest?.duplicates_caught ?? 0, accent: "neutral" },
    {
      label: "exactly-once violations",
      value: violations,
      accent: everViolated ? "bad" : "good",
    },
  ];

  return (
    <Panel title="Recovery" cols={6} accent={everViolated ? "bad" : "neutral"}>
      <div className="grid h-full grid-cols-4 gap-4">
        {tiles.map((t) => (
          <div key={t.label} className="flex flex-col items-center justify-center gap-3 rounded-xl bg-surface/60 px-2 text-center">
            <span
              className={`font-mono text-7xl font-black leading-none tabular-nums ${
                t.accent === "bad" ? "text-bad" : t.accent === "good" ? "text-good" : "text-ink"
              }`}
            >
              {t.value}
            </span>
            <span className="text-sm font-bold uppercase leading-tight tracking-wide text-ink-muted">
              {t.label}
            </span>
          </div>
        ))}
      </div>
    </Panel>
  );
}
