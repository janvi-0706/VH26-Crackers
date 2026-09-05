import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { RUNG_LABEL, RUNG_STYLE, TIERS } from "../../types/metrics";

/**
 * ladder.Rung (src/triage/ladder.py), read directly off the wire —
 * MetricsFrame.ladder_rung reports the rung each tier's most recently
 * observed *real* decision actually landed on. Unlike Mode-by-tier (Stage
 * D, still recomputing decide()'s own pressure bands client-side because
 * that formula alone is all that existed then), this panel does not
 * recompute anything: rungs 3 and 4 (SAMPLE_ROLLUP, SHED) depend on
 * codel.py's sojourn-driven sampling state and the hard-shed pressure
 * threshold, neither of which a pressure number alone can reconstruct.
 *
 * P0's rung should read STREAM permanently — ladder.MAX_RUNG's own
 * ceiling — and P1 should never show past DEFER; if either ever does, that
 * is this dashboard surfacing a real bug, not a display quirk to fix here.
 */
export function LadderPanel({ latest }: { latest: MetricsFrame | null }) {
  const hasData = latest !== null && latest.ingested > 0;

  return (
    <Panel title="Ladder rung by tier" cols={2}>
      <div className="flex h-full flex-col items-center justify-center gap-3">
        {TIERS.map((tier) => {
          const rung = latest?.ladder_rung[tier] ?? 0;
          const label = RUNG_LABEL[rung] ?? `rung ${rung}`;
          const style = RUNG_STYLE[rung] ?? "border-surface-border text-ink-faint";
          return (
            <div key={tier} className="flex w-full items-center justify-between gap-3 px-2">
              <span className="font-mono text-sm font-bold text-ink-muted">{tier}</span>
              <span
                className={`rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-wide ${
                  hasData ? style : "border-surface-border text-ink-faint"
                }`}
              >
                {hasData ? label : "—"}
              </span>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
