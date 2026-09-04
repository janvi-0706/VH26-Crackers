import { Panel } from "../Panel";
import type { MetricsFrame, Tier } from "../../types/metrics";
import { PRESSURE_BATCH_MAX, PRESSURE_STREAM_MAX, TIERS } from "../../types/metrics";

type TierMode = "STREAM_NOW" | "MICRO_BATCH" | "DEFER";

/**
 * Mirrors decision.decide() (src/triage/decision.py) exactly: P0 is always
 * STREAM_NOW, unconditionally, before pressure is even consulted; P1/P2
 * step through the same 0.40/0.75 pressure bands decide() itself uses.
 * This recomputes decide()'s public rule client-side from the live
 * pressure value already on the wire — Stage D does not publish a
 * per-event decision feed, so "current mode per tier" means "what would
 * happen to an event of this tier admitted right now", not a log of what
 * already happened to one.
 */
function modeFor(tier: Tier, pressure: number): TierMode {
  if (tier === "P0") return "STREAM_NOW";
  if (pressure < PRESSURE_STREAM_MAX) return "STREAM_NOW";
  if (pressure < PRESSURE_BATCH_MAX) return "MICRO_BATCH";
  return "DEFER";
}

const MODE_LABEL: Record<TierMode, string> = {
  STREAM_NOW: "stream",
  MICRO_BATCH: "micro-batch",
  DEFER: "defer",
};

const MODE_STYLE: Record<TierMode, string> = {
  STREAM_NOW: "border-good/40 bg-good/15 text-good",
  MICRO_BATCH: "border-warn/40 bg-warn/15 text-warn",
  DEFER: "border-bad/40 bg-bad/15 text-bad",
};

export function ModeByTierPanel({ latest }: { latest: MetricsFrame | null }) {
  const hasData = latest !== null && latest.ingested > 0;
  const pressure = latest?.pressure ?? 0;

  return (
    <Panel title="Mode by tier" size="sm">
      <div className="flex h-full flex-col items-center justify-center gap-2.5">
        {TIERS.map((tier) => {
          const mode = modeFor(tier, pressure);
          return (
            <div key={tier} className="flex w-full items-center justify-between gap-2 px-1">
              <span className="font-mono text-xs font-semibold text-ink-muted">{tier}</span>
              <span
                className={`rounded-full border px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${
                  hasData ? MODE_STYLE[mode] : "border-surface-border text-ink-faint"
                }`}
              >
                {hasData ? MODE_LABEL[mode] : "—"}
              </span>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
