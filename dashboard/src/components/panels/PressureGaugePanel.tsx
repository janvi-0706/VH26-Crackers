import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { PRESSURE_BATCH_MAX, PRESSURE_STREAM_MAX } from "../../types/metrics";

type Zone = "good" | "warn" | "bad";

function zoneFor(p: number): Zone {
  if (p < PRESSURE_STREAM_MAX) return "good";
  if (p < PRESSURE_BATCH_MAX) return "warn";
  return "bad";
}

const ZONE_LABEL: Record<Zone, string> = {
  good: "stream",
  warn: "micro-batch",
  bad: "defer",
};

const ZONE_FILL: Record<Zone, string> = {
  good: "bg-good",
  warn: "bg-warn",
  bad: "bg-bad",
};

const ZONE_TEXT: Record<Zone, string> = {
  good: "text-good",
  warn: "text-warn",
  bad: "text-bad",
};

/**
 * decision.pressure()'s live output, 0.0 (calm) to 1.0 (saturated), as one
 * bar a judge can read from across the room. The two threshold ticks are
 * drawn at exactly decide()'s own 0.40/0.75 bands, so the moment the fill
 * crosses a tick is the moment P1/P2 routing genuinely steps to the next
 * mode — not a decorative gradient, the actual control-loop boundary.
 */
export function PressureGaugePanel({ latest }: { latest: MetricsFrame | null }) {
  const hasData = latest !== null && latest.ingested > 0;
  const p = latest?.pressure ?? 0;
  const clamped = Math.min(Math.max(p, 0), 1);
  const zone = zoneFor(clamped);

  return (
    <Panel title="Pressure" cols={2} accent={hasData ? zone : "neutral"}>
      <div className="flex h-full flex-col items-center justify-center gap-3">
        <div
          className={`font-mono text-5xl font-bold leading-none tabular-nums ${
            hasData ? ZONE_TEXT[zone] : "text-ink-faint"
          }`}
        >
          {hasData ? clamped.toFixed(2) : "—"}
        </div>
        <div className="relative h-2 w-4/5 overflow-hidden rounded-full bg-surface-raised">
          <div
            className={`h-full rounded-full transition-[width] duration-150 ${
              hasData ? ZONE_FILL[zone] : "bg-ink-faint"
            }`}
            style={{ width: `${(hasData ? clamped : 0) * 100}%` }}
          />
          {[PRESSURE_STREAM_MAX, PRESSURE_BATCH_MAX].map((tick) => (
            <div
              key={tick}
              className="absolute inset-y-0 w-px bg-surface"
              style={{ left: `${tick * 100}%` }}
            />
          ))}
        </div>
        <div className="text-xs font-semibold uppercase tracking-widest text-ink-muted">
          {hasData ? ZONE_LABEL[zone] : "waiting for data"}
        </div>
      </div>
    </Panel>
  );
}
