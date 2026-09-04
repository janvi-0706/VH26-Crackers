import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { P0_LATENCY_TARGET_MS } from "../../types/metrics";
import { formatMs } from "../../lib/format";

/**
 * The single number the whole project stands or falls on: is the protected
 * tier meeting its deadline. Large, binary, readable from across a room —
 * this is the panel a judge should be able to read without walking closer.
 */
export function P0ScoreboardPanel({ latest }: { latest: MetricsFrame | null }) {
  const p99 = latest?.latency_p99.P0 ?? null;
  const met = p99 !== null && p99 <= P0_LATENCY_TARGET_MS;
  const hasData = latest !== null && latest.ingested > 0;

  const accent = !hasData ? "neutral" : met ? "good" : "bad";
  const numberClass = !hasData
    ? "text-ink-faint"
    : met
      ? "text-good"
      : "text-bad";

  return (
    <Panel title="P0 p99 vs 200ms target" cols={3} accent={accent}>
      <div className="flex h-full flex-col items-center justify-center gap-1">
        <div className={`font-mono text-5xl font-bold tabular-nums ${numberClass}`}>
          {hasData && p99 !== null ? formatMs(p99) : "—"}
        </div>
        <div className="text-xs font-medium uppercase tracking-wide text-ink-muted">
          {!hasData ? "waiting for data" : met ? "within SLA" : "SLA breached"}
        </div>
      </div>
    </Panel>
  );
}
