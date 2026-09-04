import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";

const TIER_COLOR: Record<string, string> = {
  P0: "text-tier-p0",
  P1: "text-tier-p1",
  P2: "text-tier-p2",
};

/**
 * `MetricsFrame.recent_sheds` (real since Stage A/D — `ShedRecord`,
 * newest first, up to 50) rendered as a scrolling log rather than a chart:
 * a shed event is a story ("this, at this pressure, because of this"),
 * not a number, and `reason` is exactly the human sentence
 * `decision.decide()`/`ladder.escalate()` wrote for it.
 */
export function ShedLogPanel({ latest }: { latest: MetricsFrame | null }) {
  const sheds = latest?.recent_sheds ?? [];
  const nowTs = latest?.ts ?? 0;

  return (
    <Panel title="Shed log" cols={4} headline={`${latest?.shed ?? 0} total`}>
      <div className="flex h-full flex-col gap-1.5 overflow-y-auto pr-1">
        {sheds.length === 0 && (
          <p className="p-2 text-xs italic text-ink-faint">nothing shed yet</p>
        )}
        {sheds.map((s, i) => {
          const agoSeconds = Math.max(0, Math.round(nowTs - s.ts));
          return (
            <div
              key={`${s.event_id}-${s.seq}-${i}`}
              className="rounded-md border border-bad/30 bg-bad/5 px-2.5 py-1.5"
            >
              <div className="flex items-center justify-between gap-2 text-[11px]">
                <span className={`font-mono font-bold ${TIER_COLOR[s.tier ?? ""] ?? "text-ink"}`}>
                  {s.tier ?? "?"}
                </span>
                <span className="font-mono text-ink-muted">{s.type ?? "?"}</span>
                <span className="font-mono tabular-nums text-ink-faint">
                  {agoSeconds === 0 ? "now" : `${agoSeconds}s ago`}
                </span>
              </div>
              <p className="mt-0.5 text-xs text-ink-muted">{s.reason}</p>
              <div className="mt-0.5 flex gap-3 font-mono text-[10px] text-ink-faint">
                <span>value {s.value.toFixed(1)}</span>
                <span>pressure {s.pressure.toFixed(2)}</span>
                <span title={s.event_id}>{s.event_id}</span>
              </div>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
