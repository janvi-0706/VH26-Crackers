import { useEffect, useState } from "react";
import { Panel } from "../Panel";

/**
 * Phase J7's own dashboard addition: two SEPARATE pressure gauges
 * (server1/server2 — never averaged, per this phase's own instruction),
 * transport latency, a topology strip, server2's own live instance count
 * (meaningful once HPA exists), and the outstanding-dispatch/redispatch
 * counters. Polls GET /control/topology directly (not the /ws frame —
 * this data only exists in split mode, and MetricsFrame is frozen) at
 * the same 4Hz the WS socket already uses, so it feels live without
 * needing a second transport mechanism.
 */

const API_BASE = "http://localhost:8000";

interface TopologyData {
  mode: "split" | "monolith";
  server1: { pressure: number | null; instance_count: number };
  server2: { pressure: number | null; instance_count: number };
  transport_latency_ms: { p50: number; p95: number; p99: number };
  outstanding_dispatch: number;
  redispatch_count: number;
}

function useTopology(): TopologyData | null {
  const [data, setData] = useState<TopologyData | null>(null);
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`${API_BASE}/control/topology`);
        if (!r.ok) return;
        const json = (await r.json()) as TopologyData;
        if (!cancelled) setData(json);
      } catch {
        // backend not reachable yet / not in split mode — leave stale data
      }
    };
    poll();
    const id = setInterval(poll, 250);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);
  return data;
}

function zoneFor(p: number | null): "good" | "warn" | "bad" | "neutral" {
  if (p === null) return "neutral";
  if (p < 0.4) return "good";
  if (p < 0.75) return "warn";
  return "bad";
}

const ZONE_TEXT: Record<string, string> = {
  good: "text-good", warn: "text-warn", bad: "text-bad", neutral: "text-ink-faint",
};

function PressureMini({ label, pressure }: { label: string; pressure: number | null }) {
  const zone = zoneFor(pressure);
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-1">
      <div className="text-xs font-semibold uppercase tracking-widest text-ink-muted">{label}</div>
      <div className={`font-mono text-4xl font-bold tabular-nums ${ZONE_TEXT[zone]}`}>
        {pressure === null ? "—" : pressure.toFixed(2)}
      </div>
    </div>
  );
}

export function TopologyPanel() {
  const data = useTopology();

  if (!data) {
    return (
      <Panel title="Topology" cols={5} accent="neutral">
        <div className="flex h-full items-center justify-center text-sm text-ink-faint">
          waiting for /control/topology (split mode only)…
        </div>
      </Panel>
    );
  }

  return (
    <Panel title={`Topology — ${data.mode}`} cols={5} accent={data.mode === "split" ? "good" : "neutral"}>
      <div className="flex h-full flex-col gap-3">
        <div className="flex gap-4">
          <PressureMini label="Server 1 (P0)" pressure={data.server1.pressure} />
          <PressureMini label="Server 2 (P1/P2)" pressure={data.server2.pressure} />
        </div>
        <div className="grid grid-cols-3 gap-2 text-center text-xs">
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Server1 pods</div>
            <div className="font-mono text-lg font-bold text-ink">{data.server1.instance_count}</div>
          </div>
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Server2 pods</div>
            <div className="font-mono text-lg font-bold text-ink">{data.server2.instance_count}</div>
          </div>
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Outstanding</div>
            <div className="font-mono text-lg font-bold text-ink">{data.outstanding_dispatch}</div>
          </div>
        </div>
        <div className="grid grid-cols-4 gap-2 text-center text-xs">
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Transport p50</div>
            <div className="font-mono text-sm font-bold text-ink">
              {data.transport_latency_ms.p50.toFixed(1)}ms
            </div>
          </div>
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Transport p99</div>
            <div className="font-mono text-sm font-bold text-ink">
              {data.transport_latency_ms.p99.toFixed(1)}ms
            </div>
          </div>
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Redispatches</div>
            <div className="font-mono text-sm font-bold text-ink">{data.redispatch_count}</div>
          </div>
          <div className="rounded bg-surface-raised p-2">
            <div className="text-ink-muted">Mode</div>
            <div className="font-mono text-sm font-bold text-ink">{data.mode}</div>
          </div>
        </div>
      </div>
    </Panel>
  );
}
