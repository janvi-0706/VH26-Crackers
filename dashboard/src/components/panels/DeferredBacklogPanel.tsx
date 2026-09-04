import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { formatSecondsAgo, toChartPoints } from "../../lib/format";

/**
 * deferral.py's durable buffer size, live — how many DEFER decisions are
 * currently parked waiting for pressure to fall below the drainer's
 * DRAIN_PRESSURE_THRESHOLD. This is the number the acceptance line names
 * directly: it should visibly rise while a spike holds pressure high, and
 * visibly fall back to zero once RESET restarts the pipeline and the
 * drainer works through whatever was parked.
 */
export function DeferredBacklogPanel({ history }: { history: MetricsFrame[] }) {
  const latest = history[history.length - 1];
  const points = toChartPoints(history, {
    deferred: (f) => f.deferred_pending,
  });

  return (
    <Panel
      title="Deferred backlog"
      size="lg"
      headline={latest ? `${latest.deferred_pending} parked` : "—"}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={points} margin={{ top: 4, right: 8, bottom: 0, left: -12 }}>
          <CartesianGrid stroke="#232a3b" strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="t"
            tickFormatter={formatSecondsAgo}
            stroke="#5b6478"
            fontSize={11}
            tickLine={false}
          />
          <YAxis stroke="#5b6478" fontSize={11} tickLine={false} width={44} allowDecimals={false} />
          <Tooltip
            contentStyle={{
              background: "#12161f",
              border: "1px solid #232a3b",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${formatSecondsAgo(t as number)} ago`}
            formatter={(value: number) => [`${value}`, "deferred"]}
          />
          <Area
            type="monotone"
            dataKey="deferred"
            name="deferred"
            stroke="#f87171"
            fill="#f87171"
            fillOpacity={0.35}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </Panel>
  );
}
