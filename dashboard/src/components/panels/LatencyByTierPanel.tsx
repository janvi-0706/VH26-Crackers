import {
  CartesianGrid,
  Line,
  LineChart,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { formatMs, formatSecondsAgo, toChartPoints } from "../../lib/format";

const TIER_COLOR: Record<"P0" | "P1" | "P2", string> = {
  P0: "#60a5fa",
  P1: "#c084fc",
  P2: "#fb923c",
};

/** p99 latency, one line per tier. Stage B has no priority queue yet, but
 * the generator already tags every event with its tier, so this chart is
 * meaningful from day one — and it is the exact chart that has to show P0
 * flat while P2 climbs, once Stage C's scheduler exists. */
export function LatencyByTierPanel({ history }: { history: MetricsFrame[] }) {
  const points = toChartPoints(history, {
    P0: (f) => f.latency_p99.P0,
    P1: (f) => f.latency_p99.P1,
    P2: (f) => f.latency_p99.P2,
  });

  return (
    <Panel title="p99 latency by tier" size="wide">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 4, right: 8, bottom: 0, left: -12 }}>
          <CartesianGrid stroke="#232a3b" strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="t"
            tickFormatter={formatSecondsAgo}
            stroke="#5b6478"
            fontSize={11}
            tickLine={false}
          />
          <YAxis
            stroke="#5b6478"
            fontSize={11}
            tickLine={false}
            width={48}
            tickFormatter={(v: number) => formatMs(v)}
          />
          <Tooltip
            contentStyle={{
              background: "#12161f",
              border: "1px solid #232a3b",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${formatSecondsAgo(t as number)} ago`}
            formatter={(value: number, name: string) => [formatMs(value), name]}
          />
          <Legend
            wrapperStyle={{ fontSize: 12, color: "#8b93a7" }}
            formatter={(value) => value}
          />
          {(["P0", "P1", "P2"] as const).map((tier) => (
            <Line
              key={tier}
              type="monotone"
              dataKey={tier}
              name={tier}
              stroke={TIER_COLOR[tier]}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </Panel>
  );
}
