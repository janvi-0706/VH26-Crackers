import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { formatSecondsAgo, toChartPoints } from "../../lib/format";

const TIER_COLOR: Record<"P0" | "P1" | "P2", string> = {
  P0: "#60a5fa",
  P1: "#c084fc",
  P2: "#fb923c",
};

/**
 * Stacked queue depth by tier: the direct picture of what Stage C's three
 * heaps are actually holding, and the panel that makes the priority story
 * legible without reading a single number. Watch it during a spike: P2's
 * band balloons while P0's stays a thin, flat sliver at the bottom — the
 * aging guard shows up here too, as brief P2 dips that don't correspond to
 * any change in offered load.
 */
export function QueueDepthPanel({ history }: { history: MetricsFrame[] }) {
  const latest = history[history.length - 1];
  const total = latest
    ? latest.queue_depth.P0 + latest.queue_depth.P1 + latest.queue_depth.P2
    : 0;

  const points = toChartPoints(history, {
    P0: (f) => f.queue_depth.P0,
    P1: (f) => f.queue_depth.P1,
    P2: (f) => f.queue_depth.P2,
  });

  return (
    <Panel title="Queue depth by tier" cols={4} headline={`${total} queued`}>
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
          <YAxis stroke="#5b6478" fontSize={11} tickLine={false} width={40} allowDecimals={false} />
          <Tooltip
            contentStyle={{
              background: "#12161f",
              border: "1px solid #232a3b",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${formatSecondsAgo(t as number)} ago`}
            formatter={(value: number, name: string) => [`${value}`, name]}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: "#8b93a7" }} />
          {(["P2", "P1", "P0"] as const).map((tier) => (
            // Stack order P2 -> P1 -> P0 (bottom to top) so P0's sliver is
            // always the top band, easiest to eyeball as "still thin".
            <Area
              key={tier}
              type="monotone"
              dataKey={tier}
              name={tier}
              stackId="depth"
              stroke={TIER_COLOR[tier]}
              fill={TIER_COLOR[tier]}
              fillOpacity={0.55}
              isAnimationActive={false}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
    </Panel>
  );
}
