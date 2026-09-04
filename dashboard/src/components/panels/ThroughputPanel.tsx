import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Panel } from "../Panel";
import type { MetricsFrame } from "../../types/metrics";
import { formatRate, formatSecondsAgo, toChartPoints } from "../../lib/format";

/** Events completed per second, over the rolling window. The simplest
 * possible "is it alive and keeping up" signal — offered/admitted/service
 * rate join it once Stage E's backpressure lands. */
export function ThroughputPanel({ history }: { history: MetricsFrame[] }) {
  const latest = history[history.length - 1];
  const points = toChartPoints(history, {
    throughput: (f) => f.throughput,
  });

  return (
    <Panel
      title="Throughput"
      size="lg"
      headline={latest ? formatRate(latest.throughput) : "—"}
    >
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
          <YAxis stroke="#5b6478" fontSize={11} tickLine={false} width={44} />
          <Tooltip
            contentStyle={{
              background: "#12161f",
              border: "1px solid #232a3b",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${formatSecondsAgo(t as number)} ago`}
            formatter={(value: number) => [formatRate(value), "throughput"]}
          />
          <Line
            type="monotone"
            dataKey="throughput"
            stroke="#60a5fa"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </Panel>
  );
}
