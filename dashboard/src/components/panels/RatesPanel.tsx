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
import { formatRate, formatSecondsAgo, toChartPoints } from "../../lib/format";

const SERIES_COLOR = {
  offered: "#fbbf24", // amber — demand presented at the door
  admitted: "#34d399", // emerald — what actually got a credit
  service: "#60a5fa", // blue — what workers are actually completing
} as const;

/**
 * offered_rate / admitted_rate / service_rate, one chart, all three in the
 * same work-units/sec basis (src/triage/admission.py's own reasoning for
 * why offered_rate and admitted_rate are tracked on that basis rather than
 * raw event counts) so they are directly comparable on one y-axis.
 *
 * offered is the rate admission.py's AIMD credit gate is being *asked* to
 * admit, post the generator's own rate-throttle; admitted is what it
 * actually let through. The gap between those two lines is this stage's
 * whole point, made visible: upstream backpressure the generator itself is
 * applying, before an event even exists — not a downstream decision about
 * something already queued. service is what workers are completing right
 * now (real since Stage D) — the same number pressure's own b-term
 * (arrival/service) already reasons about, drawn here for direct
 * comparison against how much is even being let in.
 */
export function RatesPanel({ history }: { history: MetricsFrame[] }) {
  const latest = history[history.length - 1];
  const points = toChartPoints(history, {
    offered: (f) => f.offered_rate,
    admitted: (f) => f.admitted_rate,
    service: (f) => f.service_rate,
  });

  return (
    <Panel
      title="Offered / admitted / service rate"
      size="wide"
      headline={latest ? `gap ${formatRate(Math.max(0, latest.offered_rate - latest.admitted_rate))}` : "—"}
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
          <YAxis
            stroke="#5b6478"
            fontSize={11}
            tickLine={false}
            width={44}
            tickFormatter={(v: number) => formatRate(v)}
          />
          <Tooltip
            contentStyle={{
              background: "#12161f",
              border: "1px solid #232a3b",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${formatSecondsAgo(t as number)} ago`}
            formatter={(value: number, name: string) => [formatRate(value), name]}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: "#8b93a7" }} />
          {(["offered", "admitted", "service"] as const).map((key) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              name={key}
              stroke={SERIES_COLOR[key]}
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
