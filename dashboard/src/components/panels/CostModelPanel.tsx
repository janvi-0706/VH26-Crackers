import { useEffect, useRef, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Panel } from "../Panel";
import * as api from "../../lib/api";
import type { CostModelRow } from "../../lib/api";

const EVENT_TYPES = ["payment", "order", "inventory", "click", "log"] as const;
type EventTypeName = (typeof EVENT_TYPES)[number];

const TYPE_COLOR: Record<EventTypeName, string> = {
  payment: "#60a5fa", // matches tier.p0
  order: "#60a5fa",
  inventory: "#c084fc", // matches tier.p1
  click: "#fb923c", // matches tier.p2
  log: "#fbbf24",
};

const POLL_MS = 1000;
const MAX_POINTS = 180; // 3 minutes at 1s — long enough to watch a real re-adaptation play out

interface Point {
  t: number; // seconds since this panel started watching
  learned: number;
}

/**
 * costmodel.py's own learned-vs-prior estimate, polled from `GET
 * /control/costmodel` (not the 4Hz `/ws` stream — this is deliberately a
 * separate, slower poll: MetricsFrame is frozen, and "learned vs prior
 * cost per type" is not one of its fields, so this panel reads the
 * dedicated endpoint Stage I's own prompt asks to expose, on its own
 * cadence, rather than forcing a contract change for one panel).
 *
 * One type at a time, picked by the small tab row above the chart — five
 * learned lines sharing one y-axis would need normalising against each
 * type's own very different prior (payment ~3.5u vs log ~0.3u) to read as
 * anything but visual noise; a single active type keeps "the config prior
 * as a dotted line" a literal, one-glance comparison against ITS OWN
 * learned line, exactly what the prompt asks for.
 */
export function CostModelPanel() {
  const [selected, setSelected] = useState<EventTypeName>("payment");
  const [rows, setRows] = useState<CostModelRow[]>([]);
  const [multiplierBusy, setMultiplierBusy] = useState(false);
  const [multiplierStatus, setMultiplierStatus] = useState("normal payload mix");
  const historyRef = useRef<Record<EventTypeName, Point[]>>({
    payment: [], order: [], inventory: [], click: [], log: [],
  });
  const startRef = useRef<number>(Date.now());
  const [, forceRender] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const fresh = await api.getCostModel();
      if (cancelled || fresh.length === 0) return;
      const t = (Date.now() - startRef.current) / 1000;
      for (const row of fresh) {
        const name = row.event_type as EventTypeName;
        const series = historyRef.current[name];
        if (!series) continue;
        series.push({ t, learned: row.learned });
        if (series.length > MAX_POINTS) series.shift();
      }
      setRows(fresh);
      forceRender((n) => n + 1);
    };
    tick();
    const id = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const activeRow = rows.find((r) => r.event_type === selected);
  const points = historyRef.current[selected];

  const runHeavyMix = async (multiplier: number) => {
    setMultiplierBusy(true);
    try {
      await api.setPayloadMultiplier(multiplier);
      setMultiplierStatus(
        multiplier === 1.0 ? "normal payload mix" : `${multiplier.toFixed(1)}x heavier payload mix`
      );
    } finally {
      setMultiplierBusy(false);
    }
  };

  return (
    <Panel
      title="Cost model: learned vs. prior"
      cols={12}
      headline={
        activeRow
          ? `${activeRow.learned.toFixed(3)}u learned · ${(activeRow.confidence * 100).toFixed(0)}% confidence`
          : "—"
      }
      footer={
        <div className="flex items-center justify-between gap-3">
          <span>{multiplierStatus}</span>
          <div className="flex gap-1">
            <button
              type="button"
              disabled={multiplierBusy}
              onClick={() => runHeavyMix(1.0)}
              className="rounded border border-surface-border px-2 py-0.5 text-[10px] uppercase text-ink-muted hover:text-ink disabled:opacity-60"
            >
              normal
            </button>
            <button
              type="button"
              disabled={multiplierBusy}
              onClick={() => runHeavyMix(3.0)}
              className="rounded border border-warn/50 px-2 py-0.5 text-[10px] uppercase text-warn hover:bg-warn/10 disabled:opacity-60"
            >
              3x heavier mix
            </button>
          </div>
        </div>
      }
    >
      <div className="flex h-full flex-col gap-1">
        <div className="flex shrink-0 gap-1">
          {EVENT_TYPES.map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setSelected(t)}
              className={`rounded px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide transition-colors ${
                selected === t
                  ? "bg-surface-raised text-ink"
                  : "text-ink-faint hover:text-ink-muted"
              }`}
            >
              {t}
            </button>
          ))}
        </div>
        <div className="min-h-0 flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points} margin={{ top: 4, right: 8, bottom: 0, left: -12 }}>
              <CartesianGrid stroke="#232a3b" strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="t"
                tickFormatter={(v: number) => `${v.toFixed(0)}s`}
                stroke="#5b6478"
                fontSize={11}
                tickLine={false}
              />
              <YAxis
                stroke="#5b6478"
                fontSize={11}
                tickLine={false}
                width={40}
                domain={["auto", "auto"]}
                tickFormatter={(v: number) => v.toFixed(1)}
              />
              <Tooltip
                contentStyle={{
                  background: "#12161f",
                  border: "1px solid #232a3b",
                  borderRadius: 8,
                  fontSize: 12,
                }}
                labelFormatter={(t) => `${(t as number).toFixed(0)}s`}
                formatter={(value: number) => [`${value.toFixed(3)}u`, "learned"]}
              />
              {activeRow && (
                <ReferenceLine
                  y={activeRow.prior}
                  stroke="#8b93a7"
                  strokeDasharray="4 4"
                  label={{ value: "config prior", position: "insideTopRight", fill: "#8b93a7", fontSize: 10 }}
                />
              )}
              <Line
                type="monotone"
                dataKey="learned"
                name="learned"
                stroke={TYPE_COLOR[selected]}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </Panel>
  );
}
