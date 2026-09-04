import type { MetricsFrame } from "../types/metrics";

/** One point on a time-series chart: seconds-ago on the x-axis rather than a
 * wall-clock timestamp, so the chart reads the same whether the page has
 * been open five seconds or five hours. */
export interface ChartPoint {
  t: number; // seconds before "now" (the latest frame in the window)
  [series: string]: number;
}

/** Turn a window of frames into chart points, keyed by whatever accessor
 * each series needs. Shared by every time-series panel so "how do we turn
 * frames into an x-axis" is answered once. */
export function toChartPoints(
  frames: MetricsFrame[],
  series: Record<string, (frame: MetricsFrame) => number>,
): ChartPoint[] {
  if (frames.length === 0) return [];
  const latestTs = frames[frames.length - 1].ts;
  return frames.map((frame) => {
    const point: ChartPoint = { t: Math.round((frame.ts - latestTs) * 10) / 10 };
    for (const [key, accessor] of Object.entries(series)) {
      point[key] = accessor(frame);
    }
    return point;
  });
}

export function formatMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  return `${ms.toFixed(0)}ms`;
}

export function formatRate(perSecond: number): string {
  if (perSecond >= 1000) return `${(perSecond / 1000).toFixed(1)}k/s`;
  return `${perSecond.toFixed(1)}/s`;
}

export function formatSecondsAgo(t: number): string {
  if (t === 0) return "now";
  return `${t.toFixed(0)}s`;
}
