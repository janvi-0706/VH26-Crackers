/**
 * Mirrors src/triage/contracts.py. This file has no import from the backend
 * on purpose — the WebSocket boundary is the actual contract, so the two are
 * kept in sync by hand and by eye, the same way any two ends of a wire
 * protocol are. If a field is added to MetricsFrame, add it here too.
 */

export type Tier = "P0" | "P1" | "P2";

export const TIERS: readonly Tier[] = ["P0", "P1", "P2"];

export type EventType = "payment" | "order" | "inventory" | "click" | "log";

export type Decision =
  | "STREAM_NOW"
  | "MICRO_BATCH"
  | "DEFER"
  | "SAMPLE_ROLLUP"
  | "SHED";

export type Mode = "adaptive" | "naive";

export type PerTier<T> = Record<Tier, T>;

export interface DecisionTrace {
  seq: number;
  event_id: string;
  type: EventType | null;
  tier: Tier | null;
  decision: Decision | null;
  reason: string;
  pressure: number;
  value: number;
  ts: number;
}

export interface ShedRecord {
  seq: number;
  event_id: string;
  type: EventType | null;
  tier: Tier | null;
  reason: string;
  pressure: number;
  value: number;
  ts: number;
}

/** One instant of the pipeline, pushed over /ws at 4 Hz. Every field is
 * always present — contracts.py defaults every one of them — so this type
 * has no optional properties. */
export interface MetricsFrame {
  schema_version: number;
  ts: number;
  mode: Mode;

  queue_depth: PerTier<number>;

  latency_p50: PerTier<number>;
  latency_p95: PerTier<number>;
  latency_p99: PerTier<number>;
  latency_p50_all: number;
  latency_p95_all: number;
  latency_p99_all: number;

  throughput: number;
  offered_rate: number;
  admitted_rate: number;
  service_rate: number;

  pressure: number;
  ladder_rung: PerTier<number>;
  spike_multiplier: number;

  worker_count: number;
  active_workers: number;

  ingested: number;
  processed: number;
  in_queue: number;
  in_flight: number;
  deferred_pending: number;
  sampled_out: number;
  shed: number;

  weighted_click_count: number;
  true_click_count: number;

  cost_adaptive: number;
  cost_naive: number;
  value_delivered: number;
  value_shed: number;

  sla_met: PerTier<number>;
  sla_missed: PerTier<number>;

  retries: number;
  duplicates_caught: number;
  exactly_once_violations: number;

  recent_decisions: DecisionTrace[];
  recent_sheds: ShedRecord[];
}

/** The P0 SLA target from config/tiers.yaml (payment: 200ms). Duplicated
 * here deliberately, the same way the TS types duplicate contracts.py:
 * the dashboard should not need a live config fetch just to draw one line. */
export const P0_LATENCY_TARGET_MS = 200;

/** decision.decide()'s own pressure bands (src/triage/decision.py):
 * P < STREAM_MAX -> STREAM_NOW, [STREAM_MAX, BATCH_MAX) -> MICRO_BATCH,
 * >= BATCH_MAX -> DEFER. Duplicated here for the same reason as the target
 * above — the gauge and the per-tier mode panel both need these thresholds
 * to draw their own bands and labels without a live config fetch. */
export const PRESSURE_STREAM_MAX = 0.4;
export const PRESSURE_BATCH_MAX = 0.75;
