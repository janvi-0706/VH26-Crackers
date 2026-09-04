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

/** ladder.Rung (src/triage/ladder.py) — the five-rung escalation ladder.
 * MetricsFrame.ladder_rung reports the integer value of the rung each
 * tier's most recently observed real decision actually landed on (Stage
 * E), not a client-side recomputation from pressure alone — the two extra
 * rungs above DEFER depend on CoDel/hard-shed state a pressure formula
 * cannot express. */
export const RUNG_LABEL: readonly string[] = [
  "stream",
  "micro-batch",
  "defer",
  "sample",
  "shed",
];

/** One CSS class bundle per rung, escalating good -> warn -> bad exactly
 * once congestion becomes lossy (rung 3, SAMPLE_ROLLUP) rather than at
 * DEFER (rung 2), which still preserves every field. */
export const RUNG_STYLE: readonly string[] = [
  "border-good/40 bg-good/15 text-good", // STREAM
  "border-good/40 bg-good/15 text-good", // MICRO_BATCH
  "border-warn/40 bg-warn/15 text-warn", // DEFER
  "border-bad/40 bg-bad/15 text-bad", // SAMPLE_ROLLUP
  "border-bad/40 bg-bad/15 text-bad", // SHED
];

/** config/tiers.yaml's own worker capacity (25 work-units/sec/worker,
 * frozen since Stage A). Duplicated here for the same reason as the
 * constants above — the cost-comparison panel needs it to turn
 * MetricsFrame.offered_rate (real, work-units/sec) into "workers this load
 * would need if linearly scaled", the same formula bench/run.py's own
 * cost model uses, without a live config fetch. */
export const WORKER_CAPACITY_UPS = 25;

/** bench/run.py's own stated, illustrative rate — duplicated here so the
 * live cost-comparison panel and the offline benchmark report tell the
 * same dollar story. Not tied to any vendor's real pricing; the ratio
 * between actual and naive-scaled cost is the argument, not this number. */
export const COST_PER_WORKER_SECOND_USD = 0.36 / 3600;
