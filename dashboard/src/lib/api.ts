/**
 * Thin wrapper over the backend's /control/* endpoints.
 *
 * Hardcoded to :8000, same as useMetricsSocket's WS_URL — the backend
 * always listens there regardless of which port actually served this page
 * (FastAPI's own static mount on 8000 itself, or the Vite dev server on
 * 5173 during development). Keeping both at a hardcoded absolute origin
 * means `npm run dev` works against a real backend with zero proxy config.
 */

import type { DecisionTrace } from "../types/metrics";

const API_BASE = "http://localhost:8000";

/** GET /audit.csv's own URL — the download button in ControlBar links here
 * directly (the backend already sets Content-Disposition: attachment, so
 * a plain anchor is all a real download needs; no fetch-and-blob dance). */
export const AUDIT_CSV_URL = `${API_BASE}/audit.csv`;

export type QueueMode = "naive" | "adaptive";

async function post(path: string, body?: unknown): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

export async function setRate(rate: number): Promise<void> {
  await post("/control/rate", { rate });
}

/** Instant jump to the spec's 20000/min step function — no ramp. */
export async function spike(): Promise<void> {
  await post("/control/spike");
}

/** Walks the pipeline back to a clean baseline. Leaves mode untouched. */
export async function reset(): Promise<void> {
  await post("/control/reset");
}

export async function setMode(mode: QueueMode): Promise<void> {
  await post("/control/mode", { mode });
}

/** decision.py's six live weights: score's w1/w2, pressure's a/b/c/d.
 * Mirrors decision.get_weights()'s flat shape exactly. */
export interface Weights {
  w1: number;
  w2: number;
  a: number;
  b: number;
  c: number;
  d: number;
}

export async function getWeights(): Promise<Weights> {
  const res = await fetch(`${API_BASE}/control/weights`);
  return (await res.json()) as Weights;
}

/** Partial on purpose: a dashboard slider only ever reports the one value
 * it moved. The backend renormalises that value's group (w1+w2, a+b+c+d)
 * back to summing to 1.0 and returns the resulting full set — the response
 * is the new source of truth, including for sliders this call didn't name. */
export async function setWeights(partial: Partial<Weights>): Promise<Weights> {
  const res = await post("/control/weights", partial);
  return (await res.json()) as Weights;
}

/** GET /audit/trace/{event_id} — one decision trace from the backend's
 * 500-item ring buffer. `null` for a 404 (unknown id, or aged out of the
 * buffer — the two are indistinguishable from here, same as the backend
 * itself); throws for anything else, so the caller can tell "not found"
 * apart from "the request itself failed". */
export async function getTrace(eventId: string): Promise<DecisionTrace | null> {
  const res = await fetch(`${API_BASE}/audit/trace/${encodeURIComponent(eventId)}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`GET /audit/trace/${eventId} -> ${res.status}`);
  return (await res.json()) as DecisionTrace;
}

/** POST /chaos/kill-worker — cancels one live worker task for real (the
 * same cancellation an actual crash would deliver). `worker_id: null` means
 * the pool had no live worker to kill, not a failure. */
export interface KillWorkerResult {
  worker_id: number | null;
}

export async function killWorker(): Promise<KillWorkerResult> {
  const res = await post("/chaos/kill-worker");
  return (await res.json()) as KillWorkerResult;
}

/** POST /chaos/duplicate-flood — replays up to `count` of the most
 * recently sink-committed events as genuine new duplicate deliveries
 * (same dedup_key/idempotency_key, new event_id). */
export interface DuplicateFloodResult {
  requested: number;
  replayed: number;
  admitted: number;
  suppressed: number;
}

export async function duplicateFlood(count = 1000): Promise<DuplicateFloodResult> {
  const res = await post("/chaos/duplicate-flood", { count });
  return (await res.json()) as DuplicateFloodResult;
}
