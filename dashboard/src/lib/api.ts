/**
 * Thin wrapper over the backend's /control/* endpoints.
 *
 * Hardcoded to :8000, same as useMetricsSocket's WS_URL — the backend
 * always listens there regardless of which port actually served this page
 * (FastAPI's own static mount on 8000 itself, or the Vite dev server on
 * 5173 during development). Keeping both at a hardcoded absolute origin
 * means `npm run dev` works against a real backend with zero proxy config.
 */

const API_BASE = "http://localhost:8000";

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
