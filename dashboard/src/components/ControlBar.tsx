import { useState } from "react";
import * as api from "../lib/api";
import type { QueueMode } from "../lib/api";
import type { Mode } from "../types/metrics";

// Slider ceiling: comfortably above baseline (16.65 eps) so small
// adjustments are visible, without needing to drag past the point where
// SPIKE is the only button that matters anyway.
const RATE_SLIDER_MAX = 400;

export interface ControlBarProps {
  /** The backend's own report of which mode is live — the toggle reflects
   * this, not local optimistic state, so it can never show something the
   * queue isn't actually doing. */
  currentMode: Mode | null;
  /** Composed by the caller: the reset API call plus whatever local UI
   * state (chart history) also needs wiping. ControlBar doesn't own the
   * chart, so it doesn't decide that on its own — see App.tsx. */
  onReset: () => void | Promise<void>;
}

export function ControlBar({ currentMode, onReset }: ControlBarProps) {
  const [rate, setRateValue] = useState(16.65);
  const [busy, setBusy] = useState<string | null>(null);

  const run = (label: string, action: () => Promise<void>) => async () => {
    setBusy(label);
    try {
      await action();
    } finally {
      setBusy(null);
    }
  };

  const mode: QueueMode = currentMode === "naive" ? "naive" : "adaptive";

  return (
    <div className="mb-5 flex flex-wrap items-center gap-4 rounded-xl border border-surface-border bg-surface-panel px-4 py-3">
      {/* -- rate slider ----------------------------------------------- */}
      <div className="flex min-w-[220px] flex-1 items-center gap-3">
        <label htmlFor="rate-slider" className="text-xs font-medium uppercase tracking-wide text-ink-muted">
          Rate
        </label>
        <input
          id="rate-slider"
          type="range"
          min={0}
          max={RATE_SLIDER_MAX}
          step={1}
          value={Math.min(rate, RATE_SLIDER_MAX)}
          onChange={(e) => setRateValue(Number(e.target.value))}
          onMouseUp={() => api.setRate(rate)}
          onTouchEnd={() => api.setRate(rate)}
          className="h-2 flex-1 cursor-pointer accent-tier-p0"
        />
        <span className="w-24 shrink-0 font-mono text-sm tabular-nums text-ink">
          {rate.toFixed(1)} eps
        </span>
      </div>

      {/* -- naive / adaptive toggle ------------------------------------ */}
      <div className="flex overflow-hidden rounded-lg border border-surface-border" role="group" aria-label="scheduling mode">
        {(["adaptive", "naive"] as const).map((m) => (
          <button
            key={m}
            type="button"
            onClick={run(`mode-${m}`, () => api.setMode(m))}
            disabled={busy !== null}
            aria-pressed={mode === m}
            className={`px-3 py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors disabled:opacity-60 ${
              mode === m
                ? "bg-tier-p0 text-surface"
                : "bg-transparent text-ink-muted hover:text-ink"
            }`}
          >
            {m}
          </button>
        ))}
      </div>

      {/* -- reset ------------------------------------------------------- */}
      <button
        type="button"
        onClick={run("reset", async () => { await onReset(); })}
        disabled={busy !== null}
        className="rounded-lg border border-surface-border px-4 py-2 text-xs font-semibold uppercase tracking-wide text-ink-muted transition-colors hover:border-ink-muted hover:text-ink disabled:opacity-60"
      >
        Reset
      </button>

      {/* -- audit.csv export --------------------------------------------
          A plain anchor, not a fetch-and-blob dance: the backend already
          sets Content-Disposition: attachment on GET /audit.csv, so the
          browser's own download handling is all this needs. */}
      <a
        href={api.AUDIT_CSV_URL}
        className="rounded-lg border border-surface-border px-4 py-2 text-xs font-semibold uppercase tracking-wide text-ink-muted transition-colors hover:border-ink-muted hover:text-ink"
      >
        ⬇ audit.csv
      </a>

      {/* -- SPIKE: unmissable, impossible to misclick ------------------- */}
      <button
        type="button"
        onClick={run("spike", api.spike)}
        disabled={busy !== null}
        className="rounded-xl bg-bad px-8 py-3 text-base font-black uppercase tracking-widest text-white
          shadow-lg shadow-bad/40 transition-transform hover:scale-[1.03] hover:shadow-bad/60
          active:scale-[0.98] disabled:opacity-60 disabled:hover:scale-100"
      >
        {busy === "spike" ? "Spiking…" : "⚡ Spike"}
      </button>
    </div>
  );
}
