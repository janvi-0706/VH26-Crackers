import { useEffect, useRef, useState } from "react";
import { Panel } from "../Panel";
import * as api from "../../lib/api";
import type { Weights } from "../../lib/api";

// decision.py's own defaults (ScoreWeights/PressureWeights) — shown until
// the initial GET /control/weights resolves, so the sliders never render
// at zero for a frame.
const DEFAULT_WEIGHTS: Weights = { w1: 0.7, w2: 0.3, a: 0.35, b: 0.35, c: 0.2, d: 0.1 };

// A slider fires onChange on every animation frame while dragging. Posting
// every one of those would flood the backend for no visible benefit; this
// caps it to a rate a human drag can't outrun while still reading as
// "instant" on stage.
const POST_THROTTLE_MS = 80;

interface SliderSpec {
  key: keyof Weights;
  group: "score" | "pressure";
  hint: string;
}

// Grouped exactly as decision.set_weights() renormalises them: w1+w2 must
// sum to 1.0 for score(), a+b+c+d must sum to 1.0 for pressure() (enforced
// by PressureWeights.__post_init__). Moving one slider in a group visibly
// moves the others in that same group once the response comes back.
const SLIDERS: SliderSpec[] = [
  { key: "w1", group: "score", hint: "density × urgency" },
  { key: "w2", group: "score", hint: "aging" },
  { key: "a", group: "pressure", hint: "queue depth ÷ saturation" },
  { key: "b", group: "pressure", hint: "arrival ÷ service rate" },
  { key: "c", group: "pressure", hint: "p95 sojourn ÷ SLA" },
  { key: "d", group: "pressure", hint: "worker utilisation" },
];

/**
 * The demo centrepiece: six sliders bound live to GET/POST /control/weights
 * (added in app.py for this panel). Dragging one posts the single changed
 * value; decision.set_weights() renormalises its group back to summing to
 * 1.0 and hands back the full live set, which is what this component then
 * renders — so a drag on `w1` is also, visibly, a drag on `w2` the instant
 * the response lands. No local recomputation of the sum-to-1 constraint:
 * the backend already owns that invariant (PressureWeights.__post_init__),
 * and duplicating it here would be exactly the kind of two-copies-of-one-
 * rule bug the rest of this project has gone out of its way to avoid.
 */
export function WeightsPanel() {
  const [weights, setWeightsState] = useState<Weights>(DEFAULT_WEIGHTS);
  const [loaded, setLoaded] = useState(false);
  const lastPostAtRef = useRef(0);
  const pendingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getWeights()
      .then((w) => {
        if (!cancelled) setWeightsState(w);
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const post = (key: keyof Weights, value: number) => {
    lastPostAtRef.current = performance.now();
    api
      .setWeights({ [key]: value } as Partial<Weights>)
      .then((w) => setWeightsState(w))
      .catch(() => {
        /* a dropped weight update is not worth breaking the slider over —
           the next drag tick (or the panel's next GET on remount) corrects
           it, and the demo must never crash mid-drag. */
      });
  };

  const scheduleThrottled = (key: keyof Weights, value: number) => {
    const elapsed = performance.now() - lastPostAtRef.current;
    if (pendingTimerRef.current) {
      clearTimeout(pendingTimerRef.current);
      pendingTimerRef.current = null;
    }
    if (elapsed >= POST_THROTTLE_MS) {
      post(key, value);
    } else {
      pendingTimerRef.current = setTimeout(() => {
        pendingTimerRef.current = null;
        post(key, value);
      }, POST_THROTTLE_MS - elapsed);
    }
  };

  const onChange = (key: keyof Weights, raw: string) => {
    const value = Number(raw);
    // Optimistic local update so the dragged handle itself never stutters
    // waiting on a round trip; the eventual response reconciles the whole
    // group (including this key) to the server's renormalised truth.
    setWeightsState((prev) => ({ ...prev, [key]: value }));
    scheduleThrottled(key, value);
  };

  return (
    <Panel
      title="Routing weights"
      size="wide"
      headline="live — /control/weights"
      footer={!loaded ? "loading current weights…" : undefined}
    >
      <div className="grid h-full grid-cols-1 content-center gap-x-6 gap-y-2.5 sm:grid-cols-2">
        {SLIDERS.map(({ key, hint }) => (
          <div key={key} className="flex items-center gap-3">
            <label
              htmlFor={`weight-${key}`}
              className="w-5 shrink-0 font-mono text-xs font-bold uppercase text-ink-muted"
            >
              {key}
            </label>
            <input
              id={`weight-${key}`}
              type="range"
              min={0}
              max={1}
              step={0.01}
              value={weights[key]}
              onChange={(e) => onChange(key, e.target.value)}
              className="h-2 flex-1 cursor-pointer accent-tier-p1"
            />
            <span className="w-11 shrink-0 text-right font-mono text-xs tabular-nums text-ink">
              {weights[key].toFixed(2)}
            </span>
            <span className="hidden w-36 shrink-0 truncate text-[11px] text-ink-faint xl:inline">
              {hint}
            </span>
          </div>
        ))}
      </div>
    </Panel>
  );
}
