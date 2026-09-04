import { useState } from "react";
import { Panel } from "../Panel";
import * as api from "../../lib/api";
import type { DecisionTrace } from "../../types/metrics";

type LookupState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "not_found" }
  | { kind: "error"; message: string }
  | { kind: "found"; trace: DecisionTrace };

const FIELD_ORDER: (keyof DecisionTrace)[] = [
  "event_id", "seq", "type", "tier", "decision", "reason", "pressure", "value", "ts",
];

/**
 * Paste an event_id, see the full DecisionTrace — GET /audit/trace/{id},
 * backed by ledger.py's own 500-item ring buffer (Stage F). A 404 there is
 * genuinely ambiguous (unknown id vs. aged out of the buffer under a real
 * spike's decision rate — measured directly at ~700/sec, so the buffer
 * can rotate completely in well under a second) and is shown as such
 * rather than guessing which one it was.
 */
export function EventInspectorPanel() {
  const [eventId, setEventId] = useState("");
  const [state, setState] = useState<LookupState>({ kind: "idle" });

  const lookup = async () => {
    const id = eventId.trim();
    if (!id) return;
    setState({ kind: "loading" });
    try {
      const trace = await api.getTrace(id);
      setState(trace === null ? { kind: "not_found" } : { kind: "found", trace });
    } catch {
      setState({ kind: "error", message: "request failed — is the backend reachable?" });
    }
  };

  return (
    <Panel title="Event inspector" cols={4}>
      <div className="flex h-full flex-col gap-2">
        <div className="flex gap-2">
          <input
            value={eventId}
            onChange={(e) => setEventId(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") lookup();
            }}
            placeholder="evt-00012345"
            spellCheck={false}
            className="min-w-0 flex-1 rounded-md border border-surface-border bg-surface px-2.5 py-1.5 font-mono text-xs text-ink placeholder:text-ink-faint focus:border-tier-p0 focus:outline-none"
          />
          <button
            type="button"
            onClick={lookup}
            disabled={state.kind === "loading" || eventId.trim() === ""}
            className="shrink-0 rounded-md bg-tier-p0 px-3 py-1.5 text-xs font-semibold text-surface disabled:opacity-50"
          >
            {state.kind === "loading" ? "…" : "Look up"}
          </button>
        </div>

        <div className="flex-1 overflow-y-auto">
          {state.kind === "idle" && (
            <p className="text-xs italic text-ink-faint">
              paste an event_id from the shed log or a row in audit.csv
            </p>
          )}
          {state.kind === "not_found" && (
            <p className="text-xs text-bad">
              no trace found — unknown id, or aged out of the 500-item ring buffer
            </p>
          )}
          {state.kind === "error" && <p className="text-xs text-bad">{state.message}</p>}
          {state.kind === "found" && (
            <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 font-mono text-xs">
              {FIELD_ORDER.map((key) => (
                <div key={key} className="contents">
                  <dt className="text-ink-faint">{key}</dt>
                  <dd className="break-all text-ink">{String(state.trace[key] ?? "—")}</dd>
                </div>
              ))}
            </dl>
          )}
        </div>
      </div>
    </Panel>
  );
}
