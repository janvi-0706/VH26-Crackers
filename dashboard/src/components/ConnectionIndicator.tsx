import type { ConnectionStatus } from "../hooks/useMetricsSocket";

const LABEL: Record<ConnectionStatus, string> = {
  connecting: "connecting…",
  open: "live",
  reconnecting: "reconnecting…",
  closed: "disconnected",
};

const DOT_CLASS: Record<ConnectionStatus, string> = {
  connecting: "bg-warn animate-pulse",
  open: "bg-good",
  reconnecting: "bg-warn animate-pulse",
  closed: "bg-bad",
};

/** Must be impossible to miss: this is the "is the demo even connected"
 * signal a judge notices before any chart. */
export function ConnectionIndicator({ status }: { status: ConnectionStatus }) {
  return (
    <div className="flex items-center gap-2 rounded-full border border-surface-border bg-surface-panel px-3 py-1.5">
      <span className={`h-2.5 w-2.5 rounded-full ${DOT_CLASS[status]}`} />
      <span className="font-mono text-xs text-ink-muted">{LABEL[status]}</span>
    </div>
  );
}
