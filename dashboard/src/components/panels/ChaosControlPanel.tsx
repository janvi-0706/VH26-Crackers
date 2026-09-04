import { useState } from "react";
import { Panel } from "../Panel";
import * as api from "../../lib/api";

interface ChaosControlPanelProps {
  /** Lifted to App.tsx so RecoveryPanel's own "workers killed" tile can
   * show it too — a POST response is the only place this number exists;
   * nothing streams it. */
  onWorkerKilled: (workerId: number) => void;
}

/**
 * "The kill button is the most memorable ten seconds in our demo" — this
 * panel is built to be watched, not just clicked: a live, blunt status
 * line under each button says exactly what just happened, in the same
 * words the ADR/PROGRESS docs use, so a presenter can read it straight off
 * the screen instead of narrating from memory.
 */
export function ChaosControlPanel({ onWorkerKilled }: ChaosControlPanelProps) {
  const [busy, setBusy] = useState<string | null>(null);
  const [killStatus, setKillStatus] = useState<string>("no worker killed yet");
  const [floodStatus, setFloodStatus] = useState<string>("no flood run yet");

  const runKill = async () => {
    setBusy("kill");
    try {
      const { worker_id } = await api.killWorker();
      if (worker_id === null) {
        setKillStatus("no live worker to kill (pool not running)");
      } else {
        onWorkerKilled(worker_id);
        setKillStatus(`worker ${worker_id} killed — recovering…`);
      }
    } catch {
      setKillStatus("request failed");
    } finally {
      setBusy(null);
    }
  };

  const runFlood = async () => {
    setBusy("flood");
    try {
      const result = await api.duplicateFlood(1000);
      setFloodStatus(
        `replayed ${result.replayed} · suppressed ${result.suppressed} · admitted ${result.admitted}`
      );
    } catch {
      setFloodStatus("request failed");
    } finally {
      setBusy(null);
    }
  };

  return (
    <Panel title="Chaos control" cols={6} accent="warn">
      <div className="flex h-full items-center justify-center gap-10 px-8">
        <div className="flex flex-1 flex-col items-center gap-4">
          <button
            type="button"
            onClick={runKill}
            disabled={busy !== null}
            className="w-full rounded-2xl bg-bad px-8 py-12 text-4xl font-black uppercase tracking-widest text-white
              shadow-lg shadow-bad/40 transition-transform hover:scale-[1.03] hover:shadow-bad/60
              active:scale-[0.98] disabled:opacity-60 disabled:hover:scale-100"
          >
            {busy === "kill" ? "Killing…" : "☠ Kill worker"}
          </button>
          <span className="text-center text-base text-ink-muted">{killStatus}</span>
        </div>
        <div className="flex flex-1 flex-col items-center gap-4">
          <button
            type="button"
            onClick={runFlood}
            disabled={busy !== null}
            className="w-full rounded-2xl bg-warn px-8 py-12 text-4xl font-black uppercase tracking-widest text-surface
              shadow-lg shadow-warn/40 transition-transform hover:scale-[1.03] hover:shadow-warn/60
              active:scale-[0.98] disabled:opacity-60 disabled:hover:scale-100"
          >
            {busy === "flood" ? "Flooding…" : "🌊 Duplicate flood"}
          </button>
          <span className="text-center text-base text-ink-muted">{floodStatus}</span>
        </div>
      </div>
    </Panel>
  );
}
