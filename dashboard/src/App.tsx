import { PanelGrid } from "./components/Panel";
import { ConnectionIndicator } from "./components/ConnectionIndicator";
import { ThroughputPanel } from "./components/panels/ThroughputPanel";
import { LatencyByTierPanel } from "./components/panels/LatencyByTierPanel";
import { P0ScoreboardPanel } from "./components/panels/P0ScoreboardPanel";
import { QueueDepthPanel } from "./components/panels/QueueDepthPanel";
import { useMetricsSocket } from "./hooks/useMetricsSocket";

export default function App() {
  const { status, latest, history } = useMetricsSocket();

  return (
    <div className="min-h-screen bg-surface px-6 py-5">
      <header className="mb-5 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold tracking-tight text-ink">PULSE</h1>
          <p className="text-xs text-ink-muted">
            adaptive event pipeline — {latest ? latest.mode : "…"}
          </p>
        </div>
        <ConnectionIndicator status={status} />
      </header>

      <PanelGrid>
        <P0ScoreboardPanel latest={latest} />
        <ThroughputPanel history={history} />
        <LatencyByTierPanel history={history} />
        <QueueDepthPanel history={history} />
      </PanelGrid>
    </div>
  );
}
