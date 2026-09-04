import { PanelGrid } from "./components/Panel";
import { ConnectionIndicator } from "./components/ConnectionIndicator";
import { ControlBar } from "./components/ControlBar";
import { ThroughputPanel } from "./components/panels/ThroughputPanel";
import { LatencyByTierPanel } from "./components/panels/LatencyByTierPanel";
import { P0ScoreboardPanel } from "./components/panels/P0ScoreboardPanel";
import { QueueDepthPanel } from "./components/panels/QueueDepthPanel";
import { PressureGaugePanel } from "./components/panels/PressureGaugePanel";
import { ModeByTierPanel } from "./components/panels/ModeByTierPanel";
import { DeferredBacklogPanel } from "./components/panels/DeferredBacklogPanel";
import { WeightsPanel } from "./components/panels/WeightsPanel";
import { LadderPanel } from "./components/panels/LadderPanel";
import { RatesPanel } from "./components/panels/RatesPanel";
import { ConservationPanel } from "./components/panels/ConservationPanel";
import { ShedLogPanel } from "./components/panels/ShedLogPanel";
import { EventInspectorPanel } from "./components/panels/EventInspectorPanel";
import { useMetricsSocket } from "./hooks/useMetricsSocket";
import * as api from "./lib/api";

export default function App() {
  const { status, latest, history, clearHistory } = useMetricsSocket();

  const handleReset = async () => {
    await api.reset();
    clearHistory();
  };

  return (
    <div className="min-h-screen bg-transparent px-6 py-5">
      <header className="mb-5 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold tracking-tight text-ink">PULSE</h1>
          <p className="text-xs text-ink-muted">
            adaptive event pipeline — {latest ? latest.mode : "…"}
          </p>
        </div>
        <ConnectionIndicator status={status} />
      </header>

      <ControlBar currentMode={latest?.mode ?? null} onReset={handleReset} />

      <PanelGrid>
        <ConservationPanel latest={latest} />
        <P0ScoreboardPanel latest={latest} />
        <PressureGaugePanel latest={latest} />
        <ModeByTierPanel latest={latest} />
        <LadderPanel latest={latest} />
        <RatesPanel history={history} />
        <ThroughputPanel history={history} />
        <LatencyByTierPanel history={history} />
        <QueueDepthPanel history={history} />
        <DeferredBacklogPanel history={history} />
        <ShedLogPanel latest={latest} />
        <EventInspectorPanel />
        <WeightsPanel />
      </PanelGrid>
    </div>
  );
}
