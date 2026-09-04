import { useState } from "react";
import { PanelGrid } from "./components/Panel";
import { ConnectionIndicator } from "./components/ConnectionIndicator";
import { ControlBar } from "./components/ControlBar";
import { TabBar } from "./components/TabBar";
import { LatencyByTierPanel } from "./components/panels/LatencyByTierPanel";
import { P0ScoreboardPanel } from "./components/panels/P0ScoreboardPanel";
import { QueueDepthPanel } from "./components/panels/QueueDepthPanel";
import { PressureGaugePanel } from "./components/panels/PressureGaugePanel";
import { DeferredBacklogPanel } from "./components/panels/DeferredBacklogPanel";
import { WeightsPanel } from "./components/panels/WeightsPanel";
import { LadderPanel } from "./components/panels/LadderPanel";
import { RatesPanel } from "./components/panels/RatesPanel";
import { ConservationPanel } from "./components/panels/ConservationPanel";
import { ShedLogPanel } from "./components/panels/ShedLogPanel";
import { EventInspectorPanel } from "./components/panels/EventInspectorPanel";
import { WorkerPoolGridPanel } from "./components/panels/WorkerPoolGridPanel";
import { CostComparisonPanel } from "./components/panels/CostComparisonPanel";
import { ChaosControlPanel } from "./components/panels/ChaosControlPanel";
import { RecoveryPanel } from "./components/panels/RecoveryPanel";
import { CostModelPanel } from "./components/panels/CostModelPanel";
import { useMetricsSocket } from "./hooks/useMetricsSocket";
import * as api from "./lib/api";

/**
 * Rebuilt from Stage H's own "everything in one 6-row grid" layout: with
 * 16 panels, cramming all of them onto one 1920x1080 screen at once meant
 * every chart got a slice barely 150px tall — technically "fits without
 * scrolling", but not actually readable, and charts in particular need
 * real vertical room to be more than a squint-and-guess trend line.
 *
 * The fix is tabs, not a smaller grid: only ONE tab's own panels ever
 * render at a time, each still using `PanelGrid`'s own fixed-row-count
 * approach (so a tab's own panels always fill exactly the space
 * available, no scrolling within a tab either) but now with `rows={1}`
 * for every tab — a handful of panels each get the ENTIRE remaining
 * height, not a sixth of it. Every panel component is unchanged from
 * Stage H/I; only which tab renders it, and each one's own `cols` (still
 * owned by the panel itself, not passed down), changed to fit its new
 * row.
 *
 *   Status   — Conservation(5) + P0 scoreboard(3) + Pressure(2) + Ladder(2)
 *   Traffic  — Rates(4) + Latency by tier(4) + Queue depth(4)
 *   Backlog  — Deferred(4) + Worker pool(4) + Shed log(4)
 *   Cost     — Cost comparison(4) + Cost model convergence(8)
 *   Chaos    — Chaos control(6) + Recovery(6)
 *   Controls — Event inspector(4) + Weights(8)
 */
type TabId = "status" | "traffic" | "backlog" | "cost" | "chaos" | "controls";

const TABS: readonly { id: TabId; label: string }[] = [
  { id: "status", label: "Status" },
  { id: "traffic", label: "Traffic" },
  { id: "backlog", label: "Backlog" },
  { id: "cost", label: "Cost" },
  { id: "chaos", label: "Chaos" },
  { id: "controls", label: "Controls" },
];

export default function App() {
  const { status, latest, history, clearHistory } = useMetricsSocket();
  const [workersKilled, setWorkersKilled] = useState(0);
  const [tab, setTab] = useState<TabId>("status");

  const handleReset = async () => {
    await api.reset();
    clearHistory();
  };

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-transparent px-5 py-3">
      <header className="mb-2 flex shrink-0 items-center justify-between">
        <div>
          <h1 className="text-lg font-bold tracking-tight text-ink">PULSE</h1>
          <p className="text-xs text-ink-muted">
            adaptive event pipeline — {latest ? latest.mode : "…"}
          </p>
        </div>
        <ConnectionIndicator status={status} />
      </header>

      <div className="shrink-0">
        <ControlBar currentMode={latest?.mode ?? null} onReset={handleReset} />
      </div>

      <div className="mb-3 shrink-0">
        <TabBar tabs={TABS} active={tab} onChange={setTab} />
      </div>

      <div className="min-h-0 flex-1">
        {tab === "status" && (
          <PanelGrid rows={1}>
            <ConservationPanel latest={latest} />
            <P0ScoreboardPanel latest={latest} />
            <PressureGaugePanel latest={latest} />
            <LadderPanel latest={latest} />
          </PanelGrid>
        )}

        {tab === "traffic" && (
          <PanelGrid rows={1}>
            <RatesPanel history={history} />
            <LatencyByTierPanel history={history} />
            <QueueDepthPanel history={history} />
          </PanelGrid>
        )}

        {tab === "backlog" && (
          <PanelGrid rows={1}>
            <DeferredBacklogPanel history={history} />
            <WorkerPoolGridPanel latest={latest} />
            <ShedLogPanel latest={latest} />
          </PanelGrid>
        )}

        {tab === "cost" && (
          <PanelGrid rows={1}>
            <CostComparisonPanel latest={latest} />
            <CostModelPanel />
          </PanelGrid>
        )}

        {tab === "chaos" && (
          <PanelGrid rows={1}>
            <ChaosControlPanel onWorkerKilled={() => setWorkersKilled((n) => n + 1)} />
            <RecoveryPanel latest={latest} workersKilled={workersKilled} />
          </PanelGrid>
        )}

        {tab === "controls" && (
          <PanelGrid rows={1}>
            <EventInspectorPanel />
            <WeightsPanel />
          </PanelGrid>
        )}
      </div>
    </div>
  );
}
