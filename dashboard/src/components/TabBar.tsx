export interface TabBarProps<T extends string> {
  tabs: readonly { id: T; label: string }[];
  active: T;
  onChange: (id: T) => void;
}

/**
 * A dashboard-wide tab switcher, not a per-panel one: the whole point of
 * tabbing the dashboard is that only ONE tab's worth of panels ever exist
 * in the DOM/layout at a time — a handful of panels, each given a full
 * share of the remaining screen, instead of every panel this project has
 * ever built all fighting for a slice of one fixed 6-row grid. Switching
 * tabs is instant (plain local state, no network round trip) since every
 * panel already gets its data from the same live `/ws` stream or its own
 * poll regardless of which tab is showing it.
 */
export function TabBar<T extends string>({ tabs, active, onChange }: TabBarProps<T>) {
  return (
    <div className="flex gap-1 rounded-xl border border-surface-border bg-surface-panel p-1">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          type="button"
          onClick={() => onChange(tab.id)}
          aria-pressed={active === tab.id}
          className={`rounded-lg px-4 py-2 text-xs font-semibold uppercase tracking-wide transition-colors ${
            active === tab.id
              ? "bg-tier-p0 text-surface"
              : "text-ink-muted hover:bg-surface-raised hover:text-ink"
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
