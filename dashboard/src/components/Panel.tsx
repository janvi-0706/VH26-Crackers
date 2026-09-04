import type { ReactNode } from "react";

/**
 * The layout system, built first and on purpose (per the Stage B prompt):
 * roughly ten panels land here over the next several stages, and a shared
 * sizing vocabulary matters more up front than the three charts that ship
 * with it today.
 *
 * `size` maps to a fixed span on the 12-column grid defined by
 * `PanelGrid` below. Because every panel only ever claims its own span,
 * adding the 11th panel cannot reflow the 10 already on screen — it just
 * takes the next slot the grid's auto-placement finds.
 */
export type PanelSize = "sm" | "md" | "lg" | "wide" | "tall" | "full";

const SIZE_CLASSES: Record<PanelSize, string> = {
  sm: "col-span-12 sm:col-span-6 lg:col-span-3 row-span-1",
  md: "col-span-12 sm:col-span-6 lg:col-span-4 row-span-1",
  lg: "col-span-12 lg:col-span-6 row-span-1",
  wide: "col-span-12 lg:col-span-8 row-span-1",
  tall: "col-span-12 sm:col-span-6 lg:col-span-4 row-span-2",
  full: "col-span-12 row-span-1",
};

export type PanelAccent = "neutral" | "good" | "bad" | "warn";

const ACCENT_BORDER: Record<PanelAccent, string> = {
  neutral: "border-surface-border",
  good: "border-good/60",
  bad: "border-bad/60",
  warn: "border-warn/60",
};

export interface PanelProps {
  title: string;
  size?: PanelSize;
  accent?: PanelAccent;
  /** Small right-aligned status text next to the title, e.g. a live value. */
  headline?: string;
  footer?: ReactNode;
  children: ReactNode;
}

export function Panel({
  title,
  size = "md",
  accent = "neutral",
  headline,
  footer,
  children,
}: PanelProps) {
  return (
    <section
      className={`${SIZE_CLASSES[size]} flex flex-col rounded-xl border bg-surface-panel
        ${ACCENT_BORDER[accent]} shadow-sm shadow-black/20 overflow-hidden`}
    >
      <header className="flex items-baseline justify-between gap-3 border-b border-surface-border px-4 py-2.5">
        <h2 className="text-xs font-semibold uppercase tracking-wide text-ink-muted">
          {title}
        </h2>
        {headline && (
          <span className="font-mono text-sm text-ink-muted tabular-nums">{headline}</span>
        )}
      </header>
      <div className="flex-1 min-h-0 p-3">{children}</div>
      {footer && (
        <footer className="border-t border-surface-border px-4 py-2 text-xs text-ink-faint">
          {footer}
        </footer>
      )}
    </section>
  );
}

/** The 12-column grid every Panel drops into. Fixed row height so a panel's
 * span (via row-span) is predictable instead of depending on content. */
export function PanelGrid({ children }: { children: ReactNode }) {
  return (
    <div
      className="grid grid-cols-12 gap-4 auto-rows-[220px]"
      style={{ gridAutoFlow: "dense" }}
    >
      {children}
    </div>
  );
}
