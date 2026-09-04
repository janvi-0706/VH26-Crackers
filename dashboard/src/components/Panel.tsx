import type { ReactNode } from "react";

/**
 * Stage H's own "final layout" pass: everything has to fit one 1920x1080
 * screen with no page scroll, at the same time as two more panels arrive.
 * The old system (a `size` enum picking a column span, `auto-rows-[220px]`,
 * `gridAutoFlow: dense`, letting however many rows the content happened to
 * need stack up) could not guarantee that — "however many rows fit" was
 * never actually checked against "however many rows the viewport has".
 *
 * The fix: `PanelGrid` now claims the *entire* remaining viewport height
 * (its parent is `flex-1 min-h-0` inside an `h-screen overflow-hidden`
 * page — see App.tsx) and divides it into a FIXED number of rows via
 * `grid-rows-N`, each an equal `1fr` share of whatever height is actually
 * left after the header and control bar. That is what makes "fits without
 * scrolling" true by construction, on any real screen size, rather than a
 * fixed pixel guess that happens to work at exactly one resolution.
 * `Panel` itself now takes a plain 1-12 column count instead of a named
 * size — with a deliberately fixed row count, the interesting layout
 * decision is only ever "how wide", never "how tall".
 */
const COL_SPAN: Record<number, string> = {
  1: "col-span-1", 2: "col-span-2", 3: "col-span-3", 4: "col-span-4",
  5: "col-span-5", 6: "col-span-6", 7: "col-span-7", 8: "col-span-8",
  9: "col-span-9", 10: "col-span-10", 11: "col-span-11", 12: "col-span-12",
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
  /** 1-12: how many of the grid's 12 columns this panel claims this row.
   * Row height is never chosen per-panel — see this module's own
   * docstring on why that is now PanelGrid's job alone. */
  cols?: number;
  accent?: PanelAccent;
  /** Small right-aligned status text next to the title, e.g. a live value. */
  headline?: string;
  footer?: ReactNode;
  children: ReactNode;
}

export function Panel({
  title,
  cols = 4,
  accent = "neutral",
  headline,
  footer,
  children,
}: PanelProps) {
  return (
    <section
      className={`${COL_SPAN[cols]} flex flex-col rounded-xl border bg-surface-panel
        ${ACCENT_BORDER[accent]} shadow-sm shadow-black/20 overflow-hidden min-h-0`}
    >
      <header className="flex shrink-0 items-baseline justify-between gap-3 border-b border-surface-border px-3 py-1.5">
        <h2 className="text-[11px] font-semibold uppercase tracking-wide text-ink-muted">
          {title}
        </h2>
        {headline && (
          <span className="font-mono text-xs text-ink-muted tabular-nums">{headline}</span>
        )}
      </header>
      {/* overflow-hidden here (not just min-h-0) is load-bearing: Stage I
          grew the grid from 4 rows to 6, shrinking every row's own height,
          and a panel whose content was sized for the taller 4-row era
          (ConservationPanel's own text-8xl icon was the real case found
          live) would otherwise visibly spill past this panel's own
          border into whatever sits above it, instead of clipping. */}
      <div className="min-h-0 flex-1 overflow-hidden p-2">{children}</div>
      {footer && (
        <footer className="shrink-0 border-t border-surface-border px-3 py-1 text-[10px] text-ink-faint">
          {footer}
        </footer>
      )}
    </section>
  );
}

/** Fills whatever height its parent gives it (see App.tsx: a flex-1
 * min-h-0 child of an h-screen overflow-hidden page) and divides that
 * height into exactly `rows` equal tracks. Panels are declared in the
 * exact row-major order they should render in; as long as each row's
 * `cols` values sum to 12, plain (non-dense) grid auto-flow places them
 * exactly where intended — no `gridAutoFlow: dense` repacking needed once
 * the layout is planned up front instead of left emergent. */
export function PanelGrid({ children, rows = 4 }: { children: ReactNode; rows?: number }) {
  return (
    <div
      className="grid h-full grid-cols-12 gap-3"
      style={{ gridTemplateRows: `repeat(${rows}, minmax(0, 1fr))` }}
    >
      {children}
    </div>
  );
}
