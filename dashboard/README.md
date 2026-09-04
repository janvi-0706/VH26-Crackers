# PULSE dashboard

Vite + React + TypeScript + Tailwind + Recharts, dark theme. Connects to
`ws://localhost:8000/ws` and renders `MetricsFrame` — see
`src/types/metrics.ts`, hand-kept in sync with `src/triage/contracts.py`.

## Layout system

`src/components/Panel.tsx` defines the grid every panel drops into: a
12-column CSS grid with fixed-height rows (`PanelGrid`), and a `Panel`
wrapper whose `size` prop (`sm | md | lg | wide | tall | full`) claims a
fixed span. Because a panel only ever claims its own span, adding the 11th
panel cannot reflow the 10 already on screen.

## Panels (this stage)

- `ThroughputPanel` — events/sec, rolling 60s window
- `LatencyByTierPanel` — p99 latency, one line per tier (P0/P1/P2)
- `P0ScoreboardPanel` — large P0 p99 vs the 200ms target, green/red

## Connection handling

`src/hooks/useMetricsSocket.ts` owns the one WebSocket, exposes
`status: "connecting" | "open" | "reconnecting" | "closed"`, and reconnects
on any drop with capped exponential backoff (0.5s → 8s). `App.tsx` renders
that status via `ConnectionIndicator` in the header at all times.

## Running it

Two ways to develop against the backend:

**Vite dev server** (hot reload, port 5173), against a backend already
running on 8000:

```bash
cd dashboard
npm install
npm run dev
# separately: make dev   (or) make fake     — from Code/
```

**Built and served by FastAPI** (what `make dev` serves in production/demo):

```bash
cd dashboard
npm install
npm run build          # writes dashboard/dist
cd ..
make dev PY=./.venv/Scripts/python.exe    # mounts dashboard/dist at "/"
```

`src/triage/app.py` mounts `dashboard/dist` as static (with SPA fallback)
when it exists, and otherwise serves a small JSON notice at `/` instead of
failing — that fallback is what you will see until the first `npm run build`
has been run.

## Known gap: not built or run in this session

This machine has no Node.js/npm installed, so `npm install`, `npm run build`,
and `npm run dev` could not be executed here — the acceptance line ("`make
dev` starts one process; charts move at 1000 events/min; `POST
/control/rate 20000` makes latency visibly climb") has **not** been visually
verified. What was checked without a JS runtime: `package.json` and both
`tsconfig*.json` parse as valid JSON; every `.ts`/`.tsx` file has balanced
brackets/braces; import paths and exported names were cross-checked by hand
across all files; `MetricsFrame`'s TypeScript shape was checked field-by-field
against `contracts.py`. The backend side of the acceptance line (rate control,
throughput, latency climbing under load) is already covered by
`tests/test_engine.py` and `tests/test_app.py`.

Once Node is available: run the "Built and served by FastAPI" steps above,
then `make dev`, open `http://localhost:8000/`, and drive the rate up with
`curl -X POST localhost:8000/control/rate -d '{"rate": 20000}' -H 'Content-Type: application/json'`
to see the P0 scoreboard and latency chart react.
