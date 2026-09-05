# PY auto-detects the project's own .venv (Windows or POSIX layout) so
# `make dev` works out of the box regardless of what `python` resolves to
# on whoever's PATH — the machine this was first built on has a Python 3.13
# Anaconda install as the default `python`, with none of our dependencies,
# which silently breaks `make dev` for anyone who hasn't been told to pass
# PY= explicitly. Override only if you really want a different interpreter:
#   make dev PY=/usr/bin/python3.11
VENV_PY_WINDOWS := .venv/Scripts/python.exe
VENV_PY_POSIX   := .venv/bin/python
ifneq (,$(wildcard $(VENV_PY_WINDOWS)))
  PY ?= $(VENV_PY_WINDOWS)
else ifneq (,$(wildcard $(VENV_PY_POSIX)))
  PY ?= $(VENV_PY_POSIX)
else
  PY ?= python
endif

export PYTHONPATH := src

.PHONY: dev fake test bench config dev-http dev-split server1 server2

# Run the real pipeline: generator -> classifier -> queue -> workers -> sink,
# FastAPI serving /health, /control/*, and /ws on :8000.
dev:
	@echo "Using interpreter: $(PY)"
	$(PY) -m triage.app

# Run the app in --fake mode: no engine, /ws streams triage.fake_metrics.
# Lets the dashboard be built before the engine exists (Stage A onward).
fake:
	@echo "Using interpreter: $(PY)"
	$(PY) -m triage.app --fake

# Phase J3: ingress with real HTTP transport (dispatch/ack/redispatch,
# metrics-fragment reporting) instead of `dev`'s own in-process loopback.
# Run alongside `make server1` and `make server2` in two other terminals —
# `dev-http` alone still processes every event itself locally (see
# app.py's own top docstring: this phase does not yet reroute Engine's
# pipeline through transport.py), so the three together are what actually
# demonstrates the split's wire protocol end to end.
dev-http:
	@echo "Using interpreter: $(PY)"
	$(PY) -m triage.app --transport http

# Phase J5: the real three-process split, all in one command — ingress
# (--transport http, :8000), server1 (the dedicated, hardened P0-only
# process, :8001), and server2 (the dedicated P1/P2 process, :8002, with
# its own decision engine, pressure, ladder, CoDel, and micro-batching).
# `make dev` above is untouched and keeps working exactly as before — this
# is the split ALONGSIDE that fallback, not a replacement for it.
#
# One shell (the backslash line continuations matter: `trap`/`wait` must
# see the same job table), ingress started first and given a moment to
# bind its port before server1/server2's own /readyz starts polling it,
# and `trap 'kill 0' ...` so Ctrl+C tears down all three instead of
# leaving orphaned background processes — the same problem a real
# Kubernetes deployment hands to the orchestrator instead, which a local
# `make` target has to do by hand.
dev-split:
	@echo "Using interpreter: $(PY)"
	@echo "ingress (:8000, --transport http) + server1 (:8001) + server2 (:8002)"
	@trap 'kill 0' EXIT INT TERM; \
	$(PY) -m triage.app --transport http & \
	sleep 1; \
	$(PY) -m triage.server1 & \
	$(PY) -m triage.server2 & \
	wait

# The two downstream servers config/servers.yaml names — real, runnable
# FastAPI apps, each deriving its own worker count and per-worker rate
# from its own declared capacity (servers_config.py). server1 (Phase J4)
# is P0 only, no batching/CoDel/ladder/deferral, EDF ordering, /drain,
# /readyz gated on ingress. server2 (Phase J5) is P1/P2, with its own
# decision engine, pressure, ladder, CoDel, and micro-batching — stateless
# and horizontally scalable, so Kubernetes can run one to three of these.
server1:
	@echo "Using interpreter: $(PY)"
	$(PY) -m triage.server1

server2:
	@echo "Using interpreter: $(PY)"
	$(PY) -m triage.server2

# Print the tier table and re-check the three calibration invariants
config:
	$(PY) -m triage.config

# Invariant + contract tests
test:
	$(PY) -m pytest -q

# Headless benchmark: four configs (naive/adaptive x baseline/spike, 90s
# each) plus a 5x/10x/20x/40x sensitivity sweep. ~10.5 real minutes.
# Writes bench/report.md and bench/report.html. PYTHONPATH=src is already
# exported above, but bench/run.py also inserts src/ itself so `python
# bench/run.py` works standalone, outside make, too.
bench:
	@echo "Using interpreter: $(PY)"
	$(PY) bench/run.py
