# PY defaults to whatever `python` is on PATH. On Windows we run a 3.11 venv:
#   make dev PY=./.venv/Scripts/python.exe
PY ?= python
export PYTHONPATH := src

.PHONY: dev fake test bench config

# Run the real pipeline: generator -> classifier -> queue -> workers -> sink,
# FastAPI serving /health, /control/rate and /ws on :8000.
dev:
	$(PY) -m triage.app

# Run the app in --fake mode: no engine, /ws streams triage.fake_metrics.
# Lets the dashboard be built before the engine exists (Stage A onward).
fake:
	$(PY) -m triage.app --fake

# Print the tier table and re-check the three calibration invariants
config:
	$(PY) -m triage.config

# Invariant + contract tests
test:
	$(PY) -m pytest -q

# Headless benchmark: adaptive vs naive (Lane D, Stage G)
bench:
	@echo "bench: not implemented yet"
