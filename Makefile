# PY defaults to whatever `python` is on PATH. On Windows we run a 3.11 venv:
#   make fake PY=./.venv/Scripts/python.exe
PY ?= python
export PYTHONPATH := src

.PHONY: dev fake test bench config

# Run the pipeline + dashboard (Lane D, Stage B)
dev:
	@echo "dev: not implemented yet"

# Emit plausible MetricsFrames at 4 Hz so the dashboard can be built before
# the engine exists (Lane D, Stage A)
fake:
	$(PY) -m triage.fake_metrics

# Print the tier table and re-check the three calibration invariants
config:
	$(PY) -m triage.config

# Invariant + contract tests
test:
	$(PY) -m pytest -q

# Headless benchmark: adaptive vs naive (Lane D, Stage F)
bench:
	@echo "bench: not implemented yet"
