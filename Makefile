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

.PHONY: dev fake test bench config

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

# Print the tier table and re-check the three calibration invariants
config:
	$(PY) -m triage.config

# Invariant + contract tests
test:
	$(PY) -m pytest -q

# Headless benchmark: adaptive vs naive (Lane D, Stage G)
bench:
	@echo "bench: not implemented yet"
