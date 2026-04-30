.PHONY: install ingest-hn query test lint clean

# Use uv if available, otherwise fall back to pip in a venv.
PYTHON ?= python3
VENV   ?= .venv

install:
	@if command -v uv >/dev/null 2>&1; then \
		uv venv && uv pip install -e ".[dev]"; \
	else \
		$(PYTHON) -m venv $(VENV) && $(VENV)/bin/pip install -U pip && $(VENV)/bin/pip install -e ".[dev]"; \
	fi
	@echo ""
	@echo "Done. Activate with:  source $(VENV)/bin/activate"

ingest-hn:
	finpulse ingest hackernews --duration 60

query:
	finpulse query

test:
	pytest

lint:
	ruff check src tests

clean:
	rm -rf data/ .pytest_cache .ruff_cache .mypy_cache **/__pycache__
