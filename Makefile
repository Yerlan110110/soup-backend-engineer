.PHONY: check chaos probes load
check:
	uv run ruff check .
	uv run ruff format --check .
	uv run basedpyright
	uv run pytest -q
chaos:
	uv run python -m harness.chaos
probes:
	uv run python -m harness.probes
load:
	uv run python -m harness.load
