# Soup training-job queue

A small, single-host queue whose correctness claim is deliberately narrow:
**at-least-once attempts; exactly one committed SQLite effect for each succeeded
job; zero effects for failed or cancelled jobs.** It does not promise exactly-once
CPU execution or exactly-once effects in an external service.

**Production verdict at 100 requests/second: DON'T SHIP.** The implementation has
real failure evidence, but host/storage failure recovery, production capacity,
and operational controls still need validation.
See [the evidence report](docs/REPORT.md) for measurements and concrete next steps.

## Start

Docker Engine/Desktop with Docker Compose v2:

```sh
docker compose up
```

This builds the image, starts the API and **four independent worker containers**,
and persists SQLite in the `queue-data` named volume. API: <http://localhost:8000>;
OpenAPI UI: <http://localhost:8000/docs>. The host port binds to loopback.

```sh
# Optional: eight workers
docker compose up --scale worker=8
# Stop while preserving jobs
docker compose down
```

The Docker Compose smoke test passed on Ubuntu 24.04 in GitHub Actions.
It built and started the API and four workers, completed a job, then stopped
and restarted the stack. Replaying the same request returned the same completed
job. See [the CI log](evidence/verification/compose-smoke-ci.txt). The larger
chaos and load campaigns ran as native macOS processes. Keep the SQLite WAL
database on a local volume shared by processes on the same host.

Local alternative (Python 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/)):

```sh
uv sync --frozen
uv run uvicorn soup_queue.api:create_app --factory --host 127.0.0.1 --port 8000
# In four separate terminals, after the API has initialized the database:
uv run python -m soup_queue.worker
```

Configuration: `QUEUE_DB` (default `data/queue.sqlite`), `QUEUE_LEASE_MS` (3000),
`QUEUE_POLL_MS` (40), `LOG_LEVEL` (INFO). Workers must use the same database path;
API and worker clocks belong to the same host. SIGTERM stops a worker at its next
checkpoint; its uncompleted lease can subsequently be reclaimed.

## HTTP contract

```sh
curl -i -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: experiment-001' \
  -d '{"duration_ms":500,"should_fail":false,"value":"model-artifact"}'

curl http://localhost:8000/jobs/1
curl 'http://localhost:8000/jobs?limit=20&cursor=0'
curl -i -X POST http://localhost:8000/jobs/1/cancel
```

| Operation | Success | Other outcomes |
|---|---|---|
| `POST /jobs`, `Idempotency-Key` header | 201 new; 200 replay with current state | 409 same key/different normalized body; 422 invalid input |
| `GET /jobs/{id}` | 200 job | 404 unknown ID |
| `GET /jobs?cursor=0&limit=100` | 200 `{items,next_cursor}` | 422 invalid cursor/limit |
| `POST /jobs/{id}/cancel` | 200 cancelled (also on retry) | 409 succeeded/failed already; 404 unknown ID |
| `GET /healthz` | 200 database readable | 503 storage unavailable |

Bodies reject unknown fields and type coercion. `duration_ms` is 0..300000,
`should_fail` defaults to false, `value` is at most 1024 characters. Keys accept
1..128 ASCII letters, digits, `.`, `_`, `:`, `-`. Omitted defaults and explicitly
supplied defaults are equivalent. Field order does not affect identity. Keys have
no expiration or cleanup policy; they live as long as jobs.

Job responses expose the original normalized JSON in `payload`, the current
`state`, `generation` (claim count), owner/lease information and millisecond UTC
timestamps. IDs are increasing but may have gaps. Pagination is by `id > cursor`,
with limits 1..500. It is stable for existing rows; it is **not** a snapshot across
requests, and concurrently added jobs may appear in later pages.

Cancel and complete serialize on the writer lock. A successful cancellation
prevents any later effect commit; a prior successful completion makes cancellation
return 409. Running cancellation does not instantly terminate arbitrary external
computation. `should_fail=true` is an intentional simulated training failure, not
an infrastructure-test failure. Failed jobs do not retry; expired attempts do.

## Verify

Run from the repository root. The process harnesses require POSIX signals
(macOS/Linux), create isolated databases/ports and terminate only their own children.

```sh
uv run ruff check .
uv run ruff format --check .
uv run basedpyright
uv run pytest -q

# Targeted SIGKILL-at-commit and resumed-zombie probes
uv run python -m harness.probes

# Required workload: 10,000 jobs, 8 workers, a random SIGKILL about every 2 seconds
uv run python -m harness.chaos --seed 42
uv run python -m harness.chaos --seed 43
uv run python -m harness.chaos --seed 44

# Separate open-loop measurement, default four workers
uv run python -m harness.load --rps 100 --seconds 60
```

Each harness invocation creates a new directory under `evidence/`. It contains raw
JSONL events, process stdout/stderr, the database, runtime versions and SHA-256
hashes of the sources present at run start. Never reuse or delete a failed run to
improve the story. The supplied console files also preserve exception tracebacks.
Seeds reproduce request patterns and victim selection, **not** OS scheduling.

## Read the submission

- [One-page design PDF](docs/DESIGN.pdf) / [original Markdown](docs/DESIGN.md): claim, leases, cancellation, guarantee and scope.
- [Evidence and verdict](docs/REPORT.md): every run, failure analysis, silent failures,
  capacity measurement and limits.
- [Reflection](docs/REFLECTION.md): the surprising result and remaining concern.
- [AI use](docs/AI_DISCLOSURE.md): assistance, accepted choices, corrections and verification.
- `soup_queue/`: API, storage transactions, schema and worker.
- `harness/`: real HTTP traffic, process lifecycle, fault injection and independent SQL oracle.
- `tests/`: races, fencing, rollback, isolation, clocks, API and oracle-negative tests.

A claim row in `attempts` means a durable claim was made; a process might die before
performing any simulated work. An `effects` row is the committed observable output.
Do not infer exactly-once execution by counting effects alone.
