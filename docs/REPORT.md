# Evidence report and production verdict

## Verdict: DON'T SHIP at 100 requests/second

The bounded, single-host database-effect contract survived the tested failures.
That is not a production-readiness certificate. The separate 100 requests/s run
was only 60 seconds on one laptop, with 50% creates, short simulated work and no
process faults. There is no authentication, queue admission limit, storage/host
failover, backup/restore exercise or production monitoring.
An external training/upload effect would require a different end-to-end contract.

First steps before production:

1. Exercise database restore and full-host failure on the target storage. The
   Docker Compose smoke test confirms startup and basic restart persistence in CI.
2. Define the actual request mix and completion/queue-age SLOs. Add a bounded queue,
   admission/backpressure and metrics for queue age, reclaims, rejected stale
   completions, lock waits and errors. Run a long soak plus saturation tests with
   realistic job durations and I/O. Scale workers according to measured service
   time; the mixed API benchmark is not 100 newly completed jobs per second.
3. Add authentication/authorization and resource limits before exposure beyond
   loopback. If availability or multiple worker hosts are required, move to a
   shared server database such as Postgres and retest the actual implementation;
   SQLite WAL is deliberately confined to one host.
4. For any external side effect, require a receiver-supported idempotency/fencing
   contract. An outbox alone does not make delivery exactly once.

## Environment and provenance

Recorded on 16 November 2026, macOS 15.7.3 ARM64, CPython 3.13.15, SQLite 3.53.1,
uv 0.12.17. Full dependency versions and hashes are in `uv.lock`. Evidence timestamps
use UTC (Almaty is UTC+5). Each run uses a fresh database, an ephemeral loopback
port and independently spawned API/worker processes. No existing user processes
are selected as victims. Worker replacement uses a new UUID identity.

The design Markdown was written before the first implementation files. Its SHA-256
is `ff47f9c29fa959c0dfd15405ad2b8746753c3b618a56bcb05164b919faac2d21`.
The PDF is a later one-page typeset rendering of that note. The note's phrase
“actual starts” refers to durable claimed attempts: a claim cannot prove that CPU
work began before a crash. The implementation and README make this distinction
explicit. Per-run source manifests identify the files present when each run began;
formatting and additional tests were added during preparation. They are hashes,
not a full historical source checkout.

## All chaos runs

Six chaos harness invocations: two small smoke runs and four 10,000-job runs.
One smoke invocation failed. The first full run passed the original invariants
but failed our subsequent coverage audit. **Three full runs satisfy the strengthened
coverage gate.** No failed output was edited away or replaced by a rerun.

| Raw output | Assessment | SIGKILLs | Reclaims | Succeeded/effects | Failed jobs | Cancelled | Duplicate POSTs | Seconds |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| [seed 40, 300 jobs](../evidence/chaos-20261116T113021Z-s40-35a0ff/events.jsonl) | FAIL | 0 | 0 | - | - | - | 0 | 1.04 |
| [seed 40, 300 jobs](../evidence/chaos-20261116T113216Z-s40-f9b2cc/events.jsonl) | PASS | 8 | 0 | 244 | 8 | 48 | 880 | 4.65 |
| [seed 41, 10,000 jobs](../evidence/chaos-20261116T113253Z-s41-43c0f0/events.jsonl) | PASS invariants; recovery coverage inadequate | 35 | 0 | 8255 | 221 | 1524 | 14417 | 72.82 |
| [seed 42, 10,000 jobs](../evidence/chaos-20261116T113549Z-s42-af6e2f/events.jsonl) | PASS | 34 | 29 | 8204 | 215 | 1581 | 14009 | 70.88 |
| [seed 43, 10,000 jobs](../evidence/chaos-20261116T113832Z-s43-a2ceec/events.jsonl) | PASS | 34 | 32 | 8219 | 229 | 1552 | 14109 | 71.45 |
| [seed 44, 10,000 jobs](../evidence/chaos-20261116T114127Z-s44-ebe546/events.jsonl) | PASS | 34 | 32 | 8162 | 230 | 1608 | 14008 | 70.83 |

“Failed jobs” are requested simulations with `should_fail=true`, not failed test
runs. For every successful run, succeeded + failed + cancelled equals the intended
job count, and effect count equals succeeded. The final snapshot has zero queued
or running jobs. The failed first smoke test created no jobs: its HTTP requests
were rejected before storage.

Seeds 42/43/44 used eight worker processes, 10,000 unique job keys, 16 creation lanes,
two duplicate-request loops, concurrent cancellation and random worker SIGKILL
approximately every two seconds (uniform 1.6–2.4 seconds). They produced **102 kills,
93 reclaims and 42,126 duplicate POSTs** in total. The respective counts of killed
workers actually owning a running job were 28, 30 and 32. The seed fixes victim and
request choices; scheduling still varies. Workers were killed during submission
and drain, not merely once before the workload.

### Failure and coverage findings

**Smoke 1, seed 40:** the workload sent JSON bytes without `Content-Type:
application/json`. The API correctly returned 422. The raw traceback remains in
[the first console output](../evidence/chaos-smoke-console.txt). A minimal replay
changed only the header and changed the result from 422 to 201. The new regression
test failed on the original client and passed after the header fix. See
[the red regression output](../evidence/verification/http422-regression-red.txt)
and [the response comparison](../evidence/verification/http422-reproduction.txt).
The smoke rerun then passed; the service validation was not weakened.

**Full seed 41:** 35 kills looked convincing, but a post-run SQL query found zero
jobs with generation > 1. Cancelling the oldest running job tended to cancel the
abandoned job before lease recovery. The original `PASS` event is retained as-is;
it proves the original invariant checks passed, not that recovery was exercised.
The revised harness protects observed abandoned jobs from cancellation, selects
live cancellation targets differently, and refuses to pass a full run without
actual reclaims and at least five killed owners. Seeds 42/43/44 use this gate.

The harness oracle also has negative tests: removing a job, leaving one running,
deleting a required effect, or changing an effect value must make verification fail.

## Targeted silent-failure checks

| Failure mode | Check and observed result |
|---|---|
| Double claim | Eight simultaneous claim transactions on one queued job using a barrier and separate connections: exactly one claim succeeds. The claim row and attempt row share a transaction. |
| Zombie after expiry | Unit checks reject expired renew/complete and older generations. A separate test reuses the same owner so it cannot pass solely because owner IDs differ. The real-process probe SIGSTOPs worker A, lets B reclaim and complete generation 2, then SIGCONTs A: A logs `committed=false`; exactly one effect remains. |
| Cancel/complete race | Both ordered cases plus 20 simultaneous barrier races. Exactly one outcome wins; cancellation means zero effect, completion means one effect and cancel conflict. Chaos additionally acknowledges cancellations while workers run. |
| Request retry | Concurrent identical creates return one ID; default normalization is checked through HTTP. Conflicting bodies under one key return 409. Every duplicate POST in the chaos run must return the known ID. |
| Isolation | WAL reader retains its snapshot across another connection's commit, and sees the new value after ending its transaction. A second `BEGIN IMMEDIATE` with zero busy timeout cannot enter while the first writer holds the lock. No shared cache or dirty-read mode is enabled. |
| Clock skew | Replacing Python `time.time()` does not alter SQLite lease time. SQL-clock +5 minute then return-to-normal steps do not let a stale generation commit. A -5 minute step delays recovery, explicitly demonstrating a liveness limit. These inject the clock expression; they do not change the OS clock or simulate NTP. |
| Effect/status atomicity | A trigger-induced failure after effect insertion rolls back the effect. Real SIGKILL before the terminal UPDATE leaves zero committed effects, and recovery creates one; SIGKILL immediately after commit leaves the existing single effect intact. |
| Oracle blindness | Deliberately corrupt fixtures cause the independent final-state checker to fail. It is not merely a summary of application return values. |

Raw process evidence: [targeted probe events](../evidence/probes-20261116T113441Z-4e608a/events.jsonl).
The directory also contains each child's output and retained databases. The SIGSTOP
pause is about one lease plus completion time, not a literal five-minute wait;
clock-step tests separately exercise the five-minute timestamp displacement.

The final-state oracle compares the intended key set, row count, original payloads,
acknowledged IDs and cancellation outcomes; checks effect cardinality/value/generation/
owner; checks attempt generations, metadata, foreign keys and database integrity;
and traverses the complete list API with 137-row pages. It does not call the
application's state-transition methods to decide whether the final data is valid.

## Load measurement

One separate open-loop run: **6,000 requests in 59.995 seconds**,
**100.01 requests/s**, four workers, zero unexpected HTTP responses.
Mix: 50% create, 20% get, 20% list, 10% cancel. There were 20 warm-up jobs, 3,000
measured creates and 600 cancellations. All 3,020 jobs drained; 2,420 effects remained.

| Metric | Observed |
|---|---:|
| p50 scheduled-to-response latency | 4.13 ms |
| p95 scheduled-to-response latency | 6.50 ms |
| p99 scheduled-to-response latency | 9.53 ms |
| Maximum client dispatch lag | 6.99 ms |
| Unexpected responses / transport errors | 0 |

Latencies start at the request's scheduled arrival, so client dispatch lag is
included. The client did not wait for each response before scheduling the next.
No worker faults were injected during this load run. Jobs used a 50 ms delay,
not real model training. The measurement is loopback, short duration and mixed
read/write traffic; it cannot be extrapolated to 100 new long-running jobs/s.

[Raw measurement events](../evidence/load-20261116T114238Z-fc93e9/events.jsonl),
[all 6,000 request observations](../evidence/load-20261116T114238Z-fc93e9/requests.jsonl).

## What these tests do not establish

- They do not prove arbitrary schedules, unbounded liveness or absence of every race.
  Repeating three seeds increases evidence, not a probabilistic correctness guarantee.
- The chaos and load campaigns ran as macOS processes. A separate Docker Compose
  smoke test ran on Ubuntu 24.04. That run checks the Docker entry point and basic
  restart persistence. It does not repeat the fault campaign inside containers.
- There is no power-cut, disk-full, corruption/restore, filesystem failure, network
  partition, database-host loss, API-crash or rolling-migration campaign. Returning
  a committed response across a severed client connection is not explicitly fault-
  injected; normal identical POST retries and concurrent retries are exercised.
- The effect lives in the same database transaction as job state. Counting its rows
  cannot detect repeated computation, rolled-back attempts or external non-idempotent
  effects. The unique constraint is part of the implementation, not independent
  evidence that training ran once.
- One wall clock serves all SQLite processes on the same host. Clock steps can
  delay recovery or cause extra attempts; fencing protects stale results after
  reassignment. This is not a multi-host clock-synchronization test.
- The 10,000-job finite run cannot establish long-term fairness, growing-table/WAL
  behaviour, retention cost, production latency tails or hardware capacity.

## Final checks

Final suite: **51 passed**. Ruff lint and format checks passed; basedpyright reported
zero errors/warnings. See [the unedited final check output](../evidence/verification/final-check.txt).
Earlier test/lint/type-check output, including the failing regression, is retained
under `evidence/verification/`. The final suite includes intentional exception tests;
those expected faults are not test failures.

Docker Compose passed a smoke test on Ubuntu 24.04 in GitHub Actions. The
workflow built the image, started the API and four worker containers, submitted
a job and observed `succeeded` with generation 1. It stopped and restarted the
stack with the named volume intact, then replayed the same POST. The API returned
HTTP 200 with the original job ID and `succeeded` state. The workflow and source
used by this run are in commit `ec7518a`.
[Raw CI log](../evidence/verification/compose-smoke-ci.txt).
All source modules remain below 200 non-comment, nonblank lines. Request models
parse untrusted input at the API boundary; SQLite rows are parsed into typed domain
models; the state transition dispatch is exhaustive. Raw SQLite and JSON observations
have narrow documented typing exceptions rather than untyped domain interfaces.
