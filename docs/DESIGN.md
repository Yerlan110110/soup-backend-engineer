# Design note (written before implementation)

## Scope and storage

A single-host training-job queue, with a FastAPI HTTP API and independent worker
processes. SQLite in WAL mode, `synchronous=FULL`, foreign keys enabled, private
connections and short `BEGIN IMMEDIATE` write transactions. SQLite is deliberate:
one inspectable local durable resource makes the job state and the emulated effect
atomic, and permits running exactly the same storage in tests and Docker. It is a
single-writer, single-host choice; it is not a claim of horizontal scalability or HA.
The database, WAL and shared-memory files must share a local persistent volume.

## Claim and recovery

A worker locks the writer before choosing the oldest queued or expired running job.
In that transaction it changes the state to running, increases a monotonically
increasing generation, sets a unique worker identity and a lease deadline, and
records the attempt. The committed generation is the fencing token. Independent
connections cannot both claim the same live generation. All lease comparisons and
lease timestamps come from SQLite's time expression, never from a timestamp sent
by a worker. Every worker can reclaim expired leases; there is no leader/reaper.
A worker renews its lease while emulating execution. A dead or paused worker loses
ownership when its lease expires; a replacement attempt increments the generation.
Progress assumes a live worker, writable storage and eventual sensible host time.

## Effect and completion

Execution is a cancellable delay followed by an emulated effect: inserting one row
into `effects`. Under the writer lock the worker checks state, owner, generation
and an unexpired lease. Only the current owner may insert the effect and transition
the job to succeeded, in the SAME transaction. `effects.job_id` is unique as a
second defence; insert conflicts are errors, never silently hidden. A failed
simulation transitions to failed without an effect. A crash before commit rolls
back both writes; after commit both persist. Attempt history counts actual starts;
effect rows count committed effects. These are different quantities.

## Cancellation and guarantee

Cancel and completion serialize on the same SQLite writer lock. If cancel commits
first, queued/running becomes cancelled, ownership is cleared, and any later
completion is rejected. If completion commits first, cancel returns HTTP 409 and
the terminal result remains unchanged. Repeated cancel of cancelled is HTTP 200.
Cancellation revokes permission to commit, not a promise of immediate CPU preemption.
Workers notice revocation at renewal or completion.

The guarantee is **at-least-once attempts with exactly one committed database effect
for a succeeded job, and zero for failed/cancelled jobs**. This is not exactly-once
execution, and it does not extend to an external file, HTTP call or GPU training
side effect. Such a target needs its own idempotency/fencing contract (or an outbox
plus an idempotent receiver). Reusing an idempotency key with the same normalized
body returns the original job; a different body returns 409. The unique key and
job insertion share the same transaction. Keys are retained with jobs.

## Failure boundaries and evidence plan

Exercise double claims, expired zombie completion/renewal, both cancel/complete
orders and actual concurrent races, request retries, rollback between effect and
state writes, transaction visibility, and clock steps. Then run 10,000 jobs with
8 real processes, random SIGKILL every ~2 seconds, concurrent duplicate POSTs and
cancels, and an independent SQL oracle plus API pagination. Retain every run.
Host clock jumps may hurt liveness (especially backwards); generations protect
against stale commits after reassignment. Disk/host loss, storage lying about
fsync, external effects, unbounded starvation and production capacity remain
outside those proofs. A 100 requests/s verdict requires measured load and explicit
operational limits, not just green functional tests.
