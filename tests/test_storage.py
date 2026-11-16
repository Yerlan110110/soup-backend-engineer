# pyright: reportAny=false
# Raw sqlite test observations are checked directly by assertions.
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from soup_queue import db as database_module
from soup_queue.db import INT, NOW_MS, now_ms
from soup_queue.models import JobSpec, KeyConflict, State
from tests.conftest import Queue


def test_one_job_when_identical_requests_race(queue: Queue) -> None:
    # Given eight simultaneous requests for the same key.
    barrier = threading.Barrier(8)

    def create(_index: int) -> int:
        barrier.wait()
        return queue.jobs.create("retry", JobSpec()).job.id

    # When all requests commit.
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(create, range(8)))
    # Then every response identifies the same durable job.
    assert len(set(ids)) == 1
    assert len(queue.jobs.list(0, 100).items) == 1


def test_conflict_when_key_reused_for_another_body(queue: Queue) -> None:
    # Given an accepted idempotency key.
    original = queue.jobs.create("key", JobSpec(value="original"))
    # When a different body reuses it.
    with pytest.raises(KeyConflict):
        queue.jobs.create("key", JobSpec(value="changed"))
    # Then the original request is untouched.
    assert queue.jobs.get(original.job.id).payload == original.job.payload


def test_no_effect_when_training_fails(queue: Queue) -> None:
    # Given a job which deliberately fails training.
    job = queue.jobs.create("failure", JobSpec(should_fail=True)).job
    claim = queue.leases.claim("worker")
    assert claim is not None
    # When the attempt completes.
    assert queue.leases.complete(claim)
    # Then failure is terminal and has no successful side effect.
    assert queue.jobs.get(job.id).state == State.FAILED
    with queue.db.connection() as conn:
        assert INT.validate_python(conn.execute("SELECT count(*) FROM effects").fetchone()[0]) == 0


def test_snapshot_isolation_when_writer_commits(queue: Queue) -> None:
    # Given a reader holding a snapshot and a separate writer.
    job = queue.jobs.create("isolation", JobSpec()).job
    with queue.db.connection() as reader, queue.db.write() as writer:
        reader.execute("BEGIN")
        before = reader.execute("SELECT payload FROM jobs WHERE id=?", (job.id,)).fetchone()[0]
        writer.execute("UPDATE jobs SET payload='changed' WHERE id=?", (job.id,))
        # When the writer commits while the reader's transaction remains open.
        writer.commit()
        during = reader.execute("SELECT payload FROM jobs WHERE id=?", (job.id,)).fetchone()[0]
        reader.rollback()
        after = reader.execute("SELECT payload FROM jobs WHERE id=?", (job.id,)).fetchone()[0]
    # Then uncommitted changes and new commits do not leak into the old snapshot.
    assert before == during == job.payload
    assert after == "changed"


def test_second_writer_waits_when_immediate_transaction_owns_lock(queue: Queue) -> None:
    # Given a writer lock held by the first connection.
    with queue.db.write(), queue.db.connection() as contender:
        contender.execute("PRAGMA busy_timeout=0")
        # When another connection attempts a write transaction.
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            contender.execute("BEGIN IMMEDIATE")
        # Then it never entered a transaction based on an unlocked stale read.
        assert not contender.in_transaction


def test_python_clock_skew_ignored_when_claiming(queue: Queue, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a wildly wrong application wall clock.
    monkeypatch.setattr(time, "time", lambda: 1.0)
    queue.jobs.create("clock", JobSpec())
    # When a lease is created.
    claim = queue.leases.claim("worker")
    assert claim is not None
    job = queue.jobs.get(claim.job_id)
    # Then the lease is tied to SQLite time instead of time.time().
    with queue.db.connection() as conn:
        now = now_ms(conn)
    assert job.lease_until_ms is not None
    assert now < job.lease_until_ms <= now + 1000


def test_fence_survives_when_database_clock_jumps_forward_then_back(
    queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a live old claim, and a simulated +5 minute database clock step.
    stale = queue.claimed()
    monkeypatch.setattr(database_module, "NOW_MS", f"({NOW_MS}) + 300000")
    current = queue.leases.claim("replacement")
    assert current is not None
    monkeypatch.setattr(database_module, "NOW_MS", NOW_MS)
    # When clock time returns and the old worker tries to finish with its stale generation.
    accepted = queue.leases.complete(stale)
    # Then the generation fence still rejects it.
    assert not accepted
    assert queue.leases.complete(current)


def test_recovery_delayed_when_database_clock_jumps_back(queue: Queue, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a deadline made before a -5 minute host time step.
    queue.claimed()
    monkeypatch.setattr(database_module, "NOW_MS", f"({NOW_MS}) - 300000")
    # When another worker checks for expiration using the stepped clock.
    replacement = queue.leases.claim("replacement")
    # Then no early reassignment occurs: this documents a known liveness limit.
    assert replacement is None
