# pyright: reportAny=false
# Raw sqlite test observations are checked directly by assertions.
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from soup_queue.db import INT
from soup_queue.models import JobSpec, State, TerminalConflict
from tests.conftest import Queue


def test_single_claim_when_eight_workers_race(queue: Queue) -> None:
    # Given one queued job and eight independent database connections.
    queue.jobs.create("one", JobSpec())
    barrier = threading.Barrier(8)

    def race(index: int) -> bool:
        barrier.wait()
        return queue.leases.claim(f"worker-{index}") is not None

    # When every worker tries to claim at once.
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(race, range(8)))
    # Then exactly one worker receives the live job.
    assert sum(claims) == 1


def test_stale_completion_when_new_generation_owns_job(queue: Queue) -> None:
    # Given an expired claim which has been reassigned.
    stale = queue.claimed()
    with queue.db.write() as conn:
        conn.execute("UPDATE jobs SET lease_until_ms=0 WHERE id=?", (stale.job_id,))
    current = queue.leases.claim("worker-b")
    assert current is not None
    # When the zombie tries to finish.
    accepted = queue.leases.complete(stale)
    # Then its result is rejected and the current owner can finish exactly once.
    assert not accepted
    assert queue.leases.complete(current)
    assert not queue.leases.complete(current)
    assert queue.jobs.get(stale.job_id).generation == 2


def test_renewal_rejected_when_lease_already_expired(queue: Queue) -> None:
    # Given an expired claim, even before another worker reclaims it.
    claim = queue.claimed()
    with queue.db.write() as conn:
        conn.execute("UPDATE jobs SET lease_until_ms=0 WHERE id=?", (claim.job_id,))
    # When its original owner tries to renew or complete.
    renewed, completed = queue.leases.renew(claim), queue.leases.complete(claim)
    # Then expiration itself revokes permission.
    assert (renewed, completed) == (False, False)


def test_effect_rollback_when_terminal_update_fails(queue: Queue) -> None:
    # Given a fault exactly between the effect INSERT and final-state UPDATE.
    claim = queue.claimed()
    with queue.db.write() as conn:
        conn.execute(
            "CREATE TRIGGER fail_completion BEFORE UPDATE OF state ON jobs "
            "WHEN NEW.state='succeeded' BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    # When completion hits that fault.
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        queue.leases.complete(claim)
    # Then the inserted effect rolls back with the state change.
    with queue.db.connection() as conn:
        assert INT.validate_python(conn.execute("SELECT count(*) FROM effects").fetchone()[0]) == 0
    assert queue.jobs.get(claim.job_id).state == State.RUNNING


def test_completion_rejected_when_cancel_commits_first(queue: Queue) -> None:
    # Given a running job whose cancellation has committed.
    claim = queue.claimed()
    queue.jobs.cancel(claim.job_id)
    # When the old worker tries to commit.
    accepted = queue.leases.complete(claim)
    # Then cancellation stays terminal.
    assert not accepted
    assert queue.jobs.get(claim.job_id).state == State.CANCELLED


def test_cancel_conflict_when_completion_commits_first(queue: Queue) -> None:
    # Given an already committed result.
    claim = queue.claimed()
    queue.leases.complete(claim)
    # When cancellation arrives later.
    with pytest.raises(TerminalConflict):
        queue.jobs.cancel(claim.job_id)
    # Then success remains immutable.
    assert queue.jobs.get(claim.job_id).state == State.SUCCEEDED


@pytest.mark.parametrize("iteration", range(20))
def test_consistent_effect_when_cancel_and_complete_race(queue: Queue, iteration: int) -> None:
    # Given competing operations on distinct connections, released at the same barrier.
    claim = queue.claimed(str(iteration))
    barrier = threading.Barrier(2)

    def cancel() -> bool:
        barrier.wait()
        try:
            queue.jobs.cancel(claim.job_id)
        except TerminalConflict:
            return False
        return True

    def complete() -> bool:
        barrier.wait()
        return queue.leases.complete(claim)

    # When the two transactions race.
    with ThreadPoolExecutor(max_workers=2) as pool:
        cancelled = pool.submit(cancel)
        completed = pool.submit(complete)
        outcomes = (cancelled.result(), completed.result())
    # Then only one wins, with the matching observable state and effect count.
    assert outcomes in {(True, False), (False, True)}
    with queue.db.connection() as conn:
        effects = INT.validate_python(conn.execute("SELECT count(*) FROM effects").fetchone()[0])
    assert effects == int(outcomes[1])
    expected = State.CANCELLED if outcomes[0] else State.SUCCEEDED
    assert queue.jobs.get(claim.job_id).state == expected


def test_generation_fence_when_owner_identity_is_reused(queue: Queue) -> None:
    # Given two generations with the same owner identity (isolate the token check).
    stale = queue.claimed()
    with queue.db.write() as conn:
        conn.execute("UPDATE jobs SET lease_until_ms=0 WHERE id=?", (stale.job_id,))
    current = queue.leases.claim(stale.owner)
    assert current is not None
    # When the older token tries to commit despite matching the current owner.
    accepted = queue.leases.complete(stale)
    # Then the stale generation is rejected and the new generation still completes.
    assert not accepted
    assert queue.leases.complete(current)
