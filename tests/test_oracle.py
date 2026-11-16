from typing import Literal, assert_never

import pytest

from harness.oracle import spec_for, verify
from tests.conftest import Queue


@pytest.mark.parametrize("corruption", ["lost", "stuck", "missing_effect", "wrong_effect"])
def test_oracle_rejects_when_invariant_is_broken(
    queue: Queue, corruption: Literal["lost", "stuck", "missing_effect", "wrong_effect"]
) -> None:
    # Given a completed, valid fixture and an independently injected corruption.
    job = queue.jobs.create("job-0", spec_for(0)).job
    claim = queue.leases.claim("test")
    assert claim is not None
    queue.leases.complete(claim)
    # Job 0 intentionally fails; create a successful job to exercise effect corruption.
    second = queue.jobs.create("job-1", spec_for(1)).job
    successful = queue.leases.claim("test")
    assert successful is not None
    queue.leases.complete(successful)
    with queue.db.write() as conn:
        match corruption:
            case "lost":
                conn.execute("DELETE FROM attempts WHERE job_id=?", (job.id,))
                conn.execute("DELETE FROM jobs WHERE id=?", (job.id,))
            case "stuck":
                conn.execute(
                    "UPDATE jobs SET state='running', owner='stuck', lease_until_ms=0, "
                    "finished_ms=NULL, error=NULL WHERE id=?",
                    (job.id,),
                )
            case "missing_effect":
                conn.execute("DELETE FROM effects WHERE job_id=?", (second.id,))
            case "wrong_effect":
                conn.execute("UPDATE effects SET value='corrupted' WHERE job_id=?", (second.id,))
            case unreachable:
                assert_never(unreachable)
    # When the final-state oracle evaluates the database.
    failures = verify(queue.db.path, 2, set())
    # Then it fails, rather than declaring an incomplete or inconsistent run green.
    assert failures


def test_oracle_accepts_when_expected_terminal_states_are_consistent(queue: Queue) -> None:
    # Given one intentionally failed job with zero effects.
    queue.jobs.create("job-0", spec_for(0))
    claim = queue.leases.claim("test")
    assert claim is not None
    queue.leases.complete(claim)
    # When the independent oracle evaluates it.
    failures = verify(queue.db.path, 1, set())
    # Then simulated training failure is not confused with infrastructure loss.
    assert failures == []
