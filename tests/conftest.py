from dataclasses import dataclass
from pathlib import Path

import pytest

from soup_queue.db import Database
from soup_queue.jobs import Jobs
from soup_queue.leases import Leases
from soup_queue.models import Claim, JobSpec


@dataclass(frozen=True, slots=True)
class Queue:
    db: Database
    jobs: Jobs
    leases: Leases

    def claimed(self, key: str = "example") -> Claim:
        self.jobs.create(key, JobSpec(duration_ms=0))
        claim = self.leases.claim("worker-a")
        assert claim is not None
        return claim


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    db = Database(tmp_path / "test.sqlite")
    db.initialize()
    return Queue(db, Jobs(db), Leases(db, 1000))


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
