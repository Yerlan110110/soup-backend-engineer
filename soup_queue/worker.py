import os
import signal
import sqlite3
import threading
import time
from types import FrameType
from uuid import uuid4

from structlog.stdlib import get_logger

from soup_queue.config import Settings
from soup_queue.db import Database
from soup_queue.leases import Leases
from soup_queue.logging import configure
from soup_queue.models import Claim


def execute(claim: Claim, leases: Leases, stop: threading.Event) -> bool:
    deadline = time.monotonic() + claim.spec.duration_ms / 1000
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return leases.complete(claim)
        if stop.wait(min(remaining, leases.lease_ms / 3000)):
            return False
        if not leases.renew(claim):
            return False


def main() -> None:
    configure()
    log = get_logger()
    settings = Settings()
    leases = Leases(Database(settings.db), settings.lease_ms)
    stop = threading.Event()
    owner = f"{os.getpid()}-{uuid4().hex}"

    def shutdown(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    log.info("worker_started", owner=owner, lease_ms=settings.lease_ms)
    while not stop.is_set():
        try:
            claim = leases.claim(owner)
            if claim is None:
                stop.wait(settings.poll_ms / 1000)
                continue
            log.info("job_claimed", job_id=claim.job_id, generation=claim.generation, owner=owner)
            completed = execute(claim, leases, stop)
            log.info(
                "attempt_finished",
                job_id=claim.job_id,
                generation=claim.generation,
                committed=completed,
            )
        except sqlite3.OperationalError:
            log.exception("worker_storage_unavailable", owner=owner)
            stop.wait(0.2)
    log.info("worker_stopped", owner=owner)


if __name__ == "__main__":
    main()
