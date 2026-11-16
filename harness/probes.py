# pyright: reportAny=false
# sqlite3 observations in these process probes are asserted immediately.
import os
import signal
import sqlite3
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import anyio

from harness.cluster import Cluster
from harness.evidence import Evidence, source_manifest
from harness.oracle import snapshot
from harness.traffic import HarnessViolation
from soup_queue.db import Database
from soup_queue.jobs import Jobs
from soup_queue.leases import Leases
from soup_queue.models import JobSpec


class BoundaryDatabase(Database):
    """Test-only pause after the real effect INSERT, before the terminal UPDATE."""

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection]:
        with super().connection() as conn:

            def pause_at_update(sql: str) -> None:
                if sql.startswith("UPDATE jobs SET state='succeeded'"):
                    Path(os.environ["PROBE_READY"]).write_text("effect inserted, transaction open\n")
                    signal.pause()

            conn.set_trace_callback(pause_at_update)
            yield conn


def child() -> None:
    path = Path(os.environ["QUEUE_DB"])
    db = BoundaryDatabase(path) if os.environ["PROBE_PHASE"] == "before_commit" else Database(path)
    leases = Leases(db, 1000)
    claim = leases.claim("boundary-child")
    if claim is None or not leases.complete(claim):
        raise HarnessViolation("Boundary child did not complete its attempt")
    Path(os.environ["PROBE_READY"]).write_text("completion committed\n")
    signal.pause()


async def crash_boundary(directory: Path, phase: str, evidence: Evidence) -> None:
    db = Database(directory / f"{phase}.sqlite")
    db.initialize()
    jobs = Jobs(db)
    job = jobs.create("boundary", JobSpec(duration_ms=0)).job
    ready = directory / f"{phase}.ready"
    env = os.environ | {"QUEUE_DB": str(db.path), "PROBE_PHASE": phase, "PROBE_READY": str(ready)}
    with (directory / f"{phase}.log").open("wb") as log:
        proc = subprocess.Popen(  # noqa: ASYNC220 - Only test startup; requires precise process ownership.
            [sys.executable, "-m", "harness.probes", "child"], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            with anyio.fail_after(10):
                while not ready.exists():
                    if proc.poll() is not None:
                        raise HarnessViolation(f"Boundary child exited: {proc.returncode}")
                    await anyio.sleep(0.01)
            proc.kill()
            proc.wait(timeout=5)
            state = snapshot(db.path)
            before = phase == "before_commit"
            if state.effects != (0 if before else 1) or state.active != int(before):
                raise HarnessViolation(f"Atomicity failed at {phase}: {state}")
            if before:
                leases = Leases(db, 1000)
                with anyio.fail_after(5):
                    claim = leases.claim("recovery")
                    while claim is None:
                        await anyio.sleep(0.02)
                        claim = leases.claim("recovery")
                if not leases.complete(claim):
                    raise HarnessViolation("Reclaimed attempt could not finish")
            if snapshot(db.path).effects != 1 or jobs.get(job.id).state != "succeeded":
                raise HarnessViolation("Recovered job has inconsistent terminal result")
            evidence.emit(
                "boundary_PASS",
                phase=phase,
                signal="SIGKILL",
                returncode=proc.returncode,
                effects_before_recovery=state.effects,
                final_effects=1,
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


async def zombie(directory: Path, evidence: Evidence) -> None:
    async with Cluster(directory, 0) as cluster:
        jobs = Jobs(Database(cluster.db_path))
        job = jobs.create("zombie", JobSpec(duration_ms=1600)).job
        old = cluster.start_worker()
        with anyio.fail_after(10):
            while jobs.get(job.id).generation == 0:  # noqa: ASYNC110 - Observing an independent OS process.
                await anyio.sleep(0.01)
        old.send_signal(signal.SIGSTOP)
        evidence.emit("SIGSTOP", pid=old.pid, job_id=job.id)
        current = cluster.start_worker()
        with anyio.fail_after(10):
            while jobs.get(job.id).state != "succeeded":  # noqa: ASYNC110 - Observing an independent OS process.
                await anyio.sleep(0.02)
        old.send_signal(signal.SIGCONT)
        evidence.emit("SIGCONT", pid=old.pid, replacement_pid=current.pid)
        with anyio.fail_after(10):
            while '"committed": false' not in (directory / "worker-001.log").read_text():  # noqa: ASYNC110 - Cross-process log acknowledgement.
                await anyio.sleep(0.02)
        final = jobs.get(job.id)
        if final.generation != 2 or snapshot(cluster.db_path).effects != 1:
            raise HarnessViolation("Zombie changed the replacement's result")
        evidence.emit("zombie_PASS", generation=final.generation, final_effects=1, old_attempt_committed=False)


async def run() -> None:
    directory = (
        await anyio.to_thread.run_sync(Path("evidence").resolve)
        / f"probes-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:6]}"
    )
    directory.mkdir(parents=True)
    source_manifest(directory)
    with (directory / "events.jsonl").open("x") as stream:
        evidence = Evidence(stream)
        evidence.emit("start", suite="atomic commit boundaries and resumed zombie process")
        try:
            await crash_boundary(directory, "before_commit", evidence)
            await crash_boundary(directory, "after_commit", evidence)
            await zombie(directory, evidence)
        except BaseException as exc:
            evidence.emit("FAIL", exception=type(exc).__name__, error=str(exc))
            raise
        evidence.emit("PASS", probes=3)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "child":
        child()
    else:
        anyio.run(run)
