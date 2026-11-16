import json
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import anyio
import typer

from harness.client import client
from harness.cluster import Cluster
from harness.evidence import Evidence, source_manifest
from harness.oracle import owned_job, snapshot, verify
from harness.traffic import HarnessViolation, Traffic
from soup_queue.models import JobId, Page


@dataclass(frozen=True, slots=True)
class ChaosConfig:
    jobs: int
    workers: int
    seed: int
    timeout: float
    kill_interval: float
    output: Path


async def run(config: ChaosConfig) -> None:
    directory = config.output.resolve() / f"chaos-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-s{config.seed}-{uuid4().hex[:6]}"
    directory.mkdir(parents=True)
    source_manifest(directory)
    with (directory / "events.jsonl").open("x") as stream:
        evidence = Evidence(stream)
        evidence.emit(
            "start",
            jobs=config.jobs,
            workers=config.workers,
            seed=config.seed,
            timeout=config.timeout,
            kill_interval=config.kill_interval,
        )
        started = time.monotonic()
        try:
            await scenario(config, directory, evidence)
        except BaseException as exc:
            # Top-level evidence boundary: record failures/cancellation, then re-raise unchanged.
            evidence.emit(
                "FAIL",
                exception=type(exc).__name__,
                error=str(exc),
                elapsed_s=time.monotonic() - started,
            )
            raise


async def scenario(config: ChaosConfig, directory: Path, evidence: Evidence) -> None:
    kills: list[int] = []
    started = time.monotonic()
    async with Cluster(directory, config.workers) as cluster, client(cluster.base_url) as http:
        traffic = Traffic(http, evidence, config.jobs, config.seed)

        async def kill_loop() -> None:
            rng = random.Random(config.seed)
            while not traffic.stopping:
                await anyio.sleep(config.kill_interval * rng.uniform(0.8, 1.2))
                if traffic.stopping:
                    break
                slot = rng.randrange(config.workers)
                old, new = cluster.kill_and_replace(slot)
                kills.append(old)
                abandoned = owned_job(cluster.db_path, old)
                if abandoned is not None:
                    traffic.protected.add(abandoned)
                evidence.emit("killed_job", job_id=abandoned, old_pid=old)
                evidence.emit("SIGKILL", old_pid=old, replacement_pid=new, slot=slot)

        async with anyio.create_task_group() as group:
            group.start_soon(traffic.produce)
            group.start_soon(traffic.repeat, 0)
            group.start_soon(traffic.repeat, 1)
            group.start_soon(traffic.cancel, cluster)
            group.start_soon(traffic.conflict_probe)
            group.start_soon(kill_loop)
            with anyio.fail_after(config.timeout):
                while True:
                    await anyio.sleep(2)
                    state = await anyio.to_thread.run_sync(snapshot, cluster.db_path)
                    evidence.emit(
                        "progress",
                        jobs=state.jobs,
                        active=state.active,
                        effects=state.effects,
                        attempts=state.attempts,
                        duplicates=traffic.duplicates,
                        kills=len(kills),
                    )
                    if traffic.produced and state.jobs == config.jobs and state.active == 0:
                        break
            # Finish in-flight requests so every successful cancel response enters the oracle.
            traffic.stopping = True

        cursor = 0
        seen: set[JobId] = set()
        while True:
            response = await http.get("/jobs", params={"cursor": cursor, "limit": 137})
            response.raise_for_status()
            page = Page.model_validate_json(response.content)
            for job in page.items:
                if job.id in seen:
                    raise HarnessViolation(f"Pagination repeated job {job.id}")
                seen.add(job.id)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        if len(seen) != config.jobs or seen != set(traffic.ids.values()):
            raise HarnessViolation("Pagination did not return every acknowledged job")
        failures = await anyio.to_thread.run_sync(verify, cluster.db_path, config.jobs, traffic.cancelled)
        if failures:
            (directory / "failures.json").write_text(json.dumps(failures, indent=2))
            raise HarnessViolation("; ".join(failures[:20]))
        if any(proc.poll() is not None for proc in cluster.workers):
            raise HarnessViolation("Worker exited unexpectedly outside fault injection")
        state = await anyio.to_thread.run_sync(snapshot, cluster.db_path)
        if config.jobs >= 10_000 and (
            len(kills) < 5
            or traffic.duplicates == 0
            or traffic.running_cancels == 0
            or state.reclaims == 0
            or len(traffic.protected) < 5
        ):
            raise HarnessViolation("Insufficient fault/duplicate/running-cancel coverage")
        evidence.emit(
            "PASS",
            jobs=state.jobs,
            succeeded=state.succeeded,
            failed=state.failed,
            cancelled=state.cancelled,
            effects=state.effects,
            attempts=state.attempts,
            duplicate_posts=traffic.duplicates,
            cancel_acks=traffic.cancels,
            running_cancels=traffic.running_cancels,
            cancel_conflicts=traffic.conflicts,
            transient_errors=traffic.transient_errors,
            kills=len(kills),
            reclaims=state.reclaims,
            killed_active_jobs=len(traffic.protected),
            elapsed_s=time.monotonic() - started,
            invariants="all checked",
        )


def main(  # noqa: PLR0913 - Typer maps individual parameters to documented CLI options.
    *,
    jobs: int = 10_000,
    workers: int = 8,
    seed: int = 41,
    timeout: float = 240,
    kill_interval: float = 2,
    output: str = "evidence",
) -> None:
    if jobs < 1 or workers < 1 or kill_interval <= 0 or timeout <= 0:
        raise typer.BadParameter("jobs, workers, timeout and kill interval must be positive")
    anyio.run(run, ChaosConfig(jobs, workers, seed, timeout, kill_interval, Path(output)))


if __name__ == "__main__":
    typer.run(main)
