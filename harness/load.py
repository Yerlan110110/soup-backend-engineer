import json
import math
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import anyio
import httpx2
import typer

from harness.client import client
from harness.cluster import Cluster
from harness.evidence import Evidence, source_manifest
from harness.oracle import snapshot
from harness.traffic import HarnessViolation
from soup_queue.models import Job


@dataclass(frozen=True, slots=True)
class Observation:
    index: int
    operation: str
    status: int
    scheduled_latency_ms: float
    dispatch_lag_ms: float


@dataclass(frozen=True, slots=True)
class LoadConfig:
    rps: int
    seconds: int
    workers: int
    output: Path


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


async def run(config: LoadConfig) -> None:
    directory = config.output.resolve() / f"load-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:6]}"
    directory.mkdir(parents=True)
    source_manifest(directory)
    with (directory / "events.jsonl").open("x") as stream:
        evidence = Evidence(stream)
        evidence.emit(
            "start",
            rps=config.rps,
            seconds=config.seconds,
            workers=config.workers,
            mix="50% create / 20% get / 20% list / 10% cancel; open loop",
        )
        try:
            await scenario(config, directory, evidence)
        except BaseException as exc:
            # Preserve the failed run and propagate its original traceback.
            evidence.emit("FAIL", exception=type(exc).__name__, error=str(exc))
            raise


async def scenario(config: LoadConfig, directory: Path, evidence: Evidence) -> None:
    observations: list[Observation] = []
    ids: list[int] = []
    async with Cluster(directory, config.workers, lease_ms=3000) as cluster, client(cluster.base_url) as http:
        for i in range(20):
            response = await http.post("/jobs", headers={"Idempotency-Key": f"warm-{i}"}, json={"duration_ms": 50})
            response.raise_for_status()
            ids.append(Job.model_validate_json(response.content).id)
        started = time.monotonic()

        async def request(index: int, scheduled: float) -> None:
            dispatched = time.monotonic()
            op = index % 10
            name = "create"
            status = 0
            try:
                if op < 5:
                    response = await http.post(
                        "/jobs", headers={"Idempotency-Key": f"load-{index}"}, json={"duration_ms": 50}
                    )
                    if response.status_code == 201:
                        ids.append(Job.model_validate_json(response.content).id)
                elif op < 7:
                    name = "get"
                    response = await http.get(f"/jobs/{ids[index % len(ids)]}")
                elif op < 9:
                    name = "list"
                    response = await http.get("/jobs", params={"limit": 20})
                else:
                    name = "cancel"
                    response = await http.post(f"/jobs/{ids[-1]}/cancel")
                status = response.status_code
            except httpx2.TransportError as exc:
                evidence.emit("request_error", index=index, operation=name, error=str(exc))
            observations.append(
                Observation(index, name, status, (time.monotonic() - scheduled) * 1000, (dispatched - scheduled) * 1000)
            )

        async with anyio.create_task_group() as group:
            for index in range(config.rps * config.seconds):
                scheduled = started + index / config.rps
                await anyio.sleep(max(0, scheduled - time.monotonic()))
                group.start_soon(request, index, scheduled)
        request_elapsed = time.monotonic() - started
        # Let accepted jobs finish, with a bounded liveness check.
        with anyio.fail_after(90):
            while True:
                final = await anyio.to_thread.run_sync(snapshot, cluster.db_path)
                if final.active == 0:
                    break
                await anyio.sleep(0.25)
        with (directory / "requests.jsonl").open("x") as raw:
            for item in sorted(observations, key=lambda item: item.index):
                raw.write(
                    json.dumps(
                        {
                            "index": item.index,
                            "operation": item.operation,
                            "status": item.status,
                            "scheduled_latency_ms": item.scheduled_latency_ms,
                            "dispatch_lag_ms": item.dispatch_lag_ms,
                        }
                    )
                    + "\n"
                )
        errors = sum(
            item.status not in ((200, 409) if item.operation == "cancel" else (200, 201)) for item in observations
        )
        latencies = [item.scheduled_latency_ms for item in observations]
        counts = Counter(item.status for item in observations)
        evidence.emit(
            "measurements",
            requests=len(observations),
            elapsed_s=request_elapsed,
            achieved_rps=len(observations) / request_elapsed,
            errors=errors,
            p50_ms=percentile(latencies, 0.5),
            p95_ms=percentile(latencies, 0.95),
            p99_ms=percentile(latencies, 0.99),
            max_dispatch_lag_ms=max(item.dispatch_lag_ms for item in observations),
            final_active=final.active,
            final_jobs=final.jobs,
            final_effects=final.effects,
            http_statuses=json.dumps(dict(counts)),
        )
        if errors:
            raise HarnessViolation(f"Load test returned {errors} unexpected responses")
        evidence.emit("PASS", interpretation="local mixed-workload measurement, not production certification")


def main(*, rps: int = 100, seconds: int = 60, workers: int = 4, output: str = "evidence") -> None:
    if min(rps, seconds, workers) < 1:
        raise typer.BadParameter("rps, seconds and workers must be positive")
    anyio.run(run, LoadConfig(rps, seconds, workers, Path(output)))


if __name__ == "__main__":
    typer.run(main)
