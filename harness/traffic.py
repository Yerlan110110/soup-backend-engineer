import random
from dataclasses import dataclass, field

import anyio
import httpx2

from harness.cluster import Cluster
from harness.evidence import Evidence
from harness.oracle import running_id, spec_for
from soup_queue.models import Job, JobId


class HarnessViolation(RuntimeError):
    """An observed response contradicts the queue contract."""


@dataclass(slots=True)
class Traffic:
    """Accumulate client observations on one event loop; never used from worker threads."""

    http: httpx2.AsyncClient
    evidence: Evidence
    total: int
    seed: int
    ids: dict[int, JobId] = field(default_factory=dict)
    cancelled: set[JobId] = field(default_factory=set)
    protected: set[JobId] = field(default_factory=set)
    duplicates: int = 0
    cancels: int = 0
    running_cancels: int = 0
    conflicts: int = 0
    transient_errors: int = 0
    produced: bool = False
    stopping: bool = False
    first_created: anyio.Event = field(default_factory=anyio.Event)

    async def submit(self, index: int) -> None:
        for attempt in range(6):
            try:
                response = await self.http.post(
                    "/jobs",
                    headers={"Idempotency-Key": f"job-{index}", "Content-Type": "application/json"},
                    content=spec_for(index).model_dump_json(),
                )
                if response.status_code != 503:
                    response.raise_for_status()
                    job = Job.model_validate_json(response.content)
                    if index in self.ids and self.ids[index] != job.id:
                        raise HarnessViolation(f"Retry created another job for key {index}")
                    self.ids[index] = job.id
                    self.first_created.set()
                    if response.status_code == 200:
                        self.duplicates += 1
                    return
                self.evidence.emit("http_retry", key=index, status=503, attempt=attempt)
            except httpx2.TransportError as exc:
                self.evidence.emit("transport_retry", key=index, error=str(exc), attempt=attempt)
            self.transient_errors += 1
            await anyio.sleep(0.05 * (1 << attempt))
        raise HarnessViolation(f"POST exhausted retries for key {index}")

    async def produce(self) -> None:
        async def lane(offset: int) -> None:
            for index in range(offset, self.total, 16):
                await self.submit(index)

        async with anyio.create_task_group() as group:
            for offset in range(16):
                group.start_soon(lane, offset)
        self.produced = True
        self.evidence.emit("producers_finished", acknowledged_keys=len(self.ids))

    async def repeat(self, lane: int) -> None:
        rng = random.Random(self.seed + lane + 100)
        while not self.stopping:
            if self.ids:
                index = rng.choice(tuple(self.ids))
                await self.submit(index)
            await anyio.sleep(0.005)

    async def cancel(self, cluster: Cluster) -> None:
        rng = random.Random(self.seed + 200)
        turn = 0
        while not self.stopping:
            await anyio.sleep(0.02)
            turn += 1
            job_id = None
            if turn % 5 == 0:
                job_id = await anyio.to_thread.run_sync(running_id, cluster.db_path)
            if job_id is None and self.ids:
                job_id = rng.choice(tuple(self.ids.values()))
            if job_id is None or job_id in self.protected:
                continue
            response = await self.http.post(f"/jobs/{job_id}/cancel")
            if response.status_code == 409:
                self.conflicts += 1
                continue
            response.raise_for_status()
            job = Job.model_validate_json(response.content)
            if job.state != "cancelled":
                raise HarnessViolation(f"Cancel acknowledged wrong state: {job.id}")
            if job_id not in self.cancelled:
                self.cancels += 1
                self.running_cancels += int(job.generation > 0)
                self.evidence.emit("cancel_ack", job_id=job_id, generation=job.generation)
            self.cancelled.add(job_id)

    async def conflict_probe(self) -> None:
        await self.first_created.wait()
        index = next(iter(self.ids))
        response = await self.http.post(
            "/jobs",
            headers={"Idempotency-Key": f"job-{index}"},
            json={"duration_ms": 0, "value": "conflicting-request"},
        )
        if response.status_code != 409:
            raise HarnessViolation(f"Conflicting idempotency key returned {response.status_code}")
        self.evidence.emit("key_conflict_checked", status=response.status_code)
