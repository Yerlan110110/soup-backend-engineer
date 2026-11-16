# pyright: reportAny=false
# Independent SQL oracle intentionally reads storage without the application's repositories.
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from pydantic import TypeAdapter

from soup_queue.models import JobId, JobSpec


@dataclass(frozen=True, slots=True)
class Snapshot:
    jobs: int
    active: int
    effects: int
    attempts: int
    succeeded: int
    failed: int
    cancelled: int
    reclaims: int


def spec_for(index: int) -> JobSpec:
    return JobSpec(duration_ms=30 + index % 41, should_fail=index % 37 == 0, value=f"output-{index}")


def snapshot(db_path: Path) -> Snapshot:
    with closing(sqlite3.connect(db_path)) as conn:
        row = conn.execute(
            "SELECT count(*),coalesce(sum(state IN ('queued','running')),0),"
            "(SELECT count(*) FROM effects),(SELECT count(*) FROM attempts),"
            "coalesce(sum(state='succeeded'),0),coalesce(sum(state='failed'),0),"
            "coalesce(sum(state='cancelled'),0), "
            "coalesce(sum(max(generation-1,0)),0) FROM jobs",
        ).fetchone()
        return Snapshot(*TypeAdapter(tuple[int, int, int, int, int, int, int, int]).validate_python(row))


def running_id(db_path: Path) -> JobId | None:
    with closing(sqlite3.connect(db_path)) as conn:
        row = conn.execute("SELECT id FROM jobs WHERE state='running' ORDER BY id DESC LIMIT 1").fetchone()
        return JobId(TypeAdapter(int).validate_python(row[0])) if row else None


def verify(db_path: Path, count: int, cancelled: set[JobId]) -> list[str]:  # noqa: PLR0912 - Each branch reports a distinct invariant.
    failures: list[str] = []
    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        keys = {row["key"] for row in rows}
        expected = {f"job-{i}" for i in range(count)}
        if keys != expected or len(rows) != count:
            failures.append(f"key set/count mismatch: actual={len(rows)}, expected={count}")
        effects = {row["job_id"]: row for row in conn.execute("SELECT * FROM effects")}
        for row in rows:
            job_id = JobId(row["id"])
            effect = effects.get(job_id)
            spec = spec_for(int(row["key"].removeprefix("job-")))
            if json.loads(row["payload"]) != spec.model_dump():
                failures.append(f"payload changed: {job_id}")
            if row["state"] not in ("succeeded", "failed", "cancelled"):
                failures.append(f"nonterminal: {job_id}")
            if row["finished_ms"] is None or row["owner"] is not None or row["lease_until_ms"] is not None:
                failures.append(f"terminal metadata: {job_id}")
            if (row["state"] == "succeeded") != (effect is not None):
                failures.append(f"effect/state disagreement: {job_id}")
            if effect is not None and (effect["generation"] != row["generation"] or effect["value"] != spec.value):
                failures.append(f"stale/wrong effect: {job_id}")
            if row["state"] == "failed" and (not spec.should_fail or not row["error"]):
                failures.append(f"unexpected failure: {job_id}")
            if row["state"] == "succeeded" and spec.should_fail:
                failures.append(f"unexpected success: {job_id}")
            if job_id in cancelled and row["state"] != "cancelled":
                failures.append(f"acknowledged cancellation lost: {job_id}")
            if row["state"] == "cancelled" and job_id not in cancelled:
                failures.append(f"unrequested cancellation: {job_id}")
        checks = {
            "duplicate_effect": "SELECT job_id FROM effects GROUP BY job_id HAVING count(*)>1",
            "duplicate_key": "SELECT key FROM jobs GROUP BY key HAVING count(*)>1",
            "orphan_effect": "SELECT e.job_id FROM effects e LEFT JOIN jobs j ON e.job_id=j.id WHERE j.id IS NULL",
            "effect_owner": (
                "SELECT e.job_id FROM effects e JOIN attempts a USING(job_id,generation) WHERE e.owner!=a.owner"
            ),
            "attempt_count": (
                "SELECT j.id FROM jobs j LEFT JOIN attempts a ON j.id=a.job_id GROUP BY j.id "
                "HAVING count(a.generation)!=j.generation OR coalesce(max(a.generation),0)!=j.generation"
            ),
            "foreign_key": "PRAGMA foreign_key_check",
        }
        for name, query in checks.items():
            if conn.execute(query).fetchall():
                failures.append(name)
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            failures.append("integrity_check")
    return failures


def owned_job(db_path: Path, pid: int) -> JobId | None:
    with closing(sqlite3.connect(db_path)) as conn:
        row = conn.execute("SELECT id FROM jobs WHERE state='running' AND owner LIKE ?", (f"{pid}-%",)).fetchone()
        return JobId(TypeAdapter(int).validate_python(row[0])) if row else None
