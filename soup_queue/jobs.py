# pyright: reportAny=false
# sqlite3 rows cross the typed boundary through parse_job.
from dataclasses import dataclass
from typing import assert_never

from soup_queue.db import Database, now_ms, parse_job
from soup_queue.models import (
    Created,
    Job,
    JobId,
    JobSpec,
    KeyConflict,
    MissingJob,
    Page,
    State,
    TerminalConflict,
)


@dataclass(frozen=True, slots=True)
class Jobs:
    db: Database

    def create(self, key: str, spec: JobSpec) -> Created:
        payload = spec.model_dump_json()
        with self.db.write() as conn:
            now = now_ms(conn)
            row = conn.execute(
                "INSERT INTO jobs(key,payload,created_ms,updated_ms) VALUES(?,?,?,?) "
                "ON CONFLICT(key) DO NOTHING RETURNING *",
                (key, payload, now, now),
            ).fetchone()
            if row is not None:
                return Created(parse_job(row), fresh=True)
            existing = parse_job(conn.execute("SELECT * FROM jobs WHERE key=?", (key,)).fetchone())
            if existing.payload != payload:
                raise KeyConflict(key)
            return Created(existing, fresh=False)

    def get(self, job_id: JobId) -> Job:
        with self.db.connection() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise MissingJob(job_id)
            return parse_job(row)

    def list(self, cursor: int, limit: int) -> Page:
        with self.db.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE id>? ORDER BY id LIMIT ?",
                (cursor, limit + 1),
            ).fetchall()
            items = [parse_job(row) for row in rows[:limit]]
            return Page(items=items, next_cursor=items[-1].id if len(rows) > limit else None)

    def cancel(self, job_id: JobId) -> Job:
        with self.db.write() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise MissingJob(job_id)
            job = parse_job(row)
            match job.state:
                case State.CANCELLED:
                    return job
                case State.SUCCEEDED | State.FAILED:
                    raise TerminalConflict(job_id, job.state)
                case State.QUEUED | State.RUNNING:
                    now = now_ms(conn)
                    updated = conn.execute(
                        "UPDATE jobs SET state='cancelled', owner=NULL, lease_until_ms=NULL, "
                        "updated_ms=?, finished_ms=? WHERE id=? RETURNING *",
                        (now, now, job_id),
                    ).fetchone()
                    return parse_job(updated)
                case unreachable:
                    assert_never(unreachable)
