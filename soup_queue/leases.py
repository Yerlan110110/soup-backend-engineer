# pyright: reportAny=false
# sqlite3 rows cross the typed boundary through parse_job.
from dataclasses import dataclass

from soup_queue.db import Database, now_ms, parse_job
from soup_queue.models import Claim, JobSpec


@dataclass(frozen=True, slots=True)
class Leases:
    db: Database
    lease_ms: int

    def claim(self, owner: str) -> Claim | None:
        with self.db.write() as conn:
            now = now_ms(conn)
            row = conn.execute(
                "SELECT id FROM jobs WHERE state='queued' "
                "OR (state='running' AND lease_until_ms<=?) ORDER BY id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            job = parse_job(
                conn.execute(
                    "UPDATE jobs SET state='running', generation=generation+1, owner=?, "
                    "lease_until_ms=?, updated_ms=? WHERE id=? RETURNING *",
                    (owner, now + self.lease_ms, now, row["id"]),
                ).fetchone()
            )
            conn.execute(
                "INSERT INTO attempts(job_id,generation,owner,claimed_ms) VALUES(?,?,?,?)",
                (job.id, job.generation, owner, now),
            )
            return Claim(job.id, job.generation, owner, JobSpec.model_validate_json(job.payload))

    def renew(self, claim: Claim) -> bool:
        with self.db.write() as conn:
            now = now_ms(conn)
            return (
                conn.execute(
                    "UPDATE jobs SET lease_until_ms=?, updated_ms=? "
                    "WHERE id=? AND state='running' AND owner=? AND generation=? AND lease_until_ms>?",
                    (now + self.lease_ms, now, claim.job_id, claim.owner, claim.generation, now),
                ).rowcount
                == 1
            )

    def complete(self, claim: Claim) -> bool:
        with self.db.write() as conn:
            now = now_ms(conn)
            row = conn.execute(
                "SELECT id FROM jobs WHERE id=? AND state='running' AND owner=? AND generation=? AND lease_until_ms>?",
                (claim.job_id, claim.owner, claim.generation, now),
            ).fetchone()
            if row is None:
                return False
            state = "failed" if claim.spec.should_fail else "succeeded"
            error = "simulated training failure" if claim.spec.should_fail else None
            if not claim.spec.should_fail:
                conn.execute(
                    "INSERT INTO effects(job_id,generation,owner,value,committed_ms) VALUES(?,?,?,?,?)",
                    (claim.job_id, claim.generation, claim.owner, claim.spec.value, now),
                )
            conn.execute(
                "UPDATE jobs SET state=?, error=?, owner=NULL, lease_until_ms=NULL, "
                "finished_ms=?, updated_ms=? WHERE id=?",
                (state, error, now, now, claim.job_id),
            )
            return True
