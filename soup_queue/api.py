import sqlite3
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from structlog.stdlib import get_logger

from soup_queue.config import Settings
from soup_queue.db import Database
from soup_queue.jobs import Jobs
from soup_queue.logging import configure
from soup_queue.models import (
    Health,
    Job,
    JobId,
    JobSpec,
    KeyConflict,
    MissingJob,
    Page,
    TerminalConflict,
)


def create_app(settings: Settings | None = None) -> FastAPI:
    configure()
    config = settings or Settings()
    db = Database(config.db)
    db.initialize()
    jobs = Jobs(db)
    app = FastAPI(title="Soup Training Queue", version="0.1.0")
    log = get_logger()

    @app.exception_handler(sqlite3.OperationalError)
    async def storage_error(_: Request, exc: Exception) -> JSONResponse:
        log.error("storage_unavailable", exc_info=(type(exc), exc, exc.__traceback__))
        return JSONResponse(
            status_code=503,
            content={"detail": "Storage unavailable; retry with the same idempotency key"},
            headers={"Retry-After": "1"},
        )

    @app.get("/healthz")
    def health() -> Health:
        with db.connection() as conn:
            conn.execute("SELECT 1").fetchone()
        return Health()

    @app.post("/jobs", status_code=201, responses={200: {}, 409: {}})
    def create_job(
        spec: JobSpec,
        response: Response,
        idempotency_key: Annotated[str, Header(pattern=r"^[A-Za-z0-9._:-]{1,128}$")],
    ) -> Job:
        try:
            created = jobs.create(idempotency_key, spec)
        except KeyConflict as exc:
            raise HTTPException(409, "Idempotency key already used with a different body") from exc
        response.status_code = 201 if created.fresh else 200
        response.headers["Location"] = f"/jobs/{created.job.id}"
        return created.job

    @app.get("/jobs")
    def list_jobs(
        cursor: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> Page:
        return jobs.list(cursor, limit)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: int) -> Job:
        try:
            return jobs.get(JobId(job_id))
        except MissingJob as exc:
            raise HTTPException(404, "Job not found") from exc

    @app.post("/jobs/{job_id}/cancel", responses={409: {}})
    def cancel_job(job_id: int) -> Job:
        try:
            return jobs.cancel(JobId(job_id))
        except MissingJob as exc:
            raise HTTPException(404, "Job not found") from exc
        except TerminalConflict as exc:
            raise HTTPException(409, f"Job already {exc.state}") from exc

    return app
