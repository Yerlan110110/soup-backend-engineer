from collections.abc import AsyncGenerator
from io import StringIO

import httpx2
import pytest

from harness.evidence import Evidence
from harness.traffic import Traffic
from soup_queue.api import create_app
from soup_queue.config import Settings
from soup_queue.models import Job, JobSpec, Page
from tests.conftest import Queue


@pytest.fixture
async def api(queue: Queue) -> AsyncGenerator[httpx2.AsyncClient]:
    app = create_app(Settings(db=queue.db.path))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app),
        base_url="http://test",
        timeout=5,
    ) as client:
        yield client


@pytest.mark.anyio
async def test_harness_submits_valid_json_when_using_real_api(api: httpx2.AsyncClient) -> None:
    # Given the real workload generator and ASGI application.
    traffic = Traffic(api, Evidence(StringIO()), 1, 1)
    # When it submits its normal request.
    await traffic.submit(0)
    # Then the API accepts the intended job.
    assert len(traffic.ids) == 1


@pytest.mark.anyio
async def test_same_id_when_retry_normalizes_defaults(api: httpx2.AsyncClient) -> None:
    # Given a job accepted with defaults omitted.
    first = await api.post("/jobs", headers={"Idempotency-Key": "retry"}, json={})
    original = Job.model_validate_json(first.content)
    # When defaults are explicit on a retry.
    retry = await api.post("/jobs", headers={"Idempotency-Key": "retry"}, json=JobSpec().model_dump())
    # Then the original resource is returned, not recreated.
    assert first.status_code == 201
    assert retry.status_code == 200
    assert Job.model_validate_json(retry.content).id == original.id
    assert retry.headers["Location"] == f"/jobs/{original.id}"


@pytest.mark.anyio
@pytest.mark.parametrize("body", [{"duration_ms": -1}, {"duration_ms": "50"}, {"extra": 1}])
async def test_validation_when_request_invalid(api: httpx2.AsyncClient, body: dict[str, int | str]) -> None:
    # Given an invalid body and a valid key.
    # When the API parses the request boundary.
    response = await api.post("/jobs", headers={"Idempotency-Key": "invalid"}, json=body)
    # Then invalid data never reaches storage.
    assert response.status_code == 422


@pytest.mark.anyio
async def test_rejects_when_idempotency_key_missing(api: httpx2.AsyncClient) -> None:
    # Given a valid body without a key.
    # When it is submitted.
    response = await api.post("/jobs", json={})
    # Then it cannot create an unprotected job.
    assert response.status_code == 422


@pytest.mark.anyio
async def test_pagination_when_jobs_span_pages(api: httpx2.AsyncClient, queue: Queue) -> None:
    # Given five jobs and a small page size.
    expected = [queue.jobs.create(f"page-{i}", JobSpec()).job.id for i in range(5)]
    # When the client follows every returned cursor.
    seen: list[int] = []
    cursor = 0
    while True:
        response = await api.get("/jobs", params={"cursor": cursor, "limit": 2})
        page = Page.model_validate_json(response.content)
        seen.extend(job.id for job in page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    # Then no row is skipped or repeated.
    assert seen == expected


@pytest.mark.anyio
async def test_terminal_cancel_conflict_when_job_succeeded(api: httpx2.AsyncClient, queue: Queue) -> None:
    # Given a succeeded job.
    claim = queue.claimed()
    queue.leases.complete(claim)
    # When an HTTP client cancels it.
    response = await api.post(f"/jobs/{claim.job_id}/cancel")
    # Then the API exposes the race outcome explicitly.
    assert response.status_code == 409


@pytest.mark.anyio
async def test_cancel_is_repeatable_when_already_cancelled(api: httpx2.AsyncClient, queue: Queue) -> None:
    # Given a cancelled running job.
    claim = queue.claimed()
    queue.jobs.cancel(claim.job_id)
    # When cancellation is retried.
    response = await api.post(f"/jobs/{claim.job_id}/cancel")
    # Then the same terminal cancellation is returned.
    assert response.status_code == 200
    assert Job.model_validate_json(response.content).state == "cancelled"


@pytest.mark.anyio
async def test_not_found_when_job_missing(api: httpx2.AsyncClient) -> None:
    # Given an empty queue.
    # When a nonexistent status is requested.
    response = await api.get("/jobs/9999")
    # Then the API returns 404.
    assert response.status_code == 404
