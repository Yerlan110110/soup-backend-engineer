from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, NewType

from pydantic import BaseModel, ConfigDict, Field

JobId = NewType("JobId", int)
Generation = NewType("Generation", int)


class State(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid", strict=True)
    duration_ms: int = Field(default=50, ge=0, le=300_000)
    should_fail: bool = False
    value: str = Field(default="training-output", max_length=1024)


class Job(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    id: JobId
    key: str
    payload: str
    state: State
    generation: Generation
    owner: str | None
    lease_until_ms: int | None
    created_ms: int
    updated_ms: int
    finished_ms: int | None
    error: str | None


class Page(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    items: list[Job]
    next_cursor: int | None


class Health(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    status: str = "ok"


@dataclass(frozen=True, slots=True)
class Created:
    job: Job
    fresh: bool


@dataclass(frozen=True, slots=True)
class Claim:
    job_id: JobId
    generation: Generation
    owner: str
    spec: JobSpec


class MissingJob(Exception):
    def __init__(self, job_id: JobId) -> None:
        super().__init__(job_id)
        self.job_id: JobId = job_id


class KeyConflict(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key: str = key


class TerminalConflict(Exception):
    def __init__(self, job_id: JobId, state: State) -> None:
        super().__init__(job_id, state)
        self.job_id: JobId = job_id
        self.state: State = state
