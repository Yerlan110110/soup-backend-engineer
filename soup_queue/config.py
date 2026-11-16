from pathlib import Path
from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(env_prefix="QUEUE_", frozen=True)
    db: Path = Path("data/queue.sqlite")
    lease_ms: int = Field(default=3000, ge=300, le=300_000)
    poll_ms: int = Field(default=40, ge=1, le=1000)
