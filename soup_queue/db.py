# pyright: reportAny=false
# sqlite3 exposes dynamically typed rows; every domain row is parsed by Pydantic here.
import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pydantic import TypeAdapter

from soup_queue.models import Job

NOW_MS: Final = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
INT: Final = TypeAdapter(int)


@dataclass(frozen=True, slots=True)
class Database:
    path: Path

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection]:
        with closing(sqlite3.connect(self.path, timeout=5, isolation_level=None)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=FULL")
            yield conn

    @contextmanager
    def write(self) -> Generator[sqlite3.Connection]:
        with self.connection() as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            # Connection.__exit__ commits, or rolls back on exceptions, before close.

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(Path(__file__).with_name("schema.sql").read_text())


def now_ms(conn: sqlite3.Connection) -> int:
    return INT.validate_python(conn.execute(f"SELECT {NOW_MS}").fetchone()[0])


def parse_job(row: sqlite3.Row) -> Job:
    return Job.model_validate(dict(row))
