import hashlib
import json
import platform
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO


@dataclass(frozen=True, slots=True)
class Evidence:
    stream: TextIO

    def emit(self, event: str, **fields: str | float | bool | None) -> None:
        line = json.dumps({"utc": datetime.now(UTC).isoformat(), "event": event, **fields}, sort_keys=True)
        self.stream.write(line + "\n")
        self.stream.flush()
        print(line, flush=True)


def source_manifest(directory: Path) -> None:
    lines: list[str] = []
    paths = sorted(
        [
            *Path("soup_queue").glob("*"),
            *Path("harness").glob("*.py"),
            *Path("tests").glob("*.py"),
            Path("pyproject.toml"),
            Path("uv.lock"),
            Path("docs/DESIGN.md"),
        ]
    )
    lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path}" for path in paths if path.is_file()]
    (directory / "source.sha256").write_text("\n".join(lines) + "\n")
    (directory / "environment.json").write_text(
        json.dumps(
            {
                "python": platform.python_version(),
                "os": platform.platform(),
                "sqlite": sqlite3.sqlite_version,
            },
            indent=2,
        )
        + "\n"
    )
