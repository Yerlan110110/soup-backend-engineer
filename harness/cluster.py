import os
import socket
import subprocess
import sys
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Self

import anyio
import httpx2

from harness.client import client


@dataclass(slots=True)
class Cluster:
    """Own and clean up only this run's processes; worker slots mutate on restart."""

    directory: Path
    worker_count: int
    lease_ms: int = 1000
    stack: ExitStack = field(default_factory=ExitStack)
    workers: list[subprocess.Popen[bytes]] = field(default_factory=list)
    processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    base_url: str = ""
    generation: int = 0

    @property
    def db_path(self) -> Path:
        return self.directory / "queue.sqlite"

    def launch(self, command: list[str], name: str) -> subprocess.Popen[bytes]:
        log = self.stack.enter_context((self.directory / f"{name}.log").open("wb"))
        env = os.environ | {
            "QUEUE_DB": str(self.db_path),
            "QUEUE_LEASE_MS": str(self.lease_ms),
            "PYTHONUNBUFFERED": "1",
        }
        proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(proc)
        return proc

    def start_worker(self) -> subprocess.Popen[bytes]:
        self.generation += 1
        return self.launch([sys.executable, "-m", "soup_queue.worker"], f"worker-{self.generation:03d}")

    async def __aenter__(self) -> Self:
        # Keep the chosen port reserved until the child inherits its listening socket.
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(128)
            self.base_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
            log = self.stack.enter_context((self.directory / "api.log").open("wb"))
            api = subprocess.Popen(  # noqa: ASYNC220 - Startup only; pass_fds reserves the port atomically.
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "soup_queue.api:create_app",
                    "--factory",
                    "--fd",
                    str(listener.fileno()),
                    "--no-access-log",
                ],
                pass_fds=(listener.fileno(),),
                stdout=log,
                stderr=subprocess.STDOUT,
                env=os.environ | {"QUEUE_DB": str(self.db_path), "PYTHONUNBUFFERED": "1"},
            )
            self.processes.append(api)
        try:
            async with client(self.base_url) as http:
                with anyio.fail_after(20):
                    while True:
                        try:
                            response = await http.get("/healthz")
                            response.raise_for_status()
                            break
                        except httpx2.TransportError:
                            await anyio.sleep(0.05)
            for _ in range(self.worker_count):
                self.workers.append(self.start_worker())
        except BaseException:
            # Resource-owning context entry must clean children even on cancellation.
            self.stop()
            raise
        return self

    def kill_and_replace(self, slot: int) -> tuple[int, int]:
        old = self.workers[slot]
        if old.poll() is not None:
            raise ChildProcessError(f"Worker {old.pid} exited unexpectedly: {old.returncode}")
        old.kill()  # POSIX SIGKILL, equivalent to kill -9. No graceful handler runs.
        old.wait(timeout=5)
        replacement = self.start_worker()
        self.workers[slot] = replacement
        return old.pid, replacement.pid

    def stop(self) -> None:
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self.stack.close()

    async def __aexit__(
        self,
        _kind: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.stop()
