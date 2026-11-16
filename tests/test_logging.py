# pyright: reportAny=false
# JSON logging is an output boundary checked immediately below.
import json

import pytest
from structlog.stdlib import get_logger

from soup_queue.logging import configure


def test_exception_preserved_when_json_log_is_emitted(capsys: pytest.CaptureFixture[str]) -> None:
    # Given the production logger and a real exception.
    configure()
    # When an operation reports the exception through the configured formatter.
    try:
        _ = 1 / 0
    except ZeroDivisionError:
        get_logger().exception("probe_failure")
    # Then the type, message and stack remain visible in the emitted JSON event.
    event = json.loads(capsys.readouterr().out)
    assert event["event"] == "probe_failure"
    assert "ZeroDivisionError" in event["exception"]
    assert "division by zero" in event["exception"]
    assert "Traceback" in event["exception"]
