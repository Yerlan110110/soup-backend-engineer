# HTTP 422 in chaos smoke run 01

No live debugger or temporary runtime patch. Evidence retained as requested by task.
Hypotheses: wrong content-type, wrong body validation, malformed header.
Distinguishing check: inspect 422 detail and replay identical bytes with explicit JSON media type.
Durable regression: tests/test_api.py drives Traffic.submit through ASGI and the full smoke harness.
