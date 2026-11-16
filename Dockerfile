FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /uvx /bin/
WORKDIR /app
ENV PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY soup_queue ./soup_queue
RUN useradd --uid 10001 --create-home queue && mkdir /data && chown queue:queue /data
USER queue
ENV QUEUE_DB=/data/queue.sqlite
CMD ["uvicorn", "soup_queue.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
