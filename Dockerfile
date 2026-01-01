# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_CACHE_DIR=/root/.cache/uv \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && ln -s /root/.local/bin/uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

FROM base AS app
COPY pipelines/ /app/pipelines/

FROM app AS csv_ingest
COPY data/ /app/data/
ENV CSV_PATH=/app/data/transactions_fr.csv

CMD ["python", "-m", "pipelines.csv.main"]

FROM app AS producer
CMD ["python", "-m", "pipelines.kafka.producer.main"]


FROM app AS consumer
CMD ["python", "-m", "pipelines.kafka.consumer.main"]