# Stage 1 — builder: resolve & build wheels
FROM python:3.13-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# uv bootstrap
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# copy only dependency descriptors for cache
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --compile-bytecode --no-dev

# Stage 2 — runtime: copy venv and run
FROM python:3.13-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN addgroup --system app && adduser --system --ingroup app app
WORKDIR /app

# OS deps minimal; clean apt cache
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*

# copy virtual environment from builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# copy source and config
COPY --chown=app:app ./src ./src
COPY --chown=app:app ./config ./config
# Add venv to PATH so dagster commands work directly
ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app/src"

# create directories with proper permissions for running Dagster DAG
RUN mkdir -p /app/data/parquet /app/.cache /opt/dagster /bulletin_raw && \
    chown -R app:app /opt/dagster /bulletin_raw

USER app

# Expose gRPC port for Dagster code location
EXPOSE 4000

ENTRYPOINT ["tini","--"]
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "de_datalake_bulletin_dataload.definitions"]