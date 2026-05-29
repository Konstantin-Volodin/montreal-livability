# container image
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy bytecode-compiled, copy (not symlink) into the image's own filesystem.
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Resolve dependencies first (cached layer) from the lockfile only, so source
# edits don't force a full reinstall. --no-dev drops the webserver/dg tooling;
# the `dagster` CLI used at runtime ships with the base `dagster` dependency.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Now the project source, then install the `montreal` package itself.
COPY . .
RUN uv sync --frozen --no-dev

# Put the venv on PATH so `python` and `dagster` resolve to it.
ENV PATH="/app/.venv/bin:$PATH"

# Persistent (within the task) Dagster home so the staged materialize
# subprocesses share dynamic-partition state. No dagster.yaml: defaults are
# fine for a throwaway instance, and per-run logs go to CloudWatch via the ECS
# awslogs driver (see deployments/aws/app.py), not a Dagster compute-log manager.
ENV DAGSTER_HOME=/opt/dagster/home
RUN mkdir -p "$DAGSTER_HOME"

ENTRYPOINT ["python", "-m", "montreal.batch"]
