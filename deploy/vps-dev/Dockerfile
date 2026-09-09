# Registry index digests verified on 2026-09-08; architecture selected by Docker.
FROM ghcr.io/astral-sh/uv:0.12.0@sha256:606e70c71c852d03f611b1e56a195d08648507018a7057fab82c4974c4eae105 AS uv
FROM apache/airflow:2.11.1-python3.12@sha256:18130e49360be9e5cbe8bad00f5cda56995f347f4c7c0b5de143c6b746ede16d
USER root
COPY --from=uv /uv /usr/local/bin/uv
# Root and airflow must not share cache files created by the other user.
ENV UV_NO_CACHE=true
# Remaining reproducibility gap: Debian package repositories are not snapshotted.
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl unzip nodejs npm git \
    && rm -rf /var/lib/apt/lists/* \
    && uv venv --python /usr/local/bin/python3 /opt/dev-venv \
    && mkdir -p /opt/build-input /opt/airflow/data /opt/dev-tools \
    && chown -R 50000:0 /opt/dev-venv /opt/build-input /opt/airflow/data
COPY --chown=50000:0 pyproject.toml uv.lock /opt/build-input/
USER airflow
WORKDIR /opt/build-input
# The source lock supplies transitive versions AND artifact hashes. No stubs.
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt > requirements.txt \
    && uv pip install --python /opt/dev-venv/bin/python --require-hashes -r requirements.txt \
    && uv pip check --python /opt/dev-venv/bin/python
ENV PATH="/opt/dev-venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1
RUN python -c 'from importlib.metadata import version; assert version("apache-airflow") == "2.11.1"'
USER root
# Explicit allowlist excludes .env, OAuth files, local changes and tooling links.
COPY utils /opt/airflow/dags/repo/utils
COPY congress_videos /opt/airflow/dags/repo/congress_videos
COPY examples /opt/airflow/dags/repo/examples
COPY .airflowignore /opt/airflow/dags/repo/.airflowignore
COPY deploy/vps-dev/init.py deploy/vps-dev/verify.py deploy/vps-dev/ml_smoke.py deploy/vps-dev/app_init.py deploy/vps-dev/app_smoke.py /opt/dev-tools/
RUN chmod -R a-w /opt/airflow/dags/repo /opt/dev-tools
ARG SOURCE_REVISION
ARG RELEASE_ID
LABEL org.opencontainers.image.revision=$SOURCE_REVISION homeserver.release=$RELEASE_ID
USER airflow
WORKDIR /opt/airflow
