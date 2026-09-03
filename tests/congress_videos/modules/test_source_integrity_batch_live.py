"""Live-Postgres test for `record_source_integrity_failures` (issue #208, PR A).

Proves the per-item commit isolation claim (spec: "Earlier commit survives a
later failure") against real transaction semantics, which a mocked connection
cannot demonstrate: `record_source_integrity_failures` opens ONE connection
for the whole batch, but each item still commits independently via a
per-item `with conn:` block, so a later item's exception never rolls back an
earlier item's already-committed row.

Runs in its own schema (issue #277 pattern) so it can share a throwaway
database with other live-Postgres test files without collisions. Skips
cleanly when psycopg2 is unavailable or Postgres is unreachable — tries the
connection exactly once, no retry loop.

Run with (nushell):
    with-env {TEST_DATABASE_URL: "postgresql://<role>:<pw>@host:port/db"} {
        uv run pytest tests/congress_videos/modules/test_source_integrity_batch_live.py -o addopts= -v
    }

`-o addopts=` is mandatory: the project's default addopts carries
--cov-fail-under=80, which fails any single-file run regardless of results.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import pytest

from congress_videos.modules.database import CongressionalVideoDB

_TEST_SCHEMA = "test_source_integrity_batch"


def _skip_if_no_postgres():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "psycopg2 not installed"
    return None


# Minimal hand-scaffolded shape — every column read/written by
# _upsert_source_integrity_failure / record_source_integrity_failures
# (database.py's youtube_source_videos upsert).
_SCHEMA_SQL = """
CREATE TABLE youtube_source_videos (
    video_id VARCHAR(50) PRIMARY KEY,
    video_url VARCHAR(500),
    is_processed BOOLEAN DEFAULT FALSE,
    download_retry_after TIMESTAMP DEFAULT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@pytest.fixture(scope="module")
def pg_conn():
    """Live Postgres connection, schema-isolated (issue #277). Skips when
    psycopg2 is unavailable or the DB is unreachable — one connection
    attempt, no retry loop."""
    reason = _skip_if_no_postgres()
    if reason:
        pytest.skip(reason)

    import psycopg2
    from psycopg2.extras import RealDictCursor

    dsn = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/test_airflow_dags",
    )
    try:
        conn = psycopg2.connect(
            dsn,
            cursor_factory=RealDictCursor,
            # Startup option only, for bootstrapping unqualified DDL — the
            # production method under test schema-qualifies every name
            # itself via PostgresConnection.get_qualified_table().
            options=f"-c search_path={_TEST_SCHEMA}",
            connect_timeout=5,
        )
        conn.autocommit = False
    except Exception as exc:
        pytest.skip(f"Postgres unavailable: {exc}")
        return

    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {_TEST_SCHEMA}")
        cur.execute(_SCHEMA_SQL)
    conn.commit()

    yield conn

    try:
        conn.rollback()  # a failed test may leave an aborted transaction
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def clean_table(pg_conn):
    """Committed TRUNCATE before/after each test — NOT savepoint rollback.

    record_source_integrity_failures() opens its OWN connection, so
    uncommitted rows would be invisible to it.
    """
    truncate = "TRUNCATE youtube_source_videos RESTART IDENTITY CASCADE"
    with pg_conn.cursor() as cur:
        cur.execute(truncate)
    pg_conn.commit()
    yield pg_conn
    with pg_conn.cursor() as cur:
        cur.execute(truncate)
    pg_conn.commit()


@pytest.fixture()
def db_env(monkeypatch):
    """Point CongressionalVideoDB's OWN PostgresConnection at the test schema.

    PostgresConnection reads POSTGRES_* from os.getenv at construction time,
    so each test constructs a fresh CongressionalVideoDB() after this fixture
    runs (postgres_helpers.py:20-26).
    """
    dsn = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/test_airflow_dags",
    )
    parsed = urlparse(dsn)
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname or "localhost")
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port or 5432))
    monkeypatch.setenv("POSTGRES_DB", (parsed.path or "/").lstrip("/"))
    monkeypatch.setenv("POSTGRES_USER", parsed.username or "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password or "postgres")
    monkeypatch.setenv("POSTGRES_SCHEMA", _TEST_SCHEMA)
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT_S", "5")


class TestRecordSourceIntegrityFailuresLive:
    """Authoritative row-level proof of per-item commit isolation on one
    shared connection, against real Postgres transaction semantics."""

    def test_earlier_commits_survive_a_later_failure(self, clean_table, db_env, mocker):
        """Spec: 'Earlier commit survives a later failure'.

        Seed 3 ids; force the underlying upsert to raise on the 3rd. The
        batch call re-raises, but ids 1-2 must remain committed and
        readable — the shared connection's per-item `with conn:` blocks
        must not roll back each other."""
        db = CongressionalVideoDB()
        original_upsert = db._upsert_source_integrity_failure

        def failing_third(cur, video_id, retry_after_hours):
            if video_id == "vid_3":
                raise RuntimeError("simulated failure on item 3")
            original_upsert(cur, video_id, retry_after_hours)

        mocker.patch.object(db, "_upsert_source_integrity_failure", side_effect=failing_third)

        with pytest.raises(RuntimeError):
            db.record_source_integrity_failures(["vid_1", "vid_2", "vid_3"], retry_after_hours=12)

        with clean_table.cursor() as cur:
            cur.execute("SELECT video_id, download_retry_after FROM youtube_source_videos ORDER BY video_id")
            rows = {row["video_id"]: row for row in cur.fetchall()}

        assert set(rows) == {"vid_1", "vid_2"}, "vid_3 must not exist — its upsert raised before executing"
        assert rows["vid_1"]["download_retry_after"] is not None
        assert rows["vid_2"]["download_retry_after"] is not None

    def test_one_connection_records_a_clean_batch(self, clean_table, db_env):
        """Spec: 'One connection for the batch' — a full successful batch of
        3 ids is committed and readable in one call."""
        db = CongressionalVideoDB()

        db.record_source_integrity_failures(["vid_a", "vid_b", "vid_c"], retry_after_hours=6)

        with clean_table.cursor() as cur:
            cur.execute("SELECT video_id FROM youtube_source_videos ORDER BY video_id")
            video_ids = {row["video_id"] for row in cur.fetchall()}

        assert video_ids == {"vid_a", "vid_b", "vid_c"}
