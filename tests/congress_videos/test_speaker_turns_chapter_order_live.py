"""Live-Postgres tests for select_chapters()'s cron-branch ordering (issue #300).

Authoritative for bucket ordering semantics — mocked string-assertion tests in
test_speaker_turns_dag.py are a cheap CI smoke check only and cannot catch a
swapped operator or an inverted CASE branch.

Runs in its own Postgres schema (issue #277) to share a throwaway database
with other live-Postgres test files without column-shape collisions. Skips
cleanly when psycopg2 is unavailable or Postgres is unreachable.

Run with (nushell):
    with-env {TEST_DATABASE_URL: "postgresql://<role>:<pw>@host:port/db"} {
        uv run pytest tests/congress_videos/test_speaker_turns_chapter_order_live.py -o addopts= -v
    }

`-o addopts=` is mandatory: the project's default addopts carries
--cov-fail-under=80, which fails any single-file run regardless of results.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlparse

import pytest

import congress_videos.speaker_turns_dag as mod

_TEST_SCHEMA = "test_speaker_turns_order"
MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[2] / "congress_videos" / "sql" / "migrations"
)
_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+uploadable_chapters\b.*?;",
    re.IGNORECASE | re.DOTALL,
)


def _skip_if_no_postgres():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "psycopg2 not installed"
    return None


def _extract_view_ddl() -> str:
    """Extract the real uploadable_chapters view DDL from the migrations.

    Takes the highest-numbered migration file (sorted by filename) whose text
    matches ``CREATE (OR REPLACE )?VIEW uploadable_chapters``, sliced through
    its terminating ``;``. Never hand-copied: the view is load-bearing
    (``relevance_score >= 2``, ``is_upload_abandoned = FALSE`` gate
    eligibility; created_at/session_date/relevance_score exposure is the
    whole premise of this test file).
    """
    ddl = None
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = _VIEW_RE.search(path.read_text(encoding="utf-8"))
        if match:
            ddl = match.group(0)  # ascending sort — last match wins == highest-numbered
    assert ddl, "No migration defines CREATE (OR REPLACE) VIEW uploadable_chapters"
    assert ddl.strip().upper().startswith("CREATE")
    assert ddl.strip().endswith(";")
    return ddl


# Minimal hand-scaffolded table shapes — inert scaffolding, safe to hand-write.
# The VIEW itself is never hand-copied here (see _extract_view_ddl above).
_SCHEMA_SQL = """
CREATE TABLE youtube_source_videos (
    video_id VARCHAR(50) PRIMARY KEY,
    video_title VARCHAR(500),
    session_number INTEGER,
    session_date DATE
);

CREATE TABLE video_chapters (
    chapter_id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) REFERENCES youtube_source_videos(video_id),
    title TEXT DEFAULT 'chapter',
    description TEXT,
    start_time VARCHAR(20) DEFAULT '00:00:00,000',
    end_time VARCHAR(20) DEFAULT '00:10:00,000',
    duration_minutes NUMERIC(10, 2) DEFAULT 10,
    speakers TEXT[] DEFAULT '{}',
    topics TEXT[] DEFAULT '{}',
    timeline JSONB DEFAULT '[]'::jsonb,
    relevance_score INTEGER DEFAULT 2,
    speaker_relevance_points INTEGER DEFAULT 1,
    topic_relevance_points INTEGER DEFAULT 1,
    public_interest_points INTEGER DEFAULT 0,
    scoring_reasoning TEXT,
    key_speakers TEXT[] DEFAULT '{}',
    is_current_topic BOOLEAN DEFAULT FALSE,
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    is_upload_abandoned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_participant_slug TEXT,
    turns_detected_at TIMESTAMPTZ
);

CREATE TABLE speaker_turns (
    turn_id SERIAL PRIMARY KEY,
    chapter_id INTEGER REFERENCES video_chapters(chapter_id)
);
"""


@pytest.fixture(scope="module")
def pg_conn():
    """Live Postgres connection, schema-isolated (issue #277). Skips when
    psycopg2 is unavailable or the DB is unreachable."""
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
            # Startup option only, for bootstrapping unqualified DDL —
            # select_chapters()'s own connection schema-qualifies every name.
            options=f"-c search_path={_TEST_SCHEMA}",
            connect_timeout=5,
        )
        conn.autocommit = False
    except Exception as exc:
        pytest.skip(f"Postgres unavailable: {exc}")
        return

    view_ddl = _extract_view_ddl()
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {_TEST_SCHEMA}")
        cur.execute(_SCHEMA_SQL)
        cur.execute(view_ddl)
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
def clean_tables(pg_conn):
    """Committed TRUNCATE before/after each test — NOT savepoint rollback.

    select_chapters() opens its OWN connection (see select_chapters_env), so
    it can never see rows this fixture left uncommitted.
    """
    truncate = (
        "TRUNCATE video_chapters, youtube_source_videos, speaker_turns "
        "RESTART IDENTITY CASCADE"
    )
    with pg_conn.cursor() as cur:
        cur.execute(truncate)
    pg_conn.commit()
    yield pg_conn
    with pg_conn.cursor() as cur:
        cur.execute(truncate)
    pg_conn.commit()


@pytest.fixture()
def select_chapters_env(monkeypatch):
    """Point select_chapters()'s OWN PostgresConnection at the test schema.

    PostgresConnection reads POSTGRES_* from os.getenv at construction time
    and select_chapters() constructs one per call — env vars are the seam.
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
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password or "")
    monkeypatch.setenv("POSTGRES_SCHEMA", _TEST_SCHEMA)
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT_S", "5")


def _seed_chapter(
    conn,
    *,
    chapter_id: int,
    video_id: str,
    session_date: str | None,
    relevance_score: int,
    created_at_days_ago: float,
    is_upload_abandoned: bool = False,
    turns_detected_at: str | None = None,
) -> None:
    """Seed one eligible chapter with a RELATIVE created_at interval — never
    an absolute Python datetime (BR4: the recency boundary is wall-clock
    relative to Postgres's own NOW(), evaluated per query)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO youtube_source_videos (video_id, video_title, session_date) "
            "VALUES (%s, %s, %s)",
            (video_id, f"video {video_id}", session_date),
        )
        cur.execute(
            "INSERT INTO video_chapters ("
            "    chapter_id, video_id, relevance_score, is_upload_abandoned,"
            "    turns_detected_at, created_at"
            ") VALUES (%s, %s, %s, %s, %s, NOW() - (%s * INTERVAL '1 day'))",
            (
                chapter_id, video_id, relevance_score, is_upload_abandoned,
                turns_detected_at, created_at_days_ago,
            ),
        )
    conn.commit()


class TestChapterOrderLive:
    """Authoritative bucket-ordering tests against a real Postgres view."""

    def test_all_old_bucket_orders_by_session_date_then_relevance(
        self, clean_tables, select_chapters_env
    ):
        """AC1: 5 chapters, all older than the 7-day window, all
        relevance_score=2 (today's live production shape) — degrades to
        session_date DESC NULLS LAST."""
        dates = {1: "2026-06-01", 2: "2026-06-05", 3: "2026-06-03",
                 4: "2026-06-10", 5: "2026-06-02"}
        for chapter_id, session_date in dates.items():
            _seed_chapter(
                clean_tables, chapter_id=chapter_id, video_id=f"vid_{chapter_id}",
                session_date=session_date, relevance_score=2,
                created_at_days_ago=8 + chapter_id,
            )

        rows = mod.select_chapters(limit=10)

        assert [r["chapter_id"] for r in rows] == [4, 2, 3, 5, 1]

    def test_recency_outranks_relevance(self, clean_tables, select_chapters_env):
        """AC2: a recent low-relevance chapter outranks an old high-relevance one."""
        _seed_chapter(clean_tables, chapter_id=1, video_id="vid_1",
                      session_date="2026-01-01", relevance_score=2, created_at_days_ago=1)
        _seed_chapter(clean_tables, chapter_id=2, video_id="vid_2",
                      session_date="2026-06-01", relevance_score=5, created_at_days_ago=10)

        rows = mod.select_chapters(limit=10)

        ids = [r["chapter_id"] for r in rows]
        assert ids.index(1) < ids.index(2)

    def test_recent_bucket_orders_by_relevance_only(self, clean_tables, select_chapters_env):
        """AC3: within the recent bucket, session_date must NOT influence order."""
        _seed_chapter(clean_tables, chapter_id=1, video_id="vid_1",
                      session_date="2026-01-01", relevance_score=5, created_at_days_ago=1)
        _seed_chapter(  # later session_date, lower relevance — must still lose
            clean_tables, chapter_id=2, video_id="vid_2",
            session_date="2026-06-01", relevance_score=2, created_at_days_ago=2,
        )

        rows = mod.select_chapters(limit=10)

        ids = [r["chapter_id"] for r in rows]
        assert ids.index(1) < ids.index(2)

    def test_old_bucket_null_session_date_sorts_last(self, clean_tables, select_chapters_env):
        """AC4: NULL session_date sorts after a non-NULL one in the old bucket."""
        _seed_chapter(clean_tables, chapter_id=1, video_id="vid_1",
                      session_date=None, relevance_score=2, created_at_days_ago=10)
        _seed_chapter(clean_tables, chapter_id=2, video_id="vid_2",
                      session_date="2026-01-01", relevance_score=2, created_at_days_ago=10)

        rows = mod.select_chapters(limit=10)

        ids = [r["chapter_id"] for r in rows]
        assert ids.index(2) < ids.index(1)

    def test_old_bucket_relevance_breaks_session_date_tie(
        self, clean_tables, select_chapters_env
    ):
        """Same session_date, higher relevance_score wins within the old bucket."""
        _seed_chapter(clean_tables, chapter_id=1, video_id="vid_1",
                      session_date="2026-01-01", relevance_score=5, created_at_days_ago=10)
        _seed_chapter(clean_tables, chapter_id=2, video_id="vid_2",
                      session_date="2026-01-01", relevance_score=2, created_at_days_ago=10)

        rows = mod.select_chapters(limit=10)

        ids = [r["chapter_id"] for r in rows]
        assert ids.index(1) < ids.index(2)

    def test_limit_selects_true_top_n_winner(self, clean_tables, select_chapters_env):
        """AC5: limit=1 over a mixed pool returns the true bucket-order winner
        (chapter 1: recent bucket, relevance_score=3). An inverted bucket
        predicate or CASE branch — see the mutation check — surfaces an
        old-bucket chapter here instead."""
        _seed_chapter(clean_tables, chapter_id=1, video_id="vid_1",
                      session_date="2026-01-01", relevance_score=3, created_at_days_ago=1)
        _seed_chapter(clean_tables, chapter_id=2, video_id="vid_2",
                      session_date="2026-06-01", relevance_score=1, created_at_days_ago=2)
        _seed_chapter(clean_tables, chapter_id=3, video_id="vid_3",
                      session_date="2026-12-01", relevance_score=5, created_at_days_ago=30)
        _seed_chapter(clean_tables, chapter_id=4, video_id="vid_4",
                      session_date="2026-11-01", relevance_score=5, created_at_days_ago=30)

        rows = mod.select_chapters(limit=1)

        assert [r["chapter_id"] for r in rows] == [1]

    def test_pool_unchanged_only_resequenced(self, clean_tables, select_chapters_env):
        """AC7: already-attempted chapters stay excluded regardless of recency,
        session_date, or relevance_score — only the ORDER BY changed."""
        _seed_chapter(  # non-NULL turns_detected_at excludes it despite being
            clean_tables, chapter_id=1, video_id="vid_1", session_date="2026-01-01",
            relevance_score=5, created_at_days_ago=1, turns_detected_at="2026-08-01",
        )
        _seed_chapter(clean_tables, chapter_id=2, video_id="vid_2",
                      session_date="2026-02-01", relevance_score=2, created_at_days_ago=10)
        with clean_tables.cursor() as cur:  # existing speaker_turns row excludes it
            cur.execute("INSERT INTO speaker_turns (chapter_id) VALUES (%s)", (2,))
        clean_tables.commit()

        rows = mod.select_chapters(limit=10)

        assert rows == []
