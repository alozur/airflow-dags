"""Tests for migration 029 — add interest_score column + update uploadable_turns view.

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_028.py pattern).

SQL integration tests (TestUploadableTurnsView.*) run against a real in-memory
PostgreSQL fixture — they verify the view semantics from the migration SQL directly.
If no live Postgres is available those tests are skipped via the postgres_conn fixture.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "029_add_turn_interest_score.sql"
)


# ---------------------------------------------------------------------------
# 3.11–3.15 helpers: live-DB integration (skipped when Postgres absent)
# ---------------------------------------------------------------------------


def _skip_if_no_postgres():
    """Return pytest.skip marker string; we use this as a guard inside fixtures."""
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "psycopg2 not installed"
    return None


# Minimal schema for view integration tests — mirrors production column set.
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS youtube_source_videos (
    video_id TEXT PRIMARY KEY,
    session_number INT DEFAULT 1,
    session_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS video_chapters (
    chapter_id SERIAL PRIMARY KEY,
    video_id TEXT REFERENCES youtube_source_videos(video_id),
    title TEXT DEFAULT 'chapter',
    description TEXT DEFAULT '',
    relevance_score INT DEFAULT 3,
    key_speakers TEXT[] DEFAULT '{}',
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS speaker_turns (
    turn_id SERIAL PRIMARY KEY,
    chapter_id INT REFERENCES video_chapters(chapter_id),
    resolved_name TEXT DEFAULT '',
    start_seconds NUMERIC DEFAULT 0,
    end_seconds NUMERIC DEFAULT 100,
    interest_score NUMERIC
);

CREATE TABLE IF NOT EXISTS speaker_turn_videos (
    turn_id INT PRIMARY KEY REFERENCES speaker_turns(turn_id),
    output_path TEXT DEFAULT '/tmp/video.mp4',
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    materialized_at TIMESTAMPTZ DEFAULT NOW()
);
"""

_VIEW_SQL = """
DROP VIEW IF EXISTS uploadable_turns;
CREATE VIEW uploadable_turns AS
SELECT * FROM (
    SELECT DISTINCT ON (stv.output_path)
        stv.turn_id,
        stv.output_path,
        st.chapter_id,
        st.resolved_name,
        st.start_seconds,
        st.end_seconds,
        st.interest_score,
        vc.video_id,
        vc.title AS chapter_title,
        vc.description,
        vc.relevance_score,
        vc.key_speakers,
        ysv.session_number,
        ysv.session_date,
        stv.materialized_at
    FROM speaker_turn_videos stv
    JOIN speaker_turns st ON stv.turn_id = st.turn_id
    JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
    JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
    WHERE stv.is_uploaded_to_youtube = FALSE
      AND vc.is_uploaded_to_youtube = FALSE
      AND vc.relevance_score >= 2
      AND COALESCE(st.interest_score, 1) >= 1
    ORDER BY stv.output_path, stv.turn_id
) dedup
ORDER BY COALESCE(dedup.interest_score, 1) DESC,
         dedup.relevance_score DESC, dedup.session_date DESC;
"""


_TEST_SCHEMA = "test_migration_029"


@pytest.fixture(scope="module")
def pg_conn():
    """Live Postgres connection for SQL integration tests.

    Uses TEST_DATABASE_URL env var or falls back to
    postgresql://postgres:postgres@localhost:5432/test_airflow_dags.
    Skips when psycopg2 is unavailable or DB is unreachable.

    Runs in its own Postgres schema (issue #277) so this file can share a
    throwaway database with other live-Postgres test files without
    column-not-found collisions from differing table shapes.
    """
    reason = _skip_if_no_postgres()
    if reason:
        pytest.skip(reason)

    import os

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
            # A startup option, not a per-session SET: it cannot be silently
            # unset by a ROLLBACK TO SAVEPOINT mid-run (issue #277).
            options=f"-c search_path={_TEST_SCHEMA}",
        )
        conn.autocommit = False
    except Exception as exc:
        pytest.skip(f"Postgres unavailable: {exc}")
        return

    # Bootstrap schema + view inside a savepoint so we can roll back after each test.
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {_TEST_SCHEMA}")
        cur.execute(_SCHEMA_SQL)
        cur.execute(_VIEW_SQL)
    conn.commit()

    yield conn

    try:
        conn.rollback()  # a failed test leaves an aborted transaction
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def db(pg_conn):
    """Per-test savepoint so each test starts with a clean state.

    Issued through a cursor: a psycopg2 connection has no `.execute` (that is
    psycopg3-only API — issue #276).
    """
    with pg_conn.cursor() as cur:
        cur.execute("SAVEPOINT test_sp")
    yield pg_conn
    with pg_conn.cursor() as cur:
        cur.execute("ROLLBACK TO SAVEPOINT test_sp")


def _insert_minimal_turn(
    db,
    *,
    turn_id: int,
    interest_score=None,
    relevance_score: int = 3,
    session_date="2026-01-01",
    output_path: str | None = None,
):
    """Helper: insert a minimal video/chapter/turn/stv row set."""
    video_id = f"vid_{turn_id}"
    chapter_id_val = turn_id * 100
    output_path = output_path or f"/tmp/{turn_id}.mp4"

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO youtube_source_videos (video_id, session_date) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (video_id, session_date),
        )
        cur.execute(
            (
                "INSERT INTO video_chapters (chapter_id, video_id, relevance_score) VALUES (%s, %s, %s) "
                "ON CONFLICT DO NOTHING"
            ),
            (chapter_id_val, video_id, relevance_score),
        )
        cur.execute(
            (
                "INSERT INTO speaker_turns (turn_id, chapter_id, interest_score) VALUES (%s, %s, %s) "
                "ON CONFLICT DO NOTHING"
            ),
            (turn_id, chapter_id_val, interest_score),
        )
        cur.execute(
            "INSERT INTO speaker_turn_videos (turn_id, output_path) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (turn_id, output_path),
        )


# ---------------------------------------------------------------------------
# Static file checks
# ---------------------------------------------------------------------------


class TestMigration029FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration029ColumnAndView:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_interest_score_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS interest_score NUMERIC."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS INTEREST_SCORE" in sql

    def test_creates_uploadable_turns_view(self):
        """Migration must CREATE VIEW uploadable_turns."""
        sql = self._sql().upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in sql

    def test_drops_view_before_create(self):
        """Migration must DROP VIEW IF EXISTS uploadable_turns before creating."""
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_contains_coalesce_interest_score_filter(self):
        """View WHERE clause must include COALESCE(st.interest_score, 1) >= 1."""
        sql = self._sql().upper()
        assert re.search(r"COALESCE\s*\(\s*ST\.INTEREST_SCORE\s*,\s*1\s*\)\s*>=\s*1", sql), (
            "View must contain COALESCE(st.interest_score, 1) >= 1 in WHERE clause"
        )

    def test_outer_order_by_coalesce_interest_score_desc(self):
        """Outer ORDER BY must lead with COALESCE(dedup.interest_score, 1) DESC."""
        sql = self._sql().upper()
        assert re.search(
            r"ORDER\s+BY\s+COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC",
            sql,
        ), "Outer ORDER BY must begin with COALESCE(dedup.interest_score, 1) DESC"

    def test_outer_order_by_includes_relevance_then_date(self):
        """Outer ORDER BY must include relevance_score DESC then session_date DESC."""
        sql = self._sql().upper()
        assert re.search(r"RELEVANCE_SCORE\s+DESC", sql), "Must include relevance_score DESC"
        assert re.search(r"SESSION_DATE\s+DESC", sql), "Must include session_date DESC"

    def test_interest_score_selected_in_view(self):
        """View SELECT must expose st.interest_score."""
        sql = self._sql().upper()
        assert "ST.INTEREST_SCORE" in sql

    def test_no_schema_qualification(self):
        """Migration must not use public.- or production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        """Migration must include a DOWN block comment with rollback instructions."""
        sql = self._sql().upper()
        assert "DOWN" in sql

    def test_header_comment_names_constant(self):
        """029 SQL header comment must name INTEREST_FILTER_THRESHOLD constant."""
        sql = self._sql()
        assert "INTEREST_FILTER_THRESHOLD" in sql


# ---------------------------------------------------------------------------
# 3.11 SQL integration: low score excluded
# ---------------------------------------------------------------------------


class TestUploadableTurnsView:
    def test_low_score_excluded(self, db):
        """Turn with interest_score=0 (threshold=1) must be absent from view."""
        _insert_minimal_turn(db, turn_id=311, interest_score=0)
        with db.cursor() as cur:
            cur.execute("SELECT turn_id FROM uploadable_turns")
            ids = {r["turn_id"] for r in cur.fetchall()}
        assert 311 not in ids, "interest_score=0 turn must be soft-excluded from view"

    def test_null_score_passes_filter(self, db):
        """Turn with interest_score=NULL must appear in view (COALESCE rescue)."""
        _insert_minimal_turn(db, turn_id=312, interest_score=None)
        with db.cursor() as cur:
            cur.execute("SELECT turn_id FROM uploadable_turns")
            ids = {r["turn_id"] for r in cur.fetchall()}
        assert 312 in ids, "NULL interest_score must pass COALESCE filter"

    def test_interest_score_primary_ordering(self, db):
        """Chapter A turn interest=8/relevance=2 must outrank chapter B interest=6/relevance=5."""
        _insert_minimal_turn(db, turn_id=313, interest_score=8, relevance_score=2)
        _insert_minimal_turn(db, turn_id=314, interest_score=6, relevance_score=5)
        with db.cursor() as cur:
            cur.execute("SELECT turn_id FROM uploadable_turns")
            rows = cur.fetchall()
        ids = [r["turn_id"] for r in rows]
        assert ids.index(313) < ids.index(314), (
            f"interest=8 (turn 313) must precede interest=6 (turn 314); got order {ids}"
        )

    def test_null_scores_sort_last(self, db):
        """Turn X interest=3 must precede turn Y interest=NULL; Y still present."""
        _insert_minimal_turn(db, turn_id=315, interest_score=3, output_path="/tmp/315.mp4")
        _insert_minimal_turn(db, turn_id=316, interest_score=None, output_path="/tmp/316.mp4")
        with db.cursor() as cur:
            cur.execute("SELECT turn_id FROM uploadable_turns")
            rows = cur.fetchall()
        ids = [r["turn_id"] for r in rows]
        assert 316 in ids, "NULL-scored turn must still be present"
        assert ids.index(315) < ids.index(316), f"interest=3 (315) must precede interest=NULL (316); got order {ids}"

    def test_tie_break_by_relevance_then_date(self, db):
        """Two turns with interest=7; higher relevance appears first; date breaks ties."""
        _insert_minimal_turn(
            db, turn_id=317, interest_score=7, relevance_score=4, session_date="2026-01-01", output_path="/tmp/317.mp4"
        )
        _insert_minimal_turn(
            db, turn_id=318, interest_score=7, relevance_score=2, session_date="2026-01-02", output_path="/tmp/318.mp4"
        )
        with db.cursor() as cur:
            cur.execute("SELECT turn_id FROM uploadable_turns")
            rows = cur.fetchall()
        ids = [r["turn_id"] for r in rows]
        assert ids.index(317) < ids.index(318), (
            f"relevance=4 (317) must precede relevance=2 (318) when interest ties; got {ids}"
        )
