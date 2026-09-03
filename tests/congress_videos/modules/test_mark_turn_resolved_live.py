"""Live-Postgres tests for `mark_turn_resolved` and `select_unprepared_turns`
(issue #338).

Closes two test-quality WARNINGs from the speaker-resolution-propagation-guard
verify-report (issue #321/#322): the sibling-propagation write and the
retry/skip-eligibility read were previously proven only by mocks or by
SQL-shape assertions. Both are exercised here as real row-level UPDATEs and
SELECTs against seeded rows in an isolated Postgres schema.

Runs in its own schema (issue #277) so it can share a throwaway database with
other live-Postgres test files without column-shape collisions. Skips cleanly
when psycopg2 is unavailable or Postgres is unreachable.

Run with (nushell):
    with-env {TEST_DATABASE_URL: "postgresql://<role>:<pw>@host:port/db"} {
        uv run pytest tests/congress_videos/modules/test_mark_turn_resolved_live.py -o addopts= -v
    }

`-o addopts=` is mandatory: the project's default addopts carries
--cov-fail-under=80, which fails any single-file run regardless of results.

Residual limitation: this file proves row state as read back by production
SQL (`mark_turn_resolved`, `select_unprepared_turns`). The DAG's in-process
`already_resolved` boolean (`speaker_turn_prepare_dag.py:274-278`) stays
covered separately by its own mocked unit test — collapsing that composition
into one live path would require a production extraction, which is out of
scope for this change (test-only, zero production edits).
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import pytest

from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.speaker_resolution import SPEAKER_RESOLUTION_MIN_CONFIDENCE

_TEST_SCHEMA = "test_mark_turn_resolved"


def _skip_if_no_postgres():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "psycopg2 not installed"
    return None


# Minimal hand-scaffolded table shapes — every column read by mark_turn_resolved
# (database.py:1327-1382) and select_unprepared_turns (database.py:1225-1285).
_SCHEMA_SQL = """
CREATE TABLE youtube_source_videos (
    video_id VARCHAR(50) PRIMARY KEY,
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
    relevance_score INTEGER DEFAULT 2,
    key_speakers TEXT[] DEFAULT '{}',
    speakers TEXT[] DEFAULT '{}',
    is_uploaded_to_youtube BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE speaker_turns (
    turn_id SERIAL PRIMARY KEY,
    chapter_id INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    start_seconds NUMERIC NOT NULL DEFAULT 0,
    end_seconds NUMERIC NOT NULL DEFAULT 600,
    speaker_label TEXT NOT NULL,
    resolved_name TEXT,
    confidence NUMERIC NOT NULL DEFAULT 0.9 CHECK (confidence BETWEEN 0 AND 1),
    source TEXT NOT NULL DEFAULT 'acoustic'
        CHECK (source IN ('acoustic', 'text_confirmed', 'text_named')),
    interest_score NUMERIC,
    is_procedural BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (chapter_id, start_seconds)
);

CREATE TABLE speaker_turn_videos (
    video_id SERIAL PRIMARY KEY,
    turn_id INTEGER NOT NULL REFERENCES speaker_turns(turn_id) ON DELETE CASCADE,
    output_path TEXT NOT NULL,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_uploaded_to_youtube BOOLEAN NOT NULL DEFAULT FALSE,
    prepared_at TIMESTAMPTZ,
    turn_type TEXT NOT NULL DEFAULT 'monologue',
    keep_intervals JSONB,
    resolved_participant_slug TEXT,
    speaker_resolution_confidence FLOAT,
    speaker_resolution_method TEXT CHECK (
        speaker_resolution_method IN ('ai_srt_context', 'fuzzy', 'manual')
        OR speaker_resolution_method IS NULL
    ),
    CONSTRAINT uq_speaker_turn_videos_turn UNIQUE (turn_id)
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
            # the production methods under test schema-qualify every name
            # themselves via PostgresConnection.get_qualified_table().
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
def clean_tables(pg_conn):
    """Committed TRUNCATE before/after each test — NOT savepoint rollback.

    mark_turn_resolved() and select_unprepared_turns() each open their OWN
    connection (database.py:1360, 1243), so uncommitted rows would be
    invisible to them.
    """
    truncate = (
        "TRUNCATE speaker_turn_videos, speaker_turns, video_chapters, youtube_source_videos RESTART IDENTITY CASCADE"
    )
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

    Gotcha: PostgresConnection.__init__ rejects a falsy POSTGRES_PASSWORD
    with ValueError — the DSN must always carry a password.
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


def _already_resolved(row: dict) -> bool:
    """Mirrors speaker_turn_prepare_dag.py:274-278's Gate-A skip predicate,
    using the same SPEAKER_RESOLUTION_MIN_CONFIDENCE constant the DAG gate
    consumes — zero drift risk."""
    return bool(
        row.get("resolved_participant_slug")
        and float(row.get("speaker_resolution_confidence") or 0) >= SPEAKER_RESOLUTION_MIN_CONFIDENCE
    )


def _seed_turn(
    conn,
    *,
    turn_id: int,
    chapter_id: int,
    video_id: str,
    speaker_label: str,
    output_path: str,
) -> None:
    """Seed one speaker_turns + speaker_turn_videos row, creating the parent
    youtube_source_videos/video_chapters rows on first use per chapter_id.
    start_seconds defaults to turn_id so multiple turns sharing a chapter
    never collide on the (chapter_id, start_seconds) UNIQUE constraint."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO youtube_source_videos (video_id) VALUES (%s) ON CONFLICT (video_id) DO NOTHING",
            (video_id,),
        )
        cur.execute(
            "INSERT INTO video_chapters (chapter_id, video_id) VALUES (%s, %s) ON CONFLICT (chapter_id) DO NOTHING",
            (chapter_id, video_id),
        )
        cur.execute(
            "INSERT INTO speaker_turns (turn_id, chapter_id, speaker_label, start_seconds) VALUES (%s, %s, %s, %s)",
            (turn_id, chapter_id, speaker_label, turn_id),
        )
        cur.execute(
            "INSERT INTO speaker_turn_videos (turn_id, output_path) VALUES (%s, %s)",
            (turn_id, output_path),
        )
    conn.commit()


class TestMarkTurnResolvedLive:
    """Authoritative row-level tests against real mark_turn_resolved /
    select_unprepared_turns SQL."""

    def test_mark_turn_resolved_propagates_to_same_label_siblings_only(self, clean_tables, db_env):
        """Spec: 'Same-label siblings propagate, mismatched sibling withheld'.

        Group /data/g1.mp4: turns 101 (representative) and 102 share
        SPEAKER_00; turn 103 is SPEAKER_01. Only the SPEAKER_00 rows may
        receive the resolved slug — 103 must stay untouched."""
        output_path = "/data/g1.mp4"
        _seed_turn(
            clean_tables,
            turn_id=101,
            chapter_id=1,
            video_id="vid_g1",
            speaker_label="SPEAKER_00",
            output_path=output_path,
        )
        _seed_turn(
            clean_tables,
            turn_id=102,
            chapter_id=1,
            video_id="vid_g1",
            speaker_label="SPEAKER_00",
            output_path=output_path,
        )
        _seed_turn(
            clean_tables,
            turn_id=103,
            chapter_id=1,
            video_id="vid_g1",
            speaker_label="SPEAKER_01",
            output_path=output_path,
        )

        db = CongressionalVideoDB()
        db.mark_turn_resolved(output_path, "pedro-sanchez", 0.95, "ai_srt_context", 101)

        with clean_tables.cursor() as cur:
            cur.execute(
                "SELECT turn_id, resolved_participant_slug, "
                "speaker_resolution_confidence, speaker_resolution_method "
                "FROM speaker_turn_videos ORDER BY turn_id"
            )
            rows = {row["turn_id"]: row for row in cur.fetchall()}

        for turn_id in (101, 102):
            assert rows[turn_id]["resolved_participant_slug"] == "pedro-sanchez"
            assert rows[turn_id]["speaker_resolution_confidence"] == pytest.approx(0.95)
            assert rows[turn_id]["speaker_resolution_method"] == "ai_srt_context"

        assert rows[103]["resolved_participant_slug"] is None
        assert rows[103]["speaker_resolution_confidence"] is None
        assert rows[103]["speaker_resolution_method"] is None

    def test_select_unprepared_turns_distinguishes_retry_and_skip_eligible_rows(self, clean_tables, db_env):
        """Spec: 'Withheld groups resurface via the production selector' +
        'Skip-eligible turn is distinguished from retry-eligible turns'.

        G2 /data/g2.mp4: 201 SPEAKER_01 (lower turn_id, surfaced by
        DISTINCT ON ... ORDER BY turn_id), 202 SPEAKER_00 (Gate-A rep) — the
        resolution write lands only on 202, so the surfaced row (201) stays
        NULL: retry-eligible.
        G3 /data/g3.mp4: 301 SPEAKER_00, no write at all (Gate B withheld):
        retry-eligible.
        G4 /data/g4.mp4: 401 SPEAKER_00, Gate-A rep=401, confidence 0.95:
        skip-eligible.
        """
        _seed_turn(
            clean_tables,
            turn_id=201,
            chapter_id=2,
            video_id="vid_g2",
            speaker_label="SPEAKER_01",
            output_path="/data/g2.mp4",
        )
        _seed_turn(
            clean_tables,
            turn_id=202,
            chapter_id=2,
            video_id="vid_g2",
            speaker_label="SPEAKER_00",
            output_path="/data/g2.mp4",
        )
        _seed_turn(
            clean_tables,
            turn_id=301,
            chapter_id=3,
            video_id="vid_g3",
            speaker_label="SPEAKER_00",
            output_path="/data/g3.mp4",
        )
        _seed_turn(
            clean_tables,
            turn_id=401,
            chapter_id=4,
            video_id="vid_g4",
            speaker_label="SPEAKER_00",
            output_path="/data/g4.mp4",
        )

        db = CongressionalVideoDB()
        db.mark_turn_resolved("/data/g2.mp4", "someone-else", 0.90, "ai_srt_context", 202)
        db.mark_turn_resolved("/data/g4.mp4", "pedro-sanchez", 0.95, "ai_srt_context", 401)

        rows = db.select_unprepared_turns(limit=10)

        # Anti-vacuity guard FIRST: a filter mismatch that empties the result
        # set must fail this assertion rather than passing vacuously.
        assert len(rows) == 3
        assert {row["output_path"] for row in rows} == {
            "/data/g2.mp4",
            "/data/g3.mp4",
            "/data/g4.mp4",
        }

        by_path = {row["output_path"]: row for row in rows}

        g2_row = by_path["/data/g2.mp4"]
        assert g2_row["resolved_participant_slug"] is None
        assert _already_resolved(g2_row) is False

        g3_row = by_path["/data/g3.mp4"]
        assert g3_row["resolved_participant_slug"] is None
        assert _already_resolved(g3_row) is False

        g4_row = by_path["/data/g4.mp4"]
        assert g4_row["resolved_participant_slug"] == "pedro-sanchez"
        assert g4_row["speaker_resolution_confidence"] >= SPEAKER_RESOLUTION_MIN_CONFIDENCE
        assert _already_resolved(g4_row) is True
