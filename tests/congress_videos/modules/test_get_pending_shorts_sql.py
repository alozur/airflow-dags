"""Live-Postgres integration tests for `pending_shorts_candidate_sql`.

These tests execute the real candidate query from
`congress_videos.modules.database.pending_shorts_candidate_sql` against a
disposable Postgres schema, proving the per-chapter Tier-1 cap behaves
correctly on an actual query plan rather than only on mocked cursor calls.

Fixture shape mirrors `tests/congress_videos/sql/test_migration_029.py`, with
one deliberate fix: that file's per-test `db` fixture calls
`pg_conn.execute(...)`, which does not exist on a psycopg2 connection (that is
psycopg3 API; this repo pins `psycopg2-binary`). Those tests have therefore
never actually executed a savepoint. Here we use `pg_conn.cursor()` for both
the SAVEPOINT and the ROLLBACK TO SAVEPOINT statements.

Skips cleanly (not fails) when psycopg2 is unavailable or no local Postgres is
reachable at TEST_DATABASE_URL / the default DSN below.
"""

from __future__ import annotations

import os

import pytest

from congress_videos.modules.database import (
    SHORTS_PENDING_CANDIDATE_LIMIT,
    SHORTS_TIER1_PER_CHAPTER_LIMIT,
    pending_shorts_candidate_sql,
)

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _skip_if_no_postgres():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "psycopg2 not installed"
    return None


# Minimal schema mirroring the production video_chapters / video_shorts
# columns this query touches (see migrations 004 and 012).
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS video_chapters (
    chapter_id SERIAL PRIMARY KEY,
    video_id TEXT NOT NULL DEFAULT 'vid',
    youtube_upload_date TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS video_shorts (
    id SERIAL PRIMARY KEY,
    chapter_id INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    reap_status VARCHAR(50) NOT NULL DEFAULT 'downloaded',
    reap_virality_score FLOAT,
    local_file_path VARCHAR(2048) DEFAULT '/tmp/clip.mp4',
    is_uploaded BOOLEAN NOT NULL DEFAULT FALSE,
    is_upload_abandoned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"""


_TEST_SCHEMA = "test_get_pending_shorts_sql"


@pytest.fixture(scope="module")
def pg_conn():
    """Live Postgres connection for SQL integration tests.

    Uses TEST_DATABASE_URL env var or falls back to
    postgresql://postgres:postgres@localhost:5432/test_airflow_dags.
    Skips when psycopg2 is unavailable or the DB is unreachable.

    Runs in its own Postgres schema (issue #277) so this file can share a
    throwaway database with other live-Postgres test files without
    column-not-found collisions from differing table shapes.
    """
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
            # A startup option, not a per-session SET: it cannot be silently
            # unset by a ROLLBACK TO SAVEPOINT mid-run (issue #277).
            options=f"-c search_path={_TEST_SCHEMA}",
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
        conn.rollback()  # a failed test leaves an aborted transaction
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def db(pg_conn):
    """Per-test savepoint so each test starts with a clean state.

    Deviates from test_migration_029.py's broken `pg_conn.execute(...)` calls
    (psycopg3-only API) by using a cursor explicitly, matching psycopg2.
    """
    with pg_conn.cursor() as cur:
        cur.execute("SAVEPOINT test_sp")
    yield pg_conn
    with pg_conn.cursor() as cur:
        cur.execute("ROLLBACK TO SAVEPOINT test_sp")


# --------------------------------------------------------------------------- #
# Insert helpers
# --------------------------------------------------------------------------- #


def _insert_chapter(cur, *, video_id: str = "vid", youtube_upload_date: str = "2026-01-01") -> int:
    cur.execute(
        "INSERT INTO video_chapters (video_id, youtube_upload_date) VALUES (%s, %s) RETURNING chapter_id",
        (video_id, youtube_upload_date),
    )
    return cur.fetchone()["chapter_id"]


def _insert_clip(
    cur,
    chapter_id: int,
    *,
    virality_score: float | None = None,
    is_uploaded: bool = False,
    is_upload_abandoned: bool = False,
    local_file_path: str | None = "/tmp/x.mp4",
    reap_status: str = "downloaded",
) -> int:
    cur.execute(
        """
        INSERT INTO video_shorts
            (chapter_id, reap_virality_score, is_uploaded, is_upload_abandoned,
             local_file_path, reap_status)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (chapter_id, virality_score, is_uploaded, is_upload_abandoned, local_file_path, reap_status),
    )
    return cur.fetchone()["id"]


def _run_candidate_query(cur, min_virality_score: float = 0.0):
    sql = pending_shorts_candidate_sql("video_shorts", "video_chapters")
    cur.execute(
        sql,
        (SHORTS_TIER1_PER_CHAPTER_LIMIT, min_virality_score, SHORTS_PENDING_CANDIDATE_LIMIT),
    )
    return cur.fetchall()


# --------------------------------------------------------------------------- #
# R4 — highest-value guard: ranking universe includes already-uploaded clips
# --------------------------------------------------------------------------- #


def test_uploaded_top_clips_consume_chapter_tier1_slots(db):
    """The chapter's top-3 scored clips are already uploaded (is_uploaded=TRUE).
    They still occupy Tier-1 ranks 1-3 in the ranking universe, so none of the
    still-pending clips returned by the outer query may be Tier 1, and every
    returned row must have chapter_rank >= 4.

    This is the D1/R4 regression guard: if `is_uploaded = FALSE` is ever moved
    into the ranking CTE, the top-3 pending clips would re-rank into Tier 1
    every run, making the cap inert.
    """
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        scores = [9.0, 8.5, 8.0, 7.5, 7.0, 6.5, 6.0, 5.5, 5.0, 4.5, 4.0, 3.5]
        for index, score in enumerate(scores):
            _insert_clip(cur, chapter_id, virality_score=score, is_uploaded=(index < 3))

        rows = _run_candidate_query(cur)

    chapter_rows = [r for r in rows if r["chapter_id"] == chapter_id]
    assert chapter_rows, "expected pending rows for this chapter"
    assert all(r["tier"] != 1 for r in chapter_rows)
    assert all(r["chapter_rank"] >= 4 for r in chapter_rows)


# --------------------------------------------------------------------------- #
# R1 — tier beats cross-chapter recency
# --------------------------------------------------------------------------- #


def test_tier_beats_recency_across_chapters(db):
    """Two chapters with different youtube_upload_date values, both with
    Tier-1 and Tier-2 candidates. Every returned Tier-2 row must sort after
    every returned Tier-1 row, regardless of chapter recency."""
    with db.cursor() as cur:
        chapter_old = _insert_chapter(cur, video_id="old", youtube_upload_date="2020-01-01")
        chapter_new = _insert_chapter(cur, video_id="new", youtube_upload_date="2026-01-01")

        for score in [9.0, 8.0, 7.0, 6.0, 5.0]:
            _insert_clip(cur, chapter_old, virality_score=score)
        for score in [9.0, 8.0, 7.0, 6.0, 5.0]:
            _insert_clip(cur, chapter_new, virality_score=score)

        rows = _run_candidate_query(cur)

    tiers = [r["tier"] for r in rows]
    assert tiers, "expected candidate rows"
    assert tiers == sorted(tiers), f"tier-2 row sorted before a tier-1 row: {tiers}"


# --------------------------------------------------------------------------- #
# R8 — min_virality_score never changes tier assignment
# --------------------------------------------------------------------------- #


def test_min_virality_score_does_not_change_tier(db):
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        for score in [9.0, 8.0, 7.0, 1.0, 0.5]:
            _insert_clip(cur, chapter_id, virality_score=score)

        low_threshold_rows = {r["id"]: r["tier"] for r in _run_candidate_query(cur, min_virality_score=0.0)}
        high_threshold_rows = {r["id"]: r["tier"] for r in _run_candidate_query(cur, min_virality_score=5.0)}

    shared_ids = set(low_threshold_rows) & set(high_threshold_rows)
    assert shared_ids, "expected at least one row present at both thresholds"
    for row_id in shared_ids:
        assert low_threshold_rows[row_id] == high_threshold_rows[row_id]


# --------------------------------------------------------------------------- #
# R1, R3, R5, R6, R7 — folded ranking scenarios
# --------------------------------------------------------------------------- #


def test_baseline_top_three_clips_are_tier1(db):
    """5 clips, no uploads -> exactly 3 Tier-1 (the 3 top scores), 2 Tier-2."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        scores = [9.0, 8.0, 7.0, 6.0, 5.0]
        ids = [_insert_clip(cur, chapter_id, virality_score=score) for score in scores]

        rows = _run_candidate_query(cur)

    by_id = {r["id"]: r for r in rows}
    assert set(by_id) == set(ids)
    assert {by_id[i]["tier"] for i in ids[:3]} == {1}
    assert {by_id[i]["tier"] for i in ids[3:]} == {2}
    assert sorted(by_id[i]["chapter_rank"] for i in ids) == [1, 2, 3, 4, 5]


def test_row_set_identity_matches_legacy_query(db):
    """Pending set returned equals exactly the set of inserted ids (no drops,
    no phantom rows) when nothing is uploaded or abandoned."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        scores = [4.0, 3.0, 2.0]
        ids = [_insert_clip(cur, chapter_id, virality_score=score) for score in scores]

        rows = _run_candidate_query(cur)

    returned_ids = {r["id"] for r in rows}
    assert returned_ids == set(ids)


def test_null_score_clip_below_cap_is_tier1(db):
    """2 scored + 1 NULL: the NULL-score clip ranks 3rd (NULLS LAST) and is
    within the Tier-1 cap of 3."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        for score in [9.0, 8.0]:
            _insert_clip(cur, chapter_id, virality_score=score)
        null_id = _insert_clip(cur, chapter_id, virality_score=None)

        rows = _run_candidate_query(cur)

    by_id = {r["id"]: r for r in rows}
    assert by_id[null_id]["chapter_rank"] == 3
    assert by_id[null_id]["tier"] == 1


def test_null_score_clip_above_cap_is_tier2(db):
    """3 scored + 1 NULL: the NULL-score clip ranks 4th and falls to Tier 2."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        for score in [9.0, 8.0, 7.0]:
            _insert_clip(cur, chapter_id, virality_score=score)
        null_id = _insert_clip(cur, chapter_id, virality_score=None)

        rows = _run_candidate_query(cur)

    by_id = {r["id"]: r for r in rows}
    assert by_id[null_id]["chapter_rank"] == 4
    assert by_id[null_id]["tier"] == 2


def test_abandoned_top_clips_free_tier1_slots(db):
    """Abandoned clips never enter the ranking CTE at all (R7): the next 3
    non-abandoned clips become Tier 1 with ranks 1-3, and no abandoned id is
    ever returned."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        abandoned_ids = [
            _insert_clip(cur, chapter_id, virality_score=score, is_upload_abandoned=True) for score in [9.0, 8.0, 7.0]
        ]
        active_ids = [_insert_clip(cur, chapter_id, virality_score=score) for score in [6.0, 5.0, 4.0]]

        rows = _run_candidate_query(cur)

    by_id = {r["id"]: r for r in rows}
    for abandoned_id in abandoned_ids:
        assert abandoned_id not in by_id
    assert sorted(by_id[i]["chapter_rank"] for i in active_ids) == [1, 2, 3]
    assert {by_id[i]["tier"] for i in active_ids} == {1}


def test_null_local_file_path_consumes_rank_but_is_not_returned(db):
    """Highest-scoring clip has no local file yet — it still consumes rank 1
    in the ranking universe, but the outer query excludes it from the
    returned rows (local_file_path IS NOT NULL)."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        missing_file_id = _insert_clip(cur, chapter_id, virality_score=9.0, local_file_path=None)
        present_ids = [_insert_clip(cur, chapter_id, virality_score=score) for score in [8.0, 7.0, 6.0]]

        rows = _run_candidate_query(cur)

    by_id = {r["id"]: r for r in rows}
    assert missing_file_id not in by_id
    # Rank 1 was consumed by the excluded clip, so only 2 of the 3 remaining
    # clips fit inside the Tier-1 cap (ranks 2 and 3); the 3rd falls to Tier 2.
    tier1_ids = {r["id"] for r in rows if r["tier"] == 1}
    tier2_ids = {r["id"] for r in rows if r["tier"] == 2}
    assert len(tier1_ids) == 2
    assert len(tier2_ids) == 1
    assert tier1_ids | tier2_ids == set(present_ids)


def test_null_score_ties_break_deterministically_by_id(db):
    """Two NULL-score clips in the same chapter get distinct chapter_rank
    values ordered by id ASC, and repeated runs of the same query produce the
    identical assignment (R6)."""
    with db.cursor() as cur:
        chapter_id = _insert_chapter(cur)
        first_id = _insert_clip(cur, chapter_id, virality_score=None)
        second_id = _insert_clip(cur, chapter_id, virality_score=None)
        assert first_id < second_id

        run_1 = {r["id"]: r["chapter_rank"] for r in _run_candidate_query(cur)}
        run_2 = {r["id"]: r["chapter_rank"] for r in _run_candidate_query(cur)}

    assert run_1[first_id] == 1
    assert run_1[second_id] == 2
    assert run_1 == run_2
