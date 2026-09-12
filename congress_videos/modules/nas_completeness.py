"""Congress-video upload-completeness query (issue: NAS archive / NAS reclaim).

Pure module: no Airflow imports, no subprocess/network calls — the only
side-effecting collaborator is ``PostgresConnection``, so this module is
directly unit-testable with a mocked connection.

Moved out of ``congress_videos/nas_archive_dag.py`` (design D3/D5: the
``nas_reclaim`` DAG needs the same completeness gate as an independent,
Airflow-free import, without re-parsing a module that builds a ``DAG``
object at import time — see ``nas_archive_dag``'s own D1-equivalent
rationale).

A video is "complete" when every chapter and every speaker-turn video
derived from it has cleared the YouTube upload+verification pipeline (or was
permanently abandoned — see ``congress_videos/modules/post_upload_verification.py``)
and nothing about it remains pending in ``uploadable_chapters``/``uploadable_turns``.
The schema has no explicit "this turn will never be materialized" flag, so
"every speaker-turn video uploaded AND verified" cannot be expressed as a
literal join without risking candidates that never converge (a turn with no
``speaker_turn_videos`` row could mean "not yet materialized" OR "filtered
out and will never be materialized", e.g. ``is_procedural``/low
``interest_score``). ``complete_video_ids`` therefore uses the documented,
safe fallback: chapters exist, nothing for the video is pending in
``uploadable_chapters``/``uploadable_turns``, and no uploaded chapter or turn
is missing ``upload_verified_at``. A video with zero speaker turns (or zero
uploadable chapters left) satisfies this vacuously, so it qualifies once its
files clear the age gate — matching the "zero turns but old enough" case.
"""

from __future__ import annotations

from utils.postgres_helpers import PostgresConnection


def complete_video_ids(pool_limit: int) -> list[str]:
    """Return source video_ids with nothing left pending in the upload pipeline.

    Relies on:
      - ``video_chapters``       (congress_videos/sql/production_schema.sql:62)
      - ``uploadable_chapters``  (congress_videos/sql/production_schema.sql:475,
        migration 038 — relevance_score >= 2 AND NOT is_upload_abandoned gate)
      - ``uploadable_turns``     (congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql:52,
        cumulative view lineage documented at production_schema.sql:565)
      - ``speaker_turns``        (congress_videos/sql/production_schema.sql:265)
      - ``speaker_turn_videos``  (congress_videos/sql/production_schema.sql:320)

    Ordered oldest-chapter-first (FIFO) so the batch drains the longest-idle
    videos first.
    """
    pg = PostgresConnection()
    chapters_table = pg.get_qualified_table("video_chapters")
    uploadable_chapters_table = pg.get_qualified_table("uploadable_chapters")
    uploadable_turns_table = pg.get_qualified_table("uploadable_turns")
    turn_videos_table = pg.get_qualified_table("speaker_turn_videos")
    turns_table = pg.get_qualified_table("speaker_turns")

    query = f"""
        SELECT vc.video_id
        FROM {chapters_table} vc
        WHERE NOT EXISTS (
            SELECT 1 FROM {uploadable_chapters_table} uc WHERE uc.video_id = vc.video_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM {chapters_table} vc2
            WHERE vc2.video_id = vc.video_id
              AND vc2.is_uploaded_to_youtube = TRUE
              AND vc2.upload_verified_at IS NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM {uploadable_turns_table} ut WHERE ut.video_id = vc.video_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {turn_videos_table} stv
            JOIN {turns_table} st ON st.turn_id = stv.turn_id
            JOIN {chapters_table} vc3 ON vc3.chapter_id = st.chapter_id
            WHERE vc3.video_id = vc.video_id
              AND stv.is_uploaded_to_youtube = TRUE
              AND stv.upload_verified_at IS NULL
        )
        GROUP BY vc.video_id
        ORDER BY MIN(vc.created_at) ASC
        LIMIT %s
    """
    with pg.get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (pool_limit,))
        return [row["video_id"] for row in cur.fetchall()]
