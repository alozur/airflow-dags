#!/usr/bin/env python3
"""Repair issue #499's turn-caused parent-chapter upload marks.

Dry run is the default. ``--execute`` resets only the documented chapters whose
currently marked parent YouTube id is also present on an uploaded sibling turn
and which still have a prepared, pending, non-abandoned sibling. A genuine
legacy whole-chapter upload cannot satisfy that proof and is never selected.

Usage:
    uv run python scripts/repair_orphaned_turn_chapters.py
    uv run python scripts/repair_orphaned_turn_chapters.py --execute
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.postgres_helpers import PostgresConnection  # noqa: E402

ALLOWED_CHAPTER_IDS = (263, 264, 265, 266, 519)

_CANDIDATE_QUERY = """
    SELECT vc.chapter_id,
           ARRAY_AGG(pending.turn_id ORDER BY pending.turn_id) AS pending_turn_ids
    FROM {chapters_table} AS vc
    JOIN {turns_table} AS pending_turn ON pending_turn.chapter_id = vc.chapter_id
    JOIN {turn_videos_table} AS pending ON pending.turn_id = pending_turn.turn_id
    WHERE vc.chapter_id = ANY(%s)
      AND vc.is_uploaded_to_youtube = TRUE
      AND vc.youtube_video_id IS NOT NULL
      AND pending.prepared_at IS NOT NULL
      AND pending.is_uploaded_to_youtube = FALSE
      AND COALESCE(pending.is_upload_abandoned, FALSE) = FALSE
      AND EXISTS (
          SELECT 1
          FROM {turns_table} AS uploaded_turn
          JOIN {turn_videos_table} AS uploaded ON uploaded.turn_id = uploaded_turn.turn_id
          WHERE uploaded_turn.chapter_id = vc.chapter_id
            AND uploaded.is_uploaded_to_youtube = TRUE
            AND uploaded.youtube_video_id = vc.youtube_video_id
      )
    GROUP BY vc.chapter_id
    ORDER BY vc.chapter_id
"""

_UPDATE_QUERY = """
    UPDATE {chapters_table} AS vc
    SET is_uploaded_to_youtube = FALSE,
        youtube_video_id = NULL,
        youtube_upload_date = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE vc.chapter_id = ANY(%s)
      AND vc.is_uploaded_to_youtube = TRUE
      AND vc.youtube_video_id IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM {turns_table} AS uploaded_turn
          JOIN {turn_videos_table} AS uploaded ON uploaded.turn_id = uploaded_turn.turn_id
          WHERE uploaded_turn.chapter_id = vc.chapter_id
            AND uploaded.is_uploaded_to_youtube = TRUE
            AND uploaded.youtube_video_id = vc.youtube_video_id
      )
      AND EXISTS (
          SELECT 1
          FROM {turns_table} AS pending_turn
          JOIN {turn_videos_table} AS pending ON pending.turn_id = pending_turn.turn_id
          WHERE pending_turn.chapter_id = vc.chapter_id
            AND pending.prepared_at IS NOT NULL
            AND pending.is_uploaded_to_youtube = FALSE
            AND COALESCE(pending.is_upload_abandoned, FALSE) = FALSE
      )
    RETURNING vc.chapter_id
"""


def _query(template: str, chapters_table: str, turns_table: str, turn_videos_table: str) -> str:
    return template.format(
        chapters_table=chapters_table,
        turns_table=turns_table,
        turn_videos_table=turn_videos_table,
    )


def fetch_candidates(conn, chapters_table: str, turns_table: str, turn_videos_table: str) -> list[dict]:
    """Return only parent rows proven to have been marked by a sibling turn upload."""
    with conn.cursor() as cur:
        cur.execute(
            _query(_CANDIDATE_QUERY, chapters_table, turns_table, turn_videos_table),
            (list(ALLOWED_CHAPTER_IDS),),
        )
        return list(cur.fetchall())


def apply_repair(conn, chapters_table: str, turns_table: str, turn_videos_table: str) -> list[int]:
    """Atomically clear only rows that still satisfy the full safety predicate."""
    if getattr(conn, "autocommit", False) is True:
        raise ValueError("refusing --execute with autocommit enabled")
    with conn.cursor() as cur:
        cur.execute(
            _query(_UPDATE_QUERY, chapters_table, turns_table, turn_videos_table),
            (list(ALLOWED_CHAPTER_IDS),),
        )
        return [row["chapter_id"] for row in cur.fetchall()]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Apply the guarded update; default is dry run.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        pg_conn = PostgresConnection()
        chapters_table = pg_conn.get_qualified_table("video_chapters")
        turns_table = pg_conn.get_qualified_table("speaker_turns")
        turn_videos_table = pg_conn.get_qualified_table("speaker_turn_videos")
        with pg_conn.get_connection() as conn:
            candidates = fetch_candidates(conn, chapters_table, turns_table, turn_videos_table)
            candidate_ids = [row["chapter_id"] for row in candidates]
            if not args.execute:
                conn.rollback()
                print(f"DRY RUN: {len(candidates)} qualified chapter(s): {candidate_ids}")
                for row in candidates:
                    print(f"chapter_id={row['chapter_id']} pending_turn_ids={row['pending_turn_ids']}")
                return 0

            updated_ids = apply_repair(conn, chapters_table, turns_table, turn_videos_table)
            if set(updated_ids) != set(candidate_ids):
                raise RuntimeError(
                    f"refusing partial repair: preflight candidates={candidate_ids}, update returned={updated_ids}"
                )
            print(f"EXECUTED: repaired {len(updated_ids)} chapter(s): {updated_ids}")
            return 0
    except Exception as exc:
        print(f"Repair refused: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
