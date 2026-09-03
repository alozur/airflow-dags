# congreso_youtube/congress_database.py
"""
Database operations specific to congressional video management.
"""

import json
import logging
from datetime import date
from typing import Any

from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)

CHAPTER_UPLOAD_ABANDON_THRESHOLD = 3  # 3rd recorded failure excludes the chapter
THUMBNAIL_REPUBLISH_ABANDON_THRESHOLD = 3  # 3rd recorded failure abandons the republish
THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT = 50  # over-fetch bound; the DAG may pass its own
SHORTS_UPLOAD_ABANDON_THRESHOLD = 3  # 3rd recorded failure excludes the short
SHORTS_SOURCE_VIDEO_COOLDOWN = 5  # other-video upload events before V is eligible again
SHORTS_UPLOAD_HISTORY_LIMIT = 50  # bounded upload-history window
SHORTS_PENDING_CANDIDATE_LIMIT = 200  # candidate over-fetch before the Python cool-down filter
SHORTS_TIER1_PER_CHAPTER_LIMIT = 3  # Tier-1 upload slots per source chapter


def filter_shorts_by_source_cooldown(
    candidates: list[dict[str, Any]],
    upload_history: list[dict[str, Any]],
    cooldown: int = SHORTS_SOURCE_VIDEO_COOLDOWN,
) -> list[dict[str, Any]]:
    """
    Filter out candidate shorts whose source video is still within its
    per-source-video cool-down.

    `upload_history` MUST be ordered most-recent-first (each row a dict with
    a "video_id" key). For a candidate with source video V, only the FIRST
    (most recent) occurrence of V in `upload_history` matters — its ordinal
    position (0-based) equals the number of other-video upload events since
    V was last uploaded. V is eligible when that position is >= `cooldown`,
    or when V does not appear in `upload_history` at all (never uploaded,
    or its last upload fell outside the bounded history window).

    Pure function: no I/O, no datetime handling, order-preserving, and does
    NOT truncate — callers apply their own result limit after filtering.
    """
    if cooldown <= 0 or not upload_history:
        return list(candidates)

    history_video_ids = [row.get("video_id") for row in upload_history]

    eligible = []
    for candidate in candidates:
        video_id = candidate.get("video_id")
        if video_id is None:
            eligible.append(candidate)
            continue
        try:
            position = history_video_ids.index(video_id)
        except ValueError:
            eligible.append(candidate)
            continue
        if position >= cooldown:
            eligible.append(candidate)

    return eligible


def pending_shorts_candidate_sql(shorts_table: str, chapters_table: str) -> str:
    """Candidate query for get_pending_shorts. Params: (tier1_limit, min_virality_score, row_limit).

    Ranks each chapter's downloaded, non-abandoned clips (uploaded and
    pending alike) by virality score, then caps how many of them can be
    Tier 1 per chapter. Only after tiers are computed does the outer query
    filter down to the still-pending, upload-eligible rows.
    """
    return f"""
                    WITH ranked AS (
                        SELECT
                            vs.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY vs.chapter_id
                                ORDER BY vs.reap_virality_score DESC NULLS LAST,
                                         vs.id ASC
                            ) AS chapter_rank
                        FROM {shorts_table} vs
                        WHERE vs.reap_status = 'downloaded'
                          AND vs.is_upload_abandoned = FALSE
                    )
                    SELECT
                        ranked.*,
                        CASE WHEN ranked.chapter_rank <= %s THEN 1 ELSE 2 END AS tier,
                        vc.video_id
                    FROM ranked
                    JOIN {chapters_table} vc ON vc.chapter_id = ranked.chapter_id
                    WHERE ranked.is_uploaded = FALSE
                      AND ranked.is_upload_abandoned = FALSE
                      AND ranked.local_file_path IS NOT NULL
                      AND ranked.reap_status = 'downloaded'
                      AND (ranked.reap_virality_score >= %s OR ranked.reap_virality_score IS NULL)
                      AND vc.youtube_upload_date IS NOT NULL
                    ORDER BY tier ASC,
                             vc.youtube_upload_date DESC NULLS LAST,
                             ranked.reap_virality_score DESC NULLS LAST,
                             ranked.id ASC
                    LIMIT %s
                """


class CongressionalVideoDB:
    """Database operations for congressional video management"""

    def __init__(self):
        self.pg_conn = PostgresConnection()
        # Get schema-qualified table names
        self.sessions_table = self.pg_conn.get_qualified_table("congressional_sessions")
        self.topics_table = self.pg_conn.get_qualified_table("video_topics")
        self.queue_table = self.pg_conn.get_qualified_table("upload_queue")
        self.uploadable_view = self.pg_conn.get_qualified_table("uploadable_videos")

    def update_thumbnail_youtube_video_id(self, chapter_id: int, youtube_video_id: str) -> None:
        """Back-fill the youtube_video_id for a chapter's thumbnail row after upload.

        Called after the YouTube upload completes to associate the real video ID
        with the pre-generated thumbnail persisted in ``video_thumbnails``.

        Args:
            chapter_id: FK to ``video_chapters.chapter_id``.
            youtube_video_id: The YouTube video ID returned by the upload task.
        """
        thumbnails_table = self.pg_conn.get_qualified_table("video_thumbnails")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {thumbnails_table}
                    SET youtube_video_id = %s
                    WHERE chapter_id = %s
                    """,
                    (youtube_video_id, chapter_id),
                )
        logger.info(
            "update_thumbnail_youtube_video_id: chapter_id=%d -> youtube_video_id=%r",
            chapter_id,
            youtube_video_id,
        )

    # ==================== YouTube Chapter Management ====================

    def save_youtube_chapters_to_db(
        self, scored_chapters_data: dict[str, Any], session_number: int = None, session_date: date = None
    ) -> dict[str, Any]:
        """
        Save scored YouTube video chapters to the database.

        This function processes the output from score_chapters_relevance() and stores:
        1. YouTube source videos (youtube_source_videos table)
        2. Individual chapters with relevance scores (video_chapters table)

        Args:
            scored_chapters_data: Results from score_chapters_relevance function
                Expected structure:
                {
                    'total_videos': int,
                    'total_chapters_scored': int,
                    'successful_scores': int,
                    'failed_scores': int,
                    'videos': [
                        {
                            'video_id': str,
                            'video_title': str,
                            'total_chapters': int,
                            'scored_chapters': [
                                {
                                    'title': str,
                                    'description': str,
                                    'duration_minutes': float,
                                    'speakers': [str],
                                    'topics': [str],
                                    'start_time': str,
                                    'end_time': str,
                                    'relevance_score': int (0-5),
                                    'speaker_relevance_points': int (0-2),
                                    'topic_relevance_points': int (0-2),
                                    'public_interest_points': int (0-1),
                                    'scoring_reasoning': str,
                                    'key_speakers': [str],
                                    'is_current_topic': bool,
                                    'scoring_error': str or None
                                }
                            ]
                        }
                    ]
                }
            session_number: Optional congressional session number to link videos to
            session_date: Optional session date for linkage

        Returns:
            Dict with save results:
            {
                'total_videos_saved': int,
                'total_chapters_saved': int,
                'videos': [
                    {
                        'video_id': str,
                        'chapters_saved': int,
                        'error': str or None,
                        # One entry per chapter actually inserted; [] when
                        # skipped or its SAVEPOINT was rolled back (#340).
                        'chapters': [{'chapter_id': int, 'start_time': str, 'end_time': str}]
                    }
                ]
            }
        """
        if not scored_chapters_data or not scored_chapters_data.get("videos"):
            logger.warning("No scored chapters data to save")
            return {"total_videos_saved": 0, "total_chapters_saved": 0, "total_videos_failed": 0, "videos": []}

        save_results = {"total_videos_saved": 0, "total_chapters_saved": 0, "total_videos_failed": 0, "videos": []}

        youtube_videos_table = self.pg_conn.get_qualified_table("youtube_source_videos")
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        # Each video is isolated inside its own SAVEPOINT so that a failure on one
        # video (e.g. an aborted statement) does not poison the whole transaction
        # and discard the videos that did succeed. get_connection() owns the full
        # connection lifecycle (commit/rollback/close); `with conn` adds the inner
        # transaction scope.
        with self.pg_conn.get_connection() as conn:
            with conn:
                with conn.cursor() as cur:
                    for video_data in scored_chapters_data["videos"]:
                        video_id = video_data.get("video_id")
                        video_title = video_data.get("video_title", "Unknown Video")
                        scored_chapters = video_data.get("scored_chapters", [])

                        if video_data.get("error"):
                            logger.warning(f"Skipping video {video_id} due to error: {video_data.get('error')}")
                            save_results["videos"].append(
                                {
                                    "video_id": video_id,
                                    "chapters_saved": 0,
                                    "error": video_data.get("error"),
                                    "chapters": [],
                                }
                            )
                            continue

                        cur.execute("SAVEPOINT sp_video")
                        video_chapters: list[dict[str, Any]] = []
                        try:
                            # Step 1: Upsert YouTube source video
                            video_url = f"https://www.youtube.com/watch?v={video_id}"
                            total_chapters = len(scored_chapters)

                            cur.execute(
                                f"""
                                INSERT INTO {youtube_videos_table}
                                (video_id, video_title, video_url, session_number, session_date, total_chapters, is_processed)
                                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                                ON CONFLICT (video_id) DO UPDATE SET
                                    video_title = EXCLUDED.video_title,
                                    session_number = EXCLUDED.session_number,
                                    session_date = EXCLUDED.session_date,
                                    total_chapters = EXCLUDED.total_chapters,
                                    is_processed = TRUE,
                                    updated_at = CURRENT_TIMESTAMP
                                RETURNING video_id
                            """,
                                (video_id, video_title, video_url, session_number, session_date, total_chapters),
                            )

                            logger.info(f"Saved/updated YouTube source video: {video_id}")

                            # Step 2: Save all chapters for this video
                            chapters_saved_count = 0

                            for chapter in scored_chapters:
                                # Extract chapter data
                                title = chapter.get("title", "Untitled Chapter")
                                description = chapter.get("description", "")
                                start_time = chapter.get("start_time")
                                end_time = chapter.get("end_time")
                                duration_minutes = chapter.get("duration_minutes", 0)

                                # Speaker and topic arrays
                                speakers = chapter.get("speakers", [])
                                topics = chapter.get("topics", [])

                                # Timeline (key moments) — stored as JSONB.
                                # List of {time, speaker, content} with absolute
                                # source-video timestamps.
                                timeline = chapter.get("timeline", [])

                                # Scoring data
                                relevance_score = chapter.get("relevance_score", 0)
                                speaker_pts = chapter.get("speaker_relevance_points", 0)
                                topic_pts = chapter.get("topic_relevance_points", 0)
                                interest_pts = chapter.get("public_interest_points", 0)
                                scoring_reasoning = chapter.get("scoring_reasoning", "")
                                key_speakers = chapter.get("key_speakers", speakers)
                                is_current_topic = chapter.get("is_current_topic", False)
                                scoring_error = chapter.get("scoring_error")

                                cur.execute(
                                    f"""
                                    INSERT INTO {chapters_table}
                                    (video_id, title, description, start_time, end_time, duration_minutes,
                                     speakers, topics, timeline, relevance_score, speaker_relevance_points, topic_relevance_points,
                                     public_interest_points, scoring_reasoning, key_speakers, is_current_topic,
                                     scoring_error, scored_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                    ON CONFLICT (video_id, start_time, end_time) DO UPDATE SET
                                        title = EXCLUDED.title, description = EXCLUDED.description,
                                        duration_minutes = EXCLUDED.duration_minutes, speakers = EXCLUDED.speakers,
                                        topics = EXCLUDED.topics, timeline = EXCLUDED.timeline, relevance_score = EXCLUDED.relevance_score,
                                        speaker_relevance_points = EXCLUDED.speaker_relevance_points,
                                        topic_relevance_points = EXCLUDED.topic_relevance_points,
                                        public_interest_points = EXCLUDED.public_interest_points,
                                        scoring_reasoning = EXCLUDED.scoring_reasoning, key_speakers = EXCLUDED.key_speakers,
                                        is_current_topic = EXCLUDED.is_current_topic, scoring_error = EXCLUDED.scoring_error,
                                        scored_at = CURRENT_TIMESTAMP
                                    RETURNING chapter_id
                                """,
                                    (
                                        video_id,
                                        title,
                                        description,
                                        start_time,
                                        end_time,
                                        duration_minutes,
                                        speakers,
                                        topics,
                                        json.dumps(timeline),
                                        relevance_score,
                                        speaker_pts,
                                        topic_pts,
                                        interest_pts,
                                        scoring_reasoning,
                                        key_speakers,
                                        is_current_topic,
                                        scoring_error,
                                    ),
                                )

                                chapter_id = cur.fetchone()["chapter_id"]
                                chapters_saved_count += 1
                                video_chapters.append(
                                    {
                                        "chapter_id": chapter_id,
                                        "start_time": start_time,
                                        "end_time": end_time,
                                    }
                                )

                                logger.info(f"Saved chapter {chapter_id}: '{title}' (score: {relevance_score}/5)")

                            # Both steps succeeded for this video — make it durable
                            # within the transaction so a later video's failure
                            # cannot roll it back.
                            cur.execute("RELEASE SAVEPOINT sp_video")

                            save_results["total_videos_saved"] += 1
                            save_results["total_chapters_saved"] += chapters_saved_count
                            save_results["videos"].append(
                                {
                                    "video_id": video_id,
                                    "chapters_saved": chapters_saved_count,
                                    "error": None,
                                    "chapters": video_chapters,
                                }
                            )

                            logger.info(f"Successfully saved {chapters_saved_count} chapters for video {video_id}")

                        except Exception as e:
                            # Roll back ONLY this video's work and clear the
                            # aborted-statement state so the remaining videos in
                            # the batch can still be processed.
                            cur.execute("ROLLBACK TO SAVEPOINT sp_video")
                            error_msg = f"Error saving chapters for video {video_id}: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            save_results["total_videos_failed"] += 1
                            save_results["videos"].append(
                                {"video_id": video_id, "chapters_saved": 0, "error": error_msg, "chapters": []}
                            )

        # `with conn:` has committed the videos whose savepoints were released.
        logger.info(
            f"Chapter save complete: {save_results['total_videos_saved']} videos, "
            f"{save_results['total_chapters_saved']} chapters saved to database "
            f"({save_results['total_videos_failed']} video(s) failed)"
        )

        # Total failure: at least one video was attempted and every attempt
        # failed. Raise so the Airflow task fails visibly instead of silently
        # reporting success and letting the hourly DAG reprocess forever.
        if save_results["total_videos_saved"] == 0 and save_results["total_videos_failed"] > 0:
            raise RuntimeError(
                f"save_youtube_chapters_to_db: all {save_results['total_videos_failed']} "
                f"video(s) failed to save; see logs"
            )

        return save_results

    def get_uploadable_chapters(self, limit: int = None, min_relevance_score: int = 4) -> list[dict]:
        """
        Get chapters eligible for YouTube upload.

        Args:
            limit: Maximum number of chapters to return
            min_relevance_score: Minimum relevance score (default: 4/5)

        Returns:
            List of chapter records from the uploadable_chapters view
        """
        uploadable_chapters_view = self.pg_conn.get_qualified_table("uploadable_chapters")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT * FROM {uploadable_chapters_view}
                    WHERE relevance_score >= %s
                    ORDER BY relevance_score DESC, created_at DESC
                """
                if limit:
                    query += f" LIMIT {limit}"

                cur.execute(query, (min_relevance_score,))
                chapters = cur.fetchall()
                logger.info(
                    f"Retrieved {len(chapters)} uploadable chapters (min_score={min_relevance_score}, limit={limit})"
                )
                return chapters

    def mark_chapter_uploaded(self, chapter_id: int, youtube_video_id: str):
        """
        Mark a chapter as uploaded to YouTube.

        Args:
            chapter_id: Database ID of the chapter
            youtube_video_id: YouTube video ID of the uploaded chapter video
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {chapters_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chapter_id = %s
                """,
                    (youtube_video_id, chapter_id),
                )
                logger.info(f"Marked chapter {chapter_id} as uploaded to YouTube: {youtube_video_id}")

    def record_chapter_upload_failure(self, chapter_id: int, error_message: str | None = None) -> None:
        """
        Record a failed per-chapter YouTube upload attempt.

        Increments the cumulative failure counter and, once it reaches
        CHAPTER_UPLOAD_ABANDON_THRESHOLD (3), marks the chapter abandoned so it is
        excluded from uploadable_chapters. The row is never deleted; the counter is
        cumulative for the life of the row and never resets or un-abandons.

        Args:
            chapter_id: Database ID of the chapter that failed to upload.
            error_message: Optional last error text from the upload result payload.
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {chapters_table} SET
                        upload_attempts = upload_attempts + 1,
                        last_upload_error = %s,
                        is_upload_abandoned = (upload_attempts + 1 >= {CHAPTER_UPLOAD_ABANDON_THRESHOLD}),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chapter_id = %s
                    RETURNING upload_attempts, is_upload_abandoned
                """,
                    (error_message, chapter_id),
                )
                result = cur.fetchone()
                logger.info(
                    f"Recorded upload failure for chapter {chapter_id} (threshold={CHAPTER_UPLOAD_ABANDON_THRESHOLD})"
                )
                if result and result.get("upload_attempts") == CHAPTER_UPLOAD_ABANDON_THRESHOLD:
                    logger.warning(
                        f"Chapter {chapter_id} abandoned after {CHAPTER_UPLOAD_ABANDON_THRESHOLD} failed uploads"
                    )

    # ==================== Video Shorts (Reap Pipeline) ====================

    def get_chapters_for_shorts(self, limit: int | None = None, min_relevance_score: int = 3) -> list[dict]:
        """
        Get video chapters eligible for Reap Shorts processing.

        A chapter qualifies when ALL of the following hold:
        - is_uploaded_to_youtube = TRUE (only already-published chapters)
        - relevance_score >= min_relevance_score
        - Duration between 120 and 900 seconds (2-15 min)
        - No existing video_shorts row for the chapter (any reap_status)

        Args:
            limit: Maximum number of chapters to return (None = no limit, default None)
            min_relevance_score: Minimum relevance score threshold (default 3)

        Returns:
            List of chapter records ordered by relevance_score DESC, created_at DESC
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT vc.*
                    FROM {chapters_table} vc
                    WHERE vc.is_uploaded_to_youtube = TRUE
                      AND vc.relevance_score >= %s
                      AND (
                          EXTRACT(EPOCH FROM (
                              REPLACE(vc.end_time, ',', '.')::interval
                              - REPLACE(vc.start_time, ',', '.')::interval
                          )) >= 120
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM {shorts_table} vs
                          WHERE vs.chapter_id = vc.chapter_id
                      )
                    ORDER BY vc.relevance_score DESC, vc.created_at DESC
                """
                params: list = [min_relevance_score]
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                cur.execute(query, params)
                chapters = cur.fetchall()
                logger.info(
                    f"Found {len(chapters)} chapters eligible for Shorts "
                    f"(min_score={min_relevance_score}, limit={limit})"
                )
                return chapters

    def insert_video_short(
        self,
        chapter_id: int,
        reap_project_id: str | None = None,
        reap_status: str = "pending",
        pretrim_start_secs: float = None,
        pretrim_end_secs: float = None,
        pretrim_used_srt: bool = False,
        staged_clip_path: str | None = None,
        scoring_reasoning: str | None = None,
    ) -> int:
        """
        Insert a video_shorts row.

        DAG 1 uses this to insert pending rows (reap_project_id=None, staged_clip_path set).
        DAG 2 legacy path (pre-redesign) used this for processing rows — now replaced by
        claim_pending_clip + update_video_short_project.

        Args:
            chapter_id: FK to video_chapters.id
            reap_project_id: Reap project ID (None for pending rows inserted by DAG 1)
            reap_status: Initial status (default 'pending')
            pretrim_start_secs: Start of pre-trim window in seconds (None if no trim)
            pretrim_end_secs: End of pre-trim window in seconds (None if no trim)
            pretrim_used_srt: True if an SRT file was used for AI window selection
            staged_clip_path: Local path to the pre-trimmed clip file (set by DAG 1)
            scoring_reasoning: AI scoring reasoning text (optional)

        Returns:
            The id of the newly inserted video_shorts row
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {shorts_table}
                    (chapter_id, reap_project_id, reap_status,
                     pretrim_start_secs, pretrim_end_secs, pretrim_used_srt,
                     staged_clip_path, scoring_reasoning)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """,
                    (
                        chapter_id,
                        reap_project_id,
                        reap_status,
                        pretrim_start_secs,
                        pretrim_end_secs,
                        pretrim_used_srt,
                        staged_clip_path,
                        scoring_reasoning,
                    ),
                )
                row_id = cur.fetchone()["id"]
                logger.info(
                    f"Inserted video_short row {row_id} "
                    f"for chapter {chapter_id}, status={reap_status}, project={reap_project_id}"
                )
                return row_id

    def insert_video_short_clip(
        self,
        chapter_id: int,
        reap_project_id: str,
        reap_clip_id: str,
        reap_virality_score: float,
        reap_clip_url: str,
        local_file_path: str,
        reap_status: str = "downloaded",
    ) -> int:
        """
        Insert one video_shorts row per downloaded Reap clip.

        Called by the ReapJobSensor once per clip when the Reap job completes.
        Each call produces a distinct row with a unique reap_clip_id.

        Args:
            chapter_id: FK to video_chapters.id
            reap_project_id: Reap project ID that produced this clip
            reap_clip_id: Unique Reap clip identifier (stored in UNIQUE column)
            reap_virality_score: Virality score assigned by Reap
            reap_clip_url: Reap-hosted URL of the clip
            local_file_path: Absolute path to the downloaded MP4 on disk
            reap_status: Status to set (default 'downloaded')

        Returns:
            The id of the newly inserted video_shorts row
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {shorts_table}
                    (chapter_id, reap_project_id, reap_clip_id,
                     reap_virality_score, reap_clip_url, local_file_path, reap_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """,
                    (
                        chapter_id,
                        reap_project_id,
                        reap_clip_id,
                        reap_virality_score,
                        reap_clip_url,
                        local_file_path,
                        reap_status,
                    ),
                )
                row_id = cur.fetchone()["id"]
                logger.info(
                    f"Inserted video_short clip row {row_id}: clip_id={reap_clip_id}, virality={reap_virality_score}"
                )
                return row_id

    def update_video_short_status(self, reap_project_id: str, status: str) -> None:
        """
        Update the reap_status for all video_shorts rows belonging to a Reap project.

        Used by the ReapJobSensor to record terminal failure states
        (e.g. 'failed', 'expired', 'invalid', 'error', 'credits_exhausted').

        Args:
            reap_project_id: Reap project ID whose rows should be updated
            status: New reap_status value
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {shorts_table} SET
                        reap_status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE reap_project_id = %s
                """,
                    (status, reap_project_id),
                )
                logger.info(f"Updated video_shorts status to '{status}' for project {reap_project_id}")

    def get_source_video_id_for_chapter(self, chapter_id: int) -> str | None:
        """Return the source YouTube video_id for a chapter row.

        Args:
            chapter_id: Database chapter ID to look up.

        Returns:
            The ``video_id`` string when the chapter exists and has a non-NULL
            ``video_id``; ``None`` otherwise (no row or NULL value).
        """
        table = self.pg_conn.get_qualified_table("video_chapters")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT video_id FROM {table} WHERE chapter_id = %s",
                    (chapter_id,),
                )
                row = cur.fetchone()
                return row["video_id"] if row and row["video_id"] else None

    def claim_pending_clip(self) -> dict | None:
        """
        Atomically claim one pending clip from the queue, ordered by priority.

        Uses SELECT FOR UPDATE SKIP LOCKED so concurrent DAG 2 runs each claim
        a distinct row without blocking each other.

        Priority order: session_date DESC NULLS LAST, relevance_score DESC NULLS LAST
        (most recent and most relevant chapters are processed first).

        Returns:
            The claimed video_shorts row as a dict, or None if no pending rows exist.
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        videos_table = self.pg_conn.get_qualified_table("youtube_source_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {shorts_table} SET reap_status = 'processing', updated_at = CURRENT_TIMESTAMP
                    WHERE id = (
                        SELECT vs.id
                        FROM {shorts_table} vs
                        WHERE vs.reap_status = 'pending'
                        ORDER BY (
                            SELECT ysv.session_date
                            FROM {chapters_table} vc
                            LEFT JOIN {videos_table} ysv ON ysv.video_id = vc.video_id
                            WHERE vc.chapter_id = vs.chapter_id
                            LIMIT 1
                        ) DESC NULLS LAST,
                        (
                            SELECT vc.relevance_score
                            FROM {chapters_table} vc
                            WHERE vc.chapter_id = vs.chapter_id
                            LIMIT 1
                        ) DESC NULLS LAST
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                """)
                row = cur.fetchone()
                if row is None:
                    logger.info("claim_pending_clip: no pending clips in queue")
                    return None
                logger.info(f"Claimed pending clip: short_id={row['id']}, chapter_id={row['chapter_id']}")
                return dict(row)

    def update_video_short_project(self, short_id: int, reap_project_id: str) -> None:
        """
        Set the reap_project_id on an existing video_shorts row.

        Called by DAG 2 after a Reap project is created for the claimed clip.

        Args:
            short_id: Primary key of the video_shorts row to update
            reap_project_id: Reap project ID returned by create_clips_job
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {shorts_table} SET
                        reap_project_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (reap_project_id, short_id),
                )
                logger.info(f"Updated video_short {short_id}: reap_project_id={reap_project_id}")

    def update_video_short_status_by_id(self, short_id: int, status: str) -> None:
        """Update reap_status for a single video_shorts row by primary key."""
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {shorts_table}
                    SET reap_status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (status, short_id),
                )
                logger.info(f"Updated video_short {short_id}: reap_status={status}")

    def get_pending_shorts(self, limit: int = 2, min_virality_score: float = 0.0) -> list[dict]:
        """
        Get downloaded Shorts clips that are ready for YouTube upload.

        Only returns clips whose parent long-form video (chapter) is already
        uploaded to YouTube. Each chapter's downloaded, non-abandoned clips
        are ranked by virality score and capped at SHORTS_TIER1_PER_CHAPTER_LIMIT
        Tier-1 slots; the rest fall to Tier 2. Tier is the PRIMARY sort key,
        then the chapter's YouTube upload date descending (most recently
        uploaded long-form video first), then virality score descending as a
        tie-breaker within the same tier and long-form video. Only clips with
        a local file present are returned.

        The per-chapter ranking universe deliberately INCLUDES clips that are
        already uploaded (`is_uploaded = TRUE`): an upload permanently
        consumes its chapter's Tier-1 slot. Ranking pending-only clips would
        make the cap inert, since ranks would recompute after every upload
        and the chapter would perpetually re-present 3 fresh Tier-1
        candidates, draining its whole batch before other chapters get a
        turn — the exact bug this method fixes. `local_file_path` and
        `min_virality_score` are applied only in the OUTER query, after tiers
        are computed, so tier assignment stays independent of the runtime
        virality threshold. Tier-2 rows are never abandoned, deleted, or
        hard-filtered; they remain selectable as a fallback whenever the
        fetched window has no Tier-1 row left.

        Additionally applies a per-source-video cool-down: a candidate is
        excluded when fewer than SHORTS_SOURCE_VIDEO_COOLDOWN shorts from
        OTHER source videos have uploaded since its own source video's last
        upload (see `filter_shorts_by_source_cooldown`). Candidates are
        over-fetched (up to SHORTS_PENDING_CANDIDATE_LIMIT) and the cool-down
        filter is applied in Python BEFORE truncating to `limit`, so that a
        cooling-down row at the head of the queue does not zero out the run
        while eligible rows sit deeper in it. If every remaining candidate is
        cooling down, this returns an empty list (strict-skip — never
        publishes a cooling-down short just to fill the run).

        Args:
            limit: Maximum number of rows to return (default 2)
            min_virality_score: Minimum virality score threshold (default 0.0)

        Returns:
            List of video_shorts records (each including `video_id`,
            `chapter_rank`, and `tier` keys) ordered by tier ASC, then parent
            chapter youtube_upload_date DESC, then reap_virality_score DESC
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        min_virality_score = min_virality_score if min_virality_score is not None else 0.0

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # updated_at doubles as an implicit uploaded_at proxy here: only
                # mark_short_uploaded() touches it, and it always sets
                # is_uploaded = TRUE in the same UPDATE, so DESC-ordering
                # uploaded rows by updated_at reconstructs upload recency.
                cur.execute(
                    f"""
                    SELECT vc.video_id, vs.updated_at
                    FROM {shorts_table} vs
                    JOIN {chapters_table} vc ON vc.chapter_id = vs.chapter_id
                    WHERE vs.is_uploaded = TRUE
                    ORDER BY vs.updated_at DESC
                    LIMIT %s
                """,
                    (SHORTS_UPLOAD_HISTORY_LIMIT,),
                )
                upload_history = cur.fetchall()

                cur.execute(
                    pending_shorts_candidate_sql(shorts_table, chapters_table),
                    (SHORTS_TIER1_PER_CHAPTER_LIMIT, min_virality_score, SHORTS_PENDING_CANDIDATE_LIMIT),
                )
                candidates = cur.fetchall()

                eligible = filter_shorts_by_source_cooldown(candidates, upload_history)
                blocked = len(candidates) - len(eligible)
                if blocked > 0:
                    logger.info(
                        f"Source-video cool-down blocked {blocked} of {len(candidates)} "
                        f"pending shorts (cooldown={SHORTS_SOURCE_VIDEO_COOLDOWN} uploads)"
                    )
                if candidates and not eligible:
                    logger.info(
                        f"All {len(candidates)} pending shorts are cooling down "
                        f"(source-video cool-down={SHORTS_SOURCE_VIDEO_COOLDOWN}); "
                        "uploading nothing this run"
                    )

                shorts = eligible[:limit]
                logger.info(
                    f"Found {len(shorts)} pending shorts for upload (min_virality={min_virality_score}, limit={limit})"
                )
                return shorts

    def mark_short_uploaded(self, reap_clip_id: str, youtube_video_id: str) -> None:
        """
        Mark a Shorts clip as successfully uploaded to YouTube.

        Args:
            reap_clip_id: Unique Reap clip identifier (used as the row lookup key)
            youtube_video_id: YouTube video ID assigned after upload
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {shorts_table} SET
                        is_uploaded = TRUE,
                        youtube_video_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE reap_clip_id = %s
                """,
                    (youtube_video_id, reap_clip_id),
                )
                logger.info(f"Marked short clip {reap_clip_id} as uploaded to YouTube: {youtube_video_id}")

    def record_short_upload_failure(self, reap_clip_id: str, error_message: str | None = None) -> None:
        """
        Record a failed per-short YouTube upload attempt.

        Increments the cumulative failure counter and, once it reaches
        SHORTS_UPLOAD_ABANDON_THRESHOLD (3), marks the short abandoned so it is
        excluded from get_pending_shorts. The row is never deleted; the counter is
        cumulative for the life of the row and never resets or un-abandons.

        Args:
            reap_clip_id: Unique Reap clip identifier (used as the row lookup key).
            error_message: Optional last error text from the upload result payload.
        """
        shorts_table = self.pg_conn.get_qualified_table("video_shorts")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {shorts_table} SET
                        upload_attempts = upload_attempts + 1,
                        last_upload_error = %s,
                        is_upload_abandoned = (upload_attempts + 1 >= {SHORTS_UPLOAD_ABANDON_THRESHOLD}),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE reap_clip_id = %s
                    RETURNING upload_attempts, is_upload_abandoned
                """,
                    (error_message, reap_clip_id),
                )
                result = cur.fetchone()
                logger.info(
                    f"Recorded upload failure for short clip {reap_clip_id} "
                    f"(threshold={SHORTS_UPLOAD_ABANDON_THRESHOLD})"
                )
                if result and result.get("upload_attempts") == SHORTS_UPLOAD_ABANDON_THRESHOLD:
                    logger.warning(
                        f"Short clip {reap_clip_id} abandoned after {SHORTS_UPLOAD_ABANDON_THRESHOLD} failed uploads"
                    )

    def _count_records(self, table_or_view: str, where_clause: str = "", params: tuple = ()) -> int:
        table = self.pg_conn.get_qualified_table(table_or_view)
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"SELECT COUNT(*) AS count FROM {table}"
                if where_clause:
                    query += f" WHERE {where_clause}"
                cur.execute(query, params)
                result = cur.fetchone()
                return result["count"] if result else 0

    def get_uploadable_turns(self, limit: int = 1) -> list[dict]:
        """Get speaker turn videos eligible for YouTube upload.

        Args:
            limit: Maximum number of turns to return (default: 1).

        Returns:
            List of turn records from the uploadable_turns view.
        """
        uploadable_turns_view = self.pg_conn.get_qualified_table("uploadable_turns")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {uploadable_turns_view} LIMIT %s",
                    (limit,),
                )
                turns = cur.fetchall()
                logger.info(f"Retrieved {len(turns)} uploadable turns (limit={limit})")
                return turns

    def mark_turns_uploaded(self, turn_id: int, youtube_video_id: str) -> None:
        """Mark a speaker turn video as uploaded to YouTube.

        Sets is_uploaded_to_youtube=TRUE, youtube_video_id, and
        youtube_upload_date=NOW() for the given turn_id.

        Args:
            turn_id: Database ID of the speaker turn.
            youtube_video_id: YouTube video ID of the uploaded turn video.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = NOW()
                    WHERE output_path = (
                        SELECT output_path FROM {stv_table} WHERE turn_id = %s
                    )
                    """,
                    (youtube_video_id, turn_id),
                )
                logger.info(f"Marked turn {turn_id} as uploaded to YouTube: {youtube_video_id}")

    def mark_turns_uploaded_by_output_path(self, output_path: str, youtube_video_id: str) -> int:
        """Mark ALL speaker turn video rows sharing output_path as uploaded.

        Fallback marking path for when the caller does not have a turn_id
        (e.g. upload_detail predates issue #230's turn_id propagation fix).
        Mirrors the ``mark_upload_verified`` output_path predicate (#141).

        Args:
            output_path: Absolute path to the grouped turn's video.mp4 file.
            youtube_video_id: YouTube video ID of the uploaded turn video.

        Returns:
            Number of rows updated (``cur.rowcount``), so callers can detect
            when no row matched output_path.

        Raises:
            ValueError: If output_path is falsy (would generate an unbounded
                ``WHERE output_path = NULL`` update).
        """
        if not output_path:
            raise ValueError("mark_turns_uploaded_by_output_path: output_path is required")

        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = NOW()
                    WHERE output_path = %s
                    """,
                    (youtube_video_id, output_path),
                )
                logger.info(
                    "mark_turns_uploaded_by_output_path: output_path=%r marked uploaded to YouTube: %s (%d rows)",
                    output_path,
                    youtube_video_id,
                    cur.rowcount,
                )
                return cur.rowcount

    def mark_turn_thumbnail_republish_needed(
        self,
        *,
        output_path: str | None = None,
        turn_id: int | None = None,
        error_message: str | None = None,
    ) -> int:
        """Arm the thumbnail-republish marker for every row sharing one output_path.

        Dual-key (issue #230's turn_id-primary / output_path-fallback reality).
        Nulling thumbnail_republished_at re-arms a row that broke again;
        attempts/abandoned are NOT reset — cumulative, never un-abandon.

        Raises:
            ValueError: If both output_path and turn_id are falsy.
        """
        if not output_path and not turn_id:
            raise ValueError("mark_turn_thumbnail_republish_needed: output_path/turn_id required")

        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")
        where_clause, where_param = (
            ("WHERE output_path = %s", output_path)
            if output_path
            else (
                f"WHERE output_path = (SELECT output_path FROM {stv_table} WHERE turn_id = %s)",
                turn_id,
            )
        )

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table} SET
                        thumbnail_republish_needed_at  = NOW(),
                        thumbnail_republished_at       = NULL,
                        last_thumbnail_republish_error = %s
                    {where_clause}
                    """,
                    (error_message, where_param),
                )
                logger.info(
                    "mark_turn_thumbnail_republish_needed: output_path=%r turn_id=%r marked for republish (%d rows)",
                    output_path,
                    turn_id,
                    cur.rowcount,
                )
                return cur.rowcount

    # ================ Thumbnail Republish Healer (issue #331, WU2a) ================
    #
    # Turn-only, thumbnail-only by construction: no item_type dispatch, no chapter
    # branch. NEVER reuse record_upload_verification_failure's SET shape here — that
    # method flips is_uploaded_to_youtube and would trigger a spurious FULL video
    # re-upload for what is only a thumbnail-publish failure. The DD4 structural
    # guard in test_database_thumbnail_republish.py enforces this permanently.

    def select_turns_needing_thumbnail_republish(self, limit: int = THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT) -> list[dict]:
        """Return healer candidates, deduplicated one row per output_path.

        Wrapped-dedup shape (mirrors select_unverified_uploads, database.py
        ~1339): DISTINCT ON forces the inner ORDER BY to lead with the
        dedup key, so priority ordering must live in an outer query.

        The outer ORDER BY is attempts ASC, needed_at ASC, output_path ASC.
        output_path is the real tiebreaker (issue #300): same-day siblings
        tie on both prior keys, and without a total order Postgres may
        return any permutation, making the per-run cap non-reproducible.

        Args:
            limit: Maximum candidates to return.

        Returns:
            List of dicts: turn_id, output_path, youtube_video_id,
            thumbnail_republish_needed_at, thumbnail_republish_attempts.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM (
                        SELECT DISTINCT ON (output_path)
                            turn_id,
                            output_path,
                            youtube_video_id,
                            thumbnail_republish_needed_at,
                            thumbnail_republish_attempts
                        FROM {stv_table}
                        WHERE thumbnail_republish_needed_at IS NOT NULL
                          AND thumbnail_republished_at IS NULL
                          AND NOT COALESCE(thumbnail_republish_abandoned, FALSE)
                          AND COALESCE(thumbnail_republish_attempts, 0)
                              < {THUMBNAIL_REPUBLISH_ABANDON_THRESHOLD}
                          AND is_uploaded_to_youtube = TRUE
                          AND youtube_video_id IS NOT NULL
                        ORDER BY output_path, turn_id
                    ) dedup
                    ORDER BY dedup.thumbnail_republish_attempts   ASC,
                             dedup.thumbnail_republish_needed_at  ASC,
                             dedup.output_path                    ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                logger.info(
                    "select_turns_needing_thumbnail_republish: %d candidates (limit=%d)",
                    len(rows),
                    limit,
                )
                return rows

    def mark_turn_thumbnail_republished(self, output_path: str) -> int:
        """Record a successful republish. Sets exactly two columns.

        Deliberately does NOT touch is_uploaded_to_youtube, youtube_video_id,
        youtube_upload_date, upload_attempts, is_upload_abandoned, or
        upload_verified_at — see the DD4 structural guard.

        Args:
            output_path: Absolute path shared by the healed turn's siblings.

        Returns:
            Number of rows updated (cur.rowcount).
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table} SET
                        thumbnail_republished_at       = NOW(),
                        last_thumbnail_republish_error = NULL
                    WHERE output_path = %s
                    """,
                    (output_path,),
                )
                logger.info(
                    "mark_turn_thumbnail_republished: output_path=%r healed (%d rows)",
                    output_path,
                    cur.rowcount,
                )
                return cur.rowcount

    def record_turn_thumbnail_republish_failure(
        self,
        output_path: str,
        error_message: str | None = None,
        *,
        abandon: bool = False,
    ) -> dict | None:
        """Record a republish failure; abandon at threshold or immediately.

        Increments thumbnail_republish_attempts and stores error_message.
        Sets thumbnail_republish_abandoned=TRUE when either abandon=True
        (missing thumbnail.png on disk, proposal D3 — no regeneration, no
        further attempts) or the incremented attempts count reaches
        THUMBNAIL_REPUBLISH_ABANDON_THRESHOLD. Deliberately does NOT touch
        upload-verification state — see the DD4 structural guard.

        Args:
            output_path: Absolute path shared by the failed turn's siblings.
            error_message: Description of the failure.
            abandon: Force immediate abandonment regardless of attempt count.

        Returns:
            Dict with thumbnail_republish_attempts/thumbnail_republish_abandoned,
            or None if no row matched output_path.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table} SET
                        thumbnail_republish_attempts   = COALESCE(thumbnail_republish_attempts, 0) + 1,
                        last_thumbnail_republish_error = %s,
                        thumbnail_republish_abandoned  = (
                            %s OR COALESCE(thumbnail_republish_attempts, 0) + 1
                                  >= {THUMBNAIL_REPUBLISH_ABANDON_THRESHOLD}
                        )
                    WHERE output_path = %s
                    RETURNING thumbnail_republish_attempts, thumbnail_republish_abandoned
                    """,
                    (error_message, abandon, output_path),
                )
                result = cur.fetchone()
                if result and result.get("thumbnail_republish_abandoned"):
                    logger.warning(
                        "record_turn_thumbnail_republish_failure: output_path=%r "
                        "abandoned after %s attempts (forced=%s)",
                        output_path,
                        result.get("thumbnail_republish_attempts"),
                        abandon,
                    )
                else:
                    logger.info(
                        "record_turn_thumbnail_republish_failure: output_path=%r still eligible after this failure",
                        output_path,
                    )
                return result

    def select_unprepared_turns(self, limit: int = 2) -> list[dict]:
        """Select speaker turns that have not been prepared yet.

        Returns turns where prepared_at IS NULL and is_uploaded_to_youtube = FALSE,
        ordered by COALESCE(interest_score, 1) DESC (uploadable_turns priority order).
        Used exclusively by the speaker_turn_prepare DAG nightly loop.

        Args:
            limit: Maximum number of turns to return (default: 2, the nightly buffer).

        Returns:
            List of speaker_turn_videos rows joined to speaker_turns.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")
        st_table = self.pg_conn.get_qualified_table("speaker_turns")
        vc_table = self.pg_conn.get_qualified_table("video_chapters")
        ysv_table = self.pg_conn.get_qualified_table("youtube_source_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT * FROM (
                        SELECT DISTINCT ON (stv.output_path)
                            stv.turn_id, stv.output_path, st.chapter_id, st.resolved_name,
                            st.start_seconds, st.end_seconds, st.interest_score,
                            stv.turn_type,
                            MIN(st.start_seconds) OVER (PARTITION BY stv.output_path) AS group_start_seconds,
                            MAX(st.end_seconds) OVER (PARTITION BY stv.output_path) AS group_end_seconds,
                            stv.keep_intervals,
                            vc.video_id, vc.title AS chapter_title, vc.description,
                            vc.relevance_score, vc.key_speakers, vc.speakers,
                            vc.start_time, vc.end_time,
                            ysv.session_number, ysv.session_date, stv.materialized_at,
                            stv.resolved_participant_slug,
                            stv.speaker_resolution_confidence
                        FROM {stv_table} stv
                        JOIN {st_table} st ON stv.turn_id = st.turn_id
                        JOIN {vc_table} vc ON st.chapter_id = vc.chapter_id
                        JOIN {ysv_table} ysv ON vc.video_id = ysv.video_id
                        WHERE stv.prepared_at IS NULL
                          AND stv.is_uploaded_to_youtube = FALSE
                          AND vc.is_uploaded_to_youtube = FALSE
                          AND vc.relevance_score >= 2
                          AND COALESCE(st.interest_score, 1) >= 1
                          AND NOT st.is_procedural
                        ORDER BY stv.output_path, stv.turn_id
                    ) dedup
                    ORDER BY COALESCE(dedup.interest_score, 1) DESC,
                             dedup.relevance_score DESC, dedup.session_date DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                turns = cur.fetchall()
                logger.info(
                    "select_unprepared_turns: %d unprepared turns (limit=%d)",
                    len(turns),
                    limit,
                )
                return turns

    def mark_turn_prepared(self, turn_id: int) -> None:
        """Set prepared_at = now() for all speaker_turn_videos rows sharing this
        turn's output_path.

        Marks **all** ``speaker_turn_videos`` rows sharing this turn's
        ``output_path`` (grouped turns, issue #129/#237), not just the given
        ``turn_id``, mirroring ``mark_turns_uploaded``/``mark_turn_resolved``.

        Called LAST by the prepare pipeline, only after:
        - All four sidecars (thumbnail.png, title.txt, description.txt, subtitles.srt)
          have been written to disk successfully, AND
        - ffprobe returns rc==0 confirming video integrity.

        Must NOT update is_uploaded_to_youtube (that is the upload gate, not prepare gate).

        Args:
            turn_id: FK / PK of any speaker_turn_videos row whose output_path
                group should be marked prepared.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table}
                    SET prepared_at = NOW()
                    WHERE output_path = (
                        SELECT output_path FROM {stv_table} WHERE turn_id = %s
                    )
                    """,
                    (turn_id,),
                )
                logger.info(
                    "mark_turn_prepared: turn_id=%s marked prepared (%s sibling rows sharing output_path)",
                    turn_id,
                    cur.rowcount,
                )

    def mark_turn_resolved(
        self,
        output_path: str,
        slug: str,
        confidence: float,
        method: str,
        representative_turn_id: int,
    ) -> None:
        """Persist the speaker resolution result for label-matching sibling rows.

        Updates resolved_participant_slug, speaker_resolution_confidence, and
        speaker_resolution_method on the speaker_turn_videos rows that share
        BOTH the given output_path AND the representative turn's
        speaker_turns.speaker_label (Gate A, issue #321). A grouped clip can
        contain multiple diarization speaker_label values; a sibling whose
        label differs from the representative's never receives the
        representative's resolved slug (resolved_participant_slug stays NULL).

        Args:
            output_path: Absolute path to the grouped turn's video.mp4 file.
            slug: Validated participant slug from congress_participants.
            confidence: Model confidence in [0.0, 1.0].
            method: Resolution method, one of 'ai_srt_context', 'fuzzy', 'manual'.
            representative_turn_id: turn_id of the turn that was actually
                resolved — its speaker_label scopes which sibling rows may
                receive the write. Required (no default) so an un-migrated
                caller fails loudly instead of silently blanket-writing.
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")
        st_table = self.pg_conn.get_qualified_table("speaker_turns")

        group_size = self._count_records("speaker_turn_videos", "output_path = %s", (output_path,))

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table}
                    SET resolved_participant_slug = %s,
                        speaker_resolution_confidence = %s,
                        speaker_resolution_method = %s
                    WHERE turn_id IN (
                        SELECT stv2.turn_id
                        FROM {stv_table} stv2
                        JOIN {st_table} st2 ON stv2.turn_id = st2.turn_id
                        WHERE stv2.output_path = %s
                          AND st2.speaker_label = (
                              SELECT st3.speaker_label FROM {st_table} st3
                              WHERE st3.turn_id = %s
                          )
                    )
                    """,
                    (slug, confidence, method, output_path, representative_turn_id),
                )
                logger.info(
                    "mark_turn_resolved: output_path=%s representative_turn_id=%s → "
                    "slug=%r confidence=%.2f method=%s (%d/%d sibling rows updated; "
                    "label-mismatched siblings withheld)",
                    output_path,
                    representative_turn_id,
                    slug,
                    confidence,
                    method,
                    cur.rowcount,
                    group_size,
                )

    def promote_turn_type_to_qa(self, output_path: str) -> int:
        """Promote-only monologue->qa write-back after speaker resolution.

        Fires from the prepare loop (issue #282 rule 4) when a newly
        resolved participant name differs from an already-distinct real
        name in the same output_path group — evidence the group holds
        >=2 real speakers that classify_turn_type's acoustic label path
        did not catch at materialization time. Idempotent: a second call
        for an already-'qa' output_path affects zero rows. Never demotes
        an existing 'qa' back to 'monologue'.

        Args:
            output_path: Grouped turn's video.mp4 path (shared key across
                all speaker_turn_videos rows in the group).

        Returns:
            Number of rows updated (0 when already 'qa' or output_path unknown).
        """
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {stv_table}
                    SET turn_type = 'qa'
                    WHERE output_path = %s
                      AND turn_type = 'monologue'
                    """,
                    (output_path,),
                )
                logger.info(
                    "promote_turn_type_to_qa: output_path=%s (rowcount=%d)",
                    output_path,
                    cur.rowcount,
                )
                return cur.rowcount

    def mark_chapter_resolved(self, chapter_id: int, slug: str) -> None:
        """Never-override write-back of a resolved chapter speaker slug (issue #263).

        Guarded UPDATE that only fills a NULL resolved_participant_slug —
        the upload seam is a lower-trust second chance and must never
        override a slug already written by the monitor-time seam.

        Args:
            chapter_id: PK of the video_chapters row.
            slug: Roster-validated participant slug from
                :func:`chapter_speaker_resolution.resolve_chapter_speakers`.
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {chapters_table}
                    SET resolved_participant_slug = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chapter_id = %s
                      AND resolved_participant_slug IS NULL
                    """,
                    (slug, chapter_id),
                )
                logger.info(
                    "mark_chapter_resolved: chapter_id=%s → slug=%r (rowcount=%d)",
                    chapter_id,
                    slug,
                    cur.rowcount,
                )

    def count_pending_uploadable_turns(self) -> int:
        """Returns the count of speaker turn videos pending upload."""
        count = self._count_records("uploadable_turns")
        logger.info(f"Pending uploadable turns: {count}")
        return count

    def count_chapters_uploaded_today(self) -> int:
        """Returns the number of chapters uploaded to YouTube today (UTC date)."""
        count = self._count_records("video_chapters", "youtube_upload_date >= CURRENT_DATE")
        logger.info(f"Chapters uploaded today: {count}")
        return count

    def count_turns_uploaded_today(self) -> int:
        """Returns the number of distinct turn videos uploaded to YouTube today (UTC date).

        Uses COUNT(DISTINCT output_path) because a single grouped video can
        span multiple speaker_turn_videos rows sharing one output_path
        (issue #244) — counting rows would over-count uploaded videos.
        """
        table = self.pg_conn.get_qualified_table("speaker_turn_videos")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(DISTINCT output_path) AS count
                    FROM {table}
                    WHERE youtube_upload_date >= CURRENT_DATE
                    """
                )
                result = cur.fetchone()
                count = result["count"] if result else 0
        logger.info(f"Turn video(s) uploaded today: {count}")
        return count

    # ==================== Post-Upload Verification (issue #141) ====================

    def select_unverified_uploads(self, min_h: int = 1, max_h: int = 48) -> list[dict]:
        """Return uploaded rows whose youtube_video_id has not yet been verified.

        Covers both video_chapters and speaker_turn_videos.  Rows are filtered
        to the configurable verification window [min_h, max_h] hours after
        youtube_upload_date so that very-recent uploads (still processing) and
        very-old uploads (past the grace period) are excluded.

        Turns are deduplicated DISTINCT ON (output_path) because multiple
        speaker_turn_videos rows share one youtube_video_id.

        Args:
            min_h: Minimum age in hours (default 1).
            max_h: Maximum age in hours (default 48).

        Returns:
            List of dicts with at least:
            ``{item_type, chapter_id|turn_id|id, youtube_video_id, output_path?}``
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        'chapter' AS item_type,
                        chapter_id AS id,
                        youtube_video_id,
                        NULL::TEXT AS output_path
                    FROM {chapters_table}
                    WHERE is_uploaded_to_youtube = TRUE
                      AND upload_verified_at IS NULL
                      AND youtube_upload_date BETWEEN NOW() - INTERVAL '%s hours'
                                                  AND NOW() - INTERVAL '%s hours'

                    UNION ALL

                    SELECT
                        'turn' AS item_type,
                        turn_id AS id,
                        youtube_video_id,
                        output_path
                    FROM (
                        SELECT DISTINCT ON (output_path)
                            turn_id,
                            youtube_video_id,
                            output_path,
                            youtube_upload_date,
                            upload_verified_at,
                            is_uploaded_to_youtube
                        FROM {stv_table}
                        ORDER BY output_path, turn_id
                    ) dedup_turns
                    WHERE is_uploaded_to_youtube = TRUE
                      AND upload_verified_at IS NULL
                      AND youtube_upload_date BETWEEN NOW() - INTERVAL '%s hours'
                                                  AND NOW() - INTERVAL '%s hours'
                    """,
                    (max_h, min_h, max_h, min_h),
                )
                rows = cur.fetchall()
                logger.info(
                    "select_unverified_uploads: %d candidates (min_h=%d, max_h=%d)",
                    len(rows),
                    min_h,
                    max_h,
                )
                return rows

    def mark_upload_verified(self, item_type: str, id_or_output_path) -> None:
        """Set upload_verified_at = NOW() for a verified upload.

        For chapters, ``id_or_output_path`` is the integer ``chapter_id``.
        For turns, ``id_or_output_path`` is the ``output_path`` string; ALL
        rows sharing that output_path are updated atomically (mirror of
        ``mark_turns_uploaded``).

        Args:
            item_type: ``"chapter"`` or ``"turn"``.
            id_or_output_path: chapter_id (int) or output_path (str).
        """
        if item_type == "chapter":
            chapters_table = self.pg_conn.get_qualified_table("video_chapters")
            with self.pg_conn.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {chapters_table}
                        SET upload_verified_at = NOW()
                        WHERE chapter_id = %s
                        """,
                        (id_or_output_path,),
                    )
            logger.info("mark_upload_verified: chapter_id=%s verified", id_or_output_path)
        elif item_type == "turn":
            stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")
            with self.pg_conn.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {stv_table}
                        SET upload_verified_at = NOW()
                        WHERE output_path = %s
                        """,
                        (id_or_output_path,),
                    )
            logger.info(
                "mark_upload_verified: output_path=%r all siblings verified",
                id_or_output_path,
            )
        else:
            raise ValueError(f"mark_upload_verified: unknown item_type={item_type!r}")

    def record_upload_verification_failure(
        self,
        item_type: str,
        id_or_output_path,
        error_message: str | None = None,
    ) -> None:
        """Record a verification failure and re-queue or permanently abandon the item.

        Behaviour mirrors ``record_chapter_upload_failure`` / ``record_short_upload_failure``:
        - upload_attempts is incremented by 1.
        - last_upload_error is updated.
        - If upload_attempts + 1 reaches CHAPTER_UPLOAD_ABANDON_THRESHOLD → is_upload_abandoned=TRUE.
        - Otherwise → is_uploaded_to_youtube=FALSE so the row re-enters the upload queue.

        For turns: ALL rows sharing ``output_path`` are updated atomically.
        ``prepared_at`` is intentionally NOT modified.

        Args:
            item_type: ``"chapter"`` or ``"turn"``.
            id_or_output_path: chapter_id (int) or output_path (str).
            error_message: Optional description of the failure.
        """
        if item_type == "chapter":
            chapters_table = self.pg_conn.get_qualified_table("video_chapters")
            with self.pg_conn.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {chapters_table} SET
                            upload_attempts = upload_attempts + 1,
                            last_upload_error = %s,
                            is_uploaded_to_youtube = (upload_attempts + 1 < {CHAPTER_UPLOAD_ABANDON_THRESHOLD}),
                            is_upload_abandoned = (upload_attempts + 1 >= {CHAPTER_UPLOAD_ABANDON_THRESHOLD}),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chapter_id = %s
                        RETURNING upload_attempts, is_upload_abandoned
                        """,
                        (error_message, id_or_output_path),
                    )
                    result = cur.fetchone()
                    if result and result.get("upload_attempts") == CHAPTER_UPLOAD_ABANDON_THRESHOLD:
                        logger.warning(
                            "record_upload_verification_failure: chapter_id=%s abandoned after %d failures",
                            id_or_output_path,
                            CHAPTER_UPLOAD_ABANDON_THRESHOLD,
                        )
                    else:
                        logger.info(
                            "record_upload_verification_failure: chapter_id=%s re-queued",
                            id_or_output_path,
                        )

        elif item_type == "turn":
            stv_table = self.pg_conn.get_qualified_table("speaker_turn_videos")
            with self.pg_conn.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {stv_table} SET
                            upload_attempts = upload_attempts + 1,
                            last_upload_error = %s,
                            is_uploaded_to_youtube = (upload_attempts + 1 < {CHAPTER_UPLOAD_ABANDON_THRESHOLD}),
                            is_upload_abandoned = (upload_attempts + 1 >= {CHAPTER_UPLOAD_ABANDON_THRESHOLD}),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE output_path = %s
                        RETURNING upload_attempts, is_upload_abandoned
                        """,
                        (error_message, id_or_output_path),
                    )
                    result = cur.fetchone()
                    if result and result.get("upload_attempts") == CHAPTER_UPLOAD_ABANDON_THRESHOLD:
                        logger.warning(
                            "record_upload_verification_failure: output_path=%r group abandoned after %d failures",
                            id_or_output_path,
                            CHAPTER_UPLOAD_ABANDON_THRESHOLD,
                        )
                    else:
                        logger.info(
                            "record_upload_verification_failure: output_path=%r group re-queued",
                            id_or_output_path,
                        )
        else:
            raise ValueError(f"record_upload_verification_failure: unknown item_type={item_type!r}")

    def count_pending_uploadable_chapters(self, min_relevance_score: int = 2) -> int:
        """Returns the number of chapters pending upload in the uploadable queue."""
        count = self._count_records("uploadable_chapters", "relevance_score >= %s", (min_relevance_score,))
        logger.info(f"Pending uploadable chapters (min_score={min_relevance_score}): {count}")
        return count

    def get_chapter_titles(self, chapter_ids: list) -> dict:
        """Returns {chapter_id: title} for the given IDs."""
        if not chapter_ids:
            return {}
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT chapter_id, title FROM {chapters_table} WHERE chapter_id = ANY(%s)",
                    (chapter_ids,),
                )
                return {row["chapter_id"]: row["title"] for row in cur.fetchall()}

    def get_chapter_metadata(self, chapter_id: int) -> dict | None:
        """Returns full chapter metadata for AI-generated YouTube Shorts title/description.

        Includes source video title, URL, session_number and session_date via LEFT JOIN
        to youtube_source_videos. All JOIN-sourced fields are None when no source video
        is linked to the chapter.
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        youtube_source_videos_table = self.pg_conn.get_qualified_table("youtube_source_videos")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT vc.chapter_id, vc.title, vc.description, vc.speakers,
                               vc.key_speakers, vc.topics, vc.scoring_reasoning,
                               vc.relevance_score, vc.youtube_video_id,
                               ysv.video_title AS source_video_title,
                               ysv.video_url   AS source_video_url,
                               ysv.session_number,
                               ysv.session_date
                        FROM {chapters_table} vc
                        LEFT JOIN {youtube_source_videos_table} ysv ON ysv.video_id = vc.video_id
                        WHERE vc.chapter_id = %s""",
                    (chapter_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def record_source_integrity_failure(self, video_id: str, retry_after_hours: int = 12) -> None:
        """Early-upsert an integrity failure for a source video.

        Sets ``download_retry_after = NOW() + retry_after_hours`` so the video is
        excluded from ``filter_unprocessed_videos`` until the VOD has had time to
        finalise on YouTube.

        Rules:
        - ``is_processed`` is intentionally NOT in the DO UPDATE SET clause so a
          row that was already marked ``is_processed = TRUE`` never regresses to FALSE.
        - ``GREATEST(...)`` ensures ``download_retry_after`` only ever advances forward;
          a second call with a *smaller* window does not decrease an existing future
          timestamp.
        - On INSERT the row starts with ``is_processed = FALSE`` and the computed
          retry timestamp.

        Args:
            video_id: YouTube video ID.
            retry_after_hours: Hours to defer re-download (default 12).
        """
        tbl = self.pg_conn.get_qualified_table("youtube_source_videos")
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        with self.pg_conn.get_connection() as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {tbl} (video_id, video_url, is_processed, download_retry_after)
                        VALUES (%s, %s, FALSE, NOW() + (%s || ' hours')::interval)
                        ON CONFLICT (video_id) DO UPDATE SET
                            download_retry_after = GREATEST(
                                {tbl}.download_retry_after,
                                EXCLUDED.download_retry_after),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (video_id, video_url, str(retry_after_hours)),
                    )

    def update_chapter_speakers(
        self,
        chapter_id: int,
        speakers: list,
        key_speakers: list,
        timeline: list,
        resolved_participant_slug: str | None = None,
    ) -> None:
        """Overwrite the speakers, key_speakers and timeline JSONB for a chapter.

        Called by normalize_chapter_speakers after confirmed AI matches have been
        applied to all three fields.

        Args:
            chapter_id: PK of the video_chapters row to update.
            speakers: Updated list of speaker display names.
            key_speakers: Updated list of key speaker display names.
            timeline: Updated list of timeline dicts (each has 'speaker' field).
            resolved_participant_slug: When provided, also writes the FK column
                ``resolved_participant_slug`` linking this chapter to a verified
                ``congress_participants`` row. Omit (or pass None) to leave the
                column unchanged.
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                if resolved_participant_slug is not None:
                    cur.execute(
                        f"""
                        UPDATE {chapters_table}
                        SET speakers = %s,
                            key_speakers = %s,
                            timeline = %s::jsonb,
                            resolved_participant_slug = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chapter_id = %s
                        """,
                        (speakers, key_speakers, json.dumps(timeline), resolved_participant_slug, chapter_id),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {chapters_table}
                        SET speakers = %s,
                            key_speakers = %s,
                            timeline = %s::jsonb,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chapter_id = %s
                        """,
                        (speakers, key_speakers, json.dumps(timeline), chapter_id),
                    )
                logger.info(
                    "update_chapter_speakers: updated chapter %d (%d speakers, %d key_speakers, %d timeline entries%s)",
                    chapter_id,
                    len(speakers),
                    len(key_speakers),
                    len(timeline),
                    f", slug={resolved_participant_slug!r}" if resolved_participant_slug else "",
                )

    def get_processed_video_ids(self, video_ids: list[str]) -> set[str]:
        """Return the subset of video_ids that should be skipped for download.

        A video is skipped when EITHER:
        - ``is_processed = TRUE`` (already fully processed), OR
        - ``download_retry_after > NOW()`` (deferred within the integrity retry window).

        Cheap pre-download idempotency check. Empty input -> empty set
        (no query executed). Read-only; raises on DB error (fail-closed).
        """
        if not video_ids:
            return set()

        youtube_videos_table = self.pg_conn.get_qualified_table("youtube_source_videos")

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT video_id
                    FROM {youtube_videos_table}
                    WHERE video_id = ANY(%s)
                      AND (is_processed = TRUE OR download_retry_after > NOW())
                    """,
                    (video_ids,),
                )
                return {row["video_id"] for row in cur.fetchall()}

    # ==================== Video Analytics ====================

    def get_pending_analytics_checkpoints(self) -> list:
        """Return candidate video_chapters rows for analytics collection.

        Returns chapters that:
        - have a non-null youtube_video_id
        - are marked as uploaded to YouTube
        - were uploaded within the last 90 days

        The pure function pending_checkpoints() in video_analytics.py expands
        these rows into (youtube_video_id, checkpoint) pairs and excludes
        already-collected pairs, so this query stays simple.

        Returns:
            List of row dicts: {chapter_id, youtube_video_id, youtube_upload_date}
        """
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT chapter_id, youtube_video_id, youtube_upload_date
                    FROM {chapters_table}
                    WHERE youtube_video_id IS NOT NULL
                      AND is_uploaded_to_youtube = TRUE
                      AND youtube_upload_date >= NOW() - INTERVAL '90 days'
                    ORDER BY youtube_upload_date DESC
                    """,
                )
                rows = cur.fetchall()
                logger.info(
                    "get_pending_analytics_checkpoints: %d candidate chapters found",
                    len(rows),
                )
                return list(rows)

    def get_collected_analytics_pairs(self, youtube_video_ids: list[str]) -> set[tuple[str, str]]:
        """Return already-collected (youtube_video_id, checkpoint) pairs.

        Used by the hourly analytics DAG to skip re-fetching data already
        persisted in video_analytics_snapshots, saving Analytics API quota.

        Args:
            youtube_video_ids: List of YouTube video IDs to check. An empty
                list returns an empty set immediately without hitting the DB.

        Returns:
            Set of (youtube_video_id, checkpoint) tuples for which a snapshot
            row already exists.
        """
        if not youtube_video_ids:
            return set()

        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT youtube_video_id, checkpoint
                    FROM {snapshots_table}
                    WHERE youtube_video_id = ANY(%s)
                    """,
                    (youtube_video_ids,),
                )
                rows = cur.fetchall()
                logger.info(
                    "get_collected_analytics_pairs: %d already-collected pairs for %d video ids",
                    len(rows),
                    len(youtube_video_ids),
                )
                return {(row["youtube_video_id"], row["checkpoint"]) for row in rows}

    def record_analytics_snapshot(
        self,
        chapter_id: int,
        youtube_video_id: str,
        checkpoint: str,
        metrics: dict,
    ) -> None:
        """Persist one analytics snapshot row (ON CONFLICT DO NOTHING).

        Writes the METRIC_FIELDS metrics as a JSONB blob. Does NOT write
        action_taken (reserved NULL placeholder for issue #102).

        Args:
            chapter_id: FK to video_chapters.chapter_id.
            youtube_video_id: YouTube video ID string.
            checkpoint: One of '24h','48h','7d','30d','90d'.
            metrics: Dict keyed by config.analytics_config.METRIC_FIELDS (views,
                estimatedMinutesWatched, averageViewDuration,
                averageViewPercentage, likes, dislikes, comments, shares,
                subscribersGained, subscribersLost).
        """
        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {snapshots_table}
                        (chapter_id, youtube_video_id, checkpoint, metrics)
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (youtube_video_id, checkpoint) DO NOTHING
                    """,
                    (chapter_id, youtube_video_id, checkpoint, json.dumps(metrics)),
                )
                logger.info(
                    "record_analytics_snapshot: chapter_id=%d yt_id=%s checkpoint=%s",
                    chapter_id,
                    youtube_video_id,
                    checkpoint,
                )

    # ---- Checkpoint actions (issue #102) ----

    def get_unactioned_snapshots(self) -> list:
        """Return NULL-action_taken snapshot rows joined to video_chapters.

        Provides both the analytics fields needed by evaluate_action()
        (checkpoint, metrics) and the conf fields needed to regenerate a
        thumbnail/title for the video (chapter_title, description,
        session_number, session_date, key_speakers, resolved_participant_slug
        — the same shape _prepare_thumbnail_config() consumes).

        Returns:
            List of row dicts: {snapshot_id, chapter_id, youtube_video_id,
            checkpoint, metrics, collected_at, chapter_title, description,
            session_number, session_date, key_speakers,
            resolved_participant_slug}. ``collected_at`` lets callers report
            how stale the measurement behind an action was (issue #311).
        """
        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        chapters_table = self.pg_conn.get_qualified_table("video_chapters")
        source_videos_table = self.pg_conn.get_qualified_table("youtube_source_videos")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        s.snapshot_id, s.chapter_id, s.youtube_video_id,
                        s.checkpoint, s.metrics, s.collected_at,
                        vc.title AS chapter_title, vc.description,
                        ysv.session_number, ysv.session_date,
                        vc.key_speakers, vc.resolved_participant_slug
                    FROM {snapshots_table} s
                    JOIN {chapters_table} vc ON vc.chapter_id = s.chapter_id
                    LEFT JOIN {source_videos_table} ysv ON ysv.video_id = vc.video_id
                    WHERE s.action_taken IS NULL
                    ORDER BY s.collected_at ASC
                    """,
                )
                rows = cur.fetchall()
                logger.info(
                    "get_unactioned_snapshots: %d candidate snapshots found",
                    len(rows),
                )
                return list(rows)

    def get_checkpoint_view_medians(self) -> dict:
        """Return the channel's own historical median views per checkpoint.

        Single grouped query across all checkpoints. The evaluated video's
        own snapshot is included in the median (strictly conservative — see
        spec "Self-inclusion is conservative"); the caller excludes self from
        the sample-size gate arithmetically (sample_size - 1 >= threshold).

        Returns:
            Dict keyed by checkpoint: {checkpoint: {median_views, sample_size}}.
        """
        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        checkpoint,
                        percentile_cont(0.5) WITHIN GROUP (
                            ORDER BY (metrics->>'views')::numeric
                        ) AS median_views,
                        COUNT(*) AS sample_size
                    FROM {snapshots_table}
                    GROUP BY checkpoint
                    """,
                )
                rows = cur.fetchall()
                return {
                    row["checkpoint"]: {
                        "median_views": row["median_views"],
                        "sample_size": row["sample_size"],
                    }
                    for row in rows
                }

    def get_video_action_history(self, youtube_video_ids: list) -> dict:
        """Return consumed lifetime action-cap slots per video.

        `in_progress` rows count as consumed slots alongside completed
        `thumbnail_regenerated`/`thumbnail_and_title_regenerated` records —
        conservative, because the external YouTube write may already have
        happened for an in_progress row even though it was never finalized.
        Whether an in_progress row also consumes a title slot is derived
        from its own checkpoint column (title actions only ever occur at
        TITLE_UPDATE_CHECKPOINTS).

        Args:
            youtube_video_ids: Video IDs to look up. Empty list returns an
                empty dict immediately without hitting the DB.

        Returns:
            Dict keyed by youtube_video_id: {"thumbnail": int, "title": int}.
            Every requested id is present, defaulting to zero counts.
        """
        from congress_videos.config.analytics_config import TITLE_UPDATE_CHECKPOINTS

        history = {yt_id: {"thumbnail": 0, "title": 0} for yt_id in youtube_video_ids}
        if not youtube_video_ids:
            return history

        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT youtube_video_id, checkpoint, action_taken
                    FROM {snapshots_table}
                    WHERE youtube_video_id = ANY(%s)
                      AND action_taken IN (
                          'in_progress',
                          'thumbnail_regenerated',
                          'thumbnail_and_title_regenerated'
                      )
                    """,
                    (youtube_video_ids,),
                )
                rows = cur.fetchall()

        for row in rows:
            yt_id = row["youtube_video_id"]
            action = row["action_taken"]
            checkpoint = row["checkpoint"]
            entry = history.setdefault(yt_id, {"thumbnail": 0, "title": 0})

            entry["thumbnail"] += 1
            if (
                action == "thumbnail_and_title_regenerated"
                or action == "in_progress"
                and checkpoint in TITLE_UPDATE_CHECKPOINTS
            ):
                entry["title"] += 1

        return history

    def get_chosen_thumbnail(self, chapter_id: int) -> dict | None:
        """Return the is_chosen=TRUE video_thumbnails row for a chapter.

        Includes the persisted archetype (migration 041) so a later
        regeneration can pass it as the anti-convergence exclusion.

        Returns:
            Row dict, or None if no chosen thumbnail exists yet.
        """
        thumbnails_table = self.pg_conn.get_qualified_table("video_thumbnails")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM {thumbnails_table}
                    WHERE chapter_id = %s AND is_chosen = TRUE
                    LIMIT 1
                    """,
                    (chapter_id,),
                )
                return cur.fetchone()

    def claim_snapshot_action(self, snapshot_id: int) -> bool:
        """Claim a snapshot row for automated action before any external call.

        Rowcount-gated: sets action_taken='in_progress' only when it is
        still NULL. A concurrent/second claim attempt on the same row
        affects zero rows and returns False.

        Returns:
            True if this call claimed the row, False if it was already
            claimed (or otherwise no longer NULL).
        """
        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {snapshots_table}
                    SET action_taken = 'in_progress'
                    WHERE snapshot_id = %s AND action_taken IS NULL
                    """,
                    (snapshot_id,),
                )
                claimed = cur.rowcount == 1
                logger.info(
                    "claim_snapshot_action: snapshot_id=%d claimed=%s",
                    snapshot_id,
                    claimed,
                )
                return claimed

    def mark_action_taken(self, snapshot_id: int, action: str, detail: dict) -> None:
        """Write the final action_taken literal and action_detail audit JSONB.

        Args:
            snapshot_id: FK to video_analytics_snapshots.snapshot_id.
            action: Final action_taken value (must satisfy migration 041's
                CHECK constraint — 'in_progress' is not a valid terminal
                value passed here; use claim_snapshot_action for that).
            detail: Audit payload — see design's action_detail shape.
        """
        snapshots_table = self.pg_conn.get_qualified_table("video_analytics_snapshots")
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {snapshots_table}
                    SET action_taken = %s, action_detail = %s::jsonb
                    WHERE snapshot_id = %s
                    """,
                    (action, json.dumps(detail), snapshot_id),
                )
                logger.info(
                    "mark_action_taken: snapshot_id=%d action=%s",
                    snapshot_id,
                    action,
                )
