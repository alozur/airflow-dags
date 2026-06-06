# congreso_youtube/congress_database.py
"""
Database operations specific to congressional video management.
"""
from utils.postgres_helpers import PostgresConnection
from typing import Dict, List, Optional, Any
from datetime import date
import logging

logger = logging.getLogger(__name__)

class CongressionalVideoDB:
    """Database operations for congressional video management"""

    def __init__(self):
        self.pg_conn = PostgresConnection()
        # Get schema-qualified table names
        self.sessions_table = self.pg_conn.get_qualified_table('congressional_sessions')
        self.topics_table = self.pg_conn.get_qualified_table('video_topics')
        self.queue_table = self.pg_conn.get_qualified_table('upload_queue')
        self.uploadable_view = self.pg_conn.get_qualified_table('uploadable_videos')

    def create_or_update_session(self, session_number: int, session_date: date,
                               target_date: date, session_url: str = None) -> int:
        """
        Create or update a congressional session record
        Returns the session_number
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # Try to update existing session first
                cur.execute(f"""
                    UPDATE {self.sessions_table}
                    SET session_url = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE session_number = %s AND session_date = %s
                    RETURNING session_number
                """, (session_url, session_number, session_date))

                result = cur.fetchone()
                if result:
                    logger.info(f"Updated existing session {session_number}")
                    return result['session_number']

                # Insert new session if not found
                cur.execute(f"""
                    INSERT INTO {self.sessions_table}
                    (session_number, session_date, target_date, session_url)
                    VALUES (%s, %s, %s, %s)
                    RETURNING session_number
                """, (session_number, session_date, target_date, session_url))

                result = cur.fetchone()
                returned_session_number = result['session_number']
                logger.info(f"Created new session {returned_session_number}")
                return returned_session_number

    def upsert_video_topic(self, session_number: int, entry_id: str, topic_data: Dict[str, Any]) -> str:
        """
        Insert or update a video topic record
        Returns the video topic entry_id
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # Check if topic already exists
                cur.execute(f"""
                    SELECT entry_id FROM {self.topics_table}
                    WHERE entry_id = %s
                """, (entry_id,))

                existing = cur.fetchone()

                if existing:
                    # Update existing topic
                    is_main_topic = topic_data.get('is_main_topic', False)
                    # upload_eligible is TRUE only for main topics
                    upload_eligible = is_main_topic

                    cur.execute(f"""
                        UPDATE {self.topics_table} SET
                            session_number = %s,
                            topic_title = %s,
                            video_url = %s,
                            video_file_path = %s,
                            speaker_name = %s,
                            role = %s,
                            profile_link = %s,
                            main_topic_entry_id = %s,
                            file_size_bytes = %s,
                            duration_seconds = %s,
                            is_main_topic = %s,
                            upload_eligible = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE entry_id = %s
                        RETURNING entry_id
                    """, (
                        session_number,
                        topic_data.get('topic_title'),
                        topic_data.get('video_url'),
                        topic_data.get('video_file_path'),
                        topic_data.get('speaker_name'),
                        topic_data.get('role'),
                        topic_data.get('profile_link'),
                        topic_data.get('main_topic_entry_id'),
                        topic_data.get('file_size_bytes'),
                        topic_data.get('duration_seconds'),
                        is_main_topic,
                        upload_eligible,
                        entry_id
                    ))
                    logger.info(f"Updated video topic {entry_id}")
                else:
                    # Insert new topic
                    is_main_topic = topic_data.get('is_main_topic', False)
                    # upload_eligible is TRUE only for main topics
                    upload_eligible = is_main_topic

                    cur.execute(f"""
                        INSERT INTO {self.topics_table}
                        (entry_id, session_number, topic_title, video_url, video_file_path,
                         speaker_name, role, profile_link, main_topic_entry_id, file_size_bytes, duration_seconds, is_main_topic, upload_eligible)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING entry_id
                    """, (
                        entry_id,
                        session_number,
                        topic_data.get('topic_title'),
                        topic_data.get('video_url'),
                        topic_data.get('video_file_path'),
                        topic_data.get('speaker_name'),
                        topic_data.get('role'),
                        topic_data.get('profile_link'),
                        topic_data.get('main_topic_entry_id'),
                        topic_data.get('file_size_bytes'),
                        topic_data.get('duration_seconds'),
                        is_main_topic,
                        upload_eligible
                    ))
                    logger.info(f"Created new video topic {entry_id}")

                return entry_id

    # REMOVED: update_openai_classification() - OpenAI classification fields removed from schema
    # The following fields were removed from video_topics table:
    # - openai_category, openai_summary, openai_keywords, openai_priority_score, openai_processed_at
    # Only ai_interest_score is used now for YouTube upload prioritization

    def mark_video_uploaded(self, video_topic_entry_id: str, youtube_video_id: str):
        """Mark a video as uploaded to YouTube"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (youtube_video_id, video_topic_entry_id))
                logger.info(f"Marked video topic {video_topic_entry_id} as uploaded: {youtube_video_id}")

    def get_uploadable_videos(self, limit: int = None) -> List[Dict]:
        """Get videos eligible for YouTube upload"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"SELECT * FROM {self.uploadable_view}"
                if limit:
                    query += f" LIMIT {limit}"

                cur.execute(query)
                return cur.fetchall()

    def add_to_upload_queue(self, video_topic_entry_id: str, priority: int = 5):
        """Add a video to the upload queue"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {self.queue_table} (video_topic_entry_id, queue_priority)
                    VALUES (%s, %s)
                    ON CONFLICT (video_topic_entry_id) DO UPDATE SET
                        queue_priority = EXCLUDED.queue_priority,
                        queued_at = CURRENT_TIMESTAMP
                """, (video_topic_entry_id, priority))
                logger.info(f"Added video topic {video_topic_entry_id} to upload queue with priority {priority}")

    def get_session_by_number_and_date(self, session_number: int, session_date: date) -> Optional[Dict]:
        """Get session by session number and date"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.sessions_table}
                    WHERE session_number = %s AND session_date = %s
                """, (session_number, session_date))
                return cur.fetchone()

    def get_video_topics_by_session(self, session_number: int) -> List[Dict]:
        """Get all video topics for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.topics_table}
                    WHERE session_number = %s
                    ORDER BY created_at
                """, (session_number,))
                return cur.fetchall()

    def update_session_total_topics(self, session_number: int):
        """Update the total_topics count for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.sessions_table} SET
                        total_topics = (
                            SELECT COUNT(*) FROM {self.topics_table}
                            WHERE session_number = %s
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE session_number = %s
                """, (session_number, session_number))
                logger.info(f"Updated total_topics count for session {session_number}")

    def update_download_info(self, video_topic_entry_id: str, file_path: str, file_size: int = None, duration: int = None):
        """Update download information for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        video_file_path = %s,
                        file_size_bytes = %s,
                        duration_seconds = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (file_path, file_size, duration, video_topic_entry_id))
                logger.info(f"Updated download info for video topic {video_topic_entry_id}")

    def update_main_topic_status(self, video_topic_entry_id: str, is_main_topic: bool):
        """Update the is_main_topic status for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        is_main_topic = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (is_main_topic, video_topic_entry_id))
                logger.info(f"Updated main topic status for video topic {video_topic_entry_id} to {is_main_topic}")

    def get_main_topics_by_session(self, session_number: int) -> List[Dict]:
        """Get all main topics for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.topics_table}
                    WHERE session_number = %s AND is_main_topic = TRUE
                    ORDER BY created_at
                """, (session_number,))
                return cur.fetchall()

    def update_youtube_metadata(self, video_topic_entry_id: str, youtube_title: str, youtube_description: str):
        """Update YouTube metadata for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        youtube_title = %s,
                        youtube_description = %s,
                        youtube_metadata_generated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (youtube_title, youtube_description, video_topic_entry_id))
                logger.info(f"Updated YouTube metadata for video topic {video_topic_entry_id}")

    def update_thumbnail_info(self, video_topic_entry_id: str, thumbnail_text: str, thumbnail_path: str):
        """Update thumbnail information for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        thumbnail_text = %s,
                        thumbnail_path = %s,
                        thumbnail_generated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (thumbnail_text, thumbnail_path, video_topic_entry_id))
                logger.info(f"Updated thumbnail info for video topic {video_topic_entry_id}")

    def get_interventions_by_main_topic(self, main_topic_entry_id: str) -> List[Dict]:
        """Get all interventions for a specific main topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.topics_table}
                    WHERE main_topic_entry_id = %s AND is_main_topic = FALSE
                    ORDER BY created_at
                """, (main_topic_entry_id,))
                return cur.fetchall()

    def update_ai_interest_evaluation(self, video_topic_entry_id: str, interest_score: int, reasoning: str):
        """Update AI interest evaluation for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        ai_interest_score = %s,
                        ai_interest_reasoning = %s,
                        ai_interest_evaluated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (interest_score, reasoning, video_topic_entry_id))
                logger.info(f"Updated AI interest evaluation for video topic {video_topic_entry_id}: score={interest_score}")

    def get_top_videos_for_upload(self, max_videos: int = 5, min_score: int = 6) -> List[Dict]:
        """
        Get top videos by AI interest score for YouTube upload.
        Only returns videos that:
        - Are main topics (not interventions)
        - Have not been uploaded yet
        - Have AI interest score >= min_score
        - Are upload eligible

        Args:
            max_videos: Maximum number of videos to return
            min_score: Minimum AI interest score

        Returns:
            List of video records ordered by AI interest score (highest first)
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.topics_table}
                    WHERE is_main_topic = TRUE
                      AND is_uploaded_to_youtube = FALSE
                      AND upload_eligible = TRUE
                      AND ai_interest_score >= %s
                      AND ai_interest_score IS NOT NULL
                    ORDER BY ai_interest_score DESC, created_at DESC
                    LIMIT %s
                """, (min_score, max_videos))
                videos = cur.fetchall()
                logger.info(f"Found {len(videos)} videos for upload (max={max_videos}, min_score={min_score})")
                return videos

    def update_youtube_upload_status(self, video_topic_entry_id: str, youtube_video_id: str):
        """Mark a video as uploaded to YouTube with the YouTube video ID"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.topics_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entry_id = %s
                """, (youtube_video_id, video_topic_entry_id))
                logger.info(f"Marked video {video_topic_entry_id} as uploaded to YouTube: {youtube_video_id}")

    def get_videos_from_upload_queue(self, limit: int = 5) -> List[Dict]:
        """
        Get videos from upload queue with status 'pending' or 'failed'.
        Returns videos ordered by priority (lowest number = highest priority).

        Args:
            limit: Maximum number of videos to return

        Returns:
            List of video records from the uploadable_videos view
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.uploadable_view}
                    LIMIT %s
                """, (limit,))
                videos = cur.fetchall()
                logger.info(f"Retrieved {len(videos)} videos from upload queue (limit={limit})")
                return videos

    def add_videos_to_upload_queue(self, video_entry_ids: List[str], base_priority: int = 5):
        """
        Add multiple videos to the upload queue.
        Queue priority matches AI interest score directly (1-10, where 10 = highest priority).

        Args:
            video_entry_ids: List of entry_ids to add to queue
            base_priority: Base priority value (used as fallback if no AI score)
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                for entry_id in video_entry_ids:
                    # Get AI interest score to use as priority
                    cur.execute(f"""
                        SELECT ai_interest_score FROM {self.topics_table}
                        WHERE entry_id = %s
                    """, (entry_id,))
                    result = cur.fetchone()
                    ai_score = result['ai_interest_score'] if result and result['ai_interest_score'] else base_priority

                    # Priority = AI score directly (1-10, where 10 is highest)
                    priority = max(1, min(10, ai_score))

                    cur.execute(f"""
                        INSERT INTO {self.queue_table} (video_topic_entry_id, queue_priority)
                        VALUES (%s, %s)
                        ON CONFLICT (video_topic_entry_id) DO UPDATE SET
                            queue_priority = EXCLUDED.queue_priority,
                            upload_status = 'pending',
                            queued_at = CURRENT_TIMESTAMP
                    """, (entry_id, priority))
                    logger.info(f"Added {entry_id} to upload queue with priority {priority} (AI score: {ai_score})")

    def update_upload_queue_status(self, video_topic_entry_id: str, status: str, error_message: str = None):
        """
        Update the status of a video in the upload queue.

        Args:
            video_topic_entry_id: The entry_id of the video
            status: New status ('pending', 'processing', 'completed', 'failed', 'skipped')
            error_message: Optional error message for failed uploads
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # Only increment attempted_uploads for actual upload attempts (completed or failed)
                # Don't increment for 'processing' (just downloaded) or 'pending'
                if status in ('completed', 'failed'):
                    cur.execute(f"""
                        UPDATE {self.queue_table} SET
                            upload_status = %s,
                            error_message = %s,
                            last_attempt_at = CURRENT_TIMESTAMP,
                            attempted_uploads = attempted_uploads + 1
                        WHERE video_topic_entry_id = %s
                    """, (status, error_message, video_topic_entry_id))
                else:
                    cur.execute(f"""
                        UPDATE {self.queue_table} SET
                            upload_status = %s,
                            error_message = %s,
                            last_attempt_at = CURRENT_TIMESTAMP
                        WHERE video_topic_entry_id = %s
                    """, (status, error_message, video_topic_entry_id))
                logger.info(f"Updated upload queue status for {video_topic_entry_id}: {status}")

    def remove_from_upload_queue(self, video_topic_entry_id: str):
        """Remove a video from the upload queue (after successful upload)"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    DELETE FROM {self.queue_table}
                    WHERE video_topic_entry_id = %s
                """, (video_topic_entry_id,))
                logger.info(f"Removed {video_topic_entry_id} from upload queue")

    # ==================== YouTube Chapter Management ====================

    def save_youtube_chapters_to_db(self, scored_chapters_data: Dict[str, Any], session_number: int = None, session_date: date = None) -> Dict[str, Any]:
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
                        'error': str or None
                    }
                ]
            }
        """
        if not scored_chapters_data or not scored_chapters_data.get('videos'):
            logger.warning("No scored chapters data to save")
            return {
                'total_videos_saved': 0,
                'total_chapters_saved': 0,
                'videos': []
            }

        save_results = {
            'total_videos_saved': 0,
            'total_chapters_saved': 0,
            'videos': []
        }

        youtube_videos_table = self.pg_conn.get_qualified_table('youtube_source_videos')
        chapters_table = self.pg_conn.get_qualified_table('video_chapters')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                for video_data in scored_chapters_data['videos']:
                    video_id = video_data.get('video_id')
                    video_title = video_data.get('video_title', 'Unknown Video')
                    scored_chapters = video_data.get('scored_chapters', [])

                    if video_data.get('error'):
                        logger.warning(f"Skipping video {video_id} due to error: {video_data.get('error')}")
                        save_results['videos'].append({
                            'video_id': video_id,
                            'chapters_saved': 0,
                            'error': video_data.get('error')
                        })
                        continue

                    try:
                        # Step 1: Upsert YouTube source video
                        video_url = f"https://www.youtube.com/watch?v={video_id}"
                        total_chapters = len(scored_chapters)

                        cur.execute(f"""
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
                        """, (video_id, video_title, video_url, session_number, session_date, total_chapters))

                        logger.info(f"Saved/updated YouTube source video: {video_id}")

                        # Step 2: Save all chapters for this video
                        chapters_saved_count = 0

                        for chapter in scored_chapters:
                            # Extract chapter data
                            title = chapter.get('title', 'Untitled Chapter')
                            description = chapter.get('description', '')
                            start_time = chapter.get('start_time')
                            end_time = chapter.get('end_time')
                            duration_minutes = chapter.get('duration_minutes', 0)

                            # Speaker and topic arrays
                            speakers = chapter.get('speakers', [])
                            topics = chapter.get('topics', [])

                            # Scoring data
                            relevance_score = chapter.get('relevance_score', 0)
                            speaker_pts = chapter.get('speaker_relevance_points', 0)
                            topic_pts = chapter.get('topic_relevance_points', 0)
                            interest_pts = chapter.get('public_interest_points', 0)
                            scoring_reasoning = chapter.get('scoring_reasoning', '')
                            key_speakers = chapter.get('key_speakers', speakers)
                            is_current_topic = chapter.get('is_current_topic', False)
                            scoring_error = chapter.get('scoring_error')

                            # Insert chapter (no conflict handling - allow duplicates for now)
                            # In production, you might want to add a unique constraint on (video_id, start_time, end_time)
                            cur.execute(f"""
                                INSERT INTO {chapters_table}
                                (video_id, title, description, start_time, end_time, duration_minutes,
                                 speakers, topics, relevance_score, speaker_relevance_points, topic_relevance_points,
                                 public_interest_points, scoring_reasoning, key_speakers, is_current_topic,
                                 scoring_error, scored_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                RETURNING chapter_id
                            """, (
                                video_id, title, description, start_time, end_time, duration_minutes,
                                speakers, topics, relevance_score, speaker_pts, topic_pts,
                                interest_pts, scoring_reasoning, key_speakers, is_current_topic,
                                scoring_error
                            ))

                            chapter_id = cur.fetchone()['chapter_id']
                            chapters_saved_count += 1

                            logger.info(
                                f"Saved chapter {chapter_id}: '{title}' (score: {relevance_score}/5)"
                            )

                        save_results['total_videos_saved'] += 1
                        save_results['total_chapters_saved'] += chapters_saved_count
                        save_results['videos'].append({
                            'video_id': video_id,
                            'chapters_saved': chapters_saved_count,
                            'error': None
                        })

                        logger.info(
                            f"Successfully saved {chapters_saved_count} chapters for video {video_id}"
                        )

                    except Exception as e:
                        error_msg = f"Error saving chapters for video {video_id}: {str(e)}"
                        logger.error(error_msg, exc_info=True)
                        save_results['videos'].append({
                            'video_id': video_id,
                            'chapters_saved': 0,
                            'error': error_msg
                        })

        logger.info(
            f"Chapter save complete: {save_results['total_videos_saved']} videos, "
            f"{save_results['total_chapters_saved']} chapters saved to database"
        )

        return save_results

    def get_uploadable_chapters(self, limit: int = None, min_relevance_score: int = 4) -> List[Dict]:
        """
        Get chapters eligible for YouTube upload.

        Args:
            limit: Maximum number of chapters to return
            min_relevance_score: Minimum relevance score (default: 4/5)

        Returns:
            List of chapter records from the uploadable_chapters view
        """
        uploadable_chapters_view = self.pg_conn.get_qualified_table('uploadable_chapters')

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
                    f"Retrieved {len(chapters)} uploadable chapters "
                    f"(min_score={min_relevance_score}, limit={limit})"
                )
                return chapters

    def get_chapter_statistics(self, video_id: str = None) -> List[Dict]:
        """
        Get statistics about chapters, optionally filtered by video_id.

        Args:
            video_id: Optional YouTube video ID to filter statistics

        Returns:
            List of statistics records from the chapter_statistics view
        """
        chapter_stats_view = self.pg_conn.get_qualified_table('chapter_statistics')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                if video_id:
                    cur.execute(f"""
                        SELECT * FROM {chapter_stats_view}
                        WHERE video_id = %s
                    """, (video_id,))
                else:
                    cur.execute(f"SELECT * FROM {chapter_stats_view}")

                stats = cur.fetchall()
                logger.info(f"Retrieved statistics for {len(stats)} videos")
                return stats

    def mark_chapter_uploaded(self, chapter_id: int, youtube_video_id: str):
        """
        Mark a chapter as uploaded to YouTube.

        Args:
            chapter_id: Database ID of the chapter
            youtube_video_id: YouTube video ID of the uploaded chapter video
        """
        chapters_table = self.pg_conn.get_qualified_table('video_chapters')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {chapters_table} SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chapter_id = %s
                """, (youtube_video_id, chapter_id))
                logger.info(f"Marked chapter {chapter_id} as uploaded to YouTube: {youtube_video_id}")

    # ==================== Video Shorts (Reap Pipeline) ====================

    def get_chapters_for_shorts(self, limit: int = 3, min_relevance_score: int = 3) -> List[Dict]:
        """
        Get video chapters eligible for Reap Shorts processing.

        A chapter qualifies when ALL of the following hold:
        - is_uploaded_to_youtube = TRUE (only already-published chapters)
        - relevance_score >= min_relevance_score
        - Duration between 120 and 900 seconds (2-15 min)
        - No existing video_shorts row with reap_status IN ('processing', 'downloaded')

        Args:
            limit: Maximum number of chapters to return (default 3)
            min_relevance_score: Minimum relevance score threshold (default 3)

        Returns:
            List of chapter records ordered by relevance_score DESC, created_at DESC
        """
        chapters_table = self.pg_conn.get_qualified_table('video_chapters')
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT vc.*
                    FROM {chapters_table} vc
                    WHERE vc.is_uploaded_to_youtube = TRUE
                      AND vc.relevance_score >= %s
                      AND (
                          EXTRACT(EPOCH FROM (vc.end_time::interval - vc.start_time::interval))
                          BETWEEN 120 AND 900
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM {shorts_table} vs
                          WHERE vs.chapter_id = vc.id
                            AND vs.reap_status IN ('processing', 'downloaded')
                      )
                    ORDER BY vc.relevance_score DESC, vc.created_at DESC
                    LIMIT %s
                """, (min_relevance_score, limit))
                chapters = cur.fetchall()
                logger.info(
                    f"Found {len(chapters)} chapters eligible for Shorts "
                    f"(min_score={min_relevance_score}, limit={limit})"
                )
                return chapters

    def insert_video_short(self, chapter_id: int, reap_project_id: str,
                           reap_status: str = "processing",
                           pretrim_start_secs: float = None,
                           pretrim_end_secs: float = None,
                           pretrim_used_srt: bool = False) -> int:
        """
        Insert a placeholder video_shorts row when a Reap job is created.

        The placeholder row has reap_clip_id=NULL; it is updated or replaced
        with per-clip rows once the sensor downloads clips on job completion.

        Args:
            chapter_id: FK to video_chapters.id
            reap_project_id: Reap project ID returned by create_clips_job
            reap_status: Initial status (default 'processing')
            pretrim_start_secs: Start of pre-trim window in seconds (None if no trim)
            pretrim_end_secs: End of pre-trim window in seconds (None if no trim)
            pretrim_used_srt: True if an SRT file was used for AI window selection

        Returns:
            The id of the newly inserted video_shorts row
        """
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {shorts_table}
                    (chapter_id, reap_project_id, reap_status,
                     pretrim_start_secs, pretrim_end_secs, pretrim_used_srt)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    chapter_id, reap_project_id, reap_status,
                    pretrim_start_secs, pretrim_end_secs, pretrim_used_srt
                ))
                row_id = cur.fetchone()['id']
                logger.info(
                    f"Inserted video_short placeholder row {row_id} "
                    f"for chapter {chapter_id}, project {reap_project_id}"
                )
                return row_id

    def insert_video_short_clip(self, chapter_id: int, reap_project_id: str,
                                reap_clip_id: str, reap_virality_score: float,
                                reap_clip_url: str, local_file_path: str,
                                reap_status: str = "downloaded") -> int:
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
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {shorts_table}
                    (chapter_id, reap_project_id, reap_clip_id,
                     reap_virality_score, reap_clip_url, local_file_path, reap_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    chapter_id, reap_project_id, reap_clip_id,
                    reap_virality_score, reap_clip_url, local_file_path, reap_status
                ))
                row_id = cur.fetchone()['id']
                logger.info(
                    f"Inserted video_short clip row {row_id}: "
                    f"clip_id={reap_clip_id}, virality={reap_virality_score}"
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
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {shorts_table} SET
                        reap_status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE reap_project_id = %s
                """, (status, reap_project_id))
                logger.info(
                    f"Updated video_shorts status to '{status}' "
                    f"for project {reap_project_id}"
                )

    def get_pending_shorts(self, limit: int = 2, min_virality_score: float = 0.0) -> List[Dict]:
        """
        Get downloaded Shorts clips that are ready for YouTube upload.

        Returns clips ordered by virality score descending so the best clips
        are uploaded first. Only clips with a local file present are returned.

        Args:
            limit: Maximum number of rows to return (default 2)
            min_virality_score: Minimum virality score threshold (default 0.0)

        Returns:
            List of video_shorts records ordered by reap_virality_score DESC
        """
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {shorts_table}
                    WHERE is_uploaded = FALSE
                      AND local_file_path IS NOT NULL
                      AND reap_status = 'downloaded'
                      AND (reap_virality_score >= %s OR reap_virality_score IS NULL)
                    ORDER BY reap_virality_score DESC NULLS LAST
                    LIMIT %s
                """, (min_virality_score, limit))
                shorts = cur.fetchall()
                logger.info(
                    f"Found {len(shorts)} pending shorts for upload "
                    f"(min_virality={min_virality_score}, limit={limit})"
                )
                return shorts

    def mark_short_uploaded(self, reap_clip_id: str, youtube_video_id: str) -> None:
        """
        Mark a Shorts clip as successfully uploaded to YouTube.

        Args:
            reap_clip_id: Unique Reap clip identifier (used as the row lookup key)
            youtube_video_id: YouTube video ID assigned after upload
        """
        shorts_table = self.pg_conn.get_qualified_table('video_shorts')

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {shorts_table} SET
                        is_uploaded = TRUE,
                        youtube_video_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE reap_clip_id = %s
                """, (youtube_video_id, reap_clip_id))
                logger.info(
                    f"Marked short clip {reap_clip_id} as uploaded to YouTube: {youtube_video_id}"
                )

    def get_chapter_titles(self, chapter_ids: list) -> dict:
        """Returns {chapter_id: title} for the given IDs."""
        if not chapter_ids:
            return {}
        chapters_table = self.pg_conn.get_qualified_table('video_chapters')
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, title FROM {chapters_table} WHERE id = ANY(%s)",
                    (chapter_ids,),
                )
                return {row['id']: row['title'] for row in cur.fetchall()}