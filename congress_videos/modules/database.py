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