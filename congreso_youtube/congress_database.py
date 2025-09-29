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

    def create_or_update_session(self, session_number: int, session_date: date,
                               target_date: date, session_url: str = None) -> int:
        """
        Create or update a congressional session record
        Returns the session ID
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # Try to update existing session first
                cur.execute("""
                    UPDATE congressional_sessions
                    SET session_url = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE session_number = %s AND session_date = %s
                    RETURNING id
                """, (session_url, session_number, session_date))

                result = cur.fetchone()
                if result:
                    logger.info(f"Updated existing session {session_number} with ID {result['id']}")
                    return result['id']

                # Insert new session if not found
                cur.execute("""
                    INSERT INTO congressional_sessions
                    (session_number, session_date, target_date, session_url)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (session_number, session_date, target_date, session_url))

                result = cur.fetchone()
                session_id = result['id']
                logger.info(f"Created new session {session_number} with ID {session_id}")
                return session_id

    def upsert_video_topic(self, session_id: int, entry_id: str, topic_data: Dict[str, Any]) -> int:
        """
        Insert or update a video topic record
        Returns the video topic ID
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                # Check if topic already exists
                cur.execute("""
                    SELECT id FROM video_topics
                    WHERE session_id = %s AND entry_id = %s
                """, (session_id, entry_id))

                existing = cur.fetchone()

                if existing:
                    # Update existing topic
                    cur.execute("""
                        UPDATE video_topics SET
                            topic_title = %s,
                            video_url = %s,
                            video_file_path = %s,
                            speaker_name = %s,
                            role = %s,
                            profile_link = %s,
                            file_size_bytes = %s,
                            duration_seconds = %s,
                            is_main_topic = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id
                    """, (
                        topic_data.get('topic_title'),
                        topic_data.get('video_url'),
                        topic_data.get('video_file_path'),
                        topic_data.get('speaker_name'),
                        topic_data.get('role'),
                        topic_data.get('profile_link'),
                        topic_data.get('file_size_bytes'),
                        topic_data.get('duration_seconds'),
                        topic_data.get('is_main_topic', False),
                        existing['id']
                    ))
                    topic_id = existing['id']
                    logger.info(f"Updated video topic {entry_id} with ID {topic_id}")
                else:
                    # Insert new topic
                    cur.execute("""
                        INSERT INTO video_topics
                        (session_id, entry_id, topic_title, video_url, video_file_path,
                         speaker_name, role, profile_link, file_size_bytes, duration_seconds, is_main_topic)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        session_id,
                        entry_id,
                        topic_data.get('topic_title'),
                        topic_data.get('video_url'),
                        topic_data.get('video_file_path'),
                        topic_data.get('speaker_name'),
                        topic_data.get('role'),
                        topic_data.get('profile_link'),
                        topic_data.get('file_size_bytes'),
                        topic_data.get('duration_seconds'),
                        topic_data.get('is_main_topic', False)
                    ))
                    result = cur.fetchone()
                    topic_id = result['id']
                    logger.info(f"Created new video topic {entry_id} with ID {topic_id}")

                return topic_id

    def update_openai_classification(self, video_topic_id: int, openai_data: Dict[str, Any]):
        """Update OpenAI classification data for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_topics SET
                        openai_category = %s,
                        openai_summary = %s,
                        openai_keywords = %s,
                        openai_priority_score = %s,
                        openai_processed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    openai_data.get('category'),
                    openai_data.get('summary'),
                    openai_data.get('keywords', []),
                    openai_data.get('priority_score'),
                    video_topic_id
                ))
                logger.info(f"Updated OpenAI classification for video topic ID {video_topic_id}")

    def mark_video_uploaded(self, video_topic_id: int, youtube_video_id: str):
        """Mark a video as uploaded to YouTube"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_topics SET
                        is_uploaded_to_youtube = TRUE,
                        youtube_video_id = %s,
                        youtube_upload_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (youtube_video_id, video_topic_id))
                logger.info(f"Marked video topic ID {video_topic_id} as uploaded: {youtube_video_id}")

    def get_uploadable_videos(self, limit: int = None) -> List[Dict]:
        """Get videos eligible for YouTube upload"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                query = "SELECT * FROM uploadable_videos"
                if limit:
                    query += f" LIMIT {limit}"

                cur.execute(query)
                return cur.fetchall()

    def add_to_upload_queue(self, video_topic_id: int, priority: int = 5):
        """Add a video to the upload queue"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO upload_queue (video_topic_id, queue_priority)
                    VALUES (%s, %s)
                    ON CONFLICT (video_topic_id) DO UPDATE SET
                        queue_priority = EXCLUDED.queue_priority,
                        queued_at = CURRENT_TIMESTAMP
                """, (video_topic_id, priority))
                logger.info(f"Added video topic ID {video_topic_id} to upload queue with priority {priority}")

    def get_session_by_number_and_date(self, session_number: int, session_date: date) -> Optional[Dict]:
        """Get session by session number and date"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM congressional_sessions
                    WHERE session_number = %s AND session_date = %s
                """, (session_number, session_date))
                return cur.fetchone()

    def get_video_topics_by_session(self, session_id: int) -> List[Dict]:
        """Get all video topics for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM video_topics
                    WHERE session_id = %s
                    ORDER BY created_at
                """, (session_id,))
                return cur.fetchall()

    def update_session_total_topics(self, session_id: int):
        """Update the total_topics count for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE congressional_sessions SET
                        total_topics = (
                            SELECT COUNT(*) FROM video_topics
                            WHERE session_id = %s
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (session_id, session_id))
                logger.info(f"Updated total_topics count for session ID {session_id}")

    def update_download_info(self, video_topic_id: int, file_path: str, file_size: int = None, duration: int = None):
        """Update download information for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_topics SET
                        video_file_path = %s,
                        file_size_bytes = %s,
                        duration_seconds = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (file_path, file_size, duration, video_topic_id))
                logger.info(f"Updated download info for video topic ID {video_topic_id}")

    def update_main_topic_status(self, video_topic_id: int, is_main_topic: bool):
        """Update the is_main_topic status for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_topics SET
                        is_main_topic = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (is_main_topic, video_topic_id))
                logger.info(f"Updated main topic status for video topic ID {video_topic_id} to {is_main_topic}")

    def get_main_topics_by_session(self, session_id: int) -> List[Dict]:
        """Get all main topics for a session"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM video_topics
                    WHERE session_id = %s AND is_main_topic = TRUE
                    ORDER BY created_at
                """, (session_id,))
                return cur.fetchall()

    def update_youtube_metadata(self, video_topic_id: int, youtube_title: str, youtube_description: str):
        """Update YouTube metadata for a video topic"""
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_topics SET
                        youtube_title = %s,
                        youtube_description = %s,
                        youtube_metadata_generated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (youtube_title, youtube_description, video_topic_id))
                logger.info(f"Updated YouTube metadata for video topic ID {video_topic_id}")