"""Custom PostgreSQL operators for Airflow."""

import logging
from typing import Dict, Optional

from airflow.models import BaseOperator

from .database import CongressionalVideoDB

logger = logging.getLogger(__name__)

class PostgreSQLOperator(BaseOperator):
    """Custom operator for PostgreSQL operations with XCom integration"""

    def __init__(
        self,
        operation: str,
        xcom_keys: Optional[Dict[str, str]] = None,
        output_xcom_key: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.operation = operation
        self.xcom_keys = xcom_keys or {}
        self.output_xcom_key = output_xcom_key

    def execute(self, context):
        ti = context['ti']
        db = CongressionalVideoDB()

        # Pull data from XCom based on operation
        if self.operation == 'save_youtube_chapters':
            """Save scored YouTube chapter data to database"""
            scored_chapters = ti.xcom_pull(key=self.xcom_keys.get('scored_chapters', 'scored_chapters'))
            session_date_data = ti.xcom_pull(key=self.xcom_keys.get('session_date', 'session_date'))
            target_date = context["params"].get("target_date")

            # Debug logging
            print(f"DEBUG: scored_chapters type: {type(scored_chapters)}")
            print(f"DEBUG: session_date_data type: {type(session_date_data)}")
            print(f"DEBUG: target_date: {target_date}")

            if not scored_chapters:
                print("WARNING: No scored chapters data to save")
                result = {'total_videos_saved': 0, 'total_chapters_saved': 0, 'videos': []}
                return result

            # Extract session_number and session_date from session_date_data
            # Structure: {'total_processed': int, 'videos': [{'video_id': str, 'session_number': int, 'target_date': str, ...}]}
            session_number = None
            session_date_str = None

            if session_date_data and isinstance(session_date_data, dict):
                videos_list = session_date_data.get('videos', [])
                if videos_list and len(videos_list) > 0:
                    first_video = videos_list[0]
                    session_number = first_video.get('session_number')
                    session_date_str = first_video.get('target_date', target_date)
                    print(f"DEBUG: Extracted session_number={session_number}, session_date={session_date_str}")
                else:
                    print("WARNING: No videos in session_date_data")
            else:
                print(f"WARNING: Unexpected session_date_data format: {type(session_date_data)}")

            # Fallback to target_date if no session_date found
            if not session_date_str:
                session_date_str = target_date
                print(f"DEBUG: Using target_date as fallback: {session_date_str}")

            # Parse session_date
            from datetime import datetime
            if isinstance(session_date_str, str):
                session_date_obj = datetime.strptime(session_date_str, "%Y-%m-%d").date()
            else:
                session_date_obj = session_date_str

            print(f"DEBUG: Final values - session_number={session_number}, session_date={session_date_obj}")

            # Call the database function to save chapters
            result = db.save_youtube_chapters_to_db(
                scored_chapters_data=scored_chapters,
                session_number=session_number,
                session_date=session_date_obj
            )

            print(f"✅ Saved {result['total_chapters_saved']} chapters across {result['total_videos_saved']} videos")

        elif self.operation == 'check_upload_quota':
            min_relevance_score = context["params"].get("min_relevance_score", 2)

            chapters_uploaded_today = db.count_chapters_uploaded_today()
            turns_uploaded_today = db.count_turns_uploaded_today()
            uploads_today = chapters_uploaded_today + turns_uploaded_today

            chapters_pending = db.count_pending_uploadable_chapters(min_relevance_score)
            turns_pending = db.count_pending_uploadable_turns()
            queue_size = chapters_pending + turns_pending

            result = {
                "uploads_today": uploads_today,
                "queue_size": queue_size,
                "turns_pending": turns_pending,
            }
            print(f"✅ Upload quota: {uploads_today} today ({chapters_uploaded_today} chapters + {turns_uploaded_today} turns), {chapters_pending} chapters + {turns_pending} turns in queue")

        elif self.operation == 'mark_chapters_uploaded':
            """Mark chapters as uploaded to YouTube after successful upload"""
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))

            if not upload_results or not upload_results.get('upload_details'):
                print("No upload results to process")
                result = {'updated_chapters': 0, 'failed_updates': 0, 'details': []}
            else:
                updated_count = 0
                failed_count = 0
                recorded_failures = 0
                details = []

                for upload_detail in upload_results['upload_details']:
                    chapter_id = upload_detail.get('chapter_id')
                    youtube_video_id = upload_detail.get('youtube_video_id')
                    success = upload_detail.get('success', False)

                    if success and chapter_id and youtube_video_id:
                        try:
                            db.mark_chapter_uploaded(chapter_id, youtube_video_id)
                            updated_count += 1
                            details.append({
                                'chapter_id': chapter_id,
                                'youtube_video_id': youtube_video_id,
                                'status': 'updated'
                            })
                            print(f"✅ Marked chapter {chapter_id} as uploaded: {youtube_video_id}")
                        except Exception as e:
                            failed_count += 1
                            details.append({
                                'chapter_id': chapter_id,
                                'status': 'failed',
                                'error': str(e)
                            })
                            print(f"❌ Failed to mark chapter {chapter_id}: {e}")
                    elif not success:
                        if chapter_id:
                            try:
                                db.record_chapter_upload_failure(chapter_id, upload_detail.get('error'))
                                recorded_failures += 1
                                details.append({
                                    'chapter_id': chapter_id,
                                    'status': 'failure_recorded',
                                })
                                print(f"📉 Recorded upload failure for chapter {chapter_id}")
                            except Exception as e:
                                failed_count += 1
                                details.append({
                                    'chapter_id': chapter_id,
                                    'status': 'failed',
                                    'error': str(e),
                                })
                                print(f"❌ Failed to record upload failure for chapter {chapter_id}: {e}")
                                logger.error(
                                    f"Failed to RECORD upload failure for chapter {chapter_id} in the "
                                    f"database (this attempt's failure count is now uncounted): {e}"
                                )
                        else:
                            # Defensive: no resolvable chapter_id — cannot target a row, log-and-skip (no DB write)
                            print("⏭️ Skipping failed upload with no chapter_id")
                            details.append({
                                'chapter_id': chapter_id,
                                'status': 'skipped',
                                'reason': 'upload_failed_no_chapter_id',
                            })
                    elif success and (not chapter_id or not youtube_video_id):
                        # Debug: upload succeeded but missing tracking fields
                        print(f"⚠️ Upload succeeded but missing fields: chapter_id={chapter_id}, youtube_video_id={youtube_video_id}")
                        print(f"   Full upload_detail: {upload_detail}")
                        details.append({
                            'chapter_id': chapter_id,
                            'status': 'skipped',
                            'reason': f'missing_fields: chapter_id={chapter_id}, youtube_video_id={youtube_video_id}'
                        })

                result = {
                    'updated_chapters': updated_count,
                    'failed_updates': failed_count,
                    'recorded_failures': recorded_failures,
                    'details': details
                }
                print(f"✅ Updated {updated_count} chapters, {failed_count} failed, {recorded_failures} failures recorded")

        elif self.operation == 'mark_turns_uploaded':
            """Mark speaker turn videos as uploaded to YouTube after successful upload."""
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))

            if not upload_results or not upload_results.get('upload_details'):
                print("No turn upload results to process")
                result = {'updated_turns': 0, 'failed_updates': 0, 'details': []}
            else:
                updated_count = 0
                failed_count = 0
                details = []

                for upload_detail in upload_results['upload_details']:
                    turn_id = upload_detail.get('turn_id')
                    youtube_video_id = upload_detail.get('youtube_video_id')
                    success = upload_detail.get('success', False)

                    if success and turn_id and youtube_video_id:
                        try:
                            db.mark_turns_uploaded(turn_id=turn_id, youtube_video_id=youtube_video_id)
                            updated_count += 1
                            details.append({
                                'turn_id': turn_id,
                                'youtube_video_id': youtube_video_id,
                                'status': 'updated',
                            })
                            print(f"✅ Marked turn {turn_id} as uploaded: {youtube_video_id}")
                        except Exception as e:
                            failed_count += 1
                            details.append({
                                'turn_id': turn_id,
                                'status': 'failed',
                                'error': str(e),
                            })
                            print(f"❌ Failed to mark turn {turn_id}: {e}")
                    else:
                        details.append({
                            'turn_id': turn_id,
                            'status': 'skipped',
                            'reason': 'upload_failed_or_missing_fields',
                        })

                result = {
                    'updated_turns': updated_count,
                    'failed_updates': failed_count,
                    'details': details,
                }
                print(f"✅ Marked {updated_count} turns uploaded, {failed_count} failed")

        elif self.operation == 'get_pending_analytics_checkpoints':
            """Return candidate video chapters for analytics collection."""
            result = db.get_pending_analytics_checkpoints()
            print(f"✅ Retrieved {len(result)} candidate chapters for analytics")

        elif self.operation == 'record_analytics_snapshots':
            """Persist each collected (chapter, checkpoint, metrics) snapshot."""
            collected = ti.xcom_pull(
                key=self.xcom_keys.get('collected', 'collected')
            ) or []

            print(f"DEBUG: recording {len(collected)} analytics snapshots")

            for item in collected:
                db.record_analytics_snapshot(
                    chapter_id=item['chapter_id'],
                    youtube_video_id=item['youtube_video_id'],
                    checkpoint=item['checkpoint'],
                    metrics=item['metrics'],
                )
                print(
                    f"✅ Recorded snapshot: chapter_id={item['chapter_id']} "
                    f"yt_id={item['youtube_video_id']} checkpoint={item['checkpoint']}"
                )

            result = {'recorded_snapshots': len(collected)}

        elif self.operation == 'select_unverified_uploads':
            """Return uploaded rows whose verification is pending (1h–48h window)."""
            from congress_videos.modules.post_upload_verification import (
                VERIFY_WINDOW_MIN_HOURS,
                VERIFY_WINDOW_MAX_HOURS,
            )

            candidates = db.select_unverified_uploads(
                min_h=VERIFY_WINDOW_MIN_HOURS,
                max_h=VERIFY_WINDOW_MAX_HOURS,
            )
            logger.info(
                "select_unverified_uploads: %d candidates in [%dh, %dh] window",
                len(candidates),
                VERIFY_WINDOW_MIN_HOURS,
                VERIFY_WINDOW_MAX_HOURS,
            )
            result = candidates

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result