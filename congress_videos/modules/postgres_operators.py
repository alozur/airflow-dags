"""Custom PostgreSQL operators for Airflow."""

from typing import Dict, Optional

from airflow.models import BaseOperator

from .database import CongressionalVideoDB

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
        if self.operation == 'update_downloads':
            download_results_raw = ti.xcom_pull(key=self.xcom_keys.get('download_results', 'download_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            # Debug logging
            print(f"DEBUG: download_results_raw type: {type(download_results_raw)}")
            print(f"DEBUG: topic_ids type: {type(topic_ids)}")

            # Handle the download_results structure from congreso_utils
            if isinstance(download_results_raw, dict) and 'download_details' in download_results_raw:
                download_details = download_results_raw['download_details']
                print(f"DEBUG: Extracted download_details with {len(download_details)} items")
            elif isinstance(download_results_raw, list):
                download_details = download_results_raw
                print(f"DEBUG: Using download_results_raw as list with {len(download_details)} items")
            else:
                print(f"ERROR: Unexpected download_results format: {type(download_results_raw)}")
                result = {'updated_topics': 0, 'error': 'Invalid download_results format'}
                return result

            # Ensure topic_ids is a list
            if not isinstance(topic_ids, list):
                print(f"ERROR: topic_ids is not a list, got: {type(topic_ids)}")
                result = {'updated_topics': 0, 'error': 'topic_ids must be a list'}
                return result

            updated_count = 0
            for i, result_item in enumerate(download_details):
                if i < len(topic_ids):
                    if isinstance(result_item, dict):
                        success = result_item.get('success', False)
                        file_path = result_item.get('file_path')
                        file_size = result_item.get('file_size')
                        duration = result_item.get('duration')

                        print(f"DEBUG: Processing item {i}: success={success}, file_path={file_path}")

                        if success and file_path:
                            topic_entry_id = topic_ids[i]
                            try:
                                db.update_download_info(topic_entry_id, file_path, file_size, duration)
                                updated_count += 1
                                print(f"✅ Successfully updated topic {topic_entry_id}")
                            except Exception as e:
                                print(f"❌ ERROR updating topic {topic_entry_id}: {e}")
                        else:
                            print(f"⚠️ Skipping item {i}: success={success}, file_path={file_path}")
                    else:
                        print(f"WARNING: result_item {i} is not a dict: {type(result_item)}")

            result = {'updated_topics': updated_count, 'total_processed': len(download_details)}

        # REMOVED: 'save_metadata' operation - OpenAI classification fields no longer exist
        # Fields removed: openai_category, openai_summary, openai_keywords, openai_priority_score, openai_processed_at

        elif self.operation == 'save_youtube_metadata':
            metadata_results_raw = ti.xcom_pull(key=self.xcom_keys.get('metadata_results', 'youtube_metadata_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            # Debug logging
            print(f"DEBUG: metadata_results_raw type: {type(metadata_results_raw)}")
            print(f"DEBUG: topic_ids type: {type(topic_ids)}")

            # Handle the metadata_results structure from the new function
            if isinstance(metadata_results_raw, dict) and 'topic_metadata' in metadata_results_raw:
                topic_metadata = metadata_results_raw['topic_metadata']
                print(f"DEBUG: Extracted topic_metadata with {len(topic_metadata)} items")
            elif isinstance(metadata_results_raw, list):
                topic_metadata = metadata_results_raw
                print(f"DEBUG: Using metadata_results_raw as list with {len(topic_metadata)} items")
            else:
                print(f"ERROR: Unexpected metadata_results format: {type(metadata_results_raw)}")
                result = {'updated_topics': 0, 'error': 'Invalid metadata_results format'}
                return result

            # Ensure topic_ids is a list
            if not isinstance(topic_ids, list):
                print(f"ERROR: topic_ids is not a list, got: {type(topic_ids)}")
                result = {'updated_topics': 0, 'error': 'topic_ids must be a list'}
                return result

            updated_count = 0
            for i, result_item in enumerate(topic_metadata):
                if i < len(topic_ids):
                    if isinstance(result_item, dict):
                        topic_entry_id = result_item.get('topic_entry_id')
                        title_result = result_item.get('title', {})
                        description_result = result_item.get('description', {})

                        print(f"DEBUG: Processing YouTube metadata item {i}: topic_entry_id={topic_entry_id}")

                        if title_result.get('title') and description_result.get('description'):
                            topic_entry_id = topic_ids[i]

                            youtube_title = title_result.get('title')
                            youtube_description = description_result.get('description')

                            print(f"DEBUG: Saving YouTube metadata - Title: {youtube_title[:50]}...")
                            print(f"DEBUG: Description length: {len(youtube_description)} chars")

                            try:
                                db.update_youtube_metadata(topic_entry_id, youtube_title, youtube_description)
                                updated_count += 1
                                print(f"✅ Successfully updated YouTube metadata for topic {topic_entry_id}")
                            except Exception as e:
                                print(f"❌ ERROR updating YouTube metadata for topic {topic_entry_id}: {e}")
                        else:
                            print(f"⚠️ Skipping item {i}: missing title or description")
                    else:
                        print(f"WARNING: result_item {i} is not a dict: {type(result_item)}")

            result = {'updated_topics': updated_count, 'total_processed': len(topic_metadata)}

        elif self.operation == 'save_thumbnail_info':
            thumbnail_text_results = ti.xcom_pull(key=self.xcom_keys.get('thumbnail_text_results', 'thumbnail_text_results'))
            thumbnail_results = ti.xcom_pull(key=self.xcom_keys.get('thumbnail_results', 'thumbnail_results'))

            # Debug logging
            print(f"DEBUG: thumbnail_text_results type: {type(thumbnail_text_results)}")
            print(f"DEBUG: thumbnail_results type: {type(thumbnail_results)}")

            # Extract results lists
            thumbnail_texts = thumbnail_text_results.get('results', []) if isinstance(thumbnail_text_results, dict) else []
            thumbnails = thumbnail_results.get('results', []) if isinstance(thumbnail_results, dict) else []

            print(f"DEBUG: Processing {len(thumbnail_texts)} thumbnail texts and {len(thumbnails)} thumbnails")

            # Create lookups by entry_id
            text_lookup = {item.get('entry_id'): item for item in thumbnail_texts if isinstance(item, dict)}
            thumbnail_lookup = {item.get('entry_id'): item for item in thumbnails if isinstance(item, dict)}

            updated_count = 0
            for entry_id in text_lookup.keys():
                text_item = text_lookup.get(entry_id, {})
                thumbnail_item = thumbnail_lookup.get(entry_id, {})

                thumbnail_text = text_item.get('thumbnail_text', '')
                thumbnail_path = thumbnail_item.get('output_path', '')

                if thumbnail_text and thumbnail_path and thumbnail_item.get('success'):
                    try:
                        db.update_thumbnail_info(entry_id, thumbnail_text, thumbnail_path)
                        updated_count += 1
                        print(f"✅ Successfully updated thumbnail info for topic {entry_id}")
                    except Exception as e:
                        print(f"❌ ERROR updating thumbnail info for topic {entry_id}: {e}")
                else:
                    print(f"⚠️ Skipping {entry_id}: missing text or thumbnail failed")

            result = {'updated_topics': updated_count, 'total_processed': len(text_lookup)}

        elif self.operation == 'get_top_videos_for_upload':
            max_videos = context["params"].get("max_videos", 5)
            min_score = context["params"].get("min_interest_score", 6)

            print(f"DEBUG: Getting top {max_videos} videos with min_score >= {min_score}")

            result = db.get_top_videos_for_upload(max_videos, min_score)
            print(f"✅ Retrieved {len(result)} videos for upload")

        elif self.operation == 'add_to_upload_queue':
            top_videos = ti.xcom_pull(key=self.xcom_keys.get('top_videos', 'top_videos'))

            print(f"DEBUG: Adding {len(top_videos) if top_videos else 0} videos to upload queue")

            if top_videos:
                video_entry_ids = [video['entry_id'] for video in top_videos]
                db.add_videos_to_upload_queue(video_entry_ids)
                result = {'queued_videos': len(video_entry_ids), 'entry_ids': video_entry_ids}
                print(f"✅ Added {len(video_entry_ids)} videos to upload queue")
            else:
                result = {'queued_videos': 0, 'entry_ids': []}
                print("⚠️ No videos to add to queue")

        elif self.operation == 'get_from_upload_queue':
            max_videos = context["params"].get("max_videos", 5)

            print(f"DEBUG: Getting up to {max_videos} videos from upload queue")

            result = db.get_videos_from_upload_queue(max_videos)
            print(f"✅ Retrieved {len(result)} videos from upload queue")

        elif self.operation == 'update_queue_status':
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))
            download_results = ti.xcom_pull(key=self.xcom_keys.get('download_results', 'download_results'))

            # Handle both upload and download updates
            updated_count = 0

            # Update queue status based on download results
            if download_results and isinstance(download_results, dict):
                download_details = download_results.get('download_details', [])
                for detail in download_details:
                    entry_id = detail.get('entry_id')
                    if entry_id:
                        if detail.get('success'):
                            # Update queue status
                            db.update_upload_queue_status(entry_id, 'processing')

                            # Also update video_file_path in video_topics table
                            file_path = detail.get('file_path')
                            file_size = detail.get('file_size')
                            duration = detail.get('duration')
                            if file_path:
                                try:
                                    db.update_download_info(entry_id, file_path, file_size, duration)
                                    print(f"✅ Saved video path to database: {file_path}")
                                except Exception as e:
                                    print(f"⚠️ Warning: Failed to update video_file_path for {entry_id}: {e}")

                            updated_count += 1
                            print(f"✅ Updated queue status to 'processing' for {entry_id}")
                        else:
                            error_msg = detail.get('error', 'Download failed')
                            db.update_upload_queue_status(entry_id, 'failed', error_msg)
                            updated_count += 1
                            print(f"❌ Marked {entry_id} as failed: {error_msg}")

            # Update queue status based on upload results
            if upload_results and isinstance(upload_results, dict):
                upload_details = upload_results.get('upload_details', [])
                for detail in upload_details:
                    entry_id = detail.get('entry_id')
                    video_id = detail.get('video_id')  # YouTube API returns 'video_id', not 'youtube_video_id'

                    if entry_id:
                        if detail.get('success') and video_id:
                            # Mark as completed (keep in queue for history)
                            db.update_upload_queue_status(entry_id, 'completed')
                            updated_count += 1
                            print(f"✅ Marked {entry_id} as completed in queue (YouTube ID: {video_id})")
                        else:
                            error_msg = detail.get('error', 'Upload failed')
                            db.update_upload_queue_status(entry_id, 'failed', error_msg)
                            updated_count += 1
                            print(f"❌ Marked {entry_id} as failed: {error_msg}")

            result = {'updated_items': updated_count}

        elif self.operation == 'update_youtube_status':
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))

            # Debug logging
            print(f"DEBUG: upload_results type: {type(upload_results)}")

            # Extract upload details
            if isinstance(upload_results, dict) and 'upload_details' in upload_results:
                upload_details = upload_results['upload_details']
                print(f"DEBUG: Extracted upload_details with {len(upload_details)} items")
            elif isinstance(upload_results, list):
                upload_details = upload_results
                print(f"DEBUG: Using upload_results as list with {len(upload_details)} items")
            else:
                print(f"ERROR: Unexpected upload_results format: {type(upload_results)}")
                result = {'updated_videos': 0, 'error': 'Invalid upload_results format'}
                return result

            updated_count = 0
            for upload_detail in upload_details:
                if isinstance(upload_detail, dict) and upload_detail.get('success'):
                    entry_id = upload_detail.get('entry_id')
                    video_id = upload_detail.get('video_id')  # YouTube API returns 'video_id'

                    if entry_id and video_id:
                        try:
                            db.update_youtube_upload_status(entry_id, video_id)
                            updated_count += 1
                            print(f"✅ Successfully updated YouTube status for {entry_id} (video_id: {video_id})")
                        except Exception as e:
                            print(f"❌ ERROR updating YouTube status for {entry_id}: {e}")
                    else:
                        print("⚠️ Skipping: missing entry_id or video_id")
                else:
                    print("⚠️ Skipping failed upload")

            result = {'updated_videos': updated_count, 'total_uploads': len(upload_details)}

        elif self.operation == 'save_youtube_chapters':
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

            uploads_today = db.count_chapters_uploaded_today()
            queue_size = db.count_pending_uploadable_chapters(min_relevance_score)
            max_uploads = 2 if queue_size > 15 else 1
            remaining_quota = max(0, max_uploads - uploads_today)

            result = {
                "uploads_today": uploads_today,
                "queue_size": queue_size,
                "max_uploads": max_uploads,
                "remaining_quota": remaining_quota,
            }
            print(f"✅ Upload quota: {uploads_today} today, {queue_size} in queue, {remaining_quota} remaining (max={max_uploads})")

        elif self.operation == 'get_uploadable_chapters':
            upload_quota = ti.xcom_pull(key=self.xcom_keys.get('upload_quota', 'upload_quota'))
            if upload_quota and 'remaining_quota' in upload_quota:
                max_chapters = upload_quota['remaining_quota']
            else:
                max_chapters = context["params"].get("max_chapters", 1)
            min_relevance_score = context["params"].get("min_relevance_score", 2)

            print(f"DEBUG: Getting top {max_chapters} uploadable chapters with min_relevance_score >= {min_relevance_score}")

            result = db.get_uploadable_chapters(limit=max_chapters, min_relevance_score=min_relevance_score)
            print(f"✅ Retrieved {len(result)} uploadable chapters")

        elif self.operation == 'mark_chapters_uploaded':
            """Mark chapters as uploaded to YouTube after successful upload"""
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))

            if not upload_results or not upload_results.get('upload_details'):
                print("No upload results to process")
                result = {'updated_chapters': 0, 'failed_updates': 0, 'details': []}
            else:
                updated_count = 0
                failed_count = 0
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
                        print(f"⏭️ Skipping chapter {chapter_id}: upload was not successful")
                        details.append({
                            'chapter_id': chapter_id,
                            'status': 'skipped',
                            'reason': 'upload_failed'
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
                    'details': details
                }
                print(f"✅ Updated {updated_count} chapters, {failed_count} failed")

        elif self.operation == 'get_chapters_for_shorts':
            """Get video chapters eligible for Reap Shorts processing"""
            max_chapters = context["params"].get("max_chapters", 3)
            min_relevance_score = context["params"].get("min_relevance_score", 3)

            print(f"DEBUG: Getting up to {max_chapters} chapters for Shorts (min_score={min_relevance_score})")

            result = db.get_chapters_for_shorts(limit=max_chapters, min_relevance_score=min_relevance_score)
            print(f"✅ Retrieved {len(result)} chapters eligible for Shorts")

        elif self.operation == 'mark_shorts_uploaded':
            """Mark Shorts clips as uploaded to YouTube after successful upload"""
            upload_results = ti.xcom_pull(key=self.xcom_keys.get('upload_results', 'upload_results'))

            if not upload_results:
                print("No upload results to process")
                result = {'updated_shorts': 0, 'failed_updates': 0, 'details': []}
            else:
                updated_count = 0
                failed_count = 0
                details = []

                upload_details = upload_results if isinstance(upload_results, list) else upload_results.get('upload_details', [])

                for upload_detail in upload_details:
                    reap_clip_id = upload_detail.get('reap_clip_id')
                    youtube_video_id = upload_detail.get('youtube_video_id')
                    success = upload_detail.get('success', False)

                    if success and reap_clip_id and youtube_video_id:
                        try:
                            db.mark_short_uploaded(reap_clip_id, youtube_video_id)
                            updated_count += 1
                            details.append({
                                'reap_clip_id': reap_clip_id,
                                'youtube_video_id': youtube_video_id,
                                'status': 'updated'
                            })
                            print(f"✅ Marked short {reap_clip_id} as uploaded: {youtube_video_id}")
                        except Exception as e:
                            failed_count += 1
                            details.append({
                                'reap_clip_id': reap_clip_id,
                                'status': 'failed',
                                'error': str(e)
                            })
                            print(f"❌ Failed to mark short {reap_clip_id}: {e}")
                    elif not success:
                        print(f"⏭️ Skipping short {reap_clip_id}: upload was not successful")
                        details.append({'reap_clip_id': reap_clip_id, 'status': 'skipped', 'reason': 'upload_failed'})
                    else:
                        print(f"⚠️ Upload succeeded but missing fields: reap_clip_id={reap_clip_id}, youtube_video_id={youtube_video_id}")
                        details.append({
                            'reap_clip_id': reap_clip_id,
                            'status': 'skipped',
                            'reason': f'missing_fields: reap_clip_id={reap_clip_id}, youtube_video_id={youtube_video_id}'
                        })

                result = {
                    'updated_shorts': updated_count,
                    'failed_updates': failed_count,
                    'details': details
                }
                print(f"✅ Updated {updated_count} shorts, {failed_count} failed")

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result