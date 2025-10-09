"""Custom PostgreSQL operators for Airflow."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from .database import CongressionalVideoDB

class PostgreSQLOperator(BaseOperator):
    """Custom operator for PostgreSQL operations with XCom integration"""

    @apply_defaults
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
        if self.operation == 'create_session':
            session_number = ti.xcom_pull(key=self.xcom_keys.get('session_number', 'session_number'))
            target_date = context["params"].get("target_date")
            session_link = ti.xcom_pull(key=self.xcom_keys.get('session_link', 'session_link'))

            session_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
            target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()

            result = db.create_or_update_session(session_number, session_date_obj, target_date_obj, session_link)

        elif self.operation == 'save_topics':
            session_number = ti.xcom_pull(key=self.xcom_keys.get('session_number', 'db_session_id'))
            video_groups = ti.xcom_pull(key=self.xcom_keys.get('video_groups', 'enriched_video_groups'))

            print(f"DEBUG: session_number = {session_number}")
            print(f"DEBUG: video_groups type = {type(video_groups)}")
            print(f"DEBUG: video_groups length = {len(video_groups) if video_groups else 0}")

            topic_ids = []
            for i, group in enumerate(video_groups):
                print(f"DEBUG: Group {i} keys: {list(group.keys()) if isinstance(group, dict) else 'Not a dict'}")

                if 'main_topic' in group:
                    # Save main topic
                    topic_data = group['main_topic']
                    print(f"DEBUG: main_topic keys: {list(topic_data.keys()) if isinstance(topic_data, dict) else 'Not a dict'}")
                    print(f"DEBUG: main_topic data: {topic_data}")

                    # Map the fields correctly from the actual data structure
                    mapped_topic_data = {
                        'topic_title': topic_data.get('content'),  # 'content' -> 'topic_title'
                        'video_url': topic_data.get('video_url'),
                        'video_file_path': None,  # Will be updated later by download task
                        'speaker_name': topic_data.get('speaker_name'),
                        'role': topic_data.get('role'),
                        'profile_link': topic_data.get('profile_link'),
                        'main_topic_entry_id': None,  # Main topics don't have a parent
                        'file_size_bytes': None,  # Will be updated later
                        'duration_seconds': topic_data.get('metadata_url', {}).get('duration_seconds'),
                        'is_main_topic': topic_data.get('is_bold', False)  # Map is_bold to is_main_topic
                    }

                    print(f"DEBUG: mapped_topic_data: {mapped_topic_data}")

                    topic_entry_id = db.upsert_video_topic(session_number, topic_data.get('entry_id'), mapped_topic_data)
                    topic_ids.append(topic_entry_id)
                    print(f"✅ Successfully saved main topic {topic_entry_id}")

                    # Save interventions (individual speaker videos)
                    interventions = group.get('interventions', [])
                    print(f"DEBUG: Found {len(interventions)} interventions for this topic")

                    for j, intervention in enumerate(interventions):
                        print(f"DEBUG: Intervention {j} keys: {list(intervention.keys()) if isinstance(intervention, dict) else 'Not a dict'}")

                        # Map intervention data
                        mapped_intervention_data = {
                            'topic_title': intervention.get('content', f"Intervención de {intervention.get('speaker_name', 'Unknown')}"),
                            'video_url': intervention.get('video_url'),
                            'video_file_path': None,  # Will be updated later by download task
                            'speaker_name': intervention.get('speaker_name'),
                            'role': intervention.get('role'),
                            'profile_link': intervention.get('profile_link'),
                            'main_topic_entry_id': topic_entry_id,  # Link intervention to its main topic
                            'file_size_bytes': None,  # Will be updated later
                            'duration_seconds': intervention.get('metadata_url', {}).get('duration_seconds'),
                            'is_main_topic': False  # Interventions are never main topics
                        }

                        intervention_entry_id = db.upsert_video_topic(session_number, intervention.get('entry_id'), mapped_intervention_data)
                        topic_ids.append(intervention_entry_id)
                        print(f"✅ Successfully saved intervention {intervention_entry_id}")

            db.update_session_total_topics(session_number)
            result = topic_ids

        elif self.operation == 'update_downloads':
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

        elif self.operation == 'save_ai_evaluations':
            ai_evaluation_results = ti.xcom_pull(key=self.xcom_keys.get('evaluation_results', 'ai_evaluation_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            # Debug logging
            print(f"DEBUG: ai_evaluation_results type: {type(ai_evaluation_results)}")
            print(f"DEBUG: topic_ids type: {type(topic_ids)}")

            # Extract evaluations list
            if isinstance(ai_evaluation_results, dict) and 'evaluations' in ai_evaluation_results:
                evaluations = ai_evaluation_results['evaluations']
                print(f"DEBUG: Extracted evaluations with {len(evaluations)} items")
            elif isinstance(ai_evaluation_results, list):
                evaluations = ai_evaluation_results
                print(f"DEBUG: Using ai_evaluation_results as list with {len(evaluations)} items")
            else:
                print(f"ERROR: Unexpected ai_evaluation_results format: {type(ai_evaluation_results)}")
                result = {'updated_topics': 0, 'error': 'Invalid ai_evaluation_results format'}
                return result

            # Ensure topic_ids is a list
            if not isinstance(topic_ids, list):
                print(f"ERROR: topic_ids is not a list, got: {type(topic_ids)}")
                result = {'updated_topics': 0, 'error': 'topic_ids must be a list'}
                return result

            # Create a mapping from entry_id to evaluation for faster lookup
            evaluation_map = {eval_item['entry_id']: eval_item for eval_item in evaluations if isinstance(eval_item, dict)}

            updated_count = 0
            for entry_id in topic_ids:
                if entry_id in evaluation_map:
                    evaluation = evaluation_map[entry_id]

                    if evaluation.get('evaluation_success'):
                        interest_score = evaluation.get('interest_score', 5)
                        reasoning = evaluation.get('reasoning', '')

                        print(f"DEBUG: Saving AI evaluation for {entry_id}: score={interest_score}")

                        try:
                            db.update_ai_interest_evaluation(entry_id, interest_score, reasoning)
                            updated_count += 1
                            print(f"✅ Successfully updated AI evaluation for topic {entry_id}")
                        except Exception as e:
                            print(f"❌ ERROR updating AI evaluation for topic {entry_id}: {e}")
                    else:
                        print(f"⚠️ Skipping {entry_id}: evaluation failed with error: {evaluation.get('error')}")
                else:
                    print(f"⚠️ No evaluation found for entry_id: {entry_id}")

            result = {'updated_topics': updated_count, 'total_evaluations': len(evaluations)}

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
                            db.update_upload_queue_status(entry_id, 'processing')
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
                        print(f"⚠️ Skipping: missing entry_id or video_id")
                else:
                    print(f"⚠️ Skipping failed upload")

            result = {'updated_videos': updated_count, 'total_uploads': len(upload_details)}

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result