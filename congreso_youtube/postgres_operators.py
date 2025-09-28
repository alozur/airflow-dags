# congreso_youtube/postgres_operators.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, List, Optional
from .congress_database import CongressionalVideoDB
from datetime import datetime

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
            session_id = ti.xcom_pull(key=self.xcom_keys.get('session_id', 'db_session_id'))
            video_groups = ti.xcom_pull(key=self.xcom_keys.get('video_groups', 'enriched_video_groups'))

            print(f"DEBUG: session_id = {session_id}")
            print(f"DEBUG: video_groups type = {type(video_groups)}")
            print(f"DEBUG: video_groups length = {len(video_groups) if video_groups else 0}")

            topic_ids = []
            for i, group in enumerate(video_groups):
                print(f"DEBUG: Group {i} keys: {list(group.keys()) if isinstance(group, dict) else 'Not a dict'}")

                if 'main_topic' in group:
                    topic_data = group['main_topic']
                    print(f"DEBUG: main_topic keys: {list(topic_data.keys()) if isinstance(topic_data, dict) else 'Not a dict'}")
                    print(f"DEBUG: main_topic data: {topic_data}")

                    # Map the fields correctly from the actual data structure
                    mapped_topic_data = {
                        'topic_title': topic_data.get('content'),  # 'content' -> 'topic_title'
                        'topic_content': topic_data.get('content'),  # Use content for both title and content
                        'video_url': topic_data.get('video_url'),
                        'video_file_path': None,  # Will be updated later by download task
                        'file_size_bytes': None,  # Will be updated later
                        'duration_seconds': None  # Will be updated later
                    }

                    print(f"DEBUG: mapped_topic_data: {mapped_topic_data}")

                    topic_id = db.upsert_video_topic(session_id, topic_data.get('entry_id'), mapped_topic_data)
                    topic_ids.append(topic_id)
                    print(f"✅ Successfully saved topic {topic_data.get('entry_id')} with ID {topic_id}")

            db.update_session_total_topics(session_id)
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
                            topic_id = topic_ids[i]
                            try:
                                db.update_download_info(topic_id, file_path, file_size, duration)
                                updated_count += 1
                                print(f"✅ Successfully updated topic ID {topic_id}")
                            except Exception as e:
                                print(f"❌ ERROR updating topic ID {topic_id}: {e}")
                        else:
                            print(f"⚠️ Skipping item {i}: success={success}, file_path={file_path}")
                    else:
                        print(f"WARNING: result_item {i} is not a dict: {type(result_item)}")

            result = {'updated_topics': updated_count, 'total_processed': len(download_details)}

        elif self.operation == 'save_metadata':
            metadata_results_raw = ti.xcom_pull(key=self.xcom_keys.get('metadata_results', 'youtube_metadata_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            # Debug logging
            print(f"DEBUG: metadata_results_raw type: {type(metadata_results_raw)}")
            print(f"DEBUG: topic_ids type: {type(topic_ids)}")

            # Handle the metadata_results structure from congreso_utils
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
                        openai_data = result_item.get('openai_data')
                        topic_entry_id = result_item.get('topic_entry_id')

                        print(f"DEBUG: Processing metadata item {i}: topic_entry_id={topic_entry_id}, has_openai_data={bool(openai_data)}")

                        if openai_data:
                            topic_id = topic_ids[i]

                            # Map OpenAI data fields correctly
                            mapped_openai_data = {
                                'category': openai_data.get('category'),
                                'summary': openai_data.get('summary'),
                                'keywords': openai_data.get('keywords', []),
                                'priority_score': openai_data.get('priority_score')
                            }

                            print(f"DEBUG: openai_data received: {openai_data}")
                            print(f"DEBUG: mapped_openai_data: {mapped_openai_data}")

                            try:
                                db.update_openai_classification(topic_id, mapped_openai_data)
                                updated_count += 1
                                print(f"✅ Successfully updated OpenAI data for topic ID {topic_id}")
                            except Exception as e:
                                print(f"❌ ERROR updating OpenAI data for topic ID {topic_id}: {e}")
                        else:
                            print(f"⚠️ Skipping item {i}: no OpenAI data found")
                    else:
                        print(f"WARNING: result_item {i} is not a dict: {type(result_item)}")

            result = {'updated_topics': updated_count, 'total_processed': len(topic_metadata)}

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result