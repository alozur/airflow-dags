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

                    topic_entry_id = db.upsert_video_topic(session_id, topic_data.get('entry_id'), mapped_topic_data)
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

                        intervention_entry_id = db.upsert_video_topic(session_id, intervention.get('entry_id'), mapped_intervention_data)
                        topic_ids.append(intervention_entry_id)
                        print(f"✅ Successfully saved intervention {intervention_entry_id}")

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
                            topic_entry_id = topic_ids[i]

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
                                db.update_openai_classification(topic_entry_id, mapped_openai_data)
                                updated_count += 1
                                print(f"✅ Successfully updated OpenAI data for topic {topic_entry_id}")
                            except Exception as e:
                                print(f"❌ ERROR updating OpenAI data for topic {topic_entry_id}: {e}")
                        else:
                            print(f"⚠️ Skipping item {i}: no OpenAI data found")
                    else:
                        print(f"WARNING: result_item {i} is not a dict: {type(result_item)}")

            result = {'updated_topics': updated_count, 'total_processed': len(topic_metadata)}

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

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result