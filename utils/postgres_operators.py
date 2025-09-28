# utils/postgres_operators.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, List, Optional
from congreso_youtube.congress_database import CongressionalVideoDB
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

            topic_ids = []
            for group in video_groups:
                if 'main_topic' in group:
                    topic_data = group['main_topic']
                    topic_id = db.upsert_video_topic(session_id, topic_data.get('entry_id'), topic_data)
                    topic_ids.append(topic_id)

            db.update_session_total_topics(session_id)
            result = topic_ids

        elif self.operation == 'update_downloads':
            download_results = ti.xcom_pull(key=self.xcom_keys.get('download_results', 'download_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            updated_count = 0
            for i, result_item in enumerate(download_results):
                if i < len(topic_ids) and result_item.get('success'):
                    topic_id = topic_ids[i]
                    db.update_download_info(
                        topic_id,
                        result_item.get('file_path'),
                        result_item.get('file_size'),
                        result_item.get('duration')
                    )
                    updated_count += 1

            result = {'updated_topics': updated_count}

        elif self.operation == 'save_metadata':
            metadata_results = ti.xcom_pull(key=self.xcom_keys.get('metadata_results', 'youtube_metadata_results'))
            topic_ids = ti.xcom_pull(key=self.xcom_keys.get('topic_ids', 'db_topic_ids'))

            updated_count = 0
            for i, result_item in enumerate(metadata_results):
                if i < len(topic_ids) and result_item.get('openai_data'):
                    topic_id = topic_ids[i]
                    openai_data = result_item['openai_data']
                    db.update_openai_classification(topic_id, openai_data)
                    updated_count += 1

            result = {'updated_topics': updated_count}

        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Push result to XCom if specified
        if self.output_xcom_key:
            ti.xcom_push(key=self.output_xcom_key, value=result)

        return result