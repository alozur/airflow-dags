"""
Congress YouTube Uploader DAG

This DAG runs daily to upload congressional videos to YouTube using a queue-based workflow:

1. Query top videos by AI interest score from video_topics table
2. Add selected videos to upload_queue table with priority based on AI score
3. Retrieve videos from upload_queue (status: pending or failed)
4. Generate YouTube metadata (AI-generated titles and descriptions)
5. Save metadata to database
6. Download selected videos from Congress website
7. Update queue status to 'processing' (or 'failed' if download fails)
8. Prepare upload configuration with metadata
9. Trigger generic YouTube uploader DAG
10. Update queue status to 'completed' or 'failed' (maintains history)
11. Update video_topics table with YouTube upload status

The upload_queue table provides:
- Priority-based processing (higher AI score = higher priority)
- Retry capability for failed uploads
- Status tracking (pending, processing, completed, failed, skipped)
- Attempt tracking to prevent infinite retries
- Complete upload history (records are not deleted, only status updated)

Runs independently from the congress_session_processor DAG.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api

from congress_videos.modules import utils as cv_utils
from congress_videos.modules import youtube_upload as yt_upload
from congress_videos.modules import thumbnail_generator as thumb_gen
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
IS_DEVELOPMENT = POSTGRES_SCHEMA == 'development'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'congress_youtube_uploader',
    default_args=default_args,
    description='Daily YouTube upload: Select top Congress videos by AI score, generate metadata, download, and upload',
    schedule_interval='0 10 * * *',  # Run at 10:00 AM daily
    start_date=datetime(2025, 10, 3),
    catchup=False,
    tags=['congress'],
    params={
        "max_videos": 5,  # Maximum number of videos to upload per day
        "min_interest_score": 6,  # Minimum AI interest score to consider
        "isTesting": IS_DEVELOPMENT  # True in development (uploads as private), False in production (uploads as public)
    }
) as dag:

    t0 = PythonOperator(
        task_id='ensure_data_directory',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory('congress_videos'),
            'data_directory_path'
        ),
    )

    # Step 1: Query top videos from database based on AI interest score
    t1_db = PostgreSQLOperator(
        task_id='get_top_videos_by_score',
        operation='get_top_videos_for_upload',
        output_xcom_key='top_videos'
    )

    # Step 2: Add selected videos to upload queue
    t2_db = PostgreSQLOperator(
        task_id='add_to_upload_queue',
        operation='add_to_upload_queue',
        xcom_keys={'top_videos': 'top_videos'},
        output_xcom_key='queue_additions'
    )

    # Step 3: Get videos from upload queue (pending/failed)
    t3_db = PostgreSQLOperator(
        task_id='get_from_upload_queue',
        operation='get_from_upload_queue',
        output_xcom_key='queued_videos'
    )

    # Step 4: Generate YouTube metadata for queued videos
    t4 = PythonOperator(
        task_id='generate_youtube_metadata',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: cv_utils.generate_youtube_metadata_for_selected_videos(
                ti.xcom_pull(key='queued_videos')
            ),
            'youtube_metadata_results'
        ),
    )

    # Step 5: Save YouTube metadata to database
    t5_db = PostgreSQLOperator(
        task_id='save_youtube_metadata_to_db',
        operation='save_youtube_metadata',
        output_xcom_key='db_metadata_updates'
    )

    # Step 6: Download selected videos from queue
    t6 = PythonOperator(
        task_id='download_selected_videos',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: cv_utils.download_videos_for_upload(
                ti.xcom_pull(key='queued_videos'),
                ti.xcom_pull(key='data_directory_path')
            ),
            'download_results'
        ),
    )

    # Step 6a: Generate thumbnail text using AI (short, impactful 3-6 words)
    t6a = PythonOperator(
        task_id='generate_thumbnail_text',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: thumb_gen.generate_thumbnail_text_for_videos(
                ti.xcom_pull(key='queued_videos'),
                ti.xcom_pull(key='youtube_metadata_results')
            ),
            'thumbnail_text_results'
        ),
    )

    # Step 6b: Generate thumbnails with impactful text
    t6b = PythonOperator(
        task_id='generate_thumbnails',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: thumb_gen.generate_video_thumbnails(
                ti.xcom_pull(key='queued_videos'),
                ti.xcom_pull(key='thumbnail_text_results'),
                ti.xcom_pull(key='download_results'),
                ti.xcom_pull(key='data_directory_path')
            ),
            'thumbnail_results'
        ),
    )

    # Step 6c: Save thumbnail info to database
    t6c_db = PostgreSQLOperator(
        task_id='save_thumbnail_info_to_db',
        operation='save_thumbnail_info',
        xcom_keys={
            'thumbnail_text_results': 'thumbnail_text_results',
            'thumbnail_results': 'thumbnail_results'
        },
        output_xcom_key='db_thumbnail_updates'
    )

    # Step 7: Update queue status after download (mark as processing or failed)
    t7_db = PostgreSQLOperator(
        task_id='update_queue_after_download',
        operation='update_queue_status',
        xcom_keys={'download_results': 'download_results'},
        output_xcom_key='queue_download_updates'
    )

    # Step 8: Prepare upload configuration for generic YouTube uploader DAG
    t8_prep = PythonOperator(
        task_id='prepare_upload_config',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_upload.prepare_youtube_upload_config(
                ti.xcom_pull(key='download_results'),
                ti.xcom_pull(key='youtube_metadata_results'),
                is_testing=context["params"].get("isTesting", False)
            ),
            'upload_config'
        ),
    )

    # Step 9: Trigger the generic YouTube uploader DAG with config from XCom
    def trigger_upload_with_config(ti, **context):
        """Trigger the generic YouTube uploader DAG with config from XCom."""
        import time
        from airflow.models import DagRun, TaskInstance

        # Get config from XCom
        config = ti.xcom_pull(task_ids='prepare_upload_config', key='upload_config')

        if not config:
            logging.warning("No upload config found, skipping upload")
            # Return empty results structure
            ti.xcom_push(key='upload_results', value={'upload_details': []})
            return None

        # Trigger the DAG
        logging.info(f"Triggering generic_youtube_uploader with config: {config}")
        dag_run = trigger_dag_api(
            dag_id='generic_youtube_uploader',
            conf=config,
            run_id=f"triggered_from_congress_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")

        # Wait for completion
        logging.info("Waiting for upload to complete...")
        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ['success', 'failed']:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

                # Pull upload results from the triggered DAG
                from airflow.models import XCom
                upload_results = XCom.get_many(
                    execution_date=dag_run.execution_date,
                    dag_ids=['generic_youtube_uploader'],
                    task_ids=['upload_videos'],
                    key='return_value',
                    limit=1
                )

                if upload_results:
                    results_data = upload_results[0].value
                    logging.info(f"Retrieved upload results: {results_data}")
                    # Push to XCom for next task
                    ti.xcom_push(key='upload_results', value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    # Create results based on config and DAG state
                    upload_details = []
                    for video_config in config.get('videos', []):
                        upload_details.append({
                            'entry_id': video_config.get('entry_id'),  # Use entry_id from config
                            'video_file': video_config.get('video_file'),
                            'success': dag_run.state == 'success',
                            'video_id': None,  # Can't get without actual results
                            'youtube_video_id': None,  # Can't get without actual results
                            'error': 'Upload failed - no results available' if dag_run.state == 'failed' else None
                        })
                    ti.xcom_push(key='upload_results', value={'upload_details': upload_details})

                if dag_run.state == 'failed':
                    raise Exception(f"Upload DAG failed: {dag_run.run_id}")

                return dag_run.run_id

    t9_trigger = PythonOperator(
        task_id='trigger_youtube_upload',
        python_callable=trigger_upload_with_config,
    )

    # Step 10: Update queue status after upload (mark as completed or failed)
    t10_db = PostgreSQLOperator(
        task_id='update_queue_after_upload',
        operation='update_queue_status',
        xcom_keys={'upload_results': 'upload_results'},
        output_xcom_key='queue_upload_updates'
    )

    # Step 11: Update YouTube upload status in video_topics table
    t11_db = PostgreSQLOperator(
        task_id='update_youtube_status_in_db',
        operation='update_youtube_status',
        output_xcom_key='db_youtube_updates'
    )

    # Task dependencies - Optimized with parallel execution where possible
    # Initial sequential flow to get queued videos and metadata
    t0 >> t1_db >> t2_db >> t3_db >> t4

    # After metadata generation (t4), run these in parallel:
    # - Save metadata to DB (t5_db)
    # - Download videos (t6)
    # - Generate thumbnail text (t6a)
    t4 >> [t5_db, t6, t6a]

    # Thumbnail generation needs both download and text
    [t6, t6a] >> t6b

    # After thumbnails are generated, save to DB
    t6b >> t6c_db

    # After download, run these in parallel:
    # - Update queue status (t7_db)
    # - Prepare upload config (t8_prep)
    t6 >> [t7_db, t8_prep]

    # Upload needs thumbnails saved, queue status update, and config ready
    [t6c_db, t7_db, t8_prep] >> t9_trigger

    # Final status updates run sequentially
    t9_trigger >> t10_db >> t11_db
