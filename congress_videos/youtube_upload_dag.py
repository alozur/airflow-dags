"""
Congress YouTube Chapter Uploader DAG

This DAG uploads individual chapters from congressional videos to YouTube:

1. Ensure data directory exists
2. Query top 5 uploadable chapters from uploadable_chapters view
   - Ordered by relevance_score DESC (highest relevance first)
   - Then by created_at DESC (most recent first)
   - Only chapters not yet uploaded to YouTube

The uploadable_chapters view filters chapters with:
- is_uploaded_to_youtube = FALSE
- relevance_score >= 2 (configurable in view)
- Joined with source video metadata

This allows uploading the most relevant and recent congressional debate chapters
as standalone YouTube videos.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api

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
    'congress_youtube_chapter_uploader',
    default_args=default_args,
    description='Upload top congressional video chapters to YouTube based on relevance score',
    schedule='0 17 * * *',  # Run at 17:00 UTC daily
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'youtube', 'chapters'],
    params={
        "max_chapters": 1,  # Default max chapters per day (overridden by quota logic)
        "min_relevance_score": 2,  # Minimum relevance score (0-5)
        "isTesting": False  # Hardcoded to False so uploads are always public
    }
) as dag:

    # Step 0: Ensure data directory exists
    t0 = PythonOperator(
        task_id='ensure_data_directory',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory('congress_videos'),
            'data_directory_path'
        ),
    )

    # Step 1: Check daily upload quota
    # Queries DB for uploads today and pending queue size.
    # Returns remaining_quota = 1 (queue ≤ 15) or 2 (queue > 15) minus today's uploads.
    t1_quota = PostgreSQLOperator(
        task_id='check_upload_quota',
        operation='check_upload_quota',
        output_xcom_key='upload_quota',
    )

    # Step 1b: Short-circuit if daily quota already reached
    t1_skip = ShortCircuitOperator(
        task_id='skip_if_quota_reached',
        python_callable=lambda ti: (ti.xcom_pull(key='upload_quota') or {}).get('remaining_quota', 0) > 0,
    )

    # Step 2: Get uploadable chapters — limit comes from remaining_quota via XCom
    # Ordered by: session_date DESC, relevance_score DESC, created_at DESC
    t1_db = PostgreSQLOperator(
        task_id='get_uploadable_chapters',
        operation='get_uploadable_chapters',
        output_xcom_key='uploadable_chapters'
    )

    def _generate_youtube_metadata(ti):
        from congress_videos.modules.youtube import youtube_ai
        return xcom_task(
            ti,
            lambda: youtube_ai.generate_youtube_metadata_for_selected_videos(
                ti.xcom_pull(key='uploadable_chapters')
            ),
            'youtube_metadata_results'
        )

    def _generate_thumbnail_text(ti):
        from congress_videos.modules import thumbnail_generator as thumb_gen
        return xcom_task(
            ti,
            lambda: thumb_gen.generate_thumbnail_text_for_videos(
                ti.xcom_pull(key='uploadable_chapters'),
                ti.xcom_pull(key='youtube_metadata_results')
            ),
            'thumbnail_text_results'
        )

    def _generate_thumbnails(ti):
        from congress_videos.modules import thumbnail_generator as thumb_gen
        return xcom_task(
            ti,
            lambda: thumb_gen.generate_video_thumbnails(
                ti.xcom_pull(key='uploadable_chapters'),
                ti.xcom_pull(key='thumbnail_text_results'),
                None,
                ti.xcom_pull(key='data_directory_path')
            ),
            'thumbnail_results'
        )

    def _extract_chapter_videos(ti):
        from congress_videos.modules import video_splitter
        return xcom_task(
            ti,
            lambda: video_splitter.extract_chapters_from_video(
                ti.xcom_pull(key='uploadable_chapters'),
                ti.xcom_pull(key='data_directory_path')
            ),
            'chapter_extraction_results'
        )

    def _prepare_upload_config(ti, **context):
        from congress_videos.modules.youtube import prepare_chapter_upload_config
        return xcom_task(
            ti,
            lambda: prepare_chapter_upload_config(
                ti.xcom_pull(key='chapter_extraction_results'),
                ti.xcom_pull(key='youtube_metadata_results'),
                ti.xcom_pull(key='thumbnail_results'),
                is_testing=context["params"].get("isTesting", False)
            ),
            'upload_config'
        )

    # Step 2: Generate YouTube metadata for chapters
    t2 = PythonOperator(
        task_id='generate_youtube_metadata',
        python_callable=_generate_youtube_metadata,
    )

    # Step 3: Generate thumbnail text using AI (3-6 words, max 40 chars)
    t3 = PythonOperator(
        task_id='generate_thumbnail_text',
        python_callable=_generate_thumbnail_text,
    )

    # Step 4: Generate thumbnails and save to video_id/chapter_id/ folder
    t4 = PythonOperator(
        task_id='generate_thumbnails',
        python_callable=_generate_thumbnails,
    )

    # Step 5: Extract chapter videos from source YouTube videos using ffmpeg
    t5 = PythonOperator(
        task_id='extract_chapter_videos',
        python_callable=_extract_chapter_videos,
    )

    # Step 6: Prepare upload configuration for generic YouTube uploader DAG
    t6 = PythonOperator(
        task_id='prepare_upload_config',
        python_callable=_prepare_upload_config,
    )

    # Step 7: Trigger generic YouTube uploader DAG and wait for completion
    def trigger_upload_with_config(ti, **context):
        """Trigger the generic YouTube uploader DAG with config from XCom."""
        import time
        from airflow.models import XCom

        # Get config from XCom
        config = ti.xcom_pull(key='upload_config')

        if not config:
            logging.warning("No upload config found, skipping upload")
            ti.xcom_push(key='upload_results', value={'upload_details': []})
            return None

        # Trigger the DAG
        logging.info(f"Triggering generic_youtube_uploader with {len(config.get('videos', []))} videos")
        dag_run = trigger_dag_api(
            dag_id='generic_youtube_uploader',
            conf=config,
            run_id=f"chapter_upload_{context['run_id']}",
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
                    ti.xcom_push(key='upload_results', value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    # Create results based on config and DAG state
                    upload_details = []
                    for video_config in config.get('videos', []):
                        upload_details.append({
                            'chapter_id': video_config.get('chapter_id'),
                            'video_id': video_config.get('video_id'),
                            'video_file': video_config.get('video_file'),
                            'success': dag_run.state == 'success',
                            'youtube_video_id': None,
                            'error': 'Upload failed - no results available' if dag_run.state == 'failed' else None
                        })
                    ti.xcom_push(key='upload_results', value={'upload_details': upload_details})

                if dag_run.state == 'failed':
                    raise Exception(f"Upload DAG failed: {dag_run.run_id}")

                return dag_run.run_id

    t7 = PythonOperator(
        task_id='trigger_youtube_upload',
        python_callable=trigger_upload_with_config,
    )

    # Step 8: Update database to mark chapters as uploaded
    t8_db = PostgreSQLOperator(
        task_id='mark_chapters_uploaded',
        operation='mark_chapters_uploaded',
        xcom_keys={'upload_results': 'upload_results'},
        output_xcom_key='chapter_upload_updates'
    )

    # Task dependencies
    t0 >> t1_quota >> t1_skip >> t1_db >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8_db
