"""
Reap Shorts Uploader DAG

Progressive daily upload of downloaded Reap Shorts clips to YouTube.
Uploads the highest-virality-scored clips first, up to max_shorts_per_day per run.

Flow:
1. get_pending_shorts     — query video_shorts for unuploaded downloaded clips
2. trigger_youtube_upload — upload each clip via generic_youtube_uploader
3. mark_shorts_uploaded   — persist youtube_video_id + is_uploaded=TRUE
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api

from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.config.paths import YOUTUBE_TOKEN_FILE
from utils.ai_helpers import truncate_text
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local

load_env_if_local()

POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'reap_shorts_uploader',
    default_args=default_args,
    description='Upload top-virality Reap Shorts clips to YouTube (max N per day)',
    schedule='0 17 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'youtube', 'shorts', 'reap'],
    params={
        'max_shorts_per_day': 2,
        'min_virality_score': 0.0,
    }
) as dag:

    def _get_pending_shorts(ti, **context):
        max_shorts = context['params'].get('max_shorts_per_day', 2)
        min_virality = context['params'].get('min_virality_score', 0.0)

        db = CongressionalVideoDB()
        shorts = db.get_pending_shorts(limit=max_shorts, min_virality_score=min_virality)

        if not shorts:
            logging.info("No pending shorts to upload")

        ti.xcom_push(key='pending_shorts', value=shorts)

    t1 = PythonOperator(
        task_id='get_pending_shorts',
        python_callable=_get_pending_shorts,
    )

    def _trigger_youtube_upload(ti, **context):
        import time
        from airflow.models import DagRun, XCom

        pending_shorts = ti.xcom_pull(key='pending_shorts') or []

        if not pending_shorts:
            logging.info("No pending shorts — skipping upload")
            ti.xcom_push(key='upload_results', value={'upload_details': []})
            return None

        db = CongressionalVideoDB()

        # Build chapter_id → title lookup
        chapter_ids = list({s.get('chapter_id') for s in pending_shorts if s.get('chapter_id')})
        chapter_titles = db.get_chapter_titles(chapter_ids)

        videos = []
        for short in pending_shorts:
            chapter_id = short.get('chapter_id')
            chapter_title = chapter_titles.get(chapter_id) or f'Short clip {short.get("id", "")}'
            title = truncate_text(f"{chapter_title} #Shorts", max_length=100)

            description = '#Shorts'

            video_config = {
                'short_id': short.get('id'),
                'reap_clip_id': short.get('reap_clip_id'),
                'video_file': short.get('local_file_path'),
                'title': title,
                'description': description,
                'category_id': '25',
                'privacy_status': 'public',
                'tags': ['shorts', 'congress', 'politics', 'españa', 'congreso'],
                'made_for_kids': False,
            }
            videos.append(video_config)

        config = {
            'token_file': YOUTUBE_TOKEN_FILE,
            'videos': videos,
        }

        logging.info(f"Triggering generic_youtube_uploader with {len(videos)} shorts")
        dag_run = trigger_dag_api(
            dag_id='generic_youtube_uploader',
            conf=config,
            run_id=f"shorts_upload_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")
        logging.info("Waiting for upload to complete...")

        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ['success', 'failed']:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

                upload_results = XCom.get_many(
                    execution_date=dag_run.execution_date,
                    dag_ids=['generic_youtube_uploader'],
                    task_ids=['upload_videos'],
                    key='return_value',
                    limit=1,
                )

                if upload_results:
                    results_data = upload_results[0].value
                    logging.info(f"Retrieved upload results: {results_data}")
                    ti.xcom_push(key='upload_results', value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    upload_details = []
                    for video_config in videos:
                        upload_details.append({
                            'short_id': video_config.get('short_id'),
                            'reap_clip_id': video_config.get('reap_clip_id'),
                            'video_file': video_config.get('video_file'),
                            'success': dag_run.state == 'success',
                            'youtube_video_id': None,
                            'error': 'Upload failed - no results available' if dag_run.state == 'failed' else None,
                        })
                    ti.xcom_push(key='upload_results', value={'upload_details': upload_details})

                if dag_run.state == 'failed':
                    raise Exception(f"Upload DAG failed: {dag_run.run_id}")

                return dag_run.run_id

    t2 = PythonOperator(
        task_id='trigger_youtube_upload',
        python_callable=_trigger_youtube_upload,
    )

    def _mark_shorts_uploaded(ti, **context):
        upload_results = ti.xcom_pull(key='upload_results') or {}
        upload_details = upload_results.get('upload_details', [])

        if not upload_details:
            logging.info("No upload results to process")
            return

        db = CongressionalVideoDB()
        successful = 0
        failed = 0

        for detail in upload_details:
            reap_clip_id = detail.get('reap_clip_id')
            youtube_video_id = detail.get('youtube_video_id')
            if detail.get('success') and reap_clip_id and youtube_video_id:
                db.mark_short_uploaded(reap_clip_id, youtube_video_id)
                successful += 1
            else:
                failed += 1

        logging.info(f"Upload summary: {successful} successful, {failed} failed")

    t3 = PythonOperator(
        task_id='mark_shorts_uploaded',
        python_callable=_mark_shorts_uploaded,
    )

    t1 >> t2 >> t3
