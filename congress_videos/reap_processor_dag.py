"""
Congress Reap Processor DAG

Triggered by congress_reap_clip_preparer. Uploads prepared clips to Reap,
waits for job completion, and downloads all resulting shorts.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.base import BaseSensorOperator

from congress_videos.config.paths import get_short_file_path
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.reap_api import ReapApiClient, ReapCreditsExhausted
from utils.env_loader import load_env_if_local

load_env_if_local()

POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')

_TERMINAL_STATES = {'completed', 'failed', 'invalid', 'expired', 'error'}
_FAILURE_STATES = {'failed', 'invalid', 'expired', 'error'}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


class ReapJobSensor(BaseSensorOperator):
    """
    Polls the Reap API until a job reaches a terminal state.

    On 'completed': downloads all clips and inserts one video_shorts row per clip.
    On failure states: updates DB status and raises AirflowException.
    """

    def __init__(self, reap_project_id_key: str, short_id_key: str, **kwargs):
        super().__init__(**kwargs)
        self.reap_project_id_key = reap_project_id_key
        self.short_id_key = short_id_key

    def poke(self, context) -> bool:
        ti = context['ti']
        reap_project_id = ti.xcom_pull(key=self.reap_project_id_key)
        chapter_id = ti.xcom_pull(key='chapter_id_for_sensor')

        if not reap_project_id:
            raise AirflowException("ReapJobSensor: reap_project_id not found in XCom")

        reap_client = ReapApiClient()
        db = CongressionalVideoDB()

        try:
            status_data = reap_client.get_project_status(reap_project_id)
        except ReapCreditsExhausted:
            logging.warning("ReapJobSensor: credits exhausted polling project %s", reap_project_id)
            ti.xcom_push(key='credits_exhausted', value=True)
            return True

        status = status_data.get('status', '')
        logging.info("Reap project %s status: %s", reap_project_id, status)

        if status not in _TERMINAL_STATES:
            return False

        if status == 'completed':
            clips = reap_client.get_project_clips(reap_project_id)
            logging.info("Reap project %s completed — downloading %d clips", reap_project_id, len(clips))

            for clip in clips:
                clip_id = clip['clip_id']
                clip_url = clip['clip_url']
                virality = float(clip['virality_score'])

                dest_path = get_short_file_path(chapter_id, clip_id)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                reap_client.download_clip(clip_url, dest_path)

                db.insert_video_short_clip(
                    chapter_id=chapter_id,
                    reap_project_id=reap_project_id,
                    reap_clip_id=clip_id,
                    reap_virality_score=virality,
                    reap_clip_url=clip_url,
                    local_file_path=dest_path,
                )

            return True

        if status in _FAILURE_STATES:
            db.update_video_short_status(reap_project_id, status)
            raise AirflowException(f"Reap job {reap_project_id} ended with terminal status: {status}")

        return False


with DAG(
    'congress_reap_processor',
    default_args=default_args,
    description='Upload clips to Reap, wait for job completion, download resulting shorts',
    schedule=None,
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'reap', 'shorts'],
) as dag:

    def _load_clip_results(ti, **context) -> bool:
        import json
        raw = context['dag_run'].conf.get('clip_results', []) if context.get('dag_run') and context['dag_run'].conf else []
        if isinstance(raw, str):
            try:
                clip_results = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                logging.error("Could not parse clip_results conf — expected list or JSON string")
                clip_results = []
        else:
            clip_results = raw
        ti.xcom_push(key='clip_results', value=clip_results)
        return bool(clip_results)

    t0 = ShortCircuitOperator(
        task_id='load_clip_results',
        python_callable=_load_clip_results,
    )

    def _upload_to_reap(ti, **context):
        clip_results = ti.xcom_pull(key='clip_results') or []

        if not clip_results:
            ti.xcom_push(key='upload_results', value=[])
            return

        reap_client = ReapApiClient()
        upload_results = []

        for clip_info in clip_results:
            chapter_id = clip_info['chapter_id']
            clip_path = clip_info['clip_path']

            try:
                upload_data = reap_client.get_upload_url(os.path.basename(clip_path))
                upload_id = upload_data['upload_id']
                upload_url = upload_data['uploadUrl']

                reap_client.upload_file(upload_url, clip_path)
                logging.info("Chapter %s uploaded to Reap — upload_id=%s", chapter_id, upload_id)

                upload_results.append({
                    'chapter_id': chapter_id,
                    'upload_id': upload_id,
                    'pretrim_start': clip_info['pretrim_start'],
                    'pretrim_end': clip_info['pretrim_end'],
                    'pretrim_used_srt': clip_info['pretrim_used_srt'],
                    'scoring_reasoning': clip_info['scoring_reasoning'],
                })

            except ReapCreditsExhausted:
                logging.warning(
                    "Reap credits exhausted uploading chapter %s — stopping further uploads", chapter_id
                )
                ti.xcom_push(key='credits_exhausted', value=True)
                break

        ti.xcom_push(key='upload_results', value=upload_results)

    t1 = PythonOperator(
        task_id='upload_to_reap',
        python_callable=_upload_to_reap,
    )

    def _create_reap_job(ti, **context):
        upload_results = ti.xcom_pull(key='upload_results') or []

        if not upload_results:
            ti.xcom_push(key='reap_job_results', value=[])
            return

        reap_client = ReapApiClient()
        db = CongressionalVideoDB()
        job_results = []

        for upload_info in upload_results:
            chapter_id = upload_info['chapter_id']
            upload_id = upload_info['upload_id']
            scoring_reasoning = (upload_info.get('scoring_reasoning') or '')[:500]

            try:
                job_data = reap_client.create_clips_job(
                    upload_id,
                    language='es',
                    prompt=scoring_reasoning,
                )
                reap_project_id = job_data['project_id']

                short_id = db.insert_video_short(
                    chapter_id=chapter_id,
                    reap_project_id=reap_project_id,
                    reap_status='processing',
                    pretrim_start_secs=upload_info['pretrim_start'],
                    pretrim_end_secs=upload_info['pretrim_end'],
                    pretrim_used_srt=upload_info['pretrim_used_srt'],
                )

                logging.info(
                    "Reap job created — project_id=%s chapter=%s short_id=%s",
                    reap_project_id, chapter_id, short_id
                )

                job_results.append({
                    'reap_project_id': reap_project_id,
                    'short_id': short_id,
                    'chapter_id': chapter_id,
                })

            except ReapCreditsExhausted:
                logging.warning(
                    "Reap credits exhausted creating job for chapter %s", chapter_id
                )
                ti.xcom_push(key='credits_exhausted', value=True)
                break

        ti.xcom_push(key='reap_job_results', value=job_results)

        # Single-sensor approach: sensor handles the first queued job;
        # remaining jobs are left for subsequent DAG runs (max_chapters keeps the backlog small)
        if job_results:
            first_job = job_results[0]
            ti.xcom_push(key='reap_project_id_for_sensor', value=first_job['reap_project_id'])
            ti.xcom_push(key='chapter_id_for_sensor', value=first_job['chapter_id'])

    t2 = PythonOperator(
        task_id='create_reap_job',
        python_callable=_create_reap_job,
    )

    t3 = ReapJobSensor(
        task_id='wait_for_reap',
        reap_project_id_key='reap_project_id_for_sensor',
        short_id_key='short_id_for_sensor',
        poke_interval=900,
        timeout=7200,
        mode='reschedule',
    )

    def _check_credits_status(ti, **context):
        credits_exhausted = ti.xcom_pull(key='credits_exhausted')
        if credits_exhausted:
            logging.warning(
                "REAP CREDITS EXHAUSTED — no more chapters will be processed "
                "until credits are replenished"
            )

    t4 = PythonOperator(
        task_id='check_credits_status',
        python_callable=_check_credits_status,
    )

    t0 >> t1 >> t2 >> t3 >> t4
