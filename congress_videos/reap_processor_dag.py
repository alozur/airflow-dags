"""
Congress Reap Processor DAG

Runs on its own cron schedule (daily at 14:30). On each run, claims exactly one
video_shorts row with reap_status='pending' from the queue, uploads the staged clip
to Reap, waits for job completion via sensor, and downloads all resulting shorts.
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

_REAP_CLIP_TOPICS = "Spanish Parliament, Spanish politics, parliamentary debates"

_REAP_CLIP_PROMPT = (
    "Find politically charged moments: strong opinions, direct confrontations between party leaders, "
    "memorable soundbites, and statements likely to spark public debate. "
    "Prioritize: the President or opposition leader making a clear claim, heated exchanges, surprising "
    "revelations, or moments that capture the political conflict in one sentence. "
    "Skip procedural formalities, roll calls, and transition segments with no political content. "
    "IMPORTANT: each clip must feature a single continuous speaker — never split-screen, "
    "never simultaneous cuts between two speakers on screen at the same time."
)


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

    def __init__(self, reap_project_id_key: str, chapter_id_key: str, **kwargs):
        super().__init__(**kwargs)
        self.reap_project_id_key = reap_project_id_key
        self.chapter_id_key = chapter_id_key

    def poke(self, context) -> bool:
        ti = context['ti']
        reap_project_id = ti.xcom_pull(key=self.reap_project_id_key)
        chapter_id = ti.xcom_pull(key=self.chapter_id_key)

        if not reap_project_id:
            raise AirflowException("ReapJobSensor: reap_project_id not found in XCom")

        reap_client = ReapApiClient()

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
            db = CongressionalVideoDB()
            clips = reap_client.get_project_clips(reap_project_id)
            logging.info("Reap project %s completed — downloading %d clips", reap_project_id, len(clips))

            for clip in clips:
                clip_id = clip['clip_id']
                clip_url = clip['clip_url']
                virality = float(clip['virality_score'])

                dest_path = get_short_file_path(chapter_id, clip_id)
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
            CongressionalVideoDB().update_video_short_status(reap_project_id, status)
            raise AirflowException(f"Reap job {reap_project_id} ended with terminal status: {status}")


with DAG(
    'congress_reap_processor',
    default_args=default_args,
    description='Claim one pending clip from the queue, upload to Reap, wait for completion, download shorts',
    schedule='30 14 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    max_active_runs=1,
    tags=['congress', 'reap', 'shorts'],
) as dag:

    def _claim_clip_from_queue(ti, **context) -> bool:
        db = CongressionalVideoDB()
        claimed = db.claim_pending_clip()
        if not claimed:
            logging.info("No pending clips in queue — skipping run")
            return False
        ti.xcom_push(key='claimed_clip', value=claimed)
        logging.info("Claimed clip: short_id=%s chapter_id=%s", claimed['id'], claimed['chapter_id'])
        return True

    t0 = ShortCircuitOperator(
        task_id='claim_clip_from_queue',
        python_callable=_claim_clip_from_queue,
    )

    def _upload_to_reap(ti, **context):
        claimed_clip = ti.xcom_pull(key='claimed_clip')
        chapter_id = claimed_clip['chapter_id']
        clip_path = claimed_clip['staged_clip_path']

        if not clip_path or not os.path.exists(clip_path):
            CongressionalVideoDB().update_video_short_status_by_id(claimed_clip['id'], 'failed')
            raise AirflowException(
                f"upload_to_reap: staged_clip_path missing or file not found for "
                f"short_id={claimed_clip['id']} chapter_id={chapter_id}: {clip_path}"
            )

        reap_client = ReapApiClient()

        try:
            upload_data = reap_client.get_upload_url(os.path.basename(clip_path))
            upload_id = upload_data['upload_id']
            upload_url = upload_data['uploadUrl']

            reap_client.upload_file(upload_url, clip_path)
            logging.info("Chapter %s uploaded to Reap — upload_id=%s", chapter_id, upload_id)

            ti.xcom_push(key='upload_id', value=upload_id)
            ti.xcom_push(key='chapter_id', value=chapter_id)

        except ReapCreditsExhausted:
            logging.warning(
                "Reap credits exhausted uploading chapter %s — stopping", chapter_id
            )
            ti.xcom_push(key='credits_exhausted', value=True)

    t1 = PythonOperator(
        task_id='upload_to_reap',
        python_callable=_upload_to_reap,
    )

    def _create_reap_job(ti, **context):
        claimed_clip = ti.xcom_pull(key='claimed_clip')
        upload_id = ti.xcom_pull(key='upload_id')

        if not upload_id:
            logging.info("create_reap_job: no upload_id in XCom — skipping job creation")
            return

        chapter_id = claimed_clip['chapter_id']

        reap_client = ReapApiClient()

        try:
            job_data = reap_client.create_clips_job(
                upload_id,
                language='es',
                name=f"chapter_{chapter_id}",
                clipTopics=_REAP_CLIP_TOPICS,
                prompt=_REAP_CLIP_PROMPT,
            )
            reap_project_id = job_data['project_id']

            CongressionalVideoDB().update_video_short_project(claimed_clip['id'], reap_project_id)

            logging.info(
                "Reap job created — project_id=%s chapter=%s short_id=%s",
                reap_project_id, chapter_id, claimed_clip['id'],
            )

            ti.xcom_push(key='reap_project_id_for_sensor', value=reap_project_id)
            ti.xcom_push(key='chapter_id_for_sensor', value=chapter_id)

        except ReapCreditsExhausted:
            logging.warning(
                "Reap credits exhausted creating job for chapter %s", chapter_id
            )
            ti.xcom_push(key='credits_exhausted', value=True)

    t2 = PythonOperator(
        task_id='create_reap_job',
        python_callable=_create_reap_job,
    )

    t3 = ReapJobSensor(
        task_id='wait_for_reap',
        reap_project_id_key='reap_project_id_for_sensor',
        chapter_id_key='chapter_id_for_sensor',
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
