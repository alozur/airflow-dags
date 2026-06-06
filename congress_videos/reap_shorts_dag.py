"""
Congress Reap Shorts DAG

Selects eligible chapters, optionally pre-trims long clips using AI + SRT context,
uploads to Reap, waits for job completion, and downloads all resulting clips.
"""

import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.base import BaseSensorOperator

from congress_videos.config.paths import DOWNLOADS_DIR, PROJECT_DATA_DIR, get_short_file_path
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.video_splitter import convert_srt_time_to_seconds, split_video_chapter
from congress_videos.reap_api import ReapApiClient, ReapCreditsExhausted
from congress_videos.srt_helpers import find_srt_for_chapter, select_pretrim_window
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
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


def _interval_to_srt(interval_str: str) -> str:
    """Convert DB time to SRT-compatible 'HH:MM:SS,mmm'. Handles both HH:MM:SS and HH:MM:SS,mmm."""
    if ',' in interval_str:
        return interval_str
    return f"{interval_str},000"



def _find_source_video(video_id: str) -> str | None:
    """Search downloads folder across all date subfolders for a video file."""
    if not os.path.isdir(DOWNLOADS_DIR):
        return None
    for date_folder in os.listdir(DOWNLOADS_DIR):
        video_folder = os.path.join(DOWNLOADS_DIR, date_folder, str(video_id))
        if not os.path.isdir(video_folder):
            continue
        for filename in os.listdir(video_folder):
            if filename.endswith(('.mp4', '.mkv', '.webm')) and 'chapter_video' not in filename:
                return os.path.join(video_folder, filename)
    return None


def _ffmpeg_extract_window(source_path: str, dest_path: str, start_secs: float, end_secs: float) -> None:
    """Re-extract a precise time window from source into dest_path using ffmpeg."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    duration = end_secs - start_secs
    cmd = [
        'ffmpeg', '-y',
        '-ss', str(start_secs),
        '-t', str(duration),
        '-i', source_path,
        '-c', 'copy',
        '-avoid_negative_ts', 'make_zero',
        dest_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg window extract failed: {result.stderr}")


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
    'congress_reap_shorts',
    default_args=default_args,
    description='Select chapters, upload to Reap for clip generation, download resulting shorts',
    schedule='0 15 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'reap', 'shorts'],
    params={
        "max_chapters": 3,
        "min_relevance_score": 3,
        "pre_trim_threshold_secs": 480,
        "pre_trim_target_secs": 360,
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

    def _query_chapters(ti, **context):
        db = CongressionalVideoDB()
        chapters = db.get_chapters_for_shorts(
            limit=context['params']['max_chapters'],
            min_relevance_score=context['params']['min_relevance_score'],
        )
        ti.xcom_push(key='chapters_for_shorts', value=chapters)
        return bool(chapters)  # False → ShortCircuitOperator skips all downstream tasks

    t1 = ShortCircuitOperator(
        task_id='query_chapters',
        python_callable=_query_chapters,
    )

    def _extract_and_pretrim_clip(ti, **context):
        chapters = ti.xcom_pull(key='chapters_for_shorts') or []
        threshold_secs = context['params']['pre_trim_threshold_secs']
        target_secs = context['params']['pre_trim_target_secs']

        clip_results = []

        for chapter in chapters:
            chapter_id = chapter['chapter_id']
            video_id = chapter['video_id']
            start_time = str(chapter['start_time'])
            end_time = str(chapter['end_time'])

            duration = convert_srt_time_to_seconds(_interval_to_srt(end_time)) - convert_srt_time_to_seconds(_interval_to_srt(start_time))

            source_video_path = _find_source_video(video_id)
            if not source_video_path:
                logging.warning(
                    "No source video for video_id=%s chapter_id=%s — skipping", video_id, chapter_id
                )
                continue

            chapter_folder = os.path.join(PROJECT_DATA_DIR, str(video_id), str(chapter_id))
            os.makedirs(chapter_folder, exist_ok=True)
            clip_path = os.path.join(chapter_folder, 'chapter_video.mp4')

            result = split_video_chapter(
                source_video_path=source_video_path,
                output_path=clip_path,
                start_time=_interval_to_srt(start_time),
                end_time=_interval_to_srt(end_time),
            )

            if not result['success']:
                logging.warning(
                    "Chapter %s extraction failed: %s — skipping", chapter_id, result.get('error')
                )
                continue

            pretrim_start = None
            pretrim_end = None
            pretrim_used_srt = False

            if duration > threshold_secs:
                session_date = chapter.get('session_date') or None
                srt_path = find_srt_for_chapter(str(video_id), str(chapter_id), session_date)

                if srt_path:
                    window = select_pretrim_window(srt_path, target_secs=target_secs)

                    if window:
                        trimmed_path = os.path.join(chapter_folder, 'chapter_video_trimmed.mp4')
                        try:
                            _ffmpeg_extract_window(
                                source_path=clip_path,
                                dest_path=trimmed_path,
                                start_secs=window['start_seconds'],
                                end_secs=window['end_seconds'],
                            )
                            clip_path = trimmed_path
                            pretrim_start = window['start_seconds']
                            pretrim_end = window['end_seconds']
                            pretrim_used_srt = True
                            duration = pretrim_end - pretrim_start
                        except RuntimeError as exc:
                            logging.warning(
                                "Pre-trim ffmpeg failed for chapter %s: %s — using full clip",
                                chapter_id, exc
                            )
                else:
                    logging.warning(
                        "No SRT found for chapter %s (video_id=%s) — sending full clip to Reap",
                        chapter_id, video_id
                    )

            if duration < 120:
                logging.warning(
                    "Chapter %s clip duration %.1fs < 120s minimum after trim — skipping",
                    chapter_id, duration
                )
                continue

            clip_results.append({
                'chapter_id': chapter_id,
                'clip_path': clip_path,
                'pretrim_start': pretrim_start,
                'pretrim_end': pretrim_end,
                'pretrim_used_srt': pretrim_used_srt,
                'scoring_reasoning': chapter.get('scoring_reasoning', '') or '',
            })

        ti.xcom_push(key='clip_results', value=clip_results)

    t2 = PythonOperator(
        task_id='extract_and_pretrim_clip',
        python_callable=_extract_and_pretrim_clip,
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

    t3 = PythonOperator(
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

    t4 = PythonOperator(
        task_id='create_reap_job',
        python_callable=_create_reap_job,
    )

    t5 = ReapJobSensor(
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

    t6 = PythonOperator(
        task_id='check_credits_status',
        python_callable=_check_credits_status,
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6
