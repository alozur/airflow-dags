"""
Congress Reap Clip Preparer DAG

Selects ALL eligible chapters, optionally pre-trims long clips using AI + SRT context,
validates each clip with ffprobe, and inserts a video_shorts row with reap_status='pending'
for each accepted clip. DAG 2 (reap_processor) runs independently on its own schedule
and consumes the pending queue.
"""

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import DOWNLOADS_DIR, PROJECT_DATA_DIR
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.video_splitter import convert_srt_time_to_seconds, split_video_chapter
from congress_videos.srt_helpers import find_srt_for_chapter, select_pretrim_window
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

load_env_if_local()

POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
_FRAME_TOLERANCE_SECS = 3.0


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


with DAG(
    'congress_reap_clip_preparer',
    default_args=default_args,
    description='Select all eligible chapters, pre-trim clips, insert pending rows in video_shorts queue',
    schedule='0 15 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'reap', 'shorts'],
    params={
        "max_chapters": 0,
        "min_relevance_score": 3,
        "pre_trim_threshold_secs": 300,
        "pre_trim_target_secs": 300,
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
            limit=context['params']['max_chapters'] or None,
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

        db = CongressionalVideoDB()
        inserted_count = 0
        blocked_chapters = []

        for chapter in chapters:
            chapter_id = chapter['chapter_id']
            video_id = chapter['video_id']
            start_time = str(chapter['start_time'])
            end_time = str(chapter['end_time'])

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

            duration = result['duration_seconds']
            pretrim_start = None
            pretrim_end = None
            pretrim_used_srt = False

            if duration > threshold_secs:
                session_date = chapter.get('session_date')
                srt_path = find_srt_for_chapter(str(video_id), str(chapter_id), session_date)
                window = None

                if srt_path:
                    window = select_pretrim_window(srt_path, target_secs=target_secs)
                    if not window:
                        logging.warning(
                            "select_pretrim_window returned None for chapter %s — falling back to first %.0fs",
                            chapter_id, target_secs,
                        )
                else:
                    logging.warning(
                        "No SRT found for chapter %s (video_id=%s) — falling back to first %.0fs",
                        chapter_id, video_id, target_secs,
                    )

                # Pre-trim is MANDATORY: use SRT window if available, else cut first target_secs
                pretrim_start = window['start_seconds'] if window else 0.0
                pretrim_end = window['end_seconds'] if window else float(target_secs)
                pretrim_used_srt = window is not None
                trimmed_path = os.path.join(chapter_folder, 'chapter_video_trimmed.mp4')

                try:
                    _ffmpeg_extract_window(
                        source_path=clip_path,
                        dest_path=trimmed_path,
                        start_secs=pretrim_start,
                        end_secs=pretrim_end,
                    )
                    clip_path = trimmed_path
                    duration = pretrim_end - pretrim_start
                    logging.info(
                        "Chapter %s pre-trimmed: %.1f–%.1fs (%.0fs) srt_window=%s",
                        chapter_id, pretrim_start, pretrim_end, duration, pretrim_used_srt,
                    )
                except RuntimeError as exc:
                    logging.error(
                        "Pre-trim ffmpeg failed for chapter %s: %s — skipping chapter",
                        chapter_id, exc,
                    )
                    continue

            if duration < 120:
                logging.warning(
                    "Chapter %s clip duration %.1fs < 120s minimum after trim — skipping",
                    chapter_id, duration
                )
                continue

            # Inline safety gate: validate actual file duration with ffprobe before inserting to DB
            cmd = [
                'ffprobe', '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                clip_path,
            ]
            try:
                probe_result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                probe = json.loads(probe_result.stdout)
                actual_secs = float(probe['format']['duration'])
            except Exception as exc:
                logging.error(
                    "ffprobe failed for chapter %s (%s): %s — blocking",
                    chapter_id, clip_path, exc,
                )
                blocked_chapters.append(chapter_id)
                continue

            if actual_secs > float(target_secs) + _FRAME_TOLERANCE_SECS:
                logging.error(
                    "SAFETY GATE BLOCKED chapter %s: actual duration %.1fs > max %.0fs (+%.0fs tolerance) — "
                    "pre-trim did not reduce the clip enough",
                    chapter_id, actual_secs, target_secs, _FRAME_TOLERANCE_SECS,
                )
                blocked_chapters.append(chapter_id)
                continue

            logging.info(
                "Safety gate OK — chapter %s: %.1fs <= %.0fs + %.0fs",
                chapter_id, actual_secs, target_secs, _FRAME_TOLERANCE_SECS,
            )

            scoring_reasoning = chapter.get('scoring_reasoning') or ''

            db.insert_video_short(
                chapter_id=chapter_id,
                reap_status='pending',
                staged_clip_path=clip_path,
                pretrim_start_secs=pretrim_start,
                pretrim_end_secs=pretrim_end,
                pretrim_used_srt=pretrim_used_srt,
                scoring_reasoning=scoring_reasoning,
            )
            inserted_count += 1

        ti.xcom_push(key='clips_queued', value=inserted_count)

        if blocked_chapters:
            raise AirflowException(
                f"_extract_and_pretrim_clip: {len(blocked_chapters)} clip(s) blocked "
                f"(chapters {blocked_chapters}) — pre-trim failed to meet the duration limit. "
                f"Inserted {inserted_count} clip(s) before blocking."
            )

    t2 = PythonOperator(
        task_id='extract_and_pretrim_clip',
        python_callable=_extract_and_pretrim_clip,
    )

    def _log_queue_summary(ti, **context):
        count = ti.xcom_pull(key='clips_queued') or 0
        logging.info("Queue summary: %d clip(s) inserted with reap_status='pending'", count)

    t3 = PythonOperator(
        task_id='log_queue_summary',
        python_callable=_log_queue_summary,
    )

    t0 >> t1 >> t2 >> t3
