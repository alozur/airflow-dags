"""
Congress Reap Clip Preparer DAG

Selects speaker-turn videos materialized by the diarization pipeline (issue #467)
that are eligible for Reap short generation, stages each candidate's
``output_path`` directly, pre-trims clips whose ffprobe duration exceeds the
configured threshold using a leading file-relative window, and inserts a
video_shorts row with reap_status='pending' for each accepted clip, carrying
both ``chapter_id`` and ``turn_id``. DAG 2 (reap_processor) runs independently
on its own schedule and consumes the pending queue.
"""

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.video_splitter import (
    build_ffmpeg_cut_cmd,
    compute_ffmpeg_timeout,
)
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.codec_detection import (
    cut_mode_for_reencode,
    get_cached_codec,
    reencode_for_codec,
)
from utils.env_loader import load_env_if_local

load_env_if_local()

POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "development")
_FRAME_TOLERANCE_SECS = 200.0


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _ffmpeg_extract_window(
    source_path: str, dest_path: str, start_secs: float, end_secs: float, reencode: bool = True
) -> None:
    """Re-extract a precise time window from source into dest_path using ffmpeg.

    ``reencode`` defaults to ``True`` to preserve backward compatibility for any
    caller that omits it. The pre-trim call site passes the materialized turn
    video's own cached codec decision (see ``_stage_and_pretrim_clip``).
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    duration = end_secs - start_secs
    cmd = build_ffmpeg_cut_cmd(
        src=source_path,
        out=dest_path,
        start=start_secs,
        duration=duration,
        reencode=reencode,
    )
    timeout = compute_ffmpeg_timeout(duration)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg window extract failed: {result.stderr}")


def _probe_duration_secs(path: str) -> float:
    """Return a file's duration in seconds via ffprobe. Raises on failure."""
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    probe = json.loads(result.stdout)
    return float(probe["format"]["duration"])


with DAG(
    "congress_reap_clip_preparer",
    default_args=default_args,
    description="Select all eligible turn videos, pre-trim clips, insert pending rows in video_shorts queue",
    schedule="0 15 * * *",
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["congress", "reap", "shorts"],
    params={
        "max_turns": 0,
        "pre_trim_threshold_secs": 900,
        "pre_trim_target_secs": 900,
    },
) as dag:
    t0 = PythonOperator(
        task_id="ensure_data_directory",
        python_callable=lambda ti: xcom_task(
            ti, lambda: ensure_project_data_directory("congress_videos"), "data_directory_path"
        ),
    )

    def _query_turns(ti, **context):
        db = CongressionalVideoDB()
        turns = db.get_turn_videos_for_shorts(max_turns=context["params"]["max_turns"] or None)
        if not turns:
            logging.warning(
                "No eligible turn videos for Reap: 0 groups passed "
                "(output_path present, non-procedural, span >= 120s, not already queued) "
                "— skipping this run"
            )
        return bool(turns)  # False → ShortCircuitOperator skips all downstream tasks

    t1 = ShortCircuitOperator(
        task_id="query_chapters",
        python_callable=_query_turns,
    )

    def _stage_and_pretrim_clip(ti, **context):
        db = CongressionalVideoDB()
        turns = db.get_turn_videos_for_shorts(max_turns=context["params"]["max_turns"] or None)
        threshold_secs = context["params"]["pre_trim_threshold_secs"]
        target_secs = context["params"]["pre_trim_target_secs"]
        inserted_count = 0
        blocked_turns = []
        codec_cache: dict = {}

        for turn in turns:
            turn_id = turn["turn_id"]
            chapter_id = turn["chapter_id"]
            video_id = turn["video_id"]
            output_path = turn["output_path"]

            try:
                actual_secs = _probe_duration_secs(output_path)
            except Exception as exc:
                logging.error("ffprobe failed for turn %s (%s): %s — blocking", turn_id, output_path, exc)
                blocked_turns.append(turn_id)
                continue

            if actual_secs < 120:
                logging.warning("Turn %s clip duration %.1fs < 120s minimum — skipping", turn_id, actual_secs)
                continue

            staged_path = output_path
            pretrim_start = None
            pretrim_end = None
            final_secs = actual_secs

            if actual_secs > threshold_secs:
                source_codec = get_cached_codec(output_path, codec_cache)
                reencode = reencode_for_codec(source_codec)
                turn_folder = os.path.join(PROJECT_DATA_DIR, str(video_id), str(chapter_id))
                os.makedirs(turn_folder, exist_ok=True)
                staged_path = os.path.join(turn_folder, f"turn_{turn_id}_reap.mp4")

                try:
                    _ffmpeg_extract_window(
                        source_path=output_path,
                        dest_path=staged_path,
                        start_secs=0.0,
                        end_secs=float(target_secs),
                        reencode=reencode,
                    )
                except RuntimeError as exc:
                    logging.error("Pre-trim ffmpeg failed for turn %s: %s — skipping turn", turn_id, exc)
                    continue

                pretrim_start, pretrim_end = 0.0, float(target_secs)
                logging.info(
                    "Turn %s pre-trimmed: 0.0–%.1fs (%.0fs) source_codec=%s cut_mode=%s",
                    turn_id,
                    target_secs,
                    target_secs,
                    source_codec,
                    cut_mode_for_reencode(reencode),
                )

                try:
                    final_secs = _probe_duration_secs(staged_path)
                except Exception as exc:
                    logging.error(
                        "ffprobe failed for staged turn %s (%s): %s — blocking",
                        turn_id,
                        staged_path,
                        exc,
                    )
                    blocked_turns.append(turn_id)
                    continue

            if final_secs > float(target_secs) + _FRAME_TOLERANCE_SECS:
                logging.error(
                    "SAFETY GATE BLOCKED turn %s: actual duration %.1fs > max %.0fs (+%.0fs tolerance) — "
                    "pre-trim did not reduce the clip enough",
                    turn_id,
                    final_secs,
                    target_secs,
                    _FRAME_TOLERANCE_SECS,
                )
                blocked_turns.append(turn_id)
                continue

            scoring_reasoning = turn.get("scoring_reasoning") or ""

            db.insert_video_short(
                chapter_id=chapter_id,
                turn_id=turn_id,
                reap_status="pending",
                staged_clip_path=staged_path,
                pretrim_start_secs=pretrim_start,
                pretrim_end_secs=pretrim_end,
                pretrim_used_srt=False,
                scoring_reasoning=scoring_reasoning,
            )
            inserted_count += 1

        ti.xcom_push(key="clips_queued", value=inserted_count)

        if blocked_turns:
            raise AirflowException(
                f"_stage_and_pretrim_clip: {len(blocked_turns)} clip(s) blocked "
                f"(turns {blocked_turns}) — pre-trim failed to meet the duration limit. "
                f"Inserted {inserted_count} clip(s) before blocking."
            )

    t2 = PythonOperator(
        task_id="extract_and_pretrim_clip",
        python_callable=_stage_and_pretrim_clip,
    )

    def _log_queue_summary(ti, **context):
        count = ti.xcom_pull(key="clips_queued") or 0
        logging.info("Queue summary: %d clip(s) inserted with reap_status='pending'", count)

    t3 = PythonOperator(
        task_id="log_queue_summary",
        python_callable=_log_queue_summary,
    )

    t0 >> t1 >> t2 >> t3
