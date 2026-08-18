"""Speaker Turns DAG.

On-demand, config-driven DAG that detects speaker-turn boundaries **within**
existing thematic ``video_chapters`` and persists named sub-turns to the
``speaker_turns`` table. It does NOT cut, re-encode, or move any video —
materialization (1 speaker = 1 video) is issue #88.

Triggered exclusively via the Airflow trigger API (``schedule=None``). Never
chained into ``youtube_upload_dag`` — diarization runs ~4.6x realtime and must
not block the upload flow.

Usage::

    airflow dags trigger speaker_turns --conf '{"limit": 5}'
    airflow dags trigger speaker_turns --conf '{"chapter_ids": [7, 8]}'

Pipeline::

    select_chapters (uploadable_chapters view, LIMIT from conf)
      → process_chapters (per chapter, graceful skip)
          _find_source_video → extract_audio_wav (chapter-window WAV slice)
            → find_srt_for_chapter / _parse_srt_blocks (window-filtered)
              → detect_turns(diarize_fn=docker, name_resolver=fuzzy)
                → _upsert_turns (idempotent)
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.modules.participants_db import lookup_participant_fuzzy
from congress_videos.modules.speaker_turns import detect_turns, _upsert_turns
from congress_videos.modules.speaker_turns_docker import docker_diarize_fn
from congress_videos.modules.vad_helpers import _find_source_video, extract_audio_wav
from congress_videos.srt_helpers import _parse_srt_blocks, find_srt_for_chapter
from utils.postgres_helpers import PostgresConnection
from utils.time_utils import parse_timestamp

logger = logging.getLogger(__name__)

DAG_ID = "speaker_turns"
DEFAULT_LIMIT = 5


def select_chapters(limit: int = DEFAULT_LIMIT, chapter_ids: list[int] | None = None) -> list[dict]:
    """Read publishable chapters from the ``uploadable_chapters`` view.

    When ``chapter_ids`` is given, only those chapters are returned; otherwise
    the first ``limit`` uploadable chapters are returned. Each row carries the
    fields the per-chapter pipeline needs: chapter_id, video_id, session_date,
    start_time, end_time.
    """
    pg = PostgresConnection()
    view = pg.get_qualified_table("uploadable_chapters")
    cols = "chapter_id, video_id, session_date, start_time, end_time"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            if chapter_ids:
                cur.execute(
                    f"SELECT {cols} FROM {view} WHERE chapter_id = ANY(%s) "
                    f"ORDER BY chapter_id",
                    (list(chapter_ids),),
                )
            else:
                cur.execute(
                    f"SELECT {cols} FROM {view} ORDER BY chapter_id LIMIT %s",
                    (limit,),
                )
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]


def run_chapter_turns(
    chapter: dict,
    cursor,
    *,
    diarize_fn=docker_diarize_fn,
    name_resolver=lookup_participant_fuzzy,
) -> dict:
    """Detect and persist speaker turns for a single chapter.

    Locates the source video (skips the chapter if absent — never fails the
    run), extracts the chapter-window WAV, loads the window's SRT blocks (or
    runs acoustic-only when the SRT is missing), detects turns, and upserts
    them via the caller-provided cursor. Returns a status dict.
    """
    chapter_id = chapter["chapter_id"]
    video_id = chapter["video_id"]
    session_date = str(chapter.get("session_date"))

    video_path = _find_source_video(session_date, video_id)
    if not video_path:
        logger.warning(
            "chapter %s: no source video for %s/%s — skipping",
            chapter_id, session_date, video_id,
        )
        return {"status": "skipped_no_video", "chapter_id": chapter_id, "turns": 0}

    start_secs = parse_timestamp(chapter["start_time"])
    end_secs = parse_timestamp(chapter["end_time"])
    duration = max(0.0, end_secs - start_secs)

    wav_path = os.path.join(
        tempfile.gettempdir(), f"speaker_turns_{video_id}_{chapter_id}.wav"
    )
    try:
        extract_audio_wav(
            video_path, wav_path, start_secs=start_secs, duration_secs=duration
        )

        srt_path = find_srt_for_chapter(video_id, chapter_id, session_date)
        if srt_path:
            srt_blocks = [
                b for b in _parse_srt_blocks(srt_path)
                if b["start_secs"] >= start_secs and b["end_secs"] <= end_secs
            ]
        else:
            logger.warning(
                "chapter %s: no SRT — acoustic-only detection", chapter_id
            )
            srt_blocks = []

        chapter["_wav_path"] = wav_path
        chapter["_chapter_offset_seconds"] = start_secs

        turns = detect_turns(chapter, srt_blocks, diarize_fn, name_resolver)
        _upsert_turns(cursor, chapter_id, turns)
        return {"status": "ok", "chapter_id": chapter_id, "turns": len(turns)}
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)


def _select_task(**context) -> list[dict]:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    chapters = select_chapters(
        limit=conf.get("limit", DEFAULT_LIMIT),
        chapter_ids=conf.get("chapter_ids"),
    )
    logger.info("Selected %d chapter(s) for speaker-turn detection", len(chapters))
    context["ti"].xcom_push(key="chapters", value=chapters)
    return chapters


def _process_task(**context) -> dict:
    chapters = context["ti"].xcom_pull(key="chapters", task_ids="select_chapters") or []
    summary = {"processed": 0, "skipped": 0, "turns": 0}
    pg = PostgresConnection()
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            for chapter in chapters:
                try:
                    result = run_chapter_turns(chapter, cur)
                except Exception:  # noqa: BLE001 — one bad chapter must not sink the run
                    logger.exception(
                        "chapter %s failed — skipping", chapter.get("chapter_id")
                    )
                    summary["skipped"] += 1
                    continue
                if result["status"] == "ok":
                    summary["processed"] += 1
                    summary["turns"] += result["turns"]
                else:
                    summary["skipped"] += 1
        conn.commit()
    logger.info("Speaker-turn run summary: %s", summary)
    return summary


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id=DAG_ID,
    description="On-demand speaker-turn detection within video chapters (issue #86)",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["congress_videos", "speaker-turns", "on-demand"],
)

with dag:
    select_chapters_task = PythonOperator(
        task_id="select_chapters",
        python_callable=_select_task,
    )
    process_chapters_task = PythonOperator(
        task_id="process_chapters",
        python_callable=_process_task,
    )
    select_chapters_task >> process_chapters_task
