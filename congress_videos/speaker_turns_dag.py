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
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.operators.python import PythonOperator

from congress_videos.speaker_turn_videos_dag import DAG_ID as MATERIALIZE_DAG_ID

from congress_videos.modules.participants_db import lookup_participant_fuzzy
from congress_videos.modules.speaker_turns import detect_turns, _upsert_turns
from congress_videos.modules.speaker_turns_api import api_diarize_fn
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
            # PostgresConnection uses RealDictCursor: rows are dict-like.
            return [dict(row) for row in cur.fetchall()]


def run_chapter_turns(
    chapter: dict,
    cursor,
    *,
    diarize_fn=api_diarize_fn,
    name_resolver=lookup_participant_fuzzy,
    turns_table: str = "speaker_turns",
) -> dict:
    """Detect and persist speaker turns for a single chapter.

    Locates the source video (skips the chapter if absent — never fails the
    run), extracts the chapter-window WAV, loads the window's SRT blocks (or
    runs acoustic-only when the SRT is missing), detects turns, and upserts
    them via the caller-provided cursor. Returns a status dict.

    ``turns_table`` is forwarded to :func:`_upsert_turns`; the DAG passes the
    schema-qualified name so persistence targets the right schema in prod.
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
        _upsert_turns(cursor, chapter_id, turns, table=turns_table)
        return {"status": "ok", "chapter_id": chapter_id, "turns": len(turns)}
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)


def _select_task(**context) -> list[dict]:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
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
    turns_table = pg.get_qualified_table("speaker_turns")
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            for chapter in chapters:
                try:
                    result = run_chapter_turns(chapter, cur, turns_table=turns_table)
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


def _trigger_materialize(**_context) -> None:
    """Fire-and-forget trigger to speaker_turn_videos after detect completes.

    Uses trigger_rule='all_done' on the calling task so it fires even when
    process_chapters partially fails. Any exception is caught and logged —
    the detect DAG run is never failed by the trigger.
    """
    try:
        trigger_dag_api(
            dag_id=MATERIALIZE_DAG_ID,
            conf={},
            run_id=f"train_materialize_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}",
        )
        logger.info("_trigger_materialize: triggered %s", MATERIALIZE_DAG_ID)
    except Exception as exc:  # noqa: BLE001 fire-and-forget
        logger.warning("could not trigger %s: %s", MATERIALIZE_DAG_ID, exc)


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id=DAG_ID,
    description=(
        "Twice-nightly speaker-turn detection within video chapters (issue #86/#159). "
        "On completion chains to speaker_turn_videos via trigger_materialize."
    ),
    schedule="0 1,5 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["congress_videos", "speaker-turns"],
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
    trigger_materialize_task = PythonOperator(
        task_id="trigger_materialize",
        python_callable=_trigger_materialize,
        trigger_rule="all_done",
    )
    select_chapters_task >> process_chapters_task >> trigger_materialize_task
