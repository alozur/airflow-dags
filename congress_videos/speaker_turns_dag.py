"""Speaker Turns DAG.

On-demand, config-driven DAG that detects speaker-turn boundaries **within**
existing thematic ``video_chapters`` and persists named sub-turns to the
``speaker_turns`` table. It does NOT cut, re-encode, or move any video —
materialization (1 speaker = 1 video) is issue #88.

Runs once daily (``0 14 * * *`` UTC) as the first stage of the turn train
(detect → materialize → prepare, issue #159); on completion it fire-and-forget
triggers ``speaker_turn_videos``. 14:00 UTC opens the NAS quiet window
(14:00-20:00 UTC), where qBittorrent reads ~100x less disk than during the
00:00-08:00 UTC band the previous ``0 1,5 * * *`` cron sat in (issue #187).
Never chained into ``youtube_upload_dag`` — diarization runs ~4.6x realtime
and must not block the upload flow. Manual runs via the trigger API keep
working.

Usage::

    airflow dags trigger speaker_turns --conf '{"limit": 5}'
    airflow dags trigger speaker_turns --conf '{"chapter_ids": [7, 8]}'

Pipeline::

    select_chapters (uploadable_chapters view, LIMIT from conf)
      → process_chapters (per chapter, graceful skip)
          run_chapter_turns — NO database connection held here:
            _find_source_video → extract_audio_wav (chapter-window WAV slice)
              → find_srt_for_chapter / _parse_srt_blocks (window-filtered)
                → detect_turns(diarize_fn=docker, name_resolver=fuzzy)
          _persist_chapter_turns — short-lived connection scoped to
            persistence only: _upsert_turns (idempotent) → mark
            turns_detected_at → commit
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import UTC, datetime, timedelta, timezone

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.operators.python import PythonOperator

# Import the id from config.constants, NOT from the sibling DAG module: importing
# a DAG module executes it and Airflow auto-registers its DAG to THIS file,
# raising AirflowDagDuplicatedIdException at parse time.
from congress_videos.config.constants import (
    SPEAKER_TURN_VIDEOS_DAG_ID as MATERIALIZE_DAG_ID,
)
from congress_videos.modules.participants_db import lookup_participant_fuzzy
from congress_videos.modules.sidecar_api_error import SidecarApiError
from congress_videos.modules.speaker_turns import (
    ANNOUNCEMENT_WINDOW_SECONDS,
    _upsert_turns,
    detect_turns,
)
from congress_videos.modules.speaker_turns_api import api_diarize_fn, check_diarize_api_health
from congress_videos.modules.vad_helpers import _find_source_video, extract_audio_wav
from congress_videos.srt_helpers import _parse_srt_blocks, find_srt_for_chapter
from utils.llm_cache import cached_json_completion
from utils.postgres_helpers import PostgresConnection
from utils.time_utils import parse_timestamp

logger = logging.getLogger(__name__)

DAG_ID = "speaker_turns"
DEFAULT_LIMIT = 1
# Chapters newer than this outrank every older chapter regardless of relevance
# (issue #300, BR1). Wall-clock relative, evaluated by Postgres NOW() per query.
RECENT_CHAPTER_WINDOW_DAYS = 7


def select_chapters(limit: int = DEFAULT_LIMIT, chapter_ids: list[int] | None = None) -> list[dict]:
    """Read publishable chapters from the ``uploadable_chapters`` view.

    When ``chapter_ids`` is given, only those chapters are returned without any
    progress filter — this is the manual re-detection escape hatch (issue #166).
    Otherwise the top ``limit`` uploadable chapters that have not yet been
    attempted (turns_detected_at IS NULL and no existing speaker_turns rows) are
    returned, ordered by a two-bucket recency+relevance rule (issue #300):
    chapters newer than ``RECENT_CHAPTER_WINDOW_DAYS`` outrank every older
    chapter regardless of ``relevance_score``; within the recent bucket order
    is ``relevance_score DESC``; within the old bucket order is
    ``session_date DESC NULLS LAST`` then ``relevance_score DESC``. This keeps
    fresh, high-relevance chapters from queuing behind an older backlog while
    still preventing barren chapters from looping indefinitely.

    Each row carries the fields the per-chapter pipeline needs: chapter_id,
    video_id, session_date, start_time, end_time.
    """
    pg = PostgresConnection()
    view = pg.get_qualified_table("uploadable_chapters")
    vc_table = pg.get_qualified_table("video_chapters")
    turns_table = pg.get_qualified_table("speaker_turns")
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
                # Two-bucket order (issue #300). FALSE < TRUE in Postgres, so
                # `(created_at < cutoff) ASC` puts the recent bucket first. The
                # CASE key only discriminates inside the old bucket (it is NULL
                # for recent rows, which key 1 has already segregated).
                # RECENT_CHAPTER_WINDOW_DAYS is a module int constant, int()-coerced
                # here: not user input, not an injection seam. `limit` stays bound.
                recent_cutoff = (
                    f"NOW() - INTERVAL '{int(RECENT_CHAPTER_WINDOW_DAYS)} days'"
                )
                cur.execute(
                    f"SELECT {cols} FROM {view} vc"
                    f" WHERE EXISTS ("
                    f"SELECT 1 FROM {vc_table} v"
                    f" WHERE v.chapter_id = vc.chapter_id"
                    f" AND v.turns_detected_at IS NULL"
                    f")"
                    f" AND NOT EXISTS ("
                    f"SELECT 1 FROM {turns_table} st"
                    f" WHERE st.chapter_id = vc.chapter_id"
                    f")"
                    f" ORDER BY"
                    f" (vc.created_at < {recent_cutoff}) ASC,"
                    f" CASE WHEN vc.created_at < {recent_cutoff}"
                    f" THEN vc.session_date END DESC NULLS LAST,"
                    f" vc.relevance_score DESC,"
                    f" vc.chapter_id ASC"
                    f" LIMIT %s",
                    (limit,),
                )
            # PostgresConnection uses RealDictCursor: rows are dict-like.
            return [dict(row) for row in cur.fetchall()]


def run_chapter_turns(
    chapter: dict,
    *,
    diarize_fn=api_diarize_fn,
    name_resolver=lookup_participant_fuzzy,
    completion_fn=cached_json_completion,
    turns_table: str = "speaker_turns",
) -> dict:
    """Detect speaker turns for a single chapter — holds NO database connection.

    Locates the source video (skips the chapter if absent — never fails the
    run), extracts the chapter-window WAV, loads the window's SRT blocks (or
    runs acoustic-only when the SRT is missing), and detects turns. Returns a
    status dict whose ``turns`` field is the detected ``Turn`` list (empty when
    skipped). Persistence is the caller's responsibility via
    :func:`_persist_chapter_turns` — no connection is opened here, so the
    ffmpeg extraction, SRT parse, and diarize-api call never hold one open
    (issue #200).

    ``turns_table`` is accepted for interface symmetry with
    :func:`_persist_chapter_turns` so callers can thread one shared kwargs
    dict through both; it is not used in this function.
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
        return {"status": "skipped_no_video", "chapter_id": chapter_id, "turns": []}

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
            # Lower bound widened by ANNOUNCEMENT_WINDOW_SECONDS (issue #131,
            # design D2) so a chapter's first turn can see its pre-chapter
            # announcement. Upper bound stays at end_secs — no leakage into
            # the next chapter.
            srt_blocks = [
                b for b in _parse_srt_blocks(srt_path)
                if b["start_secs"] >= start_secs - ANNOUNCEMENT_WINDOW_SECONDS
                and b["end_secs"] <= end_secs
            ]
        else:
            logger.warning(
                "chapter %s: no SRT — acoustic-only detection", chapter_id
            )
            srt_blocks = []

        chapter["_wav_path"] = wav_path
        chapter["_chapter_offset_seconds"] = start_secs

        turns = detect_turns(
            chapter, srt_blocks, diarize_fn, name_resolver, completion_fn=completion_fn
        )
        return {"status": "ok", "chapter_id": chapter_id, "turns": turns}
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)


def _persist_chapter_turns(
    pg, chapter_id: int, turns: list, *, turns_table: str, vc_table: str
) -> None:
    """Persist detected turns for one chapter in a single short-lived transaction.

    Opens a connection only for the upsert of ``turns`` plus the
    ``turns_detected_at`` UPDATE, then commits and closes — no connection is
    held during detection (issue #200). ``_upsert_turns`` never commits on its
    own (see its docstring); this helper owns the transaction boundary.
    """
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            _upsert_turns(cur, chapter_id, turns, table=turns_table)
            cur.execute(
                f"UPDATE {vc_table} SET turns_detected_at = NOW() "
                f"WHERE chapter_id = %s AND turns_detected_at IS NULL",
                (chapter_id,),
            )
            conn.commit()  # durable per chapter — a later failure must not undo this


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
    check_diarize_api_health()  # fail fast on outage before DB/WAV work
    summary = {"processed": 0, "skipped": 0, "turns": 0}
    pg = PostgresConnection()
    turns_table = pg.get_qualified_table("speaker_turns")
    vc_table = pg.get_qualified_table("video_chapters")
    for chapter in chapters:
        try:
            result = run_chapter_turns(chapter, turns_table=turns_table)
        except SidecarApiError:  # noqa: BLE001 mid-run drop → fail loud
            logger.exception(
                "chapter %s: diarize-api outage — failing task",
                chapter.get("chapter_id"),
            )
            raise
        except Exception:  # noqa: BLE001 data error → skip
            logger.exception(
                "chapter %s failed — skipping", chapter.get("chapter_id")
            )
            summary["skipped"] += 1
            continue

        if result["status"] != "ok":
            summary["skipped"] += 1
            continue

        try:
            _persist_chapter_turns(
                pg, result["chapter_id"], result["turns"],
                turns_table=turns_table, vc_table=vc_table,
            )
        except Exception:  # noqa: BLE001 persistence error → skip, chapter unmarked
            logger.exception(
                "chapter %s: persistence failed — skipping", result["chapter_id"]
            )
            summary["skipped"] += 1
            continue

        summary["processed"] += 1
        summary["turns"] += len(result["turns"])
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
        "Daily speaker-turn detection within video chapters (issue #86/#159). "
        "On completion chains to speaker_turn_videos via trigger_materialize."
    ),
    schedule="0 14 * * *",  # Single daily run in the NAS quiet window (issue #187)
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
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
