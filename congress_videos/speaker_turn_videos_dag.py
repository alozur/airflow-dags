"""Speaker Turn Videos DAG.

On-demand DAG that materializes one output MP4 per speaker turn (or per
group of consecutive short turns) by executing operator-approved trim cuts.
Nothing is cut automatically — only turns with ``is_approved=TRUE``
proposals in ``speaker_turn_trim_proposals`` are excised; the rest receive a
full-window stream-copy (or re-encode when the source is AV1).

Idempotent: turns already present in ``speaker_turn_videos`` are skipped
before any ffmpeg invocation, so re-running the DAG on the same chapter
produces no duplicates.

**Airflow pool prerequisite (ops, outside DAG code)**::

    airflow pools set nas_ffmpeg 1 "NAS ffmpeg execution slot"

The ``nas_ffmpeg`` pool MUST be created by an admin before the DAG runs.
The DAG declares ``pool="nas_ffmpeg"`` on the ffmpeg-invoking task; without
the pool the task will queue indefinitely. The pool is NOT created in code.

Usage::

    airflow dags trigger speaker_turn_videos \\
        --conf '{"chapter_id": 42}'

    airflow dags trigger speaker_turn_videos \\
        --conf '{"video_id": "hy1cnx-0Oww"}'

Pipeline::

    select_turns  (speaker_turns + idempotency filter)
      → materialize_turns  (plan → execute → INSERT speaker_turn_videos)
          → collect_results  (summary XCom)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.config.paths import DOWNLOADS_DIR, get_turn_video_path
from congress_videos.modules.materialization import plan_turn_materialization
from congress_videos.modules.materialization_executor import execute_plan
from utils.codec_detection import get_cached_codec
from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)

DAG_ID = "speaker_turn_videos"

_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm")


def _find_source_video_any_date(video_id: str) -> str | None:
    """Locate the source media for a video without knowing its session date.

    ``speaker_turns`` carries no recording date, so scan every date folder
    under ``DOWNLOADS_DIR`` for ``downloads/{date}/{video_id}/`` and return the
    first real media file (mirrors ``reap_clip_preparer``'s date-less lookup).
    """
    if not os.path.isdir(DOWNLOADS_DIR):
        return None
    for date_folder in sorted(os.listdir(DOWNLOADS_DIR)):
        video_dir = os.path.join(DOWNLOADS_DIR, date_folder, str(video_id))
        if not os.path.isdir(video_dir):
            continue
        for filename in sorted(os.listdir(video_dir)):
            if filename.endswith(_MEDIA_SUFFIXES) and "chapter_video" not in filename:
                return os.path.join(video_dir, filename)
    return None


def _date_from_source_path(source_path: str) -> str:
    """Extract the ``{date}`` segment from ``{DOWNLOADS_DIR}/{date}/{video_id}/...``."""
    rel = os.path.relpath(source_path, DOWNLOADS_DIR)
    return rel.split(os.sep)[0]


# ---------------------------------------------------------------------------
# Airflow task callables
# ---------------------------------------------------------------------------


def _select_task(**context) -> list[dict]:
    """Fetch speaker turns in scope and filter out already-materialized ones.

    Reads turns by ``chapter_id`` or ``video_id`` from DAG conf, then
    cross-checks ``speaker_turn_videos`` and drops any turn_id already present.
    The filtered list is pushed to XCom under key ``"turns"``.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    chapter_id = conf.get("chapter_id")
    video_id = conf.get("video_id")

    pg = PostgresConnection()
    turns_table = pg.get_qualified_table("speaker_turns")
    chapters_table = pg.get_qualified_table("video_chapters")
    stv_table = pg.get_qualified_table("speaker_turn_videos")
    # video_id lives on video_chapters, not speaker_turns; join to resolve it.
    cols = "st.turn_id, st.chapter_id, vc.video_id, st.start_seconds, st.end_seconds"
    base = (
        f"SELECT {cols} FROM {turns_table} st "
        f"JOIN {chapters_table} vc ON vc.chapter_id = st.chapter_id"
    )

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            if chapter_id is not None:
                cur.execute(
                    f"{base} WHERE st.chapter_id = %s ORDER BY st.turn_id",
                    (chapter_id,),
                )
            elif video_id is not None:
                cur.execute(
                    f"{base} WHERE vc.video_id = %s ORDER BY st.turn_id",
                    (str(video_id),),
                )
            else:
                cur.execute(f"{base} ORDER BY st.turn_id LIMIT 10")
            names = [d[0] for d in cur.description]
            turns = [dict(zip(names, row)) for row in cur.fetchall()]

            # Idempotency: exclude turns already in speaker_turn_videos
            if turns:
                cur.execute(
                    f"SELECT turn_id FROM {stv_table} WHERE turn_id = ANY(%s)",
                    ([t["turn_id"] for t in turns],),
                )
                already_done = {row[0] for row in cur.fetchall()}
                turns = [t for t in turns if t["turn_id"] not in already_done]

    logger.info("speaker_turn_videos: selected %d turn(s) for materialization", len(turns))
    context["ti"].xcom_push(key="turns", value=turns)
    return turns


def _materialize_task(**context) -> dict:
    """Plan and execute materialization for each pending turn.

    For each turn in the XCom list:
    1. Locate the source video on disk (skip gracefully if absent).
    2. Fetch approved+voice-free trim proposals for the turn.
    3. Call ``plan_turn_materialization`` to derive keep intervals.
    4. For each plan: probe codec, run ``execute_plan``, INSERT into
       ``speaker_turn_videos``. A failed ``execute_plan`` is caught; the turn
       is logged as skipped and processing continues.

    Returns a summary dict ``{materialized: int, skipped: int}``.
    """
    turns = context["ti"].xcom_pull(key="turns", task_ids="select_turns") or []
    summary = {"materialized": 0, "skipped": 0}

    if not turns:
        logger.info("speaker_turn_videos: no turns to materialize")
        context["ti"].xcom_push(key="summary", value=summary)
        return summary

    pg = PostgresConnection()
    trims_table = pg.get_qualified_table("speaker_turn_trim_proposals")
    stv_table = pg.get_qualified_table("speaker_turn_videos")
    codec_cache: dict = {}

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Fetch all approved+voice-free trims for the selected turns
            turn_ids = [t["turn_id"] for t in turns]
            cur.execute(
                f"SELECT turn_id, start_seconds, end_seconds, is_approved, is_voice_free "
                f"FROM {trims_table} "
                f"WHERE turn_id = ANY(%s) AND is_approved = TRUE AND is_voice_free = TRUE",
                (turn_ids,),
            )
            trim_names = [d[0] for d in cur.description]
            approved_trims = [dict(zip(trim_names, row)) for row in cur.fetchall()]

        plans = plan_turn_materialization(turns, approved_trims)

        for plan in plans:
            # Use first turn in plan to locate source video
            first_turn = next(
                t for t in turns if t["turn_id"] == plan.turn_ids[0]
            )
            video_id = str(first_turn["video_id"])

            source_path = _find_source_video_any_date(video_id)
            if not source_path:
                logger.warning(
                    "speaker_turn_videos: no source video for video_id=%s — skipping plan turn_ids=%s",
                    video_id, plan.turn_ids,
                )
                summary["skipped"] += len(plan.turn_ids)
                continue

            # Recording date is not stored in the DB; derive it from the folder
            # the source video was found in, for the canonical output path.
            session_date = _date_from_source_path(source_path)

            # Derive output path using first (naming) turn_id
            output_path = get_turn_video_path(
                session_date, video_id, plan.output_turn_id
            )

            try:
                codec = get_cached_codec(source_path, codec_cache)
                execute_plan(plan, source_path, output_path, codec=codec, codec_cache=codec_cache)
            except Exception:  # noqa: BLE001 — one bad plan must not sink the run
                logger.exception(
                    "speaker_turn_videos: execute_plan failed for turn_ids=%s — skipping",
                    plan.turn_ids,
                )
                summary["skipped"] += len(plan.turn_ids)
                continue

            # INSERT idempotency rows (one per turn_id in plan)
            with conn.cursor() as cur:
                for tid in plan.turn_ids:
                    cur.execute(
                        f"INSERT INTO {stv_table} (turn_id, output_path) "
                        f"VALUES (%s, %s) "
                        f"ON CONFLICT (turn_id) DO NOTHING",
                        (tid, output_path),
                    )
            conn.commit()
            summary["materialized"] += len(plan.turn_ids)
            logger.info(
                "speaker_turn_videos: materialized turn_ids=%s -> %s",
                plan.turn_ids, output_path,
            )

    context["ti"].xcom_push(key="summary", value=summary)
    logger.info("speaker_turn_videos: run complete summary=%s", summary)
    return summary


def _collect_task(**context) -> dict:
    """Collect the materialization summary from XCom and return it.

    This task is a lightweight terminal node that surfaces the final
    ``{materialized, skipped}`` counts for operator visibility in the Airflow
    UI without requiring the caller to dig into _materialize_task's XCom.
    """
    summary = context["ti"].xcom_pull(key="summary", task_ids="materialize_turns") or {}
    logger.info("speaker_turn_videos.collect: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    description=(
        "On-demand DAG that materializes speaker-turn MP4 files from approved "
        "trim proposals (issue #88). Requires nas_ffmpeg Airflow pool (pool_slots=1)."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_tasks=1,
    default_args=default_args,
    tags=["congress_videos", "materialization", "on-demand"],
)

with dag:
    select_turns_task = PythonOperator(
        task_id="select_turns",
        python_callable=_select_task,
    )
    materialize_turns_task = PythonOperator(
        task_id="materialize_turns",
        python_callable=_materialize_task,
        pool="nas_ffmpeg",
        pool_slots=1,
    )
    collect_results_task = PythonOperator(
        task_id="collect_results",
        python_callable=_collect_task,
    )

    select_turns_task >> materialize_turns_task >> collect_results_task
