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

    airflow dags trigger speaker_turn_videos \\
        --conf '{"limit": 50}'

Selection precedence in ``select_turns`` (issue #231): ``chapter_id``, then
``video_id``, then an explicitly present ``limit`` key (global backlog
drain), then — when conf is empty — automatic chapter-aligned selection:
the whole oldest pending chapter is materialized in one run, uncapped.

Pipeline::

    select_turns  (speaker_turns + idempotency filter)
      → materialize_turns  (plan → execute → INSERT speaker_turn_videos)
          → collect_results  (summary XCom)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.operators.python import PythonOperator

# Import the id from config.constants, NOT from the sibling DAG module: importing
# a DAG module executes it and Airflow auto-registers its DAG to THIS file,
# raising AirflowDagDuplicatedIdException at parse time.
from congress_videos.config.constants import (
    SPEAKER_TURN_PREPARE_DAG_ID as PREPARE_DAG_ID,
    SPEAKER_TURN_VIDEOS_DAG_ID,
)
from congress_videos.config.paths import DOWNLOADS_DIR, get_orador_video_dir
from congress_videos.modules.materialization import (
    MONOLOGUE,
    classify_turn_type,
    plan_turn_materialization,
)
from congress_videos.modules.materialization_executor import execute_plan
from congress_videos.srt_helpers import _window_srt_text, score_turn_interest
from utils.codec_detection import get_cached_codec
from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)

DAG_ID = SPEAKER_TURN_VIDEOS_DAG_ID

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


# ---------------------------------------------------------------------------
# Airflow task callables
# ---------------------------------------------------------------------------


def _select_task(**context) -> list[dict]:
    """Fetch speaker turns in scope and filter out already-materialized ones.

    Selection precedence (issue #231): ``chapter_id``, then ``video_id``,
    then an explicitly present ``limit`` key, then — when conf carries none
    of those keys — automatic chapter-aligned selection.

    Scoped branches (``chapter_id``/``video_id``) select the full chapter or
    video and filter already-materialized turns with a post-hoc
    ``speaker_turn_videos`` lookup, so an operator re-running a scoped conf
    still sees the whole scope. A supplied ``limit`` never caps a scoped
    result.

    The explicit-limit branch (``"limit" in conf``, distinguishing an
    explicit ``null`` from an absent key) excludes already-materialized
    turns directly in SQL via ``NOT EXISTS`` and drains the backlog globally,
    across chapters, in ascending ``turn_id`` order (issue #216).

    The automatic branch (conf carries none of the three keys) instead
    selects one complete pending chapter per run: first it chooses the
    chapter containing the minimum pending ``turn_id``, then it selects
    every still-pending turn in that chapter, uncapped. Two statements are
    used deliberately so the second, under PostgreSQL READ COMMITTED,
    observes a fresh snapshot — a turn materialized between the two
    statements is excluded from the final batch.

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
    # st.is_procedural (issue #143) is selected so plan_turn_materialization
    # can SEE and excise procedural member turns — it is NEVER filtered here;
    # the planner, not this query, decides what to do with them.
    cols = (
        "st.turn_id, st.chapter_id, vc.video_id, st.start_seconds, st.end_seconds, "
        "st.resolved_name, st.speaker_label, st.is_procedural"
    )
    base = (
        f"SELECT {cols} FROM {turns_table} st "
        f"JOIN {chapters_table} vc ON vc.chapter_id = st.chapter_id"
    )
    pending_predicate = (
        f"NOT EXISTS (SELECT 1 FROM {stv_table} v WHERE v.turn_id = st.turn_id)"
    )

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            scoped = chapter_id is not None or video_id is not None
            if chapter_id is not None:
                cur.execute(
                    f"{base} WHERE st.chapter_id = %s ORDER BY st.turn_id",
                    (chapter_id,),
                )
                turns = [dict(row) for row in cur.fetchall()]
            elif video_id is not None:
                cur.execute(
                    f"{base} WHERE vc.video_id = %s ORDER BY st.turn_id",
                    (str(video_id),),
                )
                turns = [dict(row) for row in cur.fetchall()]
            elif "limit" in conf:
                cur.execute(
                    f"{base} WHERE {pending_predicate} ORDER BY st.turn_id LIMIT %s",
                    (conf.get("limit"),),
                )
                turns = [dict(row) for row in cur.fetchall()]
            else:
                turns = _select_automatic_chapter(
                    cur, turns_table, stv_table, base, pending_predicate,
                )

            # Scoped runs select the full chapter/video on purpose, so they
            # still need the post-hoc filter. The explicit-limit and
            # automatic branches already filtered in SQL.
            if scoped and turns:
                cur.execute(
                    f"SELECT turn_id FROM {stv_table} WHERE turn_id = ANY(%s)",
                    ([t["turn_id"] for t in turns],),
                )
                already_done = {row["turn_id"] for row in cur.fetchall()}
                turns = [t for t in turns if t["turn_id"] not in already_done]

    logger.info("speaker_turn_videos: selected %d turn(s) for materialization", len(turns))
    context["ti"].xcom_push(key="turns", value=turns)
    return turns


def _select_automatic_chapter(
    cur, turns_table: str, stv_table: str, base: str, pending_predicate: str,
) -> list[dict]:
    """Choose one complete pending chapter, then select its pending turns.

    Two statements: the first picks the chapter containing the minimum
    pending ``turn_id`` (materialized turns are excluded from that MIN via
    ``NOT EXISTS``, so they cannot choose or enter the batch); the second
    re-applies an independent pending-row anti-join for that chapter so a
    turn materialized between the two statements is excluded (issue #231).
    """
    cur.execute(
        f"SELECT st.chapter_id FROM {turns_table} st "
        f"WHERE st.turn_id = ("
        f"SELECT MIN(p.turn_id) FROM {turns_table} p "
        f"WHERE NOT EXISTS (SELECT 1 FROM {stv_table} v WHERE v.turn_id = p.turn_id)"
        f")"
    )
    chosen = cur.fetchone()
    if not chosen:
        return []

    chosen_chapter_id = chosen["chapter_id"]
    cur.execute(
        f"{base} WHERE st.chapter_id = %s AND {pending_predicate} ORDER BY st.turn_id",
        (chosen_chapter_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def _materialize_task(**context) -> dict:
    """Plan and execute materialization for each pending turn.

    For each turn in the XCom list:
    1. Locate the source video on disk (skip gracefully if absent).
    2. Fetch approved+voice-free trim proposals for the turn.
    3. Call ``plan_turn_materialization`` to derive keep intervals.
    4. For each plan: probe codec, run ``execute_plan``, INSERT into
       ``speaker_turn_videos``. A failed ``execute_plan`` is caught; the turn
       is logged as skipped and processing continues.

    Degenerate all-procedural groups (issue #143 D5): a group whose every
    member turn is procedural yields NO plan from ``plan_turn_materialization``
    (the cuts cover the whole span). Those turn_ids are still recorded, one
    ``speaker_turn_videos`` row each, with ``keep_intervals='[]'`` and no file
    on disk — otherwise a permanently pending turn would block
    ``_select_automatic_chapter``'s ``MIN(turn_id)`` forever.

    Returns a summary dict ``{materialized: int, skipped: int, dropped_procedural: int}``.
    """
    turns = context["ti"].xcom_pull(key="turns", task_ids="select_turns") or []
    summary = {"materialized": 0, "skipped": 0, "dropped_procedural": 0}

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
            # RealDictCursor rows are dict-like.
            approved_trims = [dict(row) for row in cur.fetchall()]

        resolved_by_id: dict[int, str | None] = {
            t["turn_id"]: t.get("resolved_name") for t in turns
        }
        turn_rows_by_id: dict[int, dict] = {t["turn_id"]: t for t in turns}
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

            # Canonical date-free output path keyed on stable DB identifiers.
            # chapter_id is typed int (non-optional dataclass field) and is
            # always populated by plan_turn_materialization from the DB column;
            # no None guard is needed.
            output_path = str(
                get_orador_video_dir(video_id, plan.chapter_id, plan.output_turn_id)
                / "video.mp4"
            )

            turn_type = classify_turn_type(plan.turn_ids, resolved_by_id, turn_rows_by_id)

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

            # INSERT idempotency rows (one per turn_id in plan). keep_intervals
            # (issue #143) records the EXECUTED cut boundaries — the same
            # values just passed to execute_plan — as absolute source seconds,
            # so SRT retiming at prepare time can never diverge from the video
            # that was actually cut.
            keep_intervals_json = json.dumps(
                [[ki.start, ki.end] for ki in plan.keep_intervals]
            )
            with conn.cursor() as cur:
                for tid in plan.turn_ids:
                    cur.execute(
                        f"INSERT INTO {stv_table} "
                        f"(turn_id, output_path, turn_type, keep_intervals) "
                        f"VALUES (%s, %s, %s, %s) "
                        f"ON CONFLICT (turn_id) DO NOTHING",
                        (tid, output_path, turn_type, keep_intervals_json),
                    )
            conn.commit()
            summary["materialized"] += len(plan.turn_ids)
            logger.info(
                "speaker_turn_videos: materialized turn_ids=%s -> %s",
                plan.turn_ids, output_path,
            )

            # Score each turn's SRT window for upload prioritisation.
            # Failures are non-fatal: log at WARNING, leave interest_score NULL.
            turns_table = pg.get_qualified_table("speaker_turns")
            for tid in plan.turn_ids:
                try:
                    window_text = _window_srt_text(
                        video_id,
                        float(plan.keep_intervals[0].start),
                        float(plan.keep_intervals[-1].end),
                    )
                    score = score_turn_interest(window_text)
                    if score is not None:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"UPDATE {turns_table} "
                                f"SET interest_score = %s WHERE turn_id = %s",
                                (score, tid),
                            )
                        conn.commit()
                        logger.info(
                            "speaker_turn_videos: interest_score=%d for turn_id=%d",
                            score, tid,
                        )
                except Exception:  # noqa: BLE001 — scoring must never crash materialization
                    logger.warning(
                        "speaker_turn_videos: interest scoring failed for turn_id=%d — "
                        "leaving interest_score NULL",
                        tid,
                        exc_info=True,
                    )

        # Degenerate all-procedural groups (issue #143 D5): turns present in
        # the input but absent from EVERY plan's turn_ids never got a plan at
        # all — _flush_group emits none when the cuts cover the whole group
        # span. Still record them (no execute_plan, no file) so select_turns'
        # NOT EXISTS idempotency treats them as handled and they never
        # permanently block _select_automatic_chapter's MIN(turn_id).
        planned_turn_ids = {tid for plan in plans for tid in plan.turn_ids}
        dropped_turns = [t for t in turns if t["turn_id"] not in planned_turn_ids]
        if dropped_turns:
            with conn.cursor() as cur:
                for turn in dropped_turns:
                    tid = turn["turn_id"]
                    dropped_output_path = str(
                        get_orador_video_dir(str(turn["video_id"]), turn["chapter_id"], tid)
                        / "video.mp4"
                    )
                    cur.execute(
                        f"INSERT INTO {stv_table} "
                        f"(turn_id, output_path, turn_type, keep_intervals) "
                        f"VALUES (%s, %s, %s, %s) "
                        f"ON CONFLICT (turn_id) DO NOTHING",
                        (tid, dropped_output_path, MONOLOGUE, json.dumps([])),
                    )
            conn.commit()
            summary["dropped_procedural"] += len(dropped_turns)
            logger.info(
                "speaker_turn_videos: dropped %d turn(s) from all-procedural groups: turn_ids=%s",
                len(dropped_turns), [t["turn_id"] for t in dropped_turns],
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


def _trigger_prepare(**_context) -> None:
    """Fire-and-forget trigger to speaker_turn_prepare after materialization completes.

    Uses trigger_rule='all_done' on the calling task so it fires even when
    materialization partially fails. Any exception is caught and logged —
    the materialize DAG run is never failed by the trigger.
    """
    try:
        trigger_dag_api(
            dag_id=PREPARE_DAG_ID,
            conf={},
            run_id=f"train_prepare_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}",
        )
        logger.info("_trigger_prepare: triggered %s", PREPARE_DAG_ID)
    except Exception as exc:  # noqa: BLE001 fire-and-forget
        logger.warning("could not trigger %s: %s", PREPARE_DAG_ID, exc)


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
        "trim proposals (issue #88/#159). Requires nas_ffmpeg Airflow pool (pool_slots=1). "
        "On completion chains to speaker_turn_prepare via trigger_prepare."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
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
    trigger_prepare_task = PythonOperator(
        task_id="trigger_prepare",
        python_callable=_trigger_prepare,
        trigger_rule="all_done",
    )

    select_turns_task >> materialize_turns_task >> collect_results_task >> trigger_prepare_task
