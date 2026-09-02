"""Thumbnail Republish Healer DAG (issue #331).

Retries thumbnail-publish failures recorded at upload time (mark_turn_
thumbnail_republish_needed, see congress_videos/modules/upload_marking.py)
without touching any upload-verification state. Structurally guarded: the
three DD4 healer methods on CongressionalVideoDB never SET
is_uploaded_to_youtube, youtube_video_id, youtube_upload_date,
upload_attempts, is_upload_abandoned, upload_verified_at, or prepared_at.

Pipeline:
    1. staleness_guard                        — ShortCircuitOperator: skip
                                                  stale replays (180m
                                                  tolerance, DD6).
    2. select_thumbnail_republish_candidates   — PythonOperator: fetch up
                                                  to CANDIDATE_LIMIT rows,
                                                  one per output_path.
    3. heal_thumbnails                         — PythonOperator: replay
                                                  set_thumbnail_for_video
                                                  per candidate, per-item
                                                  try/except, capped at
                                                  MAX_THUMBNAIL_CALLS_PER_RUN.

Runs once a day at 15:00 UTC (design DD6) — inside the NAS's 14:00-20:00 UTC
quiet band and ~4h ahead of the 19:00 UTC uploader, so a heal run never
contends with the upload run for volume I/O.

Unlike congress_videos/modules/thumbnail_republish.py, this file legitimately
defines a workflow and is exempt from the keyword-avoidance rule in design
DD5. Heavy imports (googleapiclient, utils.youtube_helpers) stay inside the
task callables, mirroring post_upload_verification_dag._verify_and_record.

Operational note: new DAGs land paused by default in this deployment, and
the pause survives git_sync. This DAG requires an explicit unpause on dev
and prod after rollout — no code action, deployment step only.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.thumbnail_republish import (
    CANDIDATE_LIMIT,
    MAX_THUMBNAIL_CALLS_PER_RUN,
    STALE_RUN_TOLERANCE_MINUTES,
    attempt_thumbnail_republish,
)
from utils.env_loader import load_env_if_local

load_env_if_local()

# Upload-scoped token: videos.setThumbnail needs the same youtube.force-ssl
# scope already carried by the upload-purpose token (design threat matrix:
# credential exposure row — no new scope).
_TOKEN_FILE = os.getenv(
    "YOUTUBE_TOKEN_FILE",
    resolve_token_path(DEFAULT_CHANNEL, "upload"),
)


def _staleness_guard(**context) -> bool:
    """Return False for stale data_interval_end replays (skip downstream tasks).

    Mirrors post_upload_verification_dag._staleness_guard, but with a 180m
    tolerance (DD6) instead of the hourly DAG's 30m — this healer runs once
    a day, so a shorter tolerance would skip legitimate runs merely delayed
    by NAS I/O contention.
    """
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        now = datetime.now(timezone.utc)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "thumbnail_republish: skipping stale run: data_interval_end=%s "
                "is %s behind now=%s (tolerance=%dm)",
                data_interval_end,
                staleness,
                now,
                STALE_RUN_TOLERANCE_MINUTES,
            )
            return False
    return True


def _run_select_candidates(ti):
    """Return healer candidates, one row per output_path (DISTINCT ON).

    Pushes XCom key 'candidates'.
    """
    db = CongressionalVideoDB()
    candidates = db.select_turns_needing_thumbnail_republish(limit=CANDIDATE_LIMIT)
    logging.info(
        "thumbnail_republish: %d candidates (limit=%d)",
        len(candidates),
        CANDIDATE_LIMIT,
    )
    ti.xcom_push(key="candidates", value=candidates)
    return candidates


def _heal_thumbnails(ti, **context) -> dict:
    """Replay set_thumbnail_for_video for each candidate and write outcomes.

    Pulls 'candidates' XCom from select_thumbnail_republish_candidates, then
    for each candidate:
    - Calls attempt_thumbnail_republish (dependency-injected, never raises).
    - healed  -> mark_turn_thumbnail_republished.
    - retry   -> record_turn_thumbnail_republish_failure(abandon=False).
    - abandon -> record_turn_thumbnail_republish_failure(abandon=True).
    - Any unexpected exception around a single candidate is logged and
      swallowed so one bad candidate cannot abort the rest of the run.
    - MAX_THUMBNAIL_CALLS_PER_RUN cap: once reached, remaining candidates
      are left unprocessed (marker stays armed) for the next run.

    Returns a summary dict pushed to XCom 'republish_summary'.
    """
    from utils.youtube_helpers import get_authenticated_youtube_service, set_thumbnail_for_video

    candidates = ti.xcom_pull(key="candidates") or []

    if not candidates:
        logging.info("thumbnail_republish: no candidates — nothing to heal")
        result = {"healed": 0, "retried": 0, "abandoned": 0, "skipped": 0, "errors": 0, "calls_made": 0}
        ti.xcom_push(key="republish_summary", value=result)
        return result

    logging.info(
        "thumbnail_republish: %d candidates to heal (cap=%d)",
        len(candidates),
        MAX_THUMBNAIL_CALLS_PER_RUN,
    )

    try:
        youtube_service = get_authenticated_youtube_service(_TOKEN_FILE)
    except Exception as exc:
        logging.error(
            "thumbnail_republish: could not build YouTube service — no thumbnail "
            "can be republished this run, all %d candidates left for next run: %s",
            len(candidates),
            exc,
        )
        result = {
            "healed": 0, "retried": 0, "abandoned": 0,
            "skipped": len(candidates), "errors": 0, "calls_made": 0,
        }
        ti.xcom_push(key="republish_summary", value=result)
        return result

    db = CongressionalVideoDB()
    calls_made = 0
    healed = 0
    retried = 0
    abandoned = 0
    skipped = 0
    errors = 0

    for candidate in candidates:
        output_path = candidate.get("output_path")
        youtube_video_id = candidate.get("youtube_video_id")

        if calls_made >= MAX_THUMBNAIL_CALLS_PER_RUN:
            logging.info(
                "thumbnail_republish: cap reached (%d), leaving output_path=%r for next run",
                MAX_THUMBNAIL_CALLS_PER_RUN,
                output_path,
            )
            skipped += 1
            continue

        try:
            def _set_thumbnail_fn(thumbnail_path, _yt=youtube_service, _vid=youtube_video_id):
                return set_thumbnail_for_video(_yt, _vid, thumbnail_path)

            status, detail = attempt_thumbnail_republish(
                output_path, set_thumbnail_fn=_set_thumbnail_fn
            )
            calls_made += 1
        except Exception as exc:
            logging.error(
                "thumbnail_republish: unexpected error healing output_path=%r yt_id=%s: %s",
                output_path,
                youtube_video_id,
                exc,
            )
            errors += 1
            continue

        try:
            if status == "healed":
                db.mark_turn_thumbnail_republished(output_path)
                healed += 1
                logging.info("thumbnail_republish: healed output_path=%r", output_path)
            elif status == "abandon":
                db.record_turn_thumbnail_republish_failure(output_path, detail, abandon=True)
                abandoned += 1
                logging.warning(
                    "thumbnail_republish: abandoned output_path=%r detail=%s",
                    output_path,
                    detail,
                )
            else:  # "retry"
                db.record_turn_thumbnail_republish_failure(output_path, detail, abandon=False)
                retried += 1
                logging.info(
                    "thumbnail_republish: retry recorded output_path=%r detail=%s",
                    output_path,
                    detail,
                )
        except Exception as exc:
            logging.error(
                "thumbnail_republish: DB error recording %s outcome for output_path=%r: %s",
                status,
                output_path,
                exc,
            )
            errors += 1

    result = {
        "healed": healed,
        "retried": retried,
        "abandoned": abandoned,
        "skipped": skipped,
        "errors": errors,
        "calls_made": calls_made,
    }
    ti.xcom_push(key="republish_summary", value=result)
    logging.info("thumbnail_republish: run complete %s", result)
    return result


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "thumbnail_republish",
    default_args=default_args,
    description="Retry failed thumbnail publishes for verified turn uploads (issue #331)",
    schedule="0 15 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["congress", "youtube", "thumbnail"],
) as dag:

    # t0: Skip stale data_interval_end replays
    t0_guard = ShortCircuitOperator(
        task_id="staleness_guard",
        python_callable=_staleness_guard,
    )

    # t1: Fetch healer candidates, one per output_path
    t1_select = PythonOperator(
        task_id="select_thumbnail_republish_candidates",
        python_callable=_run_select_candidates,
    )

    # t2: Republish each candidate; write outcomes via CongressionalVideoDB
    t2_heal = PythonOperator(
        task_id="heal_thumbnails",
        python_callable=_heal_thumbnails,
    )

    t0_guard >> t1_select >> t2_heal
