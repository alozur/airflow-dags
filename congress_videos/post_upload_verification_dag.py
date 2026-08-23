"""Post-Upload Verification DAG (issue #141).

Detects YouTube uploads silently abandoned by YouTube after being marked
is_uploaded_to_youtube=TRUE, re-queues genuinely-gone videos, and permanently
marks verified-good ones for both video_chapters and speaker_turn_videos.

Pipeline:
    1. staleness_guard        — ShortCircuitOperator: skip stale replays.
    2. select_unverified_uploads — PostgreSQLOperator: fetch candidates (1h–48h window).
    3. verify_and_record      — PythonOperator: oembed gate → Data API fallback,
                                per-video try/except, MAX_API_CALLS_PER_RUN cap.

Mirrors video_analytics_dag end-to-end in structure.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from congress_videos.modules.post_upload_verification import (
    MAX_API_CALLS_PER_RUN,
    STALE_RUN_TOLERANCE_MINUTES,
    check_video_status,
)
from utils.env_loader import load_env_if_local

load_env_if_local()

# Upload-scoped token covers videos.list (youtube.force-ssl scope).
_TOKEN_FILE = os.getenv(
    "YOUTUBE_TOKEN_FILE",
    resolve_token_path(DEFAULT_CHANNEL, "upload"),
)


def _staleness_guard(**context) -> bool:
    """Return False for stale data_interval_end replays (skip downstream tasks).

    Mirrors the pattern in video_analytics_dag._staleness_guard().
    Returns True (proceed) when the run is fresh or data_interval_end is absent.
    """
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        now = datetime.now(timezone.utc)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "post_upload_verification: skipping stale run: data_interval_end=%s "
                "is %s behind now=%s (tolerance=%dm)",
                data_interval_end,
                staleness,
                now,
                STALE_RUN_TOLERANCE_MINUTES,
            )
            return False
    return True


def _verify_and_record(ti, **context) -> dict:
    """Verify each candidate upload and write outcomes to the DB.

    Pulls 'candidates' XCom from select_unverified_uploads, then for each:
    - Calls check_video_status (oembed gate → Data API fallback).
    - ok      → mark_upload_verified.
    - abandoned → record_upload_verification_failure.
    - processing / unknown → no write (row remains eligible next run).
    - Exceptions in individual checks are logged and swallowed so one bad
      video cannot abort the rest of the run.
    - MAX_API_CALLS_PER_RUN cap: once reached, remaining candidates are
      left unprocessed for the next @hourly run.

    Returns a summary dict pushed to XCom 'verification_summary'.
    """
    import requests

    from utils.youtube_helpers import get_authenticated_youtube_service

    candidates = ti.xcom_pull(key="candidates") or []

    if not candidates:
        logging.info("post_upload_verification: no candidates — nothing to verify")
        result = {"verified": 0, "failures": 0, "skipped": 0, "errors": 0}
        ti.xcom_push(key="verification_summary", value=result)
        return result

    logging.info(
        "post_upload_verification: %d candidates to verify (cap=%d)",
        len(candidates),
        MAX_API_CALLS_PER_RUN,
    )

    try:
        youtube_service = get_authenticated_youtube_service(_TOKEN_FILE)
    except Exception as exc:
        logging.warning(
            "post_upload_verification: could not build YouTube service: %s — "
            "will treat all non-200 oembed as unknown",
            exc,
        )
        youtube_service = None

    db = CongressionalVideoDB()
    api_calls_made = 0
    verified = 0
    failures = 0
    skipped = 0
    errors = 0

    for candidate in candidates:
        youtube_video_id = candidate.get("youtube_video_id") or ""
        item_type = candidate.get("item_type", "chapter")
        item_id = candidate.get("id")
        output_path = candidate.get("output_path")

        # The identifier used for writes: output_path for turns, chapter_id for chapters
        write_id = output_path if (item_type == "turn" and output_path) else item_id

        if api_calls_made >= MAX_API_CALLS_PER_RUN:
            logging.info(
                "post_upload_verification: cap reached (%d), leaving %s id=%s for next run",
                MAX_API_CALLS_PER_RUN,
                item_type,
                item_id,
            )
            skipped += 1
            continue

        try:
            status, detail = check_video_status(
                youtube_video_id,
                http_get=requests.get,
                youtube_service=youtube_service,
            )
            # Only oembed non-200 paths consume Data API quota
            if detail != "oembed_200":
                api_calls_made += 1

        except Exception as exc:
            logging.error(
                "post_upload_verification: unexpected error checking %s id=%s yt_id=%s: %s",
                item_type,
                item_id,
                youtube_video_id,
                exc,
            )
            errors += 1
            continue

        if status == "ok":
            try:
                db.mark_upload_verified(item_type, write_id)
                verified += 1
                logging.info(
                    "post_upload_verification: verified %s id=%s yt_id=%s",
                    item_type,
                    write_id,
                    youtube_video_id,
                )
            except Exception as exc:
                logging.error(
                    "post_upload_verification: DB error marking verified %s id=%s: %s",
                    item_type,
                    write_id,
                    exc,
                )
                errors += 1

        elif status == "abandoned":
            try:
                db.record_upload_verification_failure(item_type, write_id, detail)
                failures += 1
                logging.info(
                    "post_upload_verification: recorded failure for %s id=%s detail=%s",
                    item_type,
                    write_id,
                    detail,
                )
            except Exception as exc:
                logging.error(
                    "post_upload_verification: DB error recording failure %s id=%s: %s",
                    item_type,
                    write_id,
                    exc,
                )
                errors += 1

        else:
            # processing or unknown — leave the row unmodified
            logging.debug(
                "post_upload_verification: no write for %s id=%s status=%s",
                item_type,
                item_id,
                status,
            )

    result = {
        "verified": verified,
        "failures": failures,
        "skipped": skipped,
        "errors": errors,
        "api_calls_made": api_calls_made,
    }
    ti.xcom_push(key="verification_summary", value=result)
    logging.info("post_upload_verification: run complete %s", result)
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
    "post_upload_verification",
    default_args=default_args,
    description="Verify YouTube uploads are live and heal silent 404s (issue #141)",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["congress", "youtube", "verification"],
) as dag:

    # t0: Skip stale data_interval_end replays
    t0_guard = ShortCircuitOperator(
        task_id="staleness_guard",
        python_callable=_staleness_guard,
    )

    # t1: Fetch unverified candidates from DB (1h–48h upload window)
    t1_select = PostgreSQLOperator(
        task_id="select_unverified_uploads",
        operation="select_unverified_uploads",
        output_xcom_key="candidates",
    )

    # t2: Verify each candidate; write outcomes via CongressionalVideoDB
    t2_verify = PythonOperator(
        task_id="verify_and_record",
        python_callable=_verify_and_record,
    )

    t0_guard >> t1_select >> t2_verify
