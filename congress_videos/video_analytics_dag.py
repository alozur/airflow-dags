"""
Video Analytics Collection DAG (issue #53)

Collects YouTube Analytics metrics at fixed checkpoints (24h, 48h, 7d, 30d, 90d)
for every uploaded chapter video. Runs hourly; each run:

1. staleness_guard  — ShortCircuitOperator: skip stale data_interval_end replays
2. get_pending_checkpoints — fetch candidate chapters from DB
3. fetch_analytics  — call YouTube Analytics API, filter via should_persist()
4. record_snapshots — persist surviving (chapter, checkpoint, metrics) rows
5. trigger_action_dag — fire-and-forget TriggerDagRunOperator targeting the
   video_analytics_actions child DAG (issue #102)

This collector DAG itself remains read-only: no YouTube Data API writes, no
action_taken column written. All writes (thumbnail/title regeneration,
action_taken/action_detail persistence) happen only in the triggered child
DAG, video_analytics_actions, which uses the separate upload-purpose token.
"""

import logging
import os
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path
from utils.airflow_helpers import utc_normalize_rows
from utils.env_loader import load_env_if_local

load_env_if_local()

STALE_RUN_TOLERANCE_MINUTES = int(os.getenv("ANALYTICS_STALE_RUN_TOLERANCE_MINUTES", "30"))

# Read-only analytics token for the default channel. An explicit
# YOUTUBE_TOKEN_FILE env var still overrides for ad-hoc/testing use.
_TOKEN_FILE = os.getenv(
    "YOUTUBE_TOKEN_FILE",
    resolve_token_path(DEFAULT_CHANNEL, "analytics"),
)


def _staleness_guard(**context) -> bool:
    """Return False for stale data_interval_end replays (skip).

    Mirrors the pattern in youtube_upload_dag.should_upload().
    Returns True (proceed) when the run is fresh or data_interval_end is absent.
    """
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        now = datetime.now(UTC)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "video_analytics: skipping stale run: data_interval_end=%s is %s behind now=%s (tolerance=%dm)",
                data_interval_end,
                staleness,
                now,
                STALE_RUN_TOLERANCE_MINUTES,
            )
            return False
    return True


def _fetch_analytics(ti, **context) -> list:
    """Fetch YouTube Analytics metrics for all pending (video, checkpoint) pairs.

    Pulls candidate chapters from XCom, expands via pending_checkpoints(),
    fetches Analytics API, filters with should_persist(), and pushes the
    surviving records for the record_snapshots task.

    Returns the list of collected items (also pushed to XCom 'collected').
    """
    from congress_videos.config.analytics_config import CHECKPOINTS, METRIC_FIELDS
    from congress_videos.modules.video_analytics import (
        parse_analytics_response,
        pending_checkpoints,
        should_persist,
    )
    from utils.youtube_helpers import get_youtube_analytics_service

    # Pull DB rows from upstream task
    candidate_rows = ti.xcom_pull(key="candidates") or []

    if not candidate_rows:
        logging.info("video_analytics: no candidate rows — nothing to fetch")
        ti.xcom_push(key="collected", value=[])
        return []

    # Query already-collected (youtube_video_id, checkpoint) pairs so we skip
    # re-fetching data we already have, saving Analytics API daily quota.
    from congress_videos.modules.database import CongressionalVideoDB

    youtube_video_ids = list({row["youtube_video_id"] for row in candidate_rows if row.get("youtube_video_id")})
    already_collected: set[tuple[str, str]] = set()
    try:
        db = CongressionalVideoDB()
        already_collected = db.get_collected_analytics_pairs(youtube_video_ids)
        logging.info(
            "video_analytics: %d already-collected pairs found for %d video ids",
            len(already_collected),
            len(youtube_video_ids),
        )
    except Exception as exc:
        logging.warning(
            "video_analytics: could not load collected pairs from DB, "
            "proceeding with empty set (idempotency still DB-enforced): %s",
            exc,
        )

    now = datetime.now(UTC)
    pending = pending_checkpoints(now, candidate_rows, collected=already_collected)

    if not pending:
        logging.info("video_analytics: no pending checkpoints after expansion")
        ti.xcom_push(key="collected", value=[])
        return []

    logging.info("video_analytics: %d (video, checkpoint) pairs to collect", len(pending))

    try:
        service = get_youtube_analytics_service(_TOKEN_FILE)
    except Exception as exc:
        logging.warning("video_analytics: could not build Analytics service: %s", exc)
        ti.xcom_push(key="collected", value=[])
        return []

    collected = []
    for pair in pending:
        yt_id: str = pair["youtube_video_id"]
        checkpoint: str = pair["checkpoint"]
        threshold_hours: int = CHECKPOINTS[checkpoint]

        # Build the date-range query window: from upload_date to upload_date + threshold
        upload_date: datetime = pair.get(
            "youtube_upload_date",
            now - timedelta(hours=threshold_hours),
        )
        if upload_date.tzinfo is None:
            upload_date = upload_date.replace(tzinfo=UTC)

        start_date = upload_date.strftime("%Y-%m-%d")
        end_date = (upload_date + timedelta(hours=threshold_hours + 48)).strftime("%Y-%m-%d")

        try:
            resp = (
                service.reports()
                .query(
                    ids="channel==MINE",
                    startDate=start_date,
                    endDate=end_date,
                    metrics=",".join(METRIC_FIELDS),
                    filters=f"video=={yt_id}",
                )
                .execute()
            )
        except Exception as exc:
            logging.warning(
                "video_analytics: API error for yt_id=%s checkpoint=%s: %s",
                yt_id,
                checkpoint,
                exc,
            )
            continue

        metrics = parse_analytics_response(resp)

        if not should_persist(metrics):
            logging.info(
                "video_analytics: skip-and-retry for yt_id=%s checkpoint=%s (all-None or all-zero metrics)",
                yt_id,
                checkpoint,
            )
            continue

        collected.append(
            {
                "chapter_id": pair["chapter_id"],
                "youtube_video_id": yt_id,
                "checkpoint": checkpoint,
                "metrics": metrics,
            }
        )
        logging.info(
            "video_analytics: collected yt_id=%s checkpoint=%s views=%s",
            yt_id,
            checkpoint,
            metrics.get("views"),
        )

    ti.xcom_push(key="collected", value=collected)
    logging.info("video_analytics: %d snapshots will be persisted", len(collected))
    return collected


def _run_get_pending_checkpoints(ti):
    """Return candidate video chapters for analytics collection.

    Pushes XCom key 'candidates'.

    Rows are UTC-normalized first (issue #303): psycopg2 returns
    youtube_upload_date (TIMESTAMPTZ) with an unnamed non-UTC fixed offset,
    which Airflow serializes with an empty tz name and then cannot
    deserialize on xcom_pull. The SAME normalized object is pushed and
    returned, so the implicit return_value XCom is covered too.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB()
    result = utc_normalize_rows(db.get_pending_analytics_checkpoints())
    logging.info("video_analytics: retrieved %d candidate chapters", len(result))
    ti.xcom_push(key="candidates", value=result)
    return result


def _run_record_snapshots(ti):
    """Persist each collected (chapter, checkpoint, metrics) snapshot.

    Reads the 'collected' XCom pushed by _fetch_analytics. Terminal task —
    does not push an output XCom.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB()
    collected = ti.xcom_pull(key="collected") or []
    logging.info("video_analytics: recording %d analytics snapshots", len(collected))

    for item in collected:
        db.record_analytics_snapshot(
            chapter_id=item["chapter_id"],
            youtube_video_id=item["youtube_video_id"],
            checkpoint=item["checkpoint"],
            metrics=item["metrics"],
        )
        logging.info(
            "video_analytics: recorded snapshot chapter_id=%s yt_id=%s checkpoint=%s",
            item["chapter_id"],
            item["youtube_video_id"],
            item["checkpoint"],
        )

    return {"recorded_snapshots": len(collected)}


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
    "video_analytics",
    default_args=default_args,
    description="Collect YouTube Analytics metrics at fixed checkpoints per uploaded chapter",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["congress", "youtube", "analytics"],
) as dag:
    # t0: Skip stale data_interval_end replays
    t0_staleness = ShortCircuitOperator(
        task_id="staleness_guard",
        python_callable=_staleness_guard,
    )

    # t1: Fetch candidate chapters from DB (uploaded, non-null yt_id, within 90d)
    t1_pending = PythonOperator(
        task_id="get_pending_checkpoints",
        python_callable=_run_get_pending_checkpoints,
    )

    # t2: Call Analytics API; filter via should_persist(); push 'collected' XCom
    t2_fetch = PythonOperator(
        task_id="fetch_analytics",
        python_callable=_fetch_analytics,
    )

    # t3: Persist snapshot rows (ON CONFLICT DO NOTHING keeps it idempotent)
    t3_record = PythonOperator(
        task_id="record_snapshots",
        python_callable=_run_record_snapshots,
    )

    # t4: Fire-and-forget trigger of the write-capable child DAG. This
    # collector never waits on it — regeneration can take minutes and must
    # not block the next hourly collection run.
    t4_trigger_action_dag = TriggerDagRunOperator(
        task_id="trigger_action_dag",
        trigger_dag_id="video_analytics_actions",
        wait_for_completion=False,
    )

    t0_staleness >> t1_pending >> t2_fetch >> t3_record >> t4_trigger_action_dag
