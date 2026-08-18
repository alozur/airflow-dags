"""
Video Analytics Collection DAG (issue #53)

Collects YouTube Analytics metrics at fixed checkpoints (24h, 48h, 7d, 30d, 90d)
for every uploaded chapter video. Runs hourly; each run:

1. staleness_guard  — ShortCircuitOperator: skip stale data_interval_end replays
2. get_pending_checkpoints — fetch candidate chapters from DB
3. fetch_analytics  — call YouTube Analytics API, filter via should_persist()
4. record_snapshots — persist surviving (chapter, checkpoint, metrics) rows

Read-only: no YouTube Data API writes, no action_taken column written.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.env_loader import load_env_if_local

load_env_if_local()

STALE_RUN_TOLERANCE_MINUTES = int(
    os.getenv("ANALYTICS_STALE_RUN_TOLERANCE_MINUTES", "30")
)

_TOKEN_FILE = os.getenv(
    "YOUTUBE_TOKEN_FILE",
    os.path.join(os.path.dirname(__file__), "..", "congress_youtube_token.pickle"),
)


def _staleness_guard(**context) -> bool:
    """Return False for stale data_interval_end replays (skip).

    Mirrors the pattern in youtube_upload_dag.should_upload().
    Returns True (proceed) when the run is fresh or data_interval_end is absent.
    """
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        now = datetime.now(timezone.utc)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "video_analytics: skipping stale run: data_interval_end=%s is %s "
                "behind now=%s (tolerance=%dm)",
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
    from congress_videos.config.analytics_config import CHECKPOINTS
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

    youtube_video_ids = list(
        {row["youtube_video_id"] for row in candidate_rows if row.get("youtube_video_id")}
    )
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

    now = datetime.now(timezone.utc)
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
            upload_date = upload_date.replace(tzinfo=timezone.utc)

        start_date = upload_date.strftime("%Y-%m-%d")
        end_date = (upload_date + timedelta(hours=threshold_hours + 48)).strftime(
            "%Y-%m-%d"
        )

        try:
            resp = (
                service.reports()
                .query(
                    ids=f"channel==MINE",
                    startDate=start_date,
                    endDate=end_date,
                    metrics=",".join(
                        [
                            "views",
                            "impressions",
                            "impressionClickThroughRate",
                            "averageViewDuration",
                            "watchTimeMinutes",
                        ]
                    ),
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
                "video_analytics: skip-and-retry for yt_id=%s checkpoint=%s "
                "(all-None or all-zero metrics)",
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
    logging.info(
        "video_analytics: %d snapshots will be persisted", len(collected)
    )
    return collected


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
    t1_pending = PostgreSQLOperator(
        task_id="get_pending_checkpoints",
        operation="get_pending_analytics_checkpoints",
        output_xcom_key="candidates",
    )

    # t2: Call Analytics API; filter via should_persist(); push 'collected' XCom
    t2_fetch = PythonOperator(
        task_id="fetch_analytics",
        python_callable=_fetch_analytics,
    )

    # t3: Persist snapshot rows (ON CONFLICT DO NOTHING keeps it idempotent)
    t3_record = PostgreSQLOperator(
        task_id="record_snapshots",
        operation="record_analytics_snapshots",
        xcom_keys={"collected": "collected"},
    )

    t0_staleness >> t1_pending >> t2_fetch >> t3_record
