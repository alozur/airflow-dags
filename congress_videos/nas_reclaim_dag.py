"""NAS Reclaim DAG.

Frequent maintenance DAG that reclaims local disk space for videos already
safely archived on the NAS. Runs every 4 hours — more often than
``nas_archive``'s daily cadence — so ephemeral material fetched back by
``nas_fetch.ensure_local_video`` does not sit on the VPS's constrained disk
for the full 14-day archive retention window. Disabled by default
(``NAS_ARCHIVE_HOST`` empty), same as ``nas_archive``/``nas_fetch``: reuses
the same ``ArchiveSettings``/SSH key contract.

All gate logic (lock, grace window, DB completeness, NAS verification —
design D3) lives in ``congress_videos.modules.nas_reclaim`` and is tested
directly by ``test_nas_reclaim.py``. This DAG module is thin wiring only:
resolve settings/paths, pull a DB candidate pool, aggregate outcomes.

Pipeline (this commit)::

    check_enabled       (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh keys present)
      → select_candidates  (DB completeness pool -> advisory gate pre-check, batch-capped)

The deletion step (``reclaim_videos``, re-checking every gate inside the
fetch lock immediately before ``verify_synced``/``prune_local``) is wired in
a follow-up commit — this commit only wires enablement and DB-backed
candidate selection, so no candidate is ever reclaimed yet.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL
from congress_videos.modules.nas_archive import ArchiveSettings
from congress_videos.modules.nas_completeness import complete_video_ids
from congress_videos.modules.nas_reclaim import select_reclaim_candidates
from utils.env_loader import load_env_if_local

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = "nas_reclaim"

# Not part of the infra env contract (deploy/vps-dev/release.env) — operational
# knob read directly by this DAG, matching NAS_ARCHIVE_BATCH's precedent.
NAS_RECLAIM_BATCH = int(os.getenv("NAS_RECLAIM_BATCH", "3"))

# Fetch more DB candidates than the batch needs: select_reclaim_candidates
# filters each id through the grace-window and lock-free gates in Python,
# after the SQL completeness filter, so not every DB candidate clears.
_CANDIDATE_POOL_MULTIPLIER = 5


# ---------------------------------------------------------------------------
# check_enabled
# ---------------------------------------------------------------------------


def _check_enabled(**context) -> bool:
    try:
        settings = ArchiveSettings.from_env()
    except ValueError as exc:
        logger.error("nas_reclaim: invalid NAS archive configuration — %s", exc)
        return False
    if not settings.enabled:
        logger.info("nas_reclaim: disabled (NAS_ARCHIVE_HOST empty)")
        return False
    return True


# ---------------------------------------------------------------------------
# select_candidates — DB completeness pool -> advisory gate pre-check
# ---------------------------------------------------------------------------


def _run_select_candidates(**context) -> list[dict]:
    settings = ArchiveSettings.from_env()
    pool_video_ids = complete_video_ids(NAS_RECLAIM_BATCH * _CANDIDATE_POOL_MULTIPLIER)
    logger.info("nas_reclaim: %d complete video_id candidate(s) from DB", len(pool_video_ids))

    candidates = select_reclaim_candidates(
        settings,
        Path(PROJECT_DATA_DIR),
        DEFAULT_CHANNEL,
        pool_video_ids,
        now=datetime.now(UTC),
        batch=NAS_RECLAIM_BATCH,
    )
    context["ti"].xcom_push(key="candidates", value=candidates)
    return candidates


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Reclaim local disk space for videos already safely archived on the NAS",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["congress", "nas", "reclaim"],
) as dag:
    t0_check_enabled = ShortCircuitOperator(
        task_id="check_enabled",
        python_callable=_check_enabled,
    )

    t1_select_candidates = PythonOperator(
        task_id="select_candidates",
        python_callable=_run_select_candidates,
    )

    t0_check_enabled >> t1_select_candidates
