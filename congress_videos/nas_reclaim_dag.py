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

Pipeline::

    check_enabled       (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh keys present)
      → select_candidates  (DB completeness pool -> advisory gate pre-check, batch-capped)
          → reclaim_videos (per candidate, INSIDE the fetch lock: re-check every
                             gate -> verify_synced -> prune_local -> write marker)

``reclaim_one_video`` never raises for a normal gate outcome: ``"blocked"``
(grace window / no local material / unverified) and ``"skipped"`` (fetch
lock busy) are expected non-deletions, not failures — a run where every
candidate is blocked/skipped is a quiet, successful no-op. Only an
unexpected error (e.g. ``prune_local``'s protected-path guard) is isolated
per candidate into ``summary["failed"]``; only when EVERY candidate errors
does the task raise, naming the failed video_ids (mirrors ``nas_archive``'s
all-failed rule, design D6).
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL
from congress_videos.modules.nas_archive import ArchiveSettings
from congress_videos.modules.nas_completeness import complete_video_ids
from congress_videos.modules.nas_fetch import RSYNC_TIMEOUT_SECS
from congress_videos.modules.nas_reclaim import reclaim_one_video, select_reclaim_candidates
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

# congress_videos.modules.nas_fetch.RSYNC_TIMEOUT_SECS is the single source of
# truth for the subprocess ceiling (design D7).
_RSYNC_TIMEOUT_SECS = RSYNC_TIMEOUT_SECS


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
# reclaim_videos — per-candidate isolation + all-failed raise
# ---------------------------------------------------------------------------


def _subprocess_runner(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, timeout=_RSYNC_TIMEOUT_SECS, check=False)


def _run_reclaim_videos(**context) -> dict:
    """Reclaim every candidate; one failing video isolates, not aborts (see module docstring)."""
    settings = ArchiveSettings.from_env()
    candidates = context["ti"].xcom_pull(key="candidates", task_ids="select_candidates") or []
    project_dir = Path(PROJECT_DATA_DIR)
    now = datetime.now(UTC)

    summary = {"reclaimed": 0, "blocked": 0, "skipped": 0, "failed": []}
    for candidate in candidates:
        video_id = candidate["video_id"]
        try:
            result = reclaim_one_video(
                settings,
                project_dir,
                candidate["channel_slug"],
                video_id,
                runner=_subprocess_runner,
                now=now,
            )
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            logger.error("nas_reclaim: video_id=%s aborted — %s", video_id, exc)
            summary["failed"].append({"video_id": video_id, "error": str(exc)})
            continue
        summary[result["status"]] += 1

    logger.info("nas_reclaim: run complete — %s", summary)

    if candidates and len(summary["failed"]) == len(candidates):
        failures = "; ".join(f"video_id={f['video_id']}: {f['error']}" for f in summary["failed"])
        raise AirflowException(f"nas_reclaim: all {len(summary['failed'])} candidate video(s) failed — {failures}")

    return summary


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

    t2_reclaim_videos = PythonOperator(
        task_id="reclaim_videos",
        python_callable=_run_reclaim_videos,
    )

    t0_check_enabled >> t1_select_candidates >> t2_reclaim_videos
