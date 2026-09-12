"""NAS Archive DAG.

Daily maintenance DAG that offloads local raw/derived material for
**fully-completed** congress videos to the NAS over rsync-over-SSH, then
prunes the freed space from the VPS's local disk. Disabled by default
(``NAS_ARCHIVE_HOST`` empty) so a stack without an archive target behaves
exactly as before this DAG existed.

A video is "complete" when nothing about it remains pending in the upload
pipeline — see ``congress_videos/modules/nas_completeness.complete_video_ids``
(moved there so ``nas_reclaim`` can reuse the identical gate) for the exact
completeness definition and its documented fallback rationale.

Only the single registered channel (``DEFAULT_CHANNEL``) is archived today:
the schema carries no per-video ``channel_slug`` column, so there is no way
to resolve which channel a ``video_chapters`` row belongs to. Revisit
``_run_select_candidates`` when a second channel is onboarded.

Pipeline::

    check_enabled     (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh key files present)
      → select_candidates  (eligibility SQL + marker skip + early-sync/late-prune split)
          → archive_videos (remote mkdir → rsync → verify → [age-gated] prune_local + write_marker)
              → mirror_shared (remote mkdir → rsync each MIRROR_ONLY_DIRS entry — never pruned)

Sync and prune are on two different cadences (early sync, late prune): every
DB-complete, unmarked video with local paths is synced (rsync push + verify)
on EVERY run regardless of age — rsync is incremental, so re-syncing an
unchanged video is cheap — up to ``NAS_ARCHIVE_SYNC_BATCH`` videos per run.
Pruning (local deletion + marker write) only happens for the subset whose
local material is already at least ``NAS_ARCHIVE_MIN_AGE_DAYS`` old, capped
separately at ``NAS_ARCHIVE_BATCH`` prunes per run (pruning is the
destructive, rate-limited half of this pipeline; syncing is not). A video
that clears the sync cap before the prune cap is reached stays synced but
unpruned this run — it remains eligible next run, and the re-sync is close
to a no-op since the NAS copy is already current.

One failed video aborts the run: the failing video has no partial local
deletion (sync+verify happens for every local path before any pruning), and
every not-yet-processed video in the batch is left completely untouched.

``mirror_shared`` runs on every enabled run, independent of ``select_candidates``
picking zero videos: it mirrors ``PROJECT_DATA_DIR/thumbnails/`` (small
PNG/JSON files keyed by the *uploaded* YouTube video id, so no single source
video owns that material) to the NAS with a plain, non-deleting rsync. Unlike
per-video material, mirrored directories are never verified or pruned locally
— see ``congress_videos/modules/nas_archive.mirror_paths``.
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
from congress_videos.modules import nas_archive
from congress_videos.modules.nas_archive import ArchiveSettings
from congress_videos.modules.nas_completeness import complete_video_ids
from utils.env_loader import load_env_if_local

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = "nas_archive"

# Not part of the infra env contract (deploy/vps-dev/release.env) — operational
# knobs read directly by this DAG.
# Cap on PRUNEs (local deletion + marker write) per run — the destructive,
# rate-limited half of the pipeline.
NAS_ARCHIVE_BATCH = int(os.getenv("NAS_ARCHIVE_BATCH", "2"))
# Cap on SYNCs (rsync push + verify, no deletion) per run — larger than
# NAS_ARCHIVE_BATCH since syncing is cheap and non-destructive (rsync is
# incremental, so a video that was already synced re-syncs as a near no-op).
NAS_ARCHIVE_SYNC_BATCH = int(os.getenv("NAS_ARCHIVE_SYNC_BATCH", "20"))

# Fetch more DB candidates than the sync batch needs: the local age gate and
# the already-archived marker check happen in Python, after the SQL filter.
_CANDIDATE_POOL_MULTIPLIER = 5

_SSH_TIMEOUT_SECS = 30
_RSYNC_TIMEOUT_SECS = 3600  # raw downloads are multi-GB; generous ceiling
_ITEMIZE_LOG_LINE_CAP = 50


# ---------------------------------------------------------------------------
# select_candidates — eligibility SQL + local filesystem gates
# ---------------------------------------------------------------------------


def _newest_mtime(paths: list[Path]) -> float | None:
    """Return the newest mtime (epoch seconds) across every file under ``paths``."""
    newest: float | None = None
    for path in paths:
        files = [path] if path.is_file() else list(path.rglob("*"))
        for file_path in files:
            if not file_path.is_file():
                continue
            mtime = file_path.stat().st_mtime
            if newest is None or mtime > newest:
                newest = mtime
    return newest


def select_archive_candidates(settings: ArchiveSettings, project_dir: Path, channel_slug: str) -> list[dict]:
    """Return up to ``NAS_ARCHIVE_SYNC_BATCH`` videos to sync this run.

    Applies, in order: the DB completeness query, the already-archived
    marker skip (marker == pruned, so a marked video has nothing local left
    to sync), and local-path existence. Every remaining video is a sync
    candidate regardless of age; each candidate additionally carries
    ``"prune": bool`` — ``True`` only when its local material is at least
    ``settings.min_age_days`` old AND the ``NAS_ARCHIVE_BATCH`` prune cap for
    this run has not yet been reached (see the module docstring for the
    early-sync/late-prune rationale).
    """
    min_age = timedelta(days=settings.min_age_days)
    pool_video_ids = complete_video_ids(NAS_ARCHIVE_SYNC_BATCH * _CANDIDATE_POOL_MULTIPLIER)
    logger.info("nas_archive: %d complete video_id candidate(s) from DB", len(pool_video_ids))

    candidates: list[dict] = []
    prune_count = 0
    for video_id in pool_video_ids:
        channel_dir = project_dir / channel_slug / video_id
        if nas_archive.is_archived(channel_dir):
            logger.debug("nas_archive: video_id=%s already archived — skipping", video_id)
            continue

        try:
            paths = nas_archive.video_paths(project_dir, channel_slug, video_id)
        except FileNotFoundError:
            logger.warning("nas_archive: video_id=%s has no local paths — skipping", video_id)
            continue

        newest_mtime = _newest_mtime(paths)
        if newest_mtime is None:
            logger.warning("nas_archive: video_id=%s has no files under its local paths — skipping", video_id)
            continue

        age = datetime.now(UTC) - datetime.fromtimestamp(newest_mtime, tz=UTC)
        old_enough = age >= min_age
        prune = old_enough and prune_count < NAS_ARCHIVE_BATCH
        if not old_enough:
            logger.debug(
                "nas_archive: video_id=%s is only %s old (< %s) — synced, not yet eligible for prune",
                video_id,
                age,
                min_age,
            )
        elif not prune:
            logger.debug(
                "nas_archive: video_id=%s is old enough to prune but the prune cap (%d) was already reached "
                "this run — synced only",
                video_id,
                NAS_ARCHIVE_BATCH,
            )

        candidates.append({"channel_slug": channel_slug, "video_id": video_id, "prune": prune})
        if prune:
            prune_count += 1
        if len(candidates) >= NAS_ARCHIVE_SYNC_BATCH:
            break

    logger.info(
        "nas_archive: %d candidate(s) selected for sync (sync cap=%d), %d marked for prune (prune cap=%d)",
        len(candidates),
        NAS_ARCHIVE_SYNC_BATCH,
        prune_count,
        NAS_ARCHIVE_BATCH,
    )
    return candidates


def _check_enabled(**context) -> bool:
    try:
        settings = ArchiveSettings.from_env()
    except ValueError as exc:
        logger.error("nas_archive: invalid NAS archive configuration — %s", exc)
        return False
    if not settings.enabled:
        logger.info("NAS archive disabled (NAS_ARCHIVE_HOST empty)")
        return False
    return True


def _run_select_candidates(**context) -> list[dict]:
    settings = ArchiveSettings.from_env()
    candidates = select_archive_candidates(settings, Path(PROJECT_DATA_DIR), DEFAULT_CHANNEL)
    context["ti"].xcom_push(key="candidates", value=candidates)
    return candidates


# ---------------------------------------------------------------------------
# archive_videos — remote mkdir -> rsync -> verify -> prune -> marker
# ---------------------------------------------------------------------------


def _subprocess_runner(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, timeout=_RSYNC_TIMEOUT_SECS, check=False)


def _log_itemized(stdout: str, video_id: str, local_path: Path) -> None:
    lines = stdout.splitlines()
    for line in lines[:_ITEMIZE_LOG_LINE_CAP]:
        logger.info("nas_archive: video_id=%s %s: %s", video_id, local_path, line)
    if len(lines) > _ITEMIZE_LOG_LINE_CAP:
        logger.info(
            "nas_archive: video_id=%s %s: ... (%d more line(s) truncated)",
            video_id,
            local_path,
            len(lines) - _ITEMIZE_LOG_LINE_CAP,
        )


def _bytes_to_be_freed(paths: list[Path], project_dir: Path) -> int:
    """Sum the size of exactly the files ``prune_local`` will delete.

    Mirrors ``nas_archive.prune_local``'s own selection (whole raw-download
    directory, or ``*.mp4``-only under the channel subtree) so the logged
    total matches what actually disappears from local disk.
    """
    downloads_dir = project_dir / "downloads"
    total = 0
    for path in paths:
        if path.parent.parent == downloads_dir:
            for file_path in path.rglob("*"):
                if file_path.is_file():
                    total += file_path.stat().st_size
        elif path.is_dir():
            for mp4_file in path.rglob("*.mp4"):
                total += mp4_file.stat().st_size
        elif path.suffix == ".mp4":
            total += path.stat().st_size
    return total


def archive_one_video(
    settings: ArchiveSettings, project_dir: Path, channel_slug: str, video_id: str, prune: bool = True
) -> dict:
    """Sync (and verify) every local path for one video; prune only when ``prune`` is ``True``.

    Every local path is rsynced AND verified before any local deletion
    happens, so a failure partway through (mkdir, rsync, or verification)
    leaves this video's local files completely untouched — the
    abort-before-delete invariant holds regardless of ``prune``, since
    verification always runs before the ``prune`` branch is even reached.

    When ``prune`` is ``False`` (the video isn't old enough yet, or this
    run's prune cap was already reached — see ``select_archive_candidates``),
    the video is synced but nothing local is deleted and no marker is
    written: it stays eligible for a future run's prune pass, and the next
    sync is close to a no-op since the NAS copy is already current.

    Raises:
        AirflowException: On any remote-mkdir failure, rsync failure, or
            post-sync verification mismatch.
    """
    paths = nas_archive.video_paths(project_dir, channel_slug, video_id)

    synced_dirs: list[str] = []
    for local_path in paths:
        remote_relative_dir = local_path.relative_to(project_dir).as_posix()

        mkdir_cmd = nas_archive.remote_mkdir_command(settings, remote_relative_dir)
        mkdir_result = subprocess.run(mkdir_cmd, capture_output=True, text=True, timeout=_SSH_TIMEOUT_SECS, check=False)
        if mkdir_result.returncode != 0:
            raise AirflowException(
                f"nas_archive: remote mkdir failed for video_id={video_id} path={local_path}: "
                f"{mkdir_result.stderr.strip()}"
            )

        rsync_cmd = nas_archive.rsync_command(settings, local_path, remote_relative_dir, dry_run=False)
        rsync_result = _subprocess_runner(rsync_cmd)
        _log_itemized(rsync_result.stdout, video_id, local_path)
        if rsync_result.returncode != 0:
            raise AirflowException(
                f"nas_archive: rsync failed (exit={rsync_result.returncode}) for video_id={video_id} "
                f"path={local_path}: {rsync_result.stderr.strip()}"
            )

        if not nas_archive.verify_synced(settings, local_path, remote_relative_dir, runner=_subprocess_runner):
            raise AirflowException(
                f"nas_archive: verification failed for video_id={video_id} path={local_path} — "
                "NAS mirror is not byte-identical; local files were NOT deleted"
            )
        synced_dirs.append(remote_relative_dir)

    if not prune:
        logger.info(
            "nas_archive: video_id=%s synced — %d path(s) synced (not yet eligible for prune)",
            video_id,
            len(synced_dirs),
        )
        return {"video_id": video_id, "synced": synced_dirs, "removed": [], "bytes_freed": 0, "pruned": False}

    bytes_to_free = _bytes_to_be_freed(paths, project_dir)
    removed = nas_archive.prune_local(paths, project_dir)

    channel_dir = project_dir / channel_slug / video_id
    nas_archive.write_marker(
        channel_dir,
        {
            "archived_at": datetime.now(UTC).isoformat(),
            "host": settings.host,
            "root": settings.root,
            "removed": removed,
            "synced": synced_dirs,
        },
    )
    logger.info(
        "nas_archive: video_id=%s synced+pruned — %d path(s) synced, %d local item(s) removed, ~%.1f MB freed",
        video_id,
        len(synced_dirs),
        len(removed),
        bytes_to_free / (1024 * 1024),
    )
    return {
        "video_id": video_id,
        "synced": synced_dirs,
        "removed": removed,
        "bytes_freed": bytes_to_free,
        "pruned": True,
    }


def _run_archive_videos(**context) -> dict:
    """Sync/verify/prune every candidate; one failing video isolates, not aborts.

    Mirrors ``nas_fetch_dag._run_fetch_videos``'s per-video isolation: a
    failure in ``archive_one_video`` (remote mkdir, rsync, or verification —
    see its docstring) is caught, logged, and recorded in
    ``summary["failed"]`` instead of failing the whole task, so one broken
    video no longer aborts every other already-eligible candidate in the
    batch. Only when EVERY requested candidate failed does the task raise,
    naming the failed video_ids — an empty candidate list is a normal,
    successful no-op run, not an all-failed one.
    """
    settings = ArchiveSettings.from_env()
    candidates = context["ti"].xcom_pull(key="candidates", task_ids="select_candidates") or []
    project_dir = Path(PROJECT_DATA_DIR)

    summary = {"synced": 0, "pruned": 0, "bytes_freed": 0, "failed": []}
    for candidate in candidates:
        video_id = candidate["video_id"]
        try:
            result = archive_one_video(settings, project_dir, candidate["channel_slug"], video_id, candidate["prune"])
        except (AirflowException, FileNotFoundError, ValueError, subprocess.SubprocessError, OSError) as exc:
            logger.error("nas_archive: video_id=%s aborted — %s", video_id, exc)
            summary["failed"].append({"video_id": video_id, "error": str(exc)})
            continue
        summary["synced"] += 1
        if result["pruned"]:
            summary["pruned"] += 1
        summary["bytes_freed"] += result["bytes_freed"]

    logger.info("nas_archive: run complete — %s", summary)

    if summary["failed"] and summary["synced"] == 0:
        failures = "; ".join(f"video_id={f['video_id']}: {f['error']}" for f in summary["failed"])
        raise AirflowException(f"nas_archive: all {len(summary['failed'])} candidate video(s) failed — {failures}")

    return summary


# ---------------------------------------------------------------------------
# mirror_shared — remote mkdir -> rsync (no verify, no prune) for shared dirs
# ---------------------------------------------------------------------------


def mirror_shared_dirs(settings: ArchiveSettings, project_dir: Path) -> dict:
    """Mirror every ``nas_archive.MIRROR_ONLY_DIRS`` entry to the NAS.

    Unlike :func:`archive_one_video`, this never verifies or prunes: mirrored
    directories (e.g. ``thumbnails/``) are shared, unattributable to one
    video, and archived indefinitely — see ``nas_archive.mirror_paths``.

    Raises:
        AirflowException: On any remote-mkdir failure or rsync failure.
    """
    mirrored: list[str] = []
    changed_lines = 0

    for local_path in nas_archive.mirror_paths(project_dir):
        remote_relative_dir = local_path.relative_to(project_dir).as_posix()

        mkdir_cmd = nas_archive.remote_mkdir_command(settings, remote_relative_dir)
        mkdir_result = subprocess.run(mkdir_cmd, capture_output=True, text=True, timeout=_SSH_TIMEOUT_SECS, check=False)
        if mkdir_result.returncode != 0:
            raise AirflowException(
                f"nas_archive: remote mkdir failed for mirror dir={remote_relative_dir}: {mkdir_result.stderr.strip()}"
            )

        rsync_cmd = nas_archive.rsync_command(settings, local_path, remote_relative_dir, dry_run=False)
        rsync_result = _subprocess_runner(rsync_cmd)
        _log_itemized(rsync_result.stdout, "mirror", local_path)
        if rsync_result.returncode != 0:
            raise AirflowException(
                f"nas_archive: rsync failed (exit={rsync_result.returncode}) for mirror dir={remote_relative_dir}: "
                f"{rsync_result.stderr.strip()}"
            )

        mirrored.append(remote_relative_dir)
        changed_lines += len(rsync_result.stdout.splitlines())

    summary = {"mirrored": mirrored, "changed_lines": changed_lines}
    logger.info("nas_archive: mirror complete — %s", summary)
    return summary


def _run_mirror_shared(**context) -> dict:
    settings = ArchiveSettings.from_env()
    return mirror_shared_dirs(settings, Path(PROJECT_DATA_DIR))


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
    description="Offload local raw/derived material for fully-completed videos to the NAS, then prune local disk",
    schedule="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["congress", "nas", "archive"],
) as dag:
    t0_check_enabled = ShortCircuitOperator(
        task_id="check_enabled",
        python_callable=_check_enabled,
    )

    t1_select_candidates = PythonOperator(
        task_id="select_candidates",
        python_callable=_run_select_candidates,
    )

    t2_archive_videos = PythonOperator(
        task_id="archive_videos",
        python_callable=_run_archive_videos,
    )

    t3_mirror_shared = PythonOperator(
        task_id="mirror_shared",
        python_callable=_run_mirror_shared,
    )

    t0_check_enabled >> t1_select_candidates >> t2_archive_videos >> t3_mirror_shared
