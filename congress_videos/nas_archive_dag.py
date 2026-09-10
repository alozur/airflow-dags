"""NAS Archive DAG.

Daily maintenance DAG that offloads local raw/derived material for
**fully-completed** congress videos to the NAS over rsync-over-SSH, then
prunes the freed space from the VPS's local disk. Disabled by default
(``NAS_ARCHIVE_HOST`` empty) so a stack without an archive target behaves
exactly as before this DAG existed.

A video is "complete" when every chapter and every speaker-turn video
derived from it has cleared the YouTube upload+verification pipeline (or was
permanently abandoned — see ``congress_videos/modules/post_upload_verification.py``)
and nothing about it remains pending in ``uploadable_chapters``/``uploadable_turns``.
The schema has no explicit "this turn will never be materialized" flag, so
"every speaker-turn video uploaded AND verified" cannot be expressed as a
literal join without risking candidates that never converge (a turn with no
``speaker_turn_videos`` row could mean "not yet materialized" OR "filtered
out and will never be materialized", e.g. ``is_procedural``/low
``interest_score``). ``_query_complete_video_ids`` therefore uses the
documented, safe fallback: chapters exist, nothing for the video is pending
in ``uploadable_chapters``/``uploadable_turns``, and no uploaded chapter or
turn is missing ``upload_verified_at``. A video with zero speaker turns (or
zero uploadable chapters left) satisfies this vacuously, so it qualifies once
its files clear the age gate — matching the "zero turns but old enough"
case.

Only the single registered channel (``DEFAULT_CHANNEL``) is archived today:
the schema carries no per-video ``channel_slug`` column, so there is no way
to resolve which channel a ``video_chapters`` row belongs to. Revisit
``_run_select_candidates`` when a second channel is onboarded.

Pipeline::

    check_enabled     (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh key files present)
      → select_candidates  (eligibility SQL + local age gate + marker skip)
          → archive_videos (remote mkdir → rsync → verify → prune_local → write_marker)

One failed video aborts the run: the failing video has no partial local
deletion (sync+verify happens for every local path before any pruning), and
every not-yet-processed video in the batch is left completely untouched.
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
from utils.env_loader import load_env_if_local
from utils.postgres_helpers import PostgresConnection

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = "nas_archive"

# Not part of the infra env contract (deploy/vps-dev/release.env) — an
# operational knob read directly by this DAG.
NAS_ARCHIVE_BATCH = int(os.getenv("NAS_ARCHIVE_BATCH", "2"))

# Fetch more DB candidates than the batch needs: the local age gate and the
# already-archived marker check happen in Python, after the SQL filter.
_CANDIDATE_POOL_MULTIPLIER = 5

_SSH_TIMEOUT_SECS = 30
_RSYNC_TIMEOUT_SECS = 3600  # raw downloads are multi-GB; generous ceiling
_ITEMIZE_LOG_LINE_CAP = 50


# ---------------------------------------------------------------------------
# select_candidates — eligibility SQL + local filesystem gates
# ---------------------------------------------------------------------------


def _query_complete_video_ids(pool_limit: int) -> list[str]:
    """Return source video_ids with nothing left pending in the upload pipeline.

    Relies on:
      - ``video_chapters``       (congress_videos/sql/production_schema.sql:62)
      - ``uploadable_chapters``  (congress_videos/sql/production_schema.sql:475,
        migration 038 — relevance_score >= 2 AND NOT is_upload_abandoned gate)
      - ``uploadable_turns``     (congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql:52,
        cumulative view lineage documented at production_schema.sql:565)
      - ``speaker_turns``        (congress_videos/sql/production_schema.sql:265)
      - ``speaker_turn_videos``  (congress_videos/sql/production_schema.sql:320)

    Ordered oldest-chapter-first (FIFO) so the batch drains the longest-idle
    videos first.
    """
    pg = PostgresConnection()
    chapters_table = pg.get_qualified_table("video_chapters")
    uploadable_chapters_table = pg.get_qualified_table("uploadable_chapters")
    uploadable_turns_table = pg.get_qualified_table("uploadable_turns")
    turn_videos_table = pg.get_qualified_table("speaker_turn_videos")
    turns_table = pg.get_qualified_table("speaker_turns")

    query = f"""
        SELECT vc.video_id
        FROM {chapters_table} vc
        WHERE NOT EXISTS (
            SELECT 1 FROM {uploadable_chapters_table} uc WHERE uc.video_id = vc.video_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM {chapters_table} vc2
            WHERE vc2.video_id = vc.video_id
              AND vc2.is_uploaded_to_youtube = TRUE
              AND vc2.upload_verified_at IS NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM {uploadable_turns_table} ut WHERE ut.video_id = vc.video_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {turn_videos_table} stv
            JOIN {turns_table} st ON st.turn_id = stv.turn_id
            JOIN {chapters_table} vc3 ON vc3.chapter_id = st.chapter_id
            WHERE vc3.video_id = vc.video_id
              AND stv.is_uploaded_to_youtube = TRUE
              AND stv.upload_verified_at IS NULL
        )
        GROUP BY vc.video_id
        ORDER BY MIN(vc.created_at) ASC
        LIMIT %s
    """
    with pg.get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (pool_limit,))
        return [row["video_id"] for row in cur.fetchall()]


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
    """Return up to ``NAS_ARCHIVE_BATCH`` videos ready to archive.

    Applies, in order: the DB completeness query, the already-archived
    marker skip, local-path existence, and the local age gate
    (``settings.min_age_days``).
    """
    min_age = timedelta(days=settings.min_age_days)
    pool_video_ids = _query_complete_video_ids(NAS_ARCHIVE_BATCH * _CANDIDATE_POOL_MULTIPLIER)
    logger.info("nas_archive: %d complete video_id candidate(s) from DB", len(pool_video_ids))

    candidates: list[dict] = []
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
        if age < min_age:
            logger.debug(
                "nas_archive: video_id=%s is only %s old (< %s) — not yet eligible",
                video_id,
                age,
                min_age,
            )
            continue

        candidates.append({"channel_slug": channel_slug, "video_id": video_id})
        if len(candidates) >= NAS_ARCHIVE_BATCH:
            break

    logger.info(
        "nas_archive: %d candidate(s) selected for archival (batch cap=%d)",
        len(candidates),
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


def archive_one_video(settings: ArchiveSettings, project_dir: Path, channel_slug: str, video_id: str) -> dict:
    """Sync, verify, and prune every local path for one video.

    Every local path is rsynced AND verified before any local deletion
    happens, so a failure partway through (mkdir, rsync, or verification)
    leaves this video's local files completely untouched.

    Raises:
        AirflowException: On any remote-mkdir failure, rsync failure, or
            post-sync verification mismatch.
    """
    paths = nas_archive.video_paths(project_dir, channel_slug, video_id)
    bytes_to_free = _bytes_to_be_freed(paths, project_dir)

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
        "nas_archive: video_id=%s archived — %d path(s) synced, %d local item(s) removed, ~%.1f MB freed",
        video_id,
        len(synced_dirs),
        len(removed),
        bytes_to_free / (1024 * 1024),
    )
    return {"video_id": video_id, "synced": synced_dirs, "removed": removed, "bytes_freed": bytes_to_free}


def _run_archive_videos(**context) -> dict:
    settings = ArchiveSettings.from_env()
    candidates = context["ti"].xcom_pull(key="candidates", task_ids="select_candidates") or []
    project_dir = Path(PROJECT_DATA_DIR)

    summary = {"archived": 0, "bytes_freed": 0}
    for candidate in candidates:
        result = archive_one_video(settings, project_dir, candidate["channel_slug"], candidate["video_id"])
        summary["archived"] += 1
        summary["bytes_freed"] += result["bytes_freed"]

    logger.info("nas_archive: run complete — %s", summary)
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

    t0_check_enabled >> t1_select_candidates >> t2_archive_videos
