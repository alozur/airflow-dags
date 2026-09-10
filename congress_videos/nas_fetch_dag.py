"""NAS Fetch DAG.

On-demand DAG that pulls one or more previously ``nas_archive``d videos'
raw/derived material back from the NAS onto local disk, so a downstream DAG
(``speaker_turns``, ``trim_proposals``, ``speaker_turn_videos``, ...) can
reprocess a video whose source was already offloaded and pruned locally.
Disabled by default (``NAS_ARCHIVE_HOST`` empty), same as ``nas_archive``:
this DAG reuses the exact same ``ArchiveSettings``/SSH key contract, since it
talks to the same NAS target.

The NAS copy is NEVER deleted or modified by this DAG — only local state
changes: files are pulled back, their mtime is refreshed so the video gets a
full ``NAS_ARCHIVE_MIN_AGE_DAYS`` retention window again (see
``congress_videos/modules/nas_fetch.refresh_retention``), and the local
``.nas_archived.json`` marker is removed so ``nas_archive`` treats the video
as a fresh candidate once it re-ages. Re-archiving afterwards is cheap: the
NAS copy is unchanged, so the eventual re-push is close to a no-op sync.

Usage::

    airflow dags trigger nas_fetch --conf '{"video_id": "abc123"}'
    airflow dags trigger nas_fetch --conf '{"video_ids": ["abc123", "def456"]}'
    airflow dags trigger nas_fetch --conf '{"video_id": "abc123", "channel_slug": "congreso-es-tv"}'

Pipeline::

    check_enabled  (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh key files present)
      → fetch_videos  (per video: read marker → per synced dir: rsync pull →
                        verify → refresh retention → remove marker)

Each requested video is handled independently: a failure fetching or
verifying one video aborts ONLY that video (its marker is left in place, so
nothing about it appears restored) and is recorded in the run summary: the
remaining requested videos are still attempted. This differs from
``nas_archive``'s archive_videos task, which aborts the whole batch on the
first failure — that batch is scheduler-selected and unattended, while this
one is an operator-triggered, usually small, explicit list of videos where
one bad id should not block recovering the others.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL
from congress_videos.modules import nas_fetch
from congress_videos.modules.nas_archive import ArchiveSettings
from utils.env_loader import load_env_if_local

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = "nas_fetch"

_SSH_TIMEOUT_SECS = 30
_RSYNC_TIMEOUT_SECS = 3600  # raw downloads are multi-GB; generous ceiling, matches nas_archive
_ITEMIZE_LOG_LINE_CAP = 50


# ---------------------------------------------------------------------------
# check_enabled
# ---------------------------------------------------------------------------


def _check_enabled(**context) -> bool:
    try:
        settings = ArchiveSettings.from_env()
    except ValueError as exc:
        logger.error("nas_fetch: invalid NAS archive configuration — %s", exc)
        return False
    if not settings.enabled:
        logger.info("NAS fetch disabled (NAS_ARCHIVE_HOST empty)")
        return False
    return True


# ---------------------------------------------------------------------------
# fetch_videos — read marker -> per dir: rsync pull -> verify -> refresh -> remove marker
# ---------------------------------------------------------------------------


def _subprocess_runner(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, timeout=_RSYNC_TIMEOUT_SECS, check=False)


def _log_itemized(stdout: str, video_id: str, remote_relative_dir: str) -> None:
    lines = stdout.splitlines()
    for line in lines[:_ITEMIZE_LOG_LINE_CAP]:
        logger.info("nas_fetch: video_id=%s %s: %s", video_id, remote_relative_dir, line)
    if len(lines) > _ITEMIZE_LOG_LINE_CAP:
        logger.info(
            "nas_fetch: video_id=%s %s: ... (%d more line(s) truncated)",
            video_id,
            remote_relative_dir,
            len(lines) - _ITEMIZE_LOG_LINE_CAP,
        )


def fetch_one_video(settings: ArchiveSettings, project_dir: Path, channel_slug: str, video_id: str) -> dict:
    """Restore one archived video's local material from the NAS.

    Reads the local marker to recover exactly which directories were pushed,
    pulls and verifies each one, refreshes the fetched media files' mtime so
    the video gets a fresh full retention window, then removes the marker.

    Nothing about this video is deleted anywhere: a failure at any step
    aborts before the marker is removed, leaving the video's archived state
    exactly as it was (see the module docstring for why this differs from
    ``nas_archive.archive_one_video``'s whole-batch abort).

    Raises:
        FileNotFoundError: No archive marker exists for this video.
        ValueError: The marker is malformed or unsafe (see
            ``nas_fetch.read_marker``).
        AirflowException: On any rsync failure or post-fetch verification
            mismatch.
        subprocess.SubprocessError: E.g. ``TimeoutExpired`` from
            ``_subprocess_runner`` (rsync exceeding ``_RSYNC_TIMEOUT_SECS``).
        OSError: From ``nas_fetch.remove_marker``/``refresh_retention``
            (filesystem errors touching the marker or fetched media).
        All of the above abort before the marker is removed, same as the
        AirflowException paths above — ``_run_fetch_videos`` catches all of
        them per video (see its docstring).
    """
    marker = nas_fetch.read_marker(project_dir, channel_slug, video_id)
    synced_dirs: list[str] = marker["synced"]

    restored: list[str] = []
    for remote_relative_dir in synced_dirs:
        local_path = project_dir / remote_relative_dir

        rsync_cmd = nas_fetch.fetch_rsync_command(settings, remote_relative_dir, local_path, dry_run=False)
        rsync_result = _subprocess_runner(rsync_cmd)
        _log_itemized(rsync_result.stdout, video_id, remote_relative_dir)
        if rsync_result.returncode != 0:
            raise AirflowException(
                f"nas_fetch: rsync failed (exit={rsync_result.returncode}) for video_id={video_id} "
                f"dir={remote_relative_dir}: {rsync_result.stderr.strip()}"
            )

        if not nas_fetch.verify_fetched(settings, remote_relative_dir, local_path, runner=_subprocess_runner):
            raise AirflowException(
                f"nas_fetch: verification failed for video_id={video_id} dir={remote_relative_dir} — "
                "local copy is not byte-identical to the NAS; marker was NOT removed"
            )
        restored.append(remote_relative_dir)

    touched = nas_fetch.refresh_retention([project_dir / d for d in restored], datetime.now(UTC))
    nas_fetch.remove_marker(project_dir, channel_slug, video_id)

    logger.info(
        "nas_fetch: video_id=%s restored — %d dir(s) fetched, %d media file(s) refreshed, marker removed",
        video_id,
        len(restored),
        len(touched),
    )
    return {"video_id": video_id, "channel_slug": channel_slug, "restored": restored, "media_refreshed": len(touched)}


def _requested_video_ids(conf: dict) -> list[str]:
    """Return the deduplicated, order-preserving list of video_ids from ``conf``.

    Accepts a single ``"video_id"`` and/or a ``"video_ids"`` list — both may
    be given at once (e.g. one extra id alongside a batch).
    """
    ids: list[str] = []
    single = conf.get("video_id")
    if single:
        ids.append(str(single))
    for video_id in conf.get("video_ids") or []:
        candidate = str(video_id)
        if candidate not in ids:
            ids.append(candidate)
    return ids


def _run_fetch_videos(**context) -> dict:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    video_ids = _requested_video_ids(conf)
    if not video_ids:
        raise AirflowException("nas_fetch: conf must include 'video_id' or a non-empty 'video_ids' list")
    channel_slug = conf.get("channel_slug") or DEFAULT_CHANNEL

    settings = ArchiveSettings.from_env()
    project_dir = Path(PROJECT_DATA_DIR)

    summary = {"restored": [], "failed": []}
    for video_id in video_ids:
        try:
            result = fetch_one_video(settings, project_dir, channel_slug, video_id)
        # subprocess.SubprocessError (e.g. TimeoutExpired from the rsync runner) and
        # OSError (e.g. from remove_marker/refresh_retention) must isolate the same
        # as the other three: without them here, one slow/broken video kills the
        # whole batch, contradicting the per-video isolation documented above.
        except (FileNotFoundError, ValueError, AirflowException, subprocess.SubprocessError, OSError) as exc:
            logger.error("nas_fetch: video_id=%s aborted — %s", video_id, exc)
            summary["failed"].append({"video_id": video_id, "error": str(exc)})
            continue
        summary["restored"].append(result)

    logger.info(
        "nas_fetch: run complete — %d restored, %d failed",
        len(summary["restored"]),
        len(summary["failed"]),
    )
    return summary


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description=(
        "Pull one or more nas_archive'd videos' raw/derived material back from the NAS onto local "
        "disk for reprocessing; NAS copy is never modified"
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["congress", "nas", "fetch", "on-demand"],
) as dag:
    t0_check_enabled = ShortCircuitOperator(
        task_id="check_enabled",
        python_callable=_check_enabled,
    )

    t1_fetch_videos = PythonOperator(
        task_id="fetch_videos",
        python_callable=_run_fetch_videos,
    )

    t0_check_enabled >> t1_fetch_videos
