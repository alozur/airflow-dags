"""NAS Fetch DAG.

On-demand DAG that pulls one or more previously ``nas_archive``d videos'
raw/derived material back from the NAS onto local disk, so a downstream DAG
(``speaker_turns``, ``trim_proposals``, ``speaker_turn_videos``, ...) can
reprocess a video whose source was already offloaded and pruned locally.
Disabled by default (``NAS_ARCHIVE_HOST`` empty), same as ``nas_archive``:
this DAG reuses the exact same ``ArchiveSettings``/SSH key contract, since it
talks to the same NAS target.

Two sources, chosen automatically per video:

- **Marker mode** (unchanged): when a local ``.nas_archived.json`` marker
  exists, its ``synced`` list says exactly which directories to pull, from
  ``ArchiveSettings.root``.
- **Fallback mode** (no marker — e.g. a video that predates the VPS and was
  never archived from here): a single SSH discovery command checks
  ``ArchiveSettings.root`` first, then, if configured,
  ``ArchiveSettings.legacy_root`` — a read-only pre-migration production
  tree with a different root but the same three possible directory shapes.
  The first root with at least one matching directory wins. There is no
  marker to remove in this mode, and the legacy root is never written to,
  deleted from, or rsync-pushed to — pull only. See
  ``congress_videos/modules/nas_fetch.discover_fetch_source``.

The NAS copy is NEVER deleted or modified by this DAG — only local state
changes: files are pulled back, their mtime is refreshed so the video gets a
full ``NAS_ARCHIVE_MIN_AGE_DAYS`` retention window again (see
``congress_videos/modules/nas_fetch.refresh_retention``), and — marker mode
only — the local ``.nas_archived.json`` marker is removed so ``nas_archive``
treats the video as a fresh candidate once it re-ages. Re-archiving
afterwards is cheap: the NAS copy is unchanged, so the eventual re-push is
close to a no-op sync.

Usage::

    airflow dags trigger nas_fetch --conf '{"video_id": "abc123"}'
    airflow dags trigger nas_fetch --conf '{"video_ids": ["abc123", "def456"]}'
    airflow dags trigger nas_fetch --conf '{"video_id": "abc123", "channel_slug": "congreso-es-tv"}'

Pipeline::

    check_enabled  (ShortCircuitOperator: NAS_ARCHIVE_HOST + ssh key files present)
      → fetch_videos  (per video: read marker, or fall back to remote
                        discovery → per dir: ensure local dir → rsync pull →
                        verify → refresh retention → remove marker if any)

Each requested video is handled independently: a failure fetching or
verifying one video aborts ONLY that video (its marker, if any, is left in
place, so nothing about it appears restored) and is recorded in the run
summary — along with which source it came from (``"marker"``,
``"archive-root"``, or ``"legacy-root"``) — while the remaining requested
videos are still attempted. This differs from ``nas_archive``'s
archive_videos task, which aborts the whole batch on the first failure —
that batch is scheduler-selected and unattended, while this one is an
operator-triggered, usually small, explicit list of videos where one bad id
should not block recovering the others.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL
from congress_videos.modules import nas_fetch
from congress_videos.modules.nas_archive import ArchiveSettings
from congress_videos.modules.nas_fetch import RSYNC_TIMEOUT_SECS, NasFetchError
from utils.env_loader import load_env_if_local

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = "nas_fetch"

_SSH_TIMEOUT_SECS = 30
# congress_videos.modules.nas_fetch.RSYNC_TIMEOUT_SECS is the single source of
# truth (design D7); this local alias keeps the existing call sites unchanged.
_RSYNC_TIMEOUT_SECS = RSYNC_TIMEOUT_SECS


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


def fetch_one_video(settings: ArchiveSettings, project_dir: Path, channel_slug: str, video_id: str) -> dict:
    """Restore one archived video's local material from the NAS.

    Delegates the entire fetch lifecycle to the shared
    ``nas_fetch.ensure_local_video`` (design D1), also used inline by every
    consumer DAG, then converts every non-``fetched`` outcome into an
    exception (design D6a) — an operator-triggered run names an explicit
    ``video_id``, so ``_run_fetch_videos``' ``summary["restored"]`` can
    never contain a video whose material is not actually local.

    Returns:
        A summary dict including ``"source"``: ``"marker"``,
        ``"archive-root"``, or ``"legacy-root"``.

    Raises:
        FileNotFoundError: Neither the marker nor remote discovery found this
            video (``unavailable``/``not_on_nas``).
        NasFetchError: Fetch disabled, a concurrent lock holder
            (``in_progress``), or the rsync pull/verification failed.
        ValueError: A malformed/unsafe marker or a discovery failure other
            than "nothing found".
        All abort before the marker is removed; ``_run_fetch_videos`` catches
        every one of them per video.
    """
    result = nas_fetch.ensure_local_video(project_dir, channel_slug, video_id, settings, runner=_subprocess_runner)
    status = result["status"]

    if status == "fetched":
        logger.info(
            "nas_fetch: video_id=%s restored from %s — %d dir(s) fetched",
            video_id,
            result["source"],
            len(result["restored"]),
        )
        return {
            "video_id": video_id,
            "channel_slug": channel_slug,
            "restored": result["restored"],
            "source": result["source"],
        }

    if status == "unavailable" and result.get("reason") == "not_on_nas":
        # Today's exact message shape (design D6a) — the existing catch tuple
        # and per-video isolation in _run_fetch_videos depend on it unchanged.
        raise FileNotFoundError(
            f"nas_fetch: no remote directories found for video_id={video_id!r} channel_slug={channel_slug!r} "
            "under the archive root or legacy root"
        )

    if status == "unavailable":  # reason == "disabled"
        raise NasFetchError(f"nas_fetch: NAS archive fetch is disabled while restoring video_id={video_id!r}")

    if status == "in_progress":
        raise NasFetchError(
            f"nas_fetch: another fetch already in progress for video_id={video_id!r} — try again next run"
        )

    raise NasFetchError(f"nas_fetch: unexpected ensure_local_video status {status!r} for video_id={video_id!r}")


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
        # NasFetchError (design D6a): ensure_local_video's in_progress/disabled
        # statuses and its own rsync/verify failures all surface through it.
        except (
            FileNotFoundError,
            ValueError,
            AirflowException,
            NasFetchError,
            subprocess.SubprocessError,
            OSError,
        ) as exc:
            logger.error("nas_fetch: video_id=%s aborted — %s", video_id, exc)
            summary["failed"].append({"video_id": video_id, "error": str(exc)})
            continue
        summary["restored"].append(result)

    logger.info(
        "nas_fetch: run complete — %d restored, %d failed",
        len(summary["restored"]),
        len(summary["failed"]),
    )

    # design D6: >=1 video requested, every one failed -> the task MUST fail
    # loudly rather than return a summary that looks like success. A partial
    # failure (some restored) stays a success; its failures remain visible in
    # summary["failed"] without reverting the successful restores.
    if summary["failed"] and not summary["restored"]:
        failures = "; ".join(f"video_id={f['video_id']}: {f['error']}" for f in summary["failed"])
        raise AirflowException(f"nas_fetch: all {len(summary['failed'])} requested video(s) failed — {failures}")

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
