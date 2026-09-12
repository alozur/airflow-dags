"""Tests for congress_videos.modules.nas_reclaim — pure NAS reclaim gate module.

Covers ``select_reclaim_candidates`` (advisory: grace window, lock-free peek,
DB-completeness membership, batch cap) and ``reclaim_one_video``
(re-evaluates lock/grace/NAS-verify gates INSIDE the lock immediately before
``nas_archive.prune_local``, per design D3). No Airflow imports, no
subprocess, no network — I/O-adjacent bits are real ``tmp_path`` ops or an
injected runner/``now``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from congress_videos.modules.nas_archive import ArchiveSettings, is_archived, write_marker
from congress_videos.modules.nas_fetch import fetch_lock
from congress_videos.modules.nas_reclaim import reclaim_one_video, select_reclaim_candidates

CHANNEL = "congreso-es-tv"
VIDEO_ID = "abc123"
NOW = datetime(2026, 9, 12, 12, 0, 0, tzinfo=UTC)


def _make_ssh_dir(tmp_path: Path) -> Path:
    ssh_dir = tmp_path / "nas_sync"
    ssh_dir.mkdir()
    (ssh_dir / "id_ed25519").write_text("fake-key")
    (ssh_dir / "known_hosts").write_text("fake-known-hosts")
    return ssh_dir


@pytest.fixture
def settings(tmp_path) -> ArchiveSettings:
    ssh_dir = _make_ssh_dir(tmp_path)
    return ArchiveSettings.from_env(
        {
            "NAS_ARCHIVE_HOST": "100.64.0.1",
            "NAS_ARCHIVE_USER": "nas-archive",
            "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
            "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
        }
    )


def _write_local_video(
    tmp_path: Path, *, mtime: datetime, channel_slug: str = CHANNEL, video_id: str = VIDEO_ID
) -> Path:
    channel_dir = tmp_path / channel_slug / video_id
    channel_dir.mkdir(parents=True)
    video_file = channel_dir / "video.mp4"
    video_file.write_bytes(b"data")
    srt_file = channel_dir / "video.srt"
    srt_file.write_text("subtitle")
    ts = mtime.timestamp()
    os.utime(video_file, (ts, ts))
    os.utime(srt_file, (ts, ts))
    return channel_dir


def _archive_marker(channel_dir: Path, *, channel_slug: str = CHANNEL, video_id: str = VIDEO_ID) -> None:
    write_marker(
        channel_dir,
        {
            "archived_at": "2026-09-01T00:00:00+00:00",
            "host": "100.64.0.1",
            "root": "/volume1/congress_archive",
            "removed": [],
            "synced": [f"{channel_slug}/{video_id}"],
        },
    )


def _ok_runner(command):
    return SimpleNamespace(returncode=0, stdout="", stderr="")


def _dirty_runner(command):
    return SimpleNamespace(returncode=0, stdout=">f+++++++++ video.mp4\n", stderr="")


def _err_runner(command):
    return SimpleNamespace(returncode=1, stdout="", stderr="ssh: connect failed")


class TestSelectReclaimCandidates:
    def test_all_gates_pass_selects_the_video(self, settings, tmp_path):
        _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, [VIDEO_ID], now=NOW, batch=3)

        assert candidates == [{"channel_slug": CHANNEL, "video_id": VIDEO_ID}]

    def test_excluded_when_video_id_not_in_complete_video_ids(self, settings, tmp_path):
        _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, [], now=NOW, batch=3)

        assert candidates == []

    def test_excluded_within_the_grace_window(self, settings, tmp_path):
        _write_local_video(tmp_path, mtime=NOW - timedelta(hours=1))

        candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, [VIDEO_ID], now=NOW, batch=3)

        assert candidates == []

    def test_excluded_when_no_local_material_exists(self, settings, tmp_path):
        candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, [VIDEO_ID], now=NOW, batch=3)

        assert candidates == []

    def test_excluded_while_the_fetch_lock_is_held(self, settings, tmp_path):
        _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        with fetch_lock(tmp_path, CHANNEL, VIDEO_ID, now=NOW):
            candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, [VIDEO_ID], now=NOW, batch=3)

        assert candidates == []

    def test_batch_cap_limits_candidates_per_run(self, settings, tmp_path):
        video_ids = ["vid001", "vid002", "vid003", "vid004", "vid005"]
        for video_id in video_ids:
            _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13), video_id=video_id)

        candidates = select_reclaim_candidates(settings, tmp_path, CHANNEL, video_ids, now=NOW, batch=3)

        assert [c["video_id"] for c in candidates] == video_ids[:3]


class TestReclaimOneVideo:
    def test_all_gates_pass_deletes_media_and_writes_a_marker(self, settings, tmp_path):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=_ok_runner, now=NOW)

        assert result == {"status": "reclaimed", "video_id": VIDEO_ID, "removed": [str(channel_dir / "video.mp4")]}
        assert not (channel_dir / "video.mp4").exists()
        assert is_archived(channel_dir)

    def test_existing_marker_is_kept_not_rewritten(self, settings, tmp_path):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))
        _archive_marker(channel_dir)

        result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=_ok_runner, now=NOW)

        assert result["status"] == "reclaimed"
        marker = (channel_dir / ".nas_archived.json").read_text(encoding="utf-8")
        assert "2026-09-01T00:00:00+00:00" in marker

    def test_blocked_by_grace_window_inside_the_lock(self, settings, tmp_path):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=1))

        result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=_ok_runner, now=NOW)

        assert result == {"status": "blocked", "reason": "grace_window", "video_id": VIDEO_ID}
        assert (channel_dir / "video.mp4").exists()

    @pytest.mark.parametrize("runner", [_dirty_runner, _err_runner], ids=["dirty_itemized", "errored_returncode"])
    def test_blocked_when_nas_verification_fails(self, settings, tmp_path, runner):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=runner, now=NOW)

        assert result == {"status": "blocked", "reason": "unverified", "video_id": VIDEO_ID}
        assert (channel_dir / "video.mp4").exists()

    def test_skipped_when_the_fetch_lock_is_already_held(self, settings, tmp_path):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))

        with fetch_lock(tmp_path, CHANNEL, VIDEO_ID, now=NOW):
            result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=_ok_runner, now=NOW)

        assert result == {"status": "skipped", "reason": "locked", "video_id": VIDEO_ID}
        assert (channel_dir / "video.mp4").exists()

    def test_thumbnails_and_sidecar_files_are_never_touched(self, settings, tmp_path):
        channel_dir = _write_local_video(tmp_path, mtime=NOW - timedelta(hours=13))
        thumbnails_dir = tmp_path / "thumbnails" / "yt123"
        thumbnails_dir.mkdir(parents=True)
        (thumbnails_dir / "cover.png").write_bytes(b"png")

        result = reclaim_one_video(settings, tmp_path, CHANNEL, VIDEO_ID, runner=_ok_runner, now=NOW)

        assert result["status"] == "reclaimed"
        assert (thumbnails_dir / "cover.png").exists()
        assert (channel_dir / "video.srt").exists()
        assert not (channel_dir / "video.mp4").exists()
