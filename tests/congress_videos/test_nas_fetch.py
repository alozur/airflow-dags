"""Tests for congress_videos.modules.nas_fetch — pure NAS fetch-back module.

Covers:
- read_marker: happy path, missing marker, malformed JSON, missing/empty/
  unsafe 'synced' entries.
- remove_marker: removes an existing marker, no-op (returns False) when absent.
- is_archived_elsewhere: thin wrapper over nas_archive.is_archived.
- ensure_local_dir: creates a missing local destination directory (and its
  parents), is idempotent when it already exists.
- fetch_rsync_command: exact argv shape, source/destination swapped vs. the
  push side's rsync_command, --mkpath never included.
- verify_fetched: itemized-changes parsing via an injected runner, mirroring
  verify_synced's semantics.
- refresh_retention: only media files get their mtime bumped to `now`;
  non-media sidecars are left untouched; files outside `paths` are untouched.

No Airflow imports, no subprocess, no network — everything I/O-adjacent is
either a real tmp_path filesystem op or an injected callable.
"""

from __future__ import annotations

import os
import shlex
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from congress_videos.modules.nas_archive import ArchiveSettings, ssh_command, write_marker
from congress_videos.modules.nas_fetch import (
    ensure_local_dir,
    fetch_rsync_command,
    is_archived_elsewhere,
    read_marker,
    refresh_retention,
    remove_marker,
    verify_fetched,
)


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
            "NAS_ARCHIVE_PORT": "2222",
            "NAS_ARCHIVE_USER": "nas-archive",
            "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
            "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
        }
    )


def _valid_payload(channel_slug: str = "congreso-es-tv") -> dict:
    return {
        "archived_at": "2026-09-01T04:00:00+00:00",
        "host": "100.64.0.1",
        "root": "/volume1/congress_archive",
        "removed": ["/data/congress_videos/downloads/2026-08-20/abc123/video.mp4"],
        "synced": ["downloads/2026-08-20/abc123", f"{channel_slug}/abc123"],
    }


# ---------------------------------------------------------------------------
# read_marker
# ---------------------------------------------------------------------------


class TestReadMarker:
    def test_raises_file_not_found_when_no_marker(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    def test_reads_valid_marker(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        write_marker(channel_dir, _valid_payload())

        payload = read_marker(tmp_path, "congreso-es-tv", "abc123")

        assert payload["synced"] == ["downloads/2026-08-20/abc123", "congreso-es-tv/abc123"]
        assert payload["host"] == "100.64.0.1"

    def test_malformed_json_raises_value_error(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        channel_dir.mkdir(parents=True)
        (channel_dir / ".nas_archived.json").write_text("{not json")

        with pytest.raises(ValueError, match="Malformed"):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    def test_missing_synced_field_raises(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        del payload["synced"]
        write_marker(channel_dir, payload)

        with pytest.raises(ValueError, match="synced"):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    def test_empty_synced_list_raises(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        payload["synced"] = []
        write_marker(channel_dir, payload)

        with pytest.raises(ValueError, match="synced"):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    def test_synced_not_a_list_raises(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        payload["synced"] = "downloads/2026-08-20/abc123"
        write_marker(channel_dir, payload)

        with pytest.raises(ValueError, match="synced"):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    @pytest.mark.parametrize(
        "unsafe_entry",
        [
            "/etc/passwd",
            "../../etc/passwd",
            "downloads/../../../etc/passwd",
            "thumbnails/abc123",  # not rooted at downloads/ or {channel_slug}/
            "",
        ],
    )
    def test_unsafe_synced_entry_raises(self, tmp_path, unsafe_entry):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        payload["synced"] = [unsafe_entry]
        write_marker(channel_dir, payload)

        with pytest.raises(ValueError):
            read_marker(tmp_path, "congreso-es-tv", "abc123")

    def test_downloads_and_channel_prefixes_are_both_safe(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        write_marker(channel_dir, _valid_payload())

        # Must not raise.
        read_marker(tmp_path, "congreso-es-tv", "abc123")


# ---------------------------------------------------------------------------
# remove_marker
# ---------------------------------------------------------------------------


class TestRemoveMarker:
    def test_removes_existing_marker_and_returns_true(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        write_marker(channel_dir, _valid_payload())

        result = remove_marker(tmp_path, "congreso-es-tv", "abc123")

        assert result is True
        assert not (channel_dir / ".nas_archived.json").exists()

    def test_no_marker_returns_false_without_raising(self, tmp_path):
        result = remove_marker(tmp_path, "congreso-es-tv", "abc123")
        assert result is False


# ---------------------------------------------------------------------------
# is_archived_elsewhere
# ---------------------------------------------------------------------------


class TestIsArchivedElsewhere:
    def test_false_before_any_marker(self, tmp_path):
        assert is_archived_elsewhere(tmp_path, "congreso-es-tv", "abc123") is False

    def test_true_after_write_marker(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        write_marker(channel_dir, _valid_payload())

        assert is_archived_elsewhere(tmp_path, "congreso-es-tv", "abc123") is True


# ---------------------------------------------------------------------------
# ensure_local_dir
# ---------------------------------------------------------------------------


class TestEnsureLocalDir:
    def test_creates_missing_directory_and_parents(self, tmp_path):
        local_path = tmp_path / "downloads" / "2026-08-20" / "abc123"
        assert not local_path.exists()

        result = ensure_local_dir(local_path)

        assert local_path.is_dir()
        assert result == local_path

    def test_is_idempotent_when_directory_already_exists(self, tmp_path):
        local_path = tmp_path / "abc123"
        local_path.mkdir()
        (local_path / "existing.mp4").write_bytes(b"data")

        ensure_local_dir(local_path)

        assert local_path.is_dir()
        assert (local_path / "existing.mp4").exists()  # untouched


# ---------------------------------------------------------------------------
# fetch_rsync_command
# ---------------------------------------------------------------------------


class TestFetchRsyncCommand:
    def test_exact_argv_without_dry_run(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = fetch_rsync_command(settings, "downloads/2026-08-20/abc123", local_path)

        assert command == [
            "rsync",
            "-a",
            "--partial",
            "--itemize-changes",
            "-e",
            shlex.join(ssh_command(settings)),
            "nas-archive@100.64.0.1:/volume1/congress_archive/downloads/2026-08-20/abc123/",
            f"{local_path}/",
        ]

    def test_mkpath_is_never_passed(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = fetch_rsync_command(settings, "downloads/2026-08-20/abc123", local_path)
        assert "--mkpath" not in command

    def test_source_and_destination_are_swapped_vs_push(self, settings, tmp_path):
        from congress_videos.modules.nas_archive import rsync_command

        local_path = tmp_path / "abc123"
        push_command = rsync_command(settings, local_path, "downloads/2026-08-20/abc123")
        pull_command = fetch_rsync_command(settings, "downloads/2026-08-20/abc123", local_path)

        # Push: local source, remote destination. Pull: remote source, local destination.
        assert push_command[-2] == f"{local_path}/"
        assert push_command[-1].startswith("nas-archive@100.64.0.1:")
        assert pull_command[-2].startswith("nas-archive@100.64.0.1:")
        assert pull_command[-1] == f"{local_path}/"

    def test_dry_run_inserts_flag_before_the_ssh_option(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = fetch_rsync_command(settings, "downloads/2026-08-20/abc123", local_path, dry_run=True)
        assert "--dry-run" in command
        assert command.index("--dry-run") < command.index("-e")


# ---------------------------------------------------------------------------
# verify_fetched
# ---------------------------------------------------------------------------


class TestVerifyFetched:
    def test_empty_itemized_output_means_fetched(self, settings, tmp_path):
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731
        assert verify_fetched(settings, "abc123", tmp_path, runner) is True

    def test_receive_line_means_not_fully_fetched(self, settings, tmp_path):
        stdout = ">f+++++++++ video.mp4\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_fetched(settings, "abc123", tmp_path, runner) is False

    def test_create_line_means_not_fully_fetched(self, settings, tmp_path):
        stdout = "cd+++++++++ oradores/\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_fetched(settings, "abc123", tmp_path, runner) is False

    def test_runner_receives_the_dry_run_command(self, settings, tmp_path):
        captured = {}

        def runner(command):
            captured["command"] = command
            return SimpleNamespace(stdout="", returncode=0)

        verify_fetched(settings, "abc123", tmp_path, runner)
        assert "--dry-run" in captured["command"]


# ---------------------------------------------------------------------------
# refresh_retention
# ---------------------------------------------------------------------------


class TestRefreshRetention:
    def test_bumps_media_file_mtime_under_a_directory(self, tmp_path):
        video_dir = tmp_path / "downloads" / "2026-08-20" / "abc123"
        video_dir.mkdir(parents=True)
        video_file = video_dir / "video.mp4"
        video_file.write_bytes(b"data")
        old_time = datetime(2020, 1, 1, tzinfo=UTC).timestamp()
        os.utime(video_file, (old_time, old_time))

        now = datetime(2026, 9, 10, tzinfo=UTC)
        touched = refresh_retention([video_dir], now)

        assert touched == [video_file]
        assert video_file.stat().st_mtime == pytest.approx(now.timestamp())

    def test_leaves_non_media_sidecars_untouched(self, tmp_path):
        video_dir = tmp_path / "congreso-es-tv" / "abc123"
        video_dir.mkdir(parents=True)
        video_file = video_dir / "chapter_video.mp4"
        srt_file = video_dir / "chapter_video.srt"
        video_file.write_bytes(b"data")
        srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nHola\n")
        old_time = datetime(2020, 1, 1, tzinfo=UTC).timestamp()
        os.utime(video_file, (old_time, old_time))
        os.utime(srt_file, (old_time, old_time))

        now = datetime(2026, 9, 10, tzinfo=UTC)
        touched = refresh_retention([video_dir], now)

        assert touched == [video_file]
        assert srt_file.stat().st_mtime == pytest.approx(old_time)

    def test_accepts_a_single_media_file_path(self, tmp_path):
        video_file = tmp_path / "video.mkv"
        video_file.write_bytes(b"data")

        now = datetime(2026, 9, 10, tzinfo=UTC)
        touched = refresh_retention([video_file], now)

        assert touched == [video_file]
        assert video_file.stat().st_mtime == pytest.approx(now.timestamp())

    def test_ignores_a_path_that_does_not_exist(self, tmp_path):
        missing = tmp_path / "nope"
        now = datetime(2026, 9, 10, tzinfo=UTC)
        assert refresh_retention([missing], now) == []
