"""Tests for congress_videos.modules.nas_archive — pure NAS-archival module.

Covers:
- ArchiveSettings.from_env(): disabled default, valid enabled config,
  invalid numeric fields, missing SSH key files when enabled.
- Command builders: ssh_command / remote_mkdir_command / rsync_command
  exact argv shape.
- verify_synced: itemized-changes parsing via an injected runner.
- video_paths: raw-download + channel-subtree discovery, empty-result error.
- prune_local: safety checks (outside project_dir, protected roots, shared
  downloads/<date> parent) and actual deletion behavior.
- write_marker / is_archived round-trip.

No Airflow imports, no subprocess, no network — everything I/O-adjacent is
either a real tmp_path filesystem op or an injected callable.
"""

from __future__ import annotations

import json
import shlex
from pathlib import Path
from types import SimpleNamespace

import pytest

from congress_videos.modules.nas_archive import (
    _PROTECTED_TOP_LEVEL_NAMES,
    MIRROR_ONLY_DIRS,
    ArchiveSettings,
    is_archived,
    mirror_paths,
    prune_local,
    remote_mkdir_command,
    rsync_command,
    ssh_command,
    verify_synced,
    video_paths,
    write_marker,
)


def _make_ssh_dir(tmp_path: Path) -> Path:
    ssh_dir = tmp_path / "nas_sync"
    ssh_dir.mkdir()
    (ssh_dir / "id_ed25519").write_text("fake-key")
    (ssh_dir / "known_hosts").write_text("fake-known-hosts")
    return ssh_dir


def _enabled_env(ssh_dir: Path) -> dict:
    return {
        "NAS_ARCHIVE_HOST": "100.64.0.1",
        "NAS_ARCHIVE_PORT": "2222",
        "NAS_ARCHIVE_USER": "nas-archive",
        "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
        "NAS_ARCHIVE_MIN_AGE_DAYS": "21",
        "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
    }


# ---------------------------------------------------------------------------
# ArchiveSettings.from_env
# ---------------------------------------------------------------------------


class TestArchiveSettingsFromEnv:
    def test_empty_env_is_disabled_with_safe_defaults(self):
        settings = ArchiveSettings.from_env({})
        assert settings.enabled is False
        assert settings.port == 22
        assert settings.min_age_days == 14

    def test_disabled_config_is_valid_even_with_no_ssh_dir(self, tmp_path):
        settings = ArchiveSettings.from_env({"NAS_ARCHIVE_SSH_DIR": str(tmp_path / "missing")})
        assert settings.enabled is False  # validate() must not raise

    def test_enabled_with_valid_ssh_dir_parses_cleanly(self, tmp_path):
        ssh_dir = _make_ssh_dir(tmp_path)
        settings = ArchiveSettings.from_env(_enabled_env(ssh_dir))
        assert settings.enabled is True
        assert settings.host == "100.64.0.1"
        assert settings.port == 2222
        assert settings.user == "nas-archive"
        assert settings.root == "/volume1/congress_archive"
        assert settings.min_age_days == 21
        assert settings.ssh_dir == ssh_dir

    def test_enabled_missing_ssh_key_files_raises(self, tmp_path):
        ssh_dir = tmp_path / "nas_sync"
        ssh_dir.mkdir()
        env = _enabled_env(ssh_dir)
        with pytest.raises(ValueError, match="missing required file"):
            ArchiveSettings.from_env(env)

    def test_enabled_relative_root_raises(self, tmp_path):
        ssh_dir = _make_ssh_dir(tmp_path)
        env = _enabled_env(ssh_dir)
        env["NAS_ARCHIVE_ROOT"] = "relative/path"
        with pytest.raises(ValueError, match="absolute path"):
            ArchiveSettings.from_env(env)

    def test_enabled_empty_user_raises(self, tmp_path):
        ssh_dir = _make_ssh_dir(tmp_path)
        env = _enabled_env(ssh_dir)
        env["NAS_ARCHIVE_USER"] = ""
        with pytest.raises(ValueError, match="NAS_ARCHIVE_USER"):
            ArchiveSettings.from_env(env)

    def test_invalid_port_raises(self, tmp_path):
        env = {"NAS_ARCHIVE_PORT": "not-a-number"}
        with pytest.raises(ValueError, match="NAS_ARCHIVE_PORT"):
            ArchiveSettings.from_env(env)

    def test_invalid_min_age_days_raises(self):
        with pytest.raises(ValueError, match="NAS_ARCHIVE_MIN_AGE_DAYS"):
            ArchiveSettings.from_env({"NAS_ARCHIVE_MIN_AGE_DAYS": "abc"})

    def test_negative_min_age_days_raises(self):
        with pytest.raises(ValueError, match=">= 0"):
            ArchiveSettings.from_env({"NAS_ARCHIVE_MIN_AGE_DAYS": "-1"})


# ---------------------------------------------------------------------------
# Command builders
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path) -> ArchiveSettings:
    ssh_dir = _make_ssh_dir(tmp_path)
    return ArchiveSettings.from_env(_enabled_env(ssh_dir))


class TestSshCommand:
    def test_exact_argv(self, settings):
        command = ssh_command(settings)
        assert command == [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={settings.ssh_dir / 'known_hosts'}",
            "-o",
            "IdentitiesOnly=yes",
            "-i",
            str(settings.ssh_dir / "id_ed25519"),
            "-p",
            "2222",
        ]


class TestRemoteMkdirCommand:
    def test_exact_argv(self, settings):
        command = remote_mkdir_command(settings, "downloads/2026-03-01/abc123")
        assert command == [
            *ssh_command(settings),
            "nas-archive@100.64.0.1",
            "mkdir",
            "-p",
            "/volume1/congress_archive/downloads/2026-03-01/abc123",
        ]


class TestRsyncCommand:
    def test_exact_argv_without_dry_run(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = rsync_command(settings, local_path, "downloads/2026-03-01/abc123")
        assert command == [
            "rsync",
            "-a",
            "--partial",
            "--mkpath",
            "--itemize-changes",
            "-e",
            shlex.join(ssh_command(settings)),
            f"{local_path}/",
            "nas-archive@100.64.0.1:/volume1/congress_archive/downloads/2026-03-01/abc123/",
        ]

    def test_dry_run_inserts_flag_before_the_ssh_option(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = rsync_command(settings, local_path, "downloads/2026-03-01/abc123", dry_run=True)
        assert "--dry-run" in command
        assert command.index("--dry-run") < command.index("-e")


# ---------------------------------------------------------------------------
# verify_synced
# ---------------------------------------------------------------------------


class TestVerifySynced:
    def test_empty_itemized_output_means_synced(self, settings, tmp_path):
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731
        assert verify_synced(settings, tmp_path, "abc123", runner) is True

    def test_send_line_means_not_synced(self, settings, tmp_path):
        stdout = ">f+++++++++ chapter_video.mp4\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_synced(settings, tmp_path, "abc123", runner) is False

    def test_create_line_means_not_synced(self, settings, tmp_path):
        stdout = "cd+++++++++ oradores/\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_synced(settings, tmp_path, "abc123", runner) is False

    def test_informational_only_lines_mean_synced(self, settings, tmp_path):
        stdout = "sending incremental file list\nsent 123 bytes  received 45 bytes\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_synced(settings, tmp_path, "abc123", runner) is True

    def test_runner_receives_the_dry_run_command(self, settings, tmp_path):
        captured = {}

        def runner(command):
            captured["command"] = command
            return SimpleNamespace(stdout="", returncode=0)

        verify_synced(settings, tmp_path, "abc123", runner)
        assert "--dry-run" in captured["command"]


# ---------------------------------------------------------------------------
# video_paths
# ---------------------------------------------------------------------------


class TestVideoPaths:
    def test_raises_when_nothing_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            video_paths(tmp_path, "congreso-es-tv", "abc123")

    def test_finds_raw_download_directories_across_dates(self, tmp_path):
        (tmp_path / "downloads" / "2026-03-01" / "abc123").mkdir(parents=True)
        (tmp_path / "downloads" / "2026-03-05" / "abc123").mkdir(parents=True)
        (tmp_path / "downloads" / "2026-03-01" / "other-video").mkdir(parents=True)

        paths = video_paths(tmp_path, "congreso-es-tv", "abc123")

        assert paths == [
            tmp_path / "downloads" / "2026-03-01" / "abc123",
            tmp_path / "downloads" / "2026-03-05" / "abc123",
        ]

    def test_includes_channel_subtree_when_present(self, tmp_path):
        (tmp_path / "downloads" / "2026-03-01" / "abc123").mkdir(parents=True)
        (tmp_path / "congreso-es-tv" / "abc123").mkdir(parents=True)

        paths = video_paths(tmp_path, "congreso-es-tv", "abc123")

        assert tmp_path / "congreso-es-tv" / "abc123" in paths

    def test_channel_subtree_only_still_returns(self, tmp_path):
        (tmp_path / "congreso-es-tv" / "abc123").mkdir(parents=True)
        paths = video_paths(tmp_path, "congreso-es-tv", "abc123")
        assert paths == [tmp_path / "congreso-es-tv" / "abc123"]


# ---------------------------------------------------------------------------
# mirror_paths / MIRROR_ONLY_DIRS
# ---------------------------------------------------------------------------


class TestMirrorPaths:
    def test_returns_empty_list_when_no_mirror_dirs_exist(self, tmp_path):
        assert mirror_paths(tmp_path) == []

    def test_returns_existing_mirror_dirs(self, tmp_path):
        (tmp_path / "thumbnails").mkdir()

        paths = mirror_paths(tmp_path)

        assert paths == [tmp_path / "thumbnails"]

    def test_skips_mirror_dirs_that_do_not_exist(self, tmp_path):
        # No "thumbnails" directory created — must not raise or fabricate a path.
        assert mirror_paths(tmp_path) == []

    def test_mirror_only_dirs_are_protected_from_pruning(self):
        assert set(MIRROR_ONLY_DIRS) <= _PROTECTED_TOP_LEVEL_NAMES

    def test_prune_local_refuses_a_mirror_dir(self, tmp_path):
        (tmp_path / "thumbnails").mkdir()
        with pytest.raises(ValueError, match="protected root"):
            prune_local([tmp_path / "thumbnails"], tmp_path)
        assert (tmp_path / "thumbnails").exists()


class TestRsyncCommandForMirrorPath:
    def test_rsync_argv_targets_the_mirror_dir(self, settings, tmp_path):
        (tmp_path / "thumbnails").mkdir()
        [local_path] = mirror_paths(tmp_path)

        command = rsync_command(settings, local_path, "thumbnails")

        assert command[-1] == "nas-archive@100.64.0.1:/volume1/congress_archive/thumbnails/"
        assert command[-2] == f"{local_path}/"


# ---------------------------------------------------------------------------
# prune_local
# ---------------------------------------------------------------------------


class TestPruneLocal:
    def test_refuses_path_outside_project_dir(self, tmp_path):
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        with pytest.raises(ValueError, match="outside project_dir"):
            prune_local([outside], project_dir)
        assert outside.exists()

    @pytest.mark.parametrize(
        "protected_relative",
        ["assets", "youtube_tokens", "youtube_cookies.txt", "thumbnails"],
    )
    def test_refuses_protected_top_level_roots(self, tmp_path, protected_relative):
        target = tmp_path / protected_relative
        if protected_relative.endswith(".txt"):
            target.write_text("cookie")
        else:
            target.mkdir()
        with pytest.raises(ValueError, match="protected root"):
            prune_local([target], tmp_path)
        assert target.exists()

    def test_refuses_the_downloads_date_directory_itself(self, tmp_path):
        date_dir = tmp_path / "downloads" / "2026-03-01"
        date_dir.mkdir(parents=True)
        with pytest.raises(ValueError, match="downloads/<date>"):
            prune_local([date_dir], tmp_path)
        assert date_dir.exists()

    def test_rmtree_of_raw_download_video_directory(self, tmp_path):
        video_dir = tmp_path / "downloads" / "2026-03-01" / "abc123"
        video_dir.mkdir(parents=True)
        (video_dir / "video.mp4").write_bytes(b"data")
        (video_dir / "audio_chunks").mkdir()

        removed = prune_local([video_dir], tmp_path)

        assert removed == [str(video_dir)]
        assert not video_dir.exists()

    def test_channel_subtree_deletes_only_mp4_files(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        chapter_dir = channel_dir / "video_chapters" / "7"
        chapter_dir.mkdir(parents=True)
        video_file = chapter_dir / "chapter_video.mp4"
        video_file.write_bytes(b"video")
        srt_file = chapter_dir / "chapter_video.srt"
        srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nHola\n")

        removed = prune_local([channel_dir], tmp_path)

        assert removed == [str(video_file)]
        assert not video_file.exists()
        assert srt_file.exists()

    def test_no_deletion_when_any_path_fails_validation(self, tmp_path):
        video_dir = tmp_path / "downloads" / "2026-03-01" / "abc123"
        video_dir.mkdir(parents=True)
        (video_dir / "video.mp4").write_bytes(b"data")
        outside = tmp_path.parent / "outside-project"
        outside.mkdir(exist_ok=True)
        try:
            with pytest.raises(ValueError):
                prune_local([video_dir, outside], tmp_path)
            assert video_dir.exists()
        finally:
            outside.rmdir()


# ---------------------------------------------------------------------------
# write_marker / is_archived
# ---------------------------------------------------------------------------


class TestMarker:
    def test_is_archived_false_before_write(self, tmp_path):
        assert is_archived(tmp_path / "congreso-es-tv" / "abc123") is False

    def test_write_marker_then_is_archived_true(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = {
            "archived_at": "2026-09-10T04:00:00+00:00",
            "host": "100.64.0.1",
            "root": "/volume1/congress_archive",
            "removed": [str(channel_dir / "video_chapters" / "7" / "chapter_video.mp4")],
            "synced": ["congreso-es-tv/abc123"],
        }

        marker_path = write_marker(channel_dir, payload)

        assert is_archived(channel_dir) is True
        assert json.loads(marker_path.read_text())["host"] == "100.64.0.1"
