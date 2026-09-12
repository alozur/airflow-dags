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
- discover_remote_dirs / discover_fetch_source: marker-less fallback — one
  SSH discovery command per source root, stdout parsing/validation, root
  precedence (archive root before legacy root), legacy root pull-only.
- refresh_retention: only media files get their mtime bumped to `now`;
  non-media sidecars are left untouched; files outside `paths` are untouched.
- fetch_lock / ensure_local_video: lock acquisition + staleness (D2), and
  the shared helper's fetched/in_progress/unavailable branches (D1).

No Airflow imports, no subprocess, no network — everything I/O-adjacent is
either a real tmp_path filesystem op or an injected callable.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from congress_videos.modules.nas_archive import ArchiveSettings, ssh_command, write_marker
from congress_videos.modules.nas_fetch import (
    RSYNC_TIMEOUT_SECS,
    FetchLockBusy,
    discover_fetch_source,
    discover_remote_dirs,
    ensure_local_dir,
    ensure_local_video,
    fetch_lock,
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

    def test_bare_video_id_entry_is_safe(self, tmp_path):
        """The legacy top-level scheme records a bare {video_id} entry in
        'synced' (see nas_archive.video_paths) — read_marker must accept an
        exact match for the requested video_id alongside the two prefixes."""
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        payload["synced"] = ["downloads/2026-08-20/abc123", "abc123"]
        write_marker(channel_dir, payload)

        payload_read = read_marker(tmp_path, "congreso-es-tv", "abc123")

        assert payload_read["synced"] == ["downloads/2026-08-20/abc123", "abc123"]

    def test_bare_entry_not_matching_the_requested_video_id_is_unsafe(self, tmp_path):
        channel_dir = tmp_path / "congreso-es-tv" / "abc123"
        payload = _valid_payload()
        payload["synced"] = ["other-video-id"]
        write_marker(channel_dir, payload)

        with pytest.raises(ValueError):
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
        command = fetch_rsync_command(settings, settings.root, "downloads/2026-08-20/abc123", local_path)

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
        command = fetch_rsync_command(settings, settings.root, "downloads/2026-08-20/abc123", local_path)
        assert "--mkpath" not in command

    def test_source_and_destination_are_swapped_vs_push(self, settings, tmp_path):
        from congress_videos.modules.nas_archive import rsync_command

        local_path = tmp_path / "abc123"
        push_command = rsync_command(settings, local_path, "downloads/2026-08-20/abc123")
        pull_command = fetch_rsync_command(settings, settings.root, "downloads/2026-08-20/abc123", local_path)

        # Push: local source, remote destination. Pull: remote source, local destination.
        assert push_command[-2] == f"{local_path}/"
        assert push_command[-1].startswith("nas-archive@100.64.0.1:")
        assert pull_command[-2].startswith("nas-archive@100.64.0.1:")
        assert pull_command[-1] == f"{local_path}/"

    def test_dry_run_inserts_flag_before_the_ssh_option(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        command = fetch_rsync_command(settings, settings.root, "downloads/2026-08-20/abc123", local_path, dry_run=True)
        assert "--dry-run" in command
        assert command.index("--dry-run") < command.index("-e")

    def test_uses_the_given_source_root_instead_of_settings_root(self, settings, tmp_path):
        local_path = tmp_path / "abc123"
        legacy_root = "/volume1/docker/airflow/congress_videos"

        command = fetch_rsync_command(settings, legacy_root, "abc123", local_path)

        assert command[-2] == f"nas-archive@100.64.0.1:{legacy_root}/abc123/"


# ---------------------------------------------------------------------------
# verify_fetched
# ---------------------------------------------------------------------------


class TestVerifyFetched:
    def test_empty_itemized_output_means_fetched(self, settings, tmp_path):
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731
        assert verify_fetched(settings, settings.root, "abc123", tmp_path, runner) is True

    def test_receive_line_means_not_fully_fetched(self, settings, tmp_path):
        stdout = ">f+++++++++ video.mp4\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_fetched(settings, settings.root, "abc123", tmp_path, runner) is False

    def test_create_line_means_not_fully_fetched(self, settings, tmp_path):
        stdout = "cd+++++++++ oradores/\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        assert verify_fetched(settings, settings.root, "abc123", tmp_path, runner) is False

    def test_runner_receives_the_dry_run_command(self, settings, tmp_path):
        captured = {}

        def runner(command):
            captured["command"] = command
            return SimpleNamespace(stdout="", returncode=0)

        verify_fetched(settings, settings.root, "abc123", tmp_path, runner)
        assert "--dry-run" in captured["command"]

    def test_nonzero_returncode_means_not_fetched_even_with_empty_stdout(self, settings, tmp_path):
        """D4 hardening: a dry-run rsync that ERRORS (remote dir absent, tailnet
        down) returns empty stdout, so all() over zero lines is vacuously True
        without the returncode guard — reporting a failed probe as "fetched"."""
        runner = lambda command: SimpleNamespace(stdout="", returncode=23)  # noqa: E731
        assert verify_fetched(settings, settings.root, "abc123", tmp_path, runner) is False


# ---------------------------------------------------------------------------
# discover_remote_dirs / discover_fetch_source
# ---------------------------------------------------------------------------


class TestDiscoverRemoteDirs:
    def test_parses_discovered_directories_and_validates_them(self, settings):
        source_root = settings.root
        stdout = (
            f"{source_root}/downloads/2026-08-20/abc123\n{source_root}/congreso-es-tv/abc123\n{source_root}/abc123\n"
        )
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731

        result = discover_remote_dirs(settings, source_root, "congreso-es-tv", "abc123", runner)

        assert result == ["downloads/2026-08-20/abc123", "congreso-es-tv/abc123", "abc123"]

    def test_empty_stdout_means_nothing_found(self, settings):
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731
        assert discover_remote_dirs(settings, settings.root, "congreso-es-tv", "abc123", runner) == []

    def test_runs_exactly_one_ssh_command(self, settings):
        calls = []

        def runner(command):
            calls.append(command)
            return SimpleNamespace(stdout="", returncode=0)

        discover_remote_dirs(settings, settings.root, "congreso-es-tv", "abc123", runner)

        assert len(calls) == 1

    def test_command_shape_is_ssh_single_trailing_argv(self, settings):
        """No ``sh -c`` wrapper: ssh already runs its trailing argv through the
        remote login shell, so the command is appended directly after
        ``user@host`` as ONE argv element — exactly like
        ``nas_archive.remote_mkdir_command`` does for ``mkdir -p``."""
        captured = {}

        def runner(command):
            captured["command"] = command
            return SimpleNamespace(stdout="", returncode=0)

        discover_remote_dirs(settings, settings.root, "congreso-es-tv", "abc123", runner)

        command = captured["command"]
        base_ssh = ssh_command(settings)
        assert command[: len(base_ssh)] == base_ssh
        assert command[len(base_ssh)] == "nas-archive@100.64.0.1"
        assert len(command) == len(base_ssh) + 2  # user@host + exactly one trailing argv element
        snippet = command[len(base_ssh) + 1]
        assert settings.root in snippet
        assert "congreso-es-tv/abc123" in snippet
        assert "downloads/*/abc123" in snippet
        assert "sh" not in command
        assert "-c" not in command

    def test_video_id_is_validated_before_building_the_command(self, settings):
        runner = MagicMock(side_effect=AssertionError("must not be called"))
        with pytest.raises(ValueError, match="Invalid video_id"):
            discover_remote_dirs(settings, settings.root, "congreso-es-tv", "ab", runner)
        runner.assert_not_called()

    def test_nonzero_returncode_raises(self, settings):
        runner = lambda command: SimpleNamespace(stdout="", returncode=1, stderr="permission denied")  # noqa: E731
        with pytest.raises(ValueError, match="remote discovery failed"):
            discover_remote_dirs(settings, settings.root, "congreso-es-tv", "abc123", runner)

    def test_discovered_path_outside_source_root_raises(self, settings):
        stdout = "/some/other/root/abc123\n"
        runner = lambda command: SimpleNamespace(stdout=stdout, returncode=0)  # noqa: E731
        with pytest.raises(ValueError, match="outside source_root"):
            discover_remote_dirs(settings, settings.root, "congreso-es-tv", "abc123", runner)


class TestDiscoverRemoteDirsRealShell:
    """Real-shell integration: build the discovery argv, then actually
    execute its remote command string locally via ``sh -c``.

    A runner-injection test can only ever assert the argv shape — it never
    catches that ssh naively space-joins a multi-element remote command and
    the remote shell re-parses the result, which is exactly the production
    bug this fix addresses (see ``discover_remote_dirs``'s docstring). This
    class instead runs the produced snippet for real, against a real
    directory tree, so a regression back to the ``["sh", "-c", snippet]``
    shape — or any other quoting break — fails here even without a NAS.
    """

    def _build_discovery_command(
        self, settings: ArchiveSettings, root: Path, channel_slug: str, video_id: str
    ) -> list[str]:
        captured = {}

        def runner(command):
            captured["command"] = command
            return SimpleNamespace(stdout="", returncode=0)

        discover_remote_dirs(settings, str(root), channel_slug, video_id, runner)
        return captured["command"]

    def test_finds_all_three_shapes_and_ignores_decoys(self, settings, tmp_path):
        root = tmp_path / "root"
        channel_slug = "congreso-es-tv"
        video_id = "abc123def"
        other_video_id = "zzz999yyy"

        expected_dirs = (
            root / "downloads" / "2026-02-12" / video_id,
            root / channel_slug / video_id,
            root / video_id,
        )
        for directory in expected_dirs:
            directory.mkdir(parents=True)
        (root / "downloads" / "2026-02-12" / other_video_id).mkdir(parents=True)  # decoy: different video_id

        command = self._build_discovery_command(settings, root, channel_slug, video_id)
        base_ssh = ssh_command(settings)
        assert command[: len(base_ssh)] == base_ssh
        assert command[len(base_ssh)] == f"{settings.user}@{settings.host}"
        assert len(command) == len(base_ssh) + 2  # user@host + exactly one trailing argv element
        remote_command = command[-1]

        result = subprocess.run(["sh", "-c", remote_command], capture_output=True, text=True, check=False)

        assert result.returncode == 0, result.stderr
        lines = result.stdout.splitlines()
        assert set(lines) == {str(directory) for directory in expected_dirs}
        assert len(lines) == len(expected_dirs)

    def test_root_containing_space_and_single_quote_still_works(self, settings, tmp_path):
        root = tmp_path / "weird root's name"
        channel_slug = "congreso-es-tv"
        video_id = "abc123def"

        matched_dir = root / "downloads" / "2026-02-12" / video_id
        matched_dir.mkdir(parents=True)

        command = self._build_discovery_command(settings, root, channel_slug, video_id)
        base_ssh = ssh_command(settings)
        assert len(command) == len(base_ssh) + 2  # user@host + exactly one trailing argv element
        remote_command = command[-1]

        result = subprocess.run(["sh", "-c", remote_command], capture_output=True, text=True, check=False)

        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == [str(matched_dir)]


class TestDiscoverFetchSource:
    def _settings_with_legacy(self, tmp_path) -> ArchiveSettings:
        ssh_dir = _make_ssh_dir(tmp_path)
        return ArchiveSettings.from_env(
            {
                "NAS_ARCHIVE_HOST": "100.64.0.1",
                "NAS_ARCHIVE_USER": "nas-archive",
                "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
                "NAS_FETCH_LEGACY_ROOT": "/volume1/docker/airflow/congress_videos",
                "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
            }
        )

    def test_archive_root_wins_when_it_has_a_match(self, tmp_path):
        settings = self._settings_with_legacy(tmp_path)
        calls = []

        def runner(command):
            calls.append(command)
            return SimpleNamespace(stdout=f"{settings.root}/abc123\n", returncode=0)

        source_root, relative_dirs = discover_fetch_source(settings, "congreso-es-tv", "abc123", runner)

        assert source_root == settings.root
        assert relative_dirs == ["abc123"]
        assert len(calls) == 1  # legacy root never consulted once archive root matched

    def test_falls_back_to_legacy_root_when_archive_root_has_nothing(self, tmp_path):
        settings = self._settings_with_legacy(tmp_path)

        def runner(command):
            snippet = command[-1]
            if settings.legacy_root in snippet:
                return SimpleNamespace(stdout=f"{settings.legacy_root}/congreso-es-tv/abc123\n", returncode=0)
            return SimpleNamespace(stdout="", returncode=0)

        source_root, relative_dirs = discover_fetch_source(settings, "congreso-es-tv", "abc123", runner)

        assert source_root == settings.legacy_root
        assert relative_dirs == ["congreso-es-tv/abc123"]

    def test_raises_when_legacy_root_is_unset_and_archive_root_has_nothing(self, settings):
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731
        with pytest.raises(FileNotFoundError):
            discover_fetch_source(settings, "congreso-es-tv", "abc123", runner)

    def test_raises_when_neither_root_has_anything(self, tmp_path):
        settings = self._settings_with_legacy(tmp_path)
        runner = lambda command: SimpleNamespace(stdout="", returncode=0)  # noqa: E731

        with pytest.raises(FileNotFoundError):
            discover_fetch_source(settings, "congreso-es-tv", "abc123", runner)

    def test_never_writes_to_the_legacy_root_only_reads(self, tmp_path):
        """discover_fetch_source only ever calls the injected runner — a pure
        read via a single SSH remote-command argv element (glob/test), never
        rsync, mkdir, or any other mutating command against either root."""
        settings = self._settings_with_legacy(tmp_path)
        commands = []

        def runner(command):
            commands.append(command)
            if settings.legacy_root in command[-1]:
                return SimpleNamespace(stdout=f"{settings.legacy_root}/abc123\n", returncode=0)
            return SimpleNamespace(stdout="", returncode=0)

        discover_fetch_source(settings, "congreso-es-tv", "abc123", runner)

        for command in commands:
            assert command[-2] == "nas-archive@100.64.0.1"
            assert "for d in" in command[-1]
            assert "rsync" not in command
            assert "mkdir" not in command


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


# ---------------------------------------------------------------------------
# fetch_lock
# ---------------------------------------------------------------------------


def _lock_path(tmp_path: Path, channel_slug: str = "congreso-es-tv", video_id: str = "abc123") -> Path:
    return tmp_path / channel_slug / video_id / ".nas_fetch.lock"


def _write_lock(lock_path: Path, token: str, acquired_at: datetime) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({"token": token, "pid": 999999, "acquired_at": acquired_at.isoformat()}), encoding="utf-8"
    )


class TestFetchLock:
    def test_first_acquisition_creates_lock_and_releases_on_exit(self, tmp_path):
        lock_path = _lock_path(tmp_path)

        with fetch_lock(tmp_path, "congreso-es-tv", "abc123") as token:
            payload = json.loads(lock_path.read_text(encoding="utf-8"))
            assert payload["token"] == token
            assert payload["pid"] == os.getpid()
            assert "acquired_at" in payload

        assert not lock_path.exists()

    def test_contended_lock_raises_busy_without_blocking(self, tmp_path):
        with (
            fetch_lock(tmp_path, "congreso-es-tv", "abc123"),
            pytest.raises(FetchLockBusy),
            fetch_lock(tmp_path, "congreso-es-tv", "abc123"),
        ):
            pass  # pragma: no cover — must never be entered

    @pytest.mark.parametrize(
        ("offset_secs", "expect_busy"),
        [
            pytest.param(RSYNC_TIMEOUT_SECS + 599, True, id="not_yet_stale"),
            pytest.param(RSYNC_TIMEOUT_SECS + 601, False, id="past_stale_threshold"),
        ],
    )
    def test_staleness_threshold_gates_break_and_reacquire(self, tmp_path, offset_secs, expect_busy):
        lock_path = _lock_path(tmp_path)
        acquired_at = datetime(2026, 1, 1, tzinfo=UTC)
        _write_lock(lock_path, "other-holder-token", acquired_at)
        now = acquired_at + timedelta(seconds=offset_secs)

        if expect_busy:
            with pytest.raises(FetchLockBusy), fetch_lock(tmp_path, "congreso-es-tv", "abc123", now=now):
                pass  # pragma: no cover — must never be entered
            return

        with fetch_lock(tmp_path, "congreso-es-tv", "abc123", now=now) as token:
            assert token != "other-holder-token"
            payload = json.loads(lock_path.read_text(encoding="utf-8"))
            assert payload["token"] == token

    def test_release_is_a_noop_when_the_on_disk_token_no_longer_matches(self, tmp_path):
        lock_path = _lock_path(tmp_path)

        with fetch_lock(tmp_path, "congreso-es-tv", "abc123"):
            # A foreign holder broke this (stale, from its view) lock and
            # reacquired it while we still think we own it.
            _write_lock(lock_path, "foreign-token", datetime.now(UTC))

        assert lock_path.exists()
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        assert payload["token"] == "foreign-token"


# ---------------------------------------------------------------------------
# ensure_local_video
# ---------------------------------------------------------------------------


def _write_video_marker(tmp_path: Path, channel_slug: str = "congreso-es-tv") -> Path:
    channel_dir = tmp_path / channel_slug / "abc123"
    write_marker(channel_dir, _valid_payload(channel_slug))
    return channel_dir


def _ok_runner(command):
    return SimpleNamespace(returncode=0, stdout="", stderr="")


class TestEnsureLocalVideo:
    def test_fetched_via_marker_refreshes_retention_and_removes_marker(self, settings, tmp_path):
        channel_dir = _write_video_marker(tmp_path)

        result = ensure_local_video(
            tmp_path, "congreso-es-tv", "abc123", settings, runner=_ok_runner, now=datetime.now(UTC)
        )

        assert result["status"] == "fetched"
        assert result["source"] == "marker"
        assert result["restored"] == ["downloads/2026-08-20/abc123", "congreso-es-tv/abc123"]
        assert not (channel_dir / ".nas_archived.json").exists()

    def test_unavailable_not_on_nas_when_no_marker_and_nothing_discovered(self, settings, tmp_path):
        result = ensure_local_video(tmp_path, "congreso-es-tv", "abc123", settings, runner=_ok_runner)

        assert result == {
            "status": "unavailable",
            "reason": "not_on_nas",
            "video_id": "abc123",
            "channel_slug": "congreso-es-tv",
        }

    def test_unavailable_disabled_when_nas_archive_host_is_unset(self, tmp_path):
        result = ensure_local_video(tmp_path, "congreso-es-tv", "abc123", ArchiveSettings.from_env({}))

        assert result == {
            "status": "unavailable",
            "reason": "disabled",
            "video_id": "abc123",
            "channel_slug": "congreso-es-tv",
        }

    def test_in_progress_when_another_holder_already_has_the_lock(self, settings, tmp_path):
        _write_video_marker(tmp_path)

        def fail_runner(command):  # pragma: no cover — must never be called while locked
            raise AssertionError("runner must not be invoked while the lock is held elsewhere")

        with fetch_lock(tmp_path, "congreso-es-tv", "abc123"):
            result = ensure_local_video(tmp_path, "congreso-es-tv", "abc123", settings, runner=fail_runner)

        assert result == {"status": "in_progress", "video_id": "abc123", "channel_slug": "congreso-es-tv"}


# ---------------------------------------------------------------------------
# fetch_lock / ensure_local_video — unvalidated video_id/channel_slug must
# never reach a filesystem path (path-traversal / absolute-path injection).
# ---------------------------------------------------------------------------


def _snapshot(tmp_path: Path) -> set[Path]:
    return set(tmp_path.rglob("*"))


_UNSAFE_IDS = [
    pytest.param("congreso-es-tv", None, id="absolute_video_id"),  # video_id filled in with a canary below
    pytest.param("congreso-es-tv", "../../evil", id="traversal_video_id"),
    pytest.param("not-a-slug!", "abc123", id="invalid_channel_slug"),
]


class TestPathInjectionDefenceInDepth:
    @pytest.mark.parametrize(("channel_slug", "video_id"), _UNSAFE_IDS)
    def test_fetch_lock_rejects_unsafe_ids_before_touching_disk(self, tmp_path, channel_slug, video_id):
        canary = tmp_path / "canary" / "evil"
        video_id = str(canary) if video_id is None else video_id
        before = _snapshot(tmp_path)

        with pytest.raises(ValueError), fetch_lock(tmp_path, channel_slug, video_id):
            pass  # pragma: no cover — must never be entered

        assert not canary.exists()
        assert _snapshot(tmp_path) == before

    @pytest.mark.parametrize(("channel_slug", "video_id"), _UNSAFE_IDS)
    def test_ensure_local_video_rejects_unsafe_ids_before_touching_disk(
        self, settings, tmp_path, channel_slug, video_id
    ):
        canary = tmp_path / "canary" / "evil"
        video_id = str(canary) if video_id is None else video_id
        before = _snapshot(tmp_path)

        with pytest.raises(ValueError):
            ensure_local_video(tmp_path, channel_slug, video_id, settings)

        assert not canary.exists()
        assert _snapshot(tmp_path) == before
