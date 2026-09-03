"""Tests for congress_videos.modules.materialization_executor (PR2).

All I/O collaborators are mocked — no real ffmpeg, no filesystem side effects.

Test organisation:
  TestSingleIntervalStreamCopy     — non-AV1, one keep interval → -c copy
  TestSingleIntervalAV1Reencode    — AV1 source, one interval → re-encode
  TestMultiIntervalExcision        — N intervals → N segment cuts + concat join
  TestTempFileCleanup              — temp segments deleted on failure
  TestGroupedContinuousInterval    — grouped short turns (one interval, non-AV1) → stream copy, no concat
  TestTimeoutComputation           — timeout computed from total kept duration
"""

from __future__ import annotations

import os
import subprocess
from unittest.mock import MagicMock

import pytest

from congress_videos.modules.materialization import KeepInterval, MaterializationPlan
from congress_videos.modules.materialization_executor import execute_plan

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SRC = "/data/downloads/2026-01-01/vid1/source.mp4"
_OUT = "/data/downloads/2026-01-01/vid1/turns/1/turn_video.mp4"


# Common fixture: monkeypatch os.makedirs so tests never try to create /data
def _patch_makedirs(monkeypatch):
    monkeypatch.setattr(
        "congress_videos.modules.materialization_executor.os.makedirs",
        lambda path, **kw: None,
    )


def _plan(keep_intervals, needs_reencode=False, turn_ids=(1,), chapter_id=10):
    """Factory: build a MaterializationPlan with the given keep_intervals."""
    return MaterializationPlan(
        turn_ids=tuple(turn_ids),
        chapter_id=chapter_id,
        keep_intervals=tuple(keep_intervals),
        output_turn_id=turn_ids[0],
        needs_reencode=needs_reencode,
    )


def _ki(start, end):
    return KeepInterval(start=start, end=end)


# ---------------------------------------------------------------------------
# 2.1 Single keep interval, non-AV1 → stream copy (-c copy)
# ---------------------------------------------------------------------------


class TestSingleIntervalStreamCopy:
    """Single keep interval + h264 source → build_ffmpeg_cut_cmd(reencode=False)."""

    def test_uses_stream_copy_for_h264(self, monkeypatch):
        plan = _plan([_ki(100.0, 300.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        assert "-c" in cmd
        copy_idx = cmd.index("-c")
        assert cmd[copy_idx + 1] == "copy"

    def test_output_path_is_the_target(self, monkeypatch):
        plan = _plan([_ki(100.0, 300.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        cmd = captured_cmds[0]
        assert _OUT in cmd

    def test_no_temp_concat_file_created(self, monkeypatch):
        """Single-interval path must not use -f concat."""
        plan = _plan([_ki(100.0, 300.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        # Only one subprocess call, no -f concat used
        assert all("-f" not in cmd for cmd in captured_cmds)


# ---------------------------------------------------------------------------
# 2.2 Single keep interval, AV1 source → re-encode (no -c copy)
# ---------------------------------------------------------------------------


class TestSingleIntervalAV1Reencode:
    """AV1 source → force_reencode=True → build_ffmpeg_cut_cmd(reencode=True)."""

    def test_av1_uses_reencode_flags(self, monkeypatch):
        plan = _plan([_ki(0.0, 600.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "av1",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="av1")

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        # Re-encode uses libx264, not -c copy
        assert "libx264" in cmd

    def test_av1_does_not_use_copy(self, monkeypatch):
        plan = _plan([_ki(0.0, 600.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "av1",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="av1")

        cmd = captured_cmds[0]
        # No bare -c copy (stream copy): either -c is absent, or its value != copy
        if "-c" in cmd:
            c_idx = cmd.index("-c")
            assert cmd[c_idx + 1] != "copy"


class TestSingleIntervalNonH264Reencode:
    """Conservative policy: only h264 stream-copies; every other non-AV1 codec
    (e.g. h265) still re-encodes rather than risking an invalid stream copy."""

    def test_h265_forces_reencode_not_copy(self, monkeypatch):
        plan = _plan([_ki(0.0, 600.0)], needs_reencode=False)
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h265",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h265")

        cmd = captured_cmds[0]
        # Re-encode path: libx264 present, no bare -c copy.
        assert "libx264" in cmd
        if "-c" in cmd:
            c_idx = cmd.index("-c")
            assert cmd[c_idx + 1] != "copy"


# ---------------------------------------------------------------------------
# 2.3 Multi-interval (excision): N segments + 1 concat join
# ---------------------------------------------------------------------------


class TestMultiIntervalExcision:
    """Two keep intervals → 2 segment cuts (re-encode) + 1 concat join (-c copy)."""

    def _plan_two_intervals(self):
        # Turn from 100s to 700s, trim excised 300–350 → keep [100,300] + [350,700]
        return _plan(
            [_ki(100.0, 300.0), _ki(350.0, 700.0)],
            needs_reencode=True,
        )

    def test_n_segment_cuts_plus_one_concat(self, monkeypatch, tmp_path):
        plan = self._plan_two_intervals()
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.tempfile",
            MagicMock(mkdtemp=lambda **kw: str(tmp_path)),
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        # 2 segment cuts + 1 concat → total 3 subprocess.run calls
        assert len(captured_cmds) == 3

    def test_segment_cuts_use_reencode(self, monkeypatch, tmp_path):
        plan = self._plan_two_intervals()
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.tempfile",
            MagicMock(mkdtemp=lambda **kw: str(tmp_path)),
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        # The first two commands (segment cuts) must use libx264, not -c copy
        for cmd in captured_cmds[:2]:
            assert "libx264" in cmd

    def test_concat_join_uses_stream_copy(self, monkeypatch, tmp_path):
        plan = self._plan_two_intervals()
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.tempfile",
            MagicMock(mkdtemp=lambda **kw: str(tmp_path)),
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        # Final concat command uses -f concat
        concat_cmd = captured_cmds[-1]
        assert "-f" in concat_cmd
        f_idx = concat_cmd.index("-f")
        assert concat_cmd[f_idx + 1] == "concat"
        # And stream copies the segments
        assert "-c" in concat_cmd
        c_idx = concat_cmd.index("-c")
        assert concat_cmd[c_idx + 1] == "copy"


# ---------------------------------------------------------------------------
# 2.4 Temp files cleaned up on ffmpeg failure
# ---------------------------------------------------------------------------


class TestTempFileCleanup:
    """On ffmpeg failure (non-zero returncode), temp segment files must be deleted."""

    def test_temp_segments_deleted_on_failure(self, monkeypatch, tmp_path):
        plan = _plan(
            [_ki(100.0, 300.0), _ki(350.0, 700.0)],
            needs_reencode=True,
        )
        call_count = [0]
        deleted_paths = []
        original_remove = os.remove
        original_path_exists = os.path.exists

        def tracking_remove(path):
            deleted_paths.append(path)
            # Don't call real remove; the file may not exist in tests
            pass

        def tracking_exists(path):
            # Say all segment files exist so cleanup runs
            if "seg_" in str(path) or "concat_list" in str(path):
                return True
            return original_path_exists(path)

        def fake_run(cmd, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            # First segment cut succeeds, second fails
            if call_count[0] == 1:
                result.returncode = 0
                return result
            result.returncode = 1
            result.stderr = "ffmpeg error"
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.tempfile",
            MagicMock(mkdtemp=lambda **kw: str(tmp_path)),
        )
        _patch_makedirs(monkeypatch)
        monkeypatch.setattr("congress_videos.modules.materialization_executor.os.remove", tracking_remove)
        monkeypatch.setattr("congress_videos.modules.materialization_executor.os.path.exists", tracking_exists)

        with pytest.raises(RuntimeError):
            execute_plan(plan, _SRC, _OUT, codec="h264")

        # Cleanup was triggered: os.remove was called for segment paths
        removed_segs = [p for p in deleted_paths if "seg_" in p]
        assert len(removed_segs) >= 1, f"Expected segment cleanup calls, got: {deleted_paths}"

    def test_cleanup_happens_before_reraise(self, monkeypatch, tmp_path):
        """Cleanup must occur even when the concat step fails."""
        plan = _plan(
            [_ki(100.0, 300.0), _ki(350.0, 700.0)],
            needs_reencode=True,
        )
        call_count = [0]
        deleted_paths = []
        original_path_exists = os.path.exists

        def tracking_remove(path):
            deleted_paths.append(path)

        def tracking_exists(path):
            if "seg_" in str(path) or "concat_list" in str(path):
                return True
            return original_path_exists(path)

        def fake_run(cmd, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] <= 2:
                result.returncode = 0
                return result
            # Concat step fails
            result.returncode = 1
            result.stderr = "concat failed"
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.tempfile",
            MagicMock(mkdtemp=lambda **kw: str(tmp_path)),
        )
        _patch_makedirs(monkeypatch)
        monkeypatch.setattr("congress_videos.modules.materialization_executor.os.remove", tracking_remove)
        monkeypatch.setattr("congress_videos.modules.materialization_executor.os.path.exists", tracking_exists)

        with pytest.raises(RuntimeError):
            execute_plan(plan, _SRC, _OUT, codec="h264")

        # Both segment paths AND concat_list were cleaned
        removed_segs = [p for p in deleted_paths if "seg_" in p]
        removed_list = [p for p in deleted_paths if "concat_list" in p]
        assert len(removed_segs) == 2, f"Expected 2 segment cleanups, got: {removed_segs}"
        assert len(removed_list) == 1, f"Expected concat_list cleanup, got: {removed_list}"


# ---------------------------------------------------------------------------
# 2.5 Grouped short turns: single continuous interval, non-AV1 → stream copy
# ---------------------------------------------------------------------------


class TestGroupedContinuousInterval:
    """Single continuous interval (grouped short turns, needs_reencode=False) uses stream copy."""

    def test_grouped_turns_stream_copy(self, monkeypatch):
        # Grouped short turns produce one continuous interval, no concat
        plan = _plan(
            [_ki(50.0, 250.0)],
            needs_reencode=False,
            turn_ids=(3, 4),
        )
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        # Stream copy
        assert "-c" in cmd
        c_idx = cmd.index("-c")
        assert cmd[c_idx + 1] == "copy"

    def test_grouped_turns_no_concat_invocation(self, monkeypatch):
        plan = _plan(
            [_ki(50.0, 250.0)],
            needs_reencode=False,
            turn_ids=(3, 4),
        )
        captured_cmds = []

        def fake_run(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        # Only one subprocess call, no -f concat
        assert all("-f" not in cmd for cmd in captured_cmds)


# ---------------------------------------------------------------------------
# 2.5b Timeout computed from total kept duration
# ---------------------------------------------------------------------------


class TestTimeoutComputation:
    """Timeout passed to subprocess.run is derived from total kept duration."""

    def test_timeout_scales_with_duration(self, monkeypatch):
        # 200s keep interval → compute_ffmpeg_timeout(200) = 120 + 8*200 = 1720
        plan = _plan([_ki(0.0, 200.0)], needs_reencode=False)
        captured_timeouts = []

        def fake_run(cmd, timeout=None, **kwargs):
            captured_timeouts.append(timeout)
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        assert len(captured_timeouts) == 1
        # timeout for 200s clip ≥ 120 (base) + 8*200 = 1720, capped at 3600
        assert captured_timeouts[0] >= 1720

    def test_timeout_capped_at_3600(self, monkeypatch):
        # 500s keep interval → 120 + 8*500 = 4120 → capped at 3600
        plan = _plan([_ki(0.0, 500.0)], needs_reencode=False)
        captured_timeouts = []

        def fake_run(cmd, timeout=None, **kwargs):
            captured_timeouts.append(timeout)
            result = MagicMock()
            result.returncode = 0
            return result

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        _patch_makedirs(monkeypatch)

        execute_plan(plan, _SRC, _OUT, codec="h264")

        assert captured_timeouts[0] == 3600
