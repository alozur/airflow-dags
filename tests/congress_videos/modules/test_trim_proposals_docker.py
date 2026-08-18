"""Tests for the Docker-subprocess YAMNet wrapper (PR2 — threat matrix).

The subprocess is always faked — no real Docker, YAMNet model, or network.
These four threat-matrix tests are RED until trim_proposals_docker.py exists.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


class TestArgVectorShape:
    """3.1 — Shell injection: arg-list subprocess.run, no shell=True, --network none."""

    def _build_cmd(self, monkeypatch, wav_path="/tmp/turn_7.wav", offset=0.0):
        import congress_videos.modules.trim_proposals_docker as tpd
        monkeypatch.importorskip  # ensure fresh import
        return tpd.build_yamnet_command(wav_path, offset)

    def test_arg_vector_is_a_list_not_shell_string(self, monkeypatch):
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn_7.wav", 0.0)
        assert isinstance(cmd, list)
        assert all(isinstance(tok, str) for tok in cmd)

    def test_network_none_present(self, monkeypatch):
        """3.4 — Network egress: --network none in arg vector."""
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn_7.wav", 0.0)
        assert "--network" in cmd
        idx = cmd.index("--network")
        assert cmd[idx + 1] == "none"

    def test_wav_path_is_positional_arg_not_shell_interpolated(self, monkeypatch):
        """WAV path must appear as a positional string token, not inside a shell command."""
        import congress_videos.modules.trim_proposals_docker as tpd
        wav = "/data/turns/turn_42.wav"
        cmd = tpd.build_yamnet_command(wav, 0.0)
        # The path (or its basename) must appear as a distinct token
        assert any(wav in tok or wav.split("/")[-1] in tok for tok in cmd)

    def test_no_shell_true_in_runner_call(self, monkeypatch):
        """3.1 — docker_yamnet_fn must call runner without shell=True."""
        import congress_videos.modules.trim_proposals_docker as tpd

        captured_kwargs: dict = {}
        intervals = [{"start": 5.0, "end": 20.0, "max_score": 0.9}]

        def fake_runner(cmd, **kwargs):
            captured_kwargs.update(kwargs)

            class R:
                returncode = 0
                stdout = json.dumps({"applause_intervals": intervals}).encode()
                stderr = b""

            return R()

        tpd.docker_yamnet_fn("/tmp/turn.wav", 0.0, runner=fake_runner)
        assert captured_kwargs.get("shell", False) is False


class TestUntrustedInputValidation:
    """3.2 — Untrusted input: non-int turn_id rejected before subprocess."""

    def test_build_command_with_non_int_offset_raises(self):
        """Passing a non-numeric offset raises TypeError/ValueError before shell."""
        import congress_videos.modules.trim_proposals_docker as tpd
        with pytest.raises((TypeError, ValueError)):
            tpd.build_yamnet_command("/tmp/t.wav", "NOT_A_NUMBER")  # type: ignore[arg-type]

    def test_runner_not_called_when_offset_invalid(self, monkeypatch):
        """The runner must not be called when the input is invalid."""
        import congress_videos.modules.trim_proposals_docker as tpd
        runner = MagicMock()
        with pytest.raises((TypeError, ValueError)):
            tpd.docker_yamnet_fn("/tmp/t.wav", "bad", runner=runner)  # type: ignore[arg-type]
        runner.assert_not_called()


class TestResourceExhaustionCaps:
    """3.3 — Resource exhaustion: --memory and --cpuset-cpus present in arg vector."""

    def test_memory_cap_in_arg_vector(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn.wav", 0.0)
        assert "--memory" in cmd

    def test_cpuset_cpus_in_arg_vector(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn.wav", 0.0)
        assert "--cpuset-cpus" in cmd

    def test_rm_flag_present_to_avoid_container_accumulation(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn.wav", 0.0)
        assert "--rm" in cmd


class TestNetworkEgress:
    """3.4 — Network egress (may overlap 3.1): --network none in arg vector."""

    def test_network_none_in_command(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        cmd = tpd.build_yamnet_command("/tmp/turn.wav", 0.0)
        joined = " ".join(cmd)
        assert "--network none" in joined


class TestDockerYamnetFn:
    """Green-path functional tests: JSON parsing, offset, error handling."""

    def _fake_runner_returning(self, intervals):
        def runner(cmd, **kwargs):
            class R:
                returncode = 0
                stdout = json.dumps({"applause_intervals": intervals}).encode()
                stderr = b""

            return R()

        return runner

    def test_parses_applause_intervals_from_stdout(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        intervals = [{"start": 10.0, "end": 25.0, "max_score": 0.88}]
        out = tpd.docker_yamnet_fn(
            "/tmp/t.wav", 0.0, runner=self._fake_runner_returning(intervals)
        )
        assert len(out) == 1
        assert out[0]["start"] == 10.0
        assert out[0]["max_score"] == 0.88

    def test_offset_added_to_timestamps(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        intervals = [{"start": 5.0, "end": 20.0, "max_score": 0.75}]
        out = tpd.docker_yamnet_fn(
            "/tmp/t.wav", 100.0, runner=self._fake_runner_returning(intervals)
        )
        assert out[0]["start"] == 105.0
        assert out[0]["end"] == 120.0

    def test_nonzero_exit_raises_runtime_error(self):
        import congress_videos.modules.trim_proposals_docker as tpd

        def runner(cmd, **kwargs):
            class R:
                returncode = 1
                stdout = b""
                stderr = b"container crashed"

            return R()

        with pytest.raises(RuntimeError):
            tpd.docker_yamnet_fn("/tmp/t.wav", 0.0, runner=runner)

    def test_empty_intervals_returns_empty_list(self):
        import congress_videos.modules.trim_proposals_docker as tpd
        out = tpd.docker_yamnet_fn(
            "/tmp/t.wav", 0.0, runner=self._fake_runner_returning([])
        )
        assert out == []
