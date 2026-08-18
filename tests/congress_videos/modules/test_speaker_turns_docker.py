"""Tests for the Docker-subprocess diarization backend (PR2).

The subprocess is always faked — no real Docker, pyannote, or network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from congress_videos.modules import speaker_turns_docker as std


class TestBuildDiarizeCommand:
    def test_arg_vector_is_a_list_not_a_shell_string(self):
        cmd = std.build_diarize_command("/tmp/in/chapter.wav", "/tmp/out")
        assert isinstance(cmd, list)
        assert all(isinstance(tok, str) for tok in cmd)

    def test_isolation_and_resource_caps_present(self):
        cmd = std.build_diarize_command("/tmp/in/chapter.wav", "/tmp/out")
        joined = " ".join(cmd)
        assert "--network" in cmd and "none" in cmd
        assert "--memory" in cmd
        assert "--cpuset-cpus" in cmd
        assert "--rm" in cmd
        # WAV mount must be read-only
        assert any(tok.endswith(":/in:ro") for tok in cmd), joined

    def test_input_and_output_paths_wired(self):
        cmd = std.build_diarize_command("/data/in/chapter.wav", "/data/out")
        assert "/in/chapter.wav" in cmd
        assert "/out/changes.json" in cmd
        assert std.DIARIZE_IMAGE in cmd


class TestDockerDiarizeFn:
    def _fake_runner(self, changes):
        def runner(cmd, **kwargs):
            # emulate the container writing changes.json into the -v out mount
            out_dir = None
            for i, tok in enumerate(cmd):
                if tok == "-v" and cmd[i + 1].endswith(":/out"):
                    out_dir = cmd[i + 1].split(":")[0]
            assert out_dir is not None
            Path(out_dir, "changes.json").write_text(
                json.dumps({"speaker_changes": changes})
            )

            class R:
                returncode = 0

            return R()

        return runner

    def test_parses_speaker_changes_from_output(self, tmp_path):
        wav = tmp_path / "in" / "chapter.wav"
        wav.parent.mkdir(parents=True)
        wav.write_bytes(b"RIFF")
        changes = [
            {"start_seconds": 3.0, "from_speaker": "SPEAKER_00",
             "to_speaker": "SPEAKER_01", "confirmed_block_duration_seconds": 5.0},
        ]
        out = std.docker_diarize_fn(
            str(wav), 0.0, runner=self._fake_runner(changes)
        )
        assert out[0]["from_speaker"] == "SPEAKER_00"
        assert out[0]["to_speaker"] == "SPEAKER_01"

    def test_chapter_offset_is_added_to_timestamps(self, tmp_path):
        wav = tmp_path / "in" / "chapter.wav"
        wav.parent.mkdir(parents=True)
        wav.write_bytes(b"RIFF")
        changes = [{"start_seconds": 10.0, "from_speaker": "A",
                    "to_speaker": "B", "confirmed_block_duration_seconds": 2.0}]
        out = std.docker_diarize_fn(
            str(wav), 100.0, runner=self._fake_runner(changes)
        )
        assert out[0]["start_seconds"] == 110.0

    def test_no_shell_true_ever(self, tmp_path):
        wav = tmp_path / "in" / "chapter.wav"
        wav.parent.mkdir(parents=True)
        wav.write_bytes(b"RIFF")
        captured = {}
        base = self._fake_runner([])

        def runner(cmd, **kwargs):
            captured.update(kwargs)
            return base(cmd, **kwargs)

        std.docker_diarize_fn(str(wav), 0.0, runner=runner)
        assert captured.get("shell", False) is False

    def test_nonzero_exit_raises(self, tmp_path):
        wav = tmp_path / "in" / "chapter.wav"
        wav.parent.mkdir(parents=True)
        wav.write_bytes(b"RIFF")

        def runner(cmd, **kwargs):
            class R:
                returncode = 1
                stderr = b"boom"

            return R()

        with pytest.raises(RuntimeError):
            std.docker_diarize_fn(str(wav), 0.0, runner=runner)
