"""Tests for congress_videos.modules.video_editor and video_editor_config.

Strict TDD — all tests are written BEFORE the production modules exist.
First run must produce ImportError failures.

Test groups:
    TestVideoEditorConfig     — T-01 config module (REQ-02)
    TestEscapeDrawtext        — T-02 _escape_drawtext (REQ-04)
    TestParseTime             — T-03 _parse_time helper
    TestBuildDrawtextFilter   — T-04 build_drawtext_filter (REQ-05)
    TestBuildFfmpegCmd        — T-05 build_ffmpeg_drawtext_cmd (REQ-06)
    TestResolveSourcePath     — T-06 _resolve_source_path + _default_output_path (REQ-03, REQ-10)
    TestValidateEditorInput   — T-07 validate_editor_input (REQ-01, REQ-02, REQ-08)
    TestApplyOverlays         — T-08 apply_overlays (REQ-07)
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

_MINIMAL_STYLE = {
    "fontfile": "/fonts/LiberationSans-Bold.ttf",
    "fontsize": 42,
    "fontcolor": "white",
    "box": 1,
    "boxcolor": "black@0.5",
    "boxborderw": 8,
    "x": "(w-text_w)/2",
    "y": "h-th-60",
}

_MINIMAL_DOMAIN_CFG = {
    "tipos": {
        "extracto_sesion": _MINIMAL_STYLE,
    }
}

_VALID_OVERLAY = {
    "tipo": "extracto_sesion",
    "tiempo_inicio": 0.0,
    "tiempo_fin": 5.0,
    "titulo": "Extracto",
}

_VALID_CONF = {
    "domain": "congreso",
    "source_path": "/data/chapter_video.mp4",
    "overlays": [_VALID_OVERLAY],
}


# ---------------------------------------------------------------------------
# T-01: TestVideoEditorConfig — REQ-02
# ---------------------------------------------------------------------------


class TestVideoEditorConfig:
    """T-01: Config module must export get_domain_config and ConfigError."""

    def test_config_error_importable(self) -> None:
        """ConfigError must be importable from the config module."""
        from congress_videos.config.video_editor_config import ConfigError  # noqa: F401

    def test_get_domain_config_importable(self) -> None:
        """get_domain_config must be importable from the config module."""
        from congress_videos.config.video_editor_config import get_domain_config  # noqa: F401

    def test_congreso_domain_returns_dict(self) -> None:
        """get_domain_config('congreso') must return a dict with at least one tipo key."""
        from congress_videos.config.video_editor_config import get_domain_config

        result = get_domain_config("congreso")
        assert isinstance(result, dict)
        assert "tipos" in result
        assert len(result["tipos"]) >= 1

    def test_congreso_extracto_sesion_has_required_style_fields(self) -> None:
        """The extracto_sesion tipo must have all required Pillow style keys."""
        from congress_videos.config.video_editor_config import get_domain_config

        result = get_domain_config("congreso")
        assert "extracto_sesion" in result["tipos"]
        style = result["tipos"]["extracto_sesion"]
        for key in (
            "renderer",
            "fontfile",
            "fontfile_sub",
            "fontsize_title",
            "fontsize_sub",
            "bg_color",
            "accent_color",
            "title_color",
            "width_pct",
            "height",
        ):
            assert key in style, f"Missing required style key: {key}"
        assert style["renderer"] == "pillow"

    def test_unknown_domain_raises_config_error(self) -> None:
        """get_domain_config('unknown_domain') must raise ConfigError."""
        from congress_videos.config.video_editor_config import ConfigError, get_domain_config

        with pytest.raises(ConfigError, match="unknown_domain"):
            get_domain_config("unknown_domain")

    def test_config_error_message_lists_available_domains(self) -> None:
        """ConfigError message must list available domains."""
        from congress_videos.config.video_editor_config import ConfigError, get_domain_config

        with pytest.raises(ConfigError) as exc_info:
            get_domain_config("bogus")
        assert "congreso" in str(exc_info.value)


# ---------------------------------------------------------------------------
# T-02: TestEscapeDrawtext — REQ-04
# ---------------------------------------------------------------------------


class TestEscapeDrawtext:
    """T-02: _escape_drawtext must escape ffmpeg drawtext metacharacters in the correct order."""

    def test_colon_is_escaped(self) -> None:
        """Colons must be escaped as \\:."""
        from congress_videos.modules.video_editor import _escape_drawtext

        result = _escape_drawtext("hello: world")
        assert r"\:" in result

    def test_single_quote_is_escaped(self) -> None:
        """Single quotes must be escaped as \\'."""
        from congress_videos.modules.video_editor import _escape_drawtext

        result = _escape_drawtext("it's")
        assert r"\'" in result

    def test_colon_and_single_quote_together(self) -> None:
        """Sesión: 'especial' — colon and quote both escaped."""
        from congress_videos.modules.video_editor import _escape_drawtext

        result = _escape_drawtext("Sesión: 'especial'")
        assert r"\:" in result
        assert r"\'" in result
        # Spanish chars pass through
        assert "Sesión" in result

    def test_backslash_escaped_before_percent(self) -> None:
        """Backslash must be escaped first, then percent — order matters."""
        from congress_videos.modules.video_editor import _escape_drawtext

        # Input has a literal backslash then a percent sign
        result = _escape_drawtext("50% del\\total")
        # backslash → \\; percent → \%
        assert r"\%" in result
        assert r"\\" in result

    def test_spanish_chars_pass_through(self) -> None:
        """ñ, ó, á, ¿ and other Spanish chars must not be escaped."""
        from congress_videos.modules.video_editor import _escape_drawtext

        text = "señor Ñoño: ¿cómo está?"
        result = _escape_drawtext(text)
        assert "ñ" in result
        assert "Ñ" in result
        assert "ó" in result
        assert "á" in result
        assert "¿" in result
        # But colon is escaped
        assert r"\:" in result

    def test_curly_braces_escaped(self) -> None:
        """{key} must have both braces escaped."""
        from congress_videos.modules.video_editor import _escape_drawtext

        result = _escape_drawtext("{key}")
        assert r"\{" in result
        assert r"\}" in result

    def test_newline_to_literal_backslash_n(self) -> None:
        """Actual newline character must become the literal two-char sequence \\n."""
        from congress_videos.modules.video_editor import _escape_drawtext

        result = _escape_drawtext("line1\nline2")
        assert "\\n" in result
        assert "\n" not in result

    def test_adversarial_injection_cant_break_single_quote_clause(self) -> None:
        """Text containing ':fontfile=/etc/passwd cannot break the single-quoted clause."""
        from congress_videos.modules.video_editor import _escape_drawtext

        malicious = "':fontfile=/etc/passwd"
        result = _escape_drawtext(malicious)
        # The single quote must be escaped so it cannot end the text='...' clause
        assert "\\'" in result
        # The colon must be escaped so it cannot introduce a new option
        assert "\\:" in result


# ---------------------------------------------------------------------------
# T-03: TestParseTime — design spec
# ---------------------------------------------------------------------------


class TestParseTime:
    """T-03: _parse_time normalises int/float seconds and SRT timestamp strings."""

    def test_int_passthrough(self) -> None:
        """Integer seconds must be returned as float."""
        from congress_videos.modules.video_editor import _parse_time

        assert _parse_time(10) == 10.0

    def test_float_passthrough(self) -> None:
        """Float seconds pass through unchanged."""
        from congress_videos.modules.video_editor import _parse_time

        assert _parse_time(5.5) == 5.5

    def test_srt_string_with_milliseconds(self) -> None:
        """HH:MM:SS,mmm must be converted to total seconds."""
        from congress_videos.modules.video_editor import _parse_time

        assert _parse_time("00:01:30,000") == pytest.approx(90.0)

    def test_srt_string_without_milliseconds(self) -> None:
        """HH:MM:SS without milliseconds must also be parsed correctly."""
        from congress_videos.modules.video_editor import _parse_time

        assert _parse_time("00:00:05") == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# T-04: TestBuildDrawtextFilter — REQ-05
# ---------------------------------------------------------------------------


class TestBuildDrawtextFilter:
    """T-04: build_drawtext_filter must produce a valid comma-joined drawtext string."""

    def test_single_overlay_contains_enable_expr(self) -> None:
        """Single overlay must contain enable='between(t,...)' with correct times."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [{**_VALID_OVERLAY}]
        result = build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)
        assert "enable='between(t,0.0,5.0)'" in result

    def test_single_overlay_contains_titulo(self) -> None:
        """Single overlay filter must embed the escaped titulo text."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [{"tipo": "extracto_sesion", "tiempo_inicio": 0.0, "tiempo_fin": 3.0, "titulo": "Sesión"}]
        result = build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)
        assert "Sesi" in result  # Spanish chars pass through

    def test_single_overlay_contains_required_style_keys(self) -> None:
        """Filter string must include fontfile, fontsize, fontcolor, box, boxcolor, x, y."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [{**_VALID_OVERLAY}]
        result = build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)
        assert "fontfile=" in result
        assert "fontsize=" in result
        assert "fontcolor=" in result
        assert "box=1" in result
        assert "boxcolor=" in result
        assert "x=" in result
        assert "y=" in result

    def test_two_overlays_comma_joined(self) -> None:
        """Two overlays must produce exactly one comma between the two drawtext segments."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [
            {"tipo": "extracto_sesion", "tiempo_inicio": 0.0, "tiempo_fin": 3.0, "titulo": "First"},
            {"tipo": "extracto_sesion", "tiempo_inicio": 5.0, "tiempo_fin": 8.0, "titulo": "Second"},
        ]
        result = build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)
        # Should have exactly 2 drawtext= segments
        segments = result.split(",drawtext=")
        assert len(segments) == 2, f"Expected 2 segments, got {len(segments)}: {result[:200]}"

    def test_descripcion_joined_with_newline_escape(self) -> None:
        """When descripcion is present, filter text must contain both joined with \\n."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [
            {
                "tipo": "extracto_sesion",
                "tiempo_inicio": 0.0,
                "tiempo_fin": 5.0,
                "titulo": "Titulo",
                "descripcion": "Descripcion extra",
            }
        ]
        result = build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)
        # Both texts should be present and joined with the drawtext newline escape
        assert "Titulo" in result
        assert "Descripcion" in result
        # The literal \\n escape (backslash-n) must appear between them
        assert "\\n" in result

    def test_unknown_tipo_raises(self) -> None:
        """An overlay with a tipo not in domain_cfg must raise KeyError or ValueError."""
        from congress_videos.modules.video_editor import build_drawtext_filter

        overlays = [{"tipo": "nonexistent_tipo", "tiempo_inicio": 0.0, "tiempo_fin": 5.0, "titulo": "X"}]
        with pytest.raises((KeyError, ValueError)):
            build_drawtext_filter(overlays, _MINIMAL_DOMAIN_CFG)


# ---------------------------------------------------------------------------
# T-05: TestBuildFfmpegCmd — REQ-06
# ---------------------------------------------------------------------------


class TestBuildFfmpegCmd:
    """T-05: build_ffmpeg_drawtext_cmd must return a pure argv list."""

    def test_returns_list_not_string(self) -> None:
        """The result must be a list[str], not a single shell string."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", "drawtext=text='Hello'")
        assert isinstance(result, list)
        for token in result:
            assert isinstance(token, str)

    def test_contains_overwrite_flag(self) -> None:
        """Command must include '-y' to overwrite the output."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", "drawtext=text='X'")
        assert "-y" in result

    def test_contains_input_file(self) -> None:
        """Command must include '-i' followed by the source path."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/data/in.mp4", "/data/out.mp4", "drawtext=text='X'")
        assert "-i" in result
        idx = result.index("-i")
        assert result[idx + 1] == "/data/in.mp4"

    def test_contains_vf_filter(self) -> None:
        """Command must include '-vf' followed by the filter string."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        filter_str = "drawtext=text='Hello World'"
        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", filter_str)
        assert "-vf" in result
        idx = result.index("-vf")
        assert result[idx + 1] == filter_str

    def test_contains_video_codec_flags(self) -> None:
        """Command must include -c:v libx264 -preset veryfast -crf 20."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", "drawtext=text='X'")
        assert "-c:v" in result
        assert "libx264" in result
        assert "-preset" in result
        assert "veryfast" in result
        assert "-crf" in result
        assert "20" in result

    def test_contains_audio_copy_flag(self) -> None:
        """Command must include -c:a copy to avoid re-encoding audio."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", "drawtext=text='X'")
        assert "-c:a" in result
        idx = result.index("-c:a")
        assert result[idx + 1] == "copy"

    def test_output_path_is_last_token(self) -> None:
        """Output path must be the last token in the command list."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/data/in.mp4", "/data/out.mp4", "drawtext=text='X'")
        assert result[-1] == "/data/out.mp4"

    def test_no_shell_string(self) -> None:
        """The list must not contain any token that looks like a joined shell string."""
        from congress_videos.modules.video_editor import build_ffmpeg_drawtext_cmd

        result = build_ffmpeg_drawtext_cmd("/in.mp4", "/out.mp4", "drawtext=text='X'")
        # Each token should be a distinct argument — no spaces except inside the filter
        for token in result:
            if token.startswith("drawtext="):
                continue  # filter string may contain spaces
            assert " " not in token, f"Shell-string token found: {token!r}"


# ---------------------------------------------------------------------------
# T-06: TestResolveSourcePath + TestDefaultOutputPath — REQ-03, REQ-10
# ---------------------------------------------------------------------------


class TestResolveSourcePath:
    """T-06: _resolve_source_path resolves explicit path or video_id/chapter_id derivation."""

    def test_explicit_source_path_returned_verbatim(self, mocker) -> None:
        """When source_path is provided and exists, it must be returned verbatim."""
        from congress_videos.modules.video_editor import _resolve_source_path

        mocker.patch("os.path.exists", return_value=True)
        conf = {"source_path": "/data/video.mp4"}
        result = _resolve_source_path(conf)
        assert result == "/data/video.mp4"

    def test_video_id_and_chapter_id_derive_path(self, mocker) -> None:
        """video_id + chapter_id must derive the canonical path."""
        from congress_videos.modules.video_editor import _resolve_source_path

        mocker.patch("os.path.exists", return_value=True)
        conf = {"video_id": "V1", "chapter_id": "C1"}
        result = _resolve_source_path(conf)
        assert "V1" in result
        assert "C1" in result
        assert result.endswith("chapter_video.mp4")

    def test_missing_file_raises_file_not_found_error(self, mocker) -> None:
        """When the resolved path does not exist, FileNotFoundError must be raised."""
        from congress_videos.modules.video_editor import _resolve_source_path

        mocker.patch("os.path.exists", return_value=False)
        conf = {"source_path": "/nonexistent/video.mp4"}
        with pytest.raises(FileNotFoundError):
            _resolve_source_path(conf)


class TestDefaultOutputPath:
    """T-06b: _default_output_path appends _edited before the extension."""

    def test_edited_suffix_added(self) -> None:
        """chapter_video.mp4 → chapter_video_edited.mp4 in the same dir."""
        from congress_videos.modules.video_editor import _default_output_path

        result = _default_output_path("/data/V1/C1/chapter_video.mp4")
        assert result == "/data/V1/C1/chapter_video_edited.mp4"

    def test_extension_preserved(self) -> None:
        """The output extension must match the source extension."""
        from congress_videos.modules.video_editor import _default_output_path

        result = _default_output_path("/tmp/test.mp4")
        assert result.endswith(".mp4")
        assert "_edited" in result


# ---------------------------------------------------------------------------
# T-07: TestValidateEditorInput — REQ-01, REQ-02, REQ-08
# ---------------------------------------------------------------------------


class TestValidateEditorInput:
    """T-07: validate_editor_input must enforce all conf requirements."""

    def test_happy_path_does_not_raise(self, mocker) -> None:
        """A fully valid conf must complete without raising."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        validate_editor_input(_VALID_CONF)

    def test_missing_domain_raises_value_error(self, mocker) -> None:
        """Missing 'domain' key must raise ValueError naming domain."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {k: v for k, v in _VALID_CONF.items() if k != "domain"}
        with pytest.raises(ValueError, match="domain"):
            validate_editor_input(conf)

    def test_empty_overlays_raises_value_error(self, mocker) -> None:
        """An empty overlays list must raise ValueError."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {**_VALID_CONF, "overlays": []}
        with pytest.raises(ValueError):
            validate_editor_input(conf)

    def test_overlay_missing_titulo_raises_value_error(self, mocker) -> None:
        """An overlay without 'titulo' must raise ValueError identifying the field."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {
            **_VALID_CONF,
            "overlays": [{"tipo": "extracto_sesion", "tiempo_inicio": 0.0, "tiempo_fin": 5.0}],
        }
        with pytest.raises(ValueError, match="titulo"):
            validate_editor_input(conf)

    def test_tiempo_fin_equal_tiempo_inicio_raises_value_error(self, mocker) -> None:
        """tiempo_fin == tiempo_inicio must raise ValueError."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {
            **_VALID_CONF,
            "overlays": [{"tipo": "extracto_sesion", "tiempo_inicio": 5.0, "tiempo_fin": 5.0, "titulo": "X"}],
        }
        with pytest.raises(ValueError):
            validate_editor_input(conf)

    def test_both_source_path_and_video_id_raises_value_error(self, mocker) -> None:
        """Providing both source_path and video_id must raise ValueError."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {**_VALID_CONF, "video_id": "V1", "chapter_id": "C1"}
        with pytest.raises(ValueError):
            validate_editor_input(conf)

    def test_unknown_domain_raises_config_error(self, mocker) -> None:
        """An unknown domain must propagate ConfigError from get_domain_config."""
        from congress_videos.config.video_editor_config import ConfigError
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {**_VALID_CONF, "domain": "nonexistent"}
        with pytest.raises(ConfigError):
            validate_editor_input(conf)

    def test_font_file_missing_raises_file_not_found_error(self, mocker) -> None:
        """When the font file for a tipo is absent, FileNotFoundError must be raised."""
        from congress_videos.modules.video_editor import validate_editor_input

        # Font file does not exist
        mocker.patch("os.path.exists", return_value=False)
        with pytest.raises(FileNotFoundError):
            validate_editor_input(_VALID_CONF)

    def test_unknown_tipo_in_overlay_raises(self, mocker) -> None:
        """An overlay with a tipo not in the domain config must raise ValueError or KeyError."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {
            **_VALID_CONF,
            "overlays": [{"tipo": "unknown_tipo", "tiempo_inicio": 0.0, "tiempo_fin": 5.0, "titulo": "X"}],
        }
        with pytest.raises((ValueError, KeyError)):
            validate_editor_input(conf)

    def test_second_overlay_error_uses_zero_based_index(self, mocker) -> None:
        """Overlay indices in error messages are 0-based: the SECOND overlay
        (list index 1) must report 'Overlay[1]', not 'Overlay[2]'."""
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {
            **_VALID_CONF,
            "overlays": [
                dict(_VALID_OVERLAY),
                {"tipo": "extracto_sesion", "tiempo_inicio": 0.0, "tiempo_fin": 5.0},
            ],
        }
        with pytest.raises(ValueError, match=r"Overlay\[1\]"):
            validate_editor_input(conf)

    def test_neither_source_path_nor_id_pair_raises_value_error(self, mocker) -> None:
        """Omitting both source forms must raise ValueError.

        The XOR source rule has two violating sides. The "both present" side is
        covered by test_both_source_path_and_video_id_raises_value_error; this
        locks the "neither present" side.
        """
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {k: v for k, v in _VALID_CONF.items() if k != "source_path"}
        with pytest.raises(
            ValueError,
            match=r"One of 'source_path' or \('video_id' \+ 'chapter_id'\) is required\.",
        ):
            validate_editor_input(conf)

    def test_missing_overlays_key_raises_value_error(self, mocker) -> None:
        """An absent 'overlays' key is distinct from an empty 'overlays' list.

        test_empty_overlays_raises_value_error covers the empty-list case; this
        locks the key-entirely-absent case and its distinct message.
        """
        from congress_videos.modules.video_editor import validate_editor_input

        mocker.patch("os.path.exists", return_value=True)
        conf = {k: v for k, v in _VALID_CONF.items() if k != "overlays"}
        with pytest.raises(ValueError, match=r"Missing required conf key: 'overlays'"):
            validate_editor_input(conf)


# ---------------------------------------------------------------------------
# T-08: TestApplyOverlays — REQ-07
# ---------------------------------------------------------------------------


class TestApplyOverlays:
    """T-08: apply_overlays orchestrates the full ffmpeg call."""

    @pytest.fixture()
    def mock_video_editor_subprocess(self, mocker):
        """Patch subprocess.run in the video_editor module."""
        completed = MagicMock()
        completed.returncode = 0
        completed.stdout = ""
        completed.stderr = ""
        return mocker.patch(
            "congress_videos.modules.video_editor.subprocess.run",
            return_value=completed,
        )

    @pytest.fixture()
    def mock_get_source_duration(self, mocker):
        """Patch _get_source_duration to return a fixed 300.0s duration."""
        return mocker.patch(
            "congress_videos.modules.video_editor._get_source_duration",
            return_value=300.0,
        )

    def test_success_returns_output_dict(self, mock_video_editor_subprocess, mock_get_source_duration) -> None:
        """On returncode=0, apply_overlays must return {output_path, success: True}."""
        from congress_videos.modules.video_editor import apply_overlays

        result = apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[_VALID_OVERLAY],
            domain_cfg=_MINIMAL_DOMAIN_CFG,
        )
        assert result["success"] is True
        assert result["output_path"] == "/data/out.mp4"

    def test_non_zero_exit_raises_runtime_error(self, mock_video_editor_subprocess, mock_get_source_duration) -> None:
        """On returncode=1, apply_overlays must raise RuntimeError with stderr text."""
        mock_video_editor_subprocess.return_value.returncode = 1
        mock_video_editor_subprocess.return_value.stderr = "error msg from ffmpeg"

        from congress_videos.modules.video_editor import apply_overlays

        with pytest.raises(RuntimeError, match="error msg from ffmpeg"):
            apply_overlays(
                source_path="/data/in.mp4",
                output_path="/data/out.mp4",
                overlays=[_VALID_OVERLAY],
                domain_cfg=_MINIMAL_DOMAIN_CFG,
            )

    def test_tiempo_fin_beyond_duration_emits_warning(
        self, mock_video_editor_subprocess, mock_get_source_duration, caplog
    ) -> None:
        """tiempo_fin > source_duration must emit a warning but must not raise."""
        from congress_videos.modules.video_editor import apply_overlays

        long_overlay = {**_VALID_OVERLAY, "tiempo_fin": 9999.0}
        with caplog.at_level(logging.WARNING):
            result = apply_overlays(
                source_path="/data/in.mp4",
                output_path="/data/out.mp4",
                overlays=[long_overlay],
                domain_cfg=_MINIMAL_DOMAIN_CFG,
            )
        # Must not raise; must return success
        assert result["success"] is True
        # Warning must have been emitted
        assert any("9999" in msg or "duration" in msg.lower() for msg in caplog.messages), (
            f"Expected a warning about tiempo_fin > duration, got: {caplog.messages}"
        )

    def test_compute_ffmpeg_timeout_called_with_duration(
        self, mock_video_editor_subprocess, mock_get_source_duration, mocker
    ) -> None:
        """compute_ffmpeg_timeout must be called with the source duration."""
        from congress_videos.modules.video_editor import apply_overlays

        mock_timeout = mocker.patch(
            "congress_videos.modules.video_editor.compute_ffmpeg_timeout",
            return_value=999,
        )

        apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[_VALID_OVERLAY],
            domain_cfg=_MINIMAL_DOMAIN_CFG,
        )
        mock_timeout.assert_called_once_with(300.0)


# ---------------------------------------------------------------------------
# Shared helpers for Pillow tests
# ---------------------------------------------------------------------------

_W, _H = 320, 180  # small frame keeps render tests fast

_PILLOW_OVERLAY_SPEAKER = {
    "tipo": "speaker_id",
    "tiempo_inicio": 1.0,
    "tiempo_fin": 8.0,
    "titulo": "María García López",
    "descripcion": "Portavoz · Grupo Socialista",
}
_PILLOW_OVERLAY_CITA = {
    "tipo": "cita_destacada",
    "tiempo_inicio": 2.0,
    "tiempo_fin": 15.0,
    "titulo": "Esta ley afecta a tres millones de familias",
    "descripcion": "— María García López",
}
_PILLOW_OVERLAY_URGENTE = {
    "tipo": "urgente",
    "tiempo_inicio": 0.5,
    "tiempo_fin": 10.0,
    "titulo": "El Congreso rechaza los Presupuestos",
    "descripcion": "187 en contra · 163 a favor",
}
_PILLOW_OVERLAY_DATO = {
    "tipo": "dato_contexto",
    "tiempo_inicio": 3.0,
    "tiempo_fin": 18.0,
    "titulo": "63%",
    "descripcion": "del gasto es en pensiones",
}

_PILLOW_OVERLAY_EXTRACTO = {
    "tipo": "extracto_sesion",
    "tiempo_inicio": 1.0,
    "tiempo_fin": 9.0,
    "titulo": "Extracto de la Sesión Plenaria",
    "descripcion": "15 de enero de 2026",
}

_ALL_PILLOW_OVERLAYS = [
    _PILLOW_OVERLAY_EXTRACTO,
    _PILLOW_OVERLAY_SPEAKER,
    _PILLOW_OVERLAY_CITA,
    _PILLOW_OVERLAY_URGENTE,
    _PILLOW_OVERLAY_DATO,
]

_PILLOW_TIPO_NAMES = ["extracto_sesion", "speaker_id", "cita_destacada", "urgente", "dato_contexto"]

# Minimal domain cfg with a fake pillow tipo (avoids font-file checks in unit tests)
_MINIMAL_PILLOW_STYLE = {
    "renderer": "pillow",
    "fontfile": "/nonexistent/Bold.ttf",
    "fontfile_sub": "/nonexistent/Regular.ttf",
    "fontsize_title": 24,
    "fontsize_sub": 14,
    "bg_color": (10, 20, 80, 200),
    "accent_color": (0, 100, 255, 255),
    "title_color": (255, 255, 255, 255),
    "sub_color": (180, 210, 255, 240),
    "width_pct": 0.46,
    "height": 80,
    "margin_x": 20,
    "margin_y": 20,
}
_MINIMAL_PILLOW_DOMAIN_CFG = {
    "tipos": {
        "speaker_id": _MINIMAL_PILLOW_STYLE,
    }
}


# ---------------------------------------------------------------------------
# T-09: TestVideoEditorConfigPilloTipos — Pillow tipo registration
# ---------------------------------------------------------------------------


class TestVideoEditorConfigPilloTipos:
    """T-09: The 4 Pillow tipos must be registered in the congreso domain config."""

    def _cfg(self):
        from congress_videos.config.video_editor_config import get_domain_config

        return get_domain_config("congreso")

    @pytest.mark.parametrize("tipo", _PILLOW_TIPO_NAMES)
    def test_pillow_tipo_exists_in_congreso(self, tipo: str) -> None:
        """Each Pillow tipo must be present under congreso.tipos."""
        assert tipo in self._cfg()["tipos"]

    @pytest.mark.parametrize("tipo", _PILLOW_TIPO_NAMES)
    def test_pillow_tipo_has_renderer_pillow(self, tipo: str) -> None:
        """Each Pillow tipo must declare renderer='pillow'."""
        style = self._cfg()["tipos"][tipo]
        assert style.get("renderer") == "pillow"

    @pytest.mark.parametrize("tipo", _PILLOW_TIPO_NAMES)
    def test_pillow_tipo_has_fontfile(self, tipo: str) -> None:
        """Each Pillow tipo must define a fontfile key."""
        assert "fontfile" in self._cfg()["tipos"][tipo]

    @pytest.mark.parametrize("tipo", _PILLOW_TIPO_NAMES)
    def test_pillow_tipo_has_bg_and_accent_colors(self, tipo: str) -> None:
        """Each Pillow tipo must define bg_color and accent_color as tuples."""
        style = self._cfg()["tipos"][tipo]
        assert "bg_color" in style
        assert "accent_color" in style
        assert isinstance(style["bg_color"], tuple)
        assert isinstance(style["accent_color"], tuple)

    def test_urgente_has_label_bg_color(self) -> None:
        """urgente must have a distinct label_bg_color for the badge."""
        assert "label_bg_color" in self._cfg()["tipos"]["urgente"]

    def test_urgente_label_is_config_driven(self) -> None:
        """urgente must carry a 'label' key so the badge text is not hardcoded."""
        assert "label" in self._cfg()["tipos"]["urgente"]

    def test_dato_contexto_has_header_color(self) -> None:
        """dato_contexto must have a header_color for the label."""
        assert "header_color" in self._cfg()["tipos"]["dato_contexto"]

    def test_dato_contexto_label_is_config_driven(self) -> None:
        """dato_contexto must carry a 'label' key so the header text is not hardcoded."""
        assert "label" in self._cfg()["tipos"]["dato_contexto"]


# ---------------------------------------------------------------------------
# T-10: TestRenderPillowOverlay — render dispatch and output shape
# ---------------------------------------------------------------------------


class TestRenderPillowOverlay:
    """T-10: render_pillow_overlay must return an RGBA PIL Image of the correct size."""

    @pytest.mark.parametrize("overlay", _ALL_PILLOW_OVERLAYS, ids=_PILLOW_TIPO_NAMES)
    def test_returns_rgba_image_of_correct_size(self, overlay: dict) -> None:
        """Each renderer must return a PIL Image in RGBA mode sized (W, H)."""
        from congress_videos.config.video_editor_config import get_domain_config
        from congress_videos.modules.video_editor import render_pillow_overlay

        cfg = get_domain_config("congreso")
        img = render_pillow_overlay(overlay, cfg, _W, _H)
        assert img.size == (_W, _H)
        assert img.mode == "RGBA"

    @pytest.mark.parametrize("overlay", _ALL_PILLOW_OVERLAYS, ids=_PILLOW_TIPO_NAMES)
    def test_image_is_not_fully_transparent(self, overlay: dict) -> None:
        """The rendered image must contain at least one non-transparent pixel."""
        from congress_videos.config.video_editor_config import get_domain_config
        from congress_videos.modules.video_editor import render_pillow_overlay

        cfg = get_domain_config("congreso")
        img = render_pillow_overlay(overlay, cfg, _W, _H)
        pixels = list(img.getdata())
        assert any(px[3] > 0 for px in pixels), "All pixels are transparent — nothing was drawn."

    @pytest.mark.parametrize(
        "overlay,tipo",
        [
            ({**_PILLOW_OVERLAY_EXTRACTO, "descripcion": None}, "extracto_sesion"),
            ({**_PILLOW_OVERLAY_SPEAKER, "descripcion": None}, "speaker_id"),
            ({**_PILLOW_OVERLAY_CITA, "descripcion": None}, "cita_destacada"),
            ({**_PILLOW_OVERLAY_URGENTE, "descripcion": None}, "urgente"),
            ({**_PILLOW_OVERLAY_DATO, "descripcion": None}, "dato_contexto"),
        ],
        ids=_PILLOW_TIPO_NAMES,
    )
    def test_renders_without_descripcion(self, overlay: dict, tipo: str) -> None:
        """Each renderer must succeed when 'descripcion' is absent or None."""
        from congress_videos.config.video_editor_config import get_domain_config
        from congress_videos.modules.video_editor import render_pillow_overlay

        cfg = get_domain_config("congreso")
        img = render_pillow_overlay(overlay, cfg, _W, _H)
        assert img.size == (_W, _H)

    def test_unknown_tipo_raises_key_error(self) -> None:
        """A tipo with no registered Pillow renderer must raise KeyError."""
        from congress_videos.config.video_editor_config import get_domain_config
        from congress_videos.modules.video_editor import render_pillow_overlay

        cfg = get_domain_config("congreso")
        overlay = {**_PILLOW_OVERLAY_SPEAKER, "tipo": "tipo_inexistente"}
        with pytest.raises(KeyError, match="tipo_inexistente"):
            render_pillow_overlay(overlay, cfg, _W, _H)

    def test_render_dispatch_importable(self) -> None:
        """render_pillow_overlay must be importable from the module."""
        from congress_videos.modules.video_editor import render_pillow_overlay  # noqa: F401


# ---------------------------------------------------------------------------
# T-11: TestBuildFfmpegPillowCmd — command builder
# ---------------------------------------------------------------------------


class TestBuildFfmpegPillowCmd:
    """T-11: build_ffmpeg_pillow_cmd must build a valid filter_complex ffmpeg argv."""

    def test_single_overlay_has_source_and_png_inputs(self) -> None:
        """A single-overlay command must have exactly 2 -i inputs (video + PNG)."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd("/src.mp4", "/out.mp4", [("/o.png", 2.0, 10.0)])
        assert cmd.count("-i") == 2
        assert "/src.mp4" in cmd
        assert "/o.png" in cmd

    def test_single_overlay_contains_between_expr(self) -> None:
        """filter_complex must contain enable='between(t,start,end)'."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd("/src.mp4", "/out.mp4", [("/o.png", 2.0, 10.0)])
        fc = cmd[cmd.index("-filter_complex") + 1]
        assert "between(t,2.0,10.0)" in fc

    def test_two_overlays_chains_filter(self) -> None:
        """Two overlays must produce a chained filter_complex with an intermediate label."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd(
            "/src.mp4",
            "/out.mp4",
            [("/a.png", 0.0, 5.0), ("/b.png", 6.0, 12.0)],
        )
        fc = cmd[cmd.index("-filter_complex") + 1]
        # First segment must produce an intermediate label
        assert "[v0]" in fc
        # Both timing expressions present
        assert "between(t,0.0,5.0)" in fc
        assert "between(t,6.0,12.0)" in fc

    def test_output_path_is_last_positional_arg(self) -> None:
        """The output path must be the last element of the argv list."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd("/src.mp4", "/out.mp4", [("/o.png", 1.0, 5.0)])
        assert cmd[-1] == "/out.mp4"

    def test_command_starts_with_ffmpeg(self) -> None:
        """The command must start with 'ffmpeg'."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd("/src.mp4", "/out.mp4", [("/o.png", 1.0, 5.0)])
        assert cmd[0] == "ffmpeg"

    def test_libx264_codec_present(self) -> None:
        """Output must be encoded with libx264."""
        from congress_videos.modules.video_editor import build_ffmpeg_pillow_cmd

        cmd = build_ffmpeg_pillow_cmd("/src.mp4", "/out.mp4", [("/o.png", 1.0, 5.0)])
        assert "libx264" in cmd


# ---------------------------------------------------------------------------
# T-12: TestGetSourceDimensions — ffprobe dimensions helper
# ---------------------------------------------------------------------------


class TestGetSourceDimensions:
    """T-12: _get_source_dimensions must parse ffprobe output or return None on failure."""

    def test_returns_width_height_tuple(self, mocker) -> None:
        """Valid ffprobe output '1280,720' must return (1280, 720)."""
        from congress_videos.modules.video_editor import _get_source_dimensions

        completed = MagicMock()
        completed.returncode = 0
        completed.stdout = "1280,720\n"
        mocker.patch("congress_videos.modules.video_editor.subprocess.run", return_value=completed)

        result = _get_source_dimensions("/any/video.mp4")
        assert result == (1280, 720)

    def test_returns_none_on_non_zero_returncode(self, mocker) -> None:
        """A non-zero ffprobe exit must return None (benign degradation)."""
        from congress_videos.modules.video_editor import _get_source_dimensions

        completed = MagicMock()
        completed.returncode = 1
        completed.stdout = ""
        mocker.patch("congress_videos.modules.video_editor.subprocess.run", return_value=completed)

        result = _get_source_dimensions("/any/video.mp4")
        assert result is None

    def test_returns_none_on_subprocess_exception(self, mocker) -> None:
        """A subprocess exception must return None (benign degradation)."""
        from congress_videos.modules.video_editor import _get_source_dimensions

        mocker.patch(
            "congress_videos.modules.video_editor.subprocess.run",
            side_effect=OSError("ffprobe not found"),
        )
        result = _get_source_dimensions("/any/video.mp4")
        assert result is None


# ---------------------------------------------------------------------------
# T-13: TestApplyOverlaysPillow — apply_overlays with Pillow renderer
# ---------------------------------------------------------------------------


class TestApplyOverlaysPillow:
    """T-13: apply_overlays must route pillow overlays through the PNG composite path."""

    @pytest.fixture()
    def mock_subprocess(self, mocker):
        completed = MagicMock()
        completed.returncode = 0
        completed.stdout = ""
        completed.stderr = ""
        return mocker.patch(
            "congress_videos.modules.video_editor.subprocess.run",
            return_value=completed,
        )

    @pytest.fixture()
    def mock_duration_and_dims(self, mocker):
        mocker.patch("congress_videos.modules.video_editor._get_source_duration", return_value=60.0)
        mocker.patch("congress_videos.modules.video_editor._get_source_dimensions", return_value=(1280, 720))

    @pytest.fixture()
    def mock_pillow_render(self, mocker):
        """Return a small blank RGBA Image instead of rendering real overlays."""
        from PIL import Image

        fake_img = Image.new("RGBA", (1280, 720), (0, 0, 0, 0))
        return mocker.patch(
            "congress_videos.modules.video_editor.render_pillow_overlay",
            return_value=fake_img,
        )

    def test_pillow_overlay_returns_success_dict(
        self, mock_subprocess, mock_duration_and_dims, mock_pillow_render
    ) -> None:
        """apply_overlays with a pillow tipo must return {success: True, output_path}."""
        from congress_videos.modules.video_editor import apply_overlays

        result = apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
            domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
        )
        assert result["success"] is True
        assert result["output_path"] == "/data/out.mp4"

    def test_pillow_overlay_calls_render_once_per_overlay(
        self, mock_subprocess, mock_duration_and_dims, mock_pillow_render
    ) -> None:
        """render_pillow_overlay must be called once for each overlay."""
        from congress_videos.modules.video_editor import apply_overlays

        apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
            domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
        )
        assert mock_pillow_render.call_count == 1

    def test_pillow_non_zero_ffmpeg_raises_runtime_error(
        self, mock_subprocess, mock_duration_and_dims, mock_pillow_render
    ) -> None:
        """A non-zero ffmpeg exit in the Pillow path must raise RuntimeError."""
        mock_subprocess.return_value.returncode = 1
        mock_subprocess.return_value.stderr = "pillow ffmpeg error"

        from congress_videos.modules.video_editor import apply_overlays

        with pytest.raises(RuntimeError, match="pillow ffmpeg error"):
            apply_overlays(
                source_path="/data/in.mp4",
                output_path="/data/out.mp4",
                overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
                domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
            )

    def test_pillow_temp_files_cleaned_up_on_success(
        self, mock_subprocess, mock_duration_and_dims, mock_pillow_render, mocker
    ) -> None:
        """Temp PNG files must be unlinked after a successful ffmpeg run."""
        from congress_videos.modules.video_editor import apply_overlays

        unlink_calls: list[str] = []
        original_unlink = __import__("os").unlink

        def tracking_unlink(path):
            unlink_calls.append(path)

        mocker.patch("congress_videos.modules.video_editor.os.unlink", side_effect=tracking_unlink)

        apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
            domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
        )
        assert len(unlink_calls) == 1

    def test_pillow_temp_files_cleaned_up_on_ffmpeg_failure(
        self, mock_subprocess, mock_duration_and_dims, mock_pillow_render, mocker
    ) -> None:
        """Temp PNG files must be unlinked even when ffmpeg fails."""
        mock_subprocess.return_value.returncode = 1
        mock_subprocess.return_value.stderr = "boom"

        from congress_videos.modules.video_editor import apply_overlays

        unlink_calls: list[str] = []
        mocker.patch("congress_videos.modules.video_editor.os.unlink", side_effect=lambda p: unlink_calls.append(p))

        with pytest.raises(RuntimeError):
            apply_overlays(
                source_path="/data/in.mp4",
                output_path="/data/out.mp4",
                overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
                domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
            )
        assert len(unlink_calls) == 1

    def test_mixed_renderers_raises_value_error(self) -> None:
        """Mixing drawtext and pillow overlays in one call must raise ValueError."""
        from congress_videos.modules.video_editor import apply_overlays

        mixed_cfg = {
            "tipos": {
                "extracto_sesion": {**_MINIMAL_STYLE, "renderer": "drawtext"},
                "speaker_id": {**_MINIMAL_PILLOW_STYLE, "renderer": "pillow"},
            }
        }
        with pytest.raises(ValueError, match="renderer"):
            apply_overlays(
                source_path="/data/in.mp4",
                output_path="/data/out.mp4",
                overlays=[
                    {**_VALID_OVERLAY},
                    {**_PILLOW_OVERLAY_SPEAKER},
                ],
                domain_cfg=mixed_cfg,
            )

    def test_fallback_dimensions_when_probe_fails(self, mock_subprocess, mock_pillow_render, mocker) -> None:
        """When _get_source_dimensions returns None, must default to 1280×720."""
        from congress_videos.modules.video_editor import apply_overlays

        mocker.patch("congress_videos.modules.video_editor._get_source_duration", return_value=None)
        mocker.patch("congress_videos.modules.video_editor._get_source_dimensions", return_value=None)

        result = apply_overlays(
            source_path="/data/in.mp4",
            output_path="/data/out.mp4",
            overlays=[{**_PILLOW_OVERLAY_SPEAKER}],
            domain_cfg=_MINIMAL_PILLOW_DOMAIN_CFG,
        )
        # Must succeed with fallback dimensions, not raise
        assert result["success"] is True
        # render must have been called with the 1280×720 fallback
        _, call_kwargs = mock_pillow_render.call_args
        call_args = mock_pillow_render.call_args[0]
        assert 1280 in call_args
        assert 720 in call_args
