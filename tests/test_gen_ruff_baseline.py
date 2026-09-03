"""Unit tests for scripts/gen_ruff_baseline.py (T7, issue #269).

The generator is authored logic (grouping/sorting/rendering/region-replace),
not tool output, so it gets normal TDD unit tests. Fixture JSON diagnostics
in, exact expected TOML block out; `--check` must exit non-zero on injected
drift.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATOR_PATH = REPO_ROOT / "scripts" / "gen_ruff_baseline.py"


def _load_generator_module():
    spec = importlib.util.spec_from_file_location("gen_ruff_baseline", GENERATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_ruff_baseline"] = module
    spec.loader.exec_module(module)
    return module


gen_ruff_baseline = _load_generator_module()


_FIXTURE_DIAGNOSTICS = [
    {"filename": "utils/whisper_helpers.py", "code": "F401"},
    {"filename": "utils/whisper_helpers.py", "code": "SIM117"},
    {"filename": "utils/whisper_helpers.py", "code": "F401"},  # duplicate on purpose
    {"filename": "congress_videos/modules/database.py", "code": "C901"},
]


class TestBuildEntries:
    def test_groups_codes_by_file_sorted_and_deduped(self):
        entries = gen_ruff_baseline.build_entries(_FIXTURE_DIAGNOSTICS)

        assert entries == {
            "congress_videos/modules/database.py": ["C901"],
            "utils/whisper_helpers.py": ["F401", "SIM117"],
        }

    def test_absolute_paths_are_normalised_to_repo_relative_posix(self):
        absolute_diagnostics = [
            {
                "filename": str(REPO_ROOT / "utils" / "ai_helpers.py"),
                "code": "F401",
            }
        ]

        entries = gen_ruff_baseline.build_entries(absolute_diagnostics)

        assert entries == {"utils/ai_helpers.py": ["F401"]}


class TestRenderBlock:
    def test_fixture_diagnostics_render_exact_expected_toml_block(self):
        entries = gen_ruff_baseline.build_entries(_FIXTURE_DIAGNOSTICS)

        block = gen_ruff_baseline.render_block(entries, base_sha="deadbee", ruff_version="0.16.5")

        expected = (
            "# --- ruff baseline: generated, do not edit by hand ---\n"
            "# Generated from dev tip deadbee with ruff 0.16.5 by scripts/gen_ruff_baseline.py.\n"
            "# Regenerate/verify:\n"
            "#   uv run ruff check . --no-cache --output-format json \\\n"
            '#     --config \'lint.per-file-ignores = { "__init__.py" = ["F401"] }\' \\\n'
            "#     | uv run python scripts/gen_ruff_baseline.py --check\n"
            "# REMOVE-ONLY: entries may be deleted (debt paid) or the whole entry dropped when\n"
            "# its code list empties. NEVER add an entry to turn CI green — fix the code or use\n"
            "# an inline `# noqa: CODE - reason` for a deliberate, documented exception.\n"
            '# The "C901" codes here ARE issue #272\'s backlog. #272 must prune this list, not\n'
            "# re-derive a different one.\n"
            "[tool.ruff.lint.per-file-ignores]\n"
            '"__init__.py" = ["F401"]  # permanent: re-exports (29 of them in modules/youtube/).\n'
            '"congress_videos/modules/database.py" = ["C901"]\n'
            '"utils/whisper_helpers.py" = ["F401", "SIM117"]\n'
            "# --- end ruff baseline ---\n"
        )
        assert block == expected


class TestReplaceRegion:
    def test_replaces_only_the_sentinel_delimited_region(self):
        original = (
            "[tool.ruff]\n"
            "line-length = 120\n\n"
            "# --- ruff baseline: generated, do not edit by hand ---\n"
            "OLD CONTENT\n"
            "# --- end ruff baseline ---\n\n"
            "[tool.other]\n"
            "value = 1\n"
        )
        new_block = (
            "# --- ruff baseline: generated, do not edit by hand ---\nNEW CONTENT\n# --- end ruff baseline ---\n"
        )

        updated = gen_ruff_baseline.replace_region(original, new_block)

        assert "OLD CONTENT" not in updated
        assert "NEW CONTENT" in updated
        assert "[tool.ruff]\nline-length = 120" in updated
        assert "[tool.other]\nvalue = 1" in updated


class TestCheckModeDrift:
    def test_check_exits_non_zero_when_region_drifts_from_recomputed_entries(self, tmp_path, capsys):
        pyproject_path = tmp_path / "pyproject.toml"
        stale_block = gen_ruff_baseline.render_block(
            {"utils/stale_only.py": ["F401"]}, base_sha="deadbee", ruff_version="0.16.5"
        )
        pyproject_path.write_text(
            "[tool.ruff]\nline-length = 120\n\n" + stale_block,
            encoding="utf-8",
        )

        fresh_entries = gen_ruff_baseline.build_entries(_FIXTURE_DIAGNOSTICS)
        fresh_block = gen_ruff_baseline.render_block(fresh_entries, base_sha="deadbee", ruff_version="0.16.5")
        current_region = gen_ruff_baseline.extract_region(pyproject_path.read_text(encoding="utf-8"))

        assert current_region.rstrip("\n") != fresh_block.rstrip("\n")

    def test_check_reports_no_drift_when_region_matches_recomputed_entries(self, tmp_path):
        entries = gen_ruff_baseline.build_entries(_FIXTURE_DIAGNOSTICS)
        block = gen_ruff_baseline.render_block(entries, base_sha="deadbee", ruff_version="0.16.5")
        pyproject_path = tmp_path / "pyproject.toml"
        pyproject_path.write_text("[tool.ruff]\nline-length = 120\n\n" + block, encoding="utf-8")

        current_region = gen_ruff_baseline.extract_region(pyproject_path.read_text(encoding="utf-8"))

        assert current_region.rstrip("\n") == block.rstrip("\n")


class TestMainCheckExitCode:
    def test_main_check_returns_1_on_drift(self, tmp_path, monkeypatch, capsys):
        pyproject_path = tmp_path / "pyproject.toml"
        stale_block = gen_ruff_baseline.render_block(
            {"utils/stale_only.py": ["F401"]}, base_sha="deadbee", ruff_version="0.16.5"
        )
        pyproject_path.write_text("[tool.ruff]\nline-length = 120\n\n" + stale_block, encoding="utf-8")

        monkeypatch.setattr(
            sys,
            "stdin",
            __import__("io").StringIO('[{"filename": "utils/whisper_helpers.py", "code": "F401"}]'),
        )

        exit_code = gen_ruff_baseline.main(["--check", "--pyproject", str(pyproject_path), "--base-sha", "deadbee"])

        assert exit_code == 1

    def test_main_check_returns_0_when_no_drift(self, tmp_path, monkeypatch):
        entries = gen_ruff_baseline.build_entries([{"filename": "utils/whisper_helpers.py", "code": "F401"}])
        block = gen_ruff_baseline.render_block(entries, base_sha="deadbee", ruff_version="0.16.5")
        pyproject_path = tmp_path / "pyproject.toml"
        pyproject_path.write_text("[tool.ruff]\nline-length = 120\n\n" + block, encoding="utf-8")

        monkeypatch.setattr(
            sys,
            "stdin",
            __import__("io").StringIO('[{"filename": "utils/whisper_helpers.py", "code": "F401"}]'),
        )

        exit_code = gen_ruff_baseline.main(["--check", "--pyproject", str(pyproject_path), "--base-sha", "deadbee"])

        assert exit_code == 0


class TestParseCommittedEntries:
    """`parse_committed_entries` reads the sentinel-delimited region back as TOML,
    the inverse of `render_block`, so `find_growth` (PR2) has a committed baseline
    to diff the regenerated entries against.
    """

    def test_parses_valid_multi_code_and_single_code_entries(self):
        pyproject_text = (
            "[tool.other]\nvalue = 1\n\n"
            + gen_ruff_baseline.BEGIN_SENTINEL
            + "\n"
            + "[tool.ruff.lint.per-file-ignores]\n"
            + '"__init__.py" = ["F401"]\n'
            + '"utils/foo.py" = ["F401", "SIM117"]\n'
            + '"utils/bar.py" = ["C901"]\n'
            + gen_ruff_baseline.END_SENTINEL
            + "\n"
        )

        entries = gen_ruff_baseline.parse_committed_entries(pyproject_text)

        assert entries == {
            "utils/foo.py": ["F401", "SIM117"],
            "utils/bar.py": ["C901"],
        }

    def test_bare_init_py_glob_is_dropped(self):
        pyproject_text = (
            gen_ruff_baseline.BEGIN_SENTINEL
            + "\n"
            + "[tool.ruff.lint.per-file-ignores]\n"
            + '"__init__.py" = ["F401"]\n'
            + gen_ruff_baseline.END_SENTINEL
            + "\n"
        )

        entries = gen_ruff_baseline.parse_committed_entries(pyproject_text)

        assert entries == {}
        assert "__init__.py" not in entries

    def test_header_and_trailing_inline_comments_are_ignored(self):
        pyproject_text = (
            gen_ruff_baseline.BEGIN_SENTINEL
            + "\n"
            + "# Generated from dev tip deadbee with ruff 0.16.5 by scripts/gen_ruff_baseline.py.\n"
            + "# some other header comment\n"
            + "[tool.ruff.lint.per-file-ignores]\n"
            + '"__init__.py" = ["F401"]  # permanent: re-exports\n'
            + '"utils/foo.py" = ["F401"]  # inline note\n'
            + gen_ruff_baseline.END_SENTINEL
            + "\n"
        )

        entries = gen_ruff_baseline.parse_committed_entries(pyproject_text)

        assert entries == {"utils/foo.py": ["F401"]}

    def test_decoy_per_file_ignores_outside_sentinels_is_excluded(self):
        pyproject_text = (
            "[tool.ruff.lint.per-file-ignores]\n"
            + '"decoy/outside.py" = ["E999"]\n\n'
            + gen_ruff_baseline.BEGIN_SENTINEL
            + "\n"
            + "[tool.ruff.lint.per-file-ignores]\n"
            + '"utils/foo.py" = ["F401"]\n'
            + gen_ruff_baseline.END_SENTINEL
            + "\n"
        )

        entries = gen_ruff_baseline.parse_committed_entries(pyproject_text)

        assert entries == {"utils/foo.py": ["F401"]}
        assert "decoy/outside.py" not in entries

    def test_missing_sentinel_raises_baseline_format_error(self):
        pyproject_text = "[tool.other]\nvalue = 1\n"

        with pytest.raises(gen_ruff_baseline.BaselineFormatError):
            gen_ruff_baseline.parse_committed_entries(pyproject_text)

    def test_malformed_toml_in_region_raises_baseline_format_error(self):
        pyproject_text = (
            gen_ruff_baseline.BEGIN_SENTINEL + "\n" + "[[[ not valid toml\n" + gen_ruff_baseline.END_SENTINEL + "\n"
        )

        with pytest.raises(gen_ruff_baseline.BaselineFormatError):
            gen_ruff_baseline.parse_committed_entries(pyproject_text)


class TestLoadDiagnosticsHardening:
    """`load_diagnostics` must fail loudly on malformed input instead of the
    bare-`--write`-wipes-the-block footgun: silently treating garbage stdin
    as "no diagnostics" used to regenerate an empty (wrong) baseline.
    """

    def test_empty_string_raises_diagnostics_input_error(self):
        with pytest.raises(gen_ruff_baseline.DiagnosticsInputError):
            gen_ruff_baseline.load_diagnostics(io.StringIO(""))

    def test_whitespace_only_raises_diagnostics_input_error(self):
        with pytest.raises(gen_ruff_baseline.DiagnosticsInputError):
            gen_ruff_baseline.load_diagnostics(io.StringIO("   \n\t  "))

    def test_empty_json_array_returns_empty_list(self):
        result = gen_ruff_baseline.load_diagnostics(io.StringIO("[]"))

        assert result == []

    def test_malformed_json_raises_diagnostics_input_error(self):
        with pytest.raises(gen_ruff_baseline.DiagnosticsInputError):
            gen_ruff_baseline.load_diagnostics(io.StringIO("{not json"))

    def test_non_list_json_raises_diagnostics_input_error(self):
        with pytest.raises(gen_ruff_baseline.DiagnosticsInputError):
            gen_ruff_baseline.load_diagnostics(io.StringIO('{"a": 1}'))

    def test_diagnostic_with_null_code_raises_from_build_entries(self):
        with pytest.raises(gen_ruff_baseline.DiagnosticsInputError):
            gen_ruff_baseline.build_entries([{"filename": "utils/foo.py", "code": None}])


class TestMainErrorHandling:
    """Both error classes must print `gen_ruff_baseline: {exc}` to stderr and
    return 2 in every mode, including `--write` — turning the bare-`--write`
    footgun into a hard error instead of silently wiping the baseline block.
    """

    def test_write_with_blank_stdin_exits_2_and_leaves_pyproject_unchanged(self, tmp_path, monkeypatch):
        pyproject_path = tmp_path / "pyproject.toml"
        original_text = (
            "[tool.other]\nvalue = 1\n\n"
            + gen_ruff_baseline.BEGIN_SENTINEL
            + "\n"
            + "[tool.ruff.lint.per-file-ignores]\n"
            + '"utils/foo.py" = ["F401"]\n'
            + gen_ruff_baseline.END_SENTINEL
            + "\n"
        )
        pyproject_path.write_text(original_text, encoding="utf-8")
        monkeypatch.setattr(sys, "stdin", io.StringIO("   "))

        exit_code = gen_ruff_baseline.main(["--write", "--pyproject", str(pyproject_path)])

        assert exit_code == 2
        assert pyproject_path.read_text(encoding="utf-8") == original_text

    def test_error_message_is_printed_to_stderr(self, tmp_path, monkeypatch, capsys):
        pyproject_path = tmp_path / "pyproject.toml"
        pyproject_path.write_text("irrelevant", encoding="utf-8")
        monkeypatch.setattr(sys, "stdin", io.StringIO(""))

        exit_code = gen_ruff_baseline.main(["--write", "--pyproject", str(pyproject_path)])

        captured = capsys.readouterr()
        assert exit_code == 2
        assert "gen_ruff_baseline:" in captured.err
