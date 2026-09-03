"""Config-invariant tests for ruff lint enforcement (issue #269).

These tests parse `pyproject.toml` with stdlib `tomllib` and never invoke the
`ruff` binary — sub-second, no external process. They lock down the exact
`[tool.ruff]` shape decided in design so a future edit that silently re-adds
`target-version`, disables a rule, or widens `extend-exclude` fails loudly.

`tests/test_gen_ruff_baseline.py` covers the generator script itself (T7);
this file is deliberately scoped to config-invariants only (T1-T6).
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

_EXPECTED_SELECT = {"E", "W", "F", "I", "N", "UP", "B", "C4", "C90", "SIM"}

# First-party source trees that must never be excluded from lint scope
# (A7 threat-matrix guard row 1 — an over-broad exclude silently unlints code).
_FIRST_PARTY_DIRS = {
    "utils",
    "congress_videos",
    "tests",
    "scripts",
    "examples",
    "e2e",
    "benchmarks",
}


def _load_pyproject() -> dict:
    return tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))


def _dev_group_ruff_pin() -> str:
    pyproject = _load_pyproject()
    dev_group = pyproject["dependency-groups"]["dev"]
    for entry in dev_group:
        if isinstance(entry, str) and entry.startswith("ruff=="):
            return entry.split("==", 1)[1]
    raise AssertionError("no ruff== entry found in [dependency-groups] dev")


class TestRuffTopLevelConfig:
    """T1: line-length, target-version absence, select set, ignore, mccabe."""

    def test_line_length_is_120(self):
        ruff_config = _load_pyproject()["tool"]["ruff"]
        assert ruff_config["line-length"] == 120

    def test_target_version_is_absent(self):
        ruff_config = _load_pyproject()["tool"]["ruff"]
        assert "target-version" not in ruff_config

    def test_lint_select_matches_the_ten_rule_families(self):
        lint_config = _load_pyproject()["tool"]["ruff"]["lint"]
        assert set(lint_config["select"]) == _EXPECTED_SELECT

    def test_lint_ignore_is_empty(self):
        lint_config = _load_pyproject()["tool"]["ruff"]["lint"]
        assert lint_config["ignore"] == []

    def test_mccabe_max_complexity_is_ten(self):
        mccabe_config = _load_pyproject()["tool"]["ruff"]["lint"]["mccabe"]
        assert mccabe_config["max-complexity"] == 10


class TestPerFileIgnoresPaths:
    """T2: every per-file-ignores key besides the __init__.py glob is a real path."""

    def test_every_non_glob_key_resolves_to_an_existing_file(self):
        per_file_ignores = _load_pyproject()["tool"]["ruff"]["lint"]["per-file-ignores"]
        missing = [key for key in per_file_ignores if key != "__init__.py" and not (REPO_ROOT / key).is_file()]
        assert missing == [], f"stale per-file-ignores entries (file no longer exists): {missing}"


class TestPerFileIgnoresCodeLists:
    """T3: each entry's code list is sorted and duplicate-free."""

    def test_every_code_list_is_sorted_and_deduped(self):
        per_file_ignores = _load_pyproject()["tool"]["ruff"]["lint"]["per-file-ignores"]
        unsorted_or_dupe = {
            path: codes
            for path, codes in per_file_ignores.items()
            if codes != sorted(set(codes)) or len(codes) != len(set(codes))
        }
        assert unsorted_or_dupe == {}


class TestC901BaselineCoverage:
    """T4: the baseline records the measured C901 offender-file count.

    Measured directly against the change tip (post-autofix) with
    `uv run ruff check . --output-format json --select C90` grouped by file:
    29 distinct files carry at least one C901 diagnostic (38 total C901
    diagnostics across those 29 files — some files have more than one
    over-threshold function, so file-count and diagnostic-count differ).
    The proposal's "38 offenders across 32 files" figure was measured before
    later, unrelated `dev` commits landed; this test locks the value actually
    observed at this change's tip, not the stale proposal-time snapshot.
    """

    EXPECTED_C901_FILE_COUNT = 26

    def test_exactly_the_measured_number_of_entries_carry_c901(self):
        per_file_ignores = _load_pyproject()["tool"]["ruff"]["lint"]["per-file-ignores"]
        c901_entries = [path for path, codes in per_file_ignores.items() if "C901" in codes]
        assert len(c901_entries) == self.EXPECTED_C901_FILE_COUNT


class TestBaselineHeaderRuffVersion:
    """T5: the baseline header's recorded ruff version matches the dev-group pin."""

    def test_header_ruff_version_matches_dependency_pin(self):
        raw_text = PYPROJECT_PATH.read_text(encoding="utf-8")
        match = re.search(r"with ruff (\S+) by scripts/gen_ruff_baseline\.py", raw_text)
        assert match is not None, "baseline header comment not found in pyproject.toml"
        assert match.group(1) == _dev_group_ruff_pin()


class TestExtendExclude:
    """T6: extend-exclude only ever names openspec/, never a first-party source dir."""

    def test_extend_exclude_is_exactly_openspec(self):
        ruff_config = _load_pyproject()["tool"]["ruff"]
        assert ruff_config["extend-exclude"] == ["openspec"]

    def test_extend_exclude_names_no_first_party_source_dir(self):
        ruff_config = _load_pyproject()["tool"]["ruff"]
        excluded = set(ruff_config["extend-exclude"])
        assert excluded.isdisjoint(_FIRST_PARTY_DIRS)
