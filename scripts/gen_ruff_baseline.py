#!/usr/bin/env python3
"""Generate the `[tool.ruff.lint.per-file-ignores]` baseline block in pyproject.toml.

Reads ruff JSON diagnostics from stdin (`ruff check . --output-format json`),
groups violations by file, and rewrites the generated region between the
`# --- ruff baseline: generated, do not edit by hand ---` /
`# --- end ruff baseline ---` sentinels in pyproject.toml.

REMOVE-ONLY contract (issue #269): the generated block only records
grandfathered debt. Regenerating after fixing violations shrinks or drops
entries; it must never grow the count except when a genuinely new
pre-existing violation surfaces (e.g. after a ruff version bump). The
"C901" codes recorded here ARE issue #272's backlog — #272 prunes this
list, it does not re-derive a different one.

Usage:
    uv run ruff check . --no-cache --output-format json \\
      --config 'lint.per-file-ignores = { "__init__.py" = ["F401"] }' \\
      | uv run python scripts/gen_ruff_baseline.py --write

    # Verify no drift (local ritual, NOT a blocking CI job — see design A4):
    uv run ruff check . --no-cache --output-format json \\
      --config 'lint.per-file-ignores = { "__init__.py" = ["F401"] }' \\
      | uv run python scripts/gen_ruff_baseline.py --check
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
import tomllib
from pathlib import Path

BEGIN_SENTINEL = "# --- ruff baseline: generated, do not edit by hand ---"
END_SENTINEL = "# --- end ruff baseline ---"

# Permanent entry, never part of the generated/drift-checked baseline growth.
PERMANENT_INIT_PY_KEY = "__init__.py"


class BaselineFormatError(RuntimeError):
    """The committed `pyproject.toml` baseline region is missing or unparsable."""


class DiagnosticsInputError(RuntimeError):
    """The ruff JSON diagnostics stream on stdin is empty, malformed, or shaped wrong."""


REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

# Permanent entry: intentional re-exports in package __init__.py files. Not
# generated — always present regardless of what the diagnostics stream says.
INIT_PY_ENTRY = '"__init__.py" = ["F401"]  # permanent: re-exports (29 of them in modules/youtube/).'

DEFAULT_RUFF_VERSION = "0.16.5"
DEFAULT_BASE_SHA = "b5260d7"


def repo_relative_posix(raw_path: str) -> str:
    """Normalise a ruff-reported filename to a repo-relative POSIX path."""
    path = Path(raw_path)
    if path.is_absolute():
        path = path.relative_to(REPO_ROOT)
    return path.as_posix()


def build_entries(diagnostics: list[dict]) -> dict[str, list[str]]:
    """Group ruff JSON diagnostics into {relative_path: sorted deduped codes}."""
    grouped: dict[str, set[str]] = {}
    for diag in diagnostics:
        try:
            rel_path = repo_relative_posix(diag["filename"])
            code = diag["code"]
        except (KeyError, TypeError) as exc:
            raise DiagnosticsInputError(f"malformed ruff diagnostic entry: {diag!r}") from exc
        if code is None:
            raise DiagnosticsInputError(
                f"diagnostic for {rel_path!r} has code=null (a syntax error cannot be baselined): {diag!r}"
            )
        grouped.setdefault(rel_path, set()).add(code)
    return {path: sorted(codes) for path, codes in sorted(grouped.items())}


def render_block(entries: dict[str, list[str]], base_sha: str, ruff_version: str) -> str:
    """Render the full sentinel-delimited baseline block, header included."""
    lines = [
        BEGIN_SENTINEL,
        f"# Generated from dev tip {base_sha} with ruff {ruff_version} by scripts/gen_ruff_baseline.py.",
        "# Regenerate/verify:",
        "#   uv run ruff check . --no-cache --output-format json \\",
        '#     --config \'lint.per-file-ignores = { "__init__.py" = ["F401"] }\' \\',
        "#     | uv run python scripts/gen_ruff_baseline.py --check",
        "# REMOVE-ONLY: entries may be deleted (debt paid) or the whole entry dropped when",
        "# its code list empties. NEVER add an entry to turn CI green — fix the code or use",
        "# an inline `# noqa: CODE - reason` for a deliberate, documented exception.",
        '# The "C901" codes here ARE issue #272\'s backlog. #272 must prune this list, not',
        "# re-derive a different one.",
        "[tool.ruff.lint.per-file-ignores]",
        INIT_PY_ENTRY,
    ]
    for path, codes in entries.items():
        codes_toml = ", ".join(f'"{c}"' for c in codes)
        lines.append(f'"{path}" = [{codes_toml}]')
    lines.append(END_SENTINEL)
    return "\n".join(lines) + "\n"


def extract_region(text: str) -> str:
    start = text.index(BEGIN_SENTINEL)
    end = text.index(END_SENTINEL) + len(END_SENTINEL)
    return text[start:end]


def replace_region(text: str, new_block: str) -> str:
    start = text.index(BEGIN_SENTINEL)
    end = text.index(END_SENTINEL) + len(END_SENTINEL)
    return text[:start] + new_block.rstrip("\n") + text[end:]


def parse_committed_entries(pyproject_text: str) -> dict[str, list[str]]:
    """Parse the committed sentinel-delimited region back into {path: codes}.

    Inverse of `render_block`: reads the TOML region as-is, drops the
    permanent `__init__.py` entry (never part of generated baseline growth),
    and never sees anything outside the sentinels — a decoy
    `[tool.ruff.lint.per-file-ignores]` table elsewhere in the file is
    excluded by construction, not by special-casing it here.
    """
    try:
        region = extract_region(pyproject_text)
        parsed = tomllib.loads(region)
    except ValueError as exc:
        raise BaselineFormatError(f"committed ruff baseline region is missing or malformed: {exc}") from exc

    try:
        per_file_ignores = parsed["tool"]["ruff"]["lint"]["per-file-ignores"]
    except KeyError as exc:
        raise BaselineFormatError(
            "committed ruff baseline region has no [tool.ruff.lint.per-file-ignores] table"
        ) from exc

    return {path: sorted(codes) for path, codes in per_file_ignores.items() if path != PERMANENT_INIT_PY_KEY}


def load_diagnostics(stream) -> list[dict]:
    raw = stream.read()
    if not raw.strip():
        raise DiagnosticsInputError("stdin was empty or whitespace-only — pass ruff JSON diagnostics, not nothing")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DiagnosticsInputError(f"stdin is not valid JSON: {exc}") from exc
    if not isinstance(parsed, list):
        raise DiagnosticsInputError(f"expected a JSON array of ruff diagnostics, got {type(parsed).__name__}")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--write",
        action="store_true",
        help="Rewrite the generated region in pyproject.toml in place.",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero and print a unified diff if the generated region would change (drift detection).",
    )
    parser.add_argument(
        "--base-sha",
        default=DEFAULT_BASE_SHA,
        help=f"Base sha recorded in the header comment (default: {DEFAULT_BASE_SHA}).",
    )
    parser.add_argument(
        "--ruff-version",
        default=DEFAULT_RUFF_VERSION,
        help=f"Ruff version recorded in the header comment (default: {DEFAULT_RUFF_VERSION}).",
    )
    parser.add_argument(
        "--pyproject",
        default=PYPROJECT_PATH,
        type=Path,
        help="Path to pyproject.toml (default: repo root).",
    )
    args = parser.parse_args(argv)

    try:
        diagnostics = load_diagnostics(sys.stdin)
        entries = build_entries(diagnostics)
        new_block = render_block(entries, args.base_sha, args.ruff_version)

        pyproject_text = args.pyproject.read_text(encoding="utf-8")

        if args.write:
            updated = replace_region(pyproject_text, new_block)
            args.pyproject.write_text(updated, encoding="utf-8")
            print(f"Wrote {len(entries)} generated entries to {args.pyproject}")
            return 0

        if args.check:
            current_region = extract_region(pyproject_text)
            if current_region.rstrip("\n") == new_block.rstrip("\n"):
                print("No drift: baseline is up to date.")
                return 0
            diff = difflib.unified_diff(
                current_region.splitlines(keepends=True),
                new_block.splitlines(keepends=True),
                fromfile="pyproject.toml (current)",
                tofile="pyproject.toml (regenerated)",
            )
            sys.stdout.writelines(diff)
            return 1

        sys.stdout.write(new_block)
        return 0
    except (BaselineFormatError, DiagnosticsInputError, ValueError) as exc:
        # ValueError is caught here too: extract_region() (used directly by
        # --check, above) raises it on a missing sentinel, and this is the
        # same class of "input is not shaped right" failure as the two named
        # errors above — it must exit 2 in every mode, not just --write.
        print(f"gen_ruff_baseline: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
