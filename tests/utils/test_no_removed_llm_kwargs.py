"""Static guard: `temperature`/`max_tokens`/`max_completion_tokens` must never
appear as call keywords anywhere under `utils/` or `congress_videos/` (issue #375).

This is an AST walk, NOT a grep. A textual search would falsely flag prose
mentions in docstrings/comments (`utils/llm_cache.py`, the ranking-call comment
in `congress_videos/modules/thumbnail_generation.py`) and Whisper `temperature`
fields in test fixtures. An AST keyword scan cannot see any of those.

The scan is DELIBERATELY not filtered by callee name: 5 of the 14 production
call sites this issue fixes invoke the OpenAI helpers through an injected
`completion_fn` parameter, so a callee-name allowlist would be blind to exactly
those sites. Any `ast.Call` anywhere in the two roots is in scope.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from utils.ai_helpers import generate_chat_completion, generate_json_completion

_FORBIDDEN = frozenset({"temperature", "max_tokens", "max_completion_tokens"})
_ROOTS = ("utils", "congress_videos")

# (relative path, dotted callee) pairs deliberately exempted. Empty on purpose:
# any addition here is a reviewed decision, not a silent bypass.
_ALLOWED: frozenset[tuple[str, str]] = frozenset()

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _callee_name(func_node: ast.expr) -> str:
    try:
        return ast.unparse(func_node)
    except Exception:  # pragma: no cover - defensive only
        return "<unknown>"


def _find_violations() -> list[str]:
    violations: list[str] = []

    for root_name in _ROOTS:
        root = _REPO_ROOT / root_name
        for py_file in sorted(root.rglob("*.py")):
            rel_path = py_file.relative_to(_REPO_ROOT).as_posix()
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel_path)

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                callee = _callee_name(node.func)
                for kw in node.keywords:
                    if kw.arg in _FORBIDDEN and (rel_path, callee) not in _ALLOWED:
                        violations.append(f"{rel_path}:{node.lineno}: {callee}(... {kw.arg}=...)")

    return violations


class TestNoRemovedLlmKwargsOnAnyCall:
    def test_no_forbidden_kwargs_anywhere_in_scanned_roots(self):
        violations = _find_violations()
        assert violations == [], (
            "Found forbidden temperature/max_tokens/max_completion_tokens "
            "kwargs at call sites:\n" + "\n".join(violations)
        )


class TestHelperSignaturesRejectRemovedParams:
    @pytest.mark.parametrize("fn", [generate_chat_completion, generate_json_completion])
    def test_signature_has_no_temperature_or_max_tokens_param(self, fn):
        params = inspect.signature(fn).parameters
        assert "temperature" not in params
        assert "max_tokens" not in params
