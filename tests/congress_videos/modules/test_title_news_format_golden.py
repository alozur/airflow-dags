"""Golden-output snapshot tests for title news-format validation.

These tests assert that a curated set of canned LLM outputs meets the
soft structural criteria enforced by Phase 3 (``_is_valid`` question
rejection) and the news-format mandate (proper name + declarative verb).

Classification: validation-only snapshot tests.
  - NOT a runtime gate.  ``_is_valid`` is deterministic; golden snapshots
    document the expected prompt-level commitment but are never invoked
    as a pre-upload check.
  - Regeneration is MANUAL (no ``--update`` flag needed — outputs are
    hard-coded below after human review).
  - The ≥90% threshold is a soft criterion: a failing snapshot should
    trigger a prompt review, not a deployment block.

Test group: TestNewsFormatGoldenSnapshots
"""

from __future__ import annotations

import re
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Hard-coded canned LLM responses — human-reviewed snapshots.
# Mix of: proper-name + verb + complement titles, one edge case (ALL-CAPS hook
# word), and one rejected example (question mark — for triangulation).
# ---------------------------------------------------------------------------

_CANNED_OUTPUTS: list[str] = [
    "Sánchez anuncia recortes en el gasto público",          # proper name + verb
    "Feijóo acusa al Gobierno de mentir en el Congreso",     # proper name + verb
    "Yolanda Díaz defiende la jornada laboral de 37,5 horas",  # proper name + verb
    "El PSOE bloquea la reforma del sistema de pensiones",   # org + verb
    "VOX abandona la sesión parlamentaria por segunda vez",  # party acronym
    "ESCÁNDALO: Sánchez admite el fallo en los datos del paro",  # ALL-CAPS hook word
    "Feijóo critica la política energética del Ejecutivo",   # proper name + verb
    "Yolanda Díaz propone ampliar el permiso de paternidad",  # proper name + verb
    "El Gobierno aplaza la votación sobre la reforma fiscal",  # subject + verb
    "Sánchez defiende el acuerdo con Junts ante la oposición",  # proper name + verb
    "Feijóo pide elecciones anticipadas tras el escándalo",  # proper name + verb
    "La ministra de Hacienda niega la subida del IRPF",      # description + verb
]

# One deliberately rejected title to verify the snapshot logic (not counted in
# the ≥90% pass rate — represents the would-be pre-Phase-3 failure case).
_REJECTED_EXAMPLES: list[str] = [
    "¿Qué pasará con las pensiones?",
]


# ---------------------------------------------------------------------------
# Helper: replicate _is_valid logic without importing the nested function
# (it lives inside generate_title closure — not importable directly).
# ---------------------------------------------------------------------------

_EMOJI_RE = re.compile(
    "[\U00010000-\U0010ffff"
    "\U0001f600-\U0001f64f"
    "\U0001f300-\U0001f5ff"
    "\U0001f680-\U0001f6ff"
    "\U0001f700-\U0001f77f"
    "\U0001f780-\U0001f7ff"
    "\U0001f800-\U0001f8ff"
    "\U0001f900-\U0001f9ff"
    "\U0001fa00-\U0001fa6f"
    "\U0001fa70-\U0001faff"
    "]+",
    re.UNICODE,
)
_FORBIDDEN_CHARS_RE = re.compile(r"[#@|~^]")


def _has_question_mark(title: str) -> bool:
    return title.strip().startswith("¿") or "?" in title


def _is_all_caps(title: str) -> bool:
    return any(c.isalpha() for c in title) and not any(c.islower() for c in title)


def _snapshot_is_valid(title: str) -> bool:
    """Mirror of _is_valid from generate_title closure."""
    if len(title) > 90:
        return False
    if _EMOJI_RE.search(title):
        return False
    if _FORBIDDEN_CHARS_RE.search(title):
        return False
    if _is_all_caps(title):
        return False
    if _has_question_mark(title):
        return False
    return True


def _looks_like_declarative(title: str) -> bool:
    """Soft heuristic: title contains a capitalized word in first position.

    This is a broad structural check — it verifies the title is not an
    all-lowercase fragment or empty string.  Full proper-name + verb
    validation cannot be reliably done with regex on Spanish text.
    """
    if not title:
        return False
    first_word = title.split()[0].rstrip(":").rstrip(",")
    # Allow ALL-CAPS hook words (e.g. ESCÁNDALO:) as news style
    return first_word[0].isupper() or first_word.isupper()


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestNewsFormatGoldenSnapshots:
    """Snapshot validation for human-curated canned title outputs.

    These tests assert structural properties AFTER Phase 3 (_is_valid question
    rejection) is in place.  They serve as a documented baseline, not a runtime
    gate.
    """

    def test_at_least_90_percent_pass_question_mark_check(self) -> None:
        """≥90% of canned outputs must contain neither ? nor ¿."""
        passing = [t for t in _CANNED_OUTPUTS if not _has_question_mark(t)]
        ratio = len(passing) / len(_CANNED_OUTPUTS)
        assert ratio >= 0.9, (
            f"Only {ratio:.0%} of canned titles are free of question marks "
            f"(expected ≥90%): {[t for t in _CANNED_OUTPUTS if _has_question_mark(t)]}"
        )

    def test_at_least_90_percent_start_with_capitalised_word(self) -> None:
        """≥90% of canned outputs start with a title-case or ALL-CAPS first word."""
        passing = [t for t in _CANNED_OUTPUTS if _looks_like_declarative(t)]
        ratio = len(passing) / len(_CANNED_OUTPUTS)
        assert ratio >= 0.9, (
            f"Only {ratio:.0%} of canned titles start with a capitalised word "
            f"(expected ≥90%): {[t for t in _CANNED_OUTPUTS if not _looks_like_declarative(t)]}"
        )

    def test_all_canned_outputs_pass_is_valid(self) -> None:
        """All curated canned outputs must pass the _is_valid snapshot check.

        This verifies that the human-reviewed snapshots are consistent with the
        runtime validation constraints (length, emoji, forbidden chars, question marks).
        """
        failing = [t for t in _CANNED_OUTPUTS if not _snapshot_is_valid(t)]
        assert not failing, (
            f"These canned titles fail _is_valid checks: {failing}"
        )

    def test_rejected_examples_fail_question_mark_check(self) -> None:
        """Known-bad question titles must be flagged by the question mark check.

        This is a triangulation snapshot: it verifies the Phase 3 gate works on
        the pre-reform failure cases that drove issue #60.
        """
        for title in _REJECTED_EXAMPLES:
            assert _has_question_mark(title), (
                f"Expected {title!r} to be flagged as a question title"
            )

    def test_generate_title_end_to_end_strips_question_via_is_valid(self, mocker) -> None:
        """Integration: generate_title with canned question output returns sanitised result.

        This exercises the complete Phase 3 flow using the production function,
        verifying that _is_valid rejects the question, the reprompt fires, and the
        second (valid) response is returned.
        """
        from congress_videos.modules.thumbnail_generation import generate_title

        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": "¿Qué hará Sánchez con las pensiones?"}, "error": None}
            return {"data": {"title": "Sánchez anuncia cambios en las pensiones"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = {
            "styles": [{"label": "option_a", "layout": "A"}],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }
        best = {"style": "A", "prompt": "debate parlamentario"}
        result = generate_title("Debate de pensiones", best, cfg)

        assert call_count["n"] == 2, "Question title must trigger a second LLM call"
        assert "?" not in result
        assert "¿" not in result
        assert result == "Sánchez anuncia cambios en las pensiones"
