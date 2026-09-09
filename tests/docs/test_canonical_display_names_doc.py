"""Structural checks for the canonical display-names governance doc (issue #511).

The doc is the operational contract for keeping
``congress_videos/catalogs/politician_display_names.v1.json`` correct over
time. These tests only assert the doc exists and covers the required
sections — they do not re-validate the catalogue itself (that lives in
``tests/congress_videos/test_politician_display_names.py``).
"""

from pathlib import Path

DOC_PATH = Path(__file__).resolve().parents[2] / "docs" / "CANONICAL_DISPLAY_NAMES.md"


def _doc_text() -> str:
    return DOC_PATH.read_text(encoding="utf-8")


def test_doc_exists():
    assert DOC_PATH.is_file(), f"Expected governance doc at {DOC_PATH}"


def test_doc_states_ownership_and_presentation_boundary():
    text = _doc_text()
    assert "Ownership" in text
    assert "presentation" in text.lower()
    assert "identity resolution" in text.lower()


def test_doc_states_the_selection_criterion_and_query():
    text = _doc_text()
    assert "at least twice" in text or ">= 2" in text or "≥2" in text
    assert "resolved_participant_slug" in text
    assert "video_chapters" in text
    assert "speaker_turn_videos" in text
    assert "11" in text
    assert "21" in text


def test_doc_states_review_cadence_and_triggers():
    text = _doc_text()
    assert "quarterly" in text.lower()
    assert "election" in text.lower()
    assert "reshuffle" in text.lower()


def test_doc_states_add_or_edit_procedure_with_invariants():
    text = _doc_text()
    assert "full_name" in text
    assert "subsequence" in text.lower()
    assert "duplicate" in text.lower()
    assert "colliding" in text.lower() or "collision" in text.lower()
    assert "uv run pytest tests/congress_videos/test_politician_display_names.py" in text


def test_doc_documents_the_rodriguez_collision_example():
    text = _doc_text()
    assert "isabel-rodriguez-garcia" in text
    assert "javier-rodriguez-palacios" in text
    assert "jose-antonio-rodriguez-salas" in text


def test_doc_states_the_silent_degradation_caveat():
    text = _doc_text()
    assert "never raises" in text.lower()
    assert "logs" in text.lower()
    assert "bundled catalogue" in text.lower() or "bundled catalog" in text.lower()


def test_doc_states_what_is_not_canonicalised():
    text = _doc_text()
    assert "mentioned" in text.lower()
    assert "surname" in text.lower()
