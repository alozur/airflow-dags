"""Unit tests for congress_videos.modules.monologue_speaker_window (issue #430).

Slice A1: window selection + prompt constants.
Slice A2a (this file, extended): FloorHolder/AnnouncedIdentity dataclasses and
the two LLM-step seam functions (identify_floor_holder, resolve_announced_identity).
A2b adds the never-raise orchestrator that wires the two steps together.
"""

from __future__ import annotations

import logging

import pytest

from congress_videos.config.ai_prompts import (
    MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT,
    MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE,
    MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT,
    MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE,
)
from congress_videos.modules.monologue_speaker_window import (
    MONOLOGUE_WINDOW_SECS,
    AnnouncedIdentity,
    FloorHolder,
    identify_floor_holder,
    resolve_announced_identity,
    select_preceding_window,
    turn_anchor_seconds,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _block(start_secs: float, end_secs: float, text: str = "text") -> dict:
    return {"start_secs": start_secs, "end_secs": end_secs, "text": text}


def _turn(start_seconds: float = 500.0, group_start_seconds: float | None = None) -> dict:
    turn = {"turn_id": 1, "start_seconds": start_seconds}
    if group_start_seconds is not None:
        turn["group_start_seconds"] = group_start_seconds
    return turn


# ---------------------------------------------------------------------------
# turn_anchor_seconds
# ---------------------------------------------------------------------------


def test_anchor_uses_start_seconds_when_group_start_absent():
    turn = {"turn_id": 1, "start_seconds": 500.0}

    assert turn_anchor_seconds(turn) == 500.0


def test_anchor_uses_group_start_seconds_when_present_and_differs():
    turn = {"turn_id": 1, "start_seconds": 500.0, "group_start_seconds": 300.0}

    assert turn_anchor_seconds(turn) == 300.0


def test_anchor_honours_group_start_seconds_zero():
    turn = {"turn_id": 1, "start_seconds": 500.0, "group_start_seconds": 0.0}

    assert turn_anchor_seconds(turn) == 0.0


def test_anchor_ignores_group_start_seconds_none():
    turn = {"turn_id": 1, "start_seconds": 500.0, "group_start_seconds": None}

    assert turn_anchor_seconds(turn) == 500.0


# ---------------------------------------------------------------------------
# select_preceding_window — 6 boundary cases (spec: Preceding Window Selection)
# ---------------------------------------------------------------------------


def test_block_at_window_start_boundary_is_included():
    anchor = 500.0
    block = _block(anchor - MONOLOGUE_WINDOW_SECS, anchor - MONOLOGUE_WINDOW_SECS + 5)

    result = select_preceding_window([block], anchor)

    assert result == [block]


def test_block_just_before_window_start_is_excluded():
    anchor = 500.0
    block = _block(anchor - MONOLOGUE_WINDOW_SECS - 0.001, anchor - MONOLOGUE_WINDOW_SECS)

    result = select_preceding_window([block], anchor)

    assert result == []


def test_block_at_anchor_is_excluded():
    anchor = 500.0
    block = _block(anchor, anchor + 5)

    result = select_preceding_window([block], anchor)

    assert result == []


def test_block_overlapping_anchor_is_selected_by_start_time():
    anchor = 500.0
    block = _block(anchor - 1, anchor + 10)

    result = select_preceding_window([block], anchor)

    assert result == [block]


def test_anchor_near_session_start_clamps_window_start_to_zero():
    anchor = 30.0
    before_anchor = _block(0.0, 5.0)
    negative_would_have_excluded = _block(anchor - MONOLOGUE_WINDOW_SECS, anchor - MONOLOGUE_WINDOW_SECS + 1)

    result = select_preceding_window([before_anchor, negative_would_have_excluded], anchor)

    assert result == [before_anchor]


def test_group_start_seconds_overrides_start_seconds_for_the_window():
    turn = _turn(start_seconds=500.0, group_start_seconds=300.0)
    anchor = turn_anchor_seconds(turn)
    in_group_window = _block(anchor - 10, anchor - 5)
    outside_group_window_inside_turn_window = _block(anchor + 50, anchor + 60)

    result = select_preceding_window([in_group_window, outside_group_window_inside_turn_window], anchor)

    assert result == [in_group_window]


def test_multiple_blocks_only_the_in_window_ones_are_kept():
    anchor = 500.0
    too_early = _block(anchor - MONOLOGUE_WINDOW_SECS - 1, anchor - MONOLOGUE_WINDOW_SECS - 0.5)
    in_window_1 = _block(anchor - 100, anchor - 90)
    in_window_2 = _block(anchor - 10, anchor - 2)
    at_anchor = _block(anchor, anchor + 5)

    result = select_preceding_window([too_early, in_window_1, in_window_2, at_anchor], anchor)

    assert result == [in_window_1, in_window_2]


# ---------------------------------------------------------------------------
# Prompt contracts (design.md: the only thing a unit test can prove about a
# prompt's static text — real model behaviour needs the opt-in live_llm test
# added in a later slice)
# ---------------------------------------------------------------------------


def test_floor_holder_system_prompt_states_the_addressee_rule():
    assert "ADDRESSED" in MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT
    assert "FLOOR HOLDER" in MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT


def test_floor_holder_system_prompt_states_the_found_false_rule():
    assert "found to false" in MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT


def test_floor_holder_system_prompt_states_the_verbatim_evidence_rule():
    assert "verbatim quote" in MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT


def test_floor_holder_user_template_interpolates_only_window_text():
    rendered = MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE.format(window_text="SENTINEL_WINDOW_TEXT")

    assert "SENTINEL_WINDOW_TEXT" in rendered


def test_floor_holder_user_template_has_no_other_placeholders():
    import string

    fields = {name for _, name, _, _ in string.Formatter().parse(MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE) if name}

    assert fields == {"window_text"}


def test_identity_resolution_system_prompt_states_the_roster_only_rule():
    assert "roster" in MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT
    assert "null" in MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT


def test_identity_resolution_system_prompt_states_the_confidence_rule():
    assert "confidence" in MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT


def test_identity_resolution_user_template_interpolates_only_the_three_fields():
    import string

    fields = {name for _, name, _, _ in string.Formatter().parse(MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE) if name}

    assert fields == {"announced_name_or_role", "evidence", "participant_roster"}


def test_identity_resolution_user_template_renders_all_three_fields():
    rendered = MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE.format(
        announced_name_or_role="SENTINEL_ROLE",
        evidence="SENTINEL_EVIDENCE",
        participant_roster="SENTINEL_ROSTER",
    )

    assert "SENTINEL_ROLE" in rendered
    assert "SENTINEL_EVIDENCE" in rendered
    assert "SENTINEL_ROSTER" in rendered


# ---------------------------------------------------------------------------
# identify_floor_holder — Step 1 seam (slice A2a)
# ---------------------------------------------------------------------------


def _ok_step1(announced_name_or_role, evidence, found=True):
    data = {"announced_name_or_role": announced_name_or_role, "evidence": evidence, "found": found}
    return {"error": None, "data": data}


@pytest.mark.parametrize(
    ("announced", "evidence"),
    [
        ("García", "Tiene la palabra la señora García"),
        ("el ministro de Hacienda", "tiene la palabra el ministro de Hacienda"),
        ("X", "Señor López, le contesta el ministro X"),
        ("Ruiz", "gracias señora presidenta, tiene la palabra el señor Ruiz"),
    ],
    ids=["full-name", "role-only", "addressee-vs-responder", "courtesy-then-handover"],
)
def test_identify_floor_holder_is_a_mock_echo(announced, evidence):
    """Pass-through parsing proof, not model behaviour: whatever completion_fn
    returns is parsed into FloorHolder unchanged. The addressee-vs-responder
    case ('Señor López, le contesta el ministro X') is told to return "X",
    proving the seam never re-derives an answer on its own — that exact
    string never reaches this seam through resolve_monologue_speaker in
    production, since it matches none of announcement_patterns.py's phrases
    (A2b only)."""

    def fake_completion(system, user, **kw):
        return _ok_step1(announced, evidence)

    result = identify_floor_holder([_block(380.0, 385.0, evidence)], completion_fn=fake_completion)

    assert result == FloorHolder(announced_name_or_role=announced, evidence=evidence, found=True)


def test_identify_floor_holder_found_false_yields_unresolved_sentinel():
    def fake_completion(system, user, **kw):
        return _ok_step1(None, "", found=False)

    result = identify_floor_holder([_block(380.0, 385.0, "no handover here")], completion_fn=fake_completion)

    assert result.found is False


def test_identify_floor_holder_payload_excludes_text_outside_the_window():
    """Step 1's user prompt carries only what the caller passed as window_blocks."""
    captured = {}

    def fake_completion(system, user, **kw):
        captured["user"] = user
        return _ok_step1("García", "Tiene la palabra la señora García")

    window_blocks = [_block(380.0, 385.0, "Tiene la palabra la señora García SENTINEL_IN_WINDOW")]
    identify_floor_holder(window_blocks, completion_fn=fake_completion)

    assert "SENTINEL_IN_WINDOW" in captured["user"]
    assert "SENTINEL_OUTSIDE_WINDOW" not in captured["user"]


@pytest.mark.parametrize(
    "response",
    [
        {"error": "boom", "data": None},
        {"error": None, "data": None},
        {"error": None, "data": "not-a-dict"},
    ],
)
def test_identify_floor_holder_error_response_yields_sentinel_and_one_warning(response, caplog):
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        return response

    with caplog.at_level(logging.WARNING):
        result = identify_floor_holder([_block(380.0, 385.0, "text")], completion_fn=fake_completion)

    assert result == FloorHolder()
    assert len(call_count) == 1
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1


def test_identify_floor_holder_raise_propagates_uncaught():
    def fake_completion(system, user, **kw):
        raise RuntimeError("OpenAI API error")

    with pytest.raises(RuntimeError, match="OpenAI API error"):
        identify_floor_holder([_block(380.0, 385.0, "text")], completion_fn=fake_completion)


# ---------------------------------------------------------------------------
# resolve_announced_identity — Step 2 seam (slice A2a)
# ---------------------------------------------------------------------------


def _ok_step2(full_name, participant_slug, confidence):
    return {
        "error": None,
        "data": {"full_name": full_name, "participant_slug": participant_slug, "confidence": confidence},
    }


def _floor_holder(announced="García", evidence="Tiene la palabra la señora García"):
    return FloorHolder(announced_name_or_role=announced, evidence=evidence, found=True)


def _participants(slugs=("maria-garcia", "pedro-sanchez")):
    return [{"slug": s, "display_name": s.replace("-", " ").title(), "party": "TEST"} for s in slugs]


@pytest.mark.parametrize(
    ("confidence", "expected"),
    [
        (0.80, AnnouncedIdentity(full_name="María García", participant_slug="maria-garcia", confidence=0.80)),
        (0.79, AnnouncedIdentity()),
    ],
    ids=["at-threshold-accepts", "just-below-threshold-rejects"],
)
def test_resolve_announced_identity_confidence_boundary(confidence, expected):
    def fake_completion(system, user, **kw):
        return _ok_step2("María García", "maria-garcia", confidence)

    result = resolve_announced_identity(_floor_holder(), _participants(), completion_fn=fake_completion)

    assert result == expected


def test_resolve_announced_identity_rejects_slug_outside_roster():
    def fake_completion(system, user, **kw):
        return _ok_step2("Someone Else", "not-in-roster", 0.95)

    result = resolve_announced_identity(_floor_holder(), _participants(), completion_fn=fake_completion)

    assert result == AnnouncedIdentity()


def test_resolve_announced_identity_rejects_non_numeric_confidence():
    def fake_completion(system, user, **kw):
        return _ok_step2("María García", "maria-garcia", "very confident")

    result = resolve_announced_identity(_floor_holder(), _participants(), completion_fn=fake_completion)

    assert result == AnnouncedIdentity()


def test_resolve_announced_identity_payload_excludes_window_text_beyond_evidence():
    captured = {}

    def fake_completion(system, user, **kw):
        captured["user"] = user
        return _ok_step2("María García", "maria-garcia", 0.95)

    floor_holder = _floor_holder(evidence="EVIDENCE_QUOTE_ONLY")
    resolve_announced_identity(floor_holder, _participants(), completion_fn=fake_completion)

    assert "EVIDENCE_QUOTE_ONLY" in captured["user"]
    assert "SENTINEL_WINDOW_TEXT_NOT_IN_STEP2" not in captured["user"]


def test_resolve_announced_identity_payload_contains_the_roster():
    captured = {}

    def fake_completion(system, user, **kw):
        captured["user"] = user
        return _ok_step2("María García", "maria-garcia", 0.95)

    resolve_announced_identity(_floor_holder(), _participants(("maria-garcia",)), completion_fn=fake_completion)

    assert "maria-garcia" in captured["user"]


@pytest.mark.parametrize(
    "response",
    [
        {"error": "boom", "data": None},
        {"error": None, "data": None},
        {"error": None, "data": "not-a-dict"},
    ],
)
def test_resolve_announced_identity_error_response_yields_sentinel_and_one_warning(response, caplog):
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        return response

    with caplog.at_level(logging.WARNING):
        result = resolve_announced_identity(_floor_holder(), _participants(), completion_fn=fake_completion)

    assert result == AnnouncedIdentity()
    assert len(call_count) == 1
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1


def test_resolve_announced_identity_raise_propagates_uncaught():
    def fake_completion(system, user, **kw):
        raise RuntimeError("OpenAI API error")

    with pytest.raises(RuntimeError, match="OpenAI API error"):
        resolve_announced_identity(_floor_holder(), _participants(), completion_fn=fake_completion)
