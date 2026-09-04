"""Unit tests for congress_videos.modules.monologue_speaker_window (issue #430).

Slice A1: window selection + prompt constants.
Slice A2a: FloorHolder/AnnouncedIdentity dataclasses and the two LLM-step seam
functions (identify_floor_holder, resolve_announced_identity).
Slice A2b: the never-raise orchestrator (resolve_monologue_speaker) that
loads SRT blocks, runs the announcement pre-gate, wires the two steps
together, and builds the audit JSON.
Slice C (this file, extended): one opt-in live_llm test -- the only honest
way to check the addressee/floor-holder distinction against a real model,
since every mocked test above is a pass-through proof, not a model-behaviour
proof (design.md Testing Strategy).
"""

from __future__ import annotations

import json
import logging
import os
from unittest.mock import patch

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
    resolve_monologue_speaker,
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


# ---------------------------------------------------------------------------
# resolve_monologue_speaker — never-raise orchestrator (slice A2b)
# ---------------------------------------------------------------------------

_MONOLOGUE_ANNOUNCEMENT_TEXT = "Tiene la palabra el señor García."


def _monologue_turn(start_seconds=500.0, turn_id=1):
    return {
        "turn_id": turn_id,
        "start_seconds": start_seconds,
        "video_id": "vidABC",
        "chapter_id": 10,
        "session_date": "2026-01-01",
    }


def _monologue_participants():
    return [{"slug": "pedro-garcia", "display_name": "Pedro García", "party": "TEST"}]


def _patched_monologue(all_blocks):
    """Context manager patching find_srt_for_chapter/_parse_srt_blocks on this
    module's own namespace, matching the mocking idiom used by
    test_speaker_resolution.py for the frozen module."""
    return (
        patch("congress_videos.modules.monologue_speaker_window.find_srt_for_chapter", return_value="/fake/src.srt"),
        patch("congress_videos.modules.monologue_speaker_window._parse_srt_blocks", return_value=all_blocks),
    )


def test_resolve_monologue_speaker_pre_gate_no_call_when_no_announcement_phrase():
    all_blocks = [_block(400.0, 410.0, "no handover text at all")]
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        return _ok_step1("García", "irrelevant")

    p1, p2 = _patched_monologue(all_blocks)
    with p1, p2:
        result = resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    assert result is None
    assert len(call_count) == 0


def test_resolve_monologue_speaker_payload_excludes_text_outside_the_window():
    anchor = 500.0
    all_blocks = [
        _block(anchor - MONOLOGUE_WINDOW_SECS - 50, anchor - MONOLOGUE_WINDOW_SECS - 40, "SENTINEL_BEFORE_WINDOW"),
        _block(anchor - 100, anchor - 90, _MONOLOGUE_ANNOUNCEMENT_TEXT),
        _block(anchor, anchor + 10, "SENTINEL_AFTER_ANCHOR"),
    ]
    captured = []

    def fake_completion(system, user, **kw):
        captured.append(user)
        if len(captured) == 1:
            return _ok_step1("García", _MONOLOGUE_ANNOUNCEMENT_TEXT)
        return _ok_step2("Pedro García", "pedro-garcia", 0.95)

    p1, p2 = _patched_monologue(all_blocks)
    with p1, p2:
        resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    for prompt in captured:
        assert "SENTINEL_BEFORE_WINDOW" not in prompt
        assert "SENTINEL_AFTER_ANCHOR" not in prompt


def test_resolve_monologue_speaker_found_false_stops_before_step_2():
    all_blocks = [_block(400.0, 410.0, _MONOLOGUE_ANNOUNCEMENT_TEXT)]
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        return _ok_step1(None, "", found=False)

    p1, p2 = _patched_monologue(all_blocks)
    with p1, p2:
        result = resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    assert result is None
    assert len(call_count) == 1


def test_resolve_monologue_speaker_unlocatable_evidence_returns_none():
    all_blocks = [_block(400.0, 410.0, _MONOLOGUE_ANNOUNCEMENT_TEXT)]
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        return _ok_step1("García", "this quote does not appear anywhere in the window blocks")

    p1, p2 = _patched_monologue(all_blocks)
    with p1, p2:
        result = resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    assert result is None
    assert len(call_count) == 1


def test_resolve_monologue_speaker_successful_resolution_shape_and_audit():
    all_blocks = [_block(400.0, 410.0, _MONOLOGUE_ANNOUNCEMENT_TEXT)]

    def fake_completion(system, user, **kw):
        if "ANNOUNCEMENT WINDOW" in user:
            return _ok_step1("García", _MONOLOGUE_ANNOUNCEMENT_TEXT)
        return _ok_step2("Pedro García", "pedro-garcia", 0.95)

    p1, p2 = _patched_monologue(all_blocks)
    with p1, p2:
        result = resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    assert result == {
        "participant_slug": "pedro-garcia",
        "confidence": 0.95,
        "evidence": _MONOLOGUE_ANNOUNCEMENT_TEXT,
        "audit": result["audit"],
    }
    audit = json.loads(result["audit"])
    assert set(audit) == {
        "announced_name_or_role",
        "evidence",
        "step1_found",
        "step2_confidence",
        "window_start_seconds",
        "anchor_seconds",
        "method",
    }
    assert audit["method"] == "monologue_window_v1"


@pytest.mark.parametrize("raising_step", [1, 2])
def test_resolve_monologue_speaker_never_raises_end_to_end(raising_step, caplog):
    all_blocks = [_block(400.0, 410.0, _MONOLOGUE_ANNOUNCEMENT_TEXT)]
    calls = []

    def fake_completion(system, user, **kw):
        calls.append(1)
        if len(calls) == raising_step:
            raise RuntimeError("OpenAI API error")
        return _ok_step1("García", _MONOLOGUE_ANNOUNCEMENT_TEXT)

    p1, p2 = _patched_monologue(all_blocks)
    with caplog.at_level(logging.WARNING), p1, p2:
        result = resolve_monologue_speaker(_monologue_turn(), _monologue_participants(), completion_fn=fake_completion)

    assert result is None
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1


# ---------------------------------------------------------------------------
# Opt-in live-LLM check (issue #430) -- skipped by default and in CI. Run
# with OPENAI_API_KEY and LIVE_LLM_TESTS=1 set to actually exercise the
# floor-holder prompt against the real model.
# ---------------------------------------------------------------------------


@pytest.mark.live_llm
@pytest.mark.skipif(
    not (os.getenv("OPENAI_API_KEY") and os.getenv("LIVE_LLM_TESTS") == "1"),
    reason="Opt-in: requires OPENAI_API_KEY and LIVE_LLM_TESTS=1",
)
def test_identify_floor_holder_live_model_resolves_full_name_announcement():
    """The real model, given 'Tiene la palabra la señora García', must find
    the floor holder and name García -- the one thing no mocked test above
    can prove."""
    from utils.llm_cache import cached_json_completion

    window_blocks = [_block(380.0, 385.0, "Tiene la palabra la señora García")]

    result = identify_floor_holder(window_blocks, completion_fn=cached_json_completion)

    assert result.found is True
    assert "García" in result.announced_name_or_role
