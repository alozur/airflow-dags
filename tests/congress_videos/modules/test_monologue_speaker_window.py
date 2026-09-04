"""Unit tests for congress_videos.modules.monologue_speaker_window (issue #430).

Slice A1: window selection + prompt constants only. Later slices (A2a/A2b) add
the LLM steps and the never-raise orchestrator.
"""

from __future__ import annotations

from congress_videos.config.ai_prompts import (
    MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT,
    MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE,
    MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT,
    MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE,
)
from congress_videos.modules.monologue_speaker_window import (
    MONOLOGUE_WINDOW_SECS,
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
