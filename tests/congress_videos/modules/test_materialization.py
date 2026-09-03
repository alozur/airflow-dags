"""[RED] Tests for congress_videos.modules.materialization — pure planning module.

Covers:
- KeepInterval frozen dataclass.
- MaterializationPlan frozen dataclass.
- plan_turn_materialization: long-turn/no-trims, long-turn/with-trims,
  unapproved trims ignored, is_voice_free=False trims ignored,
  short-turn grouping, cross-chapter isolation, gap-beyond-tolerance split,
  long-turn interrupts group, trim at start boundary, trim at end boundary.

All tests are pure: no ffmpeg, no DB, no Airflow imports.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

import pytest

from congress_videos.modules.materialization import (
    GROUP_GAP_TOLERANCE_SECS,
    MIN_LONG_INTERVENTION_SECS,
    MONOLOGUE,
    QA,
    KeepInterval,
    MaterializationPlan,
    classify_turn_type,
    plan_turn_materialization,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _turn(
    turn_id: int,
    chapter_id: int,
    start: float,
    end: float,
    is_procedural: bool = False,
) -> dict:
    """Minimal turn dict for plan_turn_materialization."""
    return {
        "turn_id": turn_id,
        "chapter_id": chapter_id,
        "start_seconds": start,
        "end_seconds": end,
        "is_procedural": is_procedural,
    }


def _trim(
    turn_id: int,
    start: float,
    end: float,
    is_approved: bool = True,
    is_voice_free: bool = True,
) -> dict:
    """Minimal approved trim proposal dict."""
    return {
        "turn_id": turn_id,
        "start_seconds": start,
        "end_seconds": end,
        "is_approved": is_approved,
        "is_voice_free": is_voice_free,
    }


# ---------------------------------------------------------------------------
# Dataclass sanity
# ---------------------------------------------------------------------------


class TestKeepInterval:
    def test_frozen(self):
        """KeepInterval must be immutable."""
        ki = KeepInterval(start=0.0, end=10.0)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            ki.start = 5.0  # type: ignore[misc]

    def test_fields(self):
        """KeepInterval must expose start and end floats."""
        ki = KeepInterval(start=1.5, end=3.5)
        assert ki.start == 1.5
        assert ki.end == 3.5


class TestMaterializationPlan:
    def test_frozen(self):
        """MaterializationPlan must be immutable."""
        plan = MaterializationPlan(
            turn_ids=(1,),
            chapter_id=10,
            keep_intervals=(KeepInterval(0.0, 300.0),),
            output_turn_id=1,
            needs_reencode=False,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            plan.needs_reencode = True  # type: ignore[misc]

    def test_fields(self):
        """MaterializationPlan must expose all required fields."""
        ki = KeepInterval(0.0, 300.0)
        plan = MaterializationPlan(
            turn_ids=(1,),
            chapter_id=7,
            keep_intervals=(ki,),
            output_turn_id=1,
            needs_reencode=False,
        )
        assert plan.turn_ids == (1,)
        assert plan.chapter_id == 7
        assert plan.keep_intervals == (ki,)
        assert plan.output_turn_id == 1
        assert plan.needs_reencode is False


# ---------------------------------------------------------------------------
# Task 1.8: Long turn, no trims → single keep = full window, needs_reencode=False
# ---------------------------------------------------------------------------


class TestLongTurnNoTrims:
    def test_single_plan_returned(self):
        """A lone long turn with no trims must produce exactly one plan."""
        turn = _turn(turn_id=1, chapter_id=5, start=0.0, end=600.0)
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert len(plans) == 1

    def test_turn_ids_contains_the_turn(self):
        """Plan turn_ids must contain the single long turn's id."""
        turn = _turn(turn_id=1, chapter_id=5, start=0.0, end=600.0)
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert plans[0].turn_ids == (1,)

    def test_keep_interval_is_full_window(self):
        """Single keep interval must span the full turn [start, end]."""
        turn = _turn(turn_id=1, chapter_id=5, start=100.0, end=700.0)
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert plans[0].keep_intervals == (KeepInterval(100.0, 700.0),)

    def test_needs_reencode_false(self):
        """No trims → no excision → needs_reencode must be False."""
        turn = _turn(turn_id=1, chapter_id=5, start=0.0, end=600.0)
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert plans[0].needs_reencode is False


# ---------------------------------------------------------------------------
# Task 1.9: Long turn with two approved trims → multiple keeps, needs_reencode=True
# ---------------------------------------------------------------------------


class TestLongTurnWithApprovedTrims:
    def _make_plans(self):
        # 600s turn, two non-overlapping trims at [120,150] and [300,360]
        turn = _turn(turn_id=2, chapter_id=1, start=0.0, end=600.0)
        trims = [
            _trim(turn_id=2, start=120.0, end=150.0),
            _trim(turn_id=2, start=300.0, end=360.0),
        ]
        return plan_turn_materialization([turn], approved_trims=trims)

    def test_one_plan(self):
        """Long turn with two trims produces exactly one plan."""
        assert len(self._make_plans()) == 1

    def test_three_keep_intervals(self):
        """Two trims excise two intervals → three keeper segments."""
        plan = self._make_plans()[0]
        assert len(plan.keep_intervals) == 3

    def test_keep_intervals_values(self):
        """Keep intervals must be the gaps between trim regions."""
        plan = self._make_plans()[0]
        assert plan.keep_intervals[0] == KeepInterval(0.0, 120.0)
        assert plan.keep_intervals[1] == KeepInterval(150.0, 300.0)
        assert plan.keep_intervals[2] == KeepInterval(360.0, 600.0)

    def test_needs_reencode_true(self):
        """Multiple keep intervals (excision) forces needs_reencode=True."""
        plan = self._make_plans()[0]
        assert plan.needs_reencode is True


# ---------------------------------------------------------------------------
# Task 1.10: Unapproved trims are ignored (caller filters, but function treats
#             passed trims as the approved set — unapproved trims in the list
#             must NOT be applied)
# ---------------------------------------------------------------------------


class TestUnapprovedTrimsIgnored:
    def test_unapproved_trim_produces_full_window(self):
        """A trim with is_approved=False in the input list must not be applied."""
        turn = _turn(turn_id=3, chapter_id=1, start=0.0, end=600.0)
        trims = [_trim(turn_id=3, start=100.0, end=200.0, is_approved=False)]
        plans = plan_turn_materialization([turn], approved_trims=trims)
        # Only the approved+voice_free subset counts; unapproved → full window
        assert plans[0].keep_intervals == (KeepInterval(0.0, 600.0),)
        assert plans[0].needs_reencode is False


# ---------------------------------------------------------------------------
# Task 1.11: is_voice_free=False trims are ignored
# ---------------------------------------------------------------------------


class TestVoiceFreeFilterApplied:
    def test_not_voice_free_trim_is_dropped(self):
        """A trim with is_voice_free=False must not be excised even if approved."""
        turn = _turn(turn_id=4, chapter_id=1, start=0.0, end=600.0)
        trims = [
            _trim(turn_id=4, start=100.0, end=200.0, is_approved=True, is_voice_free=False),
            _trim(turn_id=4, start=300.0, end=400.0, is_approved=True, is_voice_free=True),
        ]
        plans = plan_turn_materialization([turn], approved_trims=trims)
        # Only the voice-free+approved one is applied → two keep intervals
        assert len(plans[0].keep_intervals) == 2
        assert plans[0].keep_intervals[0] == KeepInterval(0.0, 300.0)
        assert plans[0].keep_intervals[1] == KeepInterval(400.0, 600.0)


# ---------------------------------------------------------------------------
# Task 1.12: Short-turn grouping (2 consecutive same-chapter turns)
# ---------------------------------------------------------------------------


class TestShortTurnGrouping:
    def test_two_adjacent_short_turns_become_one_plan(self):
        """Two consecutive short same-chapter turns must produce one plan."""
        # Each turn is 60s, well below 300s threshold
        t_a = _turn(turn_id=10, chapter_id=2, start=0.0, end=60.0)
        t_b = _turn(turn_id=11, chapter_id=2, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert len(plans) == 1

    def test_grouped_turn_ids(self):
        """Grouped plan must contain both turn_ids."""
        t_a = _turn(turn_id=10, chapter_id=2, start=0.0, end=60.0)
        t_b = _turn(turn_id=11, chapter_id=2, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert set(plans[0].turn_ids) == {10, 11}

    def test_grouped_keep_interval_spans_both(self):
        """Grouped keep interval must span [first.start, last.end]."""
        t_a = _turn(turn_id=10, chapter_id=2, start=0.0, end=60.0)
        t_b = _turn(turn_id=11, chapter_id=2, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert plans[0].keep_intervals == (KeepInterval(0.0, 120.0),)

    def test_grouped_needs_reencode_false(self):
        """Grouped short-turn plan has no excision → needs_reencode=False."""
        t_a = _turn(turn_id=10, chapter_id=2, start=0.0, end=60.0)
        t_b = _turn(turn_id=11, chapter_id=2, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert plans[0].needs_reencode is False


# ---------------------------------------------------------------------------
# Task 1.13: Cross-chapter short turns are NOT grouped
# ---------------------------------------------------------------------------


class TestCrossChapterNotGrouped:
    def test_different_chapters_produce_separate_plans(self):
        """Short turns in different chapters must each produce their own plan."""
        t_a = _turn(turn_id=20, chapter_id=3, start=0.0, end=60.0)
        t_b = _turn(turn_id=21, chapter_id=4, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert len(plans) == 2

    def test_each_plan_has_own_turn_id(self):
        """Each separate-chapter plan must contain only its own turn_id."""
        t_a = _turn(turn_id=20, chapter_id=3, start=0.0, end=60.0)
        t_b = _turn(turn_id=21, chapter_id=4, start=60.0, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        turn_id_sets = [set(p.turn_ids) for p in plans]
        assert {20} in turn_id_sets
        assert {21} in turn_id_sets


# ---------------------------------------------------------------------------
# Task 1.14: Gap beyond tolerance breaks grouping (gap=0.6 > 0.5 → two plans)
# ---------------------------------------------------------------------------


class TestGapBeyondToleranceSplitsGroup:
    def test_gap_exceeds_tolerance(self):
        """Short turns with a gap > GROUP_GAP_TOLERANCE_SECS must not be grouped."""
        t_a = _turn(turn_id=30, chapter_id=5, start=0.0, end=60.0)
        # 0.6s gap > 0.5 tolerance
        t_b = _turn(turn_id=31, chapter_id=5, start=60.6, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert len(plans) == 2

    def test_gap_within_tolerance_still_groups(self):
        """Short turns with a gap <= GROUP_GAP_TOLERANCE_SECS must be grouped."""
        t_a = _turn(turn_id=32, chapter_id=5, start=0.0, end=60.0)
        # 0.4s gap < 0.5 tolerance → grouped
        t_b = _turn(turn_id=33, chapter_id=5, start=60.4, end=120.0)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert len(plans) == 1


# ---------------------------------------------------------------------------
# Task 1.15: Long turn interrupts a short-turn group (three separate plans)
# ---------------------------------------------------------------------------


class TestLongTurnInterruptsGroup:
    def test_three_plans_for_short_long_short(self):
        """[short_A, long_B, short_C] in same chapter → 3 separate plans."""
        t_a = _turn(turn_id=40, chapter_id=6, start=0.0, end=60.0)  # short
        t_b = _turn(turn_id=41, chapter_id=6, start=60.0, end=400.0)  # long (340s)
        t_c = _turn(turn_id=42, chapter_id=6, start=400.0, end=460.0)  # short
        plans = plan_turn_materialization([t_a, t_b, t_c], approved_trims=[])
        assert len(plans) == 3

    def test_each_plan_contains_single_turn_id(self):
        """Each of the three plans must contain a single turn_id."""
        t_a = _turn(turn_id=40, chapter_id=6, start=0.0, end=60.0)
        t_b = _turn(turn_id=41, chapter_id=6, start=60.0, end=400.0)
        t_c = _turn(turn_id=42, chapter_id=6, start=400.0, end=460.0)
        plans = plan_turn_materialization([t_a, t_b, t_c], approved_trims=[])
        assert len(plans) == 3
        for p in plans:
            assert len(p.turn_ids) == 1


# ---------------------------------------------------------------------------
# Task 1.16: Approved trim at turn start boundary (trim.start == turn.start)
# ---------------------------------------------------------------------------


class TestTrimAtStartBoundary:
    def test_no_zero_duration_leading_segment(self):
        """Trim starting at turn start must not produce a [start, start] interval."""
        turn = _turn(turn_id=50, chapter_id=7, start=0.0, end=600.0)
        trims = [_trim(turn_id=50, start=0.0, end=60.0)]  # trim from very beginning
        plans = plan_turn_materialization([turn], approved_trims=trims)
        ki = plans[0].keep_intervals
        # First keep must start at 60.0, not 0.0
        assert all(interval.end > interval.start for interval in ki), "No zero-duration keep interval must be produced"
        assert ki[0].start == 60.0

    def test_single_keep_after_start_trim(self):
        """Trim at turn start → one keep interval covering [trim.end, turn.end]."""
        turn = _turn(turn_id=50, chapter_id=7, start=0.0, end=600.0)
        trims = [_trim(turn_id=50, start=0.0, end=60.0)]
        plans = plan_turn_materialization([turn], approved_trims=trims)
        assert len(plans[0].keep_intervals) == 1
        assert plans[0].keep_intervals[0] == KeepInterval(60.0, 600.0)


# ---------------------------------------------------------------------------
# Task 1.17: Approved trim at turn end boundary (trim.end == turn.end)
# ---------------------------------------------------------------------------


class TestTrimAtEndBoundary:
    def test_no_zero_duration_trailing_segment(self):
        """Trim ending at turn end must not produce a [end, end] interval."""
        turn = _turn(turn_id=51, chapter_id=7, start=0.0, end=600.0)
        trims = [_trim(turn_id=51, start=540.0, end=600.0)]  # trim at very end
        plans = plan_turn_materialization([turn], approved_trims=trims)
        ki = plans[0].keep_intervals
        assert all(interval.end > interval.start for interval in ki), "No zero-duration keep interval must be produced"
        assert ki[-1].end == 540.0

    def test_single_keep_before_end_trim(self):
        """Trim at turn end → one keep interval covering [turn.start, trim.start]."""
        turn = _turn(turn_id=51, chapter_id=7, start=0.0, end=600.0)
        trims = [_trim(turn_id=51, start=540.0, end=600.0)]
        plans = plan_turn_materialization([turn], approved_trims=trims)
        assert len(plans[0].keep_intervals) == 1
        assert plans[0].keep_intervals[0] == KeepInterval(0.0, 540.0)


# ---------------------------------------------------------------------------
# Procedural-turn excision within a group (issue #143)
# ---------------------------------------------------------------------------


class TestProceduralExcisionMiddleOfGroup:
    """Q(0-30s) -> handoff(30-36s, procedural) -> A(36-80s) -> keep=[(0,30),(36,80)]."""

    def _make_plans(self):
        q = _turn(turn_id=60, chapter_id=8, start=0.0, end=30.0)
        handoff = _turn(turn_id=61, chapter_id=8, start=30.0, end=36.0, is_procedural=True)
        a = _turn(turn_id=62, chapter_id=8, start=36.0, end=80.0)
        return plan_turn_materialization([q, handoff, a], approved_trims=[])

    def test_one_plan(self):
        assert len(self._make_plans()) == 1

    def test_turn_ids_include_the_procedural_member(self):
        """turn_ids must include ALL group members, including the excised one,
        so mark_turns_uploaded/select_turns idempotency still sees it."""
        plan = self._make_plans()[0]
        assert set(plan.turn_ids) == {60, 61, 62}

    def test_keep_intervals_exclude_the_handoff_span(self):
        plan = self._make_plans()[0]
        assert plan.keep_intervals == (KeepInterval(0.0, 30.0), KeepInterval(36.0, 80.0))

    def test_needs_reencode_true(self):
        """Two keep intervals (excision) forces needs_reencode=True."""
        assert self._make_plans()[0].needs_reencode is True


class TestProceduralExcisionStartAndEndOfGroup:
    """handoff(0-5s) -> Q(5-40s) -> A(40-70s) -> handoff(70-75s) -> keep=[(5,70)]."""

    def _make_plans(self):
        h1 = _turn(turn_id=70, chapter_id=9, start=0.0, end=5.0, is_procedural=True)
        q = _turn(turn_id=71, chapter_id=9, start=5.0, end=40.0)
        a = _turn(turn_id=72, chapter_id=9, start=40.0, end=70.0)
        h2 = _turn(turn_id=73, chapter_id=9, start=70.0, end=75.0, is_procedural=True)
        return plan_turn_materialization([h1, q, a, h2], approved_trims=[])

    def test_one_plan(self):
        assert len(self._make_plans()) == 1

    def test_turn_ids_include_both_procedural_edges(self):
        plan = self._make_plans()[0]
        assert set(plan.turn_ids) == {70, 71, 72, 73}

    def test_keep_interval_collapses_to_single_window(self):
        """Edge-only excision collapses to ONE keep interval — stream-copy stays
        possible (needs_reencode=False)."""
        plan = self._make_plans()[0]
        assert plan.keep_intervals == (KeepInterval(5.0, 70.0),)

    def test_needs_reencode_false_for_edge_only_excision(self):
        assert self._make_plans()[0].needs_reencode is False


class TestProceduralExcisionDegenerateAllProceduralGroup:
    """A group whose every member is procedural must yield NO plan at all."""

    def test_no_plan_produced(self):
        h1 = _turn(turn_id=80, chapter_id=10, start=0.0, end=5.0, is_procedural=True)
        h2 = _turn(turn_id=81, chapter_id=10, start=5.0, end=10.0, is_procedural=True)
        plans = plan_turn_materialization([h1, h2], approved_trims=[])
        assert plans == []

    def test_no_plan_even_with_a_third_solo_group_alongside(self):
        """The degenerate group must vanish while an unrelated chapter's turn
        still produces its own plan — the drop is scoped to the one group."""
        h1 = _turn(turn_id=82, chapter_id=11, start=0.0, end=5.0, is_procedural=True)
        h2 = _turn(turn_id=83, chapter_id=11, start=5.0, end=10.0, is_procedural=True)
        other = _turn(turn_id=84, chapter_id=12, start=0.0, end=60.0)
        plans = plan_turn_materialization([h1, h2, other], approved_trims=[])
        assert len(plans) == 1
        assert plans[0].turn_ids == (84,)


class TestProceduralUnflaggedGroupByteIdentical:
    """A group with no procedural member must be byte-identical to pre-#143
    behavior — is_procedural=False on every member changes nothing."""

    def test_keep_interval_unaffected(self):
        t_a = _turn(turn_id=90, chapter_id=13, start=0.0, end=60.0, is_procedural=False)
        t_b = _turn(turn_id=91, chapter_id=13, start=60.0, end=120.0, is_procedural=False)
        plans = plan_turn_materialization([t_a, t_b], approved_trims=[])
        assert len(plans) == 1
        assert plans[0].keep_intervals == (KeepInterval(0.0, 120.0),)
        assert plans[0].needs_reencode is False

    def test_missing_is_procedural_key_defaults_to_unflagged(self):
        """A turn dict with NO is_procedural key at all (legacy caller) must
        behave exactly as if is_procedural=False."""
        turn = {"turn_id": 92, "chapter_id": 14, "start_seconds": 0.0, "end_seconds": 600.0}
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert plans[0].keep_intervals == (KeepInterval(0.0, 600.0),)
        assert plans[0].needs_reencode is False


class TestProceduralLongTurnPathUnaffected:
    """A >=300s turn can never trip the <=15s detection gate, but this guards
    the materialization long-turn branch never even inspects is_procedural."""

    def test_long_turn_flagged_is_procedural_still_ignored_by_long_path(self):
        """Defensive: even if is_procedural were (incorrectly) True on a long
        turn upstream, the long-turn branch keeps its own full-window/trim
        behavior untouched — is_procedural is only consulted for grouping."""
        turn = _turn(turn_id=95, chapter_id=15, start=0.0, end=600.0, is_procedural=True)
        plans = plan_turn_materialization([turn], approved_trims=[])
        assert len(plans) == 1
        assert plans[0].keep_intervals == (KeepInterval(0.0, 600.0),)
        assert plans[0].needs_reencode is False


# ---------------------------------------------------------------------------
# Module constant assertions (triangulation guard)
# ---------------------------------------------------------------------------


class TestModuleConstants:
    def test_min_long_intervention_secs_value(self):
        """MIN_LONG_INTERVENTION_SECS must be 300.0."""
        assert MIN_LONG_INTERVENTION_SECS == 300.0

    def test_group_gap_tolerance_secs_value(self):
        """GROUP_GAP_TOLERANCE_SECS must be 0.5."""
        assert GROUP_GAP_TOLERANCE_SECS == 0.5


# ---------------------------------------------------------------------------
# classify_turn_type — pure turn-type classifier
# ---------------------------------------------------------------------------


def _rows(turn_ids: list[int], label: str = "SPEAKER_00") -> dict[int, dict]:
    """Build a turn_rows_by_id map where EVERY turn shares one speaker_label.

    The label path is therefore always inconclusive (<2 distinct substantial
    labels survive), so it returns None and classify_turn_type falls through
    to the name-based rule — letting the existing name-rule assertions below
    keep governing the asserted outcome (issue #282, task 3.1).
    """
    rows: dict[int, dict] = {}
    for i, tid in enumerate(turn_ids):
        start = float(i * 10)
        rows[tid] = {
            "start_seconds": start,
            "end_seconds": start + 5.0,
            "speaker_label": label,
        }
    return rows


class TestClassifyTurnType:
    """Unit tests for classify_turn_type (pure function, no I/O).

    3-arg signature (issue #282): turn_rows_by_id is now required. Every
    row here shares one speaker_label via _rows(), so the label path is
    inconclusive and the pre-existing name-based rule governs each outcome.
    """

    def test_single_turn_is_monologue(self):
        """A group of exactly one turn always yields 'monologue'."""
        resolved = {1: "Maria Lopez"}
        assert classify_turn_type([1], resolved, _rows([1])) == MONOLOGUE

    def test_all_null_resolved_names_is_monologue(self):
        """All-NULL resolved_names (all unknown) → 'monologue'."""
        resolved = {1: None, 2: None, 3: None}
        assert classify_turn_type([1, 2, 3], resolved, _rows([1, 2, 3])) == MONOLOGUE

    def test_all_placeholder_names_is_monologue(self):
        """All placeholder resolved_names → 'monologue'."""
        resolved = {1: "desconocido", 2: "unknown", 3: "(no especificado)"}
        assert classify_turn_type([1, 2, 3], resolved, _rows([1, 2, 3])) == MONOLOGUE

    def test_one_real_plus_unknowns_is_monologue(self):
        """One real name + several NULL/placeholder → 'monologue'."""
        resolved = {1: "Pedro Sanchez", 2: None, 3: "desconocido", 4: None}
        assert classify_turn_type([1, 2, 3, 4], resolved, _rows([1, 2, 3, 4])) == MONOLOGUE

    def test_two_distinct_real_names_is_qa(self):
        """Two distinct non-placeholder, non-NULL names → 'qa'."""
        resolved = {1: "Pedro Sanchez", 2: "Alberto Gonzalez"}
        assert classify_turn_type([1, 2], resolved, _rows([1, 2])) == QA

    def test_two_turns_same_real_name_is_monologue(self):
        """Repeated real name does not count as two distinct speakers → 'monologue'."""
        resolved = {1: "Pedro Sanchez", 2: "Pedro Sanchez"}
        assert classify_turn_type([1, 2], resolved, _rows([1, 2])) == MONOLOGUE

    def test_role_only_placeholder_skipped(self):
        """Role-only string (e.g. 'Portavoz del grupo') is a placeholder → skipped."""
        # 'Portavoz del grupo' matches _ROLE_ONLY_RE → placeholder → monologue
        resolved = {1: "Pedro Sanchez", 2: "Portavoz del grupo"}
        assert classify_turn_type([1, 2], resolved, _rows([1, 2])) == MONOLOGUE

    def test_constants_have_correct_values(self):
        """MONOLOGUE and QA module constants must have the string values they document."""
        assert MONOLOGUE == "monologue"
        assert QA == "qa"

    def test_mixed_real_and_placeholder_with_two_real_is_qa(self):
        """Two distinct real names even when some turns are placeholder → 'qa'."""
        resolved = {1: "Pedro Sanchez", 2: None, 3: "Alberto Gonzalez"}
        assert classify_turn_type([1, 2, 3], resolved, _rows([1, 2, 3])) == QA


class TestClassifyTurnTypeLabelRules:
    """Label-first classification rules fed by real speaker_label/duration
    data (issue #282). The label path takes priority over the name-based
    fallback whenever >=2 substantial distinct labels survive noise
    filtering (drop_micro_segments -> collapse_foreign_runs, D4)."""

    def test_two_substantial_labels_with_null_names_is_qa(self):
        """>=2 distinct substantial labels -> 'qa' even with all-NULL names."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "SPEAKER_00"},
            2: {"start_seconds": 10.0, "end_seconds": 20.0, "speaker_label": "SPEAKER_01"},
        }
        resolved = {1: None, 2: None}
        assert classify_turn_type([1, 2], resolved, rows) == QA

    def test_sub_second_blip_label_excluded(self):
        """A label surviving only via a <1s segment never counts as substantial."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "SPEAKER_00"},
            2: {"start_seconds": 10.0, "end_seconds": 10.5, "speaker_label": "SPEAKER_01"},
            3: {"start_seconds": 10.5, "end_seconds": 20.0, "speaker_label": "SPEAKER_00"},
        }
        resolved = {1: None, 2: None, 3: None}
        assert classify_turn_type([1, 2, 3], resolved, rows) == MONOLOGUE

    def test_foreign_run_under_10s_collapses_to_name_rule(self):
        """A <10s foreign run bounded by the same label collapses away, leaving
        1 distinct label -> falls through to the (inconclusive) name rule."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "A"},
            2: {"start_seconds": 10.0, "end_seconds": 15.0, "speaker_label": "B"},
            3: {"start_seconds": 15.0, "end_seconds": 25.0, "speaker_label": "A"},
        }
        resolved = {1: None, 2: None, 3: None}
        assert classify_turn_type([1, 2, 3], resolved, rows) == MONOLOGUE

    def test_chained_foreign_runs_cascade(self):
        """A chain of qualifying <10s foreign runs must all collapse into the
        anchor label, not just the first one."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "A"},
            2: {"start_seconds": 10.0, "end_seconds": 13.0, "speaker_label": "B"},
            3: {"start_seconds": 13.0, "end_seconds": 20.0, "speaker_label": "A"},
            4: {"start_seconds": 20.0, "end_seconds": 23.0, "speaker_label": "C"},
            5: {"start_seconds": 23.0, "end_seconds": 30.0, "speaker_label": "A"},
        }
        resolved = {1: None, 2: None, 3: None, 4: None, 5: None}
        assert classify_turn_type([1, 2, 3, 4, 5], resolved, rows) == MONOLOGUE

    def test_decimal_seconds_handled(self):
        """Postgres NUMERIC seconds (decimal.Decimal) must not raise; two
        distinct substantial labels still yield 'qa'."""
        rows = {
            1: {"start_seconds": Decimal("0.0"), "end_seconds": Decimal("10.0"), "speaker_label": "A"},
            2: {"start_seconds": Decimal("10.0"), "end_seconds": Decimal("20.0"), "speaker_label": "B"},
        }
        resolved = {1: None, 2: None}
        assert classify_turn_type([1, 2], resolved, rows) == QA

    def test_missing_row_falls_back_to_name_rule(self):
        """turn_ids absent from turn_rows_by_id are excluded from the label
        path; fewer than 2 usable rows -> falls through to the name rule."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "A"},
        }
        resolved = {1: None, 2: "Pedro Sanchez", 3: "Alberto Gonzalez"}
        assert classify_turn_type([1, 2, 3], resolved, rows) == QA

    def test_none_or_blank_label_ignored(self):
        """None or blank speaker_label rows are excluded from the label path."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": None},
            2: {"start_seconds": 10.0, "end_seconds": 20.0, "speaker_label": ""},
            3: {"start_seconds": 20.0, "end_seconds": 30.0, "speaker_label": "A"},
        }
        resolved = {1: "Pedro Sanchez", 2: None, 3: "Alberto Gonzalez"}
        assert classify_turn_type([1, 2, 3], resolved, rows) == QA

    def test_two_labels_plus_one_real_name_never_monologue(self):
        """The label path wins outright when it qualifies — a single real
        name among the group must never pull the result back to monologue."""
        rows = {
            1: {"start_seconds": 0.0, "end_seconds": 10.0, "speaker_label": "A"},
            2: {"start_seconds": 10.0, "end_seconds": 20.0, "speaker_label": "B"},
        }
        resolved = {1: "Pedro Sanchez", 2: None}
        assert classify_turn_type([1, 2], resolved, rows) == QA


class TestClassifyTurnTypeChapter263Regression:
    """Regression for issue #282 using the REAL turns 202-210 shape from
    chapter 263 / video 4htFnCncrkw (same source data as the #283 postproc
    fixture in test_speaker_turns.py's _chapter_263_segments()). Pre-fix,
    classify_turn_type never read speaker_label/duration, so this
    label-diverse, names-never-resolved group was frozen at 'monologue'."""

    def test_chapter_263_turns_202_210_regression(self):
        turn_ids = list(range(202, 211))
        real_segments = [
            (8870.00, 8934.683, "SPEAKER_01"),  # 202
            (8934.683, 8934.763, "SPEAKER_03"),  # 203 (0.08s blip)
            (8934.763, 8934.913, "SPEAKER_01"),  # 204 (0.15s blip)
            (8934.913, 8934.930, "SPEAKER_03"),  # 205 (0.017s blip)
            (8934.930, 9158.630, "SPEAKER_01"),  # 206 (223.7s)
            (9158.610, 9180.410, "SPEAKER_00"),  # 207 (21.8s — must survive)
            (9180.400, 9453.900, "SPEAKER_04"),  # 208
            (9453.900, 9454.400, "SPEAKER_01"),  # 209 (0.5s blip)
            (9461.160, 9844.560, "SPEAKER_04"),  # 210
        ]
        rows = {
            tid: {"start_seconds": s, "end_seconds": e, "speaker_label": label}
            for tid, (s, e, label) in zip(turn_ids, real_segments)
        }
        resolved_by_id = dict.fromkeys(turn_ids)  # names never resolved (the bug symptom)

        assert classify_turn_type(turn_ids, resolved_by_id, rows) == QA


class TestDecimalInputs:
    """Postgres NUMERIC columns arrive as decimal.Decimal via psycopg2.

    Regression for the prod-shaped crash: consecutive short turns with
    Decimal seconds hit ``Decimal + float`` in the group-gap comparison
    (TypeError). All second fields must be coerced to float at the
    function boundary without mutating caller dicts.
    """

    def test_short_turn_grouping_accepts_decimal_seconds(self):
        """Consecutive short Decimal turns must group without TypeError."""
        turns = [
            _turn(1, 410, Decimal("21195.0"), Decimal("21196.0")),
            _turn(2, 410, Decimal("21196.0"), Decimal("21197.0")),
        ]

        plans = plan_turn_materialization(turns, [])

        assert len(plans) == 1
        assert plans[0].turn_ids == (1, 2)

    def test_long_turn_with_decimal_trim_yields_float_intervals(self):
        """Decimal turn bounds and trims must produce float KeepIntervals."""
        turns = [_turn(1, 410, Decimal("0"), Decimal("400.0"))]
        trims = [_trim(1, Decimal("10.0"), Decimal("20.0"))]

        plans = plan_turn_materialization(turns, trims)

        assert len(plans) == 1
        keep = plans[0].keep_intervals
        assert keep == (KeepInterval(0.0, 10.0), KeepInterval(20.0, 400.0))
        assert all(isinstance(iv.start, float) and isinstance(iv.end, float) for iv in keep)

    def test_input_dicts_are_not_mutated(self):
        """Coercion must copy rows, never mutate the caller's dicts."""
        turn = _turn(1, 410, Decimal("21195.0"), Decimal("21196.0"))

        plan_turn_materialization([turn], [])

        assert isinstance(turn["start_seconds"], Decimal)
        assert isinstance(turn["end_seconds"], Decimal)
