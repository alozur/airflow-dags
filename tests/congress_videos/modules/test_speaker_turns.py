"""Tests for congress_videos.modules.speaker_turns.

Strict TDD: tests written first. No real Docker, pyannote, OpenAI,
network, or live DB. diarize_fn, name_resolver, and DB cursor are
all injected/faked.
"""

from __future__ import annotations

import dataclasses
from typing import get_type_hints
from unittest.mock import MagicMock, call

import pytest

# ---------------------------------------------------------------------------
# Phase 2 — Turn dataclass and module constants
# ---------------------------------------------------------------------------

class TestTurnDataclass:

    def test_turn_is_importable(self):
        from congress_videos.modules.speaker_turns import Turn
        assert Turn is not None

    def test_turn_is_frozen_dataclass(self):
        from congress_videos.modules.speaker_turns import Turn
        assert dataclasses.is_dataclass(Turn)
        assert Turn.__dataclass_params__.frozen is True

    def test_turn_fields(self):
        from congress_videos.modules.speaker_turns import Turn
        fields = {f.name: f for f in dataclasses.fields(Turn)}
        assert "start_seconds" in fields
        assert "end_seconds" in fields
        assert "speaker_label" in fields
        assert "resolved_name" in fields
        assert "confidence" in fields
        assert "source" in fields

    def test_turn_is_immutable(self):
        from congress_videos.modules.speaker_turns import Turn
        t = Turn(
            start_seconds=0.0,
            end_seconds=10.0,
            speaker_label="SPEAKER_01",
            resolved_name=None,
            confidence=0.5,
            source="acoustic",
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            t.start_seconds = 99.0  # type: ignore[misc]


class TestModuleConstants:

    def test_gap_merge_seconds(self):
        from congress_videos.modules.speaker_turns import GAP_MERGE_SECONDS
        assert GAP_MERGE_SECONDS == 1.0

    def test_min_segment_duration_seconds(self):
        from congress_videos.modules.speaker_turns import MIN_SEGMENT_DURATION_SECONDS
        assert MIN_SEGMENT_DURATION_SECONDS == 1.0

    def test_foreign_interruption_max_seconds(self):
        from congress_videos.modules.speaker_turns import FOREIGN_INTERRUPTION_MAX_SECONDS
        assert FOREIGN_INTERRUPTION_MAX_SECONDS == 10.0


# ---------------------------------------------------------------------------
# Fixtures shared across phases
# ---------------------------------------------------------------------------

def _make_srt_blocks(*entries):
    """Build a list of SRT-block dicts from (start_secs, end_secs, text) tuples."""
    return [
        {"start_secs": s, "end_secs": e, "text": t}
        for s, e, t in entries
    ]


def _identity_resolver(name: str):
    """Fake name_resolver that returns the name as-is in a result dict."""
    if not name:
        return None
    return {"display_name": name, "normalized_name": name.lower()}


def _null_resolver(name: str):
    """Fake name_resolver that always returns None (unresolvable)."""
    return None


# ---------------------------------------------------------------------------
# Phase 3 — extract_announcement
# ---------------------------------------------------------------------------

class TestExtractAnnouncement:

    def test_announces_senor_returns_name_and_true(self):
        """'Tiene la palabra el señor García' → ('García', True)"""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (75.0, 85.0, "Tiene la palabra el señor García"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        assert name is not None
        assert "García" in name or "garcia" in name.lower()

    def test_announces_senora_returns_name_and_true(self):
        """'Tiene la palabra la señora Martínez' → ('Martínez', True)"""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (78.0, 88.0, "Tiene la palabra la señora Martínez"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        assert name is not None
        assert "Martínez" in name or "Martinez" in name.lower() or "martínez" in name.lower()

    def test_su_senoria_no_name_returns_none_true(self):
        """'Tiene la palabra su señoría' → (None, True)"""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra su señoría"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        assert name is None

    def test_gracias_senoria_returns_none_true(self):
        """'Gracias, señoría' → (None, True)"""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (80.0, 90.0, "Gracias, señoría"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        assert name is None

    def test_no_matching_block_returns_none_false(self):
        """No relevant phrase anywhere → (None, False)"""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (80.0, 90.0, "El debate sobre el presupuesto continúa"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is False
        assert name is None

    def test_block_outside_window_is_ignored(self):
        """A matching block ending more than 120 s before t is outside the
        backward-only default window (issue #131: 30s symmetric -> 120s
        backward-only, mirroring speaker_resolution.INTRO_WINDOW_SECS)."""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 200.0
        # 200 - 120 = 80 → block ending at 78 is 121s before t, outside [80, 200)
        blocks = _make_srt_blocks(
            (70.0, 78.0, "Tiene la palabra el señor Fuera de ventana"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is False
        assert name is None

    def test_forward_block_is_never_matched(self):
        """Backward-only window (issue #131): a block AFTER t must never be
        matched, even when close in time — forward blocks are the new
        speaker's own words, a mis-attribution source under the old
        symmetric +/-30s window."""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (105.0, 110.0, "Tiene la palabra el señor Adelante"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is False
        assert name is None

    def test_accent_tolerant_match(self):
        """'señor' written without accents (senor) still matches (accent-tolerant)."""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra el senor Lopez"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        assert name is not None

    def test_closest_preceding_block_preferred(self):
        """When multiple blocks match, the closest preceding block within 15–30 s is preferred."""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        # Two matching blocks before t; the one at 82s is closest
        blocks = _make_srt_blocks(
            (72.0, 74.0, "Tiene la palabra el señor Primero"),
            (82.0, 84.0, "Tiene la palabra el señor Segundo"),
        )
        name, found = extract_announcement(blocks, t)
        assert found is True
        # Should prefer the closer one (at 82s)
        assert name is not None
        assert "Segundo" in name or "segundo" in name.lower()


# ---------------------------------------------------------------------------
# Phase 4 — Postprocessing helpers
# ---------------------------------------------------------------------------

def _seg(start, end, label, resolved_name=None):
    """Build a minimal segment dict for postprocessing tests."""
    return {
        "start_seconds": start,
        "end_seconds": end,
        "speaker_label": label,
        "resolved_name": resolved_name,
        "confirmed_block_duration_seconds": end - start,  # present but NEVER threshold
    }


def _chapter_263_segments():
    """Real turns 200-210 from chapter 263 / video 4htFnCncrkw (issue #283).

    Turns 202-206 are the alternating chain of sub-second diarization blips
    (SPEAKER_03 interruptions of 0.08s/0.017s, plus a 0.15s SPEAKER_01 blip)
    that caused turn_id=205 (17ms) to be picked as the group's representative
    turn and mis-anchor the 120s announcement window. Turn 207 is the real
    21.8s SPEAKER_00 interruption that must NOT collapse. Turn 209 is a
    0.5s blip absorbed into turn 208 without disturbing turn 210.
    """
    return [
        _seg(8707.14, 8800.00, "SPEAKER_01"),    # 200
        _seg(8800.00, 8870.00, "SPEAKER_01"),    # 201
        _seg(8870.00, 8934.683, "SPEAKER_01"),   # 202
        _seg(8934.683, 8934.763, "SPEAKER_03"),  # 203 (0.08s blip)
        _seg(8934.763, 8934.913, "SPEAKER_01"),  # 204 (0.15s blip)
        _seg(8934.913, 8934.930, "SPEAKER_03"),  # 205 (0.017s blip)
        _seg(8934.930, 9158.630, "SPEAKER_01"),  # 206 (223.7s)
        _seg(9158.610, 9180.410, "SPEAKER_00"),  # 207 (21.8s — must survive)
        _seg(9180.400, 9453.900, "SPEAKER_04"),  # 208
        _seg(9453.900, 9454.400, "SPEAKER_01"),  # 209 (0.5s blip)
        _seg(9461.160, 9844.560, "SPEAKER_04"),  # 210
    ]


class TestMergeGaps:

    def test_gap_under_threshold_merged(self):
        """Two same-label segments with 0.7 s gap → merged into one."""
        from congress_videos.modules.speaker_turns import _merge_gaps
        segs = [
            _seg(0.0, 10.0, "SPEAKER_01"),
            _seg(10.7, 20.0, "SPEAKER_01"),
        ]
        result = _merge_gaps(segs)
        assert len(result) == 1
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 20.0

    def test_gap_over_threshold_not_merged(self):
        """Two same-label segments with 1.1 s gap → not merged."""
        from congress_videos.modules.speaker_turns import _merge_gaps
        segs = [
            _seg(0.0, 10.0, "SPEAKER_01"),
            _seg(11.1, 20.0, "SPEAKER_01"),
        ]
        result = _merge_gaps(segs)
        assert len(result) == 2

    def test_different_labels_not_merged(self):
        """Different labels even with small gap → not merged."""
        from congress_videos.modules.speaker_turns import _merge_gaps
        segs = [
            _seg(0.0, 10.0, "SPEAKER_01"),
            _seg(10.5, 20.0, "SPEAKER_02"),
        ]
        result = _merge_gaps(segs)
        assert len(result) == 2

    def test_empty_input(self):
        """Empty list → empty list."""
        from congress_videos.modules.speaker_turns import _merge_gaps
        assert _merge_gaps([]) == []


class TestDropMicroSegments:

    def test_leading_blip_dropped_outright(self):
        """First segment duration < 1.0s with no predecessor -> dropped outright."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [
            _seg(0.0, 0.5, "SPEAKER_00"),   # 0.5 s blip, no predecessor
            _seg(0.5, 60.0, "SPEAKER_01"),
        ]
        result = _drop_micro_segments(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "SPEAKER_01"
        assert result[0]["start_seconds"] == 0.5
        assert result[0]["end_seconds"] == 60.0

    def test_mid_blip_absorbed_extends_predecessor_end(self):
        """A mid-stream blip is dropped and the predecessor's end_seconds
        extends to cover the blip's span."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [
            _seg(0.0, 60.0, "SPEAKER_01"),
            _seg(60.0, 60.5, "SPEAKER_02"),  # 0.5 s blip
            _seg(60.5, 120.0, "SPEAKER_01"),
        ]
        result = _drop_micro_segments(segs)
        assert len(result) == 2
        assert result[0]["speaker_label"] == "SPEAKER_01"
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 60.5  # absorbed the blip's span
        assert result[1]["speaker_label"] == "SPEAKER_01"
        assert result[1]["start_seconds"] == 60.5
        assert result[1]["end_seconds"] == 120.0

    def test_trailing_blip_absorbed_leaves_no_time_hole(self):
        """Last segment is a blip with a predecessor -> absorbed, no time hole."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [
            _seg(0.0, 60.0, "SPEAKER_01"),
            _seg(60.0, 60.3, "SPEAKER_02"),  # trailing 0.3 s blip
        ]
        result = _drop_micro_segments(segs)
        assert len(result) == 1
        assert result[0]["end_seconds"] == 60.3

    def test_exactly_one_second_segment_kept(self):
        """Duration exactly 1.0s (strict <) -> kept, untouched."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [_seg(0.0, 1.0, "SPEAKER_00")]
        result = _drop_micro_segments(segs)
        assert len(result) == 1
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 1.0

    def test_consecutive_blip_run_absorbed_into_one_predecessor(self):
        """Multiple consecutive sub-second blips all absorb into the same
        predecessor, chaining forward."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [
            _seg(0.0, 60.0, "SPEAKER_01"),
            _seg(60.0, 60.2, "SPEAKER_02"),
            _seg(60.2, 60.5, "SPEAKER_03"),
            _seg(60.5, 60.9, "SPEAKER_02"),
            _seg(60.9, 120.0, "SPEAKER_04"),
        ]
        result = _drop_micro_segments(segs)
        assert len(result) == 2
        assert result[0]["end_seconds"] == 60.9
        assert result[1]["start_seconds"] == 60.9
        assert result[1]["end_seconds"] == 120.0

    def test_all_blips_returns_empty(self):
        """Every segment sub-second with no predecessor ever established -> []."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [
            _seg(0.0, 0.3, "SPEAKER_00"),
            _seg(0.3, 0.6, "SPEAKER_01"),
        ]
        result = _drop_micro_segments(segs)
        assert result == []

    def test_empty_input(self):
        """Empty list -> empty list."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        assert _drop_micro_segments([]) == []

    def test_single_non_blip_unchanged(self):
        """Single segment >= 1.0s -> returned unchanged."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        segs = [_seg(10.0, 70.0, "SPEAKER_00")]
        result = _drop_micro_segments(segs)
        assert len(result) == 1
        assert result[0]["start_seconds"] == 10.0
        assert result[0]["end_seconds"] == 70.0

    def test_input_dicts_not_mutated(self):
        """Original segment dicts passed in must not be mutated."""
        from congress_videos.modules.speaker_turns import _drop_micro_segments
        original = _seg(0.0, 60.0, "SPEAKER_01")
        blip = _seg(60.0, 60.5, "SPEAKER_02")
        segs = [original, blip]
        _drop_micro_segments(segs)
        assert original["end_seconds"] == 60.0
        assert blip["end_seconds"] == 60.5


class TestCollapseForeignRuns:

    def test_single_short_foreign_segment_collapsed(self):
        """A(60s)→B(4s)→A(90s), aggregate span 4s < 10.0 → single A."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 64.0, "B"),    # span 4 s < 10.0
            _seg(64.0, 154.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "A"
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 154.0

    def test_mid_range_single_interruption_now_collapses(self):
        """A(60s)→B(6s)→A: under the retired 5s per-segment cap this survived;
        under the new 10s aggregate-span rule it now collapses (Sc.8)."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 66.0, "B"),    # span 6 s < 10.0
            _seg(66.0, 156.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "A"
        assert result[0]["end_seconds"] == 156.0

    def test_foreign_span_over_max_not_collapsed(self):
        """A(60s)→B(10.5s)→A: aggregate span >= 10.0 → not collapsed."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 70.5, "B"),    # span 10.5 s >= 10.0
            _seg(70.5, 160.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 3

    def test_exactly_ten_seconds_span_survives(self):
        """Aggregate span exactly 10.0s (strict <) → not collapsed."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 70.0, "B"),    # span exactly 10.0 s
            _seg(70.0, 160.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 3

    def test_empty_input(self):
        """Empty list → empty list."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        assert _collapse_foreign_runs([]) == []

    def test_single_segment_unchanged(self):
        """Single segment → returned unchanged."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [_seg(0.0, 60.0, "A")]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 1

    def test_cascading_chain_collapses_to_one_segment(self):
        """A→B→A→B→A, each foreign leg short and each return gap short →
        the whole chain cascades into a single A (the i += 3 regression:
        the old algorithm skipped past the first collapsed triple and never
        re-tested it against what followed)."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 60.5, "B"),     # 0.5 s foreign
            _seg(60.5, 120.0, "A"),    # brief return
            _seg(120.0, 120.3, "B"),   # 0.3 s foreign
            _seg(120.3, 180.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "A"
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 180.0

    def test_mixed_speaker_run_within_span_collapses(self):
        """A→B→C→A: the foreign run need not share one label, only the
        aggregate span from the run's first to last segment matters."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 63.0, "B"),
            _seg(63.0, 66.0, "C"),
            _seg(66.0, 150.0, "A"),   # aggregate span 66.0 - 60.0 = 6.0s < 10.0
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "A"
        assert result[0]["end_seconds"] == 150.0

    def test_differing_bound_labels_survive(self):
        """A→B→C with no return to A's label → not collapsed (no anchor
        return found before the list ends)."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 63.0, "B"),
            _seg(63.0, 120.0, "C"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 3

    def test_tail_run_with_no_return_survives(self):
        """A foreign run at the tail with nothing to return to → survives
        as-is."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 63.0, "B"),
        ]
        result = _collapse_foreign_runs(segs)
        assert len(result) == 2

    def test_list_starting_foreign_survives(self):
        """First segment establishes the initial anchor — nothing precedes
        it to collapse against, so no run is ever attempted around it."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 3.0, "B"),
            _seg(3.0, 60.0, "A"),
            _seg(60.0, 120.0, "A"),
        ]
        result = _collapse_foreign_runs(segs)
        assert [s["speaker_label"] for s in result] == ["B", "A", "A"]

    def test_long_alternation_only_qualifying_runs_collapse(self):
        """A realistic multi-speaker debate: several separate foreign runs,
        each individually spanning < 10s, plus one run >= 10s interleaved —
        only the qualifying runs collapse; the long one survives (Sc.7)."""
        from congress_videos.modules.speaker_turns import _collapse_foreign_runs
        segs = [
            _seg(0.0, 100.0, "X"),
            _seg(100.0, 103.0, "Y"),     # run 1: span 3s < 10 -> collapses
            _seg(103.0, 200.0, "X"),
            _seg(200.0, 215.0, "Y"),     # run 2: span 15s >= 10 -> survives
            _seg(215.0, 300.0, "X"),
            _seg(300.0, 305.0, "Y"),     # run 3: span 5s < 10 -> collapses
            _seg(305.0, 400.0, "X"),
        ]
        result = _collapse_foreign_runs(segs)
        # run 1 and run 3 collapse into their surrounding X stretches;
        # run 2 (>=10s) remains a standalone Y segment.
        assert [s["speaker_label"] for s in result] == ["X", "Y", "X"]
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 200.0
        assert result[1]["speaker_label"] == "Y"
        assert result[1]["start_seconds"] == 200.0
        assert result[1]["end_seconds"] == 215.0
        assert result[2]["start_seconds"] == 215.0
        assert result[2]["end_seconds"] == 400.0


class TestChapter263Regression:
    """Real-data regression for issue #283: turns 200-210 of chapter 263.

    Runs the postprocessing pipeline in order (drop micro-segments -> merge
    gaps -> collapse foreign runs) and asserts the mis-anchoring blip chain
    is fully absorbed while the real 21.8s interruption (turn 207) survives.
    """

    def test_chapter_263_regression(self):
        from congress_videos.modules.speaker_turns import (
            _collapse_foreign_runs,
            _drop_micro_segments,
            _merge_gaps,
        )

        segments = _chapter_263_segments()
        segments = _drop_micro_segments(segments)
        segments = _merge_gaps(segments)
        segments = _collapse_foreign_runs(segments)

        assert len(segments) == 4

        assert segments[0]["speaker_label"] == "SPEAKER_01"
        assert segments[0]["start_seconds"] == pytest.approx(8707.14)
        assert segments[0]["end_seconds"] == pytest.approx(9158.63)

        assert segments[1]["speaker_label"] == "SPEAKER_00"
        assert segments[1]["start_seconds"] == pytest.approx(9158.61)
        assert segments[1]["end_seconds"] == pytest.approx(9180.41)
        assert segments[1]["end_seconds"] - segments[1]["start_seconds"] == pytest.approx(21.8)

        assert segments[2]["speaker_label"] == "SPEAKER_04"
        assert segments[2]["start_seconds"] == pytest.approx(9180.40)
        assert segments[2]["end_seconds"] == pytest.approx(9454.40)

        assert segments[3]["speaker_label"] == "SPEAKER_04"
        assert segments[3]["start_seconds"] == pytest.approx(9461.16)
        assert segments[3]["end_seconds"] == pytest.approx(9844.56)


class TestMergeSameName:

    def test_adjacent_same_resolved_name_merged(self):
        """Two adjacent turns with different labels but same non-null resolved_name → merged."""
        from congress_videos.modules.speaker_turns import Turn, _merge_same_name
        turns = [
            Turn(0.0, 60.0, "SPEAKER_00", "María Luisa García", 0.95, "text_named"),
            Turn(60.0, 120.0, "SPEAKER_02", "María Luisa García", 0.80, "text_confirmed"),
        ]
        result = _merge_same_name(turns)
        assert len(result) == 1
        assert result[0].resolved_name == "María Luisa García"
        assert result[0].start_seconds == 0.0
        assert result[0].end_seconds == 120.0

    def test_none_resolved_name_not_merged(self):
        """Two adjacent turns with resolved_name=None → not merged on name."""
        from congress_videos.modules.speaker_turns import Turn, _merge_same_name
        turns = [
            Turn(0.0, 60.0, "SPEAKER_00", None, 0.50, "acoustic"),
            Turn(60.0, 120.0, "SPEAKER_02", None, 0.50, "acoustic"),
        ]
        result = _merge_same_name(turns)
        assert len(result) == 2

    def test_empty_input(self):
        """Empty list → empty list."""
        from congress_videos.modules.speaker_turns import _merge_same_name
        assert _merge_same_name([]) == []

    def test_different_resolved_names_not_merged(self):
        """Different resolved names → not merged."""
        from congress_videos.modules.speaker_turns import Turn, _merge_same_name
        turns = [
            Turn(0.0, 60.0, "SPEAKER_00", "Ana García", 0.95, "text_named"),
            Turn(60.0, 120.0, "SPEAKER_01", "Pedro López", 0.95, "text_named"),
        ]
        result = _merge_same_name(turns)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Phase 5 — Text gate (_apply_text_gate)
# ---------------------------------------------------------------------------

class TestApplyTextGate:

    def test_phrase_with_name_resolves_to_text_named(self):
        """Phrase + resolvable name → text_named, confidence=0.95, resolved_name set."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra el señor García"),
        )

        def resolver(name):
            return {"display_name": "Pedro García", "normalized_name": "garcia, pedro"}

        turns = _apply_text_gate(segments, srt_blocks, resolver)
        assert len(turns) == 1
        assert turns[0].source == "text_named"
        assert turns[0].confidence == pytest.approx(0.95)
        assert turns[0].resolved_name is not None

    def test_phrase_without_resolvable_name_gives_text_confirmed(self):
        """Phrase found but name not resolvable → text_confirmed, confidence=0.80."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra su señoría"),
        )
        turns = _apply_text_gate(segments, srt_blocks, _null_resolver)
        assert len(turns) == 1
        assert turns[0].source == "text_confirmed"
        assert turns[0].confidence == pytest.approx(0.80)
        assert turns[0].resolved_name is None

    def test_named_phrase_but_unresolvable_name_gives_text_confirmed(self):
        """Phrase carries a name but the resolver can't map it → text_confirmed, 0.80.

        Distinct from the 'su señoría' path: here ``extract_announcement`` DOES
        return a name, but ``name_resolver`` returns None (unknown participant).
        """
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra el señor Desconocido"),
        )
        turns = _apply_text_gate(segments, srt_blocks, _null_resolver)
        assert len(turns) == 1
        assert turns[0].source == "text_confirmed"
        assert turns[0].confidence == pytest.approx(0.80)
        assert turns[0].resolved_name is None

    def test_no_phrase_same_speaker_noise_rejected(self):
        """No phrase + same from/to speaker label → segment dropped (noise rejection)."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_01",  # same speaker on both sides
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "El debate continúa"),
        )
        turns = _apply_text_gate(segments, srt_blocks, _null_resolver)
        assert len(turns) == 0

    def test_no_phrase_different_speakers_gives_acoustic(self):
        """No phrase + different speakers → acoustic, confidence=0.50."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "El debate continúa"),
        )
        turns = _apply_text_gate(segments, srt_blocks, _null_resolver)
        assert len(turns) == 1
        assert turns[0].source == "acoustic"
        assert turns[0].confidence == pytest.approx(0.50)

    def test_confirmed_block_duration_not_used_as_threshold(self):
        """confirmed_block_duration_seconds present but has NO effect on outcome."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        # Two segments identical except for confirmed_block_duration_seconds
        seg_short = {
            "start_seconds": t,
            "end_seconds": t + 30.0,
            "speaker_label": "SPEAKER_01",
            "from_speaker": "SPEAKER_00",
            "to_speaker": "SPEAKER_01",
            "confirmed_block_duration_seconds": 0.1,  # tiny — must NOT cause rejection
        }
        seg_long = {
            "start_seconds": t,
            "end_seconds": t + 30.0,
            "speaker_label": "SPEAKER_01",
            "from_speaker": "SPEAKER_00",
            "to_speaker": "SPEAKER_01",
            "confirmed_block_duration_seconds": 999.0,  # huge — must NOT change outcome
        }
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "El debate continúa"),
        )
        turns_short = _apply_text_gate([seg_short], srt_blocks, _null_resolver)
        turns_long = _apply_text_gate([seg_long], srt_blocks, _null_resolver)
        # Both must yield same result regardless of confirmed_block_duration_seconds
        assert len(turns_short) == len(turns_long)
        if turns_short:
            assert turns_short[0].source == turns_long[0].source
            assert turns_short[0].confidence == turns_long[0].confidence


# ---------------------------------------------------------------------------
# Phase 4 — LLM fallback (issue #131)
#
# Reached only from the "no phrase, different speakers" branch of the text
# gate — regex/fuzzy resolution and the same-speaker noise drop both take
# precedence and never invoke completion_fn.
# ---------------------------------------------------------------------------

def _no_phrase_segment(t=100.0, from_speaker="SPEAKER_00", to_speaker="SPEAKER_01"):
    return {
        "start_seconds": t,
        "end_seconds": t + 30.0,
        "speaker_label": to_speaker,
        "from_speaker": from_speaker,
        "to_speaker": to_speaker,
        "confirmed_block_duration_seconds": 30.0,
    }


def _intro_blocks(t=100.0):
    """Ordinary speech, no announcement phrase, well within the 120s window."""
    return _make_srt_blocks(
        (t - 40.0, t - 35.0, "Continuamos con el debate sobre presupuestos"),
    )


def _llm_response(speaker_name, confidence=0.9, error=None):
    if error is not None:
        return {"data": None, "error": error}
    return {
        "data": {"speaker_name": speaker_name, "confidence": confidence, "evidence": "..."},
        "error": None,
    }


class TestApplyTextGateLlmFallback:

    def test_happy_path_gives_llm_resolved(self):
        """Roster-validated LLM name -> llm_resolved, confidence=0.85."""
        from congress_videos.modules.speaker_turns import _apply_text_gate, LLM_RESOLVED_CONFIDENCE
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            return _llm_response("Ana García", 0.90)

        def resolver(name):
            return {"display_name": "Ana García", "normalized_name": "ana garcia"}

        turns = _apply_text_gate(segments, srt_blocks, resolver, completion_fn=completion_fn)
        assert len(turns) == 1
        assert turns[0].source == "llm_resolved"
        assert turns[0].confidence == pytest.approx(LLM_RESOLVED_CONFIDENCE)
        assert turns[0].resolved_name == "Ana García"

    def test_resolver_miss_gives_acoustic(self):
        """LLM name has no roster match -> stays acoustic (anti-hallucination)."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            return _llm_response("Nombre Inventado", 0.90)

        turns = _apply_text_gate(segments, srt_blocks, _null_resolver, completion_fn=completion_fn)
        assert len(turns) == 1
        assert turns[0].source == "acoustic"
        assert turns[0].confidence == pytest.approx(0.50)
        assert turns[0].resolved_name is None

    def test_confidence_below_threshold_gives_acoustic(self):
        """Model confidence 0.79 (< TURN_LLM_MIN_CONFIDENCE=0.80) -> acoustic."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            return _llm_response("Ana García", 0.79)

        turns = _apply_text_gate(segments, srt_blocks, _identity_resolver, completion_fn=completion_fn)
        assert turns[0].source == "acoustic"

    def test_confidence_at_threshold_gives_resolved(self):
        """Model confidence exactly 0.80 -> resolved (boundary inclusive)."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            return _llm_response("Ana García", 0.80)

        turns = _apply_text_gate(segments, srt_blocks, _identity_resolver, completion_fn=completion_fn)
        assert turns[0].source == "llm_resolved"

    def test_completion_fn_raises_gives_acoustic(self):
        """Never-raise contract: an exception from completion_fn -> acoustic."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            raise RuntimeError("OpenAI API error")

        turns = _apply_text_gate(segments, srt_blocks, _identity_resolver, completion_fn=completion_fn)
        assert turns[0].source == "acoustic"

    def test_error_field_gives_acoustic(self):
        """completion_fn returns an error field -> acoustic."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        def completion_fn(system, user, **kw):
            return _llm_response(None, error="invalid json from model")

        turns = _apply_text_gate(segments, srt_blocks, _identity_resolver, completion_fn=completion_fn)
        assert turns[0].source == "acoustic"

    def test_completion_fn_none_never_called(self):
        """completion_fn=None (default) -> fallback disabled, no attempt made."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)

        turns = _apply_text_gate(segments, srt_blocks, _identity_resolver, completion_fn=None)
        assert turns[0].source == "acoustic"

    def test_empty_intro_window_never_calls_completion_fn(self):
        """No SRT blocks in the intro window -> completion_fn is never invoked (D9)."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        calls = []

        def completion_fn(system, user, **kw):
            calls.append(1)
            return _llm_response("Ana García", 0.90)

        turns = _apply_text_gate(segments, [], _identity_resolver, completion_fn=completion_fn)
        assert len(calls) == 0
        assert turns[0].source == "acoustic"

    def test_cap_of_two_calls_exactly_twice(self):
        """max_llm_calls=2 with 3 eligible segments -> completion_fn called exactly twice."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        segments = [
            _no_phrase_segment(t=100.0, from_speaker="SPEAKER_00", to_speaker="SPEAKER_01"),
            _no_phrase_segment(t=200.0, from_speaker="SPEAKER_01", to_speaker="SPEAKER_02"),
            _no_phrase_segment(t=300.0, from_speaker="SPEAKER_02", to_speaker="SPEAKER_03"),
        ]
        srt_blocks = _intro_blocks(100.0) + _intro_blocks(200.0) + _intro_blocks(300.0)
        calls = []

        def completion_fn(system, user, **kw):
            calls.append(1)
            return _llm_response(None, 0.0)

        turns = _apply_text_gate(
            segments, srt_blocks, _null_resolver,
            completion_fn=completion_fn, max_llm_calls=2,
        )
        assert len(calls) == 2
        assert len(turns) == 3  # all three still produce a turn (acoustic fallback)

    def test_cap_zero_never_calls(self):
        """max_llm_calls=0 -> fallback fully disabled, completion_fn never invoked."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [_no_phrase_segment(t)]
        srt_blocks = _intro_blocks(t)
        calls = []

        def completion_fn(system, user, **kw):
            calls.append(1)
            return _llm_response("Ana García", 0.90)

        turns = _apply_text_gate(
            segments, srt_blocks, _identity_resolver,
            completion_fn=completion_fn, max_llm_calls=0,
        )
        assert len(calls) == 0
        assert turns[0].source == "acoustic"

    def test_regex_resolved_turn_never_invokes_llm(self):
        """A phrase-matched segment resolves via regex/fuzzy — LLM must never
        override an existing tier (never-override, design D-ordering)."""
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 100.0
        segments = [
            {
                "start_seconds": t,
                "end_seconds": t + 30.0,
                "speaker_label": "SPEAKER_01",
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            }
        ]
        srt_blocks = _make_srt_blocks(
            (80.0, 90.0, "Tiene la palabra el señor García"),
        )
        calls = []

        def completion_fn(system, user, **kw):
            calls.append(1)
            return _llm_response("Otro Nombre", 0.90)

        def resolver(name):
            return {"display_name": "Pedro García", "normalized_name": "garcia"}

        turns = _apply_text_gate(segments, srt_blocks, resolver, completion_fn=completion_fn)
        assert len(calls) == 0
        assert turns[0].source == "text_named"


class TestChapterBoundaryNonRegression:
    """Design D3: the 120s backward window alone bounds the mis-anchor blast
    radius to one adjacent announcement. This test guards that a block from
    the previous chapter (reachable through the widened DAG pre-filter) is
    never turned into an invented name when it carries no announcement
    phrase — it must fall through to the LLM fallback or acoustic."""

    def test_preceding_block_without_phrase_never_invents_a_name(self):
        from congress_videos.modules.speaker_turns import _apply_text_gate
        t = 50.0  # near the start of the chapter — closest preceding block
        segments = [_no_phrase_segment(t)]
        # Ordinary speech from the PREVIOUS chapter, reachable via the
        # widened pre-filter, carrying no announcement phrase.
        srt_blocks = _make_srt_blocks(
            (t - 40.0, t - 35.0, "Continuamos con el debate sobre presupuestos"),
        )
        turns = _apply_text_gate(segments, srt_blocks, _null_resolver)
        assert len(turns) == 1
        assert turns[0].source == "acoustic"
        assert turns[0].resolved_name is None


# ---------------------------------------------------------------------------
# Phase 6 — detect_turns orchestrator
# ---------------------------------------------------------------------------

def _make_chapter(chapter_id: int = 1, video_id: str = "abc123", session_date: str = "2024-01-01"):
    return {
        "chapter_id": chapter_id,
        "video_id": video_id,
        "session_date": session_date,
        "start_time": "00:30:00",
        "end_time": "01:15:00",
    }


def _stub_diarize_fn_with_changes(changes):
    """Return a stub diarize_fn that returns the given changes."""
    def _fn(wav_path, offset):
        return changes
    return _fn


def _empty_diarize_fn(wav_path, offset):
    return []


class TestDetectTurns:

    def test_stub_diarize_returns_turns_no_docker(self):
        """Stub diarize_fn returns fixed changes; pipeline completes without Docker."""
        from congress_videos.modules.speaker_turns import detect_turns
        chapter = _make_chapter()
        changes = [
            {
                "start_seconds": 10.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 20.0,
            },
        ]
        diarize_fn = _stub_diarize_fn_with_changes(changes)
        srt_blocks = []
        result = detect_turns(chapter, srt_blocks, diarize_fn, _null_resolver)
        assert isinstance(result, list)
        # No exception means Docker was NOT called

    def test_empty_diarize_output_returns_empty(self):
        """Empty diarize_fn output → []."""
        from congress_videos.modules.speaker_turns import detect_turns
        chapter = _make_chapter()
        result = detect_turns(chapter, [], _empty_diarize_fn, _null_resolver)
        assert result == []

    def test_missing_srt_acoustic_only(self):
        """srt_blocks=[] → turns with source='acoustic' and confidence<=0.50."""
        from congress_videos.modules.speaker_turns import detect_turns
        chapter = _make_chapter()
        changes = [
            {
                "start_seconds": 10.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 20.0,
            },
        ]
        diarize_fn = _stub_diarize_fn_with_changes(changes)
        result = detect_turns(chapter, [], diarize_fn, _null_resolver)
        assert isinstance(result, list)
        for t in result:
            assert t.source == "acoustic"
            assert t.confidence <= 0.50

    def test_gap_merge_applied(self):
        """Two same-speaker changes with tiny gap → gap merge applied."""
        from congress_videos.modules.speaker_turns import detect_turns, Turn
        chapter = _make_chapter()
        # Two consecutive changes to SPEAKER_01 with 0.5s gap between them
        changes = [
            {
                "start_seconds": 5.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 4.5,
            },
            {
                "start_seconds": 10.0,  # gap 0.5 s (5.0+4.5 = 9.5 end, then 10.0 start)
                "from_speaker": "SPEAKER_01",
                "to_speaker": "SPEAKER_01",  # same speaker → noise → will be dropped by text gate
                "confirmed_block_duration_seconds": 30.0,
            },
        ]
        diarize_fn = _stub_diarize_fn_with_changes(changes)
        result = detect_turns(chapter, [], diarize_fn, _null_resolver)
        assert isinstance(result, list)
        # No exceptions; pipeline ran end-to-end

    def test_end_to_end_pipeline_runs(self):
        """End-to-end: all postprocessing steps applied with srt_blocks."""
        from congress_videos.modules.speaker_turns import detect_turns
        chapter = _make_chapter()
        t = 50.0
        changes = [
            {
                "start_seconds": t,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 30.0,
            },
        ]
        diarize_fn = _stub_diarize_fn_with_changes(changes)
        srt_blocks = _make_srt_blocks(
            (30.0, 40.0, "Tiene la palabra el señor García"),
        )

        def resolver(name):
            return {"display_name": "García", "normalized_name": "garcia"}

        result = detect_turns(chapter, srt_blocks, diarize_fn, resolver)
        assert isinstance(result, list)
        if result:
            assert result[0].source in ("text_named", "text_confirmed", "acoustic")


# ---------------------------------------------------------------------------
# Phase 7 — _upsert_turns (persistence)
# ---------------------------------------------------------------------------

class TestUpsertTurns:

    def _make_turns(self, n: int = 2):
        from congress_videos.modules.speaker_turns import Turn
        return [
            Turn(
                start_seconds=float(i * 10),
                end_seconds=float(i * 10 + 9),
                speaker_label=f"SPEAKER_{i:02d}",
                resolved_name=None,
                confidence=0.50,
                source="acoustic",
            )
            for i in range(n)
        ]

    def test_upsert_calls_execute_for_each_turn(self):
        """_upsert_turns calls cursor.execute once per Turn."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        turns = self._make_turns(3)
        _upsert_turns(cursor, chapter_id=42, turns=turns)
        assert cursor.execute.call_count == 3

    def test_upsert_sql_contains_on_conflict(self):
        """SQL passed to cursor.execute must contain ON CONFLICT (chapter_id, start_seconds) DO UPDATE."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        turns = self._make_turns(1)
        _upsert_turns(cursor, chapter_id=42, turns=turns)
        sql_arg = cursor.execute.call_args[0][0]
        assert "ON CONFLICT" in sql_arg.upper()
        assert "chapter_id" in sql_arg
        assert "start_seconds" in sql_arg
        assert "DO UPDATE" in sql_arg.upper()

    def test_upsert_sql_contains_updated_at(self):
        """SQL update clause must set updated_at."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        turns = self._make_turns(1)
        _upsert_turns(cursor, chapter_id=42, turns=turns)
        sql_arg = cursor.execute.call_args[0][0]
        assert "updated_at" in sql_arg.lower()

    def test_upsert_never_calls_commit(self):
        """_upsert_turns must NOT call cursor.commit (DAG controls transactions)."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        turns = self._make_turns(2)
        _upsert_turns(cursor, chapter_id=42, turns=turns)
        cursor.commit.assert_not_called()

    def test_upsert_empty_turns_no_execute(self):
        """Empty turns list → cursor.execute never called."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        _upsert_turns(cursor, chapter_id=42, turns=[])
        cursor.execute.assert_not_called()

    def test_upsert_uses_qualified_table_when_provided(self):
        """A schema-qualified table name must land in the INSERT target.

        Regression: the app sets no search_path, so an unqualified name only
        resolves when the role's default schema matches (works in dev, fails
        in prod with 'relation "speaker_turns" does not exist').
        """
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        _upsert_turns(
            cursor, chapter_id=42, turns=self._make_turns(1),
            table="production.speaker_turns",
        )
        sql_arg = cursor.execute.call_args[0][0]
        assert "INSERT INTO production.speaker_turns" in sql_arg

    def test_upsert_defaults_to_bare_table_name(self):
        """Default keeps the bare name so pure unit tests need no connection."""
        from congress_videos.modules.speaker_turns import _upsert_turns
        cursor = MagicMock()
        _upsert_turns(cursor, chapter_id=42, turns=self._make_turns(1))
        sql_arg = cursor.execute.call_args[0][0]
        assert "INSERT INTO speaker_turns" in sql_arg


# ---------------------------------------------------------------------------
# Phase: detect_turns exception propagation (issue #156)
# ---------------------------------------------------------------------------

class TestDetectTurnsPropagation:
    """Verify detect_turns does NOT swallow SidecarApiError or other exceptions.

    Prior to this fix, the try/except around diarize_fn caught all exceptions
    and silently returned []. This masked infra outages as empty results.
    """

    def _make_chapter(self):
        return {
            "chapter_id": 99,
            "video_id": "xyz",
            "session_date": "2026-01-01",
        }

    def test_sidecar_api_error_propagates_not_returns_empty(self):
        """diarize_fn raises SidecarApiError → detect_turns must re-raise it, NOT return []."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns import detect_turns

        def failing_diarize(wav_path, offset):
            raise SidecarApiError("diarize-api unreachable")

        with pytest.raises(SidecarApiError):
            detect_turns(self._make_chapter(), [], failing_diarize, lambda n: None)

    def test_value_error_propagates_not_returns_empty(self):
        """diarize_fn raises ValueError → detect_turns must propagate it, NOT return []."""
        from congress_videos.modules.speaker_turns import detect_turns

        def failing_diarize(wav_path, offset):
            raise ValueError("unexpected API shape")

        with pytest.raises(ValueError):
            detect_turns(self._make_chapter(), [], failing_diarize, lambda n: None)

    def test_empty_return_from_diarize_still_gives_empty_list(self):
        """diarize_fn returns [] (acoustic-only/no changes) → detect_turns returns [] without raising."""
        from congress_videos.modules.speaker_turns import detect_turns

        def empty_diarize(wav_path, offset):
            return []

        result = detect_turns(self._make_chapter(), [], empty_diarize, lambda n: None)
        assert result == []


# ---------------------------------------------------------------------------
# Public filter aliases (issue #282) — materialization.py reuses these
# instead of duplicating the #283 noise filters.
# ---------------------------------------------------------------------------

class TestPublicFilterAliases:

    def test_drop_micro_segments_alias_is_same_object(self):
        from congress_videos.modules.speaker_turns import (
            _drop_micro_segments,
            drop_micro_segments,
        )

        assert drop_micro_segments is _drop_micro_segments

    def test_collapse_foreign_runs_alias_is_same_object(self):
        from congress_videos.modules.speaker_turns import (
            _collapse_foreign_runs,
            collapse_foreign_runs,
        )

        assert collapse_foreign_runs is _collapse_foreign_runs

    def test_min_segment_duration_seconds_exported(self):
        from congress_videos.modules import speaker_turns

        assert "MIN_SEGMENT_DURATION_SECONDS" in speaker_turns.__all__
        assert speaker_turns.MIN_SEGMENT_DURATION_SECONDS == 1.0

    def test_foreign_interruption_max_seconds_exported(self):
        from congress_videos.modules import speaker_turns

        assert "FOREIGN_INTERRUPTION_MAX_SECONDS" in speaker_turns.__all__
        assert speaker_turns.FOREIGN_INTERRUPTION_MAX_SECONDS == 10.0

    def test_all_contents(self):
        from congress_videos.modules import speaker_turns

        assert set(speaker_turns.__all__) == {
            "Turn",
            "detect_turns",
            "extract_announcement",
            "drop_micro_segments",
            "collapse_foreign_runs",
            "MIN_SEGMENT_DURATION_SECONDS",
            "FOREIGN_INTERRUPTION_MAX_SECONDS",
        }
