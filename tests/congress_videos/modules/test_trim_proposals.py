"""Tests for congress_videos.modules.trim_proposals — pure module.

Covers:
- TrimProposal dataclass: frozen, required fields, FrozenInstanceError on mutation.
- is_voice_free gate: disjoint, fully overlapping, partially overlapping, boundary touch,
  applause inside voice.
- _silence_gaps: full coverage, single gap, multiple gaps, below min_duration, boundary clipping.
- generate_trim_proposals: silence path (full coverage → zero, gaps → proposals), applause path
  (empty → zero, detected → proposal, overlapping voice → dropped, below threshold, below min_duration).
- _upsert_proposals: first run inserts N rows, re-run (ON CONFLICT) stays N, empty list → 0.

All tests use stub VadFn / YamnetFn; no real model, no Docker, no audio files.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, call

import pytest

from congress_videos.modules.trim_proposals import (
    TrimProposal,
    _silence_gaps,
    _upsert_proposals,
    generate_trim_proposals,
    is_voice_free,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _turn(turn_id: int = 1, start: float = 0.0, end: float = 60.0) -> dict:
    """Minimal speaker-turn dict for generate_trim_proposals."""
    return {
        "turn_id": turn_id,
        "start_seconds": start,
        "end_seconds": end,
    }


def _stub_vad(segments: list[tuple[float, float]]):
    """Return a VadFn that always returns the given voiced segments."""
    def vad_fn(wav_path: str, offset: float) -> list[tuple[float, float]]:
        return segments
    return vad_fn


def _stub_yamnet(rounds: list[dict]):
    """Return a YamnetFn that always returns the given applause rounds."""
    def yamnet_fn(wav_path: str, offset: float) -> list[dict]:
        return rounds
    return yamnet_fn


# ---------------------------------------------------------------------------
# TrimProposal dataclass
# ---------------------------------------------------------------------------


class TestTrimProposalDataclass:

    def test_fields_required(self):
        """TrimProposal must carry all seven required fields."""
        proposal = TrimProposal(
            turn_id=1,
            start_seconds=10.0,
            end_seconds=20.0,
            kind="silence",
            score=None,
            source="vad_webrtc",
            is_voice_free=True,
        )
        assert proposal.turn_id == 1
        assert proposal.start_seconds == 10.0
        assert proposal.end_seconds == 20.0
        assert proposal.kind == "silence"
        assert proposal.score is None
        assert proposal.source == "vad_webrtc"
        assert proposal.is_voice_free is True

    def test_frozen_raises_on_mutation(self):
        """TrimProposal must be frozen (FrozenInstanceError on attribute assignment)."""
        proposal = TrimProposal(
            turn_id=1,
            start_seconds=5.0,
            end_seconds=15.0,
            kind="applause",
            score=0.88,
            source="yamnet_tflite",
            is_voice_free=True,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            proposal.score = 0.99  # type: ignore[misc]

    def test_applause_proposal_with_score(self):
        """Applause proposals carry a float score in [0, 1]."""
        proposal = TrimProposal(
            turn_id=2,
            start_seconds=40.0,
            end_seconds=55.0,
            kind="applause",
            score=0.85,
            source="yamnet_tflite",
            is_voice_free=True,
        )
        assert proposal.score == 0.85
        assert proposal.kind == "applause"

    def test_silence_proposal_score_none(self):
        """Silence proposals may have score=None."""
        proposal = TrimProposal(
            turn_id=3,
            start_seconds=20.0,
            end_seconds=30.0,
            kind="silence",
            score=None,
            source="vad_webrtc",
            is_voice_free=True,
        )
        assert proposal.score is None
        assert proposal.kind == "silence"


# ---------------------------------------------------------------------------
# is_voice_free gate
# ---------------------------------------------------------------------------


class TestIsVoiceFree:

    def test_disjoint_interval_returns_true(self):
        """Interval [5, 10] vs voiced [20, 30] — disjoint, gate returns True."""
        assert is_voice_free((5.0, 10.0), [(20.0, 30.0)]) is True

    def test_fully_overlapping_returns_false(self):
        """Interval [15, 25] vs voiced [20, 30] — partial overlap, gate returns False."""
        assert is_voice_free((15.0, 25.0), [(20.0, 30.0)]) is False

    def test_interval_inside_voice_returns_false(self):
        """Interval [45, 55] fully inside voiced [40, 60] — gate returns False."""
        assert is_voice_free((45.0, 55.0), [(40.0, 60.0)]) is False

    def test_boundary_touch_end_equals_voice_start_returns_true(self):
        """Candidate end == voice start is NOT overlap; gate returns True."""
        # voiced [20, 30], candidate [10, 20] — end == voice_start → not overlap
        assert is_voice_free((10.0, 20.0), [(20.0, 30.0)]) is True

    def test_boundary_touch_voice_end_equals_start_returns_true(self):
        """Candidate start == voice end is NOT overlap; gate returns True."""
        # voiced [20, 30], candidate [30, 40] — start == voice_end → not overlap
        assert is_voice_free((30.0, 40.0), [(20.0, 30.0)]) is True

    def test_applause_inside_voice_returns_false(self):
        """Applause interval [45, 55] inside voiced [40, 60] — gate returns False."""
        assert is_voice_free((45.0, 55.0), [(40.0, 60.0)]) is False

    def test_multiple_voiced_segments_one_overlap_returns_false(self):
        """One of several voiced segments overlaps → gate returns False."""
        voiced = [(0.0, 10.0), (50.0, 70.0), (90.0, 120.0)]
        assert is_voice_free((55.0, 65.0), voiced) is False

    def test_multiple_voiced_segments_no_overlap_returns_true(self):
        """Candidate falls between voiced segments with no overlap."""
        voiced = [(0.0, 10.0), (30.0, 40.0), (60.0, 80.0)]
        assert is_voice_free((12.0, 28.0), voiced) is True

    def test_empty_voiced_segments_returns_true(self):
        """No voiced segments — everything is voice-free."""
        assert is_voice_free((0.0, 30.0), []) is True


# ---------------------------------------------------------------------------
# _silence_gaps
# ---------------------------------------------------------------------------


class TestSilenceGaps:

    def test_full_voiced_coverage_no_gaps(self):
        """Turn [0,60] fully voiced → zero silence gaps."""
        gaps = _silence_gaps([(0.0, 60.0)], turn_start=0.0, turn_end=60.0, min_duration=1.0)
        assert gaps == [], f"Expected no gaps but got {gaps}"

    def test_single_gap_in_middle(self):
        """Turn [0,60] voiced [0,20] + [40,60] → gap (20,40) = 20s."""
        gaps = _silence_gaps([(0.0, 20.0), (40.0, 60.0)], turn_start=0.0, turn_end=60.0, min_duration=1.0)
        assert len(gaps) == 1
        assert gaps[0] == (20.0, 40.0)

    def test_multiple_gaps(self):
        """Turn [0,90] voiced [0,10]+[30,50]+[70,90] → gaps (10,30) and (50,70)."""
        voiced = [(0.0, 10.0), (30.0, 50.0), (70.0, 90.0)]
        gaps = _silence_gaps(voiced, turn_start=0.0, turn_end=90.0, min_duration=1.0)
        assert len(gaps) == 2
        assert gaps[0] == (10.0, 30.0)
        assert gaps[1] == (50.0, 70.0)

    def test_gap_below_min_duration_filtered(self):
        """Gap of 2s is below min_duration=3s → not returned."""
        # voiced [0,20]+[22,60]: gap 20-22 = 2s < 3s
        gaps = _silence_gaps([(0.0, 20.0), (22.0, 60.0)], turn_start=0.0, turn_end=60.0, min_duration=3.0)
        assert gaps == [], f"Expected gap filtered but got {gaps}"

    def test_gap_exactly_at_min_duration_included(self):
        """Gap of exactly min_duration=3s → included."""
        # voiced [0,20]+[23,60]: gap 20-23 = 3s == min_duration
        gaps = _silence_gaps([(0.0, 20.0), (23.0, 60.0)], turn_start=0.0, turn_end=60.0, min_duration=3.0)
        assert len(gaps) == 1
        assert gaps[0] == (20.0, 23.0)

    def test_gap_clipped_at_turn_boundaries(self):
        """Voiced segment starts before turn_start — gap is clipped to turn window."""
        # voiced extends before turn: [0,15], turn starts at 5 → gap [15, 30] from turn 5-30
        voiced = [(0.0, 15.0), (25.0, 40.0)]  # gap 15-25 within turn [5, 40]
        gaps = _silence_gaps(voiced, turn_start=5.0, turn_end=40.0, min_duration=1.0)
        # Gap from 15 to 25 (10s), starts after turn_start so it's valid
        assert len(gaps) == 1
        gap_start, gap_end = gaps[0]
        assert gap_start >= 5.0
        assert gap_end <= 40.0

    def test_no_voiced_at_all_full_turn_is_gap(self):
        """No voiced segments → entire turn window is one silence gap."""
        gaps = _silence_gaps([], turn_start=10.0, turn_end=50.0, min_duration=1.0)
        assert len(gaps) == 1
        assert gaps[0] == (10.0, 50.0)


# ---------------------------------------------------------------------------
# generate_trim_proposals — silence path
# ---------------------------------------------------------------------------


class TestGenerateTrimProposalsSidecarFailure:
    """yamnet_fn failures must be classified: infra loud, data silent."""

    def test_sidecar_api_error_propagates_not_swallowed(self):
        """yamnet_fn raises SidecarApiError -> generate_trim_proposals must
        re-raise it, NOT log-and-continue with an empty applause list.

        Without this, a yamnet-api outage is absorbed here and the caller
        persists silence-only proposals and reports success, so the DAG-level
        `except SidecarApiError: raise` never fires. Mirrors
        test_speaker_turns.py::test_sidecar_api_error_propagates_not_returns_empty.
        """
        from congress_videos.modules.sidecar_api_error import SidecarApiError

        turn = _turn(turn_id=1, start=0.0, end=60.0)
        vad_fn = _stub_vad([(0.0, 60.0)])

        def failing_yamnet(wav_path, offset):
            raise SidecarApiError("yamnet-api unreachable")

        with pytest.raises(SidecarApiError):
            generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, failing_yamnet)

    def test_non_sidecar_error_is_still_swallowed(self):
        """A non-infra yamnet_fn failure stays a graceful degradation: the
        applause path is skipped and silence proposals are still returned."""
        turn = _turn(turn_id=2, start=0.0, end=60.0)
        vad_fn = _stub_vad([(0.0, 20.0), (40.0, 60.0)])

        def failing_yamnet(wav_path, offset):
            raise ValueError("unexpected API shape")

        proposals = generate_trim_proposals(
            turn, "/fake/audio.wav", vad_fn, failing_yamnet, min_duration_secs=1.0
        )
        assert [p.kind for p in proposals] == ["silence"]


class TestGenerateTrimProposalsSilence:

    def test_full_voiced_no_silence_proposals(self):
        """VAD returns full turn coverage → zero silence proposals emitted."""
        turn = _turn(turn_id=1, start=0.0, end=60.0)
        vad_fn = _stub_vad([(0.0, 60.0)])
        yamnet_fn = _stub_yamnet([])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn)
        silence_props = [p for p in proposals if p.kind == "silence"]
        assert silence_props == [], f"Expected no silence proposals but got {silence_props}"

    def test_gap_produces_silence_proposal(self):
        """VAD gap [20,40] in turn [0,60] → one silence proposal."""
        turn = _turn(turn_id=7, start=0.0, end=60.0)
        vad_fn = _stub_vad([(0.0, 20.0), (40.0, 60.0)])
        yamnet_fn = _stub_yamnet([])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0)
        silence_props = [p for p in proposals if p.kind == "silence"]
        assert len(silence_props) == 1
        p = silence_props[0]
        assert p.turn_id == 7
        assert p.start_seconds == 20.0
        assert p.end_seconds == 40.0
        assert p.kind == "silence"
        assert p.is_voice_free is True  # gaps are voice-free by construction
        assert p.source  # non-empty backend identifier

    def test_silence_proposals_carry_turn_id(self):
        """Silence proposals must carry the correct turn_id."""
        turn = _turn(turn_id=42, start=0.0, end=30.0)
        vad_fn = _stub_vad([(0.0, 10.0), (20.0, 30.0)])
        yamnet_fn = _stub_yamnet([])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0)
        for p in proposals:
            assert p.turn_id == 42

    def test_silence_proposals_have_non_empty_source(self):
        """Silence proposals must carry a non-empty source string."""
        turn = _turn(start=0.0, end=30.0)
        vad_fn = _stub_vad([(0.0, 5.0), (15.0, 30.0)])
        yamnet_fn = _stub_yamnet([])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0)
        silence_props = [p for p in proposals if p.kind == "silence"]
        assert silence_props, "Expected at least one silence proposal"
        for p in silence_props:
            assert p.source, "source must be a non-empty string"


# ---------------------------------------------------------------------------
# generate_trim_proposals — applause path
# ---------------------------------------------------------------------------


class TestGenerateTrimProposalsApplause:

    def test_empty_yamnet_returns_no_applause_proposals(self):
        """YamnetFn returning [] → zero applause proposals."""
        turn = _turn(start=0.0, end=60.0)
        vad_fn = _stub_vad([])
        yamnet_fn = _stub_yamnet([])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert applause_props == [], f"Expected no applause but got {applause_props}"

    def test_applause_segment_gate_passes_produces_proposal(self):
        """YamnetFn returns one interval, gate passes → one applause proposal."""
        turn = _turn(turn_id=5, start=0.0, end=60.0)
        # No voice so gate passes
        vad_fn = _stub_vad([])
        yamnet_fn = _stub_yamnet([{"start": 5.0, "end": 15.0, "max_score": 0.92}])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0, applause_score_threshold=0.5)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert len(applause_props) == 1
        p = applause_props[0]
        assert p.turn_id == 5
        assert p.start_seconds == 5.0
        assert p.end_seconds == 15.0
        assert p.kind == "applause"
        assert p.score == 0.92
        assert p.source == "yamnet_tflite"

    def test_applause_overlapping_voice_dropped(self):
        """Applause interval overlaps a voiced segment → dropped by is_voice_free gate."""
        turn = _turn(start=0.0, end=60.0)
        # Voice covers [30, 50]; applause is at [35, 45] (inside voice)
        vad_fn = _stub_vad([(30.0, 50.0)])
        yamnet_fn = _stub_yamnet([{"start": 35.0, "end": 45.0, "max_score": 0.85}])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0, applause_score_threshold=0.5)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert applause_props == [], (
            "Applause proposal overlapping voice must be dropped by is_voice_free gate"
        )

    def test_applause_below_score_threshold_filtered(self):
        """Applause score below applause_score_threshold → not emitted."""
        turn = _turn(start=0.0, end=60.0)
        vad_fn = _stub_vad([])
        yamnet_fn = _stub_yamnet([{"start": 5.0, "end": 20.0, "max_score": 0.30}])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            applause_score_threshold=0.5)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert applause_props == [], "Applause below score threshold must be filtered"

    def test_applause_below_min_duration_filtered(self):
        """Applause interval shorter than min_duration_secs → not emitted."""
        turn = _turn(start=0.0, end=60.0)
        vad_fn = _stub_vad([])
        # Interval is 2s, min_duration is 3s
        yamnet_fn = _stub_yamnet([{"start": 5.0, "end": 7.0, "max_score": 0.90}])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=3.0, applause_score_threshold=0.5)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert applause_props == [], "Short applause below min_duration must be filtered"

    def test_applause_boundary_adjacent_to_voice_not_dropped(self):
        """Applause end == voice start is a boundary touch, NOT overlap → kept."""
        turn = _turn(start=0.0, end=60.0)
        # voiced [20, 40]; applause [5, 20] → end == voice_start → not overlap
        vad_fn = _stub_vad([(20.0, 40.0)])
        yamnet_fn = _stub_yamnet([{"start": 5.0, "end": 20.0, "max_score": 0.88}])

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, yamnet_fn,
                                            min_duration_secs=1.0, applause_score_threshold=0.5)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert len(applause_props) == 1, (
            "Applause adjacent to voice boundary (end==voice_start) must NOT be dropped"
        )


# ---------------------------------------------------------------------------
# _upsert_proposals
# ---------------------------------------------------------------------------


class TestUpsertProposals:

    def _make_proposal(self, turn_id: int = 1, start: float = 10.0) -> TrimProposal:
        return TrimProposal(
            turn_id=turn_id,
            start_seconds=start,
            end_seconds=start + 10.0,
            kind="silence",
            score=None,
            source="vad_webrtc",
            is_voice_free=True,
        )

    def test_first_run_inserts_n_rows(self):
        """First call with N proposals executes N INSERT statements."""
        proposals = [self._make_proposal(start=float(i * 20)) for i in range(3)]
        cursor = MagicMock()

        count = _upsert_proposals(cursor, proposals)

        assert count == 3
        assert cursor.execute.call_count == 3

    def test_re_run_same_proposals_executes_n_statements(self):
        """Second call (ON CONFLICT path) still executes N statements (upsert)."""
        proposals = [self._make_proposal(start=10.0)]
        cursor = MagicMock()

        # First run
        count1 = _upsert_proposals(cursor, proposals)
        # Second run (same proposals — ON CONFLICT DO UPDATE)
        count2 = _upsert_proposals(cursor, proposals)

        assert count1 == 1
        assert count2 == 1
        # Two runs → 2 execute calls total
        assert cursor.execute.call_count == 2

    def test_empty_list_returns_zero_no_db_call(self):
        """Empty proposals list returns 0 and makes no DB calls."""
        cursor = MagicMock()

        count = _upsert_proposals(cursor, [])

        assert count == 0
        cursor.execute.assert_not_called()

    def test_upsert_sql_uses_on_conflict(self):
        """SQL passed to cursor must contain ON CONFLICT clause."""
        proposals = [self._make_proposal()]
        cursor = MagicMock()

        _upsert_proposals(cursor, proposals)

        # Extract the SQL string from the first execute call
        call_args = cursor.execute.call_args
        sql_arg = call_args[0][0]
        assert "ON CONFLICT" in sql_arg.upper(), (
            "Upsert SQL must contain ON CONFLICT clause for idempotency"
        )

    def test_upsert_sql_updates_score_and_source(self):
        """ON CONFLICT branch must update score and source (and updated_at)."""
        proposals = [self._make_proposal()]
        cursor = MagicMock()

        _upsert_proposals(cursor, proposals)

        sql_arg = cursor.execute.call_args[0][0].upper()
        assert "DO UPDATE" in sql_arg
        assert "SCORE" in sql_arg
        assert "SOURCE" in sql_arg

    def test_upsert_uses_qualified_table_when_provided(self):
        """A schema-qualified table name must land in the INSERT target.

        Regression: the app sets no search_path, so an unqualified name only
        resolves when the role's default schema matches (works in dev, fails
        in prod with 'relation "speaker_turn_trim_proposals" does not exist').
        """
        cursor = MagicMock()

        _upsert_proposals(
            cursor, [self._make_proposal()],
            table="production.speaker_turn_trim_proposals",
        )

        sql_arg = cursor.execute.call_args[0][0]
        assert "INSERT INTO production.speaker_turn_trim_proposals" in sql_arg

    def test_upsert_defaults_to_bare_table_name(self):
        """Default keeps the bare name so pure unit tests need no connection."""
        cursor = MagicMock()

        _upsert_proposals(cursor, [self._make_proposal()])

        sql_arg = cursor.execute.call_args[0][0]
        assert "INSERT INTO speaker_turn_trim_proposals" in sql_arg


# ---------------------------------------------------------------------------
# generate_trim_proposals — exception handling (coverage for warning paths)
# ---------------------------------------------------------------------------


class TestGenerateTrimProposalsExceptionHandling:

    def test_vad_fn_exception_falls_back_to_full_turn_gap(self):
        """When vad_fn raises, voiced_segments defaults to [] so full turn becomes a silence gap."""
        turn = _turn(start=0.0, end=60.0)

        def failing_vad(wav_path, offset):
            raise RuntimeError("VAD backend unavailable")

        yamnet_fn = _stub_yamnet([])
        proposals = generate_trim_proposals(turn, "/fake/audio.wav", failing_vad, yamnet_fn,
                                            min_duration_secs=1.0)
        # When VAD fails, voiced_segments=[] → entire turn is one gap
        silence_props = [p for p in proposals if p.kind == "silence"]
        assert len(silence_props) == 1
        assert silence_props[0].start_seconds == 0.0
        assert silence_props[0].end_seconds == 60.0

    def test_yamnet_fn_exception_produces_no_applause_proposals(self):
        """When yamnet_fn raises, applause path yields no proposals (best-effort)."""
        turn = _turn(start=0.0, end=60.0)
        vad_fn = _stub_vad([])

        def failing_yamnet(wav_path, offset):
            raise RuntimeError("Docker unavailable")

        proposals = generate_trim_proposals(turn, "/fake/audio.wav", vad_fn, failing_yamnet)
        applause_props = [p for p in proposals if p.kind == "applause"]
        assert applause_props == []


# ---------------------------------------------------------------------------
# _upsert_proposals cursor contract (regression guard for DAG wiring)
# ---------------------------------------------------------------------------


class TestUpsertProposalsCursorContract:
    """Verify _upsert_proposals accepts a plain DB cursor, not a connection.

    The DAG (_process_task) opens a connection, then opens a cursor inside a
    with-block and passes THAT CURSOR to run_turn_proposals → _upsert_proposals.
    If _upsert_proposals called cursor.cursor() internally it would raise
    AttributeError at the first real production trigger.

    These tests use a CursorDouble that deliberately lacks a .cursor() method
    so any call to it immediately raises AttributeError — matching the real
    production failure mode.
    """

    class CursorDouble:
        """Minimal cursor double — has execute() but no cursor() method."""

        def __init__(self):
            self.executed: list[tuple] = []

        def execute(self, sql: str, params: tuple) -> None:
            self.executed.append((sql, params))

    def _make_proposal(self, turn_id: int = 1, start: float = 10.0) -> TrimProposal:
        return TrimProposal(
            turn_id=turn_id,
            start_seconds=start,
            end_seconds=start + 10.0,
            kind="silence",
            score=None,
            source="vad_webrtc",
            is_voice_free=True,
        )

    def test_upsert_accepts_cursor_not_conn(self):
        """_upsert_proposals must accept a plain cursor, not call cursor.cursor().

        If it calls cursor.cursor(), AttributeError is raised — the same crash
        that would occur in production when the DAG passes its open cursor.
        """
        cursor = self.CursorDouble()
        proposals = [self._make_proposal()]

        # Must not raise AttributeError from cursor.cursor()
        count = _upsert_proposals(cursor, proposals)

        assert count == 1
        assert len(cursor.executed) == 1

    def test_upsert_cursor_executes_n_statements(self):
        """Each proposal in the list maps to exactly one cursor.execute() call."""
        cursor = self.CursorDouble()
        proposals = [self._make_proposal(start=float(i * 20)) for i in range(3)]

        count = _upsert_proposals(cursor, proposals)

        assert count == 3
        assert len(cursor.executed) == 3

    def test_upsert_empty_list_no_execute_calls(self):
        """Empty proposals list: no execute() calls, returns 0."""
        cursor = self.CursorDouble()

        count = _upsert_proposals(cursor, [])

        assert count == 0
        assert cursor.executed == []

    def test_upsert_sql_contains_on_conflict(self):
        """SQL passed to cursor.execute() must contain ON CONFLICT clause."""
        cursor = self.CursorDouble()
        proposals = [self._make_proposal()]

        _upsert_proposals(cursor, proposals)

        sql, _ = cursor.executed[0]
        assert "ON CONFLICT" in sql.upper()

    def test_dag_wiring_run_turn_proposals_passes_cursor(self, monkeypatch):
        """run_turn_proposals in the DAG must pass its cursor arg to _upsert_proposals.

        This test exercises the REAL DAG wiring without monkeypatching _upsert_proposals.
        It verifies that when run_turn_proposals is called with a CursorDouble,
        the cursor reaches _upsert_proposals directly (no .cursor() indirection).
        """
        import importlib
        import sys

        module_name = "congress_videos.trim_proposals_dag"
        if module_name in sys.modules:
            del sys.modules[module_name]
        mod = importlib.import_module(module_name)

        cursor = self.CursorDouble()
        turn = {
            "turn_id": 99,
            "chapter_id": 1,
            "video_id": "vid99",
            "session_date": "2026-06-10",
            "start_seconds": 100.0,
            "end_seconds": 130.0,
        }

        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda *a, **k: "/v/src.mp4")
        monkeypatch.setattr(mod, "extract_audio_wav", lambda *a, **k: None)

        # Return one real proposal so _upsert_proposals is actually called
        from congress_videos.modules.trim_proposals import TrimProposal
        proposal = TrimProposal(
            turn_id=99, start_seconds=110.0, end_seconds=120.0,
            kind="silence", score=None, source="vad_webrtc", is_voice_free=True,
        )
        monkeypatch.setattr(mod, "generate_trim_proposals", lambda *a, **k: [proposal])
        # Do NOT monkeypatch _upsert_proposals — exercise real wiring

        # Must not raise AttributeError (cursor has no .cursor() method)
        result = mod.run_turn_proposals(turn, cursor)

        assert result["status"] == "ok"
        assert result["proposals"] == 1
        # The cursor was used directly — exactly one execute call
        assert len(cursor.executed) == 1
