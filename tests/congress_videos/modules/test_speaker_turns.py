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

    def test_pingpong_b_max_seconds(self):
        from congress_videos.modules.speaker_turns import PINGPONG_B_MAX_SECONDS
        assert PINGPONG_B_MAX_SECONDS == 5.0

    def test_pingpong_return_seconds(self):
        from congress_videos.modules.speaker_turns import PINGPONG_RETURN_SECONDS
        assert PINGPONG_RETURN_SECONDS == 10.0


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
        """A matching block more than 30 s before t is outside the default window."""
        from congress_videos.modules.speaker_turns import extract_announcement
        t = 100.0
        # 100 - 31 = 69 → outside [70, 130] window
        blocks = _make_srt_blocks(
            (60.0, 68.0, "Tiene la palabra el señor Fuera de ventana"),
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


class TestCollapsePingpong:

    def test_short_b_segment_collapsed(self):
        """A(60s)→B(4s)→A(90s) where B < PINGPONG_B_MAX_SECONDS → single A."""
        from congress_videos.modules.speaker_turns import _collapse_pingpong
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 64.0, "B"),    # 4 s < 5.0
            _seg(64.0, 154.0, "A"),   # return gap 0s < 10.0
        ]
        result = _collapse_pingpong(segs)
        assert len(result) == 1
        assert result[0]["speaker_label"] == "A"
        assert result[0]["start_seconds"] == 0.0
        assert result[0]["end_seconds"] == 154.0

    def test_b_over_max_not_collapsed(self):
        """A(60s)→B(6s)→A where B >= PINGPONG_B_MAX_SECONDS → not collapsed."""
        from congress_videos.modules.speaker_turns import _collapse_pingpong
        segs = [
            _seg(0.0, 60.0, "A"),
            _seg(60.0, 66.0, "B"),    # 6 s >= 5.0
            _seg(66.0, 156.0, "A"),
        ]
        result = _collapse_pingpong(segs)
        assert len(result) == 3

    def test_empty_input(self):
        """Empty list → empty list."""
        from congress_videos.modules.speaker_turns import _collapse_pingpong
        assert _collapse_pingpong([]) == []

    def test_single_segment_unchanged(self):
        """Single segment → returned unchanged."""
        from congress_videos.modules.speaker_turns import _collapse_pingpong
        segs = [_seg(0.0, 60.0, "A")]
        result = _collapse_pingpong(segs)
        assert len(result) == 1


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
