"""[RED] Tests for is_procedural_turn — the procedural-turn detection gate (issue #143).

Pure AND-gate: duration <= PROCEDURAL_MAX_DURATION_SECS AND phrase coverage
>= PROCEDURAL_MIN_COVERAGE over the turn's OWN accent-stripped, lowercased,
whitespace-collapsed text. Precision-first: ambiguous cases must NOT be flagged.

No I/O, no DB, plain strings only.
"""
from __future__ import annotations

import pytest

from congress_videos.modules.speaker_turns import (
    PROCEDURAL_MAX_DURATION_SECS,
    PROCEDURAL_MIN_COVERAGE,
    PROCEDURAL_PATTERNS,
    is_procedural_turn,
)


class TestModuleConstants:

    def test_max_duration_is_15_seconds(self):
        assert PROCEDURAL_MAX_DURATION_SECS == 15.0

    def test_min_coverage_is_0_6(self):
        assert PROCEDURAL_MIN_COVERAGE == 0.6

    def test_patterns_are_named_pairs(self):
        """PROCEDURAL_PATTERNS must be (name, compiled_pattern) tuples."""
        assert len(PROCEDURAL_PATTERNS) >= 10
        for name, pattern in PROCEDURAL_PATTERNS:
            assert isinstance(name, str) and name
            assert hasattr(pattern, "search")


class TestDurationGate:

    def test_typical_handoff_flagged(self):
        """Spec scenario: 6s turn, exact handoff phrase → flagged with reason."""
        flagged, reason = is_procedural_turn("tiene la palabra el señor Pérez", 6.0)
        assert flagged is True
        assert reason is not None
        assert "dur=6.0s" in reason

    def test_long_turn_with_handoff_phrase_not_flagged(self):
        """Spec scenario: 340s turn opening with handoff phrase then substance
        → NOT flagged regardless of text (duration gate alone excludes it)."""
        text = (
            "tiene la palabra el señor Pérez. " + "y a continuación quiero hablar largamente "
            * 20
        )
        flagged, reason = is_procedural_turn(text, 340.0)
        assert flagged is False
        assert reason is None

    def test_exactly_at_max_duration_still_eligible(self):
        """duration == PROCEDURAL_MAX_DURATION_SECS is still eligible (<=, not <)."""
        flagged, _ = is_procedural_turn("ruego silencio.", 15.0)
        assert flagged is True

    def test_just_over_max_duration_not_eligible(self):
        flagged, reason = is_procedural_turn("ruego silencio.", 15.1)
        assert flagged is False
        assert reason is None


class TestCoverageGate:

    def test_short_substantive_reply_not_flagged(self):
        """Spec scenario: 8s turn, no procedural phrase at all → NOT flagged."""
        flagged, reason = is_procedural_turn("sí, apoyo la moción", 8.0)
        assert flagged is False
        assert reason is None

    def test_handoff_prefix_followed_by_substance_not_flagged(self):
        """Spec scenario: 12s turn opening with 'gracias, señoría' then
        substantive remarks → NOT flagged because the phrase covers only a
        small fraction of the turn's own text."""
        text = (
            "gracias, señoría, quiero añadir que el grupo parlamentario "
            "considera esta cuestión de máxima importancia para la ciudadanía"
        )
        flagged, reason = is_procedural_turn(text, 12.0)
        assert flagged is False
        assert reason is None

    def test_anti_pattern_gracias_senor_presidente_never_flagged(self):
        """Explicit anti-pattern: the canonical OPENING of a real intervention
        must never be flagged, alone or with trailing substance."""
        flagged, reason = is_procedural_turn("gracias, señor presidente", 5.0)
        assert flagged is False
        assert reason is None

    def test_anti_pattern_with_substance_never_flagged(self):
        flagged, reason = is_procedural_turn(
            "gracias, señor presidente, paso a exponer mi enmienda", 9.0
        )
        assert flagged is False
        assert reason is None

    def test_coverage_above_threshold_flagged(self):
        """Match covers 14/17 ≈ 0.82 of the normalized text → flagged."""
        flagged, reason = is_procedural_turn("ruego silencio xx", 5.0)
        assert flagged is True
        assert reason is not None

    def test_coverage_below_threshold_not_flagged(self):
        """Same phrase padded so the match covers 14/25 = 0.56 < 0.6 → NOT flagged."""
        flagged, reason = is_procedural_turn("ruego silencio " + "x" * 10, 5.0)
        assert flagged is False
        assert reason is None

    def test_empty_text_not_flagged(self):
        flagged, reason = is_procedural_turn("", 5.0)
        assert flagged is False
        assert reason is None

    def test_whitespace_only_text_not_flagged(self):
        flagged, reason = is_procedural_turn("   \n  ", 5.0)
        assert flagged is False
        assert reason is None


class TestAccentAndCaseTolerance:

    def test_uppercase_variant_flagged(self):
        flagged, reason = is_procedural_turn("TIENE LA PALABRA SU SEÑORÍA.", 4.0)
        assert flagged is True
        assert reason is not None

    def test_mixed_accent_variant_flagged(self):
        """señoria without the tilde/accent must still match (accent-tolerant)."""
        flagged, reason = is_procedural_turn("Tiene la palabra su senoria.", 4.0)
        assert flagged is True


class TestEachNamedPattern:
    """Every entry in PROCEDURAL_PATTERNS must independently trip the gate
    when it is the dominant content of a short turn."""

    SAMPLE_BY_PATTERN = {
        "tiene_la_palabra_named": "Tiene la palabra el señor Pérez.",
        "tiene_la_palabra_su_senoria": "Tiene la palabra su señoría.",
        "gracias_senoria": "Gracias, señoría.",
        "tiene_la_palabra_generic": "Tiene la palabra.",
        "adelante_senoria": "Adelante, señoría.",
        "para_contestar_responder": "Para responder.",
        "concluya_senoria": "Concluya, señoría.",
        "vaya_terminando": "Vaya terminando.",
        "ha_terminado_su_tiempo": "Ha terminado su tiempo.",
        "silencio_por_favor": "Silencio, por favor.",
        "ruego_silencio": "Ruego silencio.",
        "suspende_reanuda_sesion": "Se suspende la sesión.",
        "siguiente_punto_orden_dia": "Pasamos al siguiente punto del orden del día.",
    }

    def test_every_declared_pattern_has_a_sample(self):
        """Guard: the sample table above must cover every declared pattern name."""
        declared_names = {name for name, _ in PROCEDURAL_PATTERNS}
        assert declared_names == set(self.SAMPLE_BY_PATTERN)

    @pytest.mark.parametrize("name", sorted(SAMPLE_BY_PATTERN))
    def test_pattern_flags_its_sample(self, name):
        text = self.SAMPLE_BY_PATTERN[name]
        flagged, reason = is_procedural_turn(text, 5.0)
        assert flagged is True, f"pattern {name!r} sample {text!r} was not flagged"
        assert name in reason, f"reason {reason!r} must name the firing pattern {name!r}"
