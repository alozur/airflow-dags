"""[RED] Tests for is_procedural_turn — the procedural-turn detection gate (issue #143).

Pure AND-gate: duration <= PROCEDURAL_MAX_DURATION_SECS AND phrase coverage
>= PROCEDURAL_MIN_COVERAGE over the turn's OWN accent-stripped, lowercased,
whitespace-collapsed text. Precision-first: ambiguous cases must NOT be flagged.

No I/O, no DB, plain strings only.
"""
from __future__ import annotations

import pytest

from congress_videos.modules.speaker_turns import (
    PROCEDURAL_FILLER_PATTERNS,
    PROCEDURAL_MAX_DURATION_SECS,
    PROCEDURAL_MAX_UNMATCHED_RUN,
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
        # Real-corpus additions (chapters 318/262, session 2026-06):
        "gracias_titled": "Muchas gracias, señora ministra.",
        "cuando_quiera": "Señora ministra, cuando quiera.",
        "preguntas_dirigidas": "Pasamos ahora a las preguntas dirigidas a la señora ministra de Defensa.",
        "pregunta_formula": "La siguiente pregunta la formula la diputada doña Cayetana Álvarez de Toledo.",
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


class TestFillerPatterns:
    """Filler patterns add coverage but can NEVER justify a flag alone."""

    def test_filler_constant_is_named_pairs(self):
        assert len(PROCEDURAL_FILLER_PATTERNS) >= 4
        for name, pattern in PROCEDURAL_FILLER_PATTERNS:
            assert isinstance(name, str) and name
            assert hasattr(pattern, "search")

    def test_max_unmatched_run_constant(self):
        assert PROCEDURAL_MAX_UNMATCHED_RUN == 40

    def test_fillers_alone_never_flag(self):
        """Vocatives + courtesy fillers with no core phrase → NOT flagged,
        even at full coverage (a heckle can be all vocative)."""
        flagged, reason = is_procedural_turn("Señora ministra, por favor.", 3.0)
        assert flagged is False
        assert reason is None

    def test_bare_gracias_alone_never_flags(self):
        flagged, reason = is_procedural_turn("Muchas gracias.", 2.0)
        assert flagged is False
        assert reason is None


class TestRealCorpusHandoffs:
    """Literal chair handoffs from production chapters 318 (pmLyT3dd1hQ) and
    262 (mjUgQQVHYJg) — the exact per-turn SRT text the pipeline sees,
    SRT-bleed included. Every one of these was a miss before the corpus
    tuning pass (0/90 turns flagged)."""

    MUST_FLAG = [
        # (duration, turn's own SRT text)
        (3.4, "Muchas gracias, señora ministra. Señora Funez, tiene la palabra, por favor."),
        (3.8, "Muchas gracias, señora ministra. Señor Carazo, ahora,"),
        (3.1, "Muchas gracias, señor ministro. Señor Tellado, cuando quiera. Señor Bolaños,"),
        (2.9, "Muchas gracias, señora diputada. Señor ministro, cuando quiera."),
        (3.0, "Muchas gracias, señor ministro. Señora Álvarez de Toledo."),
        (5.5, "Muchas gracias, señora ministra. Por favor, silencio. Señor Conde, tiene la palabra."),
        (2.5, "Muchas gracias, señor Rojas. Señora ministra, tiene la palabra, por favor."),
        (4.4, "Muchas gracias, señor conde, señora ministra, cuando quiera."),
        (
            8.7,
            "Muchas gracias, señor ministro. La siguiente pregunta la formula la "
            "diputada doña Cayetana Álvarez de Toledo, del grupo parlamentario "
            "popular que tiene la palabra cuando quiera.",
        ),
        (
            10.9,
            "Muchas gracias. Pasamos ahora a las preguntas dirigidas a la señora "
            "ministra de Defensa. La primera la fórmula, por el grupo parlamentario "
            "popular, la diputada doña Isabel Cedó.",
        ),
    ]

    MUST_NOT_FLAG = [
        # Short substantive turns / SRT-bleed openings followed by substance.
        (
            14.1,
            "Muchas gracias, señor ministro. Señor Tellado, cuando quiera. Señor "
            "Bolaños, no se apropie el discurso del Papa, porque el Papa habló de "
            "muchas otras cosas que a ustedes no les gustan nada.",
        ),
        (
            7.1,
            "que tiene la palabra cuando usted quiera. Muchas gracias. ¿Mantiene "
            "usted también que existe una conspiración judicial para derribar al "
            "gobierno?",
        ),
        (
            6.5,
            "señor conde, señora ministra, cuando quiera. Gracias, señoría. Usted, "
            "además, que fue secretario de Estado, conoce bien cómo dejaron el "
            "Ministerio de Defensa.",
        ),
        (2.8, "Repararemos el daño causado, señor Hurtasun, se lo aseguro."),
        (2.7, "Los de la democracia, los del diálogo. Madre mía, ¿qué es lo que les molesta?"),
        (
            14.0,
            "Muchas gracias, señor Rojas. Señora ministra, eh, mire, yo tengo una "
            "conciencia lo suficientemente fuerte para soportar sus ataques y sus "
            "insultos durante toda la legislatura.",
        ),
        # Canonical opening of a REAL intervention (thanks the chair).
        (9.0, "Gracias, señora presidenta. España es un estado de derecho sólido."),
    ]

    @pytest.mark.parametrize("duration,text", MUST_FLAG)
    def test_real_handoff_flagged(self, duration, text):
        flagged, reason = is_procedural_turn(text, duration)
        assert flagged is True, f"real handoff missed: {text[:60]!r}"
        assert reason is not None

    @pytest.mark.parametrize("duration,text", MUST_NOT_FLAG)
    def test_real_substance_not_flagged(self, duration, text):
        flagged, reason = is_procedural_turn(text, duration)
        assert flagged is False, f"false positive on: {text[:60]!r} ({reason})"
        assert reason is None
