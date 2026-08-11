"""Tests for congress_videos.modules.speaker_normalization — TASK 6 (RED).

Covers six behavioral scenarios from REQ: speaker_normalization module.
All DB and LLM calls are stubbed; no real network or DB needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    """Provide minimal DB env vars so PostgresConnection.__init__ does not raise."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def mock_db_conn(mock_psycopg2_connection):
    """Return (mock_conn, mock_cursor) with psycopg2 fully mocked."""
    _, mock_conn, mock_cursor = mock_psycopg2_connection
    return mock_conn, mock_cursor


def _make_config(enabled: bool = True, fuzzy_threshold: float = 0.90,
                 ai_model: str = "gpt-4o-mini", context_enabled: bool = True):
    """Build a minimal config-like namespace."""
    cfg = MagicMock()
    cfg.ENABLED = enabled
    cfg.FUZZY_THRESHOLD = fuzzy_threshold
    cfg.AI_MODEL = ai_model
    cfg.CONTEXT_ENABLED = context_enabled
    return cfg


def _make_ai_result(decision: str, confidence: float = 0.95, reason: str = "test"):
    """Return a cached_json_completion-style result dict."""
    return {
        "data": {"decision": decision, "confidence": confidence, "reason": reason},
        "raw_content": "",
        "error": None,
    }


def _make_participant(display_name: str, normalized_name: str = "pedro sanchez"):
    """Return a participant row dict as returned by lookup_participant_fuzzy."""
    return {
        "normalized_name": normalized_name,
        "display_name": display_name,
        "party": "PartyA",
        "parliamentary_group": "GroupA",
        "constituency": "Madrid",
        "biography": None,
        "photo_url": None,
    }


# ---------------------------------------------------------------------------
# T-01: Confident match rewrites chapter fields
# ---------------------------------------------------------------------------

class TestConfidentMatch:
    """T-01 — fuzzy hit + AI match → canonical name replaces dirty name in all fields."""

    def test_matched_speaker_updates_chapter_fields(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("match", confidence=0.95)

        chapter_id = 5
        speakers = ["Pedro Sanchez", "Unknown Person"]
        key_speakers = ["Pedro Sanchez"]
        timeline = [
            {"time": "00:01:00", "speaker": "Pedro Sanchez", "content": "Hola"},
            {"time": "00:02:00", "speaker": "Unknown Person", "content": "Bye"},
        ]

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate) as mock_fuzzy,
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result) as mock_ai,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                chapter_id, speakers, key_speakers, timeline,
                mock_conn, _make_config()
            )

        # AI must have been called for the matched dirty name
        assert mock_ai.called

        # Result reports a correction
        assert "Pedro Sanchez" in result.corrections
        assert result.corrections["Pedro Sanchez"] == "Pedro Sánchez"
        assert result.updated is True

        # Cache row for matched speaker written via cursor execute
        # Verify that an upsert was attempted (execute called)
        assert mock_cursor.execute.called

        # The DB UPDATE for video_chapters must have been issued
        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert any("update" in s and "video_chapters" in s for s in calls_sql), (
            "Expected UPDATE video_chapters to be executed for matched speaker"
        )

    def test_matched_speaker_cache_row_has_matched_status(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("match", confidence=0.95)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        assert len(result.cache_rows) == 1
        row = result.cache_rows[0]
        assert row["status"] == "matched"
        assert row["dirty_speaker"] == "Pedro Sanchez"
        assert row["canonical_speaker"] == "Pedro Sánchez"

    def test_unknown_person_left_unchanged(self, mock_db_conn):
        """Only the matched dirty name should be corrected; others remain."""
        mock_conn, mock_cursor = mock_db_conn

        def fuzzy_side_effect(name, threshold):
            if name == "Pedro Sanchez":
                return _make_participant("Pedro Sánchez", "pedro sanchez")
            return None

        ai_result = _make_ai_result("match", confidence=0.95)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  side_effect=fuzzy_side_effect),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez", "Unknown Person"], ["Pedro Sanchez"], [],
                mock_conn, _make_config()
            )

        # Unknown Person must not appear in corrections
        assert "Unknown Person" not in result.corrections


# ---------------------------------------------------------------------------
# T-02: Fuzzy threshold miss — no AI call
# ---------------------------------------------------------------------------

class TestFuzzyThresholdMiss:
    """T-02 — score below FUZZY_THRESHOLD → cached_json_completion NOT called, no_match row."""

    def test_no_ai_call_when_fuzzy_misses(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=None) as mock_fuzzy,
            patch("congress_videos.modules.speaker_normalization.cached_json_completion") as mock_ai,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Carlos Romero Alias"], [], [],
                mock_conn, _make_config()
            )

        mock_ai.assert_not_called()

    def test_no_match_cache_row_written_on_fuzzy_miss(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=None),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion"),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Carlos Romero Alias"], [], [],
                mock_conn, _make_config()
            )

        assert len(result.cache_rows) == 1
        assert result.cache_rows[0]["status"] == "no_match"

    def test_chapter_not_updated_on_fuzzy_miss(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=None),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion"),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Carlos Romero Alias"], [], [],
                mock_conn, _make_config()
            )

        assert result.updated is False

        # No UPDATE to video_chapters should have been issued
        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert not any("update" in s and "video_chapters" in s for s in calls_sql)


# ---------------------------------------------------------------------------
# T-03: AI returns no_match — chapter unchanged
# ---------------------------------------------------------------------------

class TestAINoMatch:
    """T-03 — fuzzy hit, but AI returns no_match → no UPDATE, no_match cache row."""

    def test_no_update_when_ai_returns_no_match(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("no_match", confidence=0.3)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez Variant"], [], [],
                mock_conn, _make_config()
            )

        assert result.updated is False

        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert not any("update" in s and "video_chapters" in s for s in calls_sql)

    def test_no_match_cache_row_written_when_ai_returns_no_match(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("no_match", confidence=0.3)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez Variant"], [], [],
                mock_conn, _make_config()
            )

        assert len(result.cache_rows) == 1
        assert result.cache_rows[0]["status"] == "no_match"


# ---------------------------------------------------------------------------
# T-04: AI returns needs_manual — chapter unchanged
# ---------------------------------------------------------------------------

class TestAINeedsManual:
    """T-04 — AI cannot decide → needs_manual cache row, no chapter update."""

    def test_no_update_when_ai_returns_needs_manual(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("needs_manual", confidence=0.5)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Ambiguo Real"], [], [],
                mock_conn, _make_config()
            )

        assert result.updated is False

    def test_needs_manual_cache_row_written(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("needs_manual", confidence=0.5)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Ambiguo Real"], [], [],
                mock_conn, _make_config()
            )

        assert len(result.cache_rows) == 1
        assert result.cache_rows[0]["status"] == "needs_manual"


# ---------------------------------------------------------------------------
# T-05: Multiple speakers — each processed independently
# ---------------------------------------------------------------------------

class TestMultipleSpeakers:
    """T-05 — three dirty speakers → one cache row per unique dirty name."""

    def test_each_speaker_gets_independent_cache_row(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        participant_map = {
            "Ana Ruiz Pérez": _make_participant("Ana Ruiz Pérez", "ana ruiz perez"),
            "Juan García López": _make_participant("Juan García López", "juan garcia lopez"),
        }

        def fuzzy_side(name, threshold):
            return participant_map.get(name)

        ai_result = _make_ai_result("match", confidence=0.90)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  side_effect=fuzzy_side),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                7,
                ["Ana Ruiz Pérez", "Juan García López", "María Fernández Gil"],
                [],
                [],
                mock_conn,
                _make_config(),
            )

        # One cache row per unique dirty speaker (all three are real multi-word names)
        assert len(result.cache_rows) == 3

    def test_only_matched_speakers_corrected(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        def fuzzy_side(name, threshold):
            if name == "Ana Ruiz Pérez":
                return _make_participant("Ana Ruiz Pérez", "ana ruiz perez")
            return None

        ai_result = _make_ai_result("match", confidence=0.90)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  side_effect=fuzzy_side),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                7,
                ["Ana Ruiz Pérez", "Juan García López", "María Fernández Gil"],
                [],
                [],
                mock_conn,
                _make_config(),
            )

        assert "Ana Ruiz Pérez" in result.corrections
        assert "Juan García López" not in result.corrections
        assert "María Fernández Gil" not in result.corrections


# ---------------------------------------------------------------------------
# T-06: AI call error — graceful skip, no cache write
# ---------------------------------------------------------------------------

class TestAICallError:
    """T-06 — cached_json_completion returns non-None error → skip, no cache row."""

    def test_ai_error_skips_cache_write(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_error_result = {"data": None, "raw_content": "", "error": "Timeout"}

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_error_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        # No cache row written on error
        assert len(result.cache_rows) == 0

    def test_ai_error_does_not_update_chapter(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_error_result = {"data": None, "raw_content": "", "error": "Timeout"}

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_error_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        assert result.updated is False
        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert not any("update" in s and "video_chapters" in s for s in calls_sql)


# ---------------------------------------------------------------------------
# T-08: _dedupe_dirty_speakers drops placeholder names (Task 1.7 RED)
# ---------------------------------------------------------------------------

class TestDedupeDirtySpeakersPlaceholderFilter:
    """Task 1.7 RED — _dedupe_dirty_speakers must filter placeholders before dedup."""

    def test_desconocido_dropped_from_speakers(self):
        """'Desconocido' must not appear in the deduped output."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            ["Desconocido", "Pedro Sánchez"],
            [],
            [],
        )
        assert "Desconocido" not in result
        assert "Pedro Sánchez" in result

    def test_unknown_dropped_from_speakers(self):
        """'Unknown' must not appear in the deduped output."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            ["Unknown", "María López"],
            [],
            [],
        )
        assert "Unknown" not in result
        assert "María López" in result

    def test_no_especificado_dropped_from_key_speakers(self):
        """'(No especificado)' in key_speakers must be filtered."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            [],
            ["(No especificado)", "Ana Martínez"],
            [],
        )
        assert "(No especificado)" not in result
        assert "Ana Martínez" in result

    def test_placeholder_in_timeline_speaker_dropped(self):
        """Placeholder in timeline[].speaker must be filtered."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            [],
            [],
            [{"speaker": "Desconocido", "content": "...", "time": "00:01:00"}],
        )
        assert "Desconocido" not in result

    def test_all_real_speakers_preserved(self):
        """Non-placeholder names must still appear in deduped output."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            ["Ana Ruiz", "Pedro García"],
            ["Laura Gómez"],
            [],
        )
        assert "Ana Ruiz" in result
        assert "Pedro García" in result
        assert "Laura Gómez" in result

    def test_mix_placeholders_and_real_names(self):
        """Mixed list: only real names survive, order preserved."""
        from congress_videos.modules.speaker_normalization import _dedupe_dirty_speakers

        result = _dedupe_dirty_speakers(
            ["Desconocido", "Ana Ruiz", "Unknown"],
            ["Pedro García"],
            [{"speaker": "No especificado", "content": "", "time": "00:00"}],
        )
        assert result == ["Ana Ruiz", "Pedro García"]


# ---------------------------------------------------------------------------
# T-09 (Task 2.3 RED): update_chapter_speakers — optional resolved_participant_slug param
# ---------------------------------------------------------------------------

class TestUpdateChapterSpeakersSlugParam:
    """Task 2.3 RED — update_chapter_speakers must accept and write resolved_participant_slug.

    Tests the Database.update_chapter_speakers method in database.py.
    """

    def test_slug_written_when_provided(self, mock_psycopg2_connection):
        """When resolved_participant_slug is provided, it must be included in the UPDATE."""
        _, mock_conn, mock_cursor = mock_psycopg2_connection

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.update_chapter_speakers(
            chapter_id=42,
            speakers=["Pedro Sánchez"],
            key_speakers=["Pedro Sánchez"],
            timeline=[],
            resolved_participant_slug="pedro-sanchez",
        )

        # Find the UPDATE call and assert resolved_participant_slug appears in it
        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert update_calls, "Expected UPDATE video_chapters to be called"
        sql = str(update_calls[0].args[0])
        assert "resolved_participant_slug" in sql, (
            "resolved_participant_slug column must be in the UPDATE SQL"
        )
        # Verify the slug value is in the params
        params = update_calls[0].args[1]
        assert "pedro-sanchez" in params, (
            "resolved_participant_slug value must be passed as a parameter"
        )

    def test_slug_not_written_when_none(self, mock_psycopg2_connection):
        """When resolved_participant_slug is None (default), it must NOT appear in UPDATE SQL."""
        _, mock_conn, mock_cursor = mock_psycopg2_connection

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.update_chapter_speakers(
            chapter_id=99,
            speakers=["Ana López"],
            key_speakers=[],
            timeline=[],
        )

        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert update_calls, "Expected UPDATE video_chapters to be called"
        sql = str(update_calls[0].args[0])
        assert "resolved_participant_slug" not in sql, (
            "resolved_participant_slug must not be in the UPDATE SQL when not provided"
        )


# ---------------------------------------------------------------------------
# T-10 (Task 2.5 RED): normalize_chapter_speakers — slug fill on matched entry
# ---------------------------------------------------------------------------

class TestNormalizeChapterSpeakersSlugFill:
    """Task 2.5 RED — normalize_chapter_speakers must write resolved_participant_slug on match.

    After AI confirms 'match', the function derives the slug from the candidate's
    normalized_name and writes it via update_chapter_speakers (or direct UPDATE).
    No LLM call is triggered by the slug fill itself.
    """

    def test_matched_cache_entry_triggers_slug_write(self, mock_db_conn):
        """A confirmed 'matched' cache entry must write resolved_participant_slug."""
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("match", confidence=0.95)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                42, ["Pedro Sanchez"], ["Pedro Sanchez"], [],
                mock_conn, _make_config()
            )

        # The bulk UPDATE on video_chapters must include resolved_participant_slug
        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert update_calls, "Expected UPDATE video_chapters to be called"
        sql = str(update_calls[0].args[0])
        assert "resolved_participant_slug" in sql, (
            "resolved_participant_slug must be set in the UPDATE after a matched entry"
        )
        params = update_calls[0].args[1]
        # Slug derived from normalized_name "pedro sanchez" → "pedro-sanchez"
        assert "pedro-sanchez" in params, (
            "slug 'pedro-sanchez' must be passed as a parameter in the UPDATE"
        )

    def test_unresolved_chapter_slug_stays_null(self, mock_db_conn):
        """When there is no match, resolved_participant_slug must not be written."""
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=None),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion"),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                99, ["Unknown Speaker"], [], [],
                mock_conn, _make_config()
            )

        # No UPDATE should have been issued (no match, no write)
        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert not update_calls, (
            "UPDATE video_chapters must NOT be called when there is no match"
        )

    def test_no_llm_call_on_slug_fill_path(self, mock_db_conn):
        """Slug fill must not trigger any additional LLM calls beyond the match itself."""
        mock_conn, mock_cursor = mock_db_conn

        candidate = _make_participant("Pedro Sánchez", "pedro sanchez")
        ai_result = _make_ai_result("match", confidence=0.95)

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy",
                  return_value=candidate),
            patch("congress_videos.modules.speaker_normalization.cached_json_completion",
                  return_value=ai_result) as mock_ai,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                42, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        # cached_json_completion called exactly once (for the match decision only)
        assert mock_ai.call_count == 1, (
            "cached_json_completion must be called exactly once — slug fill is LLM-free"
        )


# ---------------------------------------------------------------------------
# T-07 (Bonus from task description): ENABLED=False → immediate return
# ---------------------------------------------------------------------------

class TestEnabledFalse:
    """ENABLED=False → function returns without any DB or AI calls."""

    def test_disabled_config_returns_immediately(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_fuzzy") as mock_fuzzy,
            patch("congress_videos.modules.speaker_normalization.cached_json_completion") as mock_ai,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config(enabled=False)
            )

        mock_fuzzy.assert_not_called()
        mock_ai.assert_not_called()
        mock_cursor.execute.assert_not_called()
        assert result.updated is False
        assert result.corrections == {}
        assert result.cache_rows == []
