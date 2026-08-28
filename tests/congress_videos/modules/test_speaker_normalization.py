"""Tests for congress_videos.modules.speaker_normalization (issue #263 rewire).

Step 1 (dirty-name resolution) now delegates to the roster-validated
resolve_chapter_speakers() instead of the retired
lookup_participant_fuzzy(threshold=0.90) + cached_json_completion gate.
Step 0 (institutional-role catalog) and Step 3 (bulk UPDATE) are unaffected.
All DB and LLM calls are stubbed; no real network or DB needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from congress_videos.modules.chapter_speaker_resolution import (
    ChapterSpeakerResolution,
    SpeakerMatch,
)


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


def _make_roster(entries=None):
    if entries is not None:
        return entries
    return [
        {"slug": "pedro-sanchez", "display_name": "Pedro Sánchez", "party": "PSOE"},
        {"slug": "ana-ruiz-perez", "display_name": "Ana Ruiz Pérez", "party": "PP"},
        {"slug": "juan-garcia-lopez", "display_name": "Juan García López", "party": "VOX"},
    ]


def _make_resolution(*matches: SpeakerMatch) -> ChapterSpeakerResolution:
    return ChapterSpeakerResolution(
        matches=tuple(matches),
        by_mention={m.mention: m for m in matches},
    )


def _match(mention: str, slug: str, display_name: str, confidence: float = 0.90) -> SpeakerMatch:
    return SpeakerMatch(
        mention=mention,
        participant_slug=slug,
        display_name=display_name,
        confidence=confidence,
    )


def _patched(roster=None, resolution=None):
    """Return the standard patch context for Step 1: roster + resolver."""
    return (
        patch(
            "congress_videos.modules.speaker_normalization.get_participants_roster",
            return_value=roster if roster is not None else _make_roster(),
        ),
        patch(
            "congress_videos.modules.speaker_normalization.resolve_chapter_speakers",
            return_value=resolution if resolution is not None else _make_resolution(),
        ),
    )


# ---------------------------------------------------------------------------
# T-01: Confident match rewrites chapter fields
# ---------------------------------------------------------------------------

class TestConfidentMatch:
    """Roster-validated match → canonical name replaces dirty name in all fields."""

    def test_matched_speaker_updates_chapter_fields(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Pedro Sanchez", "pedro-sanchez", "Pedro Sánchez", confidence=0.95)
        )

        chapter_id = 5
        speakers = ["Pedro Sanchez", "Unknown Person"]
        key_speakers = ["Pedro Sanchez"]
        timeline = [
            {"time": "00:01:00", "speaker": "Pedro Sanchez", "content": "Hola"},
            {"time": "00:02:00", "speaker": "Unknown Person", "content": "Bye"},
        ]

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch as mock_resolver:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                chapter_id, speakers, key_speakers, timeline,
                mock_conn, _make_config()
            )

        assert mock_resolver.called
        assert "Pedro Sanchez" in result.corrections
        assert result.corrections["Pedro Sanchez"] == "Pedro Sánchez"
        assert result.updated is True

        assert mock_cursor.execute.called
        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert any("update" in s and "video_chapters" in s for s in calls_sql), (
            "Expected UPDATE video_chapters to be executed for matched speaker"
        )

    def test_matched_speaker_cache_row_has_matched_status(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Pedro Sanchez", "pedro-sanchez", "Pedro Sánchez", confidence=0.95)
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch:
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

        resolution = _make_resolution(
            _match("Pedro Sanchez", "pedro-sanchez", "Pedro Sánchez", confidence=0.95)
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez", "Unknown Person"], ["Pedro Sanchez"], [],
                mock_conn, _make_config()
            )

        assert "Unknown Person" not in result.corrections


# ---------------------------------------------------------------------------
# T-02 (task 4.2): resolver rejection — no_match cache row, no chapter update
# ---------------------------------------------------------------------------

class TestResolverNoMatch:
    """A mention the resolver does not accept → no_match cache row, no UPDATE."""

    def test_no_match_cache_row_written_when_resolver_rejects(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        roster_patch, resolver_patch = _patched(resolution=_make_resolution())
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Carlos Romero Alias"], [], [],
                mock_conn, _make_config()
            )

        assert len(result.cache_rows) == 1
        assert result.cache_rows[0]["status"] == "no_match"

    def test_chapter_not_updated_when_resolver_rejects(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        roster_patch, resolver_patch = _patched(resolution=_make_resolution())
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Carlos Romero Alias"], [], [],
                mock_conn, _make_config()
            )

        assert result.updated is False
        calls_sql = [str(c.args[0]).lower() for c in mock_cursor.execute.call_args_list]
        assert not any("update" in s and "video_chapters" in s for s in calls_sql)


# ---------------------------------------------------------------------------
# T-05: Multiple speakers — each processed independently
# ---------------------------------------------------------------------------

class TestMultipleSpeakers:
    """Multiple dirty speakers → one cache row per unique dirty name, batched in one call."""

    def test_each_speaker_gets_independent_cache_row(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Ana Ruiz Pérez", "ana-ruiz-perez", "Ana Ruiz Pérez"),
            _match("Juan García López", "juan-garcia-lopez", "Juan García López"),
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch as mock_resolver:
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
        # A single batched call resolves all dirty mentions together.
        assert mock_resolver.call_count == 1

    def test_only_matched_speakers_corrected(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Ana Ruiz Pérez", "ana-ruiz-perez", "Ana Ruiz Pérez"),
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch:
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
# T-08: _dedupe_dirty_speakers drops placeholder names (unchanged)
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
# T-09: update_chapter_speakers — optional resolved_participant_slug param
# (unchanged — this tests database.py, not the resolution algorithm)
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

        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert update_calls, "Expected UPDATE video_chapters to be called"
        sql = str(update_calls[0].args[0])
        assert "resolved_participant_slug" in sql, (
            "resolved_participant_slug column must be in the UPDATE SQL"
        )
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
# T-10 (task 4.3 RED): normalize_chapter_speakers — slug is first accepted match
# ---------------------------------------------------------------------------

class TestNormalizeChapterSpeakersSlugFill:
    """Task 4.3 — a resolved primary match must write resolved_participant_slug
    in the SAME bulk UPDATE that patches speakers/key_speakers/timeline."""

    def test_matched_entry_triggers_slug_write_in_same_update(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Pedro Sanchez", "pedro-sanchez", "Pedro Sánchez", confidence=0.95)
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                42, ["Pedro Sanchez"], ["Pedro Sanchez"], [],
                mock_conn, _make_config()
            )

        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert update_calls, "Expected UPDATE video_chapters to be called"
        assert len(update_calls) == 1, "speakers/key_speakers/timeline/slug must patch in ONE UPDATE"
        sql = str(update_calls[0].args[0])
        assert "resolved_participant_slug" in sql
        params = update_calls[0].args[1]
        assert "pedro-sanchez" in params

    def test_slug_is_first_accepted_match_in_input_order(self, mock_db_conn):
        """With multiple accepted matches, the slug is the FIRST in input order."""
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Ana Ruiz Pérez", "ana-ruiz-perez", "Ana Ruiz Pérez"),
            _match("Juan García López", "juan-garcia-lopez", "Juan García López"),
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                7, ["Ana Ruiz Pérez", "Juan García López"], [], [],
                mock_conn, _make_config()
            )

        assert result.resolved_participant_slug == "ana-ruiz-perez"

    def test_unresolved_chapter_slug_stays_null(self, mock_db_conn):
        """When there is no match, resolved_participant_slug must not be written."""
        mock_conn, mock_cursor = mock_db_conn

        roster_patch, resolver_patch = _patched(resolution=_make_resolution())
        with roster_patch, resolver_patch:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                99, ["Unknown Speaker"], [], [],
                mock_conn, _make_config()
            )

        update_calls = [
            c for c in mock_cursor.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper() and "video_chapters" in str(c.args[0]).lower()
        ]
        assert not update_calls, (
            "UPDATE video_chapters must NOT be called when there is no match"
        )

    def test_resolver_called_exactly_once_for_slug_fill(self, mock_db_conn):
        """Slug fill is derived from the SAME batched resolver call — no extra calls."""
        mock_conn, mock_cursor = mock_db_conn

        resolution = _make_resolution(
            _match("Pedro Sanchez", "pedro-sanchez", "Pedro Sánchez", confidence=0.95)
        )

        roster_patch, resolver_patch = _patched(resolution=resolution)
        with roster_patch, resolver_patch as mock_resolver:
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                42, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        assert mock_resolver.call_count == 1


# ---------------------------------------------------------------------------
# ENABLED=False → immediate return
# ---------------------------------------------------------------------------

class TestEnabledFalse:
    """ENABLED=False → function returns without any DB or resolver calls."""

    def test_disabled_config_returns_immediately(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.get_participants_roster") as mock_roster,
            patch("congress_videos.modules.speaker_normalization.resolve_chapter_speakers") as mock_resolver,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config(enabled=False)
            )

        mock_roster.assert_not_called()
        mock_resolver.assert_not_called()
        mock_cursor.execute.assert_not_called()
        assert result.updated is False
        assert result.corrections == {}
        assert result.cache_rows == []


# ---------------------------------------------------------------------------
# Institutional-role resolution (Step 0 — unaffected by the Step 1 rewire)
# ---------------------------------------------------------------------------

class TestInstitutionalRoleResolution:
    """Role-only mentions resolve to a canonical name via the bundled catalog."""

    def test_role_mention_resolved_for_non_deputy_uses_catalog_name(self, mock_db_conn):
        from datetime import date

        mock_conn, mock_cursor = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_by_slug",
                  return_value=None),
            patch("congress_videos.modules.speaker_normalization.get_participants_roster") as mock_roster,
            patch("congress_videos.modules.speaker_normalization.resolve_chapter_speakers") as mock_resolver,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                7, ["Ministra de Defensa"], [], [],
                mock_conn, _make_config(), session_date=date(2026, 6, 1),
            )

        assert result.corrections["Ministra de Defensa"] == "Robles Fernández, Margarita"
        # A deterministic role hit must not consult the roster or the resolver:
        # the Step-0-wins filter removes it before Step 1 ever runs.
        mock_roster.assert_not_called()
        mock_resolver.assert_not_called()

    def test_role_mention_prefers_participant_row_display_name(self, mock_db_conn):
        from datetime import date

        mock_conn, _ = mock_db_conn
        row = {
            "normalized_name": "oscar puente santiago",
            "display_name": "Puente Santiago, Óscar",
            "party": "PartyA",
        }

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_by_slug",
                  return_value=row),
            patch("congress_videos.modules.speaker_normalization.get_participants_roster"),
            patch("congress_videos.modules.speaker_normalization.resolve_chapter_speakers"),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                8, ["el Ministro de Transportes"], [], [],
                mock_conn, _make_config(), session_date=date(2026, 6, 1),
            )

        assert result.corrections["el Ministro de Transportes"] == "Puente Santiago, Óscar"

    def test_missing_session_date_skips_role_resolution(self, mock_db_conn):
        mock_conn, _ = mock_db_conn

        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_by_slug") as mock_slug,
            patch(
                "congress_videos.modules.speaker_normalization.get_participants_roster",
                return_value=[],
            ),
            patch(
                "congress_videos.modules.speaker_normalization.resolve_chapter_speakers",
                return_value=ChapterSpeakerResolution(),
            ),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                9, ["Ministra de Defensa"], [], [],
                mock_conn, _make_config(),
            )

        # Without a session date the catalog is never consulted; the role-only
        # mention is left to the placeholder filter, so it is not corrected.
        assert "Ministra de Defensa" not in result.corrections
        mock_slug.assert_not_called()


# ---------------------------------------------------------------------------
# Step-0-wins filter (A2 amendment, task 4.1) — Step 0 corrections never
# reach Step 1's resolver call.
# ---------------------------------------------------------------------------

class TestStepZeroWinsFilter:
    """A mention already corrected by Step 0 (institutional-role catalog) must be
    excluded from the mentions sent to Step 1's resolve_chapter_speakers call."""

    def test_role_resolved_mention_excluded_from_step_one_mentions(self, mock_db_conn):
        from datetime import date

        mock_conn, mock_cursor = mock_db_conn

        roster_patch, resolver_patch = _patched(resolution=_make_resolution())
        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_by_slug",
                  return_value=None),
            roster_patch,
            resolver_patch as mock_resolver,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            normalize_chapter_speakers(
                7,
                ["Ministra de Defensa", "Pedro Sanchez"],
                [],
                [],
                mock_conn,
                _make_config(),
                session_date=date(2026, 6, 1),
            )

        # Step 0 resolves "Ministra de Defensa"; only "Pedro Sanchez" should
        # reach the Step 1 resolver call.
        assert mock_resolver.call_count == 1
        mentions_arg = mock_resolver.call_args[0][0]
        assert "Ministra de Defensa" not in mentions_arg
        assert "Pedro Sanchez" in mentions_arg

    def test_step_zero_correction_not_overwritten_by_step_one(self, mock_db_conn):
        """Even if Step 1's roster happened to also match the role-resolved name,
        the mention never reaches Step 1, so Step 0's correction is never at risk."""
        from datetime import date

        mock_conn, mock_cursor = mock_db_conn

        roster_patch, resolver_patch = _patched(resolution=_make_resolution())
        with (
            patch("congress_videos.modules.speaker_normalization.lookup_participant_by_slug",
                  return_value=None),
            roster_patch,
            resolver_patch,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                7, ["Ministra de Defensa"], [], [],
                mock_conn, _make_config(), session_date=date(2026, 6, 1),
            )

        assert result.corrections["Ministra de Defensa"] == "Robles Fernández, Margarita"


# ---------------------------------------------------------------------------
# Participants roster fetch failure — degrade gracefully, never raise
# (task 4.4)
# ---------------------------------------------------------------------------

class TestParticipantsFetchFailure:
    """get_participants_roster() raising must degrade to no corrections, no raise."""

    def test_roster_fetch_failure_yields_no_corrections(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with (
            patch(
                "congress_videos.modules.speaker_normalization.get_participants_roster",
                side_effect=RuntimeError("db unavailable"),
            ),
            patch(
                "congress_videos.modules.speaker_normalization.resolve_chapter_speakers",
                return_value=ChapterSpeakerResolution(),
            ) as mock_resolver,
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            result = normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )

        assert result.corrections == {}
        assert result.updated is False
        # Degrades to an empty roster; the resolver is still called but with []
        # participants, and it returns nothing to correct.
        mock_resolver.assert_called_once_with(["Pedro Sanchez"], [])

    def test_roster_fetch_failure_does_not_raise(self, mock_db_conn):
        mock_conn, mock_cursor = mock_db_conn

        with patch(
            "congress_videos.modules.speaker_normalization.get_participants_roster",
            side_effect=RuntimeError("db unavailable"),
        ):
            from congress_videos.modules.speaker_normalization import normalize_chapter_speakers
            # Must not raise.
            normalize_chapter_speakers(
                1, ["Pedro Sanchez"], [], [],
                mock_conn, _make_config()
            )
