"""Tests for speaker-resolution DB methods in CongressionalVideoDB (issue #177).

TDD RED cycle: written before implementation.
Tests: mark_turn_resolved + select_unprepared_turns resolution columns.

Issue #321 (Gate A — sibling-label scoping): mark_turn_resolved gained a
required 5th positional ``representative_turn_id`` and its UPDATE WHERE
clause became a subselect joining speaker_turns on speaker_label, instead
of a bare ``output_path=%s`` filter.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_conn(rows=None):
    """Return a fake pg_conn context-manager whose cursor returns rows."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.fetchone.return_value = None
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor

    pg_conn_mock = MagicMock()
    pg_conn_mock.get_connection.return_value = conn
    pg_conn_mock.get_qualified_table.side_effect = lambda name: name

    return pg_conn_mock, cursor


# ---------------------------------------------------------------------------
# mark_turn_resolved
# ---------------------------------------------------------------------------


class TestMarkTurnResolved:
    """mark_turn_resolved(output_path, slug, confidence, method, representative_turn_id)
    updates only the sibling rows sharing BOTH output_path and speaker_label (Gate A)."""

    def test_sql_updates_resolved_participant_slug(self):
        """UPDATE must set resolved_participant_slug."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG" in query

    def test_sql_updates_speaker_resolution_confidence(self):
        """UPDATE must set speaker_resolution_confidence."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_CONFIDENCE" in query

    def test_sql_updates_speaker_resolution_method(self):
        """UPDATE must set speaker_resolution_method."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_METHOD" in query

    def test_sql_filters_by_output_path(self):
        """UPDATE WHERE must filter by output_path (fan-out for grouped turns)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        query = cursor.execute.call_args[0][0].upper()
        assert "OUTPUT_PATH" in query and "WHERE" in query

    def test_sql_where_is_label_scoped_subselect(self):
        """UPDATE WHERE must be a subselect joining speaker_turns on speaker_label
        for the representative turn's label — not a bare output_path=%s filter
        (issue #321 Gate A: withholds attribution from mismatched siblings)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        query = cursor.execute.call_args[0][0].upper().replace("\n", " ")
        assert "TURN_ID IN" in query, f"WHERE must scope via a turn_id subselect; got: {query}"
        assert "SPEAKER_TURNS" in query, f"subselect must join speaker_turns; got: {query}"
        assert "SPEAKER_LABEL" in query, f"subselect must match on speaker_label; got: {query}"

    def test_sql_passes_all_five_params(self):
        """UPDATE must pass slug, confidence, method, output_path, and
        representative_turn_id as params."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        output_path = "/data/turns/1/video.mp4"
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(output_path, "pedro-sanchez", 0.92, "ai_srt_context", 501)

        params = cursor.execute.call_args[0][1]
        assert "pedro-sanchez" in params
        assert 0.92 in params
        assert "ai_srt_context" in params
        assert output_path in params
        assert 501 in params

    def test_representative_turn_id_is_required(self):
        """The 5th positional arg has no default — an un-migrated 4-arg caller
        must fail loudly rather than blanket-writing (issue #321)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            with pytest.raises(TypeError):
                db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context")

    def test_returns_none(self):
        """mark_turn_resolved must return None (void operation)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        assert result is None

    # -----------------------------------------------------------------
    # evidence= (issue #430, migration 046) — optional 6th kwarg
    # -----------------------------------------------------------------

    _EXPECTED_SQL_WITHOUT_EVIDENCE = (
        "\n"
        "                    UPDATE speaker_turn_videos\n"
        "                    SET resolved_participant_slug = %s,\n"
        "                        speaker_resolution_confidence = %s,\n"
        "                        speaker_resolution_method = %s\n"
        "                    WHERE turn_id IN (\n"
        "                        SELECT stv2.turn_id\n"
        "                        FROM speaker_turn_videos stv2\n"
        "                        JOIN speaker_turns st2 ON stv2.turn_id = st2.turn_id\n"
        "                        WHERE stv2.output_path = %s\n"
        "                          AND st2.speaker_label = (\n"
        "                              SELECT st3.speaker_label FROM speaker_turns st3\n"
        "                              WHERE st3.turn_id = %s\n"
        "                          )\n"
        "                    )\n"
        "                    "
    )

    @pytest.mark.parametrize(
        ("evidence", "expect_column"),
        [
            ('{"method": "monologue_window_v1"}', True),
            (None, False),
        ],
        ids=["evidence-provided", "evidence-omitted"],
    )
    def test_sql_includes_evidence_column_only_when_provided(self, evidence, expect_column):
        """SET gains speaker_resolution_evidence = %s only when evidence is
        not None -- the 5-positional (no-evidence) path must not grow it."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501, evidence=evidence
            )

        query = cursor.execute.call_args[0][0].upper()
        assert ("SPEAKER_RESOLUTION_EVIDENCE" in query) is expect_column

    def test_five_positional_arg_call_leaves_sql_byte_identical(self):
        """A 5-positional-arg call (no evidence kwarg at all) must produce
        the exact SQL text Gate A produced before issue #430 -- byte-for-byte,
        proving the new optional parameter is additive-only."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved("/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context", 501)

        assert cursor.execute.call_args[0][0] == self._EXPECTED_SQL_WITHOUT_EVIDENCE


# ---------------------------------------------------------------------------
# promote_turn_type_to_qa (issue #282 rule 4)
# ---------------------------------------------------------------------------


class TestPromoteTurnTypeToQa:
    """promote_turn_type_to_qa(output_path) — promote-only monologue->qa write-back."""

    def test_sql_sets_turn_type_to_qa(self):
        """UPDATE must set turn_type = 'qa'."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 1
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.promote_turn_type_to_qa("/data/turns/1/video.mp4")

        query = cursor.execute.call_args[0][0].upper().replace("\n", " ")
        assert "TURN_TYPE = 'QA'" in query, f"must SET turn_type='qa'; got: {query}"

    def test_sql_guards_with_monologue_where_clause(self):
        """UPDATE WHERE must guard with turn_type = 'monologue' (promote-only)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 1
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.promote_turn_type_to_qa("/data/turns/1/video.mp4")

        query = cursor.execute.call_args[0][0].upper().replace("\n", " ")
        assert "TURN_TYPE = 'MONOLOGUE'" in query, (
            f"must guard promotion on the current value being 'monologue'; got: {query}"
        )

    def test_sql_filters_by_output_path(self):
        """UPDATE WHERE must filter by output_path, and pass it as the sole param."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 1
        output_path = "/data/turns/1/video.mp4"
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.promote_turn_type_to_qa(output_path)

        query = cursor.execute.call_args[0][0].upper()
        params = cursor.execute.call_args[0][1]
        assert "OUTPUT_PATH" in query and "WHERE" in query
        assert params == (output_path,)

    def test_returns_rowcount(self):
        """promote_turn_type_to_qa must return the UPDATE's rowcount."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 3
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.promote_turn_type_to_qa("/data/turns/1/video.mp4")

        assert result == 3

    def test_second_call_is_idempotent_noop(self):
        """A repeated call for an already-'qa' output_path affects zero rows."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 0
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.promote_turn_type_to_qa("/data/turns/1/video.mp4")

        assert result == 0


# ---------------------------------------------------------------------------
# mark_chapter_resolved (issue #263)
# ---------------------------------------------------------------------------


class TestMarkChapterResolved:
    """mark_chapter_resolved(chapter_id, slug) never-override write-back."""

    def test_sql_updates_resolved_participant_slug(self):
        """UPDATE must set resolved_participant_slug."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_chapter_resolved(42, "edurne-uriarte-bengoechea")

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG" in query

    def test_sql_guards_with_is_null(self):
        """UPDATE WHERE must guard with resolved_participant_slug IS NULL (never-override)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_chapter_resolved(42, "edurne-uriarte-bengoechea")

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG IS NULL" in query.replace("\n", " ")

    def test_sql_filters_by_chapter_id(self):
        """UPDATE WHERE must filter by chapter_id."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_chapter_resolved(42, "edurne-uriarte-bengoechea")

        query = cursor.execute.call_args[0][0].upper()
        assert "CHAPTER_ID" in query and "WHERE" in query

    def test_sql_passes_slug_and_chapter_id_params(self):
        """UPDATE params must be (slug, chapter_id)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_chapter_resolved(42, "edurne-uriarte-bengoechea")

        params = cursor.execute.call_args[0][1]
        assert params == ("edurne-uriarte-bengoechea", 42)

    def test_returns_none(self):
        """mark_chapter_resolved must return None (void operation)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.mark_chapter_resolved(42, "edurne-uriarte-bengoechea")

        assert result is None


# ---------------------------------------------------------------------------
# select_unprepared_turns — resolution columns
# ---------------------------------------------------------------------------


class TestSelectUnpreparedTurnsResolutionColumns:
    """select_unprepared_turns must expose resolution columns for idempotency check."""

    def test_sql_selects_resolved_participant_slug(self):
        """Query must SELECT stv.resolved_participant_slug."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG" in query

    def test_sql_selects_speaker_resolution_confidence(self):
        """Query must SELECT stv.speaker_resolution_confidence."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_CONFIDENCE" in query

    def test_sql_selects_vc_speakers(self):
        """Query must SELECT vc.speakers alongside vc.key_speakers (issue #321
        Gate B needs both rosters to build chapter_roster_mentions)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "VC.SPEAKERS" in query, f"must SELECT vc.speakers; got: {query}"
