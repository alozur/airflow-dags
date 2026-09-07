"""Tests for CongressionalVideoDB — Reap pipeline methods (video_shorts)."""

from __future__ import annotations

import logging

import pytest

from congress_videos.modules.database import (
    SHORTS_PENDING_CANDIDATE_LIMIT,
    SHORTS_TIER1_PER_CHAPTER_LIMIT,
    SHORTS_UPLOAD_HISTORY_LIMIT,
    filter_shorts_by_source_cooldown,
)

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def db(mocker):
    from unittest.mock import MagicMock

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)

    from congress_videos.modules.database import CongressionalVideoDB

    instance = CongressionalVideoDB()
    return instance, mock_cursor


# --------------------------------------------------------------------------- #
# get_turn_videos_for_shorts (issue #467 — replaces get_chapters_for_shorts)
# --------------------------------------------------------------------------- #


class TestGetTurnVideosForShorts:
    def test_returns_list_of_turns(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"turn_id": 1, "relevance_score": 4},
            {"turn_id": 2, "relevance_score": 5},
        ]

        result = instance.get_turn_videos_for_shorts()

        assert len(result) == 2

    def test_empty_result_returns_empty_list(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_turn_videos_for_shorts()

        assert result == []

    def test_no_limit_when_max_turns_none(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql, params = mock_cursor.execute.call_args[0]
        assert "LIMIT" not in sql, "no LIMIT clause when max_turns is None"
        assert params == []

    def test_limit_appended_when_max_turns_given(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts(max_turns=5)

        sql, params = mock_cursor.execute.call_args[0]
        assert "LIMIT %s" in sql
        assert params == [5]

    def test_query_contains_group_spans_cte(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "WITH group_spans AS" in sql
        assert "GROUP BY stv.output_path" in sql

    def test_group_spans_cte_is_unfiltered_by_procedural(self, db):
        """issue #151 trap: is_procedural must only be summed inside
        group_spans, never used to filter which rows enter the aggregate."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        cte_start = sql.index("WITH group_spans AS")
        cte_end = sql.index("GROUP BY stv.output_path") + len("GROUP BY stv.output_path")
        cte_body = sql[cte_start:cte_end]
        assert "WHERE" not in cte_body, f"group_spans CTE must have no WHERE gate; got: {cte_body}"
        assert "SUM(CASE WHEN st.is_procedural" in cte_body

    def test_query_uses_distinct_on_output_path(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "DISTINCT ON (stv.output_path)" in sql

    def test_query_dedups_on_turn_id_not_chapter_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "vs.turn_id = stv.turn_id" in sql
        assert "vs.chapter_id" not in sql, "dedup must key on turn_id, not chapter_id"

    def test_query_excludes_procedural_representative(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "NOT COALESCE(st.is_procedural, FALSE)" in sql

    def test_query_floor_is_120_seconds(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "dedup.group_duration_seconds >= 120" in sql

    def test_query_has_no_upper_duration_bound(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "<= 900" not in sql
        assert "<=" not in sql, "no upper ceiling gate on group_duration_seconds"

    def test_query_does_not_filter_prepared_at(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "prepared_at" not in sql

    def test_query_does_not_require_parent_upload_date(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "youtube_upload_date" not in sql
        assert "is_uploaded_to_youtube" not in sql

    def test_query_orders_by_editorial_keys(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_turn_videos_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        order_clause = sql[sql.rindex("ORDER BY") :]
        assert "COALESCE(dedup.interest_score, 1) DESC" in order_clause
        assert "dedup.relevance_score DESC" in order_clause
        assert "dedup.session_date DESC" in order_clause
        assert "dedup.turn_id ASC" in order_clause


# --------------------------------------------------------------------------- #
# insert_video_short
# --------------------------------------------------------------------------- #


class TestInsertVideoShort:
    def test_returns_inserted_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 42}

        result = instance.insert_video_short(
            chapter_id=10,
            reap_project_id="proj-001",
            reap_status="processing",
        )

        assert result == 42

    def test_passes_all_params_to_query(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 7}

        instance.insert_video_short(
            chapter_id=3,
            reap_project_id="proj-xyz",
            reap_status="processing",
            pretrim_start_secs=60.0,
            pretrim_end_secs=420.0,
            pretrim_used_srt=True,
        )

        _, params = mock_cursor.execute.call_args[0]
        assert 3 in params
        assert "proj-xyz" in params
        assert 60.0 in params
        assert 420.0 in params
        assert True in params

    def test_query_contains_insert_returning(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1, reap_project_id="p-1")

        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT" in sql
        assert "RETURNING" in sql

    def test_query_has_nine_placeholders(self, db):
        """issue #467: turn_id extends the column list to 9 placeholders."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1)

        sql, params = mock_cursor.execute.call_args[0]
        assert sql.count("%s") == 9
        assert len(params) == 9

    def test_column_list_places_turn_id_after_chapter_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "(chapter_id, turn_id, reap_project_id" in sql

    def test_turn_id_defaults_to_none(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1)

        _, params = mock_cursor.execute.call_args[0]
        assert params[0] == 1
        assert params[1] is None

    def test_passes_turn_id_when_given(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1, turn_id=99)

        _, params = mock_cursor.execute.call_args[0]
        assert params[1] == 99

    def test_existing_positional_call_shape_unaffected(self, db):
        """A caller that never passes turn_id keeps working (backward compat)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 42}

        result = instance.insert_video_short(
            chapter_id=10,
            reap_project_id="proj-001",
            reap_status="processing",
        )

        assert result == 42


# --------------------------------------------------------------------------- #
# insert_video_short_clip
# --------------------------------------------------------------------------- #


class TestInsertVideoShortClip:
    def test_returns_inserted_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 99}

        result = instance.insert_video_short_clip(
            chapter_id=5,
            reap_project_id="proj-abc",
            reap_clip_id="clip-001",
            reap_virality_score=0.85,
            reap_clip_url="https://cdn.reap.video/c.mp4",
            local_file_path="/data/clip.mp4",
        )

        assert result == 99

    def test_passes_clip_id_and_virality_to_query(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short_clip(
            chapter_id=1,
            reap_project_id="p",
            reap_clip_id="clip-xyz",
            reap_virality_score=0.75,
            reap_clip_url="https://cdn.example.com/clip.mp4",
            local_file_path="/data/clip.mp4",
        )

        _, params = mock_cursor.execute.call_args[0]
        assert "clip-xyz" in params
        assert 0.75 in params

    def test_column_list_places_turn_id_after_chapter_id(self, db):
        """issue #467: turn_id extends the column list right after chapter_id."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short_clip(
            chapter_id=1,
            reap_project_id="p",
            reap_clip_id="clip-xyz",
            reap_virality_score=0.75,
            reap_clip_url="https://cdn.example.com/clip.mp4",
            local_file_path="/data/clip.mp4",
        )

        sql = mock_cursor.execute.call_args[0][0]
        assert "(chapter_id, turn_id, reap_project_id" in sql

    def test_turn_id_defaults_to_none(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short_clip(
            chapter_id=1,
            reap_project_id="p",
            reap_clip_id="clip-xyz",
            reap_virality_score=0.75,
            reap_clip_url="https://cdn.example.com/clip.mp4",
            local_file_path="/data/clip.mp4",
        )

        _, params = mock_cursor.execute.call_args[0]
        assert params[0] == 1
        assert params[1] is None

    def test_passes_turn_id_when_given(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short_clip(
            chapter_id=1,
            reap_project_id="p",
            reap_clip_id="clip-xyz",
            reap_virality_score=0.75,
            reap_clip_url="https://cdn.example.com/clip.mp4",
            local_file_path="/data/clip.mp4",
            turn_id=99,
        )

        _, params = mock_cursor.execute.call_args[0]
        assert params[1] == 99

    def test_existing_positional_call_shape_unaffected(self, db):
        """A caller that never passes turn_id keeps working (backward compat)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 99}

        result = instance.insert_video_short_clip(
            chapter_id=5,
            reap_project_id="proj-abc",
            reap_clip_id="clip-001",
            reap_virality_score=0.85,
            reap_clip_url="https://cdn.reap.video/c.mp4",
            local_file_path="/data/clip.mp4",
        )

        assert result == 99


# --------------------------------------------------------------------------- #
# claim_pending_clip
# --------------------------------------------------------------------------- #


class TestClaimPendingClip:
    """design.md §4: RETURNING * cannot project joined columns, so the atomic
    claim UPDATE is wrapped in a ``claimed`` CTE, then LEFT JOINed through
    speaker_turn_videos/speaker_turns to surface the turn's group span
    alongside the claimed row. Ordering, FOR UPDATE SKIP LOCKED, and the two
    priority subqueries are carried forward verbatim (issue #467)."""

    def test_returns_none_when_no_pending_rows(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.claim_pending_clip()

        assert result is None

    def test_returns_claimed_row_as_dict(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "id": 7,
            "chapter_id": 3,
            "turn_id": None,
            "group_start_seconds": None,
            "group_end_seconds": None,
        }

        result = instance.claim_pending_clip()

        assert result["id"] == 7
        assert result["chapter_id"] == 3

    def test_query_wraps_update_in_claimed_cte(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.claim_pending_clip()

        sql = mock_cursor.execute.call_args[0][0]
        assert "WITH claimed AS" in sql
        assert "UPDATE" in sql
        assert "RETURNING *" in sql

    def test_query_left_joins_speaker_turn_videos_on_turn_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.claim_pending_clip()

        sql = mock_cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql
        assert "stv.turn_id = c.turn_id" in sql

    def test_query_uses_lateral_join_for_group_span(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.claim_pending_clip()

        sql = mock_cursor.execute.call_args[0][0]
        assert "LEFT JOIN LATERAL" in sql
        assert "group_start_seconds" in sql
        assert "group_end_seconds" in sql
        assert "sib.output_path = stv.output_path" in sql

    def test_ordering_and_locking_preserved_verbatim(self, db):
        """Priority order and SKIP LOCKED must survive the CTE wrapping unchanged."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.claim_pending_clip()

        sql = mock_cursor.execute.call_args[0][0]
        assert "reap_status = 'pending'" in sql
        assert "FOR UPDATE SKIP LOCKED" in sql
        assert "DESC NULLS LAST" in sql
        assert sql.count("DESC NULLS LAST") == 2

    def test_no_params_used(self, db):
        """The query has no %s placeholders — matches today's parameterless shape."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.claim_pending_clip()

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        assert "%s" not in sql


# --------------------------------------------------------------------------- #
# update_video_short_status
# --------------------------------------------------------------------------- #


class TestUpdateVideoShortStatus:
    def test_executes_update_with_correct_params(self, db):
        instance, mock_cursor = db

        instance.update_video_short_status("proj-001", "failed")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "reap_status" in sql
        assert params == ("failed", "proj-001")

    def test_parameterized_no_injection(self, db):
        instance, mock_cursor = db

        instance.update_video_short_status("proj-safe", "expired")

        sql, params = mock_cursor.execute.call_args[0]
        assert "proj-safe" not in sql
        assert "proj-safe" in params


# --------------------------------------------------------------------------- #
# filter_shorts_by_source_cooldown (pure helper)
# --------------------------------------------------------------------------- #


class TestFilterShortsBySourceCooldown:
    def test_blocked_before_cooldown_elapses(self):
        """Only 4 other-video uploads since V's last upload — still blocked."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "V"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == []

    def test_released_after_exactly_five_other_uploads(self):
        """Exactly 5 other-video uploads since V's last upload — eligible."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "other5"},
            {"video_id": "V"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_boundary_four_vs_five(self):
        """Index 4 (blocked) vs index 5 (eligible) for two different source videos."""
        candidates = [
            {"id": 1, "video_id": "A"},  # index 4 in history
            {"id": 2, "video_id": "B"},  # index 5 in history
        ]
        upload_history = [
            {"video_id": "x1"},
            {"video_id": "x2"},
            {"video_id": "x3"},
            {"video_id": "x4"},
            {"video_id": "A"},
            {"video_id": "B"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == [{"id": 2, "video_id": "B"}]

    def test_video_with_no_upload_history_is_eligible(self):
        """V never appears in upload_history — eligible regardless of other videos' history."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_history_window_outside_bounded_history_is_eligible(self):
        """V absent from the (bounded) history list passed in — never a stale lockout."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [{"video_id": f"other{i}"} for i in range(SHORTS_UPLOAD_HISTORY_LIMIT)]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_multi_occurrence_uses_first_match_index(self):
        """Repeated V entries in history: only the MOST RECENT (first) occurrence counts."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "V"},  # most recent V occurrence — index 1
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "other5"},
            {"video_id": "V"},  # older occurrence — must be ignored
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == []

    def test_missing_video_id_fails_open(self):
        """Candidate row without a video_id key is eligible regardless of history."""
        candidates = [{"id": 1}]
        upload_history = [{"video_id": "irrelevant"}]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_order_preserved_among_eligible(self):
        """Filtering never reorders — only removes cooling-down rows."""
        candidates = [
            {"id": 1, "video_id": "A"},
            {"id": 2, "video_id": "B"},
            {"id": 3, "video_id": "C"},
        ]
        upload_history: list[dict] = []

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_empty_upload_history_all_eligible(self):
        candidates = [{"id": 1, "video_id": "V"}, {"id": 2, "video_id": "W"}]

        result = filter_shorts_by_source_cooldown(candidates, [])

        assert result == candidates

    def test_cooldown_zero_all_eligible(self):
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [{"video_id": "V"}]

        result = filter_shorts_by_source_cooldown(candidates, upload_history, cooldown=0)

        assert result == candidates


# --------------------------------------------------------------------------- #
# get_pending_shorts
# --------------------------------------------------------------------------- #


class TestGetPendingShorts:
    def test_returns_list_of_shorts(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"id": 1, "reap_clip_id": "c-001", "reap_virality_score": 0.8},
        ]

        result = instance.get_pending_shorts()

        assert len(result) == 1

    def test_empty_result_returns_empty_list(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_pending_shorts()

        assert result == []

    def test_limit_is_applied(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_pending_shorts(limit=3)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" in sql

    def test_virality_filter_passed_as_param(self, db):
        """min_virality_score and SHORTS_PENDING_CANDIDATE_LIMIT are bound to the candidate
        query — the candidate LIMIT is decoupled from `limit`, which is applied afterward in
        Python and asserted here via the truncated return value."""
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [
            [],
            [{"id": 1, "video_id": "A"}, {"id": 2, "video_id": "B"}],
        ]

        result = instance.get_pending_shorts(limit=1, min_virality_score=0.6)

        _, candidate_params = mock_cursor.execute.call_args_list[1][0]
        assert 0.6 in candidate_params
        assert SHORTS_PENDING_CANDIDATE_LIMIT in candidate_params
        assert result == [{"id": 1, "video_id": "A"}]

    def test_query_excludes_abandoned_shorts(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_pending_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "is_upload_abandoned = FALSE" in sql

    def test_history_query_runs_first_with_expected_shape(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        history_sql, history_params = mock_cursor.execute.call_args_list[0][0]
        assert "is_uploaded = TRUE" in history_sql
        assert "ORDER BY vs.updated_at DESC" in history_sql
        assert SHORTS_UPLOAD_HISTORY_LIMIT in history_params

    def test_candidate_query_joins_video_chapters_and_selects_video_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts(min_virality_score=0.6)

        candidate_sql, candidate_params = mock_cursor.execute.call_args_list[1][0]
        assert "JOIN" in candidate_sql
        assert "video_chapters" in candidate_sql
        assert "vc.video_id" in candidate_sql
        assert 0.6 in candidate_params
        assert SHORTS_PENDING_CANDIDATE_LIMIT in candidate_params

    def test_result_truncated_to_limit(self, db):
        instance, mock_cursor = db
        candidates = [
            {"id": 1, "video_id": "A"},
            {"id": 2, "video_id": "B"},
            {"id": 3, "video_id": "C"},
        ]
        mock_cursor.fetchall.side_effect = [[], candidates]

        result = instance.get_pending_shorts(limit=2)

        assert result == candidates[:2]

    def test_eligible_row_deeper_than_limit_is_returned_past_cooling_down_head(self, db):
        """Cooling-down head row must not zero out the run — over-fetch + Python filter
        surfaces the eligible row that sits deeper in the candidate list."""
        instance, mock_cursor = db
        history = [
            {"video_id": "hot"},
            {"video_id": "other"},
            {"video_id": "hot"},
        ]
        candidates = [
            {"id": 1, "video_id": "hot"},  # cooling down (index 0 < cooldown 5)
            {"id": 2, "video_id": "cold"},  # never uploaded -> eligible
        ]
        mock_cursor.fetchall.side_effect = [history, candidates]

        result = instance.get_pending_shorts(limit=1)

        assert result == [{"id": 2, "video_id": "cold"}]

    def test_all_cooling_down_returns_empty_and_logs_info(self, db, caplog):
        instance, mock_cursor = db
        history = [{"video_id": "V"}]
        candidates = [{"id": 1, "video_id": "V"}]
        mock_cursor.fetchall.side_effect = [history, candidates]

        with caplog.at_level(logging.INFO, logger="congress_videos.modules.database"):
            result = instance.get_pending_shorts()

        assert result == []
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("cooling down" in m.lower() for m in info_messages)

    def test_partial_block_logs_blocked_count(self, db, caplog):
        instance, mock_cursor = db
        history = [{"video_id": "hot"}]
        candidates = [
            {"id": 1, "video_id": "hot"},  # blocked
            {"id": 2, "video_id": "cold"},  # eligible
        ]
        mock_cursor.fetchall.side_effect = [history, candidates]

        with caplog.at_level(logging.INFO, logger="congress_videos.modules.database"):
            result = instance.get_pending_shorts()

        assert result == [{"id": 2, "video_id": "cold"}]
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("blocked 1" in m.lower() for m in info_messages)

    def test_candidate_query_ranks_clips_per_chapter(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        candidate_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "ROW_NUMBER() OVER" in candidate_sql
        assert "PARTITION BY vs.chapter_id" in candidate_sql
        assert "AS chapter_rank" in candidate_sql

    def test_tier1_limit_is_first_candidate_param(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts(min_virality_score=0.6)

        candidate_params = mock_cursor.execute.call_args_list[1][0][1]
        assert candidate_params == (
            SHORTS_TIER1_PER_CHAPTER_LIMIT,
            0.6,
            SHORTS_PENDING_CANDIDATE_LIMIT,
        )

    def test_rank_universe_includes_uploaded_clips(self, db):
        """The ranking CTE must not filter on is_uploaded, local_file_path, or
        the virality threshold — those apply only in the outer query, AFTER
        tiers are computed (R4/R8). Ranking pending-only would make the
        per-chapter cap inert: ranks recompute after each upload and the
        chapter perpetually re-presents 3 fresh Tier-1 clips."""
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        candidate_sql = mock_cursor.execute.call_args_list[1][0][0]
        cte_sql = candidate_sql.split("FROM ranked", 1)[0]
        assert "is_uploaded" not in cte_sql
        assert "local_file_path" not in cte_sql
        assert "reap_virality_score >=" not in cte_sql

    def test_tier_is_primary_sort_key(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        candidate_sql = mock_cursor.execute.call_args_list[1][0][0]
        order_by_clause = candidate_sql[candidate_sql.rfind("ORDER BY") :]
        assert order_by_clause.index("tier") < order_by_clause.index("youtube_upload_date")

    def test_outer_where_predicates_unchanged(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        candidate_sql = mock_cursor.execute.call_args_list[1][0][0]
        outer_sql = candidate_sql.split("FROM ranked", 1)[1]
        predicates = [
            "is_uploaded = FALSE",
            "is_upload_abandoned = FALSE",
            "local_file_path IS NOT NULL",
            "reap_status = 'downloaded'",
            "reap_virality_score >= %s OR",
            "youtube_upload_date IS NOT NULL",
        ]
        for predicate in predicates:
            assert predicate in outer_sql

    def test_tier2_row_returned_when_no_tier1_available(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [
            [],
            [{"id": 1, "video_id": "A", "tier": 2}],
        ]

        result = instance.get_pending_shorts(limit=1)

        assert result == [{"id": 1, "video_id": "A", "tier": 2}]

    def test_python_preserves_database_tier_order(self, db):
        instance, mock_cursor = db
        candidates = [
            {"id": 1, "video_id": "A", "tier": 1},
            {"id": 2, "video_id": "B", "tier": 2},
        ]
        mock_cursor.fetchall.side_effect = [[], candidates]

        result = instance.get_pending_shorts(limit=1)

        assert result == [candidates[0]]

    def test_strict_skip_preserved_with_tiers(self, db, caplog):
        instance, mock_cursor = db
        history = [{"video_id": "V"}]
        candidates = [{"id": 1, "video_id": "V", "tier": 1}]
        mock_cursor.fetchall.side_effect = [history, candidates]

        with caplog.at_level(logging.INFO, logger="congress_videos.modules.database"):
            result = instance.get_pending_shorts()

        assert result == []
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("cooling down" in m.lower() for m in info_messages)


# --------------------------------------------------------------------------- #
# mark_short_uploaded
# --------------------------------------------------------------------------- #


class TestMarkShortUploaded:
    def test_executes_update_with_correct_params(self, db):
        instance, mock_cursor = db

        instance.mark_short_uploaded("clip-001", "yt-video-abc")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "is_uploaded" in sql
        assert params == ("yt-video-abc", "clip-001")

    def test_parameterized_query(self, db):
        instance, mock_cursor = db

        instance.mark_short_uploaded("clip-safe", "yt-safe")

        sql, params = mock_cursor.execute.call_args[0]
        assert "clip-safe" not in sql
        assert "clip-safe" in params


# --------------------------------------------------------------------------- #
# record_short_upload_failure
# --------------------------------------------------------------------------- #


class TestRecordShortUploadFailure:
    def test_normal_increment_updates_attempts_and_error(self, db):
        """Non-threshold-crossing failure increments upload_attempts, stores error."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-001", error_message="quota exceeded")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "upload_attempts = upload_attempts + 1" in sql
        assert "is_upload_abandoned" in sql
        assert "last_upload_error" in sql
        assert params == ("quota exceeded", "clip-001")

    def test_threshold_crossing_sets_abandoned_condition(self, db):
        """SQL encodes the >= 3 (SHORTS_UPLOAD_ABANDON_THRESHOLD) abandon condition."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-002", error_message="timeout")

        sql, _ = mock_cursor.execute.call_args[0]
        assert ">= 3" in sql

    def test_error_message_none_path(self, db):
        """error_message=None is passed through as a None param, not a crash."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-003", error_message=None)

        sql, params = mock_cursor.execute.call_args[0]
        assert params == (None, "clip-003")

    def test_warning_logged_when_threshold_crossed_on_this_call(self, db, caplog):
        """A distinct WARNING fires when this call is the one crossing the abandon threshold."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 3,
            "is_upload_abandoned": True,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_short_upload_failure(reap_clip_id="clip-004", error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "clip-004" in warnings[0].message
        assert "abandoned" in warnings[0].message.lower()

    def test_no_warning_logged_for_ordinary_retry_increment(self, db, caplog):
        """An ordinary (non-crossing) failure increment does not emit the abandonment WARNING."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 1,
            "is_upload_abandoned": False,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_short_upload_failure(reap_clip_id="clip-005", error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 0


# --------------------------------------------------------------------------- #
# get_chapter_titles
# --------------------------------------------------------------------------- #


class TestGetChapterTitles:
    def test_returns_dict_of_id_to_title(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"chapter_id": 1, "title": "Title A"},
            {"chapter_id": 2, "title": "Title B"},
        ]

        result = instance.get_chapter_titles([1, 2])

        assert result == {1: "Title A", 2: "Title B"}

    def test_empty_input_returns_empty_dict_without_db_call(self, db):
        instance, mock_cursor = db

        result = instance.get_chapter_titles([])

        assert result == {}
        mock_cursor.execute.assert_not_called()

    def test_passes_chapter_ids_as_array_param(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_chapter_titles([10, 20, 30])

        _, params = mock_cursor.execute.call_args[0]
        assert [10, 20, 30] in params


# --------------------------------------------------------------------------- #
# get_chapter_metadata
# --------------------------------------------------------------------------- #


class TestGetChapterMetadata:
    def test_get_chapter_metadata_returns_source_fields(self, db):
        """AC#1 — chapter with linked source video returns both source fields and youtube_video_id."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 1,
            "title": "Debate vivienda",
            "description": "Descripción del capítulo",
            "speakers": ["Pedro Sánchez"],
            "key_speakers": ["Pedro Sánchez"],
            "topics": ["vivienda"],
            "scoring_reasoning": "Relevant",
            "relevance_score": 4,
            "youtube_video_id": "yt-own-abc",
            "source_video_title": "Sesión plenaria 2024-01-15",
            "source_video_url": "https://youtube.com/watch?v=abc123",
        }

        result = instance.get_chapter_metadata(1)

        assert result is not None
        assert result["youtube_video_id"] == "yt-own-abc"
        assert result["source_video_title"] == "Sesión plenaria 2024-01-15"
        assert result["source_video_url"] == "https://youtube.com/watch?v=abc123"

    def test_get_chapter_metadata_null_source(self, db):
        """AC#2, AC#7 — chapter with no source video row returns None for source fields and youtube_video_id."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 2,
            "title": "Sin fuente",
            "description": "Sin descripción",
            "speakers": [],
            "key_speakers": [],
            "topics": [],
            "scoring_reasoning": "",
            "relevance_score": 3,
            "youtube_video_id": None,
            "source_video_title": None,
            "source_video_url": None,
        }

        result = instance.get_chapter_metadata(2)

        assert result is not None
        assert result["youtube_video_id"] is None
        assert result["source_video_title"] is None
        assert result["source_video_url"] is None

    def test_get_chapter_metadata_missing_chapter(self, db):
        """AC#3 — chapter_id not found returns None (unchanged behaviour)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.get_chapter_metadata(999)

        assert result is None

    def test_get_chapter_metadata_query_uses_left_join(self, db):
        """Query must include LEFT JOIN to youtube_source_videos."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql
        assert "youtube_source_videos" in sql

    def test_get_chapter_metadata_selects_source_columns(self, db):
        """Query must select youtube_video_id, source_video_title and source_video_url aliases."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "youtube_video_id" in sql
        assert "source_video_title" in sql
        assert "source_video_url" in sql


# --------------------------------------------------------------------------- #
# get_source_video_id_for_chapter
# --------------------------------------------------------------------------- #


class TestGetSourceVideoIdForChapter:
    @pytest.mark.parametrize(
        "row,expected",
        [
            ({"video_id": "src_vid_001"}, "src_vid_001"),
            (None, None),
        ],
    )
    def test_returns_video_id_or_none(self, db, row, expected):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = row

        result = instance.get_source_video_id_for_chapter(7)

        assert result == expected

    def test_null_video_id_in_row_returns_none(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"video_id": None}

        result = instance.get_source_video_id_for_chapter(99)

        assert result is None

    def test_query_selects_from_video_chapters(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_source_video_id_for_chapter(5)

        sql = mock_cursor.execute.call_args[0][0]
        assert "video_chapters" in sql
        assert "video_id" in sql

    def test_passes_chapter_id_as_param(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_source_video_id_for_chapter(42)

        _, params = mock_cursor.execute.call_args[0]
        assert 42 in params
