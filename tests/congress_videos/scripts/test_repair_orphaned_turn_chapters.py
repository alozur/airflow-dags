"""Unit tests for issue #499's guarded parent-chapter repair command."""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "repair_orphaned_turn_chapters.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("repair_orphaned_turn_chapters", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


repair = _load_script()


def _mock_connection(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def test_allowed_chapters_are_the_documented_orphan_set_only():
    assert repair.ALLOWED_CHAPTER_IDS == (263, 264, 265, 266, 519)


def test_candidate_query_requires_proven_turn_caused_mark_and_pending_sibling():
    conn, cursor = _mock_connection([])

    candidates = repair.fetch_candidates(
        conn, "production.video_chapters", "production.speaker_turns", "production.speaker_turn_videos"
    )

    assert candidates == []

    query, params = cursor.execute.call_args[0]
    normalized = " ".join(query.split())
    assert params == ([263, 264, 265, 266, 519],)
    assert "vc.is_uploaded_to_youtube = TRUE" in normalized
    assert "uploaded.youtube_video_id = vc.youtube_video_id" in normalized
    assert "pending.prepared_at IS NOT NULL" in normalized
    assert "pending.is_uploaded_to_youtube = FALSE" in normalized
    assert "COALESCE(pending.is_upload_abandoned, FALSE) = FALSE" in normalized


def test_execute_update_rechecks_every_safety_predicate_and_clears_all_parent_upload_fields():
    conn, cursor = _mock_connection([])
    cursor.fetchall.return_value = [{"chapter_id": 519}]

    updated = repair.apply_repair(
        conn,
        "production.video_chapters",
        "production.speaker_turns",
        "production.speaker_turn_videos",
    )

    assert updated == [519]
    query, params = cursor.execute.call_args[0]
    normalized = " ".join(query.split())
    assert params == ([263, 264, 265, 266, 519],)
    assert "is_uploaded_to_youtube = FALSE" in normalized
    assert "youtube_video_id = NULL" in normalized
    assert "youtube_upload_date = NULL" in normalized
    assert "uploaded.youtube_video_id = vc.youtube_video_id" in normalized
    assert "pending.prepared_at IS NOT NULL" in normalized


def test_dry_run_fetches_candidates_without_executing_update(monkeypatch, capsys):
    pg_conn = MagicMock()
    pg_conn.get_qualified_table.side_effect = lambda table: f"production.{table}"
    conn, cursor = _mock_connection([{"chapter_id": 519, "pending_turn_ids": [320, 321, 322]}])
    pg_conn.get_connection.return_value.__enter__.return_value = conn
    monkeypatch.setattr(repair, "PostgresConnection", lambda: pg_conn)

    assert repair.main([]) == 0

    output = capsys.readouterr().out
    assert "DRY RUN" in output
    assert "519" in output
    assert cursor.execute.call_count == 1


def test_execute_requires_explicit_flag(monkeypatch):
    pg_conn = MagicMock()
    pg_conn.get_qualified_table.side_effect = lambda table: f"production.{table}"
    conn, cursor = _mock_connection([{"chapter_id": 519, "pending_turn_ids": [320, 321, 322]}])
    pg_conn.get_connection.return_value.__enter__.return_value = conn
    monkeypatch.setattr(repair, "PostgresConnection", lambda: pg_conn)

    assert repair.main(["--execute"]) == 0
    assert cursor.execute.call_count == 2
    assert "UPDATE production.video_chapters" in cursor.execute.call_args_list[1][0][0]
