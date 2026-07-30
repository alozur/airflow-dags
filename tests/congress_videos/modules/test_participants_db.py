"""Tests for CongressParticipantsDB — Slice 1 (T-01 through T-09)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Shared fixture: set_pg_env is provided by conftest.py (autouse) but for
# clarity we import the DB class inside each test to avoid module-level
# ImportError before the production file exists.
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
def db_fixture(mock_psycopg2_connection):
    """Return (CongressParticipantsDB, mock_conn, mock_cursor) with DB fully mocked."""
    mock_connect, mock_conn, mock_cursor = mock_psycopg2_connection
    from congress_videos.modules.participants_db import CongressParticipantsDB
    instance = CongressParticipantsDB()
    return instance, mock_conn, mock_cursor


def _make_record(**overrides):
    """Build a minimal ParticipantRecord for testing."""
    base = {
        "normalized_name": "garcia ana",
        "display_name": "García, Ana",
        "party": "PartyA",
        "parliamentary_group": "GroupA",
        "constituency": "Madrid",
        "biography": "Bio text.",
        "full_membership_date": "2020-01-15",
        "start_date": "2020-01-15",
        "group_entry_date": "2020-02-01",
        "photo_url": None,
    }
    base.update(overrides)
    return base


# ===========================================================================
# T-01 RED: upsert_batch
# ===========================================================================


class TestUpsertBatch:

    def test_fresh_insert_executes_for_each_record(self, db_fixture):
        """Fresh insert: each record causes exactly one execute call."""
        instance, mock_conn, mock_cursor = db_fixture

        records = [_make_record(normalized_name="garcia ana"), _make_record(normalized_name="smith john")]
        result = instance.upsert_batch(records)

        assert mock_cursor.execute.call_count == 2

    def test_fresh_insert_returns_upserted_and_total(self, db_fixture):
        """Return dict has keys 'upserted' and 'total'."""
        instance, mock_conn, mock_cursor = db_fixture

        records = [_make_record()]
        result = instance.upsert_batch(records)

        assert "upserted" in result
        assert "total" in result
        assert result["total"] == 1

    def test_sql_contains_on_conflict_do_update(self, db_fixture):
        """SQL uses ON CONFLICT (normalized_name) DO UPDATE."""
        instance, mock_conn, mock_cursor = db_fixture

        instance.upsert_batch([_make_record()])

        sql = mock_cursor.execute.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql

    def test_sql_preserves_photo_url_via_coalesce(self, db_fixture):
        """SQL uses COALESCE to preserve existing photo_url on conflict."""
        instance, mock_conn, mock_cursor = db_fixture

        instance.upsert_batch([_make_record()])

        sql = mock_cursor.execute.call_args[0][0]
        assert "COALESCE" in sql
        assert "photo_url" in sql

    def test_empty_input_no_execute_calls(self, db_fixture):
        """Empty list → no execute calls, no error."""
        instance, mock_conn, mock_cursor = db_fixture

        result = instance.upsert_batch([])

        mock_cursor.execute.assert_not_called()
        assert result["upserted"] == 0
        assert result["total"] == 0

    def test_sql_contains_updated_at_now_on_conflict(self, db_fixture):
        """On conflict branch sets updated_at = NOW()."""
        instance, mock_conn, mock_cursor = db_fixture

        instance.upsert_batch([_make_record()])

        sql = mock_cursor.execute.call_args[0][0]
        assert "updated_at" in sql
        assert "NOW()" in sql

    def test_all_ten_columns_present_in_insert(self, db_fixture):
        """INSERT column list matches the design spec (10 columns)."""
        instance, mock_conn, mock_cursor = db_fixture

        instance.upsert_batch([_make_record()])

        sql = mock_cursor.execute.call_args[0][0]
        for col in (
            "normalized_name", "display_name", "party", "parliamentary_group",
            "constituency", "biography", "full_membership_date", "start_date",
            "group_entry_date", "photo_url",
        ):
            assert col in sql, f"Column {col!r} missing from INSERT SQL"

    def test_params_forwarded_correctly(self, db_fixture):
        """Execute called with a tuple containing the record's field values."""
        instance, mock_conn, mock_cursor = db_fixture
        record = _make_record(
            normalized_name="smith john",
            display_name="Smith, John",
            party="PartyB",
        )

        instance.upsert_batch([record])

        _sql, params = mock_cursor.execute.call_args[0]
        assert "smith john" in params
        assert "Smith, John" in params
        assert "PartyB" in params

    def test_sql_preserves_group_entry_date_via_coalesce_excluded_first(self, db_fixture):
        """SQL uses EXCLUDED-first COALESCE for group_entry_date.

        Incoming None must not wipe out a previously stored group_entry_date.
        EXCLUDED-first order means a fresh real value wins over stored; None
        incoming falls through to the stored value.
        """
        instance, _conn, mock_cursor = db_fixture

        instance.upsert_batch([_make_record()])

        sql = mock_cursor.execute.call_args[0][0]
        assert "COALESCE(EXCLUDED.group_entry_date, congress_participants.group_entry_date)" in sql


# ===========================================================================
# T-03 RED: lookup functions
# ===========================================================================


class TestLookupParticipant:

    def test_exact_match_returns_dict(self, monkeypatch):
        """Exact match found → returns dict with correct normalized_name."""
        rows = [
            {"normalized_name": "garcia ana", "display_name": "García, Ana", "party": "PartyA",
             "photo_url": "https://example.com/garcia.jpg", "biography": "Bio."},
        ]
        from congress_videos.modules import participants_db as _mod
        monkeypatch.setattr(_mod, "_get_participants_for_lookup", lambda: rows)

        from congress_videos.modules.participants_db import lookup_participant
        result = lookup_participant("garcia ana")

        assert result is not None
        assert result["normalized_name"] == "garcia ana"

    def test_exact_miss_returns_none(self, monkeypatch):
        """No exact match → None (lookup_participant does not try fuzzy)."""
        rows = [{"normalized_name": "garcia ana", "display_name": "García, Ana",
                  "party": "P", "photo_url": None, "biography": "Bio."}]

        from congress_videos.modules import participants_db as _mod
        monkeypatch.setattr(_mod, "_get_participants_for_lookup", lambda: rows)

        from congress_videos.modules.participants_db import lookup_participant
        result = lookup_participant("UNKNOWN PERSON NOBODY")

        assert result is None

    def test_no_match_at_all_returns_none(self, monkeypatch):
        """lookup_participant with completely unrelated name → None."""
        rows = [{"normalized_name": "garcia ana", "display_name": "García, Ana",
                  "party": "P", "photo_url": None, "biography": "Bio."}]

        from congress_videos.modules import participants_db as _mod
        monkeypatch.setattr(_mod, "_get_participants_for_lookup", lambda: rows)

        from congress_videos.modules.participants_db import lookup_participant
        result = lookup_participant("zzzunknownzzz")

        assert result is None


class TestLookupParticipantFuzzy:
    """Tests for lookup_participant_fuzzy.

    rapidfuzz is a Slice-2 runtime dependency. Tests stub token_sort_ratio
    via sys.modules so the import inside lookup_participant_fuzzy succeeds
    without the real library installed.
    """

    @staticmethod
    def _stub_rapidfuzz(monkeypatch, score: float) -> None:
        """Install a stub rapidfuzz.fuzz.token_sort_ratio returning score*100."""
        import types
        import sys

        stub_fuzz = types.SimpleNamespace(token_sort_ratio=lambda a, b: score * 100)
        stub_rapidfuzz = types.ModuleType("rapidfuzz")
        stub_rapidfuzz.fuzz = stub_fuzz
        monkeypatch.setitem(sys.modules, "rapidfuzz", stub_rapidfuzz)
        monkeypatch.setitem(sys.modules, "rapidfuzz.fuzz", stub_fuzz)

    def test_above_threshold_returns_dict(self, monkeypatch):
        """Fuzzy match at or above 0.90 → returns best matching dict."""
        rows = [
            {"normalized_name": "garcia ana", "display_name": "García, Ana",
             "party": "P", "photo_url": None, "biography": "Bio."},
        ]

        self._stub_rapidfuzz(monkeypatch, score=0.95)

        from congress_videos.modules import participants_db as _mod
        monkeypatch.setattr(_mod, "_get_participants_for_lookup", lambda: rows)

        from congress_videos.modules.participants_db import lookup_participant_fuzzy
        result = lookup_participant_fuzzy("garcia ana", threshold=0.90)

        assert result is not None
        assert result["normalized_name"] == "garcia ana"

    def test_below_threshold_returns_none(self, monkeypatch):
        """Fuzzy match below threshold → None."""
        rows = [
            {"normalized_name": "garcia ana", "display_name": "García, Ana",
             "party": "P", "photo_url": None, "biography": "Bio."},
        ]

        self._stub_rapidfuzz(monkeypatch, score=0.50)

        from congress_videos.modules import participants_db as _mod
        monkeypatch.setattr(_mod, "_get_participants_for_lookup", lambda: rows)

        from congress_videos.modules.participants_db import lookup_participant_fuzzy
        result = lookup_participant_fuzzy("zzzunknownzzz", threshold=0.90)

        assert result is None


# ===========================================================================
# T-05 RED: update_photo_url
# ===========================================================================


class TestUpdatePhotoUrl:

    def test_sql_contains_where_photo_url_is_null(self, db_fixture):
        """SQL must guard with WHERE photo_url IS NULL."""
        instance, _conn, mock_cursor = db_fixture

        instance.update_photo_url("garcia ana", "https://example.com/photo.jpg")

        sql = mock_cursor.execute.call_args[0][0]
        assert "photo_url IS NULL" in sql

    def test_sql_contains_set_photo_url(self, db_fixture):
        """SQL must SET photo_url."""
        instance, _conn, mock_cursor = db_fixture

        instance.update_photo_url("garcia ana", "https://example.com/photo.jpg")

        sql = mock_cursor.execute.call_args[0][0]
        assert "SET" in sql
        assert "photo_url" in sql

    def test_sql_contains_updated_at_now(self, db_fixture):
        """SQL must also SET updated_at = NOW()."""
        instance, _conn, mock_cursor = db_fixture

        instance.update_photo_url("garcia ana", "https://example.com/photo.jpg")

        sql = mock_cursor.execute.call_args[0][0]
        assert "updated_at" in sql
        assert "NOW()" in sql

    def test_params_order_photo_url_then_normalized_name(self, db_fixture):
        """Params tuple: (photo_url, normalized_name) per design."""
        instance, _conn, mock_cursor = db_fixture

        instance.update_photo_url("garcia ana", "https://example.com/photo.jpg")

        _sql, params = mock_cursor.execute.call_args[0]
        assert params == ("https://example.com/photo.jpg", "garcia ana")


# ===========================================================================
# T-07 RED: migration file tests
# ===========================================================================


class TestMigration016:

    from pathlib import Path as _Path
    _MIGRATION_PATH = str(
        _Path(__file__).resolve().parents[3]
        / "congress_videos/sql/migrations/016_add_nickname.sql"
    )

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        from pathlib import Path
        assert Path(self._MIGRATION_PATH).exists(), "016_add_nickname.sql not found"

    def test_sql_contains_add_column_if_not_exists_nickname_text(self):
        """SQL contains ADD COLUMN IF NOT EXISTS nickname TEXT."""
        from pathlib import Path
        content = Path(self._MIGRATION_PATH).read_text(encoding="utf-8")
        assert "ADD COLUMN IF NOT EXISTS nickname TEXT" in content

    def test_sql_does_not_contain_not_null(self):
        """Migration must NOT add NOT NULL constraint (nullable column)."""
        from pathlib import Path
        content = Path(self._MIGRATION_PATH).read_text(encoding="utf-8")
        # We should not find NOT NULL after nickname TEXT
        import re
        # Check that nickname column definition has no NOT NULL
        assert "nickname TEXT NOT NULL" not in content

    def test_sql_does_not_contain_default_clause_for_nickname(self):
        """Migration must NOT add DEFAULT value for nickname."""
        from pathlib import Path
        content = Path(self._MIGRATION_PATH).read_text(encoding="utf-8")
        assert "nickname TEXT DEFAULT" not in content

    def test_table_name_is_unqualified(self):
        """ALTER TABLE uses unqualified table name (search_path convention)."""
        from pathlib import Path
        content = Path(self._MIGRATION_PATH).read_text(encoding="utf-8")
        # Should contain 'congress_participants' without a schema prefix
        assert "ALTER TABLE congress_participants" in content
        # Must NOT contain a dotted schema prefix like 'public.congress_participants'
        import re
        assert not re.search(r"ALTER TABLE \w+\.congress_participants", content)
