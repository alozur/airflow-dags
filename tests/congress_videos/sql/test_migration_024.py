"""[RED] Test: migration 024 — add approval columns to speaker_turn_trim_proposals.

Reads the SQL file statically and asserts structural properties:
file exists, ALTER TABLE adds is_approved and approved_at idempotently
(ADD COLUMN IF NOT EXISTS), DOWN block drops both columns, no DROP TABLE.
No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "024_add_approval_to_trim_proposals.sql"
)


class TestMigration024FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration024UpBlock:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_alters_correct_table(self):
        """Migration must ALTER TABLE speaker_turn_trim_proposals."""
        sql = self._sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_TRIM_PROPOSALS" in sql

    def test_adds_is_approved_idempotent(self):
        """is_approved column must be added with ADD COLUMN IF NOT EXISTS."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS IS_APPROVED" in sql

    def test_is_approved_type_boolean_not_null_default_false(self):
        """is_approved must be BOOLEAN NOT NULL DEFAULT FALSE."""
        sql = self._sql().upper()
        assert re.search(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+IS_APPROVED\s+BOOLEAN\s+NOT\s+NULL\s+DEFAULT\s+FALSE",
            sql,
        ), "is_approved must be BOOLEAN NOT NULL DEFAULT FALSE"

    def test_adds_approved_at_idempotent(self):
        """approved_at column must be added with ADD COLUMN IF NOT EXISTS."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS APPROVED_AT" in sql

    def test_approved_at_type_timestamptz(self):
        """approved_at must be TIMESTAMPTZ (nullable — no NOT NULL)."""
        sql = self._sql().upper()
        assert re.search(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+APPROVED_AT\s+TIMESTAMPTZ",
            sql,
        ), "approved_at must be TIMESTAMPTZ"

    def test_approved_at_is_nullable(self):
        """approved_at must NOT have NOT NULL constraint (nullable column)."""
        sql = self._sql().upper()
        # Find the approved_at line and ensure NOT NULL is absent on that line
        for line in sql.splitlines():
            if "APPROVED_AT" in line and "ADD COLUMN" in line:
                assert "NOT NULL" not in line, "approved_at must be nullable (no NOT NULL)"
                break

    def test_no_schema_qualification(self):
        """Migration must use unqualified table names (runner sets search_path)."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.speaker_turn_trim_proposals\b", sql)


class TestMigration024Idempotency:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_up_uses_add_column_if_not_exists_for_is_approved(self):
        """UP block must guard is_approved with IF NOT EXISTS to be re-runnable."""
        sql = self._sql().upper()
        count = sql.count("ADD COLUMN IF NOT EXISTS IS_APPROVED")
        assert count >= 1, "is_approved must use ADD COLUMN IF NOT EXISTS"

    def test_up_uses_add_column_if_not_exists_for_approved_at(self):
        """UP block must guard approved_at with IF NOT EXISTS to be re-runnable."""
        sql = self._sql().upper()
        count = sql.count("ADD COLUMN IF NOT EXISTS APPROVED_AT")
        assert count >= 1, "approved_at must use ADD COLUMN IF NOT EXISTS"


class TestMigration024DownBlock:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_down_block_present(self):
        """Migration must include a down-migration block."""
        sql = self._sql().upper()
        assert "DROP COLUMN" in sql or "-- DOWN" in sql or "DOWN MIGRATION" in sql

    def test_down_drops_is_approved(self):
        """DOWN block must drop the is_approved column."""
        sql = self._sql().upper()
        assert "IS_APPROVED" in sql
        # Both ADD (UP) and DROP (DOWN) must reference IS_APPROVED
        assert sql.count("IS_APPROVED") >= 2, "is_approved must appear in both UP (ADD) and DOWN (DROP)"

    def test_down_drops_approved_at(self):
        """DOWN block must drop the approved_at column."""
        sql = self._sql().upper()
        assert "APPROVED_AT" in sql
        assert sql.count("APPROVED_AT") >= 2, "approved_at must appear in both UP (ADD) and DOWN (DROP)"

    def test_down_does_not_drop_table(self):
        """DOWN block must NOT drop the entire table — only the two new columns."""
        sql = self._sql().upper()
        assert "DROP TABLE" not in sql, "DOWN migration must only drop columns, not the whole table"
