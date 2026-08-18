"""[RED] Test: migration 023 — create speaker_turn_trim_proposals table.

Reads the SQL file statically and asserts structural properties:
file exists, table creation is idempotent (IF NOT EXISTS), FK references
speaker_turns(turn_id) with ON DELETE CASCADE, CHECK tipo constraint rejects
unknown literals, UNIQUE (turn_id, start_seconds, tipo) enforces idempotency,
and both required indexes exist. No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "023_create_trim_proposals.sql"
)


class TestMigration023FileExists:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration023TableDefinition:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_creates_table_if_not_exists(self):
        """Migration must use CREATE TABLE IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "CREATE TABLE IF NOT EXISTS SPEAKER_TURN_TRIM_PROPOSALS" in sql

    def test_fk_references_speaker_turns(self):
        """turn_id column must reference speaker_turns(turn_id)."""
        sql = self._sql()
        assert "REFERENCES speaker_turns(turn_id)" in sql

    def test_fk_has_on_delete_cascade(self):
        """FK must include ON DELETE CASCADE."""
        sql = self._sql().upper()
        assert "ON DELETE CASCADE" in sql

    def test_tipo_check_constraint_silence(self):
        """tipo column must allow 'silence' in CHECK constraint."""
        sql = self._sql()
        assert "'silence'" in sql

    def test_tipo_check_constraint_applause(self):
        """tipo column must allow 'applause' in CHECK constraint."""
        sql = self._sql()
        assert "'applause'" in sql

    def test_tipo_check_uses_in(self):
        """tipo column CHECK must use IN() to restrict values."""
        sql = self._sql()
        assert re.search(r"tipo\s+IN\s*\(", sql) or re.search(
            r"CHECK\s*\(.*tipo.*IN\s*\(", sql, re.DOTALL
        )

    def test_unique_constraint_turn_start_tipo(self):
        """UNIQUE (turn_id, start_seconds, tipo) constraint must be present."""
        sql = self._sql().upper()
        assert re.search(
            r"UNIQUE\s*\(\s*TURN_ID\s*,\s*START_SECONDS\s*,\s*TIPO\s*\)", sql
        )

    def test_is_voice_free_column_with_default(self):
        """is_voice_free column must be BOOLEAN NOT NULL DEFAULT TRUE."""
        sql = self._sql().upper()
        assert "IS_VOICE_FREE" in sql
        assert "BOOLEAN" in sql
        assert "DEFAULT TRUE" in sql

    def test_score_column_is_nullable(self):
        """score column must be NUMERIC (nullable, for silence which has no score)."""
        sql = self._sql().upper()
        # score must be present and NOT have NOT NULL (it is nullable)
        assert "SCORE" in sql
        # Ensure the score line is not 'SCORE NUMERIC NOT NULL'
        lines = sql.splitlines()
        for line in lines:
            if "SCORE" in line and "NUMERIC" in line:
                assert "NOT NULL" not in line, (
                    "score column must be nullable (silence proposals have no score)"
                )
                break


class TestMigration023Indexes:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_index_trim_proposals_turn(self):
        """idx_trim_proposals_turn index must be created."""
        sql = self._sql()
        assert "idx_trim_proposals_turn" in sql

    def test_index_trim_proposals_kind(self):
        """idx_trim_proposals_kind index on tipo must be created."""
        sql = self._sql()
        assert "idx_trim_proposals_kind" in sql

    def test_indexes_are_conditional(self):
        """Both indexes must use CREATE INDEX IF NOT EXISTS."""
        sql = self._sql().upper()
        assert sql.count("CREATE INDEX IF NOT EXISTS") >= 2


class TestMigration023DownMigration:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_down_migration_block_present(self):
        """Migration must include a down-migration comment or DROP TABLE block."""
        sql = self._sql().upper()
        assert "DROP TABLE" in sql or "-- DOWN" in sql or "DOWN MIGRATION" in sql
