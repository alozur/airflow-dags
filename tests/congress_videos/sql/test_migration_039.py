"""Tests for migration 039 — extend speaker_turns.source CHECK constraint to
allow 'llm_resolved' (issue #131).

Reads the SQL file statically and asserts structural properties. No DB
connection required (mirrors test_migration_038.py pattern).

Amendment W2 (design-amendments, orchestrator-ratified): migration 022
declares the ``source`` CHECK inline, so Postgres auto-names it
``speaker_turns_source_check``. The correct idempotent mechanism is
DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT (constraint replacement), NOT a
pg_constraint-guarded DO-block — this file asserts that DROP+ADD shape.
"""
from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "039_extend_speaker_turns_source.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration039FileExists:

    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_038(self):
        migrations_dir = MIGRATION_PATH.parent
        names = sorted(p.name for p in migrations_dir.glob("*.sql"))
        assert "039_extend_speaker_turns_source.sql" in names
        idx_039 = names.index("039_extend_speaker_turns_source.sql")
        idx_038 = names.index("038_restore_chapter_abandoned_gate.sql")
        assert idx_039 > idx_038


class TestMigration039ConstraintShape:

    def test_drops_the_autonamed_constraint(self):
        """DROP CONSTRAINT IF EXISTS speaker_turns_source_check — the exact
        name Postgres auto-assigns to migration 022's inline CHECK."""
        sql = _executable_sql().upper()
        assert "DROP CONSTRAINT IF EXISTS SPEAKER_TURNS_SOURCE_CHECK" in sql

    def test_adds_constraint_with_same_name(self):
        sql = _executable_sql().upper()
        assert "ADD CONSTRAINT SPEAKER_TURNS_SOURCE_CHECK" in sql

    def test_no_pg_constraint_guard_do_block(self):
        """Amendment W2: this migration must NOT use the 034-style
        pg_constraint-guarded DO-block — DROP+ADD is the correct idempotent
        mechanism for an auto-named inline constraint."""
        sql = _executable_sql().upper()
        assert "PG_CONSTRAINT" not in sql

    def test_check_allows_all_four_source_values(self):
        sql = _executable_sql()
        assert "'acoustic'" in sql
        assert "'text_confirmed'" in sql
        assert "'text_named'" in sql
        assert "'llm_resolved'" in sql

    def test_drop_precedes_add_on_speaker_turns(self):
        sql = _executable_sql().upper()
        drop_idx = sql.index("DROP CONSTRAINT IF EXISTS SPEAKER_TURNS_SOURCE_CHECK")
        add_idx = sql.index("ADD CONSTRAINT SPEAKER_TURNS_SOURCE_CHECK")
        assert drop_idx < add_idx

    def test_alters_speaker_turns_table(self):
        sql = _executable_sql().upper()
        assert "ALTER TABLE SPEAKER_TURNS" in sql


class TestMigration039DownBlock:

    def test_down_block_present(self):
        sql = _sql().upper()
        assert "-- DOWN" in sql

    def test_down_repairs_llm_resolved_rows_before_narrowing(self):
        """DOWN must UPDATE existing llm_resolved rows to acoustic/0.50/NULL
        before restoring the narrow 3-value CHECK, or the re-add fails on
        live data with llm_resolved rows still present."""
        sql = _sql()
        # DOWN content lives as commented-out prose (manual-apply convention,
        # see 038); search the raw text after the "-- DOWN" marker.
        down_idx = sql.upper().index("-- DOWN")
        down_text = sql[down_idx:]
        assert re.search(
            r"UPDATE\s+speaker_turns\s+SET\s+source\s*=\s*'acoustic'",
            down_text,
            re.IGNORECASE,
        ), "DOWN must UPDATE speaker_turns SET source='acoustic' for llm_resolved rows"
        assert "confidence" in down_text.lower()
        assert re.search(r"resolved_name\s*=\s*NULL", down_text, re.IGNORECASE)
        assert "llm_resolved" in down_text
        # The repair UPDATE must come before the narrow CHECK is restored.
        update_idx = down_text.upper().index("UPDATE SPEAKER_TURNS")
        restore_idx = down_text.upper().rindex("ADD CONSTRAINT")
        assert update_idx < restore_idx


class TestMigration039Hygiene:

    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bdevelopment\.\w+", sql), "Must not use development.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_no_bare_create_table(self):
        sql = _sql()
        assert not re.search(r"\bCREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_bare_create_index(self):
        sql = _sql()
        assert not re.search(r"\bCREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_bare_drop_table(self):
        sql = _sql()
        assert not re.search(r"\bDROP\s+TABLE\s+(?!IF\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_seed_inserts(self):
        sql = _sql()
        assert not re.search(r"\bINSERT\s+INTO\b", sql, re.IGNORECASE)
