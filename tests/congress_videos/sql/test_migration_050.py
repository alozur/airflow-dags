"""Tests for migration 050 — final-copy-verification audit columns (issue #512).

Static SQL assertions, no DB connection (mirrors test_migration_043.py).
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "050_final_copy_verification_audit.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


SPEAKER_TURN_VIDEOS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("copy_verification_verdict", "TEXT"),
    ("copy_verification_findings", "JSONB"),
    ("copy_original_title", "TEXT"),
    ("copy_original_description", "TEXT"),
    ("copy_corrected_title", "TEXT"),
    ("copy_corrected_description", "TEXT"),
    ("copy_thumbnail_text", "TEXT"),
    ("copy_content_version", "TEXT"),
    ("copy_verified_at", "TIMESTAMPTZ"),
)

VIDEO_SHORTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("copy_verification_verdict", "TEXT"),
    ("copy_verification_findings", "JSONB"),
    ("copy_original_title", "TEXT"),
    ("copy_original_description", "TEXT"),
    ("copy_corrected_title", "TEXT"),
    ("copy_corrected_description", "TEXT"),
    ("copy_content_version", "TEXT"),
    ("copy_verified_at", "TIMESTAMP"),
)


class TestMigration050FileExists:
    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_049(self):
        names = sorted(p.name for p in MIGRATION_PATH.parent.glob("*.sql"))
        assert names.index(MIGRATION_PATH.name) > names.index("049_freshness_bucket_turn_publish_order.sql")


class TestMigration050BothAlterTableBlocksPresent:
    def test_alter_table_speaker_turn_videos_present(self):
        sql = _executable_sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS" in sql

    def test_alter_table_video_shorts_present(self):
        sql = _executable_sql().upper()
        assert "ALTER TABLE VIDEO_SHORTS" in sql


class TestMigration050SpeakerTurnVideosColumns:
    def _block(self) -> str:
        sql = _executable_sql()
        start = sql.index("ALTER TABLE speaker_turn_videos")
        end = sql.index("ALTER TABLE video_shorts")
        return sql[start:end]

    def test_every_column_uses_add_column_if_not_exists(self):
        block = self._block()
        for column, _column_type in SPEAKER_TURN_VIDEOS_COLUMNS:
            clause = f"ADD COLUMN IF NOT EXISTS {column}"
            assert clause in block, f"Missing clause: {clause!r}"

    def test_copy_verified_at_is_timestamptz(self):
        block = self._block()
        match = re.search(r"ADD COLUMN IF NOT EXISTS copy_verified_at\s+(\w+)", block)
        assert match is not None
        assert match.group(1).upper() == "TIMESTAMPTZ"

    def test_copy_thumbnail_text_present_only_on_speaker_turn_videos(self):
        block = self._block()
        assert "ADD COLUMN IF NOT EXISTS copy_thumbnail_text" in block


class TestMigration050VideoShortsColumns:
    def _block(self) -> str:
        sql = _executable_sql()
        start = sql.index("ALTER TABLE video_shorts")
        return sql[start:]

    def test_every_column_uses_add_column_if_not_exists(self):
        block = self._block()
        for column, _column_type in VIDEO_SHORTS_COLUMNS:
            clause = f"ADD COLUMN IF NOT EXISTS {column}"
            assert clause in block, f"Missing clause: {clause!r}"

    def test_copy_verified_at_is_plain_timestamp(self):
        block = self._block()
        match = re.search(r"ADD COLUMN IF NOT EXISTS copy_verified_at\s+(\w+)", block)
        assert match is not None
        assert match.group(1).upper() == "TIMESTAMP"

    def test_copy_thumbnail_text_absent_from_video_shorts(self):
        block = self._block()
        assert "copy_thumbnail_text" not in block


class TestMigration050DownBlockFullyCommented:
    """`migrations_dag` executes the whole file inside ONE transaction, so an
    uncommented DOWN block would silently revert this migration in the same
    run (repo incident precedent: migration DOWN block silent-revert)."""

    def test_down_block_present(self):
        sql = _sql().lower()
        assert "-- down" in sql

    def test_down_block_documents_manual_only_transactional_caveat(self):
        sql = _sql().lower()
        down_text = sql[sql.index("-- down") :]
        assert "manual only" in down_text
        assert "transaction" in down_text

    def test_every_line_after_down_marker_is_a_comment(self):
        """Regex assertion: every non-blank line following `-- DOWN` must
        start with `--` — nothing after the marker may be executable SQL."""
        sql = _sql()
        down_idx = sql.lower().index("-- down")
        down_section = sql[down_idx:]
        for line in down_section.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            assert stripped.startswith("--"), f"Uncommented line in DOWN block: {line!r}"


class TestMigration050Hygiene:
    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql)
        assert not re.search(r"\bdevelopment\.\w+", sql)
        assert not re.search(r"\bproduction\.\w+", sql)
