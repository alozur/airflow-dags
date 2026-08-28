"""Static-text guards for the least-privilege grant scripts (issue #203).

No DB connection — pure regex/string checks against the SQL files, mirroring
the test_production_schema.py static-drift pattern. Verifies both scripts
grant DML-only to their per-environment runtime role and reserve DDL for a
separate migration role, without ever granting ALL PRIVILEGES broadly to
the legacy `airflow` role or issuing a NOLOGIN/REVOKE (additive-first
constraint — see design.md).
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[3] / "congress_videos" / "sql"

DEV_SCRIPT = SQL_DIR / "grant_permissions.sql"
PROD_SCRIPT = SQL_DIR / "grant_permissions_production.sql"

_ALL_PRIVILEGES = re.compile(r"\bALL\s+PRIVILEGES\b", re.IGNORECASE)
_GRANT_TO_AIRFLOW = re.compile(r"\bTO\s+airflow\s*;", re.IGNORECASE)
_NOLOGIN = re.compile(r"\bNOLOGIN\b", re.IGNORECASE)
_REVOKE = re.compile(r"\bREVOKE\b", re.IGNORECASE)
_DDL_VERB = re.compile(r"\b(CREATE|ALTER|DROP)\b", re.IGNORECASE)
_COMMENT_LINE = re.compile(r"--[^\n]*")


def _sql_statements_only(text: str) -> str:
    """Strip `--` line comments so checks only see executable SQL, not prose."""
    return _COMMENT_LINE.sub("", text)


@pytest.mark.parametrize("path", [DEV_SCRIPT, PROD_SCRIPT], ids=lambda p: p.name)
class TestGrantScriptsCommonInvariants:
    """Invariants both grant scripts must satisfy (additive-first, D1/D2/D3)."""

    def test_no_all_privileges_grant(self, path: Path):
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        assert not _ALL_PRIVILEGES.search(sql), (
            f"{path.name}: must not grant ALL PRIVILEGES — least-privilege only"
        )

    def test_no_broad_grant_to_legacy_airflow(self, path: Path):
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        assert not _GRANT_TO_AIRFLOW.search(sql), (
            f"{path.name}: must not target the legacy `airflow` role directly"
        )

    def test_no_nologin(self, path: Path):
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        assert not _NOLOGIN.search(sql), (
            f"{path.name}: must not touch the legacy `airflow` role's LOGIN status"
        )

    def test_no_revoke(self, path: Path):
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        assert not _REVOKE.search(sql), (
            f"{path.name}: cutover REVOKE is a manual post-merge step, not shipped in this script"
        )

    def test_ddl_verbs_only_in_migration_role_block(self, path: Path):
        """Every CREATE/ALTER/DROP statement must be scoped to the migration role."""
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        for statement in sql.split(";"):
            if _DDL_VERB.search(statement) and "ROLE" not in statement.upper().split("(")[0]:
                # Statements that create/alter objects (not roles) must mention
                # the migration role somewhere in the statement (FOR ROLE / GRANT ... TO).
                if "CREATE ROLE" in statement.upper() or "pg_roles" in statement:
                    continue  # role-creation DO block — not a schema-object DDL grant
                assert "migrations" in statement.lower(), (
                    f"{path.name}: DDL statement outside the migration-role block:\n{statement.strip()}"
                )


class TestDevScriptRoleNames:

    def test_contains_airflow_dev_role(self):
        sql = DEV_SCRIPT.read_text(encoding="utf-8")
        assert "airflow_dev" in sql

    def test_contains_migration_role(self):
        sql = DEV_SCRIPT.read_text(encoding="utf-8")
        assert "airflow_migrations" in sql

    def test_scoped_to_development_schema_only(self):
        """Executable statements target `development`; no statement targets
        `production` (comments MAY mention the sibling file by name)."""
        sql = _sql_statements_only(DEV_SCRIPT.read_text(encoding="utf-8"))
        assert "development" in sql
        assert "production" not in sql.lower()


class TestProdScriptRoleNames:

    def test_contains_airflow_prod_role(self):
        sql = PROD_SCRIPT.read_text(encoding="utf-8")
        assert "airflow_prod" in sql

    def test_contains_migration_role(self):
        sql = PROD_SCRIPT.read_text(encoding="utf-8")
        assert "airflow_migrations" in sql

    def test_scoped_to_production_schema(self):
        sql = _sql_statements_only(PROD_SCRIPT.read_text(encoding="utf-8"))
        assert "production" in sql


@pytest.mark.parametrize(
    "path,runtime_role",
    [(DEV_SCRIPT, "airflow_dev"), (PROD_SCRIPT, "airflow_prod")],
    ids=["dev", "prod"],
)
class TestRuntimeRoleHasNoDDL:
    """The runtime role's own GRANT block must never include CREATE/ALTER/DROP."""

    def test_runtime_role_grant_block_is_dml_only(self, path: Path, runtime_role: str):
        sql = _sql_statements_only(path.read_text(encoding="utf-8"))
        # Find GRANT statements that target the runtime role directly.
        runtime_grants = [
            stmt for stmt in sql.split(";")
            if re.search(rf"\bTO\s+{runtime_role}\b", stmt, re.IGNORECASE)
            and "GRANT" in stmt.upper()
        ]
        assert runtime_grants, f"No GRANT ... TO {runtime_role} statements found"
        for stmt in runtime_grants:
            assert not _DDL_VERB.search(stmt.split("GRANT", 1)[-1].split("ON", 1)[0]), (
                f"Runtime role {runtime_role} grant block must not include CREATE/ALTER/DROP:\n{stmt.strip()}"
            )
