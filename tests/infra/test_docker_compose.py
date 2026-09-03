"""Static-content guards for docker-compose.yml / docker-compose.prod.yml.

No Docker involved — pure `yaml.safe_load` + structural/string assertions.
Covers issue #207 (git-sync credential hygiene) and issue #215 (NAS deploy
topology parity). docker-compose.test.yml is a separate, untouched e2e stack
and is only referenced here to assert it stays uncoupled from these changes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

DEV_COMPOSE = REPO_ROOT / "docker-compose.yml"
PROD_COMPOSE = REPO_ROOT / "docker-compose.prod.yml"
TEST_COMPOSE = REPO_ROOT / "docker-compose.test.yml"

_TOKEN_IN_URL = re.compile(r"//[^/\s]*:[^/@\s]*@")
_GITHUB_TOKEN_AT = re.compile(r"\$\{?GITHUB_TOKEN\}?@")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _command_text(service: dict) -> str:
    """Flatten a service's `command` (string, list, or block scalar) to text."""
    command = service.get("command")
    if command is None:
        return ""
    if isinstance(command, list):
        return "\n".join(str(part) for part in command)
    return str(command)


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestComposeParses:
    def test_yaml_parses_cleanly(self, path: Path):
        config = _load(path)
        assert "services" in config


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestNoTokenInGitRemoteURL:
    """Issue #207: init-dags must authenticate via GIT_ASKPASS, never a
    token embedded directly in the git remote URL."""

    def test_init_dags_command_has_no_credential_in_url(self, path: Path):
        config = _load(path)
        command_text = _command_text(config["services"]["init-dags"])
        assert not _TOKEN_IN_URL.search(command_text), f"{path.name}: init-dags command embeds credentials in a URL"
        assert not _GITHUB_TOKEN_AT.search(command_text), (
            f"{path.name}: init-dags command embeds GITHUB_TOKEN directly in the remote URL"
        )

    def test_init_dags_command_uses_escaped_github_token(self, path: Path):
        """D6: shell refs inside the command block must be $$-escaped so
        compose doesn't interpolate the live token into the generated script
        or into `docker inspect`'s Cmd."""
        config = _load(path)
        command_text = _command_text(config["services"]["init-dags"])
        assert "$$GITHUB_TOKEN" in command_text, (
            f"{path.name}: init-dags command must reference $$GITHUB_TOKEN (escaped)"
        )


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestUntouchedMetadataConnection:
    """SQL_ALCHEMY_CONN must keep authenticating as `airflow` — the Airflow
    metadata DB connection is out of scope for this change (design.md)."""

    def test_sql_alchemy_conn_still_authenticates_as_airflow(self, path: Path):
        config = _load(path)
        common_env = config["x-airflow-common"]["environment"]
        sql_alchemy_conn = common_env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
        assert sql_alchemy_conn.startswith("postgresql+psycopg2://airflow:"), (
            f"{path.name}: SQL_ALCHEMY_CONN must still authenticate as `airflow`"
        )


class TestDevContainerNamesSuffixedDev:
    """Issue #215: dev stack container names must be suffixed `-dev` to match
    the live NAS topology (dev + prod share one Docker host)."""

    def test_every_declared_container_name_ends_dev(self):
        config = _load(DEV_COMPOSE)
        names = [service["container_name"] for service in config["services"].values() if "container_name" in service]
        assert names, "No container_name declared in docker-compose.yml"
        for name in names:
            assert name.endswith("-dev"), f"{name} must end with -dev"


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestExternalNetworkNameOverride:
    """Issue #215: ml_api_network must declare its live external name
    explicitly, matching the ml-apis stack's actual Compose project name."""

    def test_ml_api_network_has_explicit_name(self, path: Path):
        config = _load(path)
        network = config["networks"]["ml_api_network"]
        assert network == {"external": True, "name": "ml-apis_ml_api_network"}, (
            f"{path.name}: ml_api_network must be {{external: True, name: 'ml-apis_ml_api_network'}}, got {network}"
        )


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestMigrationRoleEnvPassthrough:
    """x-airflow-common must forward MIGRATION_POSTGRES_* into the containers.

    Portainer stack.env values are compose-interpolation inputs only; without
    these environment entries the migration role never reaches the DAGs and
    run_migrations silently falls back to the DML-only runtime role (#203).
    """

    def test_migration_credentials_are_forwarded(self, path: Path):
        env = _load(path)["x-airflow-common"]["environment"]
        assert env["MIGRATION_POSTGRES_USER"] == "${MIGRATION_POSTGRES_USER:-}"
        assert env["MIGRATION_POSTGRES_PASSWORD"] == "${MIGRATION_POSTGRES_PASSWORD:-}"


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestLlmTimeoutEnvPassthrough:
    """x-airflow-common must forward the OpenAI request timeout budget knobs.

    Without these environment entries, an operator override in stack.env
    never reaches the container and utils/llm_config.py silently keeps the
    committed defaults — the knob is inert (issue #355).
    """

    def test_timeout_env_vars_are_forwarded(self, path: Path):
        env = _load(path)["x-airflow-common"]["environment"]
        assert env["LLM_TIMEOUT_SECONDS"] == "${LLM_TIMEOUT_SECONDS:-}"
        assert env["LLM_CONNECT_TIMEOUT_SECONDS"] == "${LLM_CONNECT_TIMEOUT_SECONDS:-}"


@pytest.mark.parametrize("path", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
class TestExplicitDNS:
    """Issue #215: NAS containers need explicit public DNS resolvers."""

    def test_x_airflow_common_has_dns(self, path: Path):
        config = _load(path)
        assert config["x-airflow-common"].get("dns") == ["8.8.8.8", "1.1.1.1"]

    def test_init_dags_has_dns(self, path: Path):
        config = _load(path)
        assert config["services"]["init-dags"].get("dns") == ["8.8.8.8", "1.1.1.1"]


class TestDevNetworkSubnetMatchesLiveValue:
    """Issue #215: the dev bridge network's ipam subnet must match the
    verified live NAS value (was a stale placeholder)."""

    def test_dev_subnet_is_192_168_50_0_24(self):
        config = _load(DEV_COMPOSE)
        ipam_config = config["networks"]["airflow_network"]["ipam"]["config"][0]
        assert ipam_config["subnet"] == "192.168.50.0/24"
        assert ipam_config["gateway"] == "192.168.50.1"


class TestProdNetworkSubnetUnchanged:
    """D9 regression guard: the prod subnet already matches the live value —
    this change must not touch it."""

    def test_prod_subnet_stays_172_24_0_0_16(self):
        config = _load(PROD_COMPOSE)
        ipam_config = config["networks"]["airflow_network"]["ipam"]["config"][0]
        assert ipam_config["subnet"] == "172.24.0.0/16"
        assert ipam_config["gateway"] == "172.24.0.1"


class TestTestComposeUntouchedSurface:
    """docker-compose.test.yml is a separate e2e stack. It must stay
    structurally uncoupled from the git-sync/GITHUB_TOKEN surface touched by
    this change — no accidental future coupling."""

    def test_no_init_dags_service(self):
        config = _load(TEST_COMPOSE)
        assert "init-dags" not in config["services"]

    def test_no_github_token_key(self):
        raw = TEST_COMPOSE.read_text(encoding="utf-8")
        assert "GITHUB_TOKEN" not in raw
