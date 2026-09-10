"""Standalone static contracts; deliberately bypass application pytest fixtures."""

import importlib.util
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

HERE = Path(__file__).resolve().parent


class DevContract(unittest.TestCase):
    def uv_environment(self):
        environment = {}
        for line in (HERE / "Dockerfile").read_text().splitlines():
            if line.startswith("ENV "):
                environment.update(token.split("=", 1) for token in shlex.split(line[4:]))
        return {key: value for key, value in environment.items() if key.startswith("UV_")}

    def test_uv_cache_is_isolated_before_root_and_airflow_commands(self):
        text = (HERE / "Dockerfile").read_text()
        before_first_uv = text.split("&& uv venv", 1)[0]
        self.assertIn("ENV UV_NO_CACHE=true", before_first_uv)
        self.assertEqual(self.uv_environment().get("UV_NO_CACHE"), "true")

    @unittest.skipUnless(shutil.which("uv"), "uv is required for the cache permission regression")
    def test_frozen_export_avoids_inaccessible_shared_cache_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "cache"
            marker = cache / "sdists-v9" / ".git"
            marker.parent.mkdir(parents=True)
            marker.touch()
            # Model a cache marker the current build user cannot open.
            marker.chmod(0)
            environment = {key: value for key, value in os.environ.items() if not key.startswith("UV_")}
            environment.update(self.uv_environment())
            environment["UV_CACHE_DIR"] = str(cache)
            try:
                result = subprocess.run(
                    ["uv", "export", "--frozen", "--no-dev", "--no-emit-project", "--format", "requirements-txt"],
                    cwd=HERE.parents[1],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("apache-airflow==2.11.1", result.stdout)
                self.assertIn("--hash=sha256:", result.stdout)
            finally:
                marker.chmod(0o600)

    def compose(self):
        path = HERE / "compose.yml"
        self.assertTrue(path.exists(), "Isolated DEV Compose is missing")
        return yaml.safe_load(path.read_text())

    def test_only_internal_network_and_fresh_project_volumes(self):
        config = self.compose()
        runtime = config["networks"]["runtime"]
        egress = config["networks"]["egress"]
        self.assertEqual(set(config["networks"]), {"runtime", "egress"})
        self.assertIs(runtime["internal"], True)
        self.assertEqual(runtime["ipam"]["config"], [{"subnet": "${RUNTIME_SUBNET:?Required}"}])
        self.assertEqual(egress["driver"], "bridge")
        self.assertEqual(egress["internal"], "${EGRESS_INTERNAL:-true}")
        self.assertEqual(egress["ipam"]["config"], [{"subnet": "${EGRESS_SUBNET:?Required}"}])
        for value in config["volumes"].values():
            self.assertFalse(value and (value.get("external") or value.get("name")))
        expected_services = {
            "metadata",
            "application",
            "init",
            "app-init",
            "scheduler",
            "webserver",
            "diarize-api",
            "yamnet-api",
            "whisper-api",
        }
        self.assertEqual(set(config["services"]), expected_services)
        expected_volumes = {
            "metadata",
            "application",
            "logs",
            "data",
            "hf_home",
            "yamnet_model_cache",
            "whisper_models",
        }
        self.assertEqual(set(config["volumes"]), expected_volumes)
        for name, service in config["services"].items():
            attached = service["networks"]
            names = set(attached) if isinstance(attached, dict) else set(attached)
            # LocalExecutor means only the scheduler runs DAG tasks, so only it
            # may reach the egress network; every other service stays runtime-only.
            expected_networks = {"runtime", "egress"} if name == "scheduler" else {"runtime"}
            self.assertEqual(names, expected_networks, name)
            self.assertNotIn("container_name", service)
            self.assertNotIn("cpuset", service)
            self.assertNotIn("network_mode", service)
            for mount in service.get("volumes", []):
                self.assertNotIn("/volume1", mount)
                self.assertNotIn("docker.sock", mount)

    def test_scheduler_holds_youtube_tokens_and_egress_signal_only(self):
        services = self.compose()["services"]
        scheduler = services["scheduler"]
        self.assertIn(
            "${YOUTUBE_TOKENS_HOST_DIR:?Required}:/opt/airflow/data/congress_videos/youtube_tokens",
            scheduler["volumes"],
        )
        self.assertEqual(scheduler["environment"]["EGRESS_INTERNAL"], "${EGRESS_INTERNAL:-true}")
        for name in ("webserver", "init", "app-init"):
            self.assertNotIn("youtube_tokens", " ".join(services[name].get("volumes", [])))

    def test_nas_archive_mount_is_read_only_scheduler_only_and_disabled_by_default(self):
        services = self.compose()["services"]
        scheduler = services["scheduler"]
        self.assertIn(
            "${NAS_SYNC_HOST_DIR:?Required}:/opt/airflow/nas_sync:ro",
            scheduler["volumes"],
        )
        for name, service in services.items():
            if name == "scheduler":
                continue
            self.assertNotIn("nas_sync", " ".join(service.get("volumes", [])))

        env = scheduler["environment"]
        # Empty NAS_ARCHIVE_HOST is the safe default: nas_archive_dag.py treats it as disabled.
        self.assertEqual(env["NAS_ARCHIVE_HOST"], "${NAS_ARCHIVE_HOST:-}")
        self.assertEqual(env["NAS_ARCHIVE_PORT"], "${NAS_ARCHIVE_PORT:-22}")
        self.assertEqual(env["NAS_ARCHIVE_USER"], "${NAS_ARCHIVE_USER:-}")
        self.assertEqual(env["NAS_ARCHIVE_ROOT"], "${NAS_ARCHIVE_ROOT:-}")
        self.assertEqual(env["NAS_ARCHIVE_MIN_AGE_DAYS"], "${NAS_ARCHIVE_MIN_AGE_DAYS:-14}")
        self.assertEqual(env["NAS_ARCHIVE_SSH_DIR"], "/opt/airflow/nas_sync")
        # nas_fetch fallback source (read-only legacy production tree); empty disables it.
        self.assertEqual(env["NAS_FETCH_LEGACY_ROOT"], "${NAS_FETCH_LEGACY_ROOT:-}")

    def test_youtube_download_proxy_has_safe_default(self):
        env = self.compose()["services"]["scheduler"]["environment"]
        # Empty YOUTUBE_DOWNLOAD_PROXY is the safe default: youtube_downloader.py
        # downloads direct when unset.
        self.assertEqual(env["YOUTUBE_DOWNLOAD_PROXY"], "${YOUTUBE_DOWNLOAD_PROXY:-}")

    def test_external_api_keys_come_from_environment_with_safe_default(self):
        env = self.compose()["services"]["scheduler"]["environment"]
        for key in ("OPENAI_API_KEY", "YOUTUBE_API_KEY", "REAP_API_KEY", "PIKZELS_API_KEY"):
            self.assertEqual(env[key], f"${{{key}:-dev-disabled-not-a-credential}}")

    def test_git_sync_dag_is_excluded_from_the_image(self):
        text = (HERE / "Dockerfile").read_text()
        before_readonly, _, after = text.partition("RUN chmod -R a-w")
        self.assertTrue(after, "the read-only chmod step is missing")
        self.assertIn(".airflowignore", before_readonly)
        self.assertIn("git_sync_dag", before_readonly)
        self.assertNotIn("git_sync_dag", after)

    def test_no_service_publishes_ports_and_ui_upstream_is_fixed(self):
        services = self.compose()["services"]
        for service in services.values():
            self.assertNotIn("ports", service)
        expected = {"runtime": {"ipv4_address": "${UI_UPSTREAM:?Required}"}}
        self.assertEqual(services["webserver"]["networks"], expected)

    def test_business_dags_paused_and_app_database_is_isolated_from_metadata(self):
        config = self.compose()
        for name in ("scheduler", "webserver"):
            env = config["services"][name]["environment"]
            self.assertEqual(env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"], "true")
            self.assertEqual(env["AIRFLOW__CORE__LOAD_EXAMPLES"], "false")
            self.assertEqual(env["POSTGRES_HOST"], "application")
            # Business schema and runtime role are parameterized per VPS project
            # (issue #203 follow-up): POSTGRES_RUNTIME_ROLE/POSTGRES_SCHEMA are
            # rendered into release.env by the infra side, defaulting to the DEV
            # pair (airflow_dev/development) when unset.
            self.assertEqual(env["POSTGRES_USER"], "${POSTGRES_RUNTIME_ROLE:-airflow_dev}")
            self.assertEqual(env["POSTGRES_SCHEMA"], "${POSTGRES_SCHEMA:-development}")
            for key in (
                "GITHUB_TOKEN",
                "_PIP_ADDITIONAL_REQUIREMENTS",
                "MIGRATION_POSTGRES_PASSWORD",
                "MIGRATION_POSTGRES_USER",
                "APPLICATION_PASSWORD",
            ):
                self.assertNotIn(key, env)

    def test_application_database_is_isolated_and_provisioned_by_app_init(self):
        config = self.compose()
        services = config["services"]
        application = services["application"]
        metadata = services["metadata"]
        self.assertNotIn("ports", application)
        self.assertEqual(application["image"], metadata["image"])
        self.assertEqual(application["environment"]["POSTGRES_USER"], "airflow")
        self.assertEqual(application["volumes"], ["application:/var/lib/postgresql/data"])

        app_init = services["app-init"]
        self.assertEqual(app_init["depends_on"], {"application": {"condition": "service_healthy"}})
        self.assertEqual(app_init["command"], ["python", "/opt/dev-tools/app_init.py"])
        env = app_init["environment"]
        for key in ("APPLICATION_PASSWORD", "MIGRATION_POSTGRES_USER", "MIGRATION_POSTGRES_PASSWORD"):
            self.assertIn(key, env)
        self.assertEqual(env["MIGRATION_POSTGRES_USER"], "airflow_migrations")

        for name in ("scheduler", "webserver"):
            self.assertIn("application", services[name]["depends_on"])
            self.assertEqual(services[name]["depends_on"]["application"], {"condition": "service_healthy"})

    def test_app_init_reads_only_variables_the_app_init_service_provides(self):
        # Compose interpolates controller secret names into container variable names;
        # the script must read the latter, or it fails only at deploy time.
        provided = set(self.compose()["services"]["app-init"]["environment"])
        script = (HERE / "app_init.py").read_text()
        read = set(re.findall(r'os\.environ\["([A-Z_]+)"\]', script))
        self.assertTrue(read, "app_init.py reads no environment variables")
        self.assertLessEqual(
            read, provided, f"app_init.py reads variables app-init never sets: {sorted(read - provided)}"
        )
        # A fresh DEV database needs the base schema files before migration 004 can run;
        # PROD gets grant_permissions_production.sql instead (never the base schema files —
        # see app_init._bootstrap).
        for base in (
            "congressional_videos_schema.sql",
            "youtube_chapters_schema.sql",
            "grant_permissions.sql",
            "grant_permissions_production.sql",
        ):
            self.assertIn(base, script)
            self.assertTrue((HERE.parent.parent / "congress_videos/sql" / base).exists(), base)

    def test_init_creates_every_pool_the_dags_require(self):
        init = (HERE / "init.py").read_text()
        required = set()
        for dag_file in (HERE.parent.parent / "congress_videos").glob("*_dag.py"):
            required |= set(re.findall(r'pool="([a-z_]+)"', dag_file.read_text()))
        self.assertEqual(required, {"nas_ffmpeg"})
        for pool in required:
            self.assertIn(f'"{pool}"', init)

    def test_ml_sidecars_are_offline_and_bounded(self):
        services = self.compose()["services"]
        diarize = services["diarize-api"]
        self.assertEqual(diarize["environment"]["HF_TOKEN"], "dev-offline-not-a-token")
        self.assertEqual(diarize["environment"]["HF_HUB_OFFLINE"], "1")
        self.assertIn("TORCH_NUM_THREADS", diarize["environment"])
        self.assertEqual(diarize["image"], "${DIARIZE_IMAGE:?Required}")
        whisper = services["whisper-api"]
        self.assertIn("@sha256:", whisper["image"])
        self.assertEqual(whisper["environment"]["ASR_ENGINE"], "faster_whisper")
        self.assertEqual(whisper["environment"]["HF_HUB_OFFLINE"], "1")
        self.assertEqual(services["yamnet-api"]["image"], "${YAMNET_IMAGE:?Required}")
        for name in ("diarize-api", "yamnet-api", "whisper-api"):
            self.assertIn("mem_limit", services[name], name)
            self.assertIn("cpus", services[name], name)
            self.assertIn("healthcheck", services[name], name)
            self.assertEqual(services[name]["security_opt"], ["no-new-privileges:true"])
        scheduler = services["scheduler"]["environment"]
        self.assertEqual(scheduler["WHISPER_API_HOST"], "whisper-api")
        self.assertEqual(scheduler["DIARIZE_API_HOST"], "diarize-api")
        self.assertEqual(scheduler["YAMNET_API_HOST"], "yamnet-api")

    def test_smoke_probe_is_shipped_read_only_and_prints_no_payloads(self):
        text = (HERE / "Dockerfile").read_text()
        self.assertIn("deploy/vps-dev/ml_smoke.py", text)
        self.assertIn(
            "deploy/vps-dev/ml_smoke.py deploy/vps-dev/app_init.py deploy/vps-dev/app_smoke.py /opt/dev-tools/", text
        )
        smoke = (HERE / "ml_smoke.py").read_text()
        for expected in ("HF_TOKEN", "print(payload", "print(response"):
            self.assertNotIn(expected, smoke)
        for expected in ("/detect", "/diarize", "/asr", "applause_intervals", "speaker_changes"):
            self.assertIn(expected, smoke)

    def test_application_tools_are_shipped_and_print_no_credential_values(self):
        text = (HERE / "Dockerfile").read_text()
        self.assertIn("deploy/vps-dev/app_init.py", text)
        self.assertIn("deploy/vps-dev/app_smoke.py", text)
        for name in ("app_init.py", "app_smoke.py"):
            source = (HERE / name).read_text()
            for forbidden in ("print(conn", "print(cur", "print(row", 'PASSWORD}"'):
                self.assertNotIn(forbidden, source, f"{name} may print sensitive data via {forbidden}")

    def test_image_uses_frozen_lock_and_never_copies_whole_checkout(self):
        path = HERE / "Dockerfile"
        self.assertTrue(path.exists())
        text = path.read_text()
        self.assertIn("2.11.1-python3.12@sha256:", text)
        self.assertIn("--frozen", text)
        self.assertIn("--require-hashes", text)
        self.assertNotIn("COPY . ", text)
        self.assertNotIn("pip install --upgrade", text)


@unittest.skipUnless(importlib.util.find_spec("airflow"), "airflow is not installed (controller run)")
class AppInitProvisioningSelectionTests(unittest.TestCase):
    """Pure-function coverage for app_init._resolve_provisioning — no DB involved."""

    @classmethod
    def setUpClass(cls):
        if str(HERE) not in sys.path:
            sys.path.insert(0, str(HERE))
        import app_init  # local import: only meaningful once HERE is on sys.path

        cls.app_init = app_init

    def test_development_pair_selects_dev_grant_script_and_owner(self):
        with mock.patch.dict(os.environ, {"POSTGRES_SCHEMA": "development", "POSTGRES_USER": "airflow_dev"}):
            schema, owner_role, grant_script = self.app_init._resolve_provisioning()
        self.assertEqual(schema, "development")
        self.assertEqual(owner_role, "airflow_dev")
        self.assertEqual(grant_script.name, "grant_permissions.sql")

    def test_production_pair_selects_production_grant_script_and_owner(self):
        with mock.patch.dict(os.environ, {"POSTGRES_SCHEMA": "production", "POSTGRES_USER": "airflow_prod"}):
            schema, owner_role, grant_script = self.app_init._resolve_provisioning()
        self.assertEqual(schema, "production")
        self.assertEqual(owner_role, "airflow_prod")
        self.assertEqual(grant_script.name, "grant_permissions_production.sql")

    def test_mismatched_schema_and_role_fails_fast(self):
        env = {"POSTGRES_SCHEMA": "production", "POSTGRES_USER": "airflow_dev"}
        with mock.patch.dict(os.environ, env), self.assertRaises(ValueError):
            self.app_init._resolve_provisioning()

    def test_unknown_schema_fails_fast(self):
        env = {"POSTGRES_SCHEMA": "staging", "POSTGRES_USER": "airflow_dev"}
        with mock.patch.dict(os.environ, env), self.assertRaises(ValueError):
            self.app_init._resolve_provisioning()

    def test_unknown_role_for_known_schema_fails_fast(self):
        env = {"POSTGRES_SCHEMA": "development", "POSTGRES_USER": "airflow_prod"}
        with mock.patch.dict(os.environ, env), self.assertRaises(ValueError):
            self.app_init._resolve_provisioning()


class _FakeCursor:
    """Minimal cursor double: .execute() is a no-op, .fetchone() reports the
    sentinel-table check's answer — enough to drive _table_exists without a DB."""

    def __init__(self, sentinel_present: bool):
        self._sentinel_present = sentinel_present

    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return (self._sentinel_present,)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _FakeConn:
    def __init__(self, sentinel_present: bool):
        self._cursor = _FakeCursor(sentinel_present)

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


@unittest.skipUnless(importlib.util.find_spec("psycopg2"), "psycopg2 is not installed (controller run)")
class AppInitBootstrapEmptySchemaTests(unittest.TestCase):
    """_bootstrap fails fast for a non-development schema with no restored data — no real DB.

    The sentinel-table check (_table_exists) is mocked via _FakeConn/_FakeCursor; grants,
    password statements, and schema creation all go through the same no-op fake cursor, so
    nothing here ever reaches a real Postgres connection."""

    @classmethod
    def setUpClass(cls):
        if str(HERE) not in sys.path:
            sys.path.insert(0, str(HERE))
        import app_init  # local import: only meaningful once HERE is on sys.path

        cls.app_init = app_init

    def test_production_with_no_restored_data_fails_fast_before_migrations(self):
        grant_script = HERE.parent.parent / "congress_videos" / "sql" / "grant_permissions_production.sql"
        conn = _FakeConn(sentinel_present=False)
        env = {"POSTGRES_PASSWORD": "test-runtime-pw", "MIGRATION_POSTGRES_PASSWORD": "test-migration-pw"}
        with mock.patch.dict(os.environ, env), self.assertRaises(self.app_init.EmptyRestoredSchemaError) as ctx:
            self.app_init._bootstrap(conn, "production", "airflow_prod", grant_script)
        message = str(ctx.exception)
        self.assertIn("production", message)
        self.assertIn("import-db", message)

    def test_production_with_restored_data_does_not_raise(self):
        grant_script = HERE.parent.parent / "congress_videos" / "sql" / "grant_permissions_production.sql"
        conn = _FakeConn(sentinel_present=True)
        env = {"POSTGRES_PASSWORD": "test-runtime-pw", "MIGRATION_POSTGRES_PASSWORD": "test-migration-pw"}
        with mock.patch.dict(os.environ, env):
            applied = self.app_init._bootstrap(conn, "production", "airflow_prod", grant_script)
        self.assertEqual(applied, 0)  # BASE_SCHEMA_FILES never runs for a non-development schema


class VerifyGateContractTests(unittest.TestCase):
    """verify.py keeps DEV strict by default and only relaxes on an explicit false."""

    def test_verify_reads_the_business_paused_gate_with_a_strict_default(self):
        text = (HERE / "verify.py").read_text()
        self.assertIn('EXPECT_BUSINESS_PAUSED_ENV = "DEPLOY_EXPECT_BUSINESS_PAUSED"', text)
        self.assertIn('os.environ.get(EXPECT_BUSINESS_PAUSED_ENV, "true")', text)
        self.assertIn("if expect_business_paused():", text)
        self.assertIn('"An active business DAG is unpaused"', text)


if __name__ == "__main__":
    unittest.main()
