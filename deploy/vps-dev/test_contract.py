"""Standalone static contracts; deliberately bypass application pytest fixtures."""

import os
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

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
        self.assertEqual(list(config["networks"]), ["runtime"])
        self.assertIs(runtime["internal"], True)
        self.assertEqual(runtime["ipam"]["config"], [{"subnet": "${RUNTIME_SUBNET:?Required}"}])
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
        for service in config["services"].values():
            attached = service["networks"]
            names = set(attached) if isinstance(attached, dict) else set(attached)
            self.assertEqual(names, {"runtime"})
            self.assertNotIn("container_name", service)
            self.assertNotIn("cpuset", service)
            self.assertNotIn("network_mode", service)
            for mount in service.get("volumes", []):
                self.assertNotIn("/volume1", mount)
                self.assertNotIn("docker.sock", mount)

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
            self.assertEqual(env["POSTGRES_USER"], "airflow_dev")
            self.assertEqual(env["POSTGRES_SCHEMA"], "development")
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
        # A fresh database needs the base schema files before migration 004 can run.
        for base in ("congressional_videos_schema.sql", "youtube_chapters_schema.sql", "grant_permissions.sql"):
            self.assertIn(base, script)
            self.assertTrue((HERE.parent.parent / "congress_videos/sql" / base).exists(), base)

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


if __name__ == "__main__":
    unittest.main()
