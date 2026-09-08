"""Standalone static contracts; deliberately bypass application pytest fixtures."""

import os
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

    def test_business_dags_paused_and_app_database_is_not_metadata(self):
        service = self.compose()["services"]["scheduler"]
        env = service["environment"]
        self.assertEqual(env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"], "true")
        self.assertEqual(env["AIRFLOW__CORE__LOAD_EXAMPLES"], "false")
        self.assertEqual(env["POSTGRES_HOST"], "application-db-disabled.invalid")
        for key in ("GITHUB_TOKEN", "_PIP_ADDITIONAL_REQUIREMENTS", "MIGRATION_POSTGRES_PASSWORD"):
            self.assertNotIn(key, env)

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
