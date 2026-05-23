"""Tests for utils/env_loader.py."""

from __future__ import annotations

import pytest


class TestLoadEnvIfLocal:

    def test_does_not_call_load_dotenv_in_docker_env(self, monkeypatch, mocker):
        """When AIRFLOW__CORE__DAGS_FOLDER is set (Docker), dotenv is not loaded."""
        monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
        mock_load = mocker.patch("dotenv.load_dotenv")

        from utils.env_loader import load_env_if_local

        load_env_if_local()

        mock_load.assert_not_called()

    def test_calls_load_dotenv_when_not_in_docker(self, monkeypatch, mocker):
        """When AIRFLOW__CORE__DAGS_FOLDER is not set, dotenv.load_dotenv is called."""
        monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
        mock_load = mocker.patch("dotenv.load_dotenv")

        from utils.env_loader import load_env_if_local

        load_env_if_local()

        mock_load.assert_called_once()

    def test_silently_ignores_missing_dotenv_package(self, monkeypatch):
        """ImportError on dotenv import is silently ignored."""
        import sys
        monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
        # Make dotenv unavailable
        monkeypatch.setitem(sys.modules, "dotenv", None)

        from utils.env_loader import load_env_if_local

        # Should not raise
        load_env_if_local()
