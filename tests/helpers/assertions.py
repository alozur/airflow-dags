"""Custom assertion helpers for the airflow-dags test suite."""

from __future__ import annotations
from typing import Any


def assert_success_result(result: dict[str, Any]) -> None:
    """Assert that a result dict represents a successful operation."""
    assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}: {result!r}"
    assert result.get("success") is True, (
        f"Expected success=True, got success={result.get('success')!r}. "
        f"Error: {result.get('error')!r}"
    )
    error = result.get("error")
    assert not error, f"Success result should have no error, got: {error!r}"


def assert_error_result(result: dict[str, Any], error_substring: str | None = None) -> None:
    """Assert that a result dict represents a failed operation."""
    assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}: {result!r}"
    assert result.get("success") is False, (
        f"Expected success=False, got success={result.get('success')!r}"
    )
    error = result.get("error")
    assert error, f"Error result must have non-empty 'error', got: {error!r}"
    if error_substring is not None:
        assert error_substring.lower() in str(error).lower(), (
            f"Expected error to contain {error_substring!r}, got: {error!r}"
        )


def assert_result_has_keys(result: dict[str, Any], required_keys: list[str]) -> None:
    """Assert a dict contains all of the listed keys."""
    assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}"
    missing = [k for k in required_keys if k not in result]
    assert not missing, f"Result missing required keys: {missing}. Got keys: {sorted(result.keys())}"


def assert_xcom_pushed(task_instance: Any, key: str, expected_value: Any = ...) -> None:
    """Assert that ti.xcom_push was called with the given key."""
    assert key in task_instance.xcom_store, (
        f"Expected XCom key {key!r} to be pushed. Pushed keys: {list(task_instance.xcom_store)}"
    )
    if expected_value is not ...:
        actual = task_instance.xcom_store[key]
        assert actual == expected_value, (
            f"XCom {key!r}: expected {expected_value!r}, got {actual!r}"
        )
