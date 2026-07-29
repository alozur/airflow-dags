#!/usr/bin/env bash
# scripts/test-airflow-e2e.sh — e2e smoke-test driver.
#
# Boots the ephemeral docker-compose.test.yml stack, polls scheduler + webserver
# health within a bounded timeout, asserts `airflow dags list-import-errors` is
# empty, and unconditionally tears the stack down (trap-based, preserves the
# original exit code). See design.md Q2 (exit codes), Q3 (bounded health poll),
# Q5 (teardown/disposability).
#
# Named exit codes (declared readonly, per design Q2):
#   0 = EXIT_SUCCESS         stack healthy, list-import-errors empty
#   1 = EXIT_IMPORT_ERRORS   stack healthy, list-import-errors non-empty
#   2 = EXIT_HEALTH_TIMEOUT  scheduler/webserver never became healthy in time
#   3 = EXIT_UV_UNAVAILABLE  `uv --version` failed during preflight (hard-fail)
#   4 = EXIT_DOCKER_UNAVAILABLE  docker / docker compose not available (graceful)
#   5 = EXIT_INTERNAL_ERROR  unexpected/internal error (fail-closed)
set -uo pipefail

readonly EXIT_SUCCESS=0
readonly EXIT_IMPORT_ERRORS=1
readonly EXIT_HEALTH_TIMEOUT=2
readonly EXIT_UV_UNAVAILABLE=3
readonly EXIT_DOCKER_UNAVAILABLE=4
readonly EXIT_INTERNAL_ERROR=5

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly COMPOSE_FILE="$REPO_ROOT/docker-compose.test.yml"
readonly PROJECT_NAME="airflow-dags-e2e"

readonly E2E_HEALTH_TIMEOUT="${E2E_HEALTH_TIMEOUT:-120}"
readonly E2E_POLL_INTERVAL="${E2E_POLL_INTERVAL:-5}"

_compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

_stack_started=0

_teardown() {
  local rc=$?
  if [ "$_stack_started" -eq 1 ]; then
    echo "[test-airflow-e2e] tearing down ephemeral stack (rc=$rc)..." >&2
    _compose down -v --remove-orphans --timeout 30 >&2 || true
  fi
  exit "$rc"
}
trap _teardown EXIT INT TERM

# --- Preflight: uv (hard-fail, exit 3, before any Docker interaction) -----------
if ! uv --version >/dev/null 2>&1; then
  echo "[test-airflow-e2e] ERROR: 'uv' is required but not found/failing (uv --version)." >&2
  echo "[test-airflow-e2e] Install uv: https://docs.astral.sh/uv/ then re-run." >&2
  exit "$EXIT_UV_UNAVAILABLE"
fi

# --- Preflight: docker / docker compose (graceful, exit 4, no container attempt) -
if ! command -v docker >/dev/null 2>&1; then
  echo "[test-airflow-e2e] Docker is not available on PATH; skipping e2e (unavailable)." >&2
  exit "$EXIT_DOCKER_UNAVAILABLE"
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "[test-airflow-e2e] 'docker compose' plugin is not available; skipping e2e (unavailable)." >&2
  exit "$EXIT_DOCKER_UNAVAILABLE"
fi
if ! docker info >/dev/null 2>&1; then
  echo "[test-airflow-e2e] Docker daemon is not reachable (docker info failed); skipping e2e (unavailable)." >&2
  exit "$EXIT_DOCKER_UNAVAILABLE"
fi

# --- Boot the stack --------------------------------------------------------------
echo "[test-airflow-e2e] starting ephemeral stack (project=$PROJECT_NAME)..." >&2
if ! _compose up -d; then
  echo "[test-airflow-e2e] ERROR: failed to start the docker compose stack." >&2
  _stack_started=1  # still attempt teardown of whatever partial state exists
  exit "$EXIT_INTERNAL_ERROR"
fi
_stack_started=1

# --- Bounded health poll (Q3): scheduler + webserver, heartbeat each interval ----
_service_health() {
  local container="$1"
  docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown"
}

waited=0
scheduler_container="${PROJECT_NAME}-airflow-scheduler-1"
webserver_container="${PROJECT_NAME}-airflow-webserver-1"

while :; do
  scheduler_status="$(_service_health "$scheduler_container")"
  webserver_status="$(_service_health "$webserver_container")"

  echo "[test-airflow-e2e] waited ${waited}s / ${E2E_HEALTH_TIMEOUT}s; scheduler=${scheduler_status} webserver=${webserver_status}" >&2

  if [ "$scheduler_status" = "healthy" ] && [ "$webserver_status" = "healthy" ]; then
    break
  fi

  if [ "$waited" -ge "$E2E_HEALTH_TIMEOUT" ]; then
    echo "[test-airflow-e2e] ERROR: health timeout after ${E2E_HEALTH_TIMEOUT}s." >&2
    [ "$scheduler_status" != "healthy" ] && echo "[test-airflow-e2e]   scheduler never became healthy (status=${scheduler_status})" >&2
    [ "$webserver_status" != "healthy" ] && echo "[test-airflow-e2e]   webserver never became healthy (status=${webserver_status})" >&2
    echo "[test-airflow-e2e] --- scheduler log tail ---" >&2
    docker logs --tail 50 "$scheduler_container" >&2 2>&1 || true
    echo "[test-airflow-e2e] --- webserver log tail ---" >&2
    docker logs --tail 50 "$webserver_container" >&2 2>&1 || true
    exit "$EXIT_HEALTH_TIMEOUT"
  fi

  sleep "$E2E_POLL_INTERVAL"
  waited=$((waited + E2E_POLL_INTERVAL))
done

# --- Assert list-import-errors is empty ------------------------------------------
echo "[test-airflow-e2e] stack healthy; checking dags list-import-errors..." >&2
import_errors_json="$(_compose exec -T airflow-scheduler airflow dags list-import-errors --output json 2>/dev/null)"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "[test-airflow-e2e] ERROR: failed to run 'airflow dags list-import-errors' (exit ${rc})." >&2
  exit "$EXIT_INTERNAL_ERROR"
fi

# Trim whitespace/newlines for a robust empty-array check ("[]" possibly with
# surrounding whitespace); avoids a heavier JSON-parsing dependency for this check.
trimmed="$(echo "$import_errors_json" | tr -d '[:space:]')"
if [ "$trimmed" = "[]" ]; then
  echo "[test-airflow-e2e] list-import-errors is empty. PASS." >&2
  exit "$EXIT_SUCCESS"
else
  echo "[test-airflow-e2e] FAIL: list-import-errors reported errors:" >&2
  echo "$import_errors_json" >&2
  exit "$EXIT_IMPORT_ERRORS"
fi
