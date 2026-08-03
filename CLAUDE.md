# Apache Airflow Expert Agent Configuration

## Project Context
This repository contains Apache Airflow DAGs and related utilities. Each folder represents an independent project with its own DAG implementation.

## Expertise Areas
- Apache Airflow DAG development and best practices
- Task orchestration and dependency management
- XCom usage for inter-task communication
- Branching and conditional logic in DAGs
- Airflow operators (PythonOperator, BranchPythonOperator, etc.)
- Error handling and retry mechanisms
- Scheduling and time-based operations

## Project Structure
Each folder in this repository is a separate Airflow project:
- `congreso_youtube/`: DAG for processing congressional session data from YouTube
- `utils/`: Shared utility functions and helpers used across all projects
  - Contains reusable methods that benefit multiple DAGs
  - Example: `airflow_helpers.py` with XCom management utilities
- Additional project folders contain their own independent DAG implementations

## Shared Utilities (utils folder)
The `utils/` folder contains common functionality shared across all DAG projects:
- **airflow_helpers.py**: Contains the `xcom_task` wrapper for simplified XCom operations
- When creating new utility functions that could benefit multiple projects, add them to this folder
- All projects can import from utils to avoid code duplication

## Development Guidelines
1. **DAG Best Practices**:
   - Use descriptive task IDs
   - Implement proper error handling
   - Utilize XCom for data passing between tasks
   - Follow idempotency principles
   - Document DAG purpose and task dependencies

2. **Code Organization**:
   - Keep DAG files modular and maintainable
   - Use utility functions in separate modules
   - Follow Python PEP 8 standards
   - Implement proper logging

3. **Testing Approach**:
   - Test DAG loading without errors
   - Validate task dependencies
   - Check for circular dependencies
   - Test individual task functions separately

## Common Patterns
- Use `xcom_task` utility from `airflow_helpers.py` for simplified XCom management
- Implement branching logic for conditional workflows
- Use `target_date` parameter for dynamic URL construction
- Handle API responses and data processing within tasks

## Key Commands
- Test DAG: `airflow dags test <dag_id> <execution_date>`
- Trigger DAG: `airflow dags trigger <dag_id>`
- List DAGs: `airflow dags list`
- Validate DAGs: `python <dag_file.py>`

## Development Environment
- **Package manager**: uv (no conda; no manual venv activation required)
- **Install / sync dependencies**: `uv sync`  (creates .venv and installs the dev/test group)
- **Run a script**: `uv run python <script.py>`
- **Add a runtime dependency**: `uv add <package>`
- **Add a dev/test dependency**: `uv add --dev <package>`
- If `uv` is not installed, install it (https://docs.astral.sh/uv/) — commands hard-fail with an
  actionable message rather than silently falling back.

## Testing
- **Unit tests**: `uv run pytest`  (runs on every `sdd-verify`)
- **Parallel**: `uv run pytest -n auto`
- **Docker e2e smoke test**: `bash scripts/test-airflow-e2e.sh` — boots an ephemeral Airflow stack and
  asserts `airflow dags list-import-errors` is empty. Runs automatically during `sdd-verify` only when
  the change touches `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or
  `Dockerfile`. If Docker is unavailable it reports `unavailable` (not a failure); run it manually
  before merge in that case.

## Agent skills

### Issue tracker

Issues live in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout — `CONTEXT.md` + `docs/adr/` at repo root. See `docs/agents/domain.md`.