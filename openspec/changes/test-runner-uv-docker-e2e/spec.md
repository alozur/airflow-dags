# SDD Test Runner (uv) & Docker E2E Smoke Test Specification

## Purpose

Replace conda with `uv` as the single tool for dependency management and test execution across this repo (dev environment, docs, and SDD `apply`/`verify` phases), and add a conditional ephemeral Docker Compose e2e smoke test that asserts DAGs parse cleanly (`airflow dags list-import-errors` empty) whenever a change plausibly affects DAG parsing.

## Requirements

### Requirement: uv Replaces Conda for Dependency Management and Test Execution

The system MUST use `uv` exclusively for installing dev/test dependencies and running the unit test suite. No script, doc, or SDD config MAY reference `conda` for environment activation, dependency installation, or test execution after this change.

#### Scenario: Installing dev/test dependencies from a clean checkout

- GIVEN a clean checkout of the repository with `uv` installed and no pre-existing virtual environment
- WHEN a developer or SDD phase runs `uv sync` (with the dev/test dependency group)
- THEN all dependencies required to run the test suite (pytest, pytest-cov, pytest-mock, pytest-xdist, freezegun, coverage[toml]) are installed
- AND no manual `pip install` step is required
- AND no conda environment activation is required

#### Scenario: Running unit tests via uv

- GIVEN dependencies are installed via `uv sync`
- WHEN `sdd-apply` or `sdd-verify` (or a human) runs the configured unit test command (`uv run pytest`, or the exact uv-equivalent decided in design)
- THEN the test suite executes to completion and reports pass/fail results
- AND the command succeeds without conda being present, installed, or activated in the shell

#### Scenario: uv is unavailable in the environment (hard-fail, no silent fallback)

- GIVEN a shell/subagent environment where `uv --version` fails (uv not installed or not on `PATH`)
- WHEN any script or SDD phase attempts to run a uv-based command (`uv sync`, `uv run pytest`, or the e2e script's uv preflight)
- THEN the invocation MUST hard-fail immediately with a non-zero exit code
- AND the error message MUST be actionable (e.g., include an install command or link for `uv`)
- AND the system MUST NOT silently fall back to a bare `python -m pytest` or any other non-uv invocation

### Requirement: requirements-dev.txt Removed in Favor of pyproject.toml Dependency Group

The system MUST define dev/test dependencies exclusively in `pyproject.toml` via a uv-compatible dependency group (e.g., `[dependency-groups]` or equivalent uv-idiomatic mechanism), containing exactly the packages currently listed in `requirements-dev.txt`: `pytest`, `pytest-cov`, `pytest-mock`, `pytest-xdist`, `freezegun`, `coverage[toml]`. `requirements-dev.txt` MUST be deleted once this group exists and is verified to install the equivalent package set.

#### Scenario: pyproject.toml is the single source of truth for dev/test deps

- GIVEN the pyproject.toml dependency group has been added and verified equivalent to the former requirements-dev.txt contents
- WHEN a repository search is performed for `requirements-dev.txt`
- THEN the file does not exist anywhere in the repository
- AND `uv sync` (with the appropriate group flag) installs the same set of packages that `requirements-dev.txt` previously provided

### Requirement: CLAUDE.md Development Environment Section Uses uv, Not Conda

The system MUST document environment setup, package management, and script execution in `CLAUDE.md`'s "Development Environment" section using `uv` commands (`uv sync`, `uv run python <script.py>`, `uv add <package>`), with zero remaining references to `conda` in that section. A short "Testing" note MUST describe `uv run pytest` as the unit test command and describe when the Docker e2e smoke test applies.

#### Scenario: CLAUDE.md contains no conda references in Development Environment

- GIVEN the updated `CLAUDE.md`
- WHEN the "Development Environment" section is inspected
- THEN it contains uv-based setup instructions (`uv sync`, `uv run`, `uv add`)
- AND it contains no occurrence of the string `conda`
- AND it contains a "Testing" note referencing `uv run pytest` and the conditional Docker e2e smoke test trigger

### Requirement: openspec/config.yaml Reflects uv Commands and Conditional E2E Entry

The system MUST update `openspec/config.yaml` so that `rules.apply.test_command`, `rules.verify.test_command`, `testing.runner.command`, and `testing.commands.unit` use `uv run pytest` (or the exact uv-equivalent decided in design) instead of any conda-based command. The system MUST add a `testing.commands.e2e` entry documenting the conditional Docker smoke test: the trigger path globs, the invoked script, and the graceful-degradation behavior when Docker is unavailable.

#### Scenario: Config no longer references conda

- GIVEN the updated `openspec/config.yaml`
- WHEN `rules.apply.test_command`, `rules.verify.test_command`, `testing.runner.command`, and `testing.commands.unit` are inspected
- THEN each references a uv-based command and none reference conda

#### Scenario: Config documents the conditional e2e entry

- GIVEN the updated `openspec/config.yaml`
- WHEN `testing.commands.e2e` is inspected
- THEN it references `scripts/test-airflow-e2e.sh`
- AND it documents the exact trigger path globs: `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, `Dockerfile`
- AND it documents that Docker unavailability produces an `unavailable` status, not a failure

### Requirement: Ephemeral Docker Compose E2E Stack Asserts DAG Parse Health

The system MUST provide a `docker-compose.test.yml` (or equivalently named) file defining an ephemeral, disposable Airflow stack, fully separate from the production `docker-compose.yml`, satisfying all of the following:

- The Airflow image is a lightweight stock Airflow image (`apache/airflow:2.10.2-python3.12`, matching the production Airflow version) with **no** `_PIP_ADDITIONAL_REQUIREMENTS` injection, chosen for fast boot. The smoke test proves DAG *code* parses against Airflow's own bundled providers; it does NOT prove parsing against project-specific heavy dependencies (`yt-dlp`, `openai-whisper`, `google-api-python-client`, etc.), which are absent from this image. This is an accepted, documented limitation — a heavy-dependency-aware e2e is an explicit non-goal of this change (see the Non-Goals requirement).
- Postgres is disposable/ephemeral: no NAS mounts, no external network dependency, no git-sync init containers (`init-dags`/`init-perms`), and no named volumes that persist state across runs (or such volumes are explicitly torn down after the run).
- The DAG folder is bind-mounted from the local checkout rather than fetched via git-sync.
- The stack boots a scheduler and a webserver and the smoke test waits for both to become healthy before proceeding.
- The pass/fail condition is exact: `airflow dags list-import-errors` MUST return an empty result for the run to be considered a pass. Any non-empty output is a failure.
- No task execution occurs — this is a parse-level smoke test only (`airflow dags test` and real task runs are explicitly out of scope).

#### Scenario: Healthy stack with no import errors passes

- GIVEN the ephemeral stack uses the lightweight stock Airflow image and is started with a local DAG mount
- WHEN scheduler and webserver report healthy
- AND `airflow dags list-import-errors` is run
- THEN the output is empty
- AND the smoke test is considered a pass
- AND the stack is torn down with no persisted state remaining

#### Scenario: Import errors present causes a fail

- GIVEN the ephemeral stack is healthy
- WHEN `airflow dags list-import-errors` returns one or more entries
- THEN the smoke test is considered a fail
- AND the stack is torn down

#### Scenario: Production compose/Dockerfile untouched

- GIVEN this change is implemented
- WHEN production `docker-compose.yml` and its runtime `Dockerfile` are diffed against their pre-change state
- THEN they are byte-for-byte unchanged

### Requirement: E2E Image Stubs Heavy Top-Level Imports So the Check Reflects DAG Code Correctness

Because the lightweight e2e image intentionally omits project-specific heavy dependencies, and several importable project modules import those packages at **module top level** (so the import runs at DAG parse time), the system MUST ship minimal no-op **stub modules** for exactly those packages inside the e2e image, mirroring the existing `conftest.py` unit-test stub approach but applied at the container/interpreter level. This ensures `airflow dags list-import-errors` reflects **real DAG/project code correctness** (import wiring, top-level DAG construction) rather than failing purely because a heavy library is absent.

The stub set MUST cover exactly the confirmed heavy packages imported at module top level in project code that are NOT part of Airflow's own bundled dependencies. The confirmed list (verified by repo grep across `utils/**` and `congress_videos/**`) is: `yt_dlp`, `openai`, `PIL` (Pillow), `bs4` (beautifulsoup4), `googleapiclient` (google-api-python-client), `PyPDF2`, `google.auth`/`google.oauth2` (google-auth), and `numpy`. Packages imported only **lazily inside functions** (`whisper`, `webrtcvad`, `pytubefix`, `moviepy`, `dotenv`) do not run at parse time and are therefore not required for parse correctness; `pytubefix` and `whisper` MAY still be stubbed for parity with `conftest.py`. Packages that Airflow's own base image already provides (`requests`, `urllib3`, `psycopg2`) MUST NOT be stubbed.

The stub mechanism MUST NOT shadow a real namespace package: for `google.auth`/`google.oauth2` the stub MUST register only those submodules (leaving any real `google` namespace, e.g. `google.protobuf`, intact), matching how `conftest.py` injects into `sys.modules` rather than placing shadowing files on the path.

#### Scenario: Heavy top-level imports resolve to stubs so parse succeeds

- GIVEN the lightweight e2e image with no `_PIP_ADDITIONAL_REQUIREMENTS`
- AND the e2e image ships stub modules for the confirmed heavy top-level-imported packages (`yt_dlp`, `openai`, `PIL`, `bs4`, `googleapiclient`, `PyPDF2`, `google.auth`/`google.oauth2`, `numpy`)
- WHEN Airflow parses the DAG folder and `airflow dags list-import-errors` runs
- THEN no import error is reported that is caused purely by one of those heavy packages being absent
- AND any import error that IS reported reflects a real defect in DAG/project code (bad import path, syntax error, top-level exception), not a missing heavy library

#### Scenario: A new unstubbed heavy top-level import is an accepted known limitation

- GIVEN a future DAG or module adds a **new** module-top-level `import` of a heavy package that is not yet in the e2e stub set and is absent from the lightweight image
- WHEN `airflow dags list-import-errors` runs and reports a `ModuleNotFoundError` for that package
- THEN this is a **documented, accepted limitation** of the lightweight-image design (the stub set needs a new entry added), NOT a defect in this change
- AND the remediation is to add a stub entry for that package (keeping the e2e stub set a superset of the `conftest.py` stub set), not to install the real heavy dependency in the e2e image

### Requirement: scripts/test-airflow-e2e.sh Drives the E2E Smoke Test with Deterministic Exit Codes

The system MUST provide `scripts/test-airflow-e2e.sh`, callable both by developers locally and by `sdd-verify`, that builds/starts the ephemeral stack defined by `docker-compose.test.yml`, polls for health with a bounded (non-infinite) timeout, runs `airflow dags list-import-errors`, asserts the result is empty, tears the stack down (regardless of outcome), and exits with one of the following deterministic, distinguishable exit codes:

- **Success**: DAGs parse cleanly, stack healthy, `list-import-errors` empty → exit code `0`.
- **Import errors found**: stack healthy but `list-import-errors` returns non-empty → non-zero exit code distinct from the health-timeout case.
- **Health timeout**: scheduler/webserver never reach healthy state within the bounded timeout → non-zero exit code distinct from the import-errors case, with a clear diagnostic message (not an infinite poll).
- **Docker unavailable**: Docker (or Docker Compose) is not available in the environment → the script exits gracefully reporting an `unavailable` status. This condition MUST be distinguishable from a real failure (it MUST NOT reuse the same exit code as import-errors-found or health-timeout), consistent with the graceful-degradation decision — Docker being absent is not treated as a test failure.
- **uv unavailable**: if the script depends on `uv` for any preflight step, a missing `uv` MUST hard-fail with an actionable error message (per the uv hard-fail requirement above), using a distinct exit code from the Docker-unavailable case.

#### Scenario: Docker unavailable reports unavailable, not failure

- GIVEN Docker is not installed or not running in the current environment
- WHEN `scripts/test-airflow-e2e.sh` is invoked
- THEN it exits with the designated "unavailable" exit code (distinct from failure exit codes)
- AND it does not attempt to build or start containers
- AND unit tests run via uv are unaffected and still report their own pass/fail

#### Scenario: Health timeout produces bounded, diagnosable failure

- GIVEN Docker is available and the stack is started
- WHEN scheduler and/or webserver do not become healthy within the script's bounded timeout
- THEN the script exits with the designated health-timeout exit code
- AND prints a clear diagnostic message identifying which service failed to become healthy
- AND tears down the stack before exiting

#### Scenario: uv missing during e2e preflight hard-fails

- GIVEN the e2e script performs a `uv` preflight check and `uv --version` fails
- WHEN the script is invoked
- THEN it exits immediately with an actionable error message and a distinct non-zero exit code
- AND does not attempt to start Docker containers

### Requirement: Conditional Trigger — E2E Runs Only for DAG-Relevant Path Changes

The system MUST invoke `scripts/test-airflow-e2e.sh` during `sdd-verify` if and only if the change's diff touches at least one path matching: `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or `Dockerfile`. This glob list MUST NOT be expanded (e.g., no addition of `pyproject.toml` or `tests/**`) as part of this change. Unit tests via `uv run pytest` run on every `sdd-verify` regardless of changed paths.

#### Scenario: DAG-touching change triggers e2e

- GIVEN a change whose diff includes a file under `congress_videos/**`
- WHEN `sdd-verify` runs
- THEN unit tests run via `uv run pytest`
- AND `scripts/test-airflow-e2e.sh` is also invoked
- AND the verify result reflects the e2e outcome (pass/fail/unavailable)

#### Scenario: Non-DAG-touching change skips e2e

- GIVEN a change whose diff only includes files under `docs/**`
- WHEN `sdd-verify` runs
- THEN unit tests run via `uv run pytest`
- AND `scripts/test-airflow-e2e.sh` is NOT invoked

#### Scenario: Indirect dependency-driven breakage is a known, accepted limitation

- GIVEN a change that breaks DAG parsing indirectly (e.g., a shared dependency bump) without touching any of the listed glob paths
- WHEN `sdd-verify` runs
- THEN the e2e step is skipped by design
- AND this is a documented, accepted limitation of this slice, not a defect

### Requirement: Explicit Non-Goals Are Acceptance Boundaries

The system MUST NOT, as part of implementing this change:

- Add or modify any CI workflow (e.g., `.github/workflows/github_sync_main_development.yml` remains untouched in behavior).
- Modify the production `docker-compose.yml` or its runtime `Dockerfile` (NAS mounts, `postgres_infra_network`, `whisper_network`, git-sync `init-dags`/`init-perms` semantics stay untouched).
- Execute tasks at the e2e level — only `airflow dags list-import-errors` (parse-level) is asserted; `airflow dags test` and real task runs remain out of scope.
- Provide **real** heavy-dependency (project-specific package) import parity in the e2e image — the e2e uses a lightweight stock Airflow image with no `_PIP_ADDITIONAL_REQUIREMENTS` and ships **no-op stubs** (not the real libraries) for heavy top-level imports; validating behavior against the *real* `yt-dlp`, `openai-whisper`, `google-api-python-client`, and similar heavy packages is an explicit future follow-up, not part of this change. The stubs make the parse check meaningful; they do not add real heavy-dependency coverage.
- Change lint, type-checking, or coverage threshold configuration (`--cov-fail-under=80` in `pyproject.toml` stays as-is).

#### Scenario: No CI workflow changes

- GIVEN this change is implemented
- WHEN `.github/workflows/` is inspected
- THEN no new or modified workflow files exist as part of this change

#### Scenario: No coverage/lint config drift

- GIVEN this change is implemented
- WHEN `pyproject.toml`'s coverage/lint configuration is inspected (outside the new dependency group)
- THEN `--cov-fail-under=80` and existing lint/type-check configuration are unchanged

## Open questions for design

The following are under-specified in the proposal and are intentionally deferred to `sdd-design`/`sdd-tasks` rather than resolved here, per instruction not to invent new requirements:

1. **Exact uv dependency-group syntax**: whether the dev/test group uses `[dependency-groups]` (PEP 735 / uv-native) or `[project.optional-dependencies]`, and the exact `uv sync`/`uv run` invocation flags (e.g., `--group test` vs `--extra test`). The proposal explicitly leaves this to design/tasks.
2. **Exact distinct exit code values** for `scripts/test-airflow-e2e.sh` (success / import-errors-found / health-timeout / docker-unavailable / uv-unavailable) — this spec requires them to be *distinct and deterministic* but does not assign numeric values; design should fix concrete codes.
3. **Bounded timeout duration** for the health-wait polling loop — this spec requires "bounded, generous, non-infinite" but the concrete seconds/retries value is a design/tasks decision.
4. **Path-diff detection mechanism** for the conditional trigger (e.g., `git diff --name-only` against which base ref, invoked from where in the `sdd-verify` flow) — the glob list itself is locked, but the exact diffing implementation is a design decision.
5. **Named volume teardown mechanism** for `docker-compose.test.yml` (e.g., `docker compose down -v` vs no named volumes at all) — proposal requires "disposable state" but not the specific compose-file mechanism.
