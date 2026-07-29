# Proposal: test-runner-uv-docker-e2e

## Intent

Fix a broken SDD test loop and harden pre-merge safety for DAG changes:

1. **Replace conda with uv** everywhere in this repo (dev, test, docs) so `sdd-apply`/`sdd-verify` (and any human) can run tests with a single, self-contained command that does not depend on conda being activated or even installed in the shell.
2. **Add a Docker-based e2e smoke test** that boots an ephemeral Airflow stack and asserts DAGs parse without import errors, running only when the diff plausibly affects DAG parsing.

## Problem (current-state gap)

- `sdd-init` recorded the canonical test command as `conda run -n airflow python -m pytest`. In practice, subagent shells frequently do not have conda initialized on `PATH`, so the command hangs or fails to start — not a test failure, a **launcher failure**. This has been silently blocking `sdd-apply`/`sdd-verify` from ever completing.
- There is no automated signal that a DAG actually *parses* in a real Airflow environment. Unit tests mock Airflow internals (`mock_task_instance`, stubbed `yt_dlp`/`whisper`/`pytubefix`) — they do not catch import errors, bad top-level DAG code, or scheduler-level parse failures that only show up when Airflow itself loads the DAG folder.
- `requirements-dev.txt` + implicit conda env is not `uv`-native; there's no `pyproject.toml` dependency group uv can sync from today.

## Decisions (confirmed with user, not open for re-litigation in this proposal)

1. **Full conda replacement with uv.** Drop conda from `CLAUDE.md`, docs, and scripts. uv becomes the single tool for env management and test execution (`uv sync`, `uv run pytest`).
2. **New ephemeral Docker Compose e2e smoke test** (`docker-compose.test.yml` or similar), fully separate from the production `docker-compose.yml`:
   - Own disposable Postgres (no NAS, no external network, no git-sync init containers).
   - Local DAG folder bind-mount instead of the `git_dags`/`init-dags` git-sync flow.
   - Boots scheduler + webserver, waits for health, then asserts `airflow dags list-import-errors` returns empty.
   - No task execution — parse-level smoke test only.
3. **Conditional trigger.** Unit tests via `uv run pytest` run on every `sdd-verify`. The Docker e2e smoke test only runs when the diff touches `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or `Dockerfile`.
4. **Graceful degradation.** If Docker is unavailable in the subagent's environment, unit tests still run via uv; the e2e step reports status `unavailable` (not a failure). A human decides whether to run it manually before merge.

## Scope

### In scope
- **SDD testing configuration**: update `openspec/config.yaml` (`rules.apply.test_command`, `rules.verify.test_command`, `testing.runner.command`, `testing.commands.unit`, and add an `testing.commands.e2e` entry describing the conditional Docker smoke test) to point at `uv run pytest` and describe the conditional e2e step. Update the engram `sdd-init/airflow-dags` observation (topic_key `sdd-init/airflow-dags`) to replace the canonical/env-active test commands with uv equivalents and record the new conditional e2e capability, superseding the conda-only "Learned" note.
- **`CLAUDE.md`**: rewrite the "Development Environment" section to use uv instead of conda (`uv sync`, `uv run python <script.py>`, `uv add <package>`), and add a short "Testing" note describing `uv run pytest` plus when the Docker e2e smoke test applies.
- **`pyproject.toml`**: add `[tool.uv]`/`[dependency-groups]` (or `[project.optional-dependencies]`, whichever is uv-idiomatic) `dev`/`test` group with the exact packages currently in `requirements-dev.txt` (`pytest`, `pytest-cov`, `pytest-mock`, `pytest-xdist`, `freezegun`, `coverage[toml]`), so `uv sync --group test` (or equivalent) works standalone. `requirements-dev.txt` can be kept as a thin re-export or removed — decide during design/tasks, not here.
- **New `docker-compose.test.yml`**: ephemeral Postgres + Airflow scheduler + webserver, local DAG folder mount, no NAS/external network/git-sync dependency, disposable state (no named volumes that persist across runs, or volumes explicitly torn down).
- **New `scripts/test-airflow-e2e.sh`**: builds/starts the ephemeral stack, polls for health, runs `airflow dags list-import-errors` and asserts it is empty, tears the stack down, exits non-zero on any failure (including timeout). Callable both by developers locally and by `sdd-verify`.
- **Path-based conditional logic**: a documented rule (in the e2e script or the SDD config) that `sdd-verify` only invokes `scripts/test-airflow-e2e.sh` when the change diff touches `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or `Dockerfile`.

### Explicit non-goals
- No CI workflow changes — the repo currently has no test CI (`.github/workflows/github_sync_main_development.yml` only syncs main→dev branches); adding CI is out of scope.
- No changes to the **production** `docker-compose.yml` or its runtime `Dockerfile` build — NAS mounts, `postgres_infra_network`, `whisper_network`, and git-sync (`init-dags`/`init-perms`) semantics stay untouched.
- No task-level e2e execution — only DAG *parsing* (`list-import-errors`) is asserted, never `airflow dags test` or real task runs.
- No enforcement/config changes to lint, type-checking, or coverage thresholds — those stay as currently configured (`--cov-fail-under=80` in `pyproject.toml` stays as-is).

## Affected areas

| Area | Change |
|---|---|
| `openspec/config.yaml` | test_command → uv, add conditional e2e entry |
| Engram `sdd-init/airflow-dags` | superseding update: uv commands, e2e capability |
| `CLAUDE.md` | Development Environment section rewritten for uv |
| `pyproject.toml` | new uv dependency group for dev/test deps |
| `requirements-dev.txt` | kept or retired (design/tasks decision) |
| `docker-compose.test.yml` (new) | ephemeral e2e Airflow stack |
| `scripts/test-airflow-e2e.sh` (new) | smoke-test driver script |
| `sdd-apply` / `sdd-verify` future runs | now use uv + conditional Docker step |

Out of scope / untouched: production `docker-compose.yml`, production `Dockerfile`, CI workflows, DAG business logic, coverage/lint configuration.

## Risks

- **uv not installed in subagent/dev environments either** — same class of failure as conda, just with a different tool. Mitigation: design/tasks must specify a preflight check (`uv --version`) and a clear, actionable error if uv itself is missing, rather than a silent hang.
- **Docker unavailable or slow in ephemeral/sandboxed subagent environments** — mitigated explicitly by decision #4 (graceful `unavailable` degradation instead of hard failure), but this means the e2e check does not universally gate merges; a human must still remember to run it manually when the automated check reports `unavailable`.
- **Health-check flakiness** — Airflow webserver/scheduler startup + DB migration can be slow or flaky in CI-less ephemeral environments; the smoke test script needs a bounded, generous timeout and clear failure diagnostics (not an infinite poll) to avoid recreating the original "stuck forever" problem in a new form.
- **Lightweight image gives narrow parity (accepted, known limitation)** — `docker-compose.test.yml` uses a stock `apache/airflow:2.10.2-python3.12` image with **no** `_PIP_ADDITIONAL_REQUIREMENTS`. The smoke test therefore proves only that DAG *code* parses against Airflow's own bundled providers; it does **not** prove parsing against project-specific heavy dependencies (`yt-dlp`, `openai-whisper`, `google-api-python-client`, etc.), which are absent from the lightweight image. This is a deliberate, documented tradeoff (fast boot over full parity), **explicitly recorded as a known limitation, not silently assumed** — a heavy-dependency-aware e2e is an explicit future follow-up. **Consequence — RESOLVED (design Note N7):** because several importable modules import heavy packages at module top level, the e2e image ships **minimal no-op stub modules** for exactly those packages (the same testing philosophy as the existing `conftest.py` unit-test stubs for `yt_dlp`/`whisper`/`pytubefix`, applied at the container level instead of the pytest-process level), so `airflow dags list-import-errors` reflects real DAG *code* correctness rather than failing purely on absent heavy deps.
- **Path-matching false negatives** — the "touches DAG files" trigger is glob-based; a change that indirectly breaks DAG parsing without touching the listed globs (e.g., a shared dependency bump) would skip the e2e check. Acceptable for this slice per user decision on scope, but should be called out as a known limitation, not silently assumed complete coverage.
- **Migration of existing test command** — engram note update must clearly supersede, not just append to, the old conda-based "Learned" section, or future SDD phases may still see stale conda guidance mixed with the new uv guidance.

## Rollback

- All changes are additive/config-level except the `CLAUDE.md` and `pyproject.toml` edits, which are plain diffs revertible via git.
- If uv-based testing proves worse than conda (e.g., unexpected uv unavailability), revert `openspec/config.yaml` test_command and the engram `sdd-init/airflow-dags` note to the conda commands; `CLAUDE.md`/`pyproject.toml` changes can be reverted independently since they don't hard-couple to SDD config.
- The Docker e2e smoke test is purely additive and optional (conditional trigger + graceful degradation) — disabling it is as simple as removing the conditional trigger from `openspec/config.yaml`/the script invocation point, with zero impact on unit-test-only verification.
- No production infrastructure (docker-compose.yml, Dockerfile, NAS mounts) is touched, so there is no production rollback surface.

## Success criteria

1. `sdd-apply`/`sdd-verify` can run unit tests to completion via `uv run pytest` (or the exact uv-equivalent command decided in design) without depending on conda being present or activated.
2. `uv sync` (from a clean checkout, uv installed) succeeds in installing the dev/test dependency group with no manual `pip install` step.
3. `CLAUDE.md` no longer references conda in the Development Environment section; uv commands are documented and accurate.
4. A DAG-touching change (in `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or `Dockerfile`) triggers `scripts/test-airflow-e2e.sh`, which boots the ephemeral stack, asserts `airflow dags list-import-errors` is empty, and tears down cleanly, exiting non-zero if import errors exist or the stack never becomes healthy.
5. A non-DAG-touching change (e.g., only `docs/**`) does NOT trigger the Docker e2e step.
6. When Docker is unavailable, the verify flow still completes with unit-test results and a clearly labeled `unavailable` e2e status — never a hang, never a false PASS, never a hard failure that blocks unrelated non-DAG changes.
7. Production `docker-compose.yml` and `Dockerfile` are byte-for-byte unchanged by this proposal's implementation.

## Proposal question round — resolved

The user reviewed all four open gaps and confirmed the recommended answer in each case. These are now decisions, not assumptions:

1. **Dependency group shape**: `requirements-dev.txt` is **deleted outright** once `pyproject.toml` has the uv dependency group. `pyproject.toml` + `uv.lock` become the single source of truth for dev/test dependencies.
2. **uv availability fallback**: if `uv --version` fails, `sdd-apply`/`sdd-verify` (and the e2e script) **hard-fail with an actionable error message** (e.g. an install link/command). No silent fallback to a bare `python -m pytest` — consistent with the root-cause philosophy of this proposal: don't hide launcher failures.
3. **Test image provenance**: `docker-compose.test.yml` uses a **lightweight stock Airflow image** (`apache/airflow:2.10.2-python3.12`, matching the production Airflow version pinned in `Dockerfile`) with **no `_PIP_ADDITIONAL_REQUIREMENTS` injection** and a fast boot. **Revised after design review of Note N1:** true parity would require replicating the production `_PIP_ADDITIONAL_REQUIREMENTS` (torch/whisper/yt-dlp installed at every container boot, ~15 min per e2e run — the reason Q3 had a 900s timeout). The user reviewed that cost and **deliberately chose the lightweight image instead**. **Accepted, known limitation:** the smoke test proves DAG *code* parses against Airflow's own bundled providers only — it does **not** prove parsing against the project-specific heavy dependencies (`yt-dlp`, `openai-whisper`, `google-api-python-client`, etc.), which are absent from the lightweight image. A heavy-dependency-aware e2e is an explicit future follow-up, not part of this slice. This is a deliberate, informed tradeoff, not scope creep.
   - **Stub modules for heavy top-level imports (resolves design Note N7):** so the parse-level check does not fail purely because a heavy package is absent, the lightweight e2e image ships **minimal no-op stub modules** for every package that is imported at **module top level** in project code and is not part of Airflow's own bundled dependencies. This mirrors the existing `conftest.py` unit-test stubs (`yt_dlp`/`whisper`/`pytubefix`) — the **same testing philosophy applied one layer down**: at the container/interpreter level instead of inside the pytest process. The smoke test therefore validates DAG/project *code* parsing (real import wiring, top-level DAG construction), **not** the behavior of the real heavy libraries. A *new* top-level import of an as-yet-unstubbed heavy package appearing in a future DAG is an **accepted known limitation** (it would need a stub entry added), not a defect in this design. The exact confirmed package list and the wiring mechanism are fixed in `design.md` (Note N7 / “Stub modules for e2e”).
4. **Path-trigger scope**: the glob list stays **exactly as originally specified** (`congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, `Dockerfile`) — no expansion to `pyproject.toml` or `tests/**` conftest changes. The false-negative risk for indirect dependency-driven breakage remains a documented, accepted limitation of this slice.

All four are locked inputs for `sdd-spec`/`sdd-design`; no further confirmation needed before proceeding.
