# Tasks: test-runner-uv-docker-e2e

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~480-560 (excl. generated `uv.lock`), ~700-1000+ incl. `uv.lock` |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (uv migration: T1-T3) → PR 2 (e2e infra: T4-T7) → PR 3 (wiring + validation: T8-T9) |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending — flag for orchestrator/user decision before apply |

```text
Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High
```

### Per-task line estimate

| Task | Files | Est. changed lines |
|------|-------|---------------------|
| T1 | `pyproject.toml`, `uv.lock` (generated) | ~15 (+ generated lockfile, not counted at normal review weight but present in diff) |
| T2 | `requirements-dev.txt` (delete, ~7 lines), stray doc refs | ~10-15 |
| T3 | `CLAUDE.md`, `openspec/config.yaml`, engram `sdd-init/airflow-dags` | ~60-70 (file diffs only; engram update has no file-line cost) |
| T4 | `e2e/stubs/sitecustomize.py` (new, ~40), sanity check evidence (~20-30) | ~60-70 |
| T5 | `docker-compose.test.yml` (new) | ~85-95 |
| T6 | `scripts/dag-paths-changed.sh` (new, ~25), shell test/checklist (~20) | ~45 |
| T7 | `scripts/test-airflow-e2e.sh` (new, ~150-200), shell test/checklist (~20) | ~170-220 |
| T8 | `openspec/config.yaml` consistency pass (delta over T3), `CLAUDE.md` cross-check | ~10-15 |
| T9 | Evidence log (this file's completion notes / apply-progress), no new source files | ~0-10 |

`uv.lock` is machine-generated and not meaningfully human-reviewable line-by-line, but it still lands in the PR diff — call this out explicitly to reviewers so it isn't counted against the 400-line human-review budget in the same way as hand-written code.

Given the total materially exceeds 400 hand-written changed lines even before `uv.lock`, and this change has no natural single-file boundary, **chained PRs are recommended**. Chain strategy (`stacked-to-main` vs `feature-branch-chain`) is intentionally left `pending` — this is a decision for the orchestrator/user, not this phase, per the Review Workload Guard.

---

## Task Ownership Legend

- `<!-- sdd-owner: implementation -->` — RED/GREEN/TRIANGULATE/REFACTOR, code, tests, apply-owned verification.
- `<!-- sdd-owner: parent -->` — bounded review start/reuse and lifecycle-gate actions only.

Strict TDD is active for this project (`test_command: pytest`, `uv run pytest` once T1 lands). Most of this change is infra/config (shell scripts, YAML, Markdown) without pre-existing pytest coverage. Per-task test-first guidance below states explicitly what "RED first" means for each artifact type: a real pytest RED/GREEN cycle where Python behavior exists (the stub module), and a documented manual verification checklist (or a minimal `bats`/shell assertion script if `bats` is available) for shell scripts and compose files where no test harness currently exists.

---

## PR 1 — uv Migration (T1-T3)

### T1 — Migrate dependency management to uv

**Files:** `pyproject.toml`, `uv.lock` (new, generated)

**Description:**
- Add `[dependency-groups] dev = [...]` with exactly the 6 packages from `requirements-dev.txt` (`pytest>=7.4,<8.0`, `pytest-cov>=4.1,<5.0`, `pytest-mock>=3.12,<4.0`, `pytest-xdist>=3.5,<4.0`, `freezegun>=1.4,<2.0`, `coverage[toml]>=7.4,<8.0`).
- Add `[tool.uv] package = false`.
- Do NOT touch existing pytest/coverage config (`--cov-fail-under=80` stays untouched).
- Run `uv sync` to generate `uv.lock`; commit it.
- Run `uv run pytest` and confirm the full existing suite passes exactly as it did under the previous (conda) invocation — same pass count, no new failures.

**Dependencies:** none (first task).

**Test-first guidance:** No new application behavior — this is dependency config. Test-first here means: **run the existing pytest suite BEFORE and AFTER the change** and diff pass/fail counts (RED = suite fails to even start via `uv run pytest` before `[tool.uv] package = false` is added, because uv will try to build the unpackaged project; GREEN = suite runs and passes identically after the fix).

**Definition of Done (spec: "Installing dev/test dependencies from a clean checkout", "Running unit tests via uv"):**
- [x] Add `[dependency-groups] dev` (6 exact pinned packages) and `[tool.uv] package = false` to `pyproject.toml`. <!-- sdd-owner: implementation -->
- [x] Run `uv sync` from a clean state and confirm it succeeds with no manual `pip install` step; commit generated `uv.lock`. <!-- sdd-owner: implementation -->
- [x] Run `uv run pytest` and confirm it completes with the same pass/fail outcome as the pre-change conda-based run (record before/after counts as evidence). <!-- sdd-owner: implementation -->

> **Correction note (post-apply):** the original T1 evidence incorrectly called 11 collection errors "pre-existing/unrelated"; they were a real regression because `[dependency-groups] dev` only covered test tooling, not runtime imports (`numpy`, `psycopg2`, `google-auth`, `apache-airflow`, etc.). Fixed by adding a `[project.dependencies]` runtime dependency list to `pyproject.toml` (union of `congress_videos/requirements.txt` + docker-compose `_PIP_ADDITIONAL_REQUIREMENTS` + discovered gaps, excluding `openai-whisper`). See `apply-progress.md` CORRECTION section for full root cause, fix, and verification evidence.

> **Second correction note (post-apply, this batch):** fixing the runtime-dependency gap above unmasked two pre-existing test-file bugs that the old conda setup was hiding. (1) `tests/congress_videos/modules/test_postgres_operators.py` / `test_postgres_operators_extended.py` unconditionally overwrote `sys.modules["airflow.utils.decorators"]` with a fake stub, poisoning the real installed `apache-airflow` for the rest of the pytest process (49 errors, ~114 cascading failures). Fixed by deleting the shim entirely — real Airflow 2.10.2 already provides `apply_defaults`. (2) 5 tests asserted `dag.schedule` (Airflow 3.x-only API); production is pinned to Airflow 2.10.2, so fixed to `dag.schedule_interval` (the 2.10.2-equivalent, verified against each DAG's actual value). `uv run pytest` (full suite) is now genuinely green: **1153 passed, 1 skipped, 0 failed, 0 errors**, coverage **83.58%** (clears the 80% `--cov-fail-under` gate). See `apply-progress.md` for full before/after evidence.

---

### T2 — Remove requirements-dev.txt and stray references

**Files:** `requirements-dev.txt` (delete), any doc/script referencing it (found via grep)

**Description:**
- Grep the repo for `requirements-dev.txt` (Note N5) across docs, scripts, Makefile, README, CLAUDE.md.
- Update every stray reference found to point at `pyproject.toml` `[dependency-groups] dev` / `uv sync` instead.
- Delete `requirements-dev.txt`.

**Dependencies:** T1 (dev group must exist and be verified equivalent before deleting the old source).

**Test-first guidance:** Not applicable as pytest RED/GREEN (no runtime behavior). Test-first here means: **write the grep command and run it BEFORE deleting the file**, capturing its output as the "before" evidence, then re-run the same grep AFTER deletion and confirm zero remaining matches (except the lockfile/pyproject description of the equivalent group) — this is the manual verification checklist for this task.

**Definition of Done (spec: "requirements-dev.txt Removed in Favor of pyproject.toml Dependency Group"):**
- [x] Grep repo for `requirements-dev.txt` and record all hits (docs, scripts, other config). <!-- sdd-owner: implementation -->
- [x] Update every stray reference to the uv-based equivalent. <!-- sdd-owner: implementation -->
- [x] Delete `requirements-dev.txt`; re-run the grep and confirm the file no longer exists anywhere in the repo. <!-- sdd-owner: implementation -->

---

### T3 — Rewrite CLAUDE.md, openspec/config.yaml, and engram sdd-init note for uv

**Files:** `CLAUDE.md`, `openspec/config.yaml`, engram observation `sdd-init/airflow-dags`

**Description:**
- Rewrite `CLAUDE.md`'s "Development Environment" section per design's exact command set (`uv sync`, `uv run python <script.py>`, `uv add <package>`, `uv add --dev <package>`), zero remaining `conda` references.
- Add the "Testing" note block exactly as specified in design (unit via `uv run pytest`, e2e conditional trigger description).
- Update `openspec/config.yaml` to the **exact YAML shape** from design: `rules.apply.test_command`, `rules.verify.test_command` → `"uv run pytest"`; `testing.runner.command` → `"uv run pytest"`; `testing.layers.unit`/`testing.commands.unit` → uv; add `testing.layers.e2e` and `testing.commands.e2e` with `scope`, `command`, `framework`, `gate_command`, `trigger_globs` (5 exact globs), `unavailable_exit_code: 4`, `notes`.
- Update the engram observation at topic_key `sdd-init/airflow-dags`: **supersede** (do not append) the conda-based `test_command` guidance with the uv equivalent, and record the new conditional e2e capability (script, gate script, trigger globs, exit-code contract, `unavailable` semantics).

**Dependencies:** T1 (uv commands must be verified working before documenting/config-wiring them).

**Test-first guidance:** Documentation/config-only; no pytest RED/GREEN applies. Manual verification checklist: (1) grep `CLAUDE.md` for the string `conda` — expect zero hits in the Development Environment section after the change; (2) diff `openspec/config.yaml` against the design's YAML block to confirm an exact structural match; (3) read back the updated engram observation and confirm the old conda "Learned" note text is replaced, not present alongside the new note.

**Definition of Done (spec: "CLAUDE.md Development Environment Section Uses uv, Not Conda"; "openspec/config.yaml Reflects uv Commands and Conditional E2E Entry"):**
- [x] Rewrite `CLAUDE.md` Development Environment + add Testing note; grep confirms zero `conda` occurrences in that section. <!-- sdd-owner: implementation -->
- [x] Update `openspec/config.yaml` to the exact design YAML shape (test commands, e2e entry with globs/gate_command/unavailable_exit_code/notes). <!-- sdd-owner: implementation -->
- [x] Update engram `sdd-init/airflow-dags` observation, explicitly superseding the conda note (not appending). <!-- sdd-owner: implementation -->

---

## PR 2 — E2E Infrastructure (T4-T7)

### T4 — e2e/stubs/sitecustomize.py (N7 heavy-import stub mechanism)

**Files:** `e2e/stubs/sitecustomize.py` (new)

**Description:**
- Implement the stub module per design's confirmed grep-verified package list: `yt_dlp`, `openai`, `PIL` (`Image`, `ImageDraw`, `ImageFont`, `ImageEnhance`), `bs4` (`BeautifulSoup`), `googleapiclient`/`googleapiclient.discovery`/`googleapiclient.http`, `PyPDF2` (`PdfReader`), `numpy`, `google.auth`/`google.auth.transport.requests`/`google.oauth2`/`google.oauth2.credentials` (submodules only — never a fake top-level `google`), plus `pytubefix`/`whisper` for parity with `conftest.py`.
- Use `sys.modules.setdefault`-style injection via a shared `_stub(name, **attrs)` helper (never clobber a real installed package), matching `conftest.py`'s existing approach.

**Dependencies:** none within PR2 (independent of T5-T7, but logically precedes them since compose mounts it).

**Test-first guidance:** This IS real Python behavior with existing pytest infrastructure available — write a RED test first.
- RED: add a small pytest test (e.g. `tests/e2e/test_sitecustomize_stubs.py`) that imports `e2e/stubs/sitecustomize.py` directly (add `e2e/stubs` to `sys.path` in the test) and asserts each confirmed heavy package name is importable afterward (`import yt_dlp`, `import openai`, etc.) and that `google.protobuf`-style real namespace packages are NOT shadowed (assert `google.__path__` still resolves to more than just the stub, or assert no `google/__init__.py` file was created).
- GREEN: implement `sitecustomize.py` until the test passes.
- TRIANGULATE: add a case for a package NOT in the stub list (e.g. `requests`) and assert the real package still imports normally (proves the stub mechanism doesn't over-shadow).
- Additionally, manually sanity-check locally: run `python -c "import sys; sys.path.insert(0,'e2e/stubs'); import sitecustomize"` followed by a normal `uv run pytest` pass, confirming the stub module does not interfere with the existing unit-test suite when not on `PYTHONPATH` for pytest.

**Definition of Done (spec: "E2E Image Stubs Heavy Top-Level Imports So the Check Reflects DAG Code Correctness"):**
- [x] RED: write `tests/e2e/test_sitecustomize_stubs.py` asserting all 8 confirmed heavy packages become importable after loading the stub module, and that `google` namespace is not shadowed. <!-- sdd-owner: implementation -->
- [x] GREEN: implement `e2e/stubs/sitecustomize.py` with the `_stub()` helper and exact package list from design; test passes. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: add a non-stubbed package (`requests`) import assertion to confirm no over-shadowing. <!-- sdd-owner: implementation -->
- [x] Manually confirm `uv run pytest` (without `e2e/stubs` on `PYTHONPATH`) still passes unaffected. <!-- sdd-owner: implementation -->

---

### T5 — docker-compose.test.yml (ephemeral e2e stack)

**Files:** `docker-compose.test.yml` (new)

**Description:** Implement exactly per design's structure:
- `apache/airflow:2.10.2-python3.12` stock image, no `_PIP_ADDITIONAL_REQUIREMENTS`.
- Disposable Postgres via `tmpfs`, no persisted named volumes (anonymous `airflow_logs` only, removed by `down -v`).
- `-p airflow-dags-e2e` isolated project name (enforced at invocation time in T7, not in the compose file itself).
- Local DAG folder bind-mount (`./:/opt/airflow/dags:ro`), no NAS/external network/git-sync.
- Stub mount `./e2e/stubs:/opt/airflow/stubs:ro` + `PYTHONPATH=/opt/airflow/stubs`.
- `airflow-init`, `airflow-scheduler`, `airflow-webserver` services with healthchecks (`start_period: 60s`), throwaway Fernet key and test env vars neutralizing DAG-read env vars (`OPENAI_API_KEY`, `YOUTUBE_API_KEY`, `REAP_API_KEY`, Postgres creds).

**Dependencies:** T4 (stub mount path must exist for the compose file to be meaningful, though the YAML can be authored in parallel and validated together).

**Test-first guidance:** No pytest applies to a YAML file. Document a **manual verification checklist** (this task's "test"):
1. `docker compose -p airflow-dags-e2e -f docker-compose.test.yml config` validates the file (catches YAML/schema errors) — run this as the RED/GREEN proxy: RED = `config` fails before file is complete, GREEN = `config` succeeds.
2. `docker compose -p airflow-dags-e2e -f docker-compose.test.yml up -d` boots without error.
3. `docker compose -p airflow-dags-e2e -f docker-compose.test.yml down -v --remove-orphans` leaves no residual containers/volumes (`docker volume ls`, `docker ps -a` show nothing from this project).
4. Confirm production `docker-compose.yml` and `docker-compose.prod.yml` are byte-for-byte unchanged (`git diff` shows no hits).

**Definition of Done (spec: "Ephemeral Docker Compose E2E Stack Asserts DAG Parse Health"; "Production compose/Dockerfile untouched"):**
- [x] Author `docker-compose.test.yml` per design's exact structure (image, env, mounts, healthchecks, tmpfs postgres, anonymous logs volume). <!-- sdd-owner: implementation -->
- [x] `docker compose config` validates the file with no errors. <!-- sdd-owner: implementation -->
- [x] Manually boot + tear down the stack once; confirm no residual containers/volumes/networks remain (checklist above). <!-- sdd-owner: implementation -->
- [x] Confirm `docker-compose.yml`/`docker-compose.prod.yml`/production `Dockerfile` are unchanged via `git diff`. <!-- sdd-owner: implementation -->

---

### T6 — scripts/dag-paths-changed.sh (diff-detection gate)

**Files:** `scripts/dag-paths-changed.sh` (new)

**Description:** Implement exactly per design Q4: `BASE_REF` resolution (`origin/dev` → `origin/main` → `HEAD~1` fallback chain), `git merge-base`, locked extended-regex pattern (`^(congress_videos/|examples/|utils/|docker-compose[^/]*\.yml$|Dockerfile$)`), `git diff --name-only "$MERGE_BASE"...HEAD | grep -Eq "$PATTERN"`, exit `0` = run e2e / exit `1` = skip.

**Dependencies:** none (independent of T4/T5, can be authored in parallel).

**Test-first guidance:** This is scriptable, deterministic logic — write a test-first shell verification before wiring it into anything.
- If `bats` (Bash Automated Testing System) is available in the repo/environment, write a RED `bats` test first (e.g. `tests/e2e/dag-paths-changed.bats`) with cases: (a) a fake diff touching `congress_videos/foo.py` → expect exit 0; (b) a diff touching only `docs/readme.md` → expect exit 1; (c) a diff touching `docker-compose.prod.yml` → expect exit 0 (Note N2 confirms this is intended). GREEN: implement the script until all three pass.
- If `bats` is NOT available in this environment, document a **manual verification checklist** instead: create a throwaway branch, touch a file under each glob category one at a time, run the script, and record the observed exit code for each of the 3 cases above as evidence in the apply-progress notes.

**Definition of Done (spec: "Conditional Trigger — E2E Runs Only for DAG-Relevant Path Changes"):**
- [x] RED (bats, if available) or documented manual checklist (if not): 3 cases (DAG-touching, docs-only, docker-compose.prod.yml) with expected exit codes. <!-- sdd-owner: implementation -->
- [x] GREEN: implement `scripts/dag-paths-changed.sh` per design's exact logic; all 3 cases produce the expected exit code. <!-- sdd-owner: implementation -->
- [x] Confirm the glob pattern is not expanded beyond the 5 locked globs (`congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, `Dockerfile`). <!-- sdd-owner: implementation -->

---

### T7 — scripts/test-airflow-e2e.sh (e2e driver script)

**Files:** `scripts/test-airflow-e2e.sh` (new)

**Description:** Implement exactly per design Q2/Q3/Q5:
- `uv --version` preflight → hard-fail exit `3` with actionable message if missing (per spec, before any Docker interaction).
- Docker/`docker compose` preflight → exit `4` (`EXIT_DOCKER_UNAVAILABLE`) gracefully, no container attempt, if unavailable.
- `trap` on `EXIT`/`INT`/`TERM` running `docker compose -p airflow-dags-e2e -f docker-compose.test.yml down -v --remove-orphans --timeout 30`, preserving the original exit code (`rc=$?` captured first).
- Bounded health poll: `E2E_HEALTH_TIMEOUT` default 120s, `E2E_POLL_INTERVAL` default 5s, heartbeat each interval, diagnostic + service log tail on timeout → exit `2` (`EXIT_HEALTH_TIMEOUT`).
- `airflow dags list-import-errors --output json`, assert `[]` → exit `0` (`EXIT_SUCCESS`) / non-empty → exit `1` (`EXIT_IMPORT_ERRORS`) with errors printed.
- All 6 named exit codes declared `readonly` at the top of the script (`EXIT_SUCCESS=0`, `EXIT_IMPORT_ERRORS=1`, `EXIT_HEALTH_TIMEOUT=2`, `EXIT_UV_UNAVAILABLE=3`, `EXIT_DOCKER_UNAVAILABLE=4`, `EXIT_INTERNAL_ERROR=5`).

**Dependencies:** T5 (compose file must exist to be driven), T6 (independent but typically composed together in the same verify flow — no hard code dependency, only conceptual).

**Test-first guidance:** Same reasoning as T6 — deterministic script behavior, no application pytest harness exists for shell scripts.
- If `bats` is available: write RED tests first covering each exit-code path where feasible without a real Docker daemon: (a) mock `command -v uv` to fail → assert exit 3 and no docker invocation attempted; (b) mock `docker compose` absent → assert exit 4 and no container attempt; (c) (optional, requires Docker) a real run against a deliberately broken DAG fixture → assert exit 1 with printed errors; (d) a real successful run → assert exit 0. GREEN: implement until tests pass.
- If `bats` is unavailable or a real Docker daemon isn't available in this environment: document a **manual verification checklist** recording exit code + observed behavior for exit 0 (real success), exit 1 (inject a broken import into a scratch DAG file), exit 2 (temporarily shorten `E2E_HEALTH_TIMEOUT` to force a timeout), exit 3 (temporarily rename `uv` off PATH), exit 4 (simulate via `PATH` without `docker`), recorded as evidence in T9.

**Definition of Done (spec: "scripts/test-airflow-e2e.sh Drives the E2E Smoke Test with Deterministic Exit Codes"):**
- [x] RED (bats, if available) or documented manual checklist: cover uv-missing (3), docker-missing (4), and at least the happy-path success (0) cases. <!-- sdd-owner: implementation -->
- [x] GREEN: implement `scripts/test-airflow-e2e.sh` with uv preflight, docker preflight, trap-based teardown, bounded health poll with heartbeat, `list-import-errors --output json` assertion, and all 6 named exit codes. <!-- sdd-owner: implementation -->
- [x] Manually exercise the health-timeout path (temporarily lower `E2E_HEALTH_TIMEOUT`) and confirm exit 2 with a clear diagnostic naming which service failed. <!-- sdd-owner: implementation -->
- [x] Manually confirm teardown runs on every path (success, import-errors, timeout, Ctrl-C) leaving no residual containers/volumes. <!-- sdd-owner: implementation -->

---

## PR 3 — Wiring + Validation (T8-T9)

### T8 — Confirm sdd-verify wiring is consistent and complete

**Files:** `openspec/config.yaml` (consistency pass over T3's edit), `CLAUDE.md` (cross-check)

**Description:**
- This repo has no CI workflow (explicit non-goal) and no separate "sdd-verify runner" script — `sdd-verify` is an SDD phase that reads `openspec/config.yaml`'s `testing.commands.unit`/`testing.commands.e2e` (and `rules.verify.test_command`) to decide what to run. "Wiring" therefore means: confirm the config keys written in T3 exactly match the script paths/exit-code contract actually implemented in T6/T7 (no drift between what config documents and what the scripts do).
- Cross-check: `testing.commands.e2e[0].gate_command` = `"bash scripts/dag-paths-changed.sh"` matches T6's actual invocation; `testing.commands.e2e[0].command` = `"bash scripts/test-airflow-e2e.sh"` matches T7's actual path; `unavailable_exit_code: 4` matches T7's actual `EXIT_DOCKER_UNAVAILABLE` value; `trigger_globs` (5 entries) matches T6's actual regex exactly.
- Fix any drift found (config should describe the scripts as-built, not as originally designed, if anything changed during T6/T7 implementation).

**Dependencies:** T3 (config baseline), T6, T7 (scripts must exist to cross-check against).

**Test-first guidance:** Not applicable as pytest — this is a consistency/documentation audit. Manual checklist: read `openspec/config.yaml`'s e2e entry side-by-side with the actual scripts and confirm exact string/value matches for every cross-referenced field listed above.

**Definition of Done (spec: "Config documents the conditional e2e entry"):**
- [x] Cross-check `openspec/config.yaml` e2e entry fields against the actual T6/T7 script paths and exit codes; fix any drift. <!-- sdd-owner: implementation -->
- [x] Confirm no separate CI/runner change was introduced (non-goal boundary respected). <!-- sdd-owner: implementation -->

---

### T9 — End-to-end validation of the full new flow

**Files:** none new (validation/evidence task); may touch a scratch/throwaway DAG file for negative-path testing, reverted after

**Description:** Run the complete flow for real and record evidence for each scenario:
1. `uv sync` from a clean state, then `uv run pytest` — full suite green.
2. A DAG-touching diff (e.g. touch a file under `congress_videos/`) → `scripts/dag-paths-changed.sh` exits `0` → `scripts/test-airflow-e2e.sh` runs → stack healthy → `list-import-errors` empty → exit `0`.
3. A non-DAG-touching diff (e.g. only `docs/**` or this `tasks.md`) → `scripts/dag-paths-changed.sh` exits `1` → e2e script is NOT invoked.
4. If feasible in this environment, a Docker-unavailable simulation (temporarily shadow `docker`/`docker compose` off `PATH`) → `scripts/test-airflow-e2e.sh` exits `4`, reports `unavailable`, does not attempt containers, and unit tests are unaffected.
5. Confirm production `docker-compose.yml`/`docker-compose.prod.yml`/`Dockerfile` are byte-for-byte unchanged at the end of the full change (final `git diff` check).

**Dependencies:** T1-T8 (this is the final integration validation of everything above).

**Test-first guidance:** This task IS the GREEN/TRIANGULATE proof for the whole change — no new RED needed here since each artifact already had its own test-first step; this task's job is to prove they compose correctly end-to-end. Record each scenario's actual output (exit codes, `list-import-errors` output, git diff results) as evidence in the apply-progress artifact.

**Definition of Done (spec success criteria #1, #4, #5, #6, #7):**
- [x] Record evidence: `uv sync` + `uv run pytest` green from a clean state. <!-- sdd-owner: implementation -->
- [x] Record evidence: DAG-touching diff triggers e2e end-to-end, resulting in exit `0` and empty `list-import-errors`. <!-- sdd-owner: implementation -->
- [x] Record evidence: non-DAG-touching diff does NOT trigger the e2e script. <!-- sdd-owner: implementation -->
- [x] Record evidence (or explicitly note if infeasible in this environment): Docker-unavailable path exits `4` with `unavailable` status, unit tests unaffected. <!-- sdd-owner: implementation -->
- [x] Confirm final `git diff` shows zero changes to `docker-compose.yml`, `docker-compose.prod.yml`, and the production `Dockerfile`. <!-- sdd-owner: implementation -->

---

## Parent Review Actions

- [ ] Start or reuse bounded review for PR 1 (uv migration, T1-T3) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for PR 2 (e2e infrastructure, T4-T7) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for PR 3 (wiring + validation, T8-T9) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Confirm chain strategy (`stacked-to-main` vs `feature-branch-chain`) with the user/orchestrator before the first PR is opened — this is currently `pending`. <!-- sdd-owner: parent -->
