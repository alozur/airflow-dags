# Apply Progress: test-runner-uv-docker-e2e

## Scope of this batch: PR1 (T1-T3) ONLY

T4-T9 are **NOT** part of this batch — they belong to PR2 (e2e infrastructure) and PR3 (wiring + validation), delivered in later apply batches per the chained-PR plan (`stacked-to-dev` chain strategy: PR1 → PR2 → PR3, each merging to `dev`, the repo's real integration branch — not `main`).

## Completed tasks (T1-T3, all `sdd-owner: implementation`)

### T1 — Migrate dependency management to uv
- [x] Added `[dependency-groups] dev` with the exact 6 pinned packages from `requirements-dev.txt`, plus `[tool.uv] package = false`, to `pyproject.toml`. Existing `[tool.pytest.ini_options]` / `[tool.coverage.*]` untouched (`--cov-fail-under=80` unchanged).
- [x] `uv sync` succeeds from a clean state (no manual `pip install`); `uv.lock` generated and left in the working tree for commit.
- [x] `uv run pytest` runs with the same pass/fail outcome as the pre-change (non-uv) install.

## ⚠️ CORRECTION (this batch) — T1's original "11 errors, pre-existing/unrelated" claim was WRONG

**The original T1 evidence below (kept, struck through in spirit, not deleted) incorrectly concluded the 11 collection errors were pre-existing and out of scope. This was VERIFIED WRONG.** Evidence: commit `cdf9c92` (which added the test suite) states "570 tests, 0 failures, 1 skip" in its own commit message — i.e., the full suite collected and passed cleanly at the time it was written. `tests/conftest.py` only stubs `yt_dlp`/`whisper`/`pytubefix`-adjacent fixtures (`mock_openai_client`, `mock_psycopg2_connection`) — it does **not** stub `psycopg2`, `numpy`, `PIL`, `bs4`, `google-auth`, `googleapiclient`, `PyPDF2`, or `openai` at the module level. For collection to have worked at `cdf9c92`, those packages **must have been actually installed** in the old conda environment, with no manifest ever capturing them. Migrating to `uv sync` with only a `dev` tooling group (no runtime deps group) is what broke collection — **this was a real regression introduced by this change**, not a pre-existing, unrelated gap.

**Root cause:** `pyproject.toml`'s `[dependency-groups] dev` (T1's original scope) only ever covered the 6 test-tooling packages from `requirements-dev.txt` (`pytest`, `pytest-cov`, etc.). It never covered the packages that `utils/**`/`congress_videos/**` actually `import` at module scope (`requests`, `numpy`, `psycopg2`, `google.auth`, `googleapiclient`, `PIL`, `PyPDF2`, `bs4`, `yt_dlp`, `pytubefix`, `webrtcvad`, `apache-airflow` itself, `python-dotenv`). Those were present in the old conda env by accident (manual installs, never captured in any requirements file), so `uv sync` — being fully declarative — correctly installed *only* what `pyproject.toml` declared, exposing the gap.

**Fix applied:** added `[project.dependencies]` (a real runtime dependency list, not a `dependency-groups` entry, since `uv sync` installs `[project.dependencies]` by default even with `[tool.uv] package = false`) to `pyproject.toml`, covering the union of:
1. `congress_videos/requirements.txt` (`requests`, `beautifulsoup4`, `lxml`, `urllib3`, `webrtcvad-wheels`) — excluding `silero-vad`/`torch`, which that file's own comments mark lazy/optional.
2. `docker-compose.yml`'s `_PIP_ADDITIONAL_REQUIREMENTS` (`apache-airflow-providers-postgres`, `openai`, `beautifulsoup4`, `requests`, `urllib3`, `google-auth`, `google-auth-oauthlib`, `google-auth-httplib2`, `google-api-python-client`, `Pillow`, `PyPDF2`, `yt-dlp`, `pytubefix`, `openai-whisper` — **excluded**, see below, `webrtcvad-wheels`).
3. Gaps found only by iterating `uv run pytest --collect-only` to 0 errors: `apache-airflow` itself (pinned `==2.10.2` to match the production `Dockerfile`'s `apache/airflow:2.10.2`, so local test runs exercise the same DAG/Operator API surface as prod), `psycopg2-binary`, `numpy`, `python-dotenv` (needed by `utils/env_loader.py`'s lazy import and directly `mocker.patch`'d in `tests/utils/test_env_loader.py`).

**`openai-whisper` deliberately excluded from the runtime group** (flagged per instructions, not silently omitted): it is only lazy-imported inside a function in `utils/whisper_helpers.py` (`import whisper` at call time, never at module scope), so it is **not required for test collection**. Its dependency chain (`numba` → `llvmlite==0.36.0`) does not build on this dev machine's Python 3.14 (`llvmlite` 0.36.0 requires Python `<3.10` — a hard build failure, not just heaviness), and even where it can build it drags in `torch` + ~1.5GB of CUDA wheels. It remains installed in the Docker runtime exactly as before, unchanged, via `docker-compose.yml`'s `_PIP_ADDITIONAL_REQUIREMENTS` (presumably an older Python there). If local whisper-model execution outside Docker is ever needed, it should be an opt-in dependency group, not part of the default synchronous `uv sync`.

**Verification (actually run, not assumed):**
- `uv sync` (clean, `.venv` removed first): **~5s** with warm `uv` cache (cold-cache first install of the full runtime set, including `apache-airflow` + all providers + `numpy`/`Pillow`/etc., measured at **~18s** before the `apache-airflow` version was pinned down from `2.11.2` to `2.10.2`; the pinned-version resync itself took **~7s**). No `torch`/CUDA downloads since `openai-whisper` is excluded — confirmed no `nvidia-*` packages in `uv sync` output.
- `uv run pytest --collect-only -q`: **0 errors** (down from 11), **1154 tests collected** (up from 904 collected + 11 errored-out modules before the fix — `904 + 250 ≈ 1154` once the 11 broken modules' own tests are counted; the earlier "570 tests" from `cdf9c92` reflects the repo's test suite before it grew further, consistent with the parent's note that the codebase evolved since that commit).
- `uv run pytest` (full run, no exclusions): **119 failed, 985 passed, 1 skipped, 49 errors** in 88.83s. This is **not** a clean pass — see "NEW findings" below for full root-cause attribution before concluding this is worse than expected.

**NEW findings (real, not masked) — both isolated and fully attributed, NOT part of this fix's scope to change:**

1. **Pre-existing test-file bug, newly exposed by real `airflow` now being installed** (root cause of the vast majority of the 119F/49E): `tests/congress_videos/modules/test_postgres_operators.py` and `tests/congress_videos/modules/test_postgres_operators_extended.py` each contain, at *module* import time:
   ```python
   if "airflow.utils.decorators" not in sys.modules:
       _mod = types.ModuleType("airflow.utils.decorators")
       _mod.apply_defaults = _noop_apply_defaults
       sys.modules["airflow.utils.decorators"] = _mod
   ```
   This shim was written for an environment where `airflow` was **not** installed at all (so it self-stubs `apply_defaults`, which Airflow 3 removed). Now that real `apache-airflow==2.10.2` is installed (this fix), collecting either of these two files unconditionally **overwrites the real `airflow.utils.decorators` module in `sys.modules`** with a fake stub missing symbols like `fixup_decorator_warning_stack`. This poisons every subsequent import of `airflow.models.baseoperator` (and anything importing it) for the rest of the pytest process — including the two files' own `congress_videos.modules.postgres_operators` import, which itself needs `from airflow.models import BaseOperator`. Confirmed in isolation: running just these two files alone reproduces all **49 errors**; excluding both files from the run drops the result to **5 failed, 1099 passed, 1 skipped, 0 errors** (coverage jumps from a masked ~4% to **76.94%**, close to the 80% gate). This is squarely a **test-infrastructure bug in T4+-scope test files** (pre-existing, just never triggered because `airflow` was never actually installed before), not a T1/`pyproject.toml` problem — **not modified** per this fix's instructions ("do NOT touch T2/T3 files... unless directly required"; these are T4+ files, out of scope, and the fix does not require touching them). Flagging as a real finding for the parent/user: the shim needs to check for the *specific missing attribute* (e.g., `getattr(airflow.utils.decorators, "apply_defaults", None) is None`) instead of unconditionally replacing the module, or be removed now that real `apache-airflow` is a declared dependency.
2. **Genuine Airflow API version mismatch** (5 of the remaining failures, independent of finding #1): `tests/congress_videos/test_reap_clip_preparer_dag.py`, `test_reap_processor_dag.py`, `test_reap_uploader_dag.py`, `test_youtube_channel_monitor_dag.py`, and `tests/utils/test_migrations_dag.py` each assert `dag.schedule == ...`. **Airflow 2.10.2/2.11.2's `DAG` class has no `.schedule` attribute** — only `schedule_interval` / `normalized_schedule_interval` (`.schedule` is Airflow 3.x API). Confirmed via `dir(DAG)` introspection and reproducing the exact `AttributeError: 'DAG' object has no attribute 'schedule'`. Since the production `Dockerfile` pins `apache/airflow:2.10.2` (not 3.x), these 5 tests were written against an API the production runtime does not have — a genuine pre-existing test/prod version mismatch, unrelated to T1's dependency-group fix and unrelated to finding #1. **Not fixed here** (would require either changing test assertions to `schedule_interval` or bumping production to Airflow 3.x — both out of this fix's scope; flagged for the parent/user to decide).
- With both findings accounted for (excluding the 2 poisoned test files and counting only the 5 genuine `.schedule` mismatches), the *true* regression-free state after this fix is **5 known, root-caused, unrelated failures** out of 1105 tests actually exercised (1099 passed + 5 failed + 1 skipped), not 119/49.

**Corrected before/after collection evidence (supersedes the original T1 evidence below):**
- **Before this fix**: 11 collection errors (`tests/congress_videos/test_reap_api.py`, `test_reap_clip_preparer_dag.py`, `test_reap_processor_dag.py`, `tests/congress_videos/modules/test_vad_chapter_adjust.py`, `test_vad_helpers.py`, `test_web_scraping.py`, `tests/congress_videos/modules/youtube/test_youtube_ai.py`, `tests/utils/test_airflow_helpers.py`, `test_llm_cache.py`, `test_whisper_helpers.py`, `test_youtube_helpers.py`), 904 tests collected, `Total coverage: 2.42%`.
- **After this fix**: **0 collection errors**, **1154 tests collected**. Full-suite run: 119 failed / 985 passed / 1 skipped / 49 errors — but see NEW findings above: 44 of the 49 errors and 114 of the 119 failures trace to pre-existing test-file bug #1 (poisoned `sys.modules`), and the remaining 5 failures trace to pre-existing finding #2 (Airflow API version mismatch). **Zero failures are attributable to this fix's dependency changes.**

---

### Original T1 evidence (superseded above, kept for audit trail — DO NOT trust the "pre-existing, unrelated" conclusion in it)

**Test-first evidence (RED/GREEN, since no application behavior changed — infra/config task per its own guidance):**
- RED: added `[dependency-groups] dev` to `pyproject.toml` *without* `[tool.uv] package = false` first, ran `uv sync` → **failed** as expected: `Failed to build airflow-dags ... Multiple top-level packages discovered in a flat-layout: ['openspec', 'downloads', 'congress_videos']` (setuptools tries to build/install the project because it's not marked virtual).
- GREEN: added `[tool.uv] package = false`, re-ran `uv sync` → succeeded, installed the 12 resolved packages (6 direct + transitive deps) into `.venv`.
- Before/after `uv run pytest` (this sandbox has no conda; established an equivalent "before" baseline instead — see Deviation note below):
  - **Before** (plain `pip install -r requirements-dev.txt` into a scratch venv, i.e. same 6-package set the old flow provided): 11 collection errors, `FAIL Required test coverage of 80% not reached. Total coverage: 2.42%`.
  - **After** (`uv run pytest` with the new `[dependency-groups] dev`): **identical** — 11 collection errors (same test files), `Total coverage: 2.42%`.
  - Outcome (INCORRECT, see correction above): originally concluded "same pass/fail result... pre-existing, unrelated gap." **This was wrong** — see correction section above for the real root cause and fix.

**Deviation from literal task wording:** the task says "record before/after counts as evidence" against "the pre-change conda-based run." This sandbox has no conda installed at all (confirmed: `conda` not on PATH), so no conda-based run was possible here. Substituted a scientifically equivalent baseline — a scratch venv with `pip install -r requirements-dev.txt` (the exact package set the old flow provided) — and confirmed it produces an **identical** result to the new `uv run pytest` run (same 11 errors, same 2.42% coverage). Flagging this substitution explicitly since it deviates from the literal "conda" wording, though not from the intent.

### T2 — Remove requirements-dev.txt and stray references
- [x] Grepped repo for `requirements-dev.txt` **before** deletion: only hits were inside `openspec/changes/test-runner-uv-docker-e2e/**` (the SDD planning artifacts themselves — proposal.md, spec.md, design.md, tasks.md), which are historical planning records, not live doc/script references. Zero hits in README, Makefile, CLAUDE.md, scripts, or any other live file.
- [x] No stray references needed updating (none existed outside the SDD planning docs).
- [x] Deleted `requirements-dev.txt`. Re-ran the grep: zero hits anywhere outside `openspec/changes/**` (`grep -rn "requirements-dev.txt" . | grep -v openspec/changes/` → exit 1, no matches). File confirmed gone (`ls requirements-dev.txt` → No such file or directory).

### T3 — Rewrite CLAUDE.md, openspec/config.yaml, and engram sdd-init note for uv
- [x] Rewrote `CLAUDE.md`'s "Development Environment" section: uv commands only (`uv sync`, `uv run python <script.py>`, `uv add <package>`, `uv add --dev <package>`), zero remaining `conda` references except the explicit "(no conda; ...)" clarifying phrase in the tooling directive itself. Added the "Testing" note block verbatim per design (unit via `uv run pytest`, e2e conditional trigger description). Verified via `grep -n conda CLAUDE.md` → only line 62, the intentional "no conda" statement.
- [x] Updated `openspec/config.yaml` to the exact YAML shape from design: `rules.apply.test_command` / `rules.verify.test_command` → `"uv run pytest"`; `testing.runner.command` → `"uv run pytest"`; `testing.layers.unit` / `testing.commands.unit[0].command` → `"uv run pytest"`; added `testing.layers.e2e` and `testing.commands.e2e[0]` with `scope: "."`, `command: "bash scripts/test-airflow-e2e.sh"`, `framework: "docker-compose"`, `gate_command: "bash scripts/dag-paths-changed.sh"`, `trigger_globs` (5 exact globs), `unavailable_exit_code: 4`, `notes`. Verified the file parses as valid YAML and matches the design's structural shape exactly (checked via `uv run --with pyyaml python3 -c "import yaml,json; print(json.dumps(...))"`).
- [x] Updated engram observation `sdd-init/airflow-dags` (id 7) via `mem_update`: **fully replaced** (not appended) the conda-based `test_command`/"Dev environment" entries and the old "Learned" note with uv-equivalent content, explicitly recording the new conditional e2e capability (script paths, gate script, trigger globs, exit-code contract 0-5, `unavailable` semantics for exit 4). Title updated to reflect supersession. Read back via `mem_get_observation(id: 7)` to confirm the new content is the *only* content present (revision_count incremented to 2; no dual conda+uv guidance remains).

---

## ⚠️ CORRECTION #2 (this batch) — fixed the two pre-existing bugs the runtime-dependency fix unmasked; `uv run pytest` is now genuinely green

This batch is still T1 scope (a further correction to PR1, per explicit user decision: fix both bugs now, inside PR1, so `uv run pytest` is genuinely green when PR1 closes). STRICT TDD applied to both — real code-behavior fixes, not config.

### Bug 1 — `sys.modules` poisoning in `test_postgres_operators.py` / `test_postgres_operators_extended.py`

**What was wrong:** both files (`tests/congress_videos/modules/test_postgres_operators.py`, `tests/congress_videos/modules/test_postgres_operators_extended.py`) unconditionally injected a fake module into `sys.modules["airflow.utils.decorators"]` at import time:
```python
if "airflow.utils.decorators" not in sys.modules:
    _mod = types.ModuleType("airflow.utils.decorators")
    _mod.apply_defaults = _noop_apply_defaults
    sys.modules["airflow.utils.decorators"] = _mod
else:
    sys.modules["airflow.utils.decorators"].apply_defaults = _noop_apply_defaults
```
This shim was written for an environment where `airflow` was not installed at all. Now that real `apache-airflow==2.10.2` is a declared runtime dependency (previous correction in this file), collecting either file **overwrites the real `airflow.utils.decorators` module in `sys.modules`** with a fake stub missing symbols like `fixup_decorator_warning_stack`. This poisons every later import of `airflow.models.baseoperator` (and anything importing it) for the rest of the pytest process, cascading into 49 errors and ~114 failures across the whole suite whenever these files are collected alongside other airflow-dependent tests.

**RED (confirmed reproduces):**
- `uv run pytest tests/congress_videos/modules/test_postgres_operators.py tests/congress_videos/modules/test_postgres_operators_extended.py --no-cov -q` → **49 errors**, exact same count as the prior batch's finding.
- Isolated repro: manually replaying the shim then `import congress_videos.modules.postgres_operators` raises `ImportError: cannot import name 'fixup_decorator_warning_stack' from 'airflow.utils.decorators' (unknown location)` — confirms the exact failure mode described.

**Fix applied:** deleted the shim entirely (not scoped/monkeypatched — removed), including its now-unused `import sys` / `import types` in both files. Verified the real installed `apache-airflow==2.10.2` already provides `apply_defaults` (`uv run python -c "from airflow.utils.decorators import apply_defaults; print(apply_defaults)"` → succeeds) — the shim was not just unnecessary but actively destructive once real Airflow became a dependency.

**GREEN:**
- `uv run pytest tests/congress_videos/modules/test_postgres_operators.py tests/congress_videos/modules/test_postgres_operators_extended.py --no-cov -q` → **49 passed** (was 49 errors).
- Full suite: the poisoning-cascade errors are gone (see full-suite evidence below).

**TRIANGULATE (root cause, not suppression):**
- `uv run python -c "import congress_videos.modules.postgres_operators as m; from airflow.models import BaseOperator; print(issubclass(m.PostgreSQLOperator, BaseOperator))"` → `True`, using the real, unstubbed `airflow.utils.decorators` module (confirmed `hasattr(airflow.utils.decorators, 'fixup_decorator_warning_stack')` → `True`). Proves the fix targets the actual root cause (module-poisoning via unconditional `sys.modules` overwrite), not just suppressing the symptom — the two test files' own module-under-test now imports and subclasses the real `BaseOperator` correctly with zero shimming.

### Bug 2 — `dag.schedule` is Airflow 3.x-only API; production is pinned to 2.10.2

**What was wrong:** 5 tests asserted `dag.schedule`, an attribute that only exists on Airflow 3.x's `DAG` class. Production is pinned to `apache/airflow:2.10.2` (per the `Dockerfile`), whose `DAG` class has no `.schedule` attribute — the 2.10.2-equivalent is `dag.schedule_interval`.

**Locations (all 5, confirmed via grep + inspection, each read individually to confirm intent before changing):**
1. `tests/utils/test_migrations_dag.py::TestRunMigrationsDAGLoads::test_dag_schedule_is_none` — asserted `dag.schedule is None`.
2. `tests/congress_videos/test_reap_clip_preparer_dag.py::TestCongressReapClipPreparerDAGLoads::test_dag_has_correct_schedule` — asserted `dag.schedule == '0 15 * * *'`.
3. `tests/congress_videos/test_youtube_channel_monitor_dag.py::TestCongressYoutubeChannelMonitorDAGLoads::test_dag_has_correct_schedule` — asserted `dag.schedule == '0 * * * *'`.
4. `tests/congress_videos/test_reap_uploader_dag.py::TestReapShortsUploaderDAGLoads::test_dag_schedule` — asserted `dag.schedule == '0 8,10,13,15,18,20,22 * * *'`.
5. `tests/congress_videos/test_reap_processor_dag.py::TestCongressReapProcessorDAGLoads::test_dag_has_correct_schedule` — asserted `dag.schedule == '30 14,17 * * *'`.

All 5 are checking the DAG's cron schedule value (the intent is unambiguous from context/test names) — not timetable identity or any Airflow-3-specific semantics — so `schedule_interval` (not `.timetable`) is the correct 2.10.2-equivalent expressing the same intent.

**RED (confirmed):** running all 5 against real Airflow 2.10.2 → `AttributeError: 'DAG' object has no attribute 'schedule'` for each, exact match to the reported failure mode.

**Fix applied:** changed each assertion's attribute from `.schedule` to `.schedule_interval`. Values verified individually against the real installed DAG objects before editing (`uv run python -c "from <dag module> import dag; print(repr(dag.schedule_interval))"` for each of the 4 DAGs with a cron string, and confirmed `None` for the migrations DAG) — all 5 expected values matched exactly, confirming these are value-preserving API-name fixes, not blind renames.

**GREEN:** `uv run pytest tests/utils/test_migrations_dag.py tests/congress_videos/test_reap_clip_preparer_dag.py tests/congress_videos/test_youtube_channel_monitor_dag.py tests/congress_videos/test_reap_uploader_dag.py tests/congress_videos/test_reap_processor_dag.py -k schedule --no-cov -q` → **5 passed** (was 5 failed).

### Final full-suite evidence — `uv run pytest` is genuinely green

```
1153 passed, 1 skipped in 34.40s
Required test coverage of 80% reached. Total coverage: 83.58%
```

- **0 failed, 0 errors** (was 119 failed / 985 passed / 1 skipped / 49 errors before this correction).
- **Coverage 83.58%**, clearing the `--cov-fail-under=80` gate (was 76.94% when the two poisoned files were excluded from a partial run in the prior batch's estimate; fixing both bugs — rather than excluding the files — restores full collection and pulls coverage above 80% because `postgres_operators.py`'s 342 statements now execute and are measured at 83.05% coverage instead of being invisible/erroring out). No threshold change was made or needed.
- Test count: 1153 passed + 1 skipped = 1154, matching the prior batch's `--collect-only` count of 1154 tests exactly — confirms no tests were lost, skipped-away, or silently deselected by either fix.

**This is now the authoritative "`uv run pytest` is green" evidence for T1's Definition of Done.**

### Files changed (this correction)
- `tests/congress_videos/modules/test_postgres_operators.py` — removed the `sys.modules` shim (and now-unused `sys`/`types` imports), replaced with an explanatory comment.
- `tests/congress_videos/modules/test_postgres_operators_extended.py` — same removal.
- `tests/utils/test_migrations_dag.py` — `dag.schedule` → `dag.schedule_interval` (1 assertion).
- `tests/congress_videos/test_reap_clip_preparer_dag.py` — `dag.schedule` → `dag.schedule_interval` (1 assertion).
- `tests/congress_videos/test_youtube_channel_monitor_dag.py` — `dag.schedule` → `dag.schedule_interval` (1 assertion).
- `tests/congress_videos/test_reap_uploader_dag.py` — `dag.schedule` → `dag.schedule_interval` (1 assertion).
- `tests/congress_videos/test_reap_processor_dag.py` — `dag.schedule` → `dag.schedule_interval` (1 assertion).
- `openspec/changes/test-runner-uv-docker-e2e/tasks.md` — appended a second correction note under T1.

### TDD Cycle Evidence (this correction)

| Bug | RED | GREEN | TRIANGULATE | Notes |
|-----|-----|-------|-------------|-------|
| Bug 1 (sys.modules poisoning) | 49 errors reproduced in isolated run of both files; exact `ImportError: cannot import name 'fixup_decorator_warning_stack'` reproduced manually | 49 passed after removing the shim | Confirmed real `BaseOperator` subclassing works and real `airflow.utils.decorators` has `fixup_decorator_warning_stack` present (unstubbed) | Root-cause fix (deletion), not suppression/scoping |
| Bug 2 (dag.schedule API mismatch) | `AttributeError: 'DAG' object has no attribute 'schedule'` reproduced for all 5 tests | 5 passed after renaming to `.schedule_interval` | Verified each expected cron value against the real DAG's `.schedule_interval` before editing — all 5 matched exactly (value-preserving) | No TRIANGULATE test added (renamed assertion IS the triangulation: matched against real DAG values, not blindly renamed) |

### Risks resolved by this correction
- **NEW finding #1 (from prior batch) — RESOLVED.** `sys.modules` poisoning fixed by shim removal; verified in isolation and full-suite run.
- **NEW finding #2 (from prior batch) — RESOLVED.** `dag.schedule` → `dag.schedule_interval`, verified against real DAG values for all 5 tests.
- **Coverage below gate (from prior batch) — RESOLVED.** 83.58% now clears the 80% gate; no threshold change made.
- **No new risks introduced.** Both fixes are surgical (shim deletion, attribute rename) confined to the 7 test files listed above; no production code (`congress_videos/**`, `utils/**`) was touched.

---

## Files changed (this batch)
- `pyproject.toml` — added `[dependency-groups] dev` + `[tool.uv] package = false`, **plus (this correction) `[project.dependencies]` with the runtime dependency list** (union of `congress_videos/requirements.txt` + `docker-compose.yml`'s `_PIP_ADDITIONAL_REQUIREMENTS` + `apache-airflow==2.10.2`/`psycopg2-binary`/`numpy`/`python-dotenv` gaps, excluding `openai-whisper`; see CORRECTION section above)
- `uv.lock` — new, generated by `uv sync`; **regenerated** (this correction) after the runtime dependency group was added
- `requirements-dev.txt` — deleted
- `CLAUDE.md` — Development Environment rewritten to uv, Testing note added
- `openspec/config.yaml` — test commands + new `testing.commands.e2e` entry
- Engram observation `sdd-init/airflow-dags` (id 7) — superseded (uv-based)
- `openspec/changes/test-runner-uv-docker-e2e/tasks.md` — T1/T2/T3 checkboxes marked `[x]`

## Test commands run
- `uv sync` (RED before `package = false`, GREEN after)
- `uv run pytest` (after) vs. scratch-venv `pip install -r requirements-dev.txt` + `pytest` (before-equivalent baseline) — **original, later corrected**
- `grep -rn "requirements-dev.txt" .` (before and after deletion)
- `grep -n "conda" CLAUDE.md` (after rewrite)
- `uv run --with pyyaml python3 -c "import yaml..."` (YAML shape validation)

**This correction's commands (all actually run, evidence above):**
- `uv sync` (clean `.venv` removal + resync, timed) — 3 runs total (initial add w/ `apache-airflow>=2.8,<3.0`+`openai-whisper` → build failure; retry without `openai-whisper` → success ~18s cold; retry pinned `apache-airflow==2.10.2` → ~7s; final clean-`.venv` resync → ~5s warm cache)
- `uv run pytest --collect-only -q` (before: 11 errors/904 collected; after: 0 errors/1154 collected)
- `uv run pytest` (full run, no exclusions): 119 failed/985 passed/1 skipped/49 errors, 88.83s — root-caused, see CORRECTION section
- `uv run pytest --ignore=tests/congress_videos/modules/test_postgres_operators.py --ignore=tests/congress_videos/modules/test_postgres_operators_extended.py`: 5 failed/1099 passed/1 skipped/0 errors, coverage 76.94%, 41s — isolates finding #1
- `uv run pytest tests/congress_videos/modules/test_postgres_operators.py tests/congress_videos/modules/test_postgres_operators_extended.py --no-cov`: 49 errors in isolation — confirms finding #1's exact source
- `uv run python -c "import sys, types; ...; import congress_videos.modules.postgres_operators"` — reproduces the exact `ImportError: cannot import name 'fixup_decorator_warning_stack'` from the poisoned `sys.modules` shim
- `uv run python -c "from airflow.models.dag import DAG; print([a for a in dir(DAG) if 'schedul' in a.lower()])"` — confirms Airflow 2.x has no `.schedule` attribute (finding #2)

## TDD Cycle Evidence

| Task | Type | RED | GREEN | TRIANGULATE | Notes |
|------|------|-----|-------|-------------|-------|
| T1 | Infra/config, no app behavior | `uv sync` fails to build (flat-layout error) before `package=false` | `uv sync` + `uv run pytest` succeed identically to old install after `package=false` added | Confirmed old-vs-new install produce byte-identical pass/fail outcome | Per task's own test-first guidance (not generic RED/GREEN) |
| T2 | Doc/file removal, no runtime behavior | grep before deletion (baseline) | grep after deletion (zero hits) | N/A | Per task's own test-first guidance (grep-based checklist) |
| T3 | Documentation/config only | N/A (task explicitly states no pytest RED/GREEN applies) | grep `conda` in CLAUDE.md (zero unintended hits); YAML structural match; engram read-back confirms supersession | N/A | Per task's own manual verification checklist |

## Deviations from design
- None material. The only deviation is the T1 "before" baseline substitution (conda unavailable in this sandbox → used an equivalent pip-based baseline), documented above with rationale.

## Remaining tasks (NOT in this batch — PR2 and PR3)

```
- [ ] RED: write `tests/e2e/test_sitecustomize_stubs.py` asserting all 8 confirmed heavy packages become importable after loading the stub module, and that `google` namespace is not shadowed. <!-- sdd-owner: implementation -->
- [ ] GREEN: implement `e2e/stubs/sitecustomize.py` with the `_stub()` helper and exact package list from design; test passes. <!-- sdd-owner: implementation -->
- [ ] TRIANGULATE: add a non-stubbed package (`requests`) import assertion to confirm no over-shadowing. <!-- sdd-owner: implementation -->
- [ ] Manually confirm `uv run pytest` (without `e2e/stubs` on `PYTHONPATH`) still passes unaffected. <!-- sdd-owner: implementation -->
- [ ] Author `docker-compose.test.yml` per design's exact structure (image, env, mounts, healthchecks, tmpfs postgres, anonymous logs volume). <!-- sdd-owner: implementation -->
- [ ] `docker compose config` validates the file with no errors. <!-- sdd-owner: implementation -->
- [ ] Manually boot + tear down the stack once; confirm no residual containers/volumes/networks remain (checklist above). <!-- sdd-owner: implementation -->
- [ ] Confirm `docker-compose.yml`/`docker-compose.prod.yml`/production `Dockerfile` are unchanged via `git diff`. <!-- sdd-owner: implementation -->
- [ ] RED (bats, if available) or documented manual checklist: 3 cases (DAG-touching, docs-only, docker-compose.prod.yml) with expected exit codes. <!-- sdd-owner: implementation -->
- [ ] GREEN: implement `scripts/dag-paths-changed.sh` per design's exact logic; all 3 cases produce the expected exit code. <!-- sdd-owner: implementation -->
- [ ] Confirm the glob pattern is not expanded beyond the 5 locked globs (`congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, `Dockerfile`). <!-- sdd-owner: implementation -->
- [ ] RED (bats, if available) or documented manual checklist: cover uv-missing (3), docker-missing (4), and at least the happy-path success (0) cases. <!-- sdd-owner: implementation -->
- [ ] GREEN: implement `scripts/test-airflow-e2e.sh` with uv preflight, docker preflight, trap-based teardown, bounded health poll with heartbeat, `list-import-errors --output json` assertion, and all 6 named exit codes. <!-- sdd-owner: implementation -->
- [ ] Manually exercise the health-timeout path (temporarily lower `E2E_HEALTH_TIMEOUT`) and confirm exit 2 with a clear diagnostic naming which service failed. <!-- sdd-owner: implementation -->
- [ ] Manually confirm teardown runs on every path (success, import-errors, timeout, Ctrl-C) leaving no residual containers/volumes. <!-- sdd-owner: implementation -->
- [ ] Cross-check `openspec/config.yaml` e2e entry fields against the actual T6/T7 script paths and exit codes; fix any drift. <!-- sdd-owner: implementation -->
- [ ] Confirm no separate CI/runner change was introduced (non-goal boundary respected). <!-- sdd-owner: implementation -->
- [ ] Record evidence: `uv sync` + `uv run pytest` green from a clean state. <!-- sdd-owner: implementation -->
- [ ] Record evidence: DAG-touching diff triggers e2e end-to-end, resulting in exit `0` and empty `list-import-errors`. <!-- sdd-owner: implementation -->
- [ ] Record evidence: non-DAG-touching diff does NOT trigger the e2e script. <!-- sdd-owner: implementation -->
- [ ] Record evidence (or explicitly note if infeasible in this environment): Docker-unavailable path exits `4` with `unavailable` status, unit tests unaffected. <!-- sdd-owner: implementation -->
- [ ] Confirm final `git diff` shows zero changes to `docker-compose.yml`, `docker-compose.prod.yml`, and the production `Dockerfile`. <!-- sdd-owner: implementation -->
```

Parent-owned lifecycle rows (deferred, preserved byte-for-byte, not touched by this batch):
```
- [ ] Start or reuse bounded review for PR 1 (uv migration, T1-T3) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for PR 2 (e2e infrastructure, T4-T7) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for PR 3 (wiring + validation, T8-T9) once implementation completes. <!-- sdd-owner: parent -->
- [ ] Confirm chain strategy (`stacked-to-main` vs `feature-branch-chain`) with the user/orchestrator before the first PR is opened — this is currently `pending`. <!-- sdd-owner: parent -->
```
Note: chain strategy has since been resolved by the parent/user as `stacked-to-dev` (each PR merges to `dev`, not `main`) — the tasks.md "Parent Review Actions" checkbox text above still shows the original `stacked-to-main`/`feature-branch-chain` phrasing and is a parent-owned row; left byte-for-byte unchanged per ownership boundary.

## Workload / PR boundary
- This batch = **PR1 only** (T1-T3), per the Review Workload Forecast (`Chained PRs recommended: Yes`, `400-line budget risk: High`) and the resolved delivery decision: delivery strategy `ask-on-risk`, chain strategy `stacked-to-dev`.
- Changed files this batch: `pyproject.toml` (+13 lines), `uv.lock` (new, generated — not hand-review-weighted), `requirements-dev.txt` (deleted, -6 lines), `CLAUDE.md` (+21/-11 net), `openspec/config.yaml` (+29/-7 net). Matches the task-level estimate (~15+10-15+60-70 ≈ 90-100 hand-written lines, well under the 400-line budget for this slice alone).
- Left staged/unstaged in the working tree — **not committed, not pushed**, per instructions. Parent owns delivery (commit/PR against `dev`).

## Structured status consumed / produced
- Consumed: tasks.md (topic key `sdd/test-runner-uv-docker-e2e/tasks`, mirrored at `openspec/changes/test-runner-uv-docker-e2e/tasks.md`), spec.md, design.md — all read directly from the openspec files in this hybrid-store project.
- No prior apply-progress existed (first batch) — nothing to merge.
- `actionContext`: no warnings; ran in the repo root, all edits within the repository (no external edit roots needed).
- Produced: this apply-progress (openspec file + engram mirror at topic_key `sdd/test-runner-uv-docker-e2e/apply-progress`), tasks.md checkbox updates (T1-T3 `[x]`), engram `sdd-init/airflow-dags` supersession.

## Risks
- **(SUPERSEDED, kept for audit trail)** ~~Environment gap unrelated to this batch: 11 pre-existing pytest collection errors... confirmed pre-existing and out of T1-T3 scope.~~ **This was wrong** — see CORRECTION section above. The 11 collection errors were a real regression from T1's dependency-group scope being too narrow (dev-tooling only, no runtime deps), now fixed.
- **CRITICAL (this correction), now resolved**: T1's dependency migration was incomplete — `pyproject.toml`'s `[dependency-groups] dev` never covered runtime imports (`numpy`, `psycopg2`, `google.auth`, etc.), causing 11 real collection errors post-migration. Fixed by adding `[project.dependencies]` with the runtime union; verified 0 collection errors, 1154 tests collected.
- **NEW finding #1 — RESOLVED (this correction)**: `tests/congress_videos/modules/test_postgres_operators.py` and `test_postgres_operators_extended.py` unconditionally stubbed `sys.modules["airflow.utils.decorators"]` at module-import time, poisoning the real installed `apache-airflow` for the rest of the pytest process (49 errors + ~114 cascading failures). Per explicit user decision, fixed in this batch (still T1 scope): the shim was deleted entirely, since real `apache-airflow==2.10.2` already provides `apply_defaults`. Verified via isolated re-run (49 passed) and full-suite re-run (0 errors).
- **NEW finding #2 — RESOLVED (this correction)**: the 5 tests asserting `dag.schedule` (Airflow 3.x-only API) were updated to `dag.schedule_interval`, the correct Airflow 2.10.2-equivalent, verified against each DAG's real value before editing. All 5 now pass against the real installed Airflow 2.10.2.
- **Coverage below gate — RESOLVED (this correction)**: fixing both bugs (rather than excluding the affected files) restored full test collection/execution; coverage is now **83.58%**, clearing the `--cov-fail-under=80` gate. No threshold change was made.
- **No CRITICAL or WARNING risks remain from this change.** `uv run pytest` (full suite, no exclusions, no `--no-cov`) is genuinely green: 1153 passed, 1 skipped, 0 failed, 0 errors, coverage 83.58%. This is the authoritative evidence for T1's Definition of Done.
