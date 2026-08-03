# Apply Progress: add-generic-thumbnail-dag

## Meta

| Field | Value |
|-------|-------|
| Change | add-generic-thumbnail-dag |
| Branch | feat/generic-thumbnail-title-dag |
| Slice | 2 of 3 (domain logic + config + migration) |
| Mode | Strict TDD |
| Delivery | auto-chain / stacked-to-main |
| Date | 2026-07-31 |

---

## Completed Tasks (Slice 1 — Pikzels client port)

| Task | Status |
|------|--------|
| RED: test_pikzels_client.py — public surface (import & trimmed names) | [x] |
| RED: missing PIKZELS_API_KEY → EnvironmentError | [x] |
| RED: _request retries on 503 (3 calls total) | [x] |
| RED: _request raises immediately on 400 (1 call) | [x] |
| RED: thumbnail_from_text payload shape + response return | [x] |
| RED: score_thumbnail sends image_base64 field, numeric score | [x] |
| RED: download GET + mkdir + write bytes | [x] |
| RED: to_base64_data_url returns correct data URI | [x] |
| GREEN: Implement congress_videos/modules/pikzels_client.py | [x] |
| TRIANGULATE: edge-case tests (network errors exhausted, existing dir, large payload) | [x] |
| REFACTOR: Google-style docstrings, import ordering, no dead code | [x] |

### Slice 1 Test Results

| Metric | Value |
|--------|-------|
| Test file | `tests/congress_videos/modules/test_pikzels_client.py` |
| Tests collected | 34 |
| Tests passed | 34 |
| Tests failed | 0 |
| Module coverage (`pikzels_client.py`) | **88.35%** |
| Coverage gate (≥80%) | **PASS** |

---

## Completed Tasks (Slice 2 — Domain logic, config, migration)

| Task | Status |
|------|--------|
| 2a: Create `congress_videos/config/thumbnail_config.py` | [x] |
| 2a: Modify `congress_videos/config/ai_prompts.py` (add prompts) | [x] |
| 2a: Modify `congress_videos/config/paths.py` (add get_thumbnail_dir) | [x] |
| 2a: Create `congress_videos/sql/migrations/017_create_video_thumbnails.sql` | [x] |
| 2b RED: test_thumbnail_generation.py — resolve_participant_photo (4 scenarios) | [x] |
| 2b RED: generate_and_score_options (5 scenarios) | [x] |
| 2b RED: generate_and_score_options path creation | [x] |
| 2b RED: choose_best_option (3 scenarios) | [x] |
| 2b RED: generate_title (5 scenarios incl. forbidden chars, re-prompt, fallback) | [x] |
| 2b RED: generate_title calls generate_json_completion not openai directly | [x] |
| 2b RED: persist_results (4 scenarios) | [x] |
| 2b GREEN: Implement congress_videos/modules/thumbnail_generation.py | [x] |
| 2b TRIANGULATE: edge cases (Pikzels raises, HTTP 404 fallback, >2 options) | [x] |
| 2b REFACTOR: Google-style docstrings, import ordering, no direct openai | [x] |
| Coverage check (≥80% both modules) | [x] |

### Slice 2 Test Results

| Metric | Value |
|--------|-------|
| Test file | `tests/congress_videos/modules/test_thumbnail_generation.py` |
| Tests collected | 29 |
| Tests passed | 29 |
| Tests failed | 0 |
| Module coverage (`thumbnail_generation.py`) | **98.51%** |
| Module coverage (`pikzels_client.py`) | **88.35%** |
| Combined coverage (Slice 1 + 2) | **94.09%** |
| Coverage gate (≥80%) | **PASS** |

---

## Completed Tasks (Slice 3 — DAG wiring, env/config, import test)

| Task | Status |
|------|--------|
| 3a: Modify `.env` / canonical env template (docs/DEPLOYMENT_NAS.md + .env.example) | [x] |
| 3a: Modify `docker-compose.yml`, `docker-compose.prod.yml`, `docker-compose.test.yml` — add `PIKZELS_API_KEY` | [x] |
| 3a: Modify `conftest.py` — add `PIKZELS_API_KEY: "pkz_test-not-real"` to `_TEST_ENV` | [x] |
| 3b RED: test_generic_thumbnail_dag.py — DAG imports cleanly | [x] |
| 3b RED: schedule_interval is None | [x] |
| 3b RED: exact task-id set (11 tasks, no more/fewer) | [x] |
| 3b RED: dependency graph shape (10 assertions) | [x] |
| 3b RED: no Congreso-specific literals in DAG source outside imports | [x] |
| 3b RED: validate_input callable (6-key valid conf; missing key; empty string) | [x] |
| 3b GREEN: Create congress_videos/generic_thumbnail_generator_dag.py | [x] |
| 3b TRIANGULATE: PIKZELS_API_KEY absent → EnvironmentError; unknown domain → ConfigError | [x] |
| 3b REFACTOR: import ordering, remove inline imports, Google-style docstrings | [x] |

### Slice 3 Test Results

| Metric | Value |
|--------|-------|
| Test file | `tests/congress_videos/modules/test_generic_thumbnail_dag.py` |
| Tests collected | 29 |
| Tests passed | 29 |
| Tests failed | 0 |
| Module coverage (`generic_thumbnail_generator_dag.py`) | **53.12%** (task callables need live Airflow context — covered by e2e) |
| Module coverage (`pikzels_client.py`) | **88.35%** |
| Module coverage (`thumbnail_generation.py`) | **98.51%** |
| Full suite total coverage | **84.48%** (≥80% gate PASS) |
| Full suite total tests | 1443 passed, 1 skipped |

---

## W-01 Remediation (2026-07-31)

| Action | Status |
|--------|--------|
| Delete `generate_and_score_options` from `thumbnail_generation.py` | [x] |
| Remove now-unused imports (`get_thumbnail_dir`, `download`, `score_thumbnail`, `thumbnail_from_text`) from `thumbnail_generation.py` | [x] |
| Delete `TestGenerateAndScoreOptions`, `TestGenerateAndScoreOptionsPathCreation`, and `test_pikzels_exception_propagates` from `test_thumbnail_generation.py` | [x] |
| Add T-08: `_task_generate_thumbnail` unit tests (3 tests) to `test_generic_thumbnail_dag.py` | [x] |
| Add T-09: `_task_download_option` unit tests (2 tests) to `test_generic_thumbnail_dag.py` | [x] |
| Add T-10: `_task_score_option` unit tests (3 tests) to `test_generic_thumbnail_dag.py` | [x] |
| Update `design.md`: remove `generate_and_score_options` from Interfaces/Contracts, add note on per-task callables | [x] |
| Verified: `rg generate_and_score_options` returns zero matches in production/test code | [x] |
| Verified: 58 tests pass in targeted run; 1443 passed, 1 skipped in full suite | [x] |
| Verified: `thumbnail_generation.py` coverage 98.26%, `generic_thumbnail_generator_dag.py` coverage 81.25% | [x] |
| No backfilled test revealed a real bug — the three new task-callable tests all pass GREEN against current correct implementation | [x] |

### W-01 Backfill Test Evidence

| Task | Test Class | RED note | GREEN |
|------|------------|----------|-------|
| `_task_generate_thumbnail` | `TestTaskGenerateThumbnail` (3 tests) | New tests written (GREEN expected from the start — backfill, not pre-existing bug) | All 3 PASS |
| `_task_download_option` | `TestTaskDownloadOption` (2 tests) | Same | All 2 PASS |
| `_task_score_option` | `TestTaskScoreOption` (3 tests) | Same | All 3 PASS |

No real bug found: `score_thumbnail(image_base64=...)` is called correctly; `thumbnail_from_text(..., support_image_base64=...)` is called correctly; `main_score` is extracted from dict correctly.

## Remaining Tasks

- [ ] Start or reuse bounded review for Slice 2 (parent task)
- [ ] Cross-slice final verification

---

## Files Changed

### Slice 3 (current)

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/generic_thumbnail_generator_dag.py` | Created | `schedule=None`, `max_active_runs=3`, 11 `PythonOperator` tasks wired per spec, `validate_input` public function, `ConfigError` on unknown domain, generate/score tasks with `execution_timeout=timedelta(minutes=10)`, no Congreso literals. |
| `tests/congress_videos/modules/test_generic_thumbnail_dag.py` | Created | 29 tests: import clean, schedule_interval=None, exact task-id set, dependency graph shape (10 assertions), no Congreso literals, validate_input (13 parametrized cases), TRIANGULATE (PIKZELS_API_KEY absent + ConfigError). |
| `conftest.py` | Modified | Added `PIKZELS_API_KEY: "pkz_test-not-real"` to `_TEST_ENV`. |
| `docker-compose.yml` | Modified | Added `PIKZELS_API_KEY: "${PIKZELS_API_KEY:-}"` to `x-airflow-common` env block. |
| `docker-compose.prod.yml` | Modified | Added `PIKZELS_API_KEY: "${PIKZELS_API_KEY:-}"` to `x-airflow-common` env block. |
| `docker-compose.test.yml` | Modified | Added `PIKZELS_API_KEY: "pkz_test-not-real"` to e2e test env block. |
| `docs/DEPLOYMENT_NAS.md` | Modified | Added `PIKZELS_API_KEY=pkz_...` to the env template section. |
| `openspec/changes/add-generic-thumbnail-dag/tasks.md` | Modified | Slice 3 implementation checkboxes marked [x]. |

### Slice 1 (committed)

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/modules/pikzels_client.py` | Created | Trimmed Pikzels v2 client: `PikzelsClient`, `PikzelsError`, `thumbnail_from_text`, `score_thumbnail`, `download`, `to_base64_data_url`, helpers. |
| `tests/congress_videos/modules/test_pikzels_client.py` | Created | 34 tests; RED→GREEN→TRIANGULATE cycle completed. |

### Slice 2 (current)

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/modules/thumbnail_generation.py` | Created | Domain logic: `resolve_participant_photo`, `generate_and_score_options`, `choose_best_option`, `generate_title`, `persist_results`. |
| `congress_videos/config/thumbnail_config.py` | Created | `THUMBNAIL_CONFIG` dict for `"congreso"` domain; `ConfigError`; `get_domain_config()`. |
| `congress_videos/config/ai_prompts.py` | Modified | Added `THUMBNAIL_TITLE_SYSTEM_PROMPT` + `THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE`. |
| `congress_videos/config/paths.py` | Modified | Added `get_thumbnail_dir(youtube_video_id: str) -> Path`. |
| `congress_videos/sql/migrations/017_create_video_thumbnails.sql` | Created | `video_thumbnails` table + 2 indexes + unique constraint on `(chapter_id, label)`. |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Created | 29 tests covering all 5 domain functions + edge cases; RED→GREEN→TRIANGULATE→REFACTOR cycle. |
| `openspec/changes/add-generic-thumbnail-dag/tasks.md` | Modified | Slice 2 implementation checkboxes marked [x]. |

---

## Changed-Line Count

| File | Lines |
|------|-------|
| `congress_videos/modules/thumbnail_generation.py` | ~175 (new) |
| `congress_videos/config/thumbnail_config.py` | ~90 (new) |
| `congress_videos/config/ai_prompts.py` | ~30 (added) |
| `congress_videos/config/paths.py` | ~15 (added) |
| `congress_videos/sql/migrations/017_create_video_thumbnails.sql` | ~27 (new) |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | ~320 (new) |
| `tasks.md` (checkbox updates) | ~12 (modified) |
| **Total (Slice 2)** | **~669** |

---

## TDD Cycle Evidence (Slice 3)

| Task | Test Class | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|------------|-------|------------|-----|-------|-------------|----------|
| DAG import clean | `TestDagImport` | Unit | ✅ 63/63 | ✅ Written (1 test) | ✅ 1/1 passed | ➖ Single scenario (import or fail) | ✅ Clean |
| schedule_interval is None | `TestDagSchedule` | Unit | ✅ 63/63 | ✅ Written (1 test) | ✅ 1/1 passed (fixed `schedule_interval`) | ➖ Single scenario | ✅ Clean |
| Exact task-id set | `TestDagTaskIds` | Unit | ✅ 63/63 | ✅ Written (1 test) | ✅ 1/1 passed | ➖ Single assertion on set equality | ✅ Clean |
| Dependency graph shape | `TestDagDependencies` | Unit | ✅ 63/63 | ✅ Written (10 tests) | ✅ 10/10 passed | ✅ Each edge independently asserted | ✅ Clean |
| No Congreso literals | `TestNoCongresoBranding` | Unit | ✅ 63/63 | ✅ Written (1 test) | ✅ 1/1 passed (removed docstring literal) | ➖ Single invariant | ✅ Clean |
| validate_input | `TestValidateInput` | Unit | ✅ 63/63 | ✅ Written (13 parametrized) | ✅ 13/13 passed | ✅ Missing key + empty string each parametrized over all 6 keys | ✅ Clean |
| PIKZELS_API_KEY absent | `TestTriangulate` | Unit | ✅ 63/63 | ✅ Written | ✅ Passed | ✅ monkeypatch.delenv + fresh import | ✅ Clean |
| ConfigError unknown domain | `TestTriangulate` | Unit | ✅ 63/63 | ✅ Written | ✅ Passed | ✅ Distinct path from valid-domain case | ✅ Clean |

### Test Summary (Slice 3)

- **Total tests written**: 29
- **Total tests passing**: 29
- **Layers used**: Unit (29)
- **Approval tests**: None — no refactoring of existing files in logic sense
- **Pure functions created**: `validate_input` (raises deterministically on bad input)

---

## TDD Cycle Evidence (Slice 2)

| Task | Test Class | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|------------|-------|------------|-----|-------|-------------|----------|
| resolve_participant_photo | `TestResolveParticipantPhoto` | Unit | N/A (new) | ✅ Written (4 tests) | ✅ 4/4 passed | ✅ HTTP 404 + RequestException edge cases in `TestTriangulateEdgeCases` | ✅ Clean |
| generate_and_score_options | `TestGenerateAndScoreOptions` | Unit | N/A (new) | ✅ Written (6 tests) | ✅ 6/6 passed | ✅ Pikzels raises, path creation | ✅ Clean |
| choose_best_option | `TestChooseBestOption` | Unit | N/A (new) | ✅ Written (3 tests) | ✅ 3/3 passed | ✅ >2 options, preserves fields | ✅ Clean |
| generate_title | `TestGenerateTitle` | Unit | N/A (new) | ✅ Written (5 tests) | ✅ 5/5 passed | ✅ None return fallback | ✅ Clean |
| persist_results | `TestPersistResults` | Unit | N/A (new) | ✅ Written (4 tests) | ✅ 4/4 passed | ➖ SQL upsert verified by chosen/non-chosen param checks | ✅ Clean |

### Test Summary (Slice 2)

- **Total tests written**: 29
- **Total tests passing**: 29
- **Layers used**: Unit (29)
- **Approval tests**: None — no refactoring tasks
- **Pure functions created**: `choose_best_option`, `_is_valid`, `_sanitise` (internal)

---

## Deviations from Design

1. **`generate_and_score_options` signature extended**: Added `youtube_video_id: str` parameter (needed to call `get_thumbnail_dir`). Design shows `(prompt, photo_b64, cfg)` but without `youtube_video_id` there is no way to build the local path. Spec requires the path pattern `{youtube_video_id}/{label}.png` so the parameter is mandatory.

2. **`THUMBNAIL_CONFIG` uses lazy function for `participants_lookup`**: Design shows a fn reference directly in the dict. Implemented via `_get_thumbnail_config()` factory to defer the import of `lookup_participant_fuzzy` (avoids circular imports at module load time). The `THUMBNAIL_CONFIG` constant is populated once at module import by calling that factory.

3. **Title max chars is 90 in implementation**: The design/spec has a slight inconsistency — spec table says "Maximum length: 100 characters" but the design OpenAI Title Spec says "max 90 chars" and `tasks.md` says "≤90 chars". Implemented as **90** per tasks.md + design (which are authoritative for Slice 2).

---

## Deviations from Design (Slice 3)

1. **`xcom_task` wrapper not used for complex multi-XCom fan-in tasks**: The design says "PythonOperator tasks using `xcom_task` wrapper from `utils.airflow_helpers`". The `xcom_task` helper supports only a single `input_key` pull, which is insufficient for tasks that pull from multiple upstreams (e.g., `_task_persist_results` pulls from 4 different task IDs). Direct `ti.xcom_pull()` calls are used instead — matching the pattern in `youtube_uploader_dag.py`. The design intent (XCom-based data passing) is fully honoured.

2. **`schedule_interval` attribute checked in tests, not `schedule`**: Airflow 2.x exposes `dag.schedule_interval` (not `dag.schedule`) as the inspectable attribute. The test was updated to use the correct attribute name.

3. **`docker-compose-whisper.yml` not modified**: The whisper stack has no Airflow scheduler/worker services and no API key env blocks. No change needed per the task scoping ("every Airflow worker/scheduler env block that already mounts runtime env vars").

4. **`.env` not modified directly (gitignored)**: Added `PIKZELS_API_KEY=pkz_...` to `docs/DEPLOYMENT_NAS.md` (the canonical env template in this repo). The `.env.example` file creation was blocked by filesystem permissions (dotfile access denial). `docs/DEPLOYMENT_NAS.md` is the documented template for this repo.

---

## PR Boundary

### Slice 2 (completed)
- Mode: chained PR slice 2 of 3
- Scope: `thumbnail_generation.py` + config files (`thumbnail_config.py`, ai_prompts additions, paths addition) + migration `017` + test file
- Depends on: Slice 1 merged (imports `pikzels_client`)
- Rollback: delete the 3 new files, revert the 2 modified config files; migration requires `DROP TABLE video_thumbnails` manually

### Slice 3 (current)
- Mode: chained PR slice 3 of 3
- Scope: `generic_thumbnail_generator_dag.py` + env/docker-compose/conftest wiring + DAG import test
- Depends on: Slice 1 + 2 merged
- Rollback: delete `generic_thumbnail_generator_dag.py` + `test_generic_thumbnail_dag.py`; revert `conftest.py` + 3 docker-compose files + docs
- Next: cross-slice final verification (parent task) then `sdd-verify`

### Post-merge fix — migration renumber 017 → 019 (2026-07-31)
- After merge, dev already contained `017_create_speaker_normalization_cache.sql` and
  `018_add_participant_slug.sql` from concurrent work → this migration's `017` number collided.
- Renamed `017_create_video_thumbnails.sql` → `019_create_video_thumbnails.sql` (git mv) and
  updated its header comment + `design.md` references. The `run_migrations` runner keys applied
  migrations by filename, so both `017` files would still apply — this is a convention fix
  (unique/sequential numbers), not a functional break. Delivered via `fix/thumbnail-migration-renumber`.
