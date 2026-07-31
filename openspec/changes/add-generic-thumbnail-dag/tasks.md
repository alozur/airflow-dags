# Tasks: add-generic-thumbnail-dag

Spec: `openspec/changes/add-generic-thumbnail-dag/specs/generic-thumbnail-generation/spec.md`
Design: `openspec/changes/add-generic-thumbnail-dag/design.md` (approved, settled — do not reopen)

Test runner: `uv run pytest` from repo root.
Coverage gate `--cov-fail-under=80` applies to all new modules; during RED/GREEN cycles use `uv run pytest --no-cov <path>` for speed, then run the full suite with coverage before closing each slice.
Strict TDD Mode is ACTIVE: every implementation unit below is sequenced RED → GREEN → TRIANGULATE → REFACTOR. Do not bundle "implement + test" into one step.

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~800–1 050 (pikzels_client ~180 + tests ~130; thumbnail_generation ~150 + tests ~160; migration + config files ~80; DAG wiring ~100 + import test ~40; env/conftest ~15) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1: Pikzels client + tests; PR 2: domain logic + migration + config + tests; PR 3: DAG wiring + env/config wiring + import test |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main (each PR targets the previous merged base) |

```text
Decision needed before apply: No (auto-chain, delivery_strategy=auto-chain)
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: High
```

### Proposed Slice Boundary

- **Slice 1 (PR 1)**: `congress_videos/modules/pikzels_client.py` + full test suite `tests/congress_videos/modules/test_pikzels_client.py`. Self-contained — no Airflow or DB dependency. Est. ~310 lines.
- **Slice 2 (PR 2)**: Domain logic module `congress_videos/modules/thumbnail_generation.py` + config files (`thumbnail_config.py`, prompts, paths additions) + migration `017_create_video_thumbnails.sql` + test suite `tests/congress_videos/modules/test_thumbnail_generation.py`. Depends on Slice 1 merged (imports `pikzels_client`). Est. ~390–465 lines.
- **Slice 3 (PR 3)**: DAG file `congress_videos/generic_thumbnail_generator_dag.py` + env/docker-compose/conftest wiring + DAG import test `tests/congress_videos/modules/test_generic_thumbnail_dag.py`. Depends on Slice 1 and 2 merged. Est. ~155 lines.

If the user declines chaining, all slices can run as a single PR — the task list works either way; only the PR boundary changes.

---

## Slice 1 — Pikzels client port (`congress_videos/modules/pikzels_client.py`)

Satisfies: spec requirement "Pikzels Client — Trimmed Port with Retry/Backoff"; spec requirement "PIKZELS_API_KEY Wired Into All Runtime Surfaces" (client side).

- [x] RED: Create `tests/congress_videos/modules/test_pikzels_client.py`. Write test asserting `from congress_videos.modules.pikzels_client import thumbnail_from_text, score_thumbnail, download, to_base64_data_url` succeeds (module present) and `thumbnail_from_image`, `edit_thumbnail`, `generate_titles` are NOT importable from it (assert `hasattr` returns `False` or `AttributeError` on access). Confirm tests FAIL (module does not exist). <!-- sdd-owner: implementation -->
- [x] RED: Add test: when `PIKZELS_API_KEY` is absent from environment, importing `pikzels_client` or constructing `PikzelsClient()` raises `EnvironmentError` (or `RuntimeError`) with a message naming the missing variable. Use `monkeypatch.delenv("PIKZELS_API_KEY", raising=False)`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `PikzelsClient._request` retries on HTTP 503 — mock `requests.Session.request` to return 503 twice then 200; assert the call count is 3 and the final result is the 200 response. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `PikzelsClient._request` raises immediately on HTTP 400 without retry — mock to return 400 once; assert `PikzelsError` is raised and mock call count is 1. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `thumbnail_from_text` constructs the correct request payload (prompt, support_image, style/persona fields) and returns the parsed response dict; mock `_request` at the client level. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `score_thumbnail` sends the locally-downloaded image (base64-encoded bytes, not a URL) to the score endpoint; mock `_request`; assert payload contains `image_data` field (or equivalent) and returns a numeric score. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `download(url, dest_path)` performs a GET to `url`, creates the parent directory if missing, and writes bytes to `dest_path`; mock `requests.get`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] RED: Add test: `to_base64_data_url(image_bytes, mime_type)` returns the correct `data:<mime>;base64,<b64>` string for a known byte sequence. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Implement `congress_videos/modules/pikzels_client.py` — port/trim the Pikzels v2 client: keep `PikzelsClient.__init__`, `_request` (retry/exponential backoff on 5xx/timeout, immediate raise on 4xx), `thumbnail_from_text`, `score_thumbnail`, `download`, `to_base64_data_url`, `PikzelsError`, `_drop_none`, `_check_xor`, `_check_model_support`; EXCLUDE `thumbnail_from_image`, `edit_thumbnail`, `generate_titles`, all `*_pikzonality*`, `create_persona/style`. Read `PIKZELS_API_KEY` from env at construction time; raise `EnvironmentError` if absent. Run tests until GREEN. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: Add edge-case tests — network `ConnectionError`/`Timeout` is treated as retryable; retry count exhausted raises `PikzelsError`; `download` with an already-existing directory does not raise; `to_base64_data_url` with empty bytes returns a valid (empty-content) data URL string. Confirm all GREEN. <!-- sdd-owner: implementation -->
- [x] REFACTOR: Clean up `pikzels_client.py` (Google-style docstrings, stdlib/third-party import ordering, remove any duplication between retry-path branches) while keeping all tests GREEN. Run `uv run pytest tests/congress_videos/modules/test_pikzels_client.py --no-cov`. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review for Slice 1 (pikzels_client.py + tests only). <!-- sdd-owner: parent -->

---

## Slice 2 — Domain logic, config, migration (`congress_videos/modules/thumbnail_generation.py` + supporting files)

Satisfies: spec requirements "Per-Domain Config Controls Visual Styles…", "Participant Photo Resolution — DB Lookup then HTTP Download", "Exactly 2 Thumbnail Options Generated and Downloaded Immediately", "Each Thumbnail Option Is Scored via `score_thumbnail`", "Best Option Selected by Highest Pikzels Score; Tie-Break is First Option", "Title Generated by OpenAI Only…", "Results Persisted in `video_thumbnails` Table", "Test Coverage at 80% Minimum".

### 2a — Config files and migration (PARALLEL-SAFE with 2b RED tasks)

- [ ] Create `congress_videos/config/thumbnail_config.py` — `THUMBNAIL_CONFIG` dict keyed by domain (`"congreso"` entry); fields: `styles` (list of 2 dicts with style/persona), `participants_lookup` (fn reference to `lookup_participant_fuzzy`), `party_logo_map` (domain → abs path or `None`). Include `ConfigError` exception class. Raise `ConfigError` when requested domain key is absent. No test file for this step — it is exercised by `test_thumbnail_generation.py` coverage. <!-- sdd-owner: implementation -->
- [ ] Modify `congress_videos/config/ai_prompts.py` — add `THUMBNAIL_TITLE_SYSTEM_PROMPT` (dramatic Spanish political-YouTube tone; max 90 chars; no emojis, no `#@|~^`, no channel symbols, no surrounding quotes) and `THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE` (slots: `{summary}`, `{style}`, `{prompt}`). <!-- sdd-owner: implementation -->
- [ ] Modify `congress_videos/config/paths.py` — add `get_thumbnail_dir(youtube_video_id: str) -> Path` returning `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/`. <!-- sdd-owner: implementation -->
- [ ] Create `congress_videos/sql/migrations/017_create_video_thumbnails.sql` — unqualified table/index names (`IF NOT EXISTS`); columns: `thumbnail_id SERIAL PK`, `chapter_id INT NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE`, `youtube_video_id VARCHAR(50)`, `label TEXT NOT NULL`, `style TEXT`, `prompt TEXT`, `main_score NUMERIC(6,3)`, `local_path TEXT NOT NULL`, `output_url TEXT`, `openai_title TEXT`, `is_chosen BOOLEAN DEFAULT FALSE`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`; unique constraint on `(chapter_id, label)`; indexes `idx_video_thumbnails_chapter` and `idx_video_thumbnails_chosen`. <!-- sdd-owner: implementation -->

### 2b — Domain logic module (`thumbnail_generation.py`)

- [ ] RED: Create `tests/congress_videos/modules/test_thumbnail_generation.py`. Write tests for `resolve_participant_photo(name, cfg)`: (a) participant found with non-null `photo_url` → HTTP GET performed, bytes returned as base64-encoded dict `{support_image_b64, source: "photo"}`; (b) participant found but `photo_url` IS NULL AND `party_logo_path` configured → logo file bytes returned, `source: "party_logo"`, no HTTP call; (c) participant found, `photo_url` NULL, no logo → `ValueError` raised; (d) participant not found → `LookupError` raised. Mock `lookup_participant_fuzzy` and `requests.get`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add tests for `generate_and_score_options(prompt, photo_b64, cfg)`: assert exactly 2 calls to `pikzels_client.thumbnail_from_text`; each result immediately triggers `pikzels_client.download` to the correct local path pattern; each option calls `pikzels_client.score_thumbnail`; returns list of 2 dicts with `{label, output_url, local_path, main_score, style}`. Mock all pikzels_client functions. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add tests for `generate_and_score_options` path creation: when local thumbnail directory does not exist, `download` is called (which creates it); assert no `FileNotFoundError` propagates. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add tests for `choose_best_option(options)`: (a) option B score > option A → option B returned with `is_chosen=True`; (b) equal scores → option A returned (tie-break first). Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add tests for `generate_title(summary, best, cfg)`: (a) first OpenAI response is valid (≤90 chars, no emoji, no forbidden chars) → accepted without re-prompt; (b) first response is 110 chars → second call made with shorten instruction, valid second response accepted; (c) first response contains emoji → second call made; (d) both responses invalid → emojis stripped, truncated to 90 chars, `WARNING` logged, task does NOT raise. Mock `utils.ai_helpers.generate_json_completion` directly (never `openai.OpenAI`). Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add test asserting `generate_title` calls `utils.ai_helpers.generate_json_completion` (not any `openai.` direct call) — inspect mock call target. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add tests for `persist_results(chapter_id, youtube_video_id, title, options, best_label)`: both options produce INSERT/upsert rows; chosen option row has `openai_title = title` and `is_chosen = TRUE`; non-chosen row has `openai_title = None`; both rows have correct `local_path`; re-run for same `chapter_id` upserts (no duplicate key error). Mock `psycopg2` connection. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] GREEN: Implement `congress_videos/modules/thumbnail_generation.py` with functions: `resolve_participant_photo`, `generate_and_score_options`, `choose_best_option`, `generate_title`, `persist_results` per design interfaces. Import `pikzels_client` from Slice 1. Use `utils.ai_helpers.generate_json_completion` for title. Use `congress_videos.config.thumbnail_config.THUMBNAIL_CONFIG` for domain config. Use `congress_videos.config.paths.get_thumbnail_dir`. Run tests until GREEN. <!-- sdd-owner: implementation -->
- [ ] TRIANGULATE: Add edge-case tests — `generate_and_score_options` where one Pikzels call raises (task-level exception propagates); `resolve_participant_photo` where HTTP GET for photo returns non-200 (treat as no-photo → fallback path); `choose_best_option` with more than 2 options (still picks max). Confirm all GREEN. <!-- sdd-owner: implementation -->
- [ ] REFACTOR: Clean up `thumbnail_generation.py` (Google-style docstrings, import ordering, no direct `openai` calls at module level) while all tests remain GREEN. Run `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py --no-cov`. <!-- sdd-owner: implementation -->
- [ ] Run coverage check for Slice 2 modules: `uv run pytest tests/congress_videos/modules/test_pikzels_client.py tests/congress_videos/modules/test_thumbnail_generation.py --cov=congress_videos/modules/pikzels_client --cov=congress_videos/modules/thumbnail_generation --cov-fail-under=80` — confirm ≥ 80% coverage. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review for Slice 2 (thumbnail_generation.py + config files + migration + tests). <!-- sdd-owner: parent -->

---

## Slice 3 — DAG wiring, env/config, import test

Satisfies: spec requirements "DAG `generic_thumbnail_generator` Is On-Demand, Config-Driven, and Free of Domain Hardcoding", "Input Contract — Per-Video Run Conf", "Task Graph Shape", "PIKZELS_API_KEY Wired Into All Runtime Surfaces".

### 3a — Env/config wiring (PARALLEL-SAFE with 3b)

- [ ] Modify `.env` — add `PIKZELS_API_KEY=` (placeholder/empty value comment). <!-- sdd-owner: implementation -->
- [ ] Modify all `docker-compose*.yml` — add `PIKZELS_API_KEY` to every Airflow worker/scheduler env block that already mounts runtime env vars. <!-- sdd-owner: implementation -->
- [ ] Modify `conftest.py` — add `PIKZELS_API_KEY: "pkz_test-not-real"` to the `_TEST_ENV` dict (must start with `pkz_` per design note). <!-- sdd-owner: implementation -->

### 3b — DAG file and import test

- [ ] RED: Create `tests/congress_videos/modules/test_generic_thumbnail_dag.py`. Write test: `import congress_videos.generic_thumbnail_generator_dag` completes without raising any exception (DAG loads cleanly). Confirm FAIL (file does not exist). <!-- sdd-owner: implementation -->
- [ ] RED: Add test: loaded DAG object has `schedule == None`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add test: DAG task IDs include exactly `validate_input`, `resolve_participant_photo`, `generate_thumbnail_option_a`, `download_option_a`, `score_option_a`, `generate_thumbnail_option_b`, `download_option_b`, `score_option_b`, `choose_best_option`, `generate_title`, `persist_results` — no more, no fewer. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add test: task dependency graph shape — `resolve_participant_photo` upstream == `{validate_input}`; `generate_thumbnail_option_a` and `generate_thumbnail_option_b` both upstream == `{resolve_participant_photo}`; `choose_best_option` upstream == `{score_option_a, score_option_b}`; `generate_title` upstream == `{choose_best_option}`; `persist_results` upstream == `{generate_title}`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] RED: Add test: DAG source file contains no Congreso-specific string literals (`"congreso"`, `"diputado"`, `"CONGRESO"`) outside of Python import statements. Read the source file in the test and assert via regex. Confirm FAIL (file doesn't exist yet). <!-- sdd-owner: implementation -->
- [ ] RED: Add test for `validate_input` callable: valid conf dict with all 6 keys (`youtube_video_id`, `chapter_id`, `debate_summary`, `session`, `domain`, `normalized_name`) → returns without raising; omitting any one key → `ValueError`; empty string for any key → `ValueError`. Call the underlying Python function directly (not via Airflow trigger). Confirm FAIL. <!-- sdd-owner: implementation -->
- [ ] GREEN: Create `congress_videos/generic_thumbnail_generator_dag.py` — `schedule=None`, `max_active_runs=3`, `default_args` with `retries=2, retry_delay=timedelta(minutes=5)`; `PythonOperator` tasks using `xcom_task` wrapper from `utils.airflow_helpers`; task graph wired per spec; domain config loaded via `THUMBNAIL_CONFIG[conf["domain"]]` (raises `ConfigError` on unknown domain); generate/score tasks have `execution_timeout=timedelta(minutes=10)`; no Congreso-specific literals in task logic, task IDs, or default config keys. Run tests until GREEN. <!-- sdd-owner: implementation -->
- [ ] TRIANGULATE: Add test confirming `validate_input` raises `EnvironmentError` (or that `PikzelsClient` construction raises) when `PIKZELS_API_KEY` is absent, using `monkeypatch.delenv`. Also add a test for the `ConfigError` path when `conf["domain"]` is an unknown key. Confirm all GREEN. <!-- sdd-owner: implementation -->
- [ ] REFACTOR: Clean up DAG file (Google-style docstrings, import ordering stdlib/third-party/local/utils/congress_videos) while all tests remain GREEN. <!-- sdd-owner: implementation -->

---

## Cross-slice final verification (run regardless of chaining decision)

- [ ] Run full test suite with coverage from repo root: `uv run pytest --cov=congress_videos/modules/pikzels_client --cov=congress_videos/modules/thumbnail_generation --cov=congress_videos/generic_thumbnail_generator_dag --cov-fail-under=80` — confirm ≥ 80% and no failures. <!-- sdd-owner: implementation -->
- [ ] Confirm no `openai.` or `OpenAI()` direct instantiation outside `utils/ai_helpers.py` by grepping the new module files — assert clean. <!-- sdd-owner: implementation -->
- [ ] Confirm `pikzels_report.json` is neither created nor referenced in any new file. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review as final gate before archive, referencing all slice reviews. <!-- sdd-owner: parent -->
