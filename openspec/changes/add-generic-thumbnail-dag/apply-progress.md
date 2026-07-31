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

## Remaining Tasks

- [ ] Start or reuse bounded review for Slice 2 (parent task)
- [ ] Slice 3 — DAG wiring, env/config, import test (not yet started)
- [ ] Cross-slice final verification

---

## Files Changed

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

## PR Boundary

- Mode: chained PR slice 2 of 3
- Scope: `thumbnail_generation.py` + config files (`thumbnail_config.py`, ai_prompts additions, paths addition) + migration `017` + test file
- Depends on: Slice 1 merged (imports `pikzels_client`)
- Rollback: delete the 3 new files, revert the 2 modified config files; migration requires `DROP TABLE video_thumbnails` manually
- Next: Slice 3 (DAG wiring + env + import test) on top of Slice 2 base
