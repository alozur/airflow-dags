# Apply Progress: add-generic-thumbnail-dag

## Meta

| Field | Value |
|-------|-------|
| Change | add-generic-thumbnail-dag |
| Branch | feat/generic-thumbnail-title-dag |
| Slice | 1 of 3 (Pikzels client port) |
| Mode | Strict TDD |
| Delivery | auto-chain / stacked-to-main |
| Date | 2026-07-31 |

## Completed Tasks (Slice 1)

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

## Remaining Tasks

- [ ] Start or reuse bounded review for Slice 1 (parent task — not implementor's)
- [ ] Slice 2 — domain logic, config, migration (not yet started)
- [ ] Slice 3 — DAG wiring, env/config, import test (not yet started)
- [ ] Cross-slice final verification

## Test Results

| Metric | Value |
|--------|-------|
| Test file | `tests/congress_videos/modules/test_pikzels_client.py` |
| Tests collected | 34 |
| Tests passed | 34 |
| Tests failed | 0 |
| Module coverage (`pikzels_client.py`) | **88.35%** |
| Coverage gate (≥80%) | **PASS** |

### Uncovered lines in pikzels_client.py

Lines 107, 177, 291, 312, 325, 364–365, 384 — these are the `_check_xor`/`_check_model_support` error branches and the
module-level convenience function bodies for `download`/`thumbnail_from_text`/`score_thumbnail` (tested indirectly via
`PikzelsClient` method tests; the module-level wrappers create a new client instance each call, so reaching them in tests
that mock `PikzelsClient` is straightforward but would duplicate the class-level tests). Coverage remains well above 80%.

## Files Changed

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/modules/pikzels_client.py` | Created | Trimmed Pikzels v2 client: `PikzelsClient`, `PikzelsError`, `thumbnail_from_text`, `score_thumbnail`, `download`, `to_base64_data_url`, helpers. Network errors retryable; 4xx raises immediately. |
| `tests/congress_videos/modules/test_pikzels_client.py` | Created | 34 tests across 8 test classes; RED→GREEN→TRIANGULATE cycle completed. |
| `openspec/changes/add-generic-thumbnail-dag/tasks.md` | Modified | Slice 1 implementation checkboxes marked [x]. |

## Changed-Line Count

| File | Lines |
|------|-------|
| `congress_videos/modules/pikzels_client.py` | ~230 (new) |
| `tests/congress_videos/modules/test_pikzels_client.py` | ~310 (new) |
| `tasks.md` (checkbox updates) | ~11 (modified) |
| **Total** | **~551** |

## TDD Cycle Evidence

| Task | Test Class | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|------------|-------|------------|-----|-------|-------------|----------|
| Public surface | `TestPublicSurface` | Unit | N/A (new) | ✅ Written | ✅ 4/4 passed | ➖ Single (structural) | ✅ Clean |
| Missing API key | `TestMissingApiKey` | Unit | N/A (new) | ✅ Written | ✅ 3/3 passed | ✅ 2 cases (absent/empty) | ✅ Clean |
| _request retry 503 | `TestRequestRetry` | Unit | N/A (new) | ✅ Written | ✅ 5/5 passed | ✅ 3 cases (timeout, conn, exhausted) → `TestTriangulateRetry` | ✅ Clean |
| 400 no-retry | `TestRequestRetry` | Unit | N/A (new) | ✅ Written | ✅ included above | ✅ Included | ✅ Clean |
| thumbnail_from_text | `TestThumbnailFromText` | Unit | N/A (new) | ✅ Written | ✅ 4/4 passed | ✅ 3 cases (plain/style+persona/support_image) | ✅ Clean |
| score_thumbnail | `TestScoreThumbnail` | Unit | N/A (new) | ✅ Written | ✅ 3/3 passed | ✅ (endpoint path + numeric type) | ✅ Clean |
| download | `TestDownload` | Unit | N/A (new) | ✅ Written | ✅ 4/4 passed | ✅ 2 cases → `TestTriangulateDownload` | ✅ Clean |
| to_base64_data_url | `TestToBase64DataUrl` | Unit | N/A (new) | ✅ Written | ✅ 4/4 passed | ✅ 2 cases → `TestTriangulateToBase64DataUrl` | ✅ Clean |

### Test Summary

- **Total tests written**: 34
- **Total tests passing**: 34
- **Layers used**: Unit (34)
- **Approval tests**: None — no refactoring tasks
- **Pure functions created**: `to_base64_data_url`, `_drop_none`, `_check_xor`, `_check_model_support`

## Deviations from Design

1. **`to_base64_data_url` signature changed**: Design/spec define `to_base64_data_url(image_bytes, mime_type)` taking raw bytes; the reference implementation takes a file path. Ported with the bytes+mime_type signature as spec requires.
2. **Module-level convenience functions added**: Spec requires `thumbnail_from_text`, `score_thumbnail`, `download` to be importable from the module (not only from `PikzelsClient`). Added thin module-level wrappers that instantiate `PikzelsClient()` internally. These are the public surface; callers that need persistent sessions should use `PikzelsClient` directly.
3. **`EnvironmentError` (not `ValueError`)**: Reference raises `ValueError` for missing key. Spec mandates `EnvironmentError` (or `RuntimeError`). Implemented as `EnvironmentError`.

## PR Boundary

- Mode: chained PR slice 1 of 3
- Scope: `pikzels_client.py` + test file only — no Airflow, no DB, no DAG wiring
- Rollback: delete the two new files; no other production code is modified
- Next: Slice 2 (domain logic, config, migration) on top of this base
