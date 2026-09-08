# Apply Progress: issue #499

## Slice 1 — prevent future parent pollution

- Implemented the `turn_id is not None` branch in `mark_chapter_uploads`.
- It records a `turn_upload` skip and leaves `mark_turn_uploads` as the sole writer for turn results.
- The legacy successful detail test remains the regression for a detail without `turn_id`.
- Strict-TDD evidence: the new test initially failed because the function called `mark_chapter_uploaded`; after implementation, the focused suite passed: `35 passed`.

## Validation

- `uv run pytest`: **4811 passed, 32 skipped**, coverage **90.78%**.
- `bash scripts/test-airflow-e2e.sh`: Docker daemon unavailable; script exited **4** as the documented non-failure path.

## Slice 2 — pending

The guarded repair command and its tests are intentionally held for a second, review-bounded PR. It will be stack-based on Slice 1 and is not an executable production action until an exact preflight result receives separate approval.
