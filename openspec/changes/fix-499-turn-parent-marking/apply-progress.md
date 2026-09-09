# Apply Progress: issue #499

## Slice 1 — prevent future parent pollution

- Implemented the `turn_id is not None` branch in `mark_chapter_uploads`.
- It records a `turn_upload` skip and leaves `mark_turn_uploads` as the sole writer for turn results.
- The legacy successful detail test remains the regression for a detail without `turn_id`.
- Strict-TDD evidence: the new test initially failed because the function called `mark_chapter_uploaded`; after implementation, the focused suite passed: `35 passed`.

## Validation

- `uv run pytest`: **4811 passed, 32 skipped**, coverage **90.78%**.
- `bash scripts/test-airflow-e2e.sh`: Docker daemon unavailable; script exited **4** as the documented non-failure path.

## Slice 2 — guarded repair command

- Added `scripts/repair_orphaned_turn_chapters.py`; dry run is the default and `--execute` repeats the full safety predicate inside its transaction.
- Unit tests cover the exact allowlist, candidate predicate, execute predicate, dry-run default and opt-in execution.
- A production read-only preflight using the same command found exactly four qualified chapters: 263 (`195–204, 210–211` pending), 265 (`236, 242`), 266 (`244, 245, 248`) and 519 (`320–322`). Chapter 264 has no prepared pending turn, so it correctly does not qualify.
- This is not an authorization to mutate production. The final operational task remains open until the exact four-id set receives explicit approval.
