# Tasks: Freshness bucket as the leading publish-order key for `uploadable_turns`

Issue #513. Strict TDD, RED-first, per design.md's normative recipe. No application code changes.

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~262 (~175 new migration, ~17 snapshot, ~70 test file) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

Migration 049 is a near-verbatim ~100-line body copy of 044 (copy-then-patch, per D1); the raw diff
will read large but the semantic change is exactly one leading `ORDER BY` key. Counted honestly
above, not discounted — still well under the 400-line budget.

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Freshness bucket key, migration 049, snapshot sync, test suite | PR 1 | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/utils/test_migrations_dag.py` | `bash scripts/test-airflow-e2e.sh` (auto-triggers: change touches `congress_videos/**`) | Revert the PR: delete `049_freshness_bucket_turn_publish_order.sql`, revert `production_schema.sql` and test edits; no data migrated, order-only |

## Phase 1: RED — repoint fixtures for migration 049

- [x] 1.1 In `tests/congress_videos/sql/test_production_schema.py`: repoint `MIGRATION_PATH` (line 18) from `044_deterministic_turn_publish_order.sql` to `049_freshness_bucket_turn_publish_order.sql`.
- [x] 1.2 Add new constant `MIGRATION_044_PATH` (same shape as `MIGRATION_040_PATH`) pointing at the 044 file.
- [x] 1.3 Repoint `test_migration_044_body_is_040_plus_tiebreak`'s `MIGRATION_PATH` read (line 631) to `MIGRATION_044_PATH`; the 040-vs-044 splice and assertion stay unchanged.
- [x] 1.4 Rename `test_normalized_view_matches_migration_044` to `test_normalized_view_matches_migration_049`; update its docstring/failure message to say 049.
- [x] 1.5 Update module docstring (line 5): "currently 044" to "currently 049".
- [x] 1.6 Verify: `uv run pytest tests/congress_videos/sql/test_production_schema.py` — expect `test_normalized_view_matches_migration_049` to fail with `FileNotFoundError` (first RED). `test_migration_044_body_is_040_plus_tiebreak` stays green.

## Phase 2: RED — transcription guard for migration 049

- [x] 2.1 In `tests/congress_videos/sql/test_production_schema.py`, add class `TestUploadableTurns049FreshnessBucket` with constants `_LEADING_KEY`, `_OUTER_ORDER_BY_044`, `_FULL_ORDER_BY`, and method `test_migration_049_body_is_044_plus_leading_freshness_key` (splices 044's normalized body via `.replace(_OUTER_ORDER_BY_044, ...)` and compares to normalized 049) — exact code in design.md.
- [x] 2.2 Verify: this new test fails with `FileNotFoundError` (049 file does not exist yet).

## Phase 3: GREEN — create migration 049

- [x] 3.1 Copy `congress_videos/sql/migrations/044_deterministic_turn_publish_order.sql` (read-only) byte-for-byte to `congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql`, then apply exactly hunks H1 (replace lines 1-39, the header), H2 (replace line 106 with the 7-line freshness-bucket `ORDER BY` block; lines 107-119 untouched), H3 (replace lines 121-127 header prose and the DOWN block's trailing `ORDER BY` lines 166-167). All text is verbatim in design.md. Everything else stays byte-identical to 044. `-- DOWN` block stays entirely commented out.
- [x] 3.2 Verify: `test_migration_049_body_is_044_plus_leading_freshness_key` passes. `test_normalized_view_matches_migration_049` still fails (snapshot not yet synced).

## Phase 4: RED — snapshot-facing assertions

- [x] 4.1 Add `test_outer_order_by_leads_with_freshness_bucket` and `test_freshness_key_carries_issue_513_intent_comment` to `TestUploadableTurns049FreshnessBucket` (exact code in design.md).
- [x] 4.2 Verify: both fail against the un-synced `production_schema.sql`.

## Phase 5: GREEN — sync production_schema.sql

- [x] 5.1 `congress_videos/sql/production_schema.sql` line 537: `-- View: uploadable_turns (migration 044)` to `(migration 049)`.
- [x] 5.2 After line 552, append the 049 lineage note (exact text in design.md).
- [x] 5.3 Line 618: replace with hunk H2 verbatim (same text as the migration; `dedup` alias is unqualified). Lines 619-631 unchanged.
- [x] 5.4 Line 633 `COMMENT ON VIEW`: extend the existing string with the order-clause sentence (exact text in design.md); keep the `COMMENT ON VIEW production.uploadable_turns IS '` prefix; no `CREATE VIEW` token in the added text.
- [x] 5.5 Verify: `uv run pytest tests/congress_videos/sql/test_production_schema.py` — Phase 2/4 tests and the lockstep test pass. `TestUploadableTurns044PublishOrder` now fails on `_TIEBREAK_SUFFIX` and `marker` — expected per D4, not a regression.

## Phase 6: GREEN — D4 literal fixes (044 test class)

- [x] 6.1 In `test_outer_order_by_is_editorial_keys_then_fifo_tiebreak`, delete the `"ORDER BY "` prefix from `_TIEBREAK_SUFFIX`'s first line only (`"ORDER BY COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, "` to `"COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, "`); the other four lines are untouched.
- [x] 6.2 In `test_tiebreak_carries_fifo_intent_comment`, change `marker` from `"ORDER BY COALESCE(dedup.interest_score"` to `"ORDER BY (dedup.session_date >="`; `FIFO`/`#328` assertions stay as-is.
- [x] 6.3 Verify: `uv run pytest tests/congress_videos/sql/test_production_schema.py` fully green.

## Phase 7: Verify

- [x] 7.1 Run `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/utils/test_migrations_dag.py` — all green, including the repo-wide `TestMigrationIdempotency` glob picking up the new 049 file automatically.
- [x] 7.2 Run full suite: `uv run pytest`.
- [x] 7.3 Run `uv run ruff check .` and `uv run ruff format --check .` (both are separate blocking steps in `.github/workflows/lint.yml`).
- [x] 7.4 If Docker is available, run `bash scripts/test-airflow-e2e.sh` and confirm `airflow dags list-import-errors` is empty; otherwise report `unavailable` and note it must run manually before merge.
