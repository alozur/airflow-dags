# Apply Progress: Freshness bucket as the leading publish-order key for `uploadable_turns`

Issue #513. Change `uploadable-turns-freshness-bucket`. Strict TDD, RED-first.

## Status

35/35 tasks complete across 7 phases. All verification green. Ready for `sdd-verify`.

## Completed Tasks

All tasks in `tasks.md` are marked `[x]`:

- Phase 1: RED — repoint fixtures for migration 049 (1.1–1.6)
- Phase 2: RED — transcription guard for migration 049 (2.1–2.2)
- Phase 3: GREEN — create migration 049 (3.1–3.2)
- Phase 4: RED — snapshot-facing assertions (4.1–4.2)
- Phase 5: GREEN — sync `production_schema.sql` (5.1–5.5)
- Phase 6: GREEN — D4 literal fixes (044 test class) (6.1–6.3)
- Phase 7: Verify (7.1–7.4)

## Files Changed

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql` | Created | Copy-then-patch of 044 (hunks H1 header, H2 leading `ORDER BY` key, H3 DOWN prose + rollback `ORDER BY`); `-- DOWN` block stays entirely commented out |
| `congress_videos/sql/production_schema.sql` | Modified | Header lineage line 537 → `(migration 049)`; new lineage bullet after 552; outer `ORDER BY` synced with H2; `COMMENT ON VIEW` extended with the publish-order sentence |
| `tests/congress_videos/sql/test_production_schema.py` | Modified | `MIGRATION_PATH` repointed to 049; new `MIGRATION_044_PATH`; module docstring updated; lockstep test renamed to `..._049`; new `TestUploadableTurns049FreshnessBucket` (3 tests); D4 literal fixes on `TestUploadableTurns044PublishOrder` (`_TIEBREAK_SUFFIX`, `marker`) |
| `openspec/changes/uploadable-turns-freshness-bucket/tasks.md` | Modified | All 35 tasks marked `[x]` |

## TDD Cycle Evidence

| Step | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 1.1–1.6 | `test_production_schema.py` | Unit (static SQL text) | 217/217 passing baseline | Written (fixtures repointed) | Confirmed: `test_normalized_view_matches_migration_049` fails `FileNotFoundError`; `test_migration_044_body_is_040_plus_tiebreak` stays green | N/A — fixture repoint | N/A |
| 2.1–2.2 | `test_production_schema.py` | Unit (static SQL text) | N/A (new class) | Written: `test_migration_049_body_is_044_plus_leading_freshness_key` | Confirmed: fails `FileNotFoundError` (049 file absent) | N/A — single scenario per Phase 3 gate | N/A |
| 3.1–3.2 | `049_freshness_bucket_turn_publish_order.sql` | Production SQL | N/A (new file) | N/A | Confirmed: `test_migration_049_body_is_044_plus_leading_freshness_key` PASSES; lockstep test still fails (snapshot un-synced) | N/A | N/A — copy-then-patch, no free-form logic to refactor |
| 4.1–4.2 | `test_production_schema.py` | Unit (static SQL text) | N/A (new tests) | Written: `test_outer_order_by_leads_with_freshness_bucket`, `test_freshness_key_carries_issue_513_intent_comment` | Confirmed: both fail against un-synced snapshot | 2 independent scenarios (position/direction; inline comment) | N/A |
| 5.1–5.5 | `production_schema.sql` | Production SQL | N/A | N/A | Confirmed: Phase 2/4 tests + lockstep test pass. `TestUploadableTurns044PublishOrder` fails on `_TIEBREAK_SUFFIX`/`marker` — predicted by design D4, not a regression | N/A | N/A |
| 6.1–6.3 | `test_production_schema.py` | Unit (static SQL text) | N/A | N/A (literal fix, not new behavior) | Confirmed: full `test_production_schema.py` green (221 passed) | N/A | N/A — mechanical literal edits per D4 |

### Test Summary
- Total tests written: 3 new (`TestUploadableTurns049FreshnessBucket`), 2 existing literals fixed
- Total tests passing: 221/221 in `test_production_schema.py`; 440/440 in `test_production_schema.py` + `test_migrations_dag.py`
- Layers used: Unit (static SQL text) only — matches design's Testing Strategy (no integration/E2E layer added; none applicable)
- Triangulation: 2 independent scenarios in Phase 4 (position/direction check, inline-comment check), each against a different assertion path
- Pure functions created: 0 (no application code changed, per design)

## Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/utils/test_migrations_dag.py` → **440 passed** |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → **unavailable** (`Docker daemon is not reachable (docker info failed); skipping e2e (unavailable)`) — must run manually before merge per repo convention; not a failure |
| Rollback boundary | Revert commit `bd0ac11`: deletes `049_freshness_bucket_turn_publish_order.sql`, reverts `production_schema.sql` and `test_production_schema.py` edits. No data migrated; the change is order-only and the migration's `-- DOWN` block documents the manual `psql` rollback |

## Full Verification

| Command | Result |
|---|---|
| `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/utils/test_migrations_dag.py` | 440 passed |
| `uv run pytest` (full suite) | 4836 passed, 34 skipped (pre-existing live-Postgres/opt-in skips, unrelated to this change) |
| `uv run ruff check .` | All checks passed |
| `uv run ruff format --check .` | 305 files already formatted (after applying `ruff format` once to the new test additions to match repo style — no assertion text changed) |
| `bash scripts/test-airflow-e2e.sh` | unavailable (Docker daemon unreachable in this environment) |

## Deviations from Design

None — implementation matches design.md exactly, including all four `production_schema.sql` edit points and the D4 literal fixes. One mechanical formatting pass (`ruff format`) was applied to the new test assertions' error-message string wrapping to satisfy the repo's `ruff format --check` gate; no assertion text, literal, or logic changed.

## Issues Found

None.

## Commits

- `bd0ac11` — `feat(sql): prepend a 14-day freshness bucket to uploadable_turns order` (migration 049, `production_schema.sql` sync, test suite changes, `tasks.md` checkboxes)

## Workload / PR Boundary

- Mode: single PR (per tasks.md forecast: `400-line budget risk: Low`, `Chained PRs recommended: No`)
- Current work unit: Unit 1 — "Freshness bucket key, migration 049, snapshot sync, test suite"
- Boundary: starts from `origin/dev` (== `origin/main` == 8868432) plus the already-committed SDD artifacts (`52ba968`); ends with commit `bd0ac11`
- Authored changed lines: 296 insertions + 34 deletions across 3 modified files + 1 new file (182 lines) — within the 400-line budget, consistent with the ~262-line forecast (raw diff reads larger because migration 049's ~175-line body is a near-verbatim copy of 044, counted honestly per the forecast note)

### Status
35/35 tasks complete. Ready for verify.
