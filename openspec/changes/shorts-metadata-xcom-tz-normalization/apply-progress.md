# Apply Progress: shorts_metadata XCom TZ Normalization (issue #546)

**Change**: shorts-metadata-xcom-tz-normalization
**Mode**: Standard (strict_tdd not asserted by config; TDD ordering followed per orchestrator's explicit RED/GREEN instructions)
**Attempt**: acquired via `gentle-ai sdd-attempt acquire` (request-id `546-apply-1`, token `sha256:2513901978e481d995b4718cf9c97cf0ae078601ce961ae8507adeba2d5ca8c8`), state `proceed`. Orchestrator owns settlement.

## Completed Tasks (20/20 implementation tasks; 2 follow-up issue tasks explicitly deferred to orchestrator)

### Phase 1: Test Fixtures
- [x] 1.1 Module-level imports added: `json`, `from datetime import UTC, datetime, timedelta, timezone`, `from airflow.utils.json import XComDecoder, XComEncoder`.
- [x] 1.2 `_xcom_round_trip` helper added at module level near `_make_ti`, byte-identical to `tests/utils/test_airflow_helpers.py:21-23`.
- [x] 1.3 `_make_chapter_metadata` extended with `updated_at` carrying a non-zero `+02:00` offset (models the raw psycopg2 row).
- [x] 1.4 `_make_short_meta`'s nested `chapter` extended with `updated_at` in UTC (models t2's post-fix normalized output).

### Phase 2: RED — Regression Tests Proving the Defect
- [x] 2.1 `TestShortsMetadataXComNormalization.test_raw_chapter_row_breaks_real_xcom_round_trip` (T1 bug-pin) added.
- [x] 2.2 `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` (T2 primary) added.
- [x] 2.3 `test_generate_metadata_missing_turn_stays_none_after_round_trip` added.
- [x] 2.4 RED run executed and observed (see evidence below).

### Phase 3: GREEN — Production Fix
- [x] 3.1 `from utils.airflow_helpers import utc_normalize_row` added between `ai_helpers` and `env_loader` imports.
- [x] 3.2 `metadata_list.append(...)` wraps `ch` and `turn_speaker_row` in `utc_normalize_row(...)`. Operator log line (`ch.get("updated_at")`) left untouched — still prints the raw psycopg2 offset.
- [x] 3.3 GREEN run executed and observed (see evidence below).

### Phase 4: t2b Downstream Regression Coverage
- [x] 4.1 `TestVerifyFinalCopyShorts.test_verify_final_copy_repush_survives_real_xcom_round_trip` (T3) added.
- [x] 4.2 Full `TestVerifyFinalCopyShorts` class run: 6/6 pass (5 pre-existing + 1 new), no regressions.

### Phase 5: Full Verification
- [x] 5.1 `uv run pytest` (full suite): **5089 passed, 34 skipped**, exit 0.
- [x] 5.2 `uv run ruff check .`: "All checks passed!". `uv run ruff format --check .`: "313 files already formatted".
- [ ] 5.3 `bash scripts/test-airflow-e2e.sh`: **unavailable** — Docker daemon socket returned `permission denied while trying to connect to the docker API at unix:///var/run/docker.sock` in this sandbox. Must be run manually before merge (per instructions, this is not a failure).

### Phase 6: Commits & Follow-ups
- [x] 6.1 Code + tests committed as one conventional commit `fix(congress-videos): normalize shorts_metadata rows at the xcom append site` (commit `fe4faf5`), covering `congress_videos/reap_shorts_uploader_dag.py`, `tests/congress_videos/test_reap_uploader_dag.py`, and this change's `tasks.md` checkbox updates. No AI attribution in the commit body per repo convention (trailers only, per session instructions).
- [x] 6.2 `docs(sdd)` planning-artifact commit already existed on the branch before this apply run (commit `71c789a`) — verified, not redone.
- [ ] 6.3 Follow-up issue A (repo-wide xcom_push serialization guard) — **NOT filed**. Explicitly deferred to the orchestrator per session instructions.
- [ ] 6.4 Follow-up issue B (latent `pending_shorts` raw-row XCom risk) — **NOT filed**. Explicitly deferred to the orchestrator per session instructions.

## Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k TestShortsMetadataXComNormalization -v --no-cov` → 3 passed (post-fix); `uv run pytest tests/congress_videos/test_reap_uploader_dag.py::TestVerifyFinalCopyShorts -v --no-cov` → 6 passed |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → unavailable (Docker daemon unreachable in this sandbox: permission denied on `/var/run/docker.sock`). Full `uv run pytest` used as the substitute in-sandbox runtime check: 5089 passed, 34 skipped |
| Rollback boundary | Revert commit `fe4faf5` alone; no schema/migration/state change to undo. `docs(sdd)` commit `71c789a` is independently revertible |

## RED/GREEN Evidence (mandatory, actually observed)

### RED (task 2.4) — before the production fix

Command: `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k TestShortsMetadataXComNormalization -v --no-cov`

```
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_raw_chapter_row_breaks_real_xcom_round_trip PASSED
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip FAILED
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_generate_metadata_missing_turn_stays_none_after_round_trip FAILED

E   ValueError: ZoneInfo keys must be normalized relative paths, got:
```

Both failures raised from inside `_xcom_round_trip` -> `json.loads(..., cls=XComDecoder)` -> `pendulum.timezone(name)` -> `zoneinfo._tzpath._validate_tzfile_path`, exactly the defect described in the proposal. T1 (bug-pin) passed as expected — it is a permanent pin, not a RED test, and does not touch production code.

### GREEN (task 3.3) — after the production fix

Command: `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k TestShortsMetadataXComNormalization -v --no-cov`

```
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_raw_chapter_row_breaks_real_xcom_round_trip PASSED
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip PASSED
tests/congress_videos/test_reap_uploader_dag.py::TestShortsMetadataXComNormalization::test_generate_metadata_missing_turn_stays_none_after_round_trip PASSED

======================= 3 passed, 82 deselected in 2.33s =======================
```

## Files Changed

| File | Action | What Was Done |
|---|---|---|
| `congress_videos/reap_shorts_uploader_dag.py` | Modified | +1 import (`utc_normalize_row`); wrapped `chapter`/`turn_speaker_row` values in `utc_normalize_row(...)` inside `metadata_list.append(...)`. Operator log line unchanged (still raw psycopg2 offset). Net +6 lines (comment + wraps). |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modified | +3 module-level imports; `_xcom_round_trip` helper (3rd file-local copy, per design D3); `updated_at` added to `_make_chapter_metadata` (non-zero offset) and `_make_short_meta`'s nested `chapter` (UTC); new `TestShortsMetadataXComNormalization` class (T1/T2/T2b, 3 tests); new `test_verify_final_copy_repush_survives_real_xcom_round_trip` (T3) in `TestVerifyFinalCopyShorts`. +126/-18 net. |
| `openspec/changes/shorts-metadata-xcom-tz-normalization/tasks.md` | Modified | Checked off tasks 1.1-1.4, 2.1-2.4, 3.1-3.3, 4.1-4.2, 5.1-5.2. Left 5.3 (Docker e2e), 6.3, 6.4 unchecked. |

## Deviations from Design

None — implementation matches design.md exactly:
- Normalization applied inside `metadata_list.append(...)`, not early in the function (D1).
- `turn_speaker_row` also normalized (D2).
- `_xcom_round_trip` stays a third file-local module-level copy, not extracted (D3).
- Fixture asymmetry preserved as specified: `_make_chapter_metadata` gets non-zero `+02:00`, `_make_short_meta`'s nested `chapter` gets UTC (D4).
- Import placed as new line between `utils.ai_helpers` and `utils.env_loader` (D5).

One addition beyond the literal task text: test 2.2 (`test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip`) also mocks `lookup_participant_by_slug` — the un-mocked call would otherwise attempt a real Postgres connection (observed as a caught, logged `WARNING` in the RED run, not a test failure). Mocking it keeps the test hermetic and matches the pattern already used by `TestGenerateMetadataTurnSpeakerPrecedence`. This is a test-hygiene addition, not a scope or assertion change.

## Issues Found

None beyond the pre-existing, expected Postgres-unavailable `SKIPPED` tests in the full suite (34 skipped total, unrelated to this change — live-DB tests documented elsewhere as needing NAS Postgres access).

## Remaining Tasks (owned by orchestrator, not this apply run)

- [ ] 5.3 Run `bash scripts/test-airflow-e2e.sh` manually before merge (Docker unavailable in this sandbox).
- [ ] 6.3 File follow-up issue A: repo-wide `xcom_push` serialization guard (61 sites, 12 DAGs).
- [ ] 6.4 File follow-up issue B: latent `pending_shorts` raw-row XCom risk if `video_shorts` timestamps migrate to TIMESTAMPTZ.
- [ ] Push branch `fix/546-shorts-metadata-xcom-tz` and open PR (delivery is orchestrator-owned).

## Status

20/20 implementation tasks complete (Phases 1-5 code/test work + Phase 6.1 commit). 3 tasks explicitly out of this apply run's scope (5.3 Docker-blocked, 6.3/6.4 orchestrator-owned). Ready for sdd-verify.
