# Tasks: shorts_metadata XCom TZ Normalization (issue #546)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~105-120 (prod ~7, tests ~95-110) — matches proposal's ~116 estimate |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending (single PR, no chaining needed) |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Normalize `shorts_metadata` rows at the XCom append site + full regression coverage | PR 1 | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -v` | `bash scripts/test-airflow-e2e.sh` (auto-runs on `sdd-verify`; touches `congress_videos/**`) | Revert the single commit touching `congress_videos/reap_shorts_uploader_dag.py` + `tests/congress_videos/test_reap_uploader_dag.py`; no schema/migration/state to undo |
| 2 | SDD planning-artifact docs commit | PR 1 (separate commit, same PR) | N/A — docs only | N/A — no runtime boundary | Revert the `docs(sdd)` commit independently of the code commit |

## Phase 1: Test Fixtures (Foundation)

- [ ] 1.1 Add module-level imports to `tests/congress_videos/test_reap_uploader_dag.py` (top of file, near `import logging`/`import pytest`): `from datetime import UTC, datetime, timedelta, timezone`.
- [ ] 1.2 Add `_xcom_round_trip` helper (module level, byte-identical to `tests/utils/test_airflow_helpers.py:21-23`) to `tests/congress_videos/test_reap_uploader_dag.py`, near `_make_ti` (~line 113).
- [ ] 1.3 Extend `_make_chapter_metadata` (`tests/congress_videos/test_reap_uploader_dag.py:523`) with `"updated_at": datetime(2024, 3, 1, 10, 0, tzinfo=timezone(timedelta(hours=2)))` — models the raw psycopg2 row; non-zero offset per design D4.
- [ ] 1.4 Extend `_make_short_meta`'s nested `"chapter"` dict (`tests/congress_videos/test_reap_uploader_dag.py:887`) with `"updated_at": datetime(2024, 3, 1, 8, 0, tzinfo=UTC)` — models t2's post-fix normalized output per design D4 (must stay UTC, not a non-zero offset).

## Phase 2: RED — Regression Tests Proving the Defect

- [ ] 2.1 [RED] Add class `TestShortsMetadataXComNormalization` to `tests/congress_videos/test_reap_uploader_dag.py`; write bug-pin test `test_raw_chapter_row_breaks_real_xcom_round_trip` (T1): `_xcom_round_trip({"chapter": _make_chapter_metadata(), "turn_speaker_row": None})` must raise `ValueError, match="ZoneInfo keys must be normalized relative paths"`. Permanent pin — never normalizes, stays red-raising forever.
- [ ] 2.2 [RED] Same class: write `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` (T2 primary): mock `db.get_chapter_metadata` → `_make_chapter_metadata()`, `db.get_turn_speaker_slug` → a row, `os.path.exists` → `False`; call `_generate_metadata(ti)`; round-trip `ti.xcom_store["shorts_metadata"]`. Assert `chapter["updated_at"]` is a `datetime`, `.utcoffset() == timedelta(0)`, and equals the fixture instant.
- [ ] 2.3 [RED] Same class: write `test_generate_metadata_missing_turn_stays_none_after_round_trip`: pending short with no `turn_id` and `_make_chapter_metadata()` as the chapter; round-trip the whole `shorts_metadata` value; assert no exception AND the appended `"turn_speaker_row"` is `None`.
- [ ] 2.4 [RED] Run `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k TestShortsMetadataXComNormalization -v`. Expected: `test_raw_chapter_row_breaks_real_xcom_round_trip` PASSES (permanent pin, doesn't touch prod code); `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` and `test_generate_metadata_missing_turn_stays_none_after_round_trip` FAIL with `ValueError: ZoneInfo keys must be normalized relative paths` — confirms the defect reproduces pre-fix.

## Phase 3: GREEN — Production Fix

- [ ] 3.1 Add `from utils.airflow_helpers import utc_normalize_row` to `congress_videos/reap_shorts_uploader_dag.py`, module-level import block, new line between `from utils.ai_helpers import ...` (line 34) and `from utils.env_loader import ...` (line 35) — isort order (`ai_helpers` < `airflow_helpers`).
- [ ] 3.2 In `congress_videos/reap_shorts_uploader_dag.py::_generate_metadata`, wrap the two `metadata_list.append(...)` values at lines 476-477: `"chapter": utc_normalize_row(ch)`, `"turn_speaker_row": utc_normalize_row(turn_speaker_row)`. Leave the `logging.info` call at line 355 (`ch.get("updated_at")`) untouched — it must keep printing the raw psycopg2 offset as DB-snapshot evidence, per design D1.
- [ ] 3.3 [GREEN] Rerun `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k TestShortsMetadataXComNormalization -v`. Expected: all three tests PASS.

## Phase 4: t2b Downstream Regression Coverage

- [ ] 4.1 In `tests/congress_videos/test_reap_uploader_dag.py::TestVerifyFinalCopyShorts` (~line 894), add `test_verify_final_copy_repush_survives_real_xcom_round_trip` (T3): seed `ti` with `_make_short_meta()` (post-fix UTC `chapter["updated_at"]`) under key `"shorts_metadata"`; mock `verify_final_copy`/DB as the class already does; call `_verify_final_copy(ti)`; round-trip the re-pushed `ti.xcom_store["shorts_metadata"]` (line 592, the second serialization point). Assert no exception — guards a future t2b that re-queries the DB and forgets normalization.
- [ ] 4.2 Run `uv run pytest tests/congress_videos/test_reap_uploader_dag.py::TestVerifyFinalCopyShorts -v`. Expected: the new test plus every pre-existing test in the class passes unchanged — confirms `_copy_verification_evidence` extraction stays byte-identical to pre-fix behavior (spec Requirement 5, "Verification evidence is unchanged by normalization").

## Phase 5: Full Verification

- [ ] 5.1 Run `uv run pytest`. Expected: full suite green, no regressions outside `test_reap_uploader_dag.py`.
- [ ] 5.2 Run `uv run ruff check .` and `uv run ruff format --check .`. Expected: clean; `congress_videos/reap_shorts_uploader_dag.py` stays under the 800-line cap (~759→~763 lines), and the `["B905", "C901"]` per-file ignore at `pyproject.toml:129` is unaffected (no new branches added by the two wraps).
- [ ] 5.3 Runtime harness: `bash scripts/test-airflow-e2e.sh` — this change touches `congress_videos/**`, so it runs automatically during `sdd-verify`. Expected: `airflow dags list-import-errors` empty. If Docker is unavailable, report `unavailable`, not a failure, and run it manually before merge.

## Phase 6: Commits & Follow-ups

- [ ] 6.1 Commit code + tests together as one conventional commit: `fix(congress-videos): normalize shorts_metadata rows at the xcom append site` — covers `congress_videos/reap_shorts_uploader_dag.py` and `tests/congress_videos/test_reap_uploader_dag.py`. No AI attribution in the message body (repo convention).
- [ ] 6.2 Commit the SDD planning artifacts as a SEPARATE commit: `docs(sdd): capture shorts_metadata xcom tz normalization change` — covers `openspec/changes/shorts-metadata-xcom-tz-normalization/**`. Keeps the ~300-380-line planning diff out of the code-review budget, per design's risk note.
- [ ] 6.3 File follow-up issue A via `gh issue create`: "Repo-wide xcom_push serialization guard across ~61 push sites (12 DAGs)" — reference #546 and this change's design "Why this keeps happening" section; explicitly out of scope for this PR per the proposal.
- [ ] 6.4 File follow-up issue B via `gh issue create`: "Latent pending_shorts raw-row XCom risk if video_shorts timestamps migrate to TIMESTAMPTZ" — reference #546; note `video_shorts.created_at`/`updated_at`/`copy_verified_at` are currently naive `TIMESTAMP` (safe today), risk activates only if a future migration adds a timezone.

## Traceability (spec scenario -> task)

| Scenario | Task(s) |
|---|---|
| Chapter row with tz-aware `updated_at` normalized before push | 3.1, 3.2 (fix); 2.2 (RED proof); 3.3 (GREEN proof) |
| Missing turn stays `None` through normalization | 2.3 (RED proof); 3.2 (fix); 3.3 (GREEN proof) |
| Normalized payload round-trips without raising | 2.2 (RED); 3.3 (GREEN) |
| Round-tripped `updated_at` is UTC | 2.2 (assert `.utcoffset() == timedelta(0)`) |
| Un-normalized payload is pinned as failing | 2.1 (permanent bug-pin, T1) |
| Verification evidence is unchanged by normalization | 4.1 (T3 t2b re-push test); 4.2 (existing suite regression check) |

## Key Notes

- Every threat-matrix row is `N/A` per design (no routing/shell/subprocess/VCS boundary changes) — no additional RED tasks required beyond the ones above.
- `_xcom_round_trip` stays a THIRD file-local copy (design D3) — do not extract a shared helper.
- Fixture offsets are asymmetric by design (D4): `_make_chapter_metadata` gets a non-zero `+02:00` offset (raw DB row); `_make_short_meta`'s nested `chapter` gets UTC (post-fix state). Do not make them match.
