```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:81f4d6f669303e28d98c6d4b61d8cd3f2b8b41687a56f99a74995d3a8e47007f
verdict: pass
blockers: 0
critical_findings: 0
requirements: 7/7
scenarios: 11/11
test_command: uv run pytest -q
test_exit_code: 0
test_output_hash: sha256:22cbe6a4d1440d798b35d168bef10ce1a75a1395963111940b97bae2cf5f8c1f
build_command: uv run ruff check .
build_exit_code: 0
build_output_hash: sha256:82b3e6a6c090a57601d22943bd23fca9218d1031dbe5a7b754092f9a156b4f18
```

## Verification Report

**Change**: persist-title-generator-inputs (issue #549)
**Version**: N/A (single spec revision)
**Mode**: Standard
**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-549`, branch `feat/549-slice3-shorts-path` @ `edb557f` (tip of the 3-slice chain: `bc26c29` → `6e268a7` → `868f0a4` → `c9d0e31`)

### Completeness

| Metric | Value |
|--------|-------|
| Tasks total | 33 (task IDs 1.1-1.9, 2.1-2.13, 3.1-3.10 including 3.5b) |
| Tasks complete | 33 |
| Tasks incomplete | 0 |

### Build & Tests Execution

**Build (lint)**: ✅ Passed
```text
$ uv run ruff check .
All checks passed!
exit 0

$ uv run ruff format --check .
313 files already formatted
exit 0
```

**Tests**: ✅ 5147 passed / ❌ 0 failed / ⚠️ 34 skipped
```text
$ uv run pytest -q
5147 passed, 34 skipped in ~105-111s
exit 0
```
The 34 skips are all live-Postgres-dependent tests, correctly skipped in a
Postgres-less sandbox (connection refused to localhost:5432), plus one SRT
pathological-guard test whose generated fixture is too small in this env —
both pre-existing skip conditions unrelated to this change.

**E2E harness**: `bash scripts/test-airflow-e2e.sh` → `unavailable` (Docker
daemon not reachable in this sandbox), exit 0. Per `CLAUDE.md`'s documented
convention this is reported as `unavailable`, not a failure. Task 3.10 already
flagged this in apply-progress.md; it must be run manually against a real
Docker host before merge to confirm `airflow dags list-import-errors` stays
empty for the three modified DAGs (`generic_thumbnail_generator_dag.py`,
`youtube_upload_dag.py`, `reap_shorts_uploader_dag.py`).

**Coverage**: not separately measured; full-suite pass with 0 failures across
all touched modules is the coverage evidence available in this environment.

**Full-chain diffstat** (`git diff --stat f6ff2e4..HEAD`, `f6ff2e4` = merge of
PR #548 into main, the chain's base):
```text
 congress_videos/generic_thumbnail_generator_dag.py             |  21 ++
 congress_videos/modules/database.py                            |  97 +++++
 congress_videos/modules/thumbnail_generation.py                |  70 ++++
 congress_videos/reap_shorts_uploader_dag.py                    | 126 +++++++
 congress_videos/sql/migrations/051_persist_title_generation_input.sql | 12 +
 congress_videos/sql/production_schema.sql                      |   8 +-
 congress_videos/youtube_upload_dag.py                          |  64 +++-
 openspec/changes/persist-title-generator-inputs/*.md            | 1311 ++++++
 tests/congress_videos/modules/test_database.py                 | 158 ++++++++
 tests/congress_videos/modules/test_generic_thumbnail_dag.py    |  68 ++++
 tests/congress_videos/modules/test_thumbnail_generation.py     | 176 ++++++++++
 tests/congress_videos/sql/test_production_schema.py            |   7 +-
 tests/congress_videos/test_reap_uploader_dag.py                | 389 +++++++++++++++++++
 tests/congress_videos/test_youtube_upload_dag.py               | 144 ++++++++
 19 files changed, 2817 insertions(+), 4 deletions(-)
```
Excluding `openspec/**` planning artifacts: 13 files, 1336 insertions(+), 4
deletions(-) of production+test code. All 4 deletions are internal
(docstring text, migration/column-count comments) — zero lines of pre-existing
production logic were removed; the change is additive throughout.

`evidence_revision` = sha256 of `git diff f6ff2e4..HEAD` (full diff, including
`openspec/**`): `sha256:81f4d6f669303e28d98c6d4b61d8cd3f2b8b41687a56f99a74995d3a8e47007f`.

### Spec Compliance Matrix

Note on scenario count: the spec.md file contains **11** `#### Scenario`
headings (verified via `rg -c '^#### Scenario'`), not the 12 referenced in the
launch prompt. `tasks.md`'s own traceability table documents why: Scenario 3.2
is one heading holding two behaviorally distinct GIVEN/WHEN/THEN blocks
(re-run overwrite = "3.2a", non-matching-key-is-loud = "3.2b"). Both behaviors
are independently tested. This report counts by heading (11/11, matching the
native `#### Scenario:` counting rule) and separately confirms both 3.2
sub-behaviors below.

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Req 1 | 1.1 Title generated for a turn | `test_generic_thumbnail_dag.py::TestTaskThumbnailResult::test_returns_uploadable_result_after_persistence` (L571-598); write-site timing confirmed structurally: `trigger_thumbnail_generation` (youtube_upload_dag.py:874-879) writes provenance immediately on receiving `result`, before `thumbnail_result` XCom push and before any upload-outcome logic | ✅ COMPLIANT |
| Req 2 | 2.1 Shorts LLM generation | `test_reap_uploader_dag.py::TestBuildShortsTitlePayload::test_transcript_over_2000_chars_is_sliced_and_flagged_truncated` (L1767) + `TestGenerateMetadataTitleProvenance::test_llm_title_triggers_write_with_full_transcript_payload_keyed_by_short_id` (L1858) | ✅ COMPLIANT |
| Req 2 | 2.2 Fallback branch persists nothing | `test_reap_uploader_dag.py::TestGenerateMetadataTitleProvenance::test_empty_transcript_skips_llm_branch_and_records_skipped` (L1897) | ✅ COMPLIANT |
| Req 3 | 3.1 Grouped-turn write touches only its own siblings | `test_database.py::TestRecordTitleGenerationInputTurn::test_grouped_siblings_update_by_output_path_only` (L904) | ✅ COMPLIANT |
| Req 3 | 3.2a Re-run overwrites, rowcount >= 1 | `test_database.py::TestRecordTitleGenerationInputTurn::test_rerun_same_output_path_overwrites_without_raising` (L921) | ✅ COMPLIANT |
| Req 3 | 3.2b Non-matching key is loud (turn) | `test_youtube_upload_dag.py::TestTriggerThumbnailGenerationTitleProvenance::test_zero_rows_is_no_row_not_success` (L2960) | ✅ COMPLIANT |
| Req 3 | 3.2b Non-matching key is loud (shorts) | `test_reap_uploader_dag.py::TestGenerateMetadataTitleProvenance::test_zero_rows_is_no_row_not_success` (L1964) | ✅ COMPLIANT |
| Req 4 | 4.1 Turn payload round-trip | `test_thumbnail_generation.py::TestBuildTurnTitlePayload::test_round_trip_replays_generate_title_from_stored_fields_only` (L4169) | ✅ COMPLIANT |
| Req 4 | 4.1 Shorts payload round-trip | `test_reap_uploader_dag.py::TestBuildShortsTitlePayload::test_round_trip_renders_template_from_stored_fields_only` (L1814) | ✅ COMPLIANT |
| Req 5 | 5.1 Forced DB failure during turn write | `test_youtube_upload_dag.py::TestTriggerThumbnailGenerationTitleProvenance::test_db_exception_is_caught_and_publication_continues` (L2975) | ✅ COMPLIANT |
| Req 5 | 5.2 Forced DB failure during shorts write | `test_reap_uploader_dag.py::TestGenerateMetadataTitleProvenance::test_db_failure_for_one_short_does_not_abort_loop` (L1937) | ✅ COMPLIANT |
| Req 6 | 6.1 Schema-only, credential-free (turn) | `test_thumbnail_generation.py::TestBuildTurnTitlePayload::test_declared_keys_only` + `test_no_credentials_or_urls_or_paths_in_serialized_payload` (L4080/L4097) | ✅ COMPLIANT |
| Req 6 | 6.1 Schema-only, credential-free (shorts) | `test_reap_uploader_dag.py::TestBuildShortsTitlePayload::test_declared_keys_only` + `test_no_credentials_or_urls_or_paths_in_serialized_payload` (L1747/L1754) | ✅ COMPLIANT |
| Req 7 | 7.1 Empty-title guard still raises | `test_youtube_upload_dag.py::TestPrepareUploadConfigTurnRequiresThumbnailTitle::test_raises_and_pushes_no_upload_config` (L3411, pre-existing, untouched, still green) | ✅ COMPLIANT |
| Req 7 | 7.2 #512 verification seam still runs (turn) | `test_youtube_upload_dag.py::TestVerifyFinalCopy::test_description_reject_persists_and_does_not_raise` (L898, pre-existing, `record_copy_verification_turn.assert_called_once()`) | ✅ COMPLIANT |
| Req 7 | 7.2 #512 verification seam still runs (shorts) | `test_reap_uploader_dag.py::TestGenerateMetadataTitleProvenance::test_record_copy_verification_short_still_invoked_unchanged` (L2011) | ✅ COMPLIANT |

**Compliance summary**: 16/16 behavioral scenario blocks compliant (11/11 spec.md `#### Scenario` headings, each with all of its GIVEN/WHEN/THEN blocks independently covered by a passing test).

### Correctness (Static Evidence) — Design corrections C1-C4

| Correction | Verified | Evidence |
|---|---|---|
| C1a: migration 051 adds column to both tables | ✅ | `congress_videos/sql/migrations/051_persist_title_generation_input.sql`: two `ADD COLUMN IF NOT EXISTS title_generation_input JSONB` on `speaker_turn_videos` and `video_shorts` |
| C1a: migration DOWN block commented out | ✅ | Lines 10-12 of the migration are `-- ` prefixed SQL comments only; no active `DROP COLUMN` statement |
| C1b: production_schema.sql mirrors both ADD COLUMN | ✅ | `production_schema.sql:179` (`video_shorts` block, before `-- Table: llm_cache`) and `:376` (`speaker_turn_videos` block, before `CONSTRAINT uq_speaker_turn_videos_turn UNIQUE (turn_id)`) |
| C1c: test_production_schema.py lists column for both tables | ✅ | `TABLE_COLUMNS["speaker_turn_videos"]` (L246) and the separate `VIDEO_SHORTS_COLUMNS` constant inside `TestVideoShortsTableSnapshot` (L496) — confirmed `video_shorts` genuinely lives in a distinct constant, not `TABLE_COLUMNS`, matching the design's C1 finding |
| C2: turn call site uses explicit try/except, never bare | ✅ | `youtube_upload_dag.py:753-791` `_write_title_provenance`: `try: rows = ... except Exception as exc: logging.error(...); return {"status": "failed", ...}` — never re-raises, never silently swallows (always returns a typed status dict) |
| C2: shorts call site uses explicit try/except, never bare | ✅ | `reap_shorts_uploader_dag.py:348-380` `_write_shorts_title_provenance`, identical try/except shape |
| C2: neither copies `record_copy_verification_turn`'s bare-call shape | ✅ | Confirmed by direct code read of both new call sites — both wrap the DB call in `try/except`, unlike the unwrapped `record_copy_verification_turn`/`_short` calls elsewhere in the same files |
| C3: `build_shorts_title_payload` receives full transcript | ✅ | Call site `reap_shorts_uploader_dag.py:571-580`: `build_shorts_title_payload(transcript, ...)` passes the full in-scope `transcript` variable, NOT the `transcript[:2000]` slice used for the prompt at line 546; the function itself slices `transcript[:2000]` internally (L335) and computes `transcript_truncated`/`transcript_full_length` from the full value (L336-337) |
| C4: rowcount==0 is a distinct `no_row` outcome, never success | ✅ | Both `_write_title_provenance` (youtube_upload_dag.py:788-790) and `_write_shorts_title_provenance` (reap_shorts_uploader_dag.py, same shape) check `if not rows:` → `status="no_row"`, WARNING-logged, distinct from `status="written"` |

### Requirement 3 deep-dive (explicitly re-verified against actual SQL/call sites)

- `database.py:1367-1384` `record_title_generation_input_turn`: `UPDATE ... SET title_generation_input = %s::jsonb WHERE output_path = %s` — **no `IS DISTINCT FROM` predicate**, confirmed by direct read and by `test_database.py::test_update_statement_targets_speaker_turn_videos_no_content_guard` asserting `"IS DISTINCT FROM" not in sql`.
- `record_title_generation_input_short` (database.py:1386+): same unguarded shape, keyed by `short_id`.
- Both call sites (`youtube_upload_dag.py:783-790`, `reap_shorts_uploader_dag.py:376-379`) surface `rowcount == 0` as `status="no_row"`, logged at WARNING, and this is asserted never to collapse into `"written"` by `test_zero_rows_is_no_row_not_success` in both `test_youtube_upload_dag.py` (L2960) and `test_reap_uploader_dag.py` (L1964).
- Re-run overwrite: `test_database.py::test_rerun_same_output_path_overwrites_without_raising` (L921) calls the method twice with the same key and asserts `rowcount >= 1` both times, no exception.

### Turn write key (point 6)

Confirmed the turn write is keyed by `thumbnail_config["output_path"]`
(`youtube_upload_dag.py:876`, `_write_title_provenance(result.get(...), thumbnail_config.get("output_path"), db=db)`), never `result["output_path"]` (the child DAG's reconciled `thumbnail.png`). Dedicated test:
`test_youtube_upload_dag.py::TestTriggerThumbnailGenerationTitleProvenance::test_write_key_is_thumbnail_config_output_path_not_child_result_output_path` (L2942) explicitly asserts the captured key equals `/data/oradores/42/video.mp4` and differs from the mocked child result's `/data/oradores/42/thumbnail.png`.

### Payload key optional in strict validation (point 7)

`youtube_upload_dag.py:856-864` (the strict result-validation conjunction gating `success`, `chapter_id`, `output_path`, `title`) does not reference `title_generation_input` in any branch. `result.get("title_generation_input")` is read separately at line 875, after the conjunction has already decided the result is valid. Dedicated test: `test_missing_payload_records_skipped_never_fails_strict_validation` (L2996) confirms a result missing that key still reaches `thumbnail_result["success"] is True` and records `title_provenance == {"status": "skipped", ...}`.

### Credential exclusion (point 8)

Both builders construct their return dict from explicit literal keys — no `{**source}` or `dict(source)` spread anywhere in either function:
- `build_turn_title_payload` (`thumbnail_generation.py:851-861`): literal dict with `best` reduced to `{"label", "style", "prompt"}` via three explicit `.get()` calls, `key_speakers` reduced to a name-only list comprehension.
- `build_shorts_title_payload` (`reap_shorts_uploader_dag.py:332-345`): literal dict, `transcript`/`scoring_reasoning` sliced inline, no spread of the chapter row or any other source dict.

### Untouched safeguards (point 9)

`git diff f6ff2e4..HEAD` inspected file-by-file:
- `congress_videos/modules/database.py`: 97 insertions, **0 deletions** — pure addition of the two new methods after `record_copy_verification_short`; that method and `record_copy_verification_turn` are byte-identical to `main`.
- `congress_videos/reap_shorts_uploader_dag.py`: 126 insertions, **0 deletions** — pure addition.
- `congress_videos/youtube_upload_dag.py`: 64 insertions / 2 deletions — the 2 deletions are the old one-line docstring of `trigger_thumbnail_generation` and one blank-line context shift, both directly adjacent to the new function; `_prepare_upload_config`'s empty-title guard (unchanged, still raises per L3411 test), `_verify_final_copy`'s `record_copy_verification_turn` call (unchanged, still asserted per L898 test), speaker-attribution helpers (`resolved_participant_slug` reads at L323/L348/L411/L645, all outside the diff), and `video_thumbnails` back-fill logic (L886/L1348/L1571, all outside the diff) are confirmed unmodified by direct grep + diff inspection.
- `congress_videos/generic_thumbnail_generator_dag.py`: 21 insertions, 0 deletions — the sidecar `video_thumbnails` persistence function (`_task_persist_results`, referenced in the module docstring at L5) is outside the diff; `_task_generate_title`'s `str` return type is explicitly pinned unchanged by `TestTaskGenerateTitleReturnTypeUnchanged` (L634).
- `congress_videos/modules/thumbnail_generation.py`: 70 insertions, 0 deletions — pure addition after `generate_title`; `generate_title` itself is untouched.
- No file outside the 13 expected production+test files (plus `openspec/**` planning artifacts) appears in the diff.

### Coherence (Design)

| Decision | Followed? | Notes |
|----------|-----------|-------|
| D3: unguarded UPDATE, no content guard | ✅ Yes | Confirmed in SQL text and test |
| C1: schema drift test explicitly extended, not assumed covered | ✅ Yes | Both `TABLE_COLUMNS` and `VIDEO_SHORTS_COLUMNS` updated |
| C2: upload_marking.py-style try/except at both new call sites | ✅ Yes | Confirmed in both files |
| C3: full transcript passed to shorts builder | ✅ Yes | Confirmed at call site and in builder |
| C4: rowcount==0 is a loud, distinct `no_row` outcome | ✅ Yes | Confirmed at both call sites with dedicated tests |
| `_write_title_provenance` extracted as a standalone helper (deviation, slice 2) | ✅ Documented | To satisfy ruff C901 complexity gate; behaviorally equivalent to the design's inline pseudocode, confirmed by reading the extracted function |
| `scoring_reasoning[:500]` sliced inside the builder, not by the caller (deviation, slice 3) | ✅ Documented | Reasonable interpretation of an underspecified point in the design's payload schema table; does not contradict any stated decision |
| `title_provenance` write gated on `if ai_title:`, not just `if transcript:` (deviation, slice 3) | ✅ Documented | Matches design D5's stated "AND returned a non-empty ai_title" condition; covered by `test_llm_returns_no_title_skips_write` |

### Issues Found

**CRITICAL**: None.

**WARNING**: None.

**SUGGESTION**:
1. Run `bash scripts/test-airflow-e2e.sh` manually against a real Docker host before merge — it reported `unavailable` in this sandbox (Docker daemon unreachable), which is a documented non-failure per `CLAUDE.md` but still an unexecuted check for the three modified DAG files' import-error surface.
2. Slices 2 and 3 both exceeded the 400-line review-workload budget (541/2 and 515/0 changed lines respectively) and are self-flagged as `size:exception` in `apply-progress.md`, with the stated reasoning (failure-isolation and zero-row test matrices) being legitimate rather than scope creep. This is a delivery/review-strategy decision for the orchestrator, not a code-correctness defect, but is worth surfacing again here since verify is the last gate before archive.
3. The spec.md scenario-heading count is 11, not the "12 scenarios" referenced in the launch prompt; this is already self-documented in `tasks.md`'s traceability table (Scenario 3.2 splits into two behaviorally distinct blocks under one heading) and does not affect coverage completeness — every heading and every named sub-behavior has a passing covering test — but future SDD artifacts for this change should cite "11 scenario headings / 12 behavioral GIVEN-WHEN-THEN blocks" consistently to avoid the same ambiguity resurfacing.

### Verdict

**PASS**

All 33 tasks complete; all 7 requirements and all 11 spec.md scenario headings (12 underlying behavioral blocks) have passing, behavior-asserting covering tests; the four binding post-validation design corrections (C1-C4) are verified in the actual SQL and call-site code, not merely assumed; the full 3-slice diff against `origin/main` (f6ff2e4) is additive-only outside two trivial adjacent-line docstring/blank-line shifts, with zero unintended edits to existing safeguards; `uv run pytest` is fully green (5147 passed, 0 failed, 34 pre-existing environment-conditioned skips, exit 0); `ruff check` and `ruff format --check` are both clean (exit 0).
