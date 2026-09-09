# Tasks: Verify Final Copy Before Publication

Issue #512. Stacked on #511 `canonical-politician-display-names` — #511 MUST land
first; `congress_videos/modules/politician_display_names.py` does not exist in
this worktree yet (verified: no match for `canonical_display_name` outside
`design.md`). Slices 3/5's evidence-assembly tasks are blocked until it does.

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | Slice 1 ~140, Slice 2 ~380 (2a/2b fallback if breached), Slice 3 ~350, Slice 4 ~260 (~1130 total) |
| 400-line budget risk | Medium (each slice designed ≤400; Slice 2 is the tight one, contingency named) |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 → PR 2 (→ 2a/2b if needed) → PR 3 → PR 4 |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: Medium

### Suggested Work Units

| Unit | Goal | PR | Focused test command | Runtime harness | Rollback boundary |
|---|---|---|---|---|---|
| 1 | Migration 050 + schema snapshot + drift test | PR1 | `uv run pytest tests/congress_videos/sql/test_migration_050.py tests/congress_videos/sql/test_production_schema.py -o addopts=` | N/A — schema-only, no DAG import surface change | Revert PR; all columns nullable/additive, no consumer yet |
| 2 | `final_copy_verification.py` + prompts + verdict parsing + bounded correction + containment | PR2 | `uv run pytest tests/congress_videos/modules/test_final_copy_verification.py -o addopts=` | N/A — pure module, not imported by any DAG yet | Revert PR; module inert until Slice 3/4 wire it |
| 3 | Long-form seam wiring (`t6`→`t6b`→`t7`) + audit persistence + accumulator | PR3 | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -o addopts=` | `bash scripts/test-airflow-e2e.sh` (auto-runs: touches `congress_videos/**`) | Revert PR; removing `t6b` restores exact today's behavior at this seam |
| 4 | Shorts seam wiring (`t2`→`t2b`→`t3`) + audit persistence | PR4 | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -o addopts=` | `bash scripts/test-airflow-e2e.sh` | Revert PR; removing `t2b` restores exact today's behavior at this seam |

PR1 base `feat/512-verify-final-copy`; PR2 bases PR1; PR3 bases PR2; PR4 bases PR3.
Slice 1 and Slice 2 have no code dependency on each other and could parallelize,
but the chosen chain strategy (stacked-to-main) keeps them sequential for a clean
diff per PR. Slices 3 and 4 each depend on Slice 1 (columns) and Slice 2 (module)
plus #511's `canonical_display_name`.

## Phase 1: Migration + Schema (PR1, `feat/512-a-migration-050`)

- [x] 1.1 RED: `test_migration_050.py` — mirror `tests/congress_videos/sql/test_migration_043.py` shape: both `ALTER TABLE` blocks present, every column `ADD COLUMN IF NOT EXISTS`, `speaker_turn_videos.copy_verified_at` is `TIMESTAMPTZ`, `video_shorts.copy_verified_at` is `TIMESTAMP`, `copy_thumbnail_text` only on `speaker_turn_videos`, and the entire `DOWN` block is commented (regex assertion)
- [x] 1.2 RED: extend `tests/congress_videos/sql/test_production_schema.py` drift test with the 9 new `speaker_turn_videos` columns and 8 new `video_shorts` columns
- [x] 1.3 GREEN: create `congress_videos/sql/migrations/050_final_copy_verification_audit.sql` per design D4 (both `ALTER TABLE` blocks, commented `DOWN`)
- [x] 1.4 GREEN: update `congress_videos/sql/production_schema.sql` in lockstep
- [x] 1.5 REFACTOR: `uv run pytest tests/congress_videos/sql/`; `uv run ruff check .`; `uv run ruff format --check .`
- [x] 1.6 Commit: `feat(db): add final-copy-verification audit columns (migration 050)`

## Phase 2: Pure verifier module + prompts (PR2, `feat/512-b-verifier-module`, base PR1)

**Slice 2a landed** (`feat/512-b1-verifier-core`, base PR1): the module's core
— verdict schema, defensive parsing, evidence containment, the bounded-correction
contract, content versioning — shipped as `congress_videos/modules/final_copy_verification.py`
with `run_correction_round()` as the tested seam (its `call_round(title, description)`
callback stands in for "render the prompt and call completion_fn once", so this
sub-slice needed no prompt content to be fully exercised). Actual diff: 743 lines
(module 335 + tests 408) against the 400-line hard budget — reported honestly as a
`size:exception` candidate rather than thinned to fit; see apply-progress for the
full rationale.

**Slice 2b landed**: added the two `FINAL_COPY_VERIFICATION_*` prompt constants
(design D6, exact text) to `ai_prompts.py` and the public `verify_final_copy()`
wrapper composing them with `run_correction_round()` (already implemented in 2a).
Added integration tests through the `verify_final_copy` public seam per design's
"never module internals" testing rule (pass, correctable-corrected, inconclusive,
verifier-failure, default-`completion_fn` resolution, plus pinned-prompt-content
guards for the no-invention and party-variant rules) — 2a's tests targeting
`run_correction_round` internals remain a deliberate, budget-driven deviation for
the bounded-correction branch coverage, not an oversight. Diff: 291 insertions/17
deletions (308 changed lines), within the 400-line budget for this sub-slice.

- [x] 2.1 RED: `tests/congress_videos/modules/test_final_copy_verification.py` — consistent copy ⇒ `pass`; malformed/`None`/`error` response, unknown `verdict` token, non-list `findings`, `corrected` with a stray key ⇒ `ok=False` (targets `run_correction_round`, slice 2a's seam)
- [x] 2.2 RED: party true-positive (`VOX` vs evidence `PSOE`) IS a finding; party variant (`PSE-EE (PSOE)` vs `PSOE`) is NOT; spelling/grammar/language findings
- [x] 2.3 RED: thumbnail text flagged and never present in `corrected`
- [x] 2.4 RED: bounded correction — evidence-backed name correction applied + rechecked; unsupported-claim correction discarded, original published; `call_round` (completion_fn stand-in) call count `<=2` on every branch (table-driven)
- [x] 2.5 RED: `content_version` stable across identical inputs, different across changed evidence
- [x] 2.6 GREEN: added `FINAL_COPY_VERIFICATION_SYSTEM_PROMPT` + `FINAL_COPY_VERIFICATION_USER_TEMPLATE` to `congress_videos/config/ai_prompts.py` (design D6, exact text)
- [x] 2.7 GREEN: added `verify_final_copy()` to `congress_videos/modules/final_copy_verification.py` — thin wrapper composing the real prompts + `utils.llm_cache.cached_json_completion` default (`model=LLM_DEFAULT`, no `temperature`/`max_tokens`/`max_completion_tokens`) with `run_correction_round()` (already implemented in 2a); `CopyFinding`/`CopyVerdict` already exist
- [x] 2.8 GREEN: implement evidence-containment check (`is_contained`, token-based per design D2) + length bounds (title ≤100, description ≤5000) and `sha256` content-version helper (`compute_content_version`)
- [x] 2.9 REFACTOR: `uv run pytest tests/congress_videos/modules/test_final_copy_verification.py tests/utils/test_no_removed_llm_kwargs.py`; ruff check/format — all green for the 2a scope
- [x] 2.10 Commit: `feat(congress-videos): add final-copy verification prompts and public entry point`
- [x] 2.11 CONTINGENCY — triggered: full slice 2 (module+prompts+tests) measured 1098 changed lines, ~3x the ~380 estimate. Applied a wider split than the contingency's literal wording (containment-helper-only): 2a = verdict schema + defensive parsing + evidence containment + bounded-correction contract + content versioning (matching the orchestrator's budget-guard boundary for this change); 2b = prompt constants + the public `verify_final_copy` wrapper. Even 2a alone is 743 lines, still over budget — reported as `size:exception`, not further fragmented (fragmenting a tightly-coupled parse→containment→correction flow across 3+ PRs would cost more reviewer clarity than it saves).

## Phase 3: Long-form seam wiring (PR3, `feat/512-c-longform-seam`, base PR2)

**Slice 3 landed.** Deviations from the task list's literal wording, both
narrower/clarifying than the design's prose rather than contradicting it (see
apply-progress for full rationale):

- Task 3.1's evidence-write test lives in `tests/congress_videos/modules/test_database_turns.py`
  (a new `TestRecordCopyVerificationTurn` class, right next to
  `TestMarkTurnsUploadedByOutputPath`) using that file's own `_make_conn` +
  `patch("congress_videos.modules.database.PostgresConnection", ...)` pattern —
  not the `mock_psycopg2_connection` fixture (that fixture mocks raw
  `psycopg2.connect(...)` calls, the shape used by `thumbnail_generation.persist_results`,
  not by any `CongressionalVideoDB` method; `record_copy_verification_turn` is a
  `CongressionalVideoDB` method exactly like `mark_turns_uploaded_by_output_path`,
  so it needed that file's own established mocking shape instead).
- Design.md's Interfaces/Contracts section states "Persist only when `ok` is
  true" — followed literally: an `ok=True` "correctable" verdict whose
  correction was discarded (unsupported) still persists an audit row, it is
  not treated as a non-write case. `_copy_verification_problems` reports it
  separately as a "discarded unsupported correction" finding.
- `_copy_verification_problems` reports exactly the four conditions design.md
  D7 names by their literal text — reject (non-title), a discarded unsupported
  correction, a persistence/stale-copy-guard skip, and a missing XCom — plus
  `inconclusive` per task 3.4's explicit wording ("surfaces via accumulator").
  A standalone `thumbnail_text` finding under an otherwise-clean verdict is
  not separately escalated beyond these four; it remains visible in the
  `copy_verification` XCom's `findings` list either way.
- The evidence bundle's tri-valued `mentioned_participant_slugs` field is
  keyed literally as `"mencionados"` (Spanish), per design.md D5's literal
  `"mencionados": "no analizado"` / `"mencionados": []` JSON-key text — every
  other evidence key stays English, matching the D5 table.
- Updated `docs/DAGS.md`'s task-graph diagram and task count (14→15) and the
  one other pre-existing test that hardcoded the 14-task count
  (`TestDualQueueWiredIntoDag::test_dag_task_count_updated_for_wired_dual_queue`),
  plus four pre-existing "should not raise" `_check_upload_failures` fixtures
  that needed a clean `copy_verification` XCom added — the same kind of
  backward-compatibility update issue #332 required when `_turn_marking_problems`
  was introduced.

- [x] 3.1 RED: `db.record_copy_verification_turn(output_path, ...)` — guarded `UPDATE ... WHERE output_path = %s AND copy_content_version IS DISTINCT FROM %s`, second call with same `content_version` returns `rowcount == 0`, grouped `output_path` updates all sibling rows (mirror `mark_turns_uploaded_by_output_path`, `database.py:1127-1171`)
- [x] 3.2 RED: `tests/congress_videos/test_youtube_upload_dag.py` — title `reject` (no correction) raises a `ValueError`, matching the fail-loud convention already established at this seam (issue #245), without touching the existing raise
- [x] 3.3 RED: description `reject` persists the audit row, publishes original, appended as a `_copy_verification_problems` finding in `_check_upload_failures` — never raises
- [x] 3.4 RED: inconclusive verdict (verifier failure/timeout/malformed) publishes `upload_config` unchanged, no DB write, surfaces via accumulator
- [x] 3.5 RED: correctable+contained correction patches `upload_config["videos"][0]["title"/"description"]` AND rewrites sidecars via `_write_orador_sidecars` (`youtube_upload.py:16`)
- [x] 3.6 RED: stale-copy guard — recomputed `content_version` mismatch immediately before write skips the write, emits accumulator finding
- [x] 3.7 GREEN: add `record_copy_verification_turn` to `congress_videos/modules/database.py`
- [x] 3.8 GREEN: add new task `t6b` (`verify_final_copy`) between `t6` (`prepare_upload_config`) and `t7` (`trigger_youtube_upload`) in `congress_videos/youtube_upload_dag.py`; assemble evidence from `db.get_chapter_metadata(chapter_id)` (`:1866`), `db.get_turn_speaker_slug(turn_id)` (`:1893`), `lookup_participant_by_slug(slug)` (`participants_db.py:174`) for raw `display_name`, and `politician_display_names.canonical_display_name(slug)` (#511) for `short_name`; thumbnail text from `db.get_chosen_thumbnail(chapter_id)` (`:2385`) — omit the field entirely when `art_direction_brief` is `NULL`/legacy string
- [x] 3.9 GREEN: verify against `upload_config["videos"][0]["title"/"description"]` (post-sidecar-roundtrip values), never the `thumbnail_result`/`_extract_metadata_description` XComs
- [x] 3.10 GREEN: implement `_copy_verification_problems(payload)` (shaped like `_turn_marking_problems`, `youtube_upload_dag.py:574`) and append it inside `_check_upload_failures` (`:1141-1189`)
- [x] 3.11 GREEN: push XCom `copy_verification` `{verdict, findings, corrected_applied, persisted, content_version}`; missing XCom is itself an accumulator finding, not a short-circuit raise
- [x] 3.12 REFACTOR: `uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/modules/`; ruff check/format; `bash scripts/test-airflow-e2e.sh` (Docker unavailable in this environment — substituted a local `DagBag(safe_mode=True)` import-error check, clean; run the real e2e script before merge)
- [x] 3.13 Commit: `feat(youtube-upload): verify turn copy before publication`

## Phase 4: Shorts seam wiring (PR4, `feat/512-d-shorts-seam`, base PR3)

**Slice 4 landed.** Diff: 511 changed lines (302 production + 209 tests across
`congress_videos/modules/database.py`, `congress_videos/reap_shorts_uploader_dag.py`,
`tests/congress_videos/modules/test_reap_db_methods.py`,
`tests/congress_videos/test_reap_uploader_dag.py`) against the 400-line hard
budget — reported honestly as a `size:exception` candidate, matching slices 2a
and 3's precedent, rather than thinned to fit. Deviation from the task list's
literal wording, narrower/clarifying rather than contradicting design.md:

- Task 4.5 said "reuse the `ch` row and `turn_speaker_slug` already read in
  `_generate_metadata`" — implemented literally by stashing the full `ch`
  chapter dict and the full `turn_speaker_row` dict (not just the slug string,
  to preserve `speaker_resolution_confidence`/`speaker_resolution_method` for
  the evidence bundle) onto each `shorts_metadata` entry in `_generate_metadata`,
  so `verify_final_copy` (t2b) needs zero additional `db.get_chapter_metadata`/
  `db.get_turn_speaker_slug` calls — only the roster lookup (`lookup_participant_by_slug`
  + `canonical_display_name`), exactly as the task specifies.
- A new `_copy_verification_evidence(chapter, turn_speaker_row)` helper in
  `reap_shorts_uploader_dag.py` mirrors `_copy_verification_evidence` in
  `youtube_upload_dag.py` byte-for-byte in its returned shape, minus the two DB
  reads it performs internally there (this DAG passes pre-fetched data
  instead) and minus `thumbnail_text` (no thumbnail step exists on this path).
  Deliberately NOT imported cross-DAG-file (no precedent for that in this
  codebase — DAG modules stay independent of each other) and NOT hoisted into
  `final_copy_verification.py` (design.md scopes that module to
  prompt/verdict/correction logic only, evidence assembly is DAG-layer per D5)
  — the small, self-contained duplication was judged cheaper in both risk and
  changed-line cost than either alternative (a cross-DAG import breaks module
  independence; moving the function costs a delete+add on already-committed
  slice-3 code, which is MORE changed lines for the same reuse, not fewer).

- [x] 4.1 RED: `db.record_copy_verification_short(short_id, ...)` — guarded `UPDATE video_shorts ... WHERE id = %s AND copy_content_version IS DISTINCT FROM %s`, idempotent re-run affects 0 rows
- [x] 4.2 RED: `tests/congress_videos/test_reap_uploader_dag.py` — accepted correction rewrites the `shorts_metadata` entry in place; description `reject` persists + still publishes the existing fallback description (no fail-loud path, per locked asymmetry); also covers a title `reject` never raising, since the shorts path has no fail-loud path at all (stronger than the letter of 4.2, which only names description)
- [x] 4.3 RED: inconclusive verdict publishes `shorts_metadata` unchanged, no DB write
- [x] 4.4 GREEN: add `record_copy_verification_short` to `congress_videos/modules/database.py`
- [x] 4.5 GREEN: add new task `t2b` (`verify_final_copy`) between `t2` (`generate_metadata`) and `t3` (`trigger_youtube_upload`) in `congress_videos/reap_shorts_uploader_dag.py`, positioned AFTER the `_format_own_channel_footer`/`_format_session_line` appends (`:376-377`) so it verifies the truly final description; reuse the `ch` row and `turn_speaker_slug` already read in `_generate_metadata` (`:255, 276`) plus the roster lookup, no `thumbnail_text` key
- [x] 4.6 GREEN: push XCom `shorts_copy_verification`; task log only — no accumulator on this DAG (locked decision, unchanged)
- [x] 4.7 REFACTOR: `uv run pytest tests/congress_videos/test_reap_uploader_dag.py tests/congress_videos/modules/`; ruff check/format; `bash scripts/test-airflow-e2e.sh` (Docker unavailable in this environment — substituted a local `DagBag(safe_mode=True)` import-error check: no import error attributed to `reap_shorts_uploader_dag.py`, task graph verified 6 tasks in the right order; run the real e2e script before merge)
- [x] 4.8 Commit: `feat(reap-uploader): verify shorts copy before publication`

## Phase 5: Deployment verification (post-merge, not sdd-apply)

- [ ] 5.1 Full `uv run pytest` green against the parent-branch baseline (4843 passed, 34 skipped) plus this change's new tests
- [ ] 5.2 `bash scripts/test-airflow-e2e.sh` — `airflow dags list-import-errors` empty (auto-runs: change touches `congress_videos/**`)
- [ ] 5.3 Apply migration `050` on NAS `development` schema via `migrations_dag`; do NOT infer success from DAG-run "success" alone — query both tables for the new columns to confirm. This repo has a prior incident (issue #467/migration 047): the migration failed with permission denied in BOTH schemas after the infra-security cutover because the owner role lost `REFERENCES`; the fix was `GRANT REFERENCES ON ALL TABLES` to the owner
- [ ] 5.4 Apply migration `050` on NAS `production` schema via `migrations_dag`; verify identically (query both tables' columns, not just DAG status)
- [ ] 5.5 Confirm migration success in BOTH schemas BEFORE the wiring PRs (3 and 4) reach production, per design.md "Migration / Rollout" — Slice 2's module is inert without the columns, but Slices 3/4's writes will fail loudly if the columns are missing
- [ ] 5.6 `git_sync` both stacks; confirm `airflow dags list-import-errors` empty on both after each slice deploys
