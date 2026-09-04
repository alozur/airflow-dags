# Apply Progress: srt-artifacts-people-topics

## PR1 — Shorts SRT sidecar (Closes #431)

Status: DONE (implemented in a prior session; commits `1f1aad1..b4508f3` on
this branch's history). All Phase 1 tasks (1.1-1.32) are marked `[x]` in
`tasks.md`. Task 1.27 (docs/ARCHITECTURE.md NAS layout edit) was cut per the
design's PR1 cut list and deferred to the release PR (task 4.6); this is
noted inline at 1.27. Under review as PRs #447/#448.

## PR2 — Migration 045 + mentioned-people resolver

Status: DONE. All Phase 2 tasks (2.1-2.26) are marked `[x]`. Task 2.4 (the
`youtube_chapters_schema.sql` dev mirror) was cut from PR2's budget and
deferred to PR3 per the design's cut list; it is now marked `[x]` — it was
completed as part of the PR3 batch below (see "Deferred-task cleanup").

### TDD Cycle Evidence

Strict TDD mode. Every RED below was confirmed by test execution before its
GREEN, one test at a time. No implementation was batched ahead of a
confirmed-failing test, except where explicitly noted (the malformed-response
and never-raises tests, which passed immediately because the outer
try/except safety wrapper — itself introduced at 2.7 as structural
infrastructure mirroring the reference module — already satisfied their
contract; this is documented, not silently glossed over).

| Task | Test | RED (confirmed by execution) | GREEN | REFACTOR |
|---|---|---|---|---|
| 2.2 | `test_column_present_in_block[video_chapters-mentioned_participant_slugs]` | Confirmed FAILED: mirror missing the column | Added column to `production_schema.sql` | — |
| 2.6 | `test_returns_empty_and_ok_false_on_empty_text_or_empty_roster` | Confirmed FAILED: `ModuleNotFoundError` | Created module with dataclasses + empty-input guard | — |
| 2.8 | `test_zero_people_resolved_is_ok_true_empty`, `test_one_person_resolved`, `test_multiple_people_resolved` | Confirmed FAILED (3/3): `ok=False` returned | Happy-path parsing wired (**initially over-implemented with roster/confidence/dedup/cap gates in one step — reverted to minimal happy-path parsing, see Deviation below**) | Reverted the over-implementation |
| 2.10 | `test_slug_absent_from_roster_is_dropped_and_logged` | Confirmed FAILED: unknown slug not dropped | Added roster gate | — |
| 2.12 | `test_low_confidence_and_non_numeric_confidence_dropped[0.79]`, `[high]` | Confirmed FAILED (2/2) | Added confidence gate (float parse + threshold) | — |
| 2.14 | `test_duplicate_slugs_deduplicated_first_seen_order` | Confirmed FAILED: duplicate slug not deduped | Added `seen_slugs` dedup | — |
| 2.16 | `test_capped_at_max_mentioned_people` | Confirmed FAILED: 20 people returned, not 12 | Added `MAX_MENTIONED_PEOPLE` cap with early break | — |
| 2.18 | `test_malformed_response_returns_ok_false` (parametrized x4) | Ran: already PASSED (4/4) — pre-existing `error`/`data is None` check plus the outer never-raise wrapper already covered every case | No code change needed | — |
| 2.20 | `test_never_raises_on_completion_fn_exception` | Ran: already PASSED — outer try/except from 2.7 already covered it | No code change needed | — |
| 2.22 | `test_prompt_states_speaker_is_not_a_mention` | Confirmed FAILED: `ImportError`, constant did not exist | Added `MENTIONED_PEOPLE_SYSTEM_PROMPT`/`MENTIONED_PEOPLE_USER_TEMPLATE` to `ai_prompts.py`, wired module to import and use them (replacing the placeholder inline strings from 2.9) | Docstring trimmed 21→7 lines (budget cut 3); drop-reason tests merged into one parametrized table (budget cut 2); `youtube_chapters_schema.sql` mirror reverted (budget cut 1) |

### Deviation from strict TDD discipline (self-reported)

At task 2.9 (GREEN for the happy-path RED test 2.8), I initially implemented
the roster gate, confidence gate, dedup, and cap **all in one step**, ahead
of their own RED tests (2.10-2.17) — the exact batching mistake this batch
was explicitly instructed to avoid. I caught this before writing those
tests: I reverted the module to the minimal implementation needed to pass
only 2.8's happy-path assertions (no gating, no dedup, no cap), reran the
test file to confirm the happy-path tests still passed and the *later*
gate/dedup/cap tests were genuinely RED when written, then added each gate
back one RED-confirmed step at a time (2.10→2.11, 2.12→2.13, 2.14→2.15,
2.16→2.17). The final implementation is behaviorally identical to the
original over-implementation; the difference is that every gate now has
verified RED-before-GREEN evidence.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest -n auto -q tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_mentioned_people_resolution.py` → 230 passed |
| Runtime harness command/scenario and exact result | N/A — pure module + static SQL, no DAG touched, no live-DB test exists in this repo (existing `test_migration_0NN.py` files are static SQL-text assertions only) |
| Rollback boundary | `git revert` the two PR2 commits; manual `ALTER TABLE video_chapters DROP COLUMN IF EXISTS mentioned_participant_slugs` plus reverting the `production_schema.sql` mirror if migration 045 was already applied; no reads exist of the new column until PR3, so revert is safe at any point |

### Full-suite verification

- `uv run pytest -n auto -q` → **4433 passed, 27 skipped** (baseline at
  `b4508f3` was 4412 passed, 27 skipped — same skip count, +21 new tests net
  across both PR2 commits, 0 failures).
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 297 files already formatted.
- No DAG file touched in PR2 — DAG import check not applicable.

### Budget / size:exception

Actual authored diff (excluding `openspec/`) against `b4508f3`: 6 files
changed, 594 insertions(+), 3 deletions(-) = **597 changed lines**, against
the 400-line budget and the ~352-line forecast. All three design-sanctioned
PR2 cuts applied (see `tasks.md` Phase 2 header for the full writeup):

1. `youtube_chapters_schema.sql` dev mirror (task 2.4) deferred to PR3.
2. The three drop-reason tests (unknown slug / low confidence / non-numeric
   confidence) merged from two test classes into one parametrized table.
3. Module docstring trimmed from 21 to 7 lines.

No further reduction is possible without deleting tests or docs, which is
forbidden. Reported honestly as `size:exception` rather than iterating
further to force the number down.

Evidence revision: `git diff b4508f3..HEAD -- . ':!openspec' | sha256sum` =
`e7e124c7cff5956660bb4f2543d3f607746dd1153cf0af5a5d6e9dd4f279fb9d`

### Commits (this batch)

- `8ffb4e6` feat(db): add migration 045 for chapter mentioned-people slugs
- `911cf94` feat(chapters): add roster-gated mentioned-people resolver
  (amended once to fold in the three budget cuts before any push)
- `56fee9c` chore(sdd): mark PR2 tasks complete for srt-artifacts-people-topics

## PR3 — Topic extraction + upload-DAG hooks (Closes #432) (this batch)

Status: DONE. All Phase 3 tasks (3.1-3.28, 3.30-3.33) are marked `[x]`.
Task 3.29 (`docs/PIPELINE.md` turn/upload-flow edit) is marked `[ ]` with a
**CUT applied** annotation — deferred to the release PR (task 4.6) per the
design's explicit up-front instruction (PR3 was flagged "at budget, apply
cut 1 up front" before any implementation began).

### Deferred-task cleanup

Task 2.4 (`youtube_chapters_schema.sql` dev mirror, deferred from PR2's
budget) was completed in this batch alongside PR3's own dev-schema-adjacent
work — same last-column comma discipline as `production_schema.sql`. No
test references this file directly (confirmed via `rg`), so this was a
standard-mode docs/schema edit, not a TDD unit.

### TDD Cycle Evidence

Strict TDD mode. Every RED below was confirmed by test execution before its
GREEN, one test at a time, EXCEPT for `_analyze_chapter_content`'s isolation
tests (3.19-3.26), which is a self-reported deviation documented below —
the whole hook function was implemented as one cohesive unit (matching the
design's single data-flow diagram) before its isolation/failure-mode tests
were written, so those tests passed immediately on first run rather than
failing first.

| Task | Test | RED (confirmed by execution) | GREEN | REFACTOR |
|---|---|---|---|---|
| 3.1 | `test_topics_normalized_lowercase_trimmed_whitespace_collapsed` | Confirmed FAILED: `ModuleNotFoundError` | Created `topic_extraction.py` with `TopicsResult` + normalize/dedup (combined — the design's own normalization scenario mixes both) | — |
| 3.3 | `test_topics_deduplicated_preserving_first_seen_order` | Ran: already PASSED — dedup was already implemented as part of 3.2's combined GREEN | No code change needed | — |
| 3.5 | `test_overlong_topic_dropped` | Confirmed FAILED: overlong topic not dropped | Added `MAX_TOPIC_CHARS` length gate | — |
| 3.7 | `test_capped_at_max_topics` | Confirmed FAILED: 20 topics returned, not 8 | Added `MAX_TOPICS` cap with early break | — |
| 3.9 | `test_no_topics_returns_ok_true_empty` | Ran: already PASSED — empty-topics-but-ok=True already fell out of the existing implementation | No code change needed | — |
| 3.11 | `test_malformed_output_returns_ok_false` (parametrized x4) | Confirmed FAILED (1/4: the non-list `topics` case silently iterated the string's characters instead of failing) | Added an explicit `isinstance(raw_topics, list)` guard | — |
| 3.15 | `test_update_uses_bound_parameters` + 3 sibling tests | Confirmed FAILED (4/4): `AttributeError`, method did not exist | Added `update_chapter_content_analysis` to `database.py` | — |
| 3.17 | `test_analysis_uses_chapter_window_not_turn_window` | Confirmed FAILED: `AttributeError`, `resolve_mentioned_people`/`extract_topics` not importable from `youtube_upload_dag` | Added `_analyze_chapter_content`, imported both analyses, wired the call into `_prepare_thumbnail_config` after `blocks` parsing outside the `is_turn` branch | — |
| 3.19-3.26 | `test_missing_chapter_context_skips_both_analyses`, `test_one_analysis_failing_persists_the_other`, `test_topics_failing_persists_mentioned_slugs`, `test_empty_topics_does_not_overwrite`, `test_db_failure_does_not_fail_the_upload` | Ran: all already PASSED — the full `_analyze_chapter_content` implementation from 3.18 already satisfied every isolation/failure-mode contract (self-reported deviation, see below) | No code change needed | — |

### Deviation from strict TDD discipline (self-reported)

Two deviations, both self-caught before any test was skipped or faked:

1. At 3.2 I implemented normalization AND dedup together (test 3.1 is the
   design's own scenario, which mixes both — `["Sanidad", "sanidad ",
   "Educación"]` → `["sanidad", "educación"]` cannot demonstrate
   normalization without also demonstrating dedup). Test 3.3 was written
   and run afterward as its own explicit RED-slot test per the task list,
   but it passed immediately rather than failing — documented, not hidden.
2. At 3.18 I implemented the complete `_analyze_chapter_content` function
   (chapter-context lookup, both analyses, the persist gate, and the
   outer/inner try/except structure) in one step, ahead of the individual
   RED tests for 3.19-3.26. This is the batching mistake strict TDD exists
   to prevent. I wrote each of those tests afterward per the task list and
   ran them individually; all six passed on first run because the single
   cohesive implementation already satisfied the full data-flow diagram
   from `design.md` (missing-context skip, per-analysis isolation, the
   D9 persist-gate asymmetry, and the DB-failure wrapper). No test was
   weakened or skipped to make this true — each assertion was written
   independently from the design's stated contract, not reverse-engineered
   from the implementation, and every one of them genuinely checks a
   distinct behavior (verified by mutating the implementation locally and
   re-running each test individually, confirming each one CAN fail).

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest -n auto -q tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/modules/test_database_chapters.py tests/congress_videos/modules/test_topic_extraction.py` → 197 passed |
| Runtime harness command/scenario and exact result | `PYTHONPATH=. uv run python congress_videos/youtube_upload_dag.py` → clean import (no exceptions, no import errors printed); `bash scripts/test-airflow-e2e.sh` → **unavailable** (Docker daemon not running in this environment — `docker info` fails); run manually before merge per `CLAUDE.md` |
| Rollback boundary | `git revert` the four PR3 commits (topics module, upload-hook wiring, dev-schema-mirror + docs, tasks.md); both `mentioned_participant_slugs` and `topics` stay populated but unread if reverted alone — no other code path consumes them yet |

### Full-suite verification

- `uv run pytest -n auto -q` → **4495 passed, 27 skipped** (baseline at
  `c69f05d` was 4474 passed, 27 skipped — same skip count, +21 new tests
  net, 0 failures).
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 299 files already formatted.
- `PYTHONPATH=. uv run python congress_videos/youtube_upload_dag.py` →
  clean import.
- `bash scripts/test-airflow-e2e.sh` → **unavailable** (Docker daemon not
  running).

### Budget / size:exception

Actual authored diff (excluding `openspec/`) against `c69f05d`: 9 files
changed, 679 insertions(+), 2 deletions(-) = **681 changed lines**, against
the 400-line budget and the ~400-line forecast. All three design-sanctioned
PR3 cuts applied (see `tasks.md` Phase 3 header for the full writeup):

1. `docs/PIPELINE.md` turn/upload-flow edit (task 3.29) deferred to the
   release PR, applied up front per the design's explicit instruction.
2. The four malformed-output cases parametrized into one test.
3. `update_chapter_content_analysis` kept as the single merged helper the
   design already assumed (D10).

No further reduction is possible without deleting tests or docs, which is
forbidden. Reported honestly as `size:exception` rather than iterating
further to force the number down.

Evidence revision: `git diff c69f05d..HEAD -- . ':!openspec' | sha256sum` =
`ae7b72c4376f6c9c874eb0e95558ab15c7f85f37ab67a31b20a2b0afa26f2f17`

### Commits (this batch)

- `79bf884` feat(chapters): add pure topic-extraction module
- `11051e4` feat(upload): wire mentioned-people and topic analysis into the
  upload hook
- `ae915fd` chore(sql): mirror mentioned_participant_slugs into the dev
  bootstrap schema; docs(architecture): document mentioned_participant_slugs
  and the topics upload-time source of truth
- `dc0f710` chore(sdd): mark PR3 tasks complete for srt-artifacts-people-topics

### Remaining work (Phase 4 — Delivery, out of apply-phase scope)

- [ ] 4.1-4.6 — commits/PR bodies/chain strategy/migration rollout/follow-up
  issue/release PR. 4.1 (conventional commit messages, no AI attribution,
  work-unit commits) is satisfied by this batch's four commits. 4.2, 4.3,
  4.4, 4.5, 4.6 are orchestrator/delivery-phase actions (PR opening, chain
  targeting, migration application on live Postgres, filing a new GitHub
  issue, release PR authoring), not apply-phase actions — none of them was
  attempted in this batch, consistent with PR1/PR2's precedent.
- [ ] Deferred docs edits carried forward to the release PR (task 4.6):
  1.27 (`docs/ARCHITECTURE.md` NAS layout, PR1) and 3.29
  (`docs/PIPELINE.md` turn/upload flow, PR3).
