# Apply Progress: srt-artifacts-people-topics

## PR1 — Shorts SRT sidecar (Closes #431)

Status: DONE (implemented in a prior session; commits `1f1aad1..b4508f3` on
this branch's history). All Phase 1 tasks (1.1-1.32) are marked `[x]` in
`tasks.md`. Task 1.27 (docs/ARCHITECTURE.md NAS layout edit) was cut per the
design's PR1 cut list and deferred to the release PR (task 4.6); this is
noted inline at 1.27. Under review as PRs #447/#448.

## PR2 — Migration 045 + mentioned-people resolver (this batch)

Status: DONE. All Phase 2 tasks (2.1-2.26) are marked `[x]` except 2.4,
which is marked `[ ]` with a **CUT applied** annotation (deferred to PR3
per the design's cut list — see `tasks.md` Phase 2 header for the full
size:exception writeup).

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

### Remaining work (Phase 3 — PR3, NOT started, out of this batch's scope)

- [ ] 3.1-3.33 — topic extraction module + upload-DAG hooks (issue #432,
  closes it). Explicitly out of scope per the orchestrator's instructions
  for this batch.
- [ ] Phase 4 delivery tasks 4.1-4.6 (commits/PR bodies/chain
  strategy/migration rollout/follow-up issue/release PR) — partially
  satisfied by this batch's commit messages (4.1) but PR opening (4.2/4.3)
  and migration rollout (4.4) are orchestrator/delivery-phase actions, not
  apply-phase actions.
