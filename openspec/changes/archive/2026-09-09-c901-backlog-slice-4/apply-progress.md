# Apply Progress: C901 Backlog Slice 4 (issue #272)

Worktree: `/home/alozur/src/github.com/alozur/airflow-dags-wt-272-s4`
Branch: `refactor/272-c901-slice-4-pr1` (stacks on `refactor/272-c901-slice-4-pr0`, targets `dev`)
Base: `413d2fe` (= `origin/dev` / `origin/main` `1aa5681` + the planning-docs commit; `dev == main` at start, no rebase needed)

## PR1 — free prune + `is_procedural_turn` + `extract_announcement` (`congress_videos/modules/speaker_turns.py`, `congress_videos/reap_clip_preparer_dag.py`)

**Status: COMPLETE — 11/11 PR1 tasks done (1.1-1.11).**

### What was lifted

| Outer function | Helper(s) extracted | Complexity before → after (measured) |
|---|---|---|
| `is_procedural_turn` | `_collect_pattern_spans(patterns, normalized, spans, matched_names) -> None` | 12 → **6** |
| `extract_announcement` | `_first_named_announcement(sorted_blocks)`, `_first_phrase_announcement(sorted_blocks)` | 12 → **4** |
| `reap_clip_preparer_dag.py` (whole file) | none — free prune, 0 violations | n/a |

Both `speaker_turns.py` outers matched the design's predicted complexities exactly (6 and 4).

Helpers were placed module-level, immediately above their outer function, each carrying a
`Lifted verbatim out of <outer> (issue #272)` docstring, per design's technical approach. No new
module, no signature change on any public name.

- `_collect_pattern_spans` replaces the two byte-identical `for name, pattern in <PATTERNS>:` loops
  in `is_procedural_turn` (base lines 337-341 and 347-351), applying normalization (d) — parameter-alias
  substitution of the helper's `patterns` param back to `PROCEDURAL_PATTERNS` / `PROCEDURAL_FILLER_PATTERNS`
  for the proof, checked against **both** base call sites.
- `_first_named_announcement` / `_first_phrase_announcement` replace `extract_announcement`'s two
  sequential search passes (base 418+421-429, 419+434-443), applying normalization (c) — an appended
  trailing `return` — and normalization (f) — `best_phrase`'s inert initializer relocates from its
  original position (ahead of the named-announcement loop) into the phrase helper, mechanically proven
  to cross zero reads/writes of its own name.

### AST-equality proof

Scratch-only `ast_check_s4.py` (session scratchpad, never versioned). Ran against the PR1 tip
(commit `e9defe6`); all 8 checks report `OK`, exit 0:

```
OK _collect_pattern_spans vs base site 1 (PROCEDURAL_PATTERNS) (normalized: patterns->PROCEDURAL_PATTERNS)
OK _collect_pattern_spans vs base site 2 (PROCEDURAL_FILLER_PATTERNS) (normalized: patterns->PROCEDURAL_FILLER_PATTERNS)
OK _first_named_announcement vs base (init + for-loop)
OK _first_named_announcement appended return (c)
OK _first_phrase_announcement vs base (init + for-loop, (f) relocated)
OK _first_phrase_announcement appended return (c)
OK (f) best_phrase crosses zero read/write of its own name
OK call-site tail matches expected replacement shape
```

### RED-first tests added (new test functions only, zero pre-existing assertions edited)

| Helper | Test file | Test count |
|---|---|---|
| `_collect_pattern_spans` | `tests/congress_videos/modules/test_speaker_turns_procedural.py::TestCollectPatternSpans` | 3 |
| `_first_named_announcement` | `tests/congress_videos/modules/test_speaker_turns.py::TestFirstNamedAnnouncement` | 3 |
| `_first_phrase_announcement` | `tests/congress_videos/modules/test_speaker_turns.py::TestFirstPhraseAnnouncement` | 3 |

All 9 confirmed RED (ImportError — helper did not exist yet) before the corresponding lift, then GREEN
after. `git diff` on both test files shows additions only.

### `pyproject.toml` / counter ladder

| Step | Change | Counter |
|---|---|---|
| commit 1 (`5a011af`) | delete `congress_videos/reap_clip_preparer_dag.py` entry (0 violations) | 13 → **12** |
| commit 3 (`e9defe6`) | `speaker_turns.py` entry `["C901","F841","SIM102","SIM108"]` → `["F841","SIM102","SIM108"]` | 12 → **11** |

`test_every_code_list_is_sorted_and_deduped` passes; `F841`/`SIM102`/`SIM108` kept untouched on the
multi-offender entry per the requirement.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and result | `uv run pytest -o addopts= tests/congress_videos/modules/test_speaker_turns_procedural.py tests/congress_videos/modules/test_speaker_turns.py` → **168 passed** |
| Full ruff check | `uvx ruff check .` → **All checks passed!** |
| Full ruff format check | `uvx ruff format --check .` → **305 files already formatted** |
| Ruff config test | `uv run pytest -o addopts= tests/test_ruff_config.py` → **14 passed** |
| Runtime harness | N/A — pure helpers (`is_procedural_turn`, `extract_announcement`), no DAG/runtime surface change; `reap_clip_preparer_dag.py` had zero source edits |
| Rollback boundary | `git revert` of PR1's three commits (or the eventual PR1 merge commit) returns `pyproject.toml`, `tests/test_ruff_config.py`, and `speaker_turns.py` to their pre-PR1 state together; independent of every other PR in this stack |

### Diffstat (PR1 tip vs. base)

```
git diff --shortstat 413d2fe..HEAD
5 files changed, 195 insertions(+), 37 deletions(-)
```

232 changed lines, well within the 400-line budget (design estimated ~220).

### Commits (3, on `refactor/272-c901-slice-4-pr1`)

| SHA | Message |
|---|---|
| `5a011af` | `refactor(ruff): prune the zero-violation reap_clip_preparer_dag.py C901 entry (#272)` |
| `5f1b5cc` | `refactor(speaker-turns): lift pattern-span collection out of is_procedural_turn (#272)` |
| `e9defe6` | `refactor(speaker-turns): lift announcement helpers and drop C901 token (#272)` |

PR1 tip: `e9defe6`.

### TDD Cycle Evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| `_collect_pattern_spans` | 3 new tests, confirmed ImportError | Lift landed, all 3 pass + full file suite (67) green | N/A — lift is the refactor itself, no further pass needed |
| `_first_named_announcement` | 3 new tests, confirmed ImportError | Lift landed, all 3 pass + full file suite (101) green | N/A |
| `_first_phrase_announcement` | 3 new tests, confirmed ImportError | Lift landed, all 3 pass + full file suite (101) green | N/A |
| Free prune (`reap_clip_preparer_dag.py`) | N/A — no source edit, 0 violations pre-existing | `uvx ruff check` on the file passes; `test_ruff_config.py` (14 tests) green | N/A |

### Deviations from design

None — implementation matches design.md exactly: helper names, signatures, lift line ranges,
normalization catalogue entries (c), (d), (f), and the call-site replacement shape all match the
per-lift contract table.

### Issues found

None.

### Remaining tasks (PR3-PR6, final tip)

- [x] PR2 — `normalize_chapter_speakers` (`speaker_normalization.py`)
- [ ] PR3 — `_resolve_speaker_inner` (`speaker_resolution.py`, budget-risk PR)
- [ ] PR4 — `derive_candidate_intervals` (`candidate_intervals.py`, characterization tests first)
- [ ] PR5 — `_prepare_turns_callable` part 1 (`speaker_turn_prepare_dag.py`)
- [ ] PR6 — `_prepare_turns_callable` part 2 + token drop
- [ ] Final tip — full `uv run pytest`, e2e, release PR, issue #272 slice-4 report comment

### Workload / PR Boundary (PR1)

- Mode: chained PR slice (`auto-chain` / `stacked-to-main`)
- Boundary: starts at `413d2fe` (worktree base), ends at `e9defe6` (PR1 tip) — free prune of
  `reap_clip_preparer_dag.py` + full lift of both `speaker_turns.py` C901 offenders + token drop
- Estimated review budget impact: 232 changed lines, well under the 400-line budget; PR1 is independent
  and reverts cleanly without touching PR2-PR6

### Status (PR1)

11/11 PR1 tasks complete.

## PR2 — `normalize_chapter_speakers` (`congress_videos/modules/speaker_normalization.py`)

**Status: COMPLETE — 8/8 PR2 tasks done (2.1-2.8).** Branch `refactor/272-c901-slice-4-pr2`, stacks on
PR1 tip `968015e`. Commit `74e7190`.

Lifted `_apply_institutional_role_corrections` (Step 0 for-loop, base 247-279) and
`_apply_roster_resolution_step` (Step 1 `if dirty_names:` body, base 287-339), no declared
normalization; `result`/`cursor` passed as the same live objects. Complexity measured:
`normalize_chapter_speakers` 14→**6**, both helpers→**5** (all match design predictions exactly).
AST-equality proof (`ast_check_s4_pr2.py`, scratch-only): 4/4 `OK`. 7 new RED-first quirk tests
(consolidated from an initial 12-test draft that exceeded the 400-line budget at 443 lines; the
consolidated 7 preserve every quirk assertion, none removed) — all confirmed RED (ImportError)
before the lift, GREEN after. `pyproject.toml` entry pruned same commit;
`EXPECTED_C901_FILE_COUNT` 11→10. `uvx ruff check .` clean, `uvx ruff format --check .` clean,
focused `uv run pytest -o addopts= tests/congress_videos/modules/test_speaker_normalization.py
tests/test_ruff_config.py` → 47 passed. `git diff 968015e..HEAD --shortstat`: 4 files, 302
insertions(+), 88 deletions(-) = **390 changed lines**, within the 400-line budget (no
`size:exception` needed). `git diff` on the touched test file shows additions only (the one
removed line is the `EXPECTED_C901_FILE_COUNT` constant, not a test assertion). Rollback:
`git revert` of commit `74e7190`; independent of PR1 and PR3-PR6.

### Status (PR2)

8/8 PR2 tasks complete. Ready for `sdd-verify` on PR2's scope, then `sdd-apply` again for PR3.

## PR3 — `_resolve_speaker_inner` (`congress_videos/modules/speaker_resolution.py`) — budget-risk PR

**Status: COMPLETE — 9/9 PR3 tasks done (3.1-3.9).** Branch `refactor/272-c901-slice-4-pr3`,
stacks on PR2 tip `9f8c23a`. Hidden-regression pre-check: `_resolve_speaker_inner` 14, nothing
else, zero drift vs `origin/dev 1aa5681`.

**Split triggered**: a single combined lift measured 504 changed lines (>400 budget). Applied
the design's pre-approved contingency — commit `f09becf` (PR3a): lifted
`_build_resolution_user_prompt` (base 385-428) only, normalizations (c)+(e); no pyproject edit;
`_resolve_speaker_inner` measured 14→**11** (still masked, matches design's stated PR3a state
exactly); 271 changed lines. Commit `d74a395` (PR3b): lifted `_validate_completion_response`
(base 442-502, tail lift, no normalization needed); `_resolve_speaker_inner` 14→**6** (matches
design prediction), `_build_resolution_user_prompt`→**5**, `_validate_completion_response`→**6**
(both match predictions); pruned the `speaker_resolution.py` pyproject entry (sole code
`"C901"`) and decremented `EXPECTED_C901_FILE_COUNT` 10→9; 232 changed lines. Combined total
392 insertions + 111 deletions vs PR2 tip.

AST-equality proofs (`ast_check_s4_pr3a.py` for PR3a scope, `ast_check_s4_pr3.py` for the full
PR3b state, both scratch-only): 4/4 and 5/5 `OK` respectively, including the (e) abort-sentinel
call-site check (`if user_prompt is None: return None`). 9 new RED-first quirk tests across two
classes (`TestBuildResolutionUserPrompt` ×4, `TestValidateCompletionResponse` ×5), all confirmed
RED (ImportError) before their respective lift, GREEN after. `uvx ruff check .` and
`uvx ruff format --check .` clean at both tips. Focused
`uv run pytest -o addopts= tests/congress_videos/modules/test_speaker_resolution.py
tests/test_ruff_config.py` → 94 passed at the PR3b tip. `git diff 9f8c23a..HEAD -- tests/`
shows additions only (the one `-` line is the `EXPECTED_C901_FILE_COUNT` constant, not a test
assertion — same shape as PR2). Rollback: `git revert` of `d74a395` then `f09becf` (or both
together); independent of PR1/PR2 and every other PR in the stack.

### Status (PR3)

9/9 PR3 tasks complete. Ready for `sdd-verify` on PR3's scope, then `sdd-apply` again for PR4.

## PR4 — `derive_candidate_intervals` (`benchmarks/pyannote_diarization/candidate_intervals.py`) — characterization tests first

**Status: COMPLETE — 9/9 PR4 tasks done (4.1-4.9).** Branch `refactor/272-c901-slice-4-pr4`,
stacks on PR3b tip `c3c5889`. Orchestrator directed no rebase this batch (commit-only worktree,
already at the PR3 tip); hidden-regression pre-check ran against the current worktree instead of
`origin/dev` and matched exactly: `derive_candidate_intervals` 12, nothing else.

Commit `f3225f9` (test-first, per PR4's characterization exception): 3 direct unit tests pinned
from the existing CLI-subprocess literals, green against untouched source. Commit `bb278a3`:
7 RED-first quirk tests for `_merge_active_intervals`/`_intervals_to_gaps` (confirmed ImportError),
then lifted both byte-for-byte (base 63-68, base 70-90 tail lift incl. `return gaps`) immediately
above `derive_candidate_intervals`; pruned the pyproject entry (sole code was `C901`) and
decremented `EXPECTED_C901_FILE_COUNT` 9→8 in the same commit. Complexity measured:
`derive_candidate_intervals` 12→**7**, `_merge_active_intervals`→**3**, `_intervals_to_gaps`→**4**
(all match design predictions exactly). AST-equality proof (`ast_check_s4_pr4.py`, scratch-only):
5/5 `OK`. `uvx ruff check .` clean, `uvx ruff format --check .` clean, focused
`uv run pytest -o addopts= tests/benchmarks/test_pyannote_diarization_candidate_intervals.py
tests/test_ruff_config.py` → 27 passed. `git diff c3c5889..HEAD --shortstat`: 4 files, 161
insertions(+), 30 deletions(-) = 191 changed lines (well under 400, no `size:exception` needed).
Diff on the touched test file shows additions only. Rollback: `git revert` of `bb278a3` then
`f3225f9`; independent of PR1-PR3 and PR5-PR6.

### Status (PR4)

9/9 PR4 tasks complete. Ready for `sdd-verify` on PR4's scope, then `sdd-apply` again for PR5.

## PR5 — `_prepare_turns_callable` AI-resolution helpers (`congress_videos/speaker_turn_prepare_dag.py`, part 1 of 2)

**Status: COMPLETE — 8/8 PR5 tasks done (5.1-5.8).** Branch `refactor/272-c901-slice-4-pr5`,
stacks on PR4 tip `1a9efbc` (no rebase this batch — orchestrator directed commit-only worktree;
hidden-regression pre-check ran against the current worktree and matched `origin/dev` (`ac2e49e`,
which had advanced past the session's stated `1aa5681` — zero drift on this file):
`_prepare_turns_callable` 19, nothing else.

Lifted `_resolve_qa_winner` (base 328-364) and `_persist_turn_resolution` (base 366-402), no
`pyproject.toml`/counter edit (function stays masked at 11 per design). Complexity measured:
`_resolve_qa_winner`→**6**, `_persist_turn_resolution`→**4**, `_prepare_turns_callable` 19→**11**
(all match design predictions exactly). **Deviation from design**: `_persist_turn_resolution`'s
base block reads `mentions` (reject-branch audit warning) — a free variable the design's per-lift
contract table omitted from that helper's signature. Added `mentions` as a 9th parameter to keep
the lift byte-for-byte; dropping the reference would have silently changed behavior. AST-equality
proof (`ast_check_s4_pr5.py`, scratch-only): 7/7 `OK`, incl. both appended-return (c) shapes and
the call-site arg/target order. 7 new RED-first quirk tests (`TestResolveQaWinner` ×4,
`TestPersistTurnResolution` ×3) pin the design's named quirks (wide-repass gating, exception
fallback, wide-reject-then-primary-crosscheck, `turn` never mutated; reject withholds write+patch,
promotion sticky on `promote_signal` alone, `promoted` stays `False` on falsy `winner_name`) — all
confirmed RED (ImportError) before the lift, GREEN after. `uvx ruff check .` and
`uvx ruff format --check .` clean. Focused
`uv run pytest -o addopts= tests/congress_videos/test_speaker_turn_prepare_dag.py` → 82 passed.
DagBag import check clean (0 import errors). `git diff --shortstat`: 2 files, 284 insertions(+),
75 deletions(-) = 359 changed lines (under 400, no `size:exception`). Diff on the test file shows
additions only. Rollback: **PR5 may only be reverted together with PR6 (or PR6 first)** — PR6
prunes the token PR5's lift half-pays for; reverting PR5 alone returns the function to 19 in a
file whose token PR6 has already dropped.

### Status (PR5)

8/8 PR5 tasks complete. Ready for `sdd-verify` on PR5's scope, then `sdd-apply` again for PR6.

## PR6 — `_prepare_turns_callable` VAD/sidecar/decode helper + token drop (`congress_videos/speaker_turn_prepare_dag.py`, part 2 of 2)

**Status: COMPLETE — 8/8 PR6 tasks done (6.1-6.8).** Branch `refactor/272-c901-slice-4-pr6`,
stacks on PR5 tip `56e2ad8` (no rebase — orchestrator directed commit-only worktree; hidden-regression
pre-check ran against the current worktree, matched PR5 exactly: `_prepare_turns_callable` 11, nothing
else, `_resolve_qa_winner` 6, `_persist_turn_resolution` 4).

Lifted `_prepare_turn_artifacts(db, turn, turn_id, output_path) -> None` (base 438-470 / PR5-tip
473-505, whole `try/except`) immediately above `_prepare_turns_callable`; call site → one statement.
Applied normalization (a) twice (loop-exit `continue` → `return None` at both the failed-decode branch
and the outer `except`); the try/except was already the loop body's last statement, so control flow is
unchanged. Complexity measured: `_prepare_turn_artifacts`→**3**, `_prepare_turns_callable` 11→**9**
(matches design prediction exactly). AST-equality proof (`ast_check_s4_pr6.py`, scratch-only): 4/4
`OK`. 3 new RED-first quirk tests (`TestPrepareTurnArtifacts`: rc!=0 skips mark_turn_prepared;
exceptions swallowed, never raise; mark_turn_prepared is the last success-path call), all confirmed
RED (ImportError) before the lift, GREEN after. Hidden-regression re-run immediately before the token
drop: clean. Same commit: `pyproject.toml` `["C901","UP022"]`→`["UP022"]`,
`EXPECTED_C901_FILE_COUNT` 8→7; `test_every_code_list_is_sorted_and_deduped` passes.
`uvx ruff check .` and `uvx ruff format --check .` clean. Focused
`uv run pytest -o addopts= tests/congress_videos/test_speaker_turn_prepare_dag.py
tests/test_ruff_config.py` → 99 passed. DagBag import check clean (0 import errors).
`git diff --shortstat` (56e2ad8..HEAD): 4 files, 109 insertions(+), 35 deletions(-) = 144 changed
lines, well under 400 (no `size:exception`). Test-file diff shows additions only (69/0). Commit
`55f9b5a`. Rollback: `git revert` PR6 must accompany or follow a PR5 revert; PR6 is the final tip of
the six-PR stack.

### Status (PR6)

8/8 PR6 tasks complete. All six work-unit PRs of slice-4 are done. Ready for `sdd-verify` on PR6's
scope, then the orchestrator-owned final tip (full suite, e2e, release PR, issue #272 comment).
