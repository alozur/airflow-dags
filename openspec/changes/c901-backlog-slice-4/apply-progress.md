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

### Remaining tasks (PR2-PR6, final tip)

- [ ] PR2 — `normalize_chapter_speakers` (`speaker_normalization.py`)
- [ ] PR3 — `_resolve_speaker_inner` (`speaker_resolution.py`, budget-risk PR)
- [ ] PR4 — `derive_candidate_intervals` (`candidate_intervals.py`, characterization tests first)
- [ ] PR5 — `_prepare_turns_callable` part 1 (`speaker_turn_prepare_dag.py`)
- [ ] PR6 — `_prepare_turns_callable` part 2 + token drop
- [ ] Final tip — full `uv run pytest`, e2e, release PR, issue #272 slice-4 report comment

### Workload / PR Boundary

- Mode: chained PR slice (`auto-chain` / `stacked-to-main`)
- Current work unit: PR1 of 6 (+ release PR)
- Boundary: starts at `413d2fe` (worktree base), ends at `e9defe6` (PR1 tip) — free prune of
  `reap_clip_preparer_dag.py` + full lift of both `speaker_turns.py` C901 offenders + token drop
- Estimated review budget impact: 232 changed lines, well under the 400-line budget; PR1 is independent
  and reverts cleanly without touching PR2-PR6

### Status

11/11 PR1 tasks complete. Ready for `sdd-verify` on PR1's scope, then `sdd-apply` again for PR2.
