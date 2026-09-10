# Apply Progress: C901 Backlog Slice 5 (issue #272)

Batches: sdd-apply batch 1 of 3 — PR1 through PR4 (PR4 split into 4a/4b per the
budget contingency), scope `congress_videos/modules/youtube/youtube_channel.py`.
sdd-apply batch 2 of 3 — PR5 through PR7, scope
`congress_videos/modules/youtube/download.py` (this batch's addition). PR8-PR9
and the release PR are NOT started (owned by batch 3).

Worktree: `/home/alozur/src/github.com/alozur/airflow-dags-wt-272-s5`
Base: `origin/main 7e3e689` (== `origin/dev`).
Base gate measured before any change: `uv run pytest -n auto` → **5274 passed, 34 skipped**;
`uv run ruff check .` and `uv run ruff format --check .` both clean.

## Branch / commit map

```
refactor/272-c901-slice-5                 (docs(sdd) e83a24c, base of the stack)
└─ 9ed65f4  refactor(youtube-channel): lift enrichable-video-details fetch ...   [PR1]
   refactor/272-s5-pr2-filter-finished-streams
   └─ 448d930  refactor(youtube-channel): lift finished-stream-candidate ...     [PR2]
      refactor/272-s5-pr3-extract-session-date
      └─ 9a25e1c  refactor(youtube-channel): lift agenda-date parsing ...        [PR3]
         refactor/272-s5-pr4-extract-agenda-section
         └─ 80ee6a7  test(youtube-channel): add characterization tests ...       [PR4a]
            refactor/272-s5-pr4b-extract-agenda-section-lift
            └─ e3d33a4  refactor(youtube-channel): lift agenda lookup ...        [PR4b]
               refactor/272-s5-pr5-dedup-overlapping-chapters
               └─ 22385e1  refactor(download): lift chapter-boundary accessors ... [PR5]
                  refactor/272-s5-pr6-identify-interesting-chapters
                  └─ 4083139  refactor(download): lift SRT-chunk lookup ...        [PR6]
                     refactor/272-s5-pr7-analyze-single-chunk
                     └─ cecf397  refactor(download): lift chapter-identification ... [PR7]
```

Each branch's parent is the previous branch's tip (stacked-to-main / `dev`, per
the design's chain strategy). Not pushed, no PRs opened — orchestrator's job.

## PR1 — `get_video_details` (`youtube_channel.py:481-505`)

- Branch: `refactor/272-c901-slice-5` (this change's tracker branch; PR1's own branch)
- Commit: `9ed65f4`
- Files: `congress_videos/modules/youtube/youtube_channel.py`,
  `tests/congress_videos/modules/youtube/test_youtube_channel.py`
- Changed lines: `2 files changed, 219 insertions(+), 24 deletions(-)` (243 total, budget 400)
- Test-diff additions-only: confirmed empty (`git diff -- tests/ | rg '^-[^-]'`)

Gate outputs:
```
$ uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel.py -o addopts=
80 passed in 1.33s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_fetch_enrichable_video_details` is too complex (4 > 1)   # predicted 4
C901 `get_video_details` is too complex (9 > 1)                 # predicted 9

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted   (one reformat pass applied and re-verified clean)
```

Baseline (before lift) confirmed: `get_video_details` measured **11**, matching design.

AST-equality proof (`ast_check_s5.py pr1`):
```
OK _fetch_enrichable_video_details (normalized: continue->return x3, appended return)
OK get_video_details (before lifted range) (verbatim)
OK get_video_details (after lifted range) (verbatim)
OK get_video_details (call-site replacement) (declared call-site statements)
```

Design deviation: none. The 481-505 lift boundary (narrowed vs. the exploration)
was followed exactly; duration parsing (513-527) stays in the caller.
`TestGetVideoDetailsDurationLeak` (2 tests) pins the pre-existing cross-iteration
leak, green against untouched source before the lift.

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel.py -o addopts=` → 80 passed |
| Runtime harness | N/A — pure API-response helper, no scheduling/DAG surface touched |
| Rollback boundary | `git revert 9ed65f4`; per design, only together with/after PR4 (token drop) |

## PR2 — `filter_finished_streams` (`youtube_channel.py:374-418`)

- Branch: `refactor/272-s5-pr2-filter-finished-streams` (parent: PR1 tip)
- Commit: `448d930`
- Files: `congress_videos/modules/youtube/youtube_channel.py`,
  `tests/congress_videos/modules/youtube/test_youtube_channel.py`
- Changed lines: `2 files changed, 189 insertions(+), 45 deletions(-)` (234 total)
- Test-diff additions-only: confirmed empty

Gate outputs:
```
$ uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel.py -o addopts=
92 passed in 1.15s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_evaluate_finished_stream_candidate` is too complex (8 > 1)   # predicted 8
C901 `filter_finished_streams` is too complex (7 > 1)                # predicted 7

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted
```

Baseline confirmed: `filter_finished_streams` measured **13**, matching design.

AST-equality proof (`ast_check_s5.py pr2`):
```
OK _evaluate_finished_stream_candidate (normalized: continue->return x6, append->return x1, appended return)
OK _evaluate_finished_stream_candidate (zero Try nodes — fail-closed landmine guard)
OK filter_finished_streams (try/except handlers, order+body unchanged) (verbatim)
OK filter_finished_streams (try body replacement) (declared call-site statements)
```

Landmine guard verified mechanically: zero `Try` nodes inside the new helper —
`filter_finished_streams`'s existing `try/except` keeps owning fail-closed
propagation per candidate.

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel.py -o addopts=` → 92 passed |
| Runtime harness | N/A — pure per-candidate helper |
| Rollback boundary | `git revert 448d930`; only together with/after PR4 |

## PR3 — `extract_session_date` (`youtube_channel.py:985-1010, 1037-1047`)

- Branch: `refactor/272-s5-pr3-extract-session-date` (parent: PR2 tip)
- Commit: `9a25e1c`
- Files: `congress_videos/modules/youtube/youtube_channel.py`,
  `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py`
- Changed lines: `2 files changed, 213 insertions(+), 37 deletions(-)` (250 total)
- Test-diff additions-only: confirmed empty

Gate outputs:
```
$ uv run pytest tests/congress_videos/modules/youtube/ -o addopts=
385 passed in 18.62s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_parse_agenda_dates` is too complex (4 > 1)           # predicted 4
C901 `_locate_target_date_offset` is too complex (3 > 1)    # predicted 3
C901 `extract_session_date` is too complex (9 > 1)          # predicted 9

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted   (one reformat pass applied and re-verified clean)
```

Baseline confirmed: `extract_session_date` measured **14**, matching design.

AST-equality proof (`ast_check_s5.py pr3`):
```
OK _parse_agenda_dates (normalized: appended return)
OK _locate_target_date_offset (normalized: appended return)
OK extract_session_date (region before parsed_dates block) (verbatim)
OK extract_session_date (region between the two lifted blocks) (verbatim)
OK extract_session_date (region after offset block) (verbatim)
OK extract_session_date (call-site 1 replacement) (declared call-site statement)
OK extract_session_date (call-site 2 replacement) (declared call-site statement)
```

Landmine guard verified: the caller keeps `if not found_target:` verbatim;
`found_target` stays the sole disambiguator for offset 0 (the falsy-valid trap).
Pinned by `test_first_date_is_offset_zero_and_found_true`.

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_extended.py -o addopts=` → 33 passed (file-scoped); 385 passed at the `tests/congress_videos/modules/youtube/` directory level |
| Runtime harness | N/A — pure parsing helpers |
| Rollback boundary | `git revert 9a25e1c`; only together with/after PR4 |

## PR4 — `extract_agenda_section` — split into 4a + 4b (budget contingency triggered)

- Branch 4a: `refactor/272-s5-pr4-extract-agenda-section` (parent: PR3 tip), commit `80ee6a7`
- Branch 4b: `refactor/272-s5-pr4b-extract-agenda-section-lift` (parent: 4a tip), commit `e3d33a4`

**Budget check (task 4.8) fired**: combined characterization-tests + lift diff
measured `git diff --shortstat 9a25e1c..HEAD` = `2 files changed, 380 insertions(+),
43 deletions(-)` = **423 changed lines**, over the 400 budget. Applied the
pre-approved contingency exactly as designed:
- **4a** = commit 1 only (characterization tests), no source change, no prune.
- **4b** = commit 2 (both helpers + prune + counter 7→6).

### PR4a — characterization tests (own commit, no source change)

- Files: `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py`
- Changed lines: `1 file changed, 183 insertions(+)` — additions only, confirmed
- Zero prior coverage confirmed: `rg extract_agenda_section tests/` → 0 matches (before this commit)
- Ten named tests in `TestExtractAgendaSection`, all green against untouched
  pre-refactor source:
```
$ uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_extended.py::TestExtractAgendaSection -o addopts=
10 passed in 0.64s
```

**Deviation from design**: the design's worked example stated
`section_length == 45` for `test_extracts_section_between_target_header_and_next_header`.
Measured against the actual `AGENDA_TEXT`/`SESSION_INFO` fixtures given in the
design (copied verbatim), the real value is **50** (confirmed both by the
`len()`-based assertion and the function's own `logging.info` output: "Section:
50 chars"). This is a numeric-literal transcription slip in the design's worked
example, not a boundary/signature/line-range disagreement — the fixture text,
function under test, and lift boundaries are all exactly as designed. Corrected
the hardcoded literal to 50 in the test (the `len(expected)`-based assertion,
which is the authoritative one per the design's own "asserted as len" note,
was unaffected and passed as written).

`extract_agenda_section` still measures **17** at this commit (unchanged, still
masked by the `"C901"` per-file-ignores token) — confirmed no source file changed.

### PR4b — lift + prune (own commit, after 4a)

- Files: `congress_videos/modules/youtube/youtube_channel.py`, `pyproject.toml`,
  `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py`,
  `tests/test_ruff_config.py`
- Changed lines: `4 files changed, 199 insertions(+), 45 deletions(-)` (244 total, own budget)
- Test-diff additions-only: confirmed empty for the two `tests/congress_videos/...`
  files. `tests/test_ruff_config.py` shows one deletion
  (`EXPECTED_C901_FILE_COUNT = 7` → `= 6`), which is the spec-mandated
  lockstep counter decrement (Requirement: "Per-file-ignores entries drop
  their C901 token in lockstep with the counter"), not a behavior-test edit
  for any of the ten target functions — called out explicitly per the
  instructions rather than silently included in the "zero pre-existing
  assertions edited" claim.

RED-first tests added first (both helpers, 9 tests total): confirmed RED
(`ImportError`) before the lift, then GREEN after:
```
$ uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_extended.py::TestFindAgendaForVideo tests/congress_videos/modules/youtube/test_youtube_channel_extended.py::TestLocateTargetSection -o addopts= -q
(before lift)  9 failed  — all ImportError: cannot import name '_locate_target_section' / '_find_agenda_for_video'
```

Gate outputs (after the lift):
```
$ uv run pytest tests/congress_videos/modules/youtube/ -o addopts=
404 passed in 8.10s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_find_agenda_for_video` is too complex (3 > 1)      # predicted 3
C901 `_locate_target_section` is too complex (8 > 1)      # predicted 8
C901 `extract_agenda_section` is too complex (8 > 1)      # predicted 8

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted
```

Baseline confirmed: `extract_agenda_section` measured **17**, matching design.

Hidden-regression check (task 4.7, all four `youtube_channel.py` functions,
PR1-4 all applied in this worktree):
```
$ uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise congress_videos/modules/youtube/youtube_channel.py
All checks passed!
```
Zero offenders confirmed → safe to drop the `"C901"` token. Same commit:
`pyproject.toml` line 127 `["B007","C901","F841","SIM102"]` → `["B007","F841","SIM102"]`;
`EXPECTED_C901_FILE_COUNT` `7` → `6` in `tests/test_ruff_config.py`.
```
$ uv run pytest tests/test_ruff_config.py -o addopts=
14 passed in 0.12s
```

AST-equality proof (`ast_check_s5.py pr4`):
```
OK _find_agenda_for_video (normalized: appended return)
OK _locate_target_section (normalized (f): target_section=None relocated across target_date_dt=..., appended return)
OK _locate_target_section (f) relocation crosses no read/write of target_section
OK extract_agenda_section (region before agenda_item block) (verbatim)
OK extract_agenda_section (region between the two lifted blocks) (verbatim)
OK extract_agenda_section (target_date_dt statement, unmoved) (verbatim)
OK extract_agenda_section (region after target_section block) (verbatim)
OK extract_agenda_section (call-site 1 replacement) (declared call-site statement)
OK extract_agenda_section (call-site 2 replacement) (declared call-site statement)
OK extract_agenda_section (landmine: if target_section: truthy check) (verbatim — never rewritten to is not None)
```

Landmine guards verified:
- `if target_section:` at (base) line 1235 confirmed byte-identical and outside
  every lifted range — never rewritten to `is not None`.
- Norm (f) relocation of `target_section = None` (base 1195) past
  `target_date_dt = ...` (base 1196) mechanically asserted: `target_section`
  appears in neither the `Load` nor `Store` name set of the crossed statement.
- `test_returns_empty_string_when_slice_is_whitespace_only` pins the
  empty-string case reachable only through the helper's new standalone
  interface (per the design's "finding" section).

Work Unit Evidence (4a):
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_extended.py::TestExtractAgendaSection -o addopts=` → 10 passed |
| Runtime harness | N/A — characterization-only commit, no production code changed |
| Rollback boundary | `git revert 80ee6a7`; standalone (no source change to revert alongside) |

Work Unit Evidence (4b):
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_extended.py -o addopts=` → 43 passed |
| Runtime harness | N/A — pure text-scan helpers, no scheduling/DAG surface touched |
| Rollback boundary | `git revert e3d33a4`; per design, PR1-4 only revertible together/in order (this commit drops the file's C901 token — reverting it alone would restore a live offender) |

## Cumulative gate at batch 1's tip (`e3d33a4`, branch `refactor/272-s5-pr4b-extract-agenda-section-lift`)

```
$ uv run pytest -n auto
5322 passed, 34 skipped in 132.91s        (base 5274 + 48 new tests; zero regressions)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted

$ uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise congress_videos/modules/youtube/youtube_channel.py
All checks passed!

$ git diff origin/main..HEAD -- tests/ | rg '^-[^-]'
-    EXPECTED_C901_FILE_COUNT = 7
```
(That single line is the spec-mandated counter decrement in
`tests/test_ruff_config.py`, documented above — not an edit to any of the
ten target functions' behavior tests. Every functional test-assertion diff
across batch 1 is additions-only.)

## PR5 — `_dedup_overlapping_chapters` (`download.py:1079-1145`)

- Branch: `refactor/272-s5-pr5-dedup-overlapping-chapters` (parent: PR4b tip)
- Commit: `22385e1`
- Files: `congress_videos/modules/youtube/download.py`,
  `tests/congress_videos/modules/youtube/test_download.py`
- Changed lines: `2 files changed, 141 insertions(+), 40 deletions(-)` (181 total, budget 400)
- Test-diff additions-only: confirmed empty

Baseline confirmed: `_dedup_overlapping_chapters` measured **14**, matching design.

RED-first tests (7 tests, classes `TestChapterStartEndSecs` +
`TestMarkOverlappingChapters`) confirmed RED before the lift:
```
$ uv run pytest tests/congress_videos/modules/youtube/test_download.py::TestChapterStartEndSecs tests/congress_videos/modules/youtube/test_download.py::TestMarkOverlappingChapters -o addopts= -q
7 failed — all ImportError: cannot import name '_chapter_start_secs' / '_chapter_end_secs' / '_mark_overlapping_chapters'
```

Gate outputs (after the lift):
```
$ uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=
129 passed in 2.49s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_chapter_start_secs` is too complex (2 > 1)             # predicted 2
C901 `_chapter_end_secs` is too complex (2 > 1)                # predicted 2
C901 `_mark_overlapping_chapters` is too complex (9 > 1)       # predicted 9
C901 `_dedup_overlapping_chapters` is too complex (2 > 1)      # predicted 2

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted
```

AST-equality proof (`ast_check_s5_pr5.py`):
```
OK _chapter_start_secs (normalized (d): name _start_secs -> _chapter_start_secs)
OK _chapter_end_secs (normalized (d): name _end_secs -> _chapter_end_secs)
OK _mark_overlapping_chapters (normalized (d) x2: Name _chapter_start_secs->_start_secs, _chapter_end_secs->_end_secs)
OK _mark_overlapping_chapters (landmine) (overlap <= 0.0 is Compare(ops=[LtE()]), never rewritten to <)
OK _dedup_overlapping_chapters (guard clause) (verbatim)
OK _dedup_overlapping_chapters (sort statement) (normalized (d): Name _chapter_start_secs -> _start_secs)
OK _dedup_overlapping_chapters (keep init) (verbatim)
OK _dedup_overlapping_chapters (call-site replacement) (declared call-site statement)
OK _dedup_overlapping_chapters (return statement) (verbatim)
```

Landmine guard verified mechanically: `overlap <= 0.0` (base 1110) is
`Compare(ops=[LtE()])` in the shipped dump — never rewritten to `<`. The
unsorted 3-chapter quirk test (`test_touching_boundary_uses_lte_not_lt`)
makes the operator observable at the new `_mark_overlapping_chapters` seam.

**Design deviation found (documented, non-blocking)**: task 5.3 asks for a
quirk test pinning `min_dur <= 0.0 -> continue` ("degenerate chapter
skipped, not discarded"). Proved mathematically (and confirmed empirically
with a 2,000,000-sample randomized search — zero counter-examples) that
this branch is **unreachable dead code**: `overlap = max(0.0, min(end_a,
end_b) - max(start_a, start_b))` being `> 0.0` requires, by strict interval
arithmetic, both `end_a > max(start_a, start_b)` and `end_b > max(start_a,
start_b)`, which forces `dur_a > 0` and `dur_b > 0` — so by the time the
`if overlap <= 0.0: break` guard is already passed, `min_dur <= 0.0` can
never be true, for any real chapter geometry (mocking `_chapter_start_secs`/
`_chapter_end_secs` does not change this — the proof depends only on the
four returned values' ordering, not their source). This is a pure-lift
target; the `continue` statement is preserved byte-for-byte and covered by
the AST-equality proof, but no reachable-behavior test exists for it. The
achievable quirks (mutates-in-place/returns-None, the `<=` touching
boundary, narrower-discarded-and-breaks) are tested instead.

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=` → 129 passed |
| Runtime harness | N/A — pure chapter-list helpers, no scheduling/DAG surface touched |
| Rollback boundary | `git revert 22385e1`; per design, only together with/after PR7 (token drop) |

## PR6 — `identify_interesting_chapters` (`download.py:1461-1549`)

- Branch: `refactor/272-s5-pr6-identify-interesting-chapters` (parent: PR5 tip)
- Commit: `4083139`
- Files: `congress_videos/modules/youtube/download.py`,
  `tests/congress_videos/modules/youtube/test_download.py`
- Changed lines: `2 files changed, 256 insertions(+), 79 deletions(-)` (335 total, budget 400)
- Test-diff additions-only: confirmed empty

Baseline confirmed: `identify_interesting_chapters` measured **12**, matching design.

RED-first tests (12 tests, classes `TestFindSrtChunksForVideo` +
`TestCollectChunkChapters`) confirmed RED before the lift (`ImportError`).

Gate outputs (after the lift):
```
$ uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=
141 passed in 2.24s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_find_srt_chunks_for_video` is too complex (4 > 1)       # predicted 4
C901 `_collect_chunk_chapters` is too complex (4 > 1)          # predicted 4
C901 `identify_interesting_chapters` is too complex (6 > 1)    # predicted 6

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted   (one reformat pass applied and re-verified clean)
```

AST-equality proof (`ast_check_s5_pr6.py`):
```
OK _find_srt_chunks_for_video (normalized: appended return)
OK _collect_chunk_chapters (normalized: appended return)
OK identify_interesting_chapters (region before srt_chunks block) (verbatim)
OK identify_interesting_chapters (call-site 1 replacement) (declared call-site statement)
OK identify_interesting_chapters (if not srt_chunks guard) (verbatim)
OK identify_interesting_chapters (try: logging.info) (verbatim)
OK identify_interesting_chapters (call-site 2 replacement) (declared call-site statement)
OK identify_interesting_chapters (try: region after chunks_with_chapters block) (verbatim)
OK identify_interesting_chapters (except handlers) (verbatim, order+body unchanged)
```

Landmine guards verified: the intentional `if not srt_content:` falsy check
(base :1490, matching `_find_srt_chunk`'s `""`-on-no-match contract) moved
verbatim into `_collect_chunk_chapters`, pinned by
`test_missing_srt_content_yields_error_entry`; the `<=` optimal-duration
boundary (`chunk_duration <= max_optimal_duration`) pinned by
`test_duration_equal_to_max_optimal_is_whole_chunk_optimal`. The whole
per-chunk loop moved as one contiguous unit — both `continue`s stay
verbatim — with `_build_srt_chunk_index` pulled inside so no initializer
relocation was needed.

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=` → 141 passed |
| Runtime harness | N/A — pure SRT-index/chapter-collection helpers |
| Rollback boundary | `git revert 4083139`; per design, only together with/after PR7 (token drop) |

## PR7 — `_analyze_single_chunk` (`download.py:1291-1340`) + `download.py` C901 prune + counter 6→5

- Branch: `refactor/272-s5-pr7-analyze-single-chunk` (parent: PR6 tip)
- Commit: `cecf397`
- Files: `congress_videos/modules/youtube/download.py`, `pyproject.toml`,
  `tests/congress_videos/modules/youtube/test_download.py`,
  `tests/test_ruff_config.py`
- Changed lines: `4 files changed, 203 insertions(+), 52 deletions(-)` (255 total, budget 400)
- Test-diff additions-only: confirmed empty except the spec-mandated
  `EXPECTED_C901_FILE_COUNT = 6` → `= 5` lockstep decrement in
  `tests/test_ruff_config.py` (Requirement: "Per-file-ignores entries drop
  their C901 token in lockstep with the counter") — not a behavior-test
  edit for any of the ten target functions.

Baseline confirmed: `_analyze_single_chunk` measured **15**, matching design.

RED-first tests (5 tests, class `TestIdentifyChaptersForChunk`) confirmed
RED before the lift (`ImportError`), including the mandatory closure-capture
proof (`test_oversized_srt_closure_captures_summary_text`): fakes
`map_reduce_identify_chapters` to capture the `identify_fn` kwarg and
invoke it with a synthetic window, then asserts the resulting `user_prompt`
still contains the same `summary_text` built inside the helper's own scope
(`"Chunk 7 (00:00:00 - 01:00:00)"`, the speaker line, `"Topics: a, b"`,
`"Summary: Debate sobre presupuestos"`).

Gate outputs (after the lift):
```
$ uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=
146 passed in 2.66s

$ uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>
C901 `_identify_chapters_for_chunk` is too complex (8 > 1)     # predicted 8
C901 `_analyze_single_chunk` is too complex (8 > 1)            # predicted 8

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted   (one reformat pass applied and re-verified clean)
```

AST-equality proof (`ast_check_s5_pr7.py`):
```
OK _analyze_single_chunk (imports + logging.info) (verbatim)
OK _identify_chapters_for_chunk (normalized (d) x2: system_prompt->CHAPTER_IDENTIFICATION_SYSTEM_PROMPT, user_prompt_template->CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE; appended return)
OK _identify_chapters_for_chunk (_identify_window intact) (Return + Raise present, moved as one unit)
OK _identify_chapters_for_chunk (closure keyword) (map_reduce_identify_chapters(identify_fn=Name('_identify_window')))
OK _analyze_single_chunk (call-site replacement) (declared call-site statement)
OK _analyze_single_chunk (try: region after chunk-identification block) (verbatim)
OK _analyze_single_chunk (except handlers) (verbatim, order preserved: json.JSONDecodeError before Exception)
```

Landmine guards verified: `_identify_window` and its sole call site into
`map_reduce_identify_chapters` moved as one atomic unit; `summary_text` is
a plain local of `_identify_chapters_for_chunk` with no cross-boundary
capture; `interesting_chapters` stays live in the outer scope for the
`is_single_chapter` comparison at base :1381 (unaffected, outside the
lifted range); the `from congress_videos.config.ai_prompts import (...)`
at base 1281-1284 stayed in the caller, before the `try` — not moved into
the helper, so an `ImportError` there still propagates uncaught rather than
being silently converted into a whole-chunk fallback; `except
json.JSONDecodeError` stays ordered before `except Exception`, both
handlers' bodies unchanged.

Hidden-regression check (task 7.5, all three `download.py` functions,
PR5-7 all applied in this worktree):
```
$ uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise congress_videos/modules/youtube/download.py
All checks passed!
```
Zero offenders confirmed → safe to drop the `"C901"` token. Same commit:
`pyproject.toml` line 126 `["B905","C901","SIM103","SIM108"]` →
`["B905","SIM103","SIM108"]`; `EXPECTED_C901_FILE_COUNT` `6` → `5` in
`tests/test_ruff_config.py`.
```
$ uv run pytest tests/test_ruff_config.py -o addopts=
14 passed in 0.11s
```

Work Unit Evidence:
| Evidence | Value |
|---|---|
| Focused test | `uv run pytest tests/congress_videos/modules/youtube/test_download.py -o addopts=` → 146 passed |
| Runtime harness | N/A — pure LLM-call helper, no I/O boundary change |
| Rollback boundary | `git revert cecf397`; per design, PR5-7 only revertible together/in order (this commit drops the file's C901 token — reverting it alone would restore a live offender) |

## Cumulative gate at this batch's tip (`cecf397`, branch `refactor/272-s5-pr7-analyze-single-chunk`)

```
$ uv run pytest -n auto
5346 passed, 34 skipped in 69.90s        (base 5322 + 24 new tests; zero regressions)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
333 files already formatted

$ uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise congress_videos/modules/youtube/download.py
All checks passed!

$ git diff refactor/272-s5-pr4b-extract-agenda-section-lift..HEAD -- tests/ | rg '^-[^-]'
-    EXPECTED_C901_FILE_COUNT = 6
```
(That single line is the spec-mandated counter decrement in
`tests/test_ruff_config.py`, documented above — not an edit to any of the
three `download.py` target functions' behavior tests. Every functional
test-assertion diff across PR5-7 is additions-only.)

## Complexity ladder (cumulative, batch 1 + batch 2)

| Function | Base Cx | Predicted | Measured | Status |
|---|---|---|---|---|
| `get_video_details` | 11 | 9 | **9** | ✅ |
| `_fetch_enrichable_video_details` (new) | — | 4 | **4** | ✅ |
| `filter_finished_streams` | 13 | 7 | **7** | ✅ |
| `_evaluate_finished_stream_candidate` (new) | — | 8 | **8** | ✅ |
| `extract_session_date` | 14 | 9 | **9** | ✅ |
| `_parse_agenda_dates` (new) | — | 4 | **4** | ✅ |
| `_locate_target_date_offset` (new) | — | 3 | **3** | ✅ |
| `extract_agenda_section` | 17 | 8 | **8** | ✅ |
| `_find_agenda_for_video` (new) | — | 3 | **3** | ✅ |
| `_locate_target_section` (new) | — | 8 | **8** | ✅ |
| `_dedup_overlapping_chapters` | 14 | 2 | **2** | ✅ |
| `_chapter_start_secs` (new) | — | 2 | **2** | ✅ |
| `_chapter_end_secs` (new) | — | 2 | **2** | ✅ |
| `_mark_overlapping_chapters` (new) | — | 9 | **9** | ✅ |
| `identify_interesting_chapters` | 12 | 6 | **6** | ✅ |
| `_find_srt_chunks_for_video` (new) | — | 4 | **4** | ✅ |
| `_collect_chunk_chapters` (new) | — | 4 | **4** | ✅ |
| `_analyze_single_chunk` | 15 | 8 | **8** | ✅ |
| `_identify_chapters_for_chunk` (new) | — | 8 | **8** | ✅ |

## C901 counter ladder (cumulative, batch 1 + batch 2)

`youtube_channel.py` entry: `["B007","C901","F841","SIM102"]` →
`["B007","F841","SIM102"]` (token dropped in commit `e3d33a4`, PR4b).
`download.py` entry: `["B905","C901","SIM103","SIM108"]` →
`["B905","SIM103","SIM108"]` (token dropped in commit `cecf397`, PR7).
`EXPECTED_C901_FILE_COUNT`: `7` → `6` (PR4b) → `5` (PR7).

## Tasks completed (tasks.md)

PR1 (1.1-1.7), PR2 (2.1-2.6), PR3 (3.1-3.7), PR4 (4.1-4.10), PR5 (5.1-5.7),
PR6 (6.1-6.7), PR7 (7.1-7.7) — all 51 tasks marked `[x]` in
`openspec/changes/c901-backlog-slice-5/tasks.md`. PR8 onward (tasks 8.1
through 11.8) remain `[ ]` — out of this batch's scope, owned by the next
sdd-apply batch (batch 3, PR8-PR9 + release PR) per the orchestrator's
explicit "stop after PR7" scope.

## Deviations from design (full list)

1. **`section_length` literal in the design's PR4a worked example** (45 vs.
   measured 50) — corrected in the test to match the actual fixture-derived
   value; the `len()`-based assertion (the design's own stated authoritative
   form) was unaffected. Not a boundary, signature, or behavior deviation.
   (batch 1)
2. **`min_dur <= 0.0` quirk in task 5.3 is unreachable dead code** —
   mathematically proven (and empirically confirmed by a 2M-sample
   randomized search) that `_mark_overlapping_chapters` can never reach
   `min_dur <= 0.0` once `overlap > 0.0`, because interval-overlap arithmetic
   forces both chapter durations strictly positive whenever there is any
   overlap at all. The `continue` statement is preserved byte-for-byte
   (verified by the AST-equality proof) but has no reachable-behavior test;
   the three achievable quirks in `TestMarkOverlappingChapters` cover the
   rest of task 5.3. Not a boundary, signature, or lift-correctness
   deviation — a documented gap in test-writability only. (batch 2, PR5)

No other deviations. Every lift boundary, helper name, signature,
normalization, and landmine guard matched the design exactly across all
seven PRs shipped so far.

## Blockers

None. Ready for the next sdd-apply batch (PR8-PR9 + release PR) or for
sdd-verify to run independently against this batch's scope (PR1-PR7).
