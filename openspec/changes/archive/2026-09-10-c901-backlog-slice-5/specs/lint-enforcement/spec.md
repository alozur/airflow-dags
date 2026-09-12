# Delta for lint-enforcement (issue #272, slice 5)

## Purpose

Behavior-preserving refactor. Ten functions across the three multi-violator
files deferred by slice 4 (`youtube_channel.py`, `download.py`,
`utils/youtube_downloader.py`) are decomposed into module-level private
helpers so cyclomatic complexity drops to `<= 10`, letting each file's
`pyproject.toml` per-file-ignores entry lose the `"C901"` token once every
offender in that file is clean. No public signature, requirement, or
observable behavior changes.

## ADDED Requirements

### Requirement: Ten slice-5 targets report complexity <= 10 with ignores disabled

`get_video_details`, `filter_finished_streams`, `extract_session_date`,
`extract_agenda_section` (`youtube_channel.py`); `_dedup_overlapping_chapters`,
`identify_interesting_chapters`, `_analyze_single_chunk` (`download.py`);
`download_youtube_subtitles`, `download_with_pytubefix`,
`download_youtube_video_for_upload` (`utils/youtube_downloader.py`) MUST each
score `<= 10` under `uvx ruff check --select C901 --config
'lint.per-file-ignores = {}'`, and `uv run ruff check .` plus `uv run ruff
format --check .` MUST be clean at every stacked-PR tip.

#### Scenario: Neutralized ruff check passes after extraction
- GIVEN the three modified files after helper extraction
- WHEN `ruff check --select C901` runs with `per-file-ignores` neutralized
- THEN it reports zero C901 violations for all ten functions

#### Scenario: Full ruff check and format are green at every tip
- GIVEN any stacked-PR tip in this slice
- WHEN `uv run ruff check .` and `uv run ruff format --check .` run
- THEN both exit 0

### Requirement: Per-file-ignores entries drop their C901 token in lockstep with the counter

`EXPECTED_C901_FILE_COUNT` and the count of `per-file-ignores` entries
carrying `"C901"` MUST change together in the same commit or not at all,
following the ladder 7 (unchanged through `get_video_details`,
`filter_finished_streams`, `extract_session_date`) -> 6 (`youtube_channel.py`
token drop, after `extract_agenda_section`) -> 5 (`download.py` token drop,
after `_analyze_single_chunk`) -> 4 (`utils/youtube_downloader.py` token
drop, after `download_youtube_video_for_upload`), never skipping or
reordering.

#### Scenario: Entry and counter change atomically
- GIVEN a file whose entry loses its `"C901"` token
- WHEN that commit lands
- THEN `pyproject.toml` and `EXPECTED_C901_FILE_COUNT` change together, never in separate commits

#### Scenario: Ladder holds at every commit tip
- GIVEN the ten work units land in ladder order
- WHEN `EXPECTED_C901_FILE_COUNT` is read at each tip
- THEN it follows 7->6->5->4, never skipping ahead

### Requirement: Multi-offender entries drop C901 only when every offender in that file is clean

The `youtube_channel.py` entry (`["B007","C901","F841","SIM102"]`), the
`download.py` entry (`["B905","C901","SIM103","SIM108"]`), and the
`utils/youtube_downloader.py` entry (`["C901","F841"]`) MUST keep their
non-C901 codes untouched and MUST drop `"C901"` only after every C901
offender in that file scores `<= 10`; a hidden-regression check (`uvx ruff
check --select C901 --no-cache --config 'lint.per-file-ignores = {}'
--output-format concise <file>`) MUST be re-run immediately before each drop.

#### Scenario: Token dropped only after all four youtube_channel.py offenders are clean
- GIVEN `get_video_details`, `filter_finished_streams`, `extract_session_date`, `extract_agenda_section`
- WHEN all four score `<= 10` with ignores neutralized
- THEN `"C901"` is dropped from that entry and `B007`/`F841`/`SIM102` remain

#### Scenario: Token dropped only after all three download.py offenders are clean
- GIVEN `_dedup_overlapping_chapters`, `identify_interesting_chapters`, `_analyze_single_chunk`
- WHEN all three score `<= 10` with ignores neutralized
- THEN `"C901"` is dropped from that entry and `B905`/`SIM103`/`SIM108` remain

#### Scenario: Token dropped only after all three youtube_downloader.py offenders are clean
- GIVEN `download_youtube_subtitles`, `download_with_pytubefix`, `download_youtube_video_for_upload`
- WHEN all three score `<= 10` with ignores neutralized
- THEN `"C901"` is dropped from that entry and `F841` remains

#### Scenario: Early drop is rejected
- GIVEN one offender in a multi-offender file is still `> 10`
- WHEN the hidden-regression check runs before a planned token drop
- THEN the drop does not proceed for that commit

### Requirement: Extraction is a byte-for-byte lift with unchanged public signatures

Each lifted helper body MUST be AST-identical
(`ast.dump(include_attributes=False)`) to its base statements after only the
declared normalizations, and the ten refactored functions MUST keep their
existing parameter names, order, and return values for every input already
covered by existing tests.

#### Scenario: AST equality holds under declared normalizations
- GIVEN a lifted block and its base-revision counterpart
- WHEN declared normalizations are applied to the base and both are dumped
- THEN the dumps are equal

#### Scenario: Callers are unaffected
- GIVEN the existing callers of the ten refactored functions
- WHEN the extraction lands
- THEN no caller or its existing tests require any change

### Requirement: Pre-existing test assertions are never edited

A diff of test files touched by this slice MUST show additions only; no
existing assertion in a pre-existing test may be modified.

#### Scenario: Pre-existing suites stay green untouched
- GIVEN the suites covering the ten target functions
- WHEN the extraction is applied
- THEN every pre-existing test passes without any assertion line changed

### Requirement: New helpers land RED-first; extract_agenda_section gets characterization tests first

Every new helper MUST have a failing test written before the helper exists.
`extract_agenda_section`, which has zero existing test coverage, MUST
additionally get RED-first characterization tests in their own commit,
landing before any source change to that function.

#### Scenario: RED-first helper test
- GIVEN a new helper has not yet been extracted
- WHEN its test is written
- THEN the test fails against pre-extraction code, then passes after extraction

#### Scenario: Characterization tests precede the extract_agenda_section refactor
- GIVEN `extract_agenda_section` had zero test coverage
- WHEN its refactor commit is prepared
- THEN characterization tests already exist in their own prior commit and pass against pre-refactor code

### Requirement: Falsy-valid checks survive the lift unchanged

`youtube_channel.py`'s `if target_section:` check (empty-string extraction
treated as not-found) and `download.py`'s `if not srt_content:` check
(matching `_find_srt_chunk`'s documented `""`-on-no-match contract) MUST
remain truthy checks, not become `is None`/`is not None` checks.
`extract_session_date`'s target-date-offset lookup MUST keep `found_target`
as the sole truth-source distinguishing "not found" from a legitimate
offset of `0`; no lifted helper may introduce a truthiness check on the
offset value itself.

#### Scenario: Empty-string target_section still treated as not-found
- GIVEN `_locate_target_section` returns an empty string for a zero-width match
- WHEN the lifted `if target_section:` check runs
- THEN the section is treated as not-found, exactly as before the lift

#### Scenario: Offset 0 for the first agenda date is not treated as not-found
- GIVEN the target date is the first date in the sorted agenda
- WHEN `_locate_target_date_offset` returns offset `0` with `found_target=True`
- THEN the caller uses `found_target`, not the offset value, to determine that the date was found

#### Scenario: Empty-string srt_content still treated as not-found
- GIVEN `_find_srt_chunk` returns `""` for no match
- WHEN the lifted `if not srt_content:` check runs
- THEN the chunk is treated as not-found, exactly as before the lift

### Requirement: Exception ordering and propagation scope survive the lift unchanged

`download.py`'s `except json.JSONDecodeError` MUST stay ordered before its
`except Exception`, and `utils/youtube_downloader.py`'s `except
yt_dlp.utils.DownloadError` MUST stay ordered before its `except Exception`.
`get_video_details` MUST keep aborting the whole function on a single API
failure and `filter_finished_streams` MUST keep failing closed per
candidate; no lifted helper may wrap itself in its own `try/except` in a way
that harmonizes these two propagation behaviors. `download_with_pytubefix`'s
ffmpeg `subprocess.run` call MUST stay unwrapped so a `TimeoutExpired`
still reaches the function-level handler, and both its divergent cleanup
paths (success and failure) MUST survive verbatim.

#### Scenario: JSONDecodeError is caught before the generic handler
- GIVEN the lifted `download.py` exception block
- WHEN a `json.JSONDecodeError` is raised
- THEN it is caught by the `json.JSONDecodeError` handler, not by `except Exception`

#### Scenario: DownloadError is caught before the generic handler
- GIVEN the lifted `utils/youtube_downloader.py` exception block
- WHEN a `yt_dlp.utils.DownloadError` is raised
- THEN it is caught by the `DownloadError` handler, not by `except Exception`

#### Scenario: get_video_details aborts on a single API failure
- GIVEN one video's `.execute()` call raises inside the lifted per-video helper
- WHEN `get_video_details` processes the batch
- THEN the whole function aborts, matching pre-lift behavior

#### Scenario: filter_finished_streams fails closed per candidate
- GIVEN one candidate raises inside the lifted per-video helper
- WHEN `filter_finished_streams` processes the batch
- THEN only that candidate is dropped and the rest of the batch is still evaluated

#### Scenario: TimeoutExpired still reaches the outer handler
- GIVEN the lifted ffmpeg merge helper in `download_with_pytubefix`
- WHEN `subprocess.run` raises `TimeoutExpired`
- THEN the exception propagates unwrapped to the function-level `except Exception`

### Requirement: _analyze_single_chunk's closure moves as one atomic unit

The nested closure `_identify_window` and its sole call site MUST move
together into the same lifted helper; the captured local (`summary_text`)
MUST stay entirely inside that helper's scope with no cross-boundary
capture. `interesting_chapters` MUST stay live in `_analyze_single_chunk`'s
outer scope so the `is_single_chapter` comparison can read both helpers'
outputs; neither helper may consume or shadow it.

#### Scenario: Closure and call site are co-located
- GIVEN the lifted `_identify_chapters_for_chunk` helper
- WHEN its source is inspected
- THEN `_identify_window` and its call into `map_reduce_identify_chapters` are both defined inside that helper

#### Scenario: interesting_chapters remains available for the single-chapter comparison
- GIVEN both lifted helpers have returned
- WHEN `_analyze_single_chunk` computes `is_single_chapter`
- THEN it reads `interesting_chapters` from its own outer scope, unmodified by either helper

### Requirement: Full suite is green with coverage >= 80% at the final tip

`uv run pytest` MUST pass with at least 5274 passed and 34 skipped, and
coverage `>= 80%`, at the slice's final tip; focused runs during development
MAY use `-o addopts=` to bypass the coverage gate.

#### Scenario: Final tip meets the coverage gate
- GIVEN the final commit of this slice
- WHEN `uv run pytest` runs with default addopts
- THEN it passes with at least 5274 passed, 34 skipped, and reports coverage `>= 80%`

### Requirement: Deferred functions and files stay untouched

`create_app` (both benchmark servers), `_default_model_loader`,
`trim_turn_silence_with_vad`, `_generate_metadata`,
`build_shorts_metadata_context`, and the shared `spanish_months`/
`date_pattern` duplication between `extract_agenda_section` and
`extract_session_date` MUST NOT change in this slice, and each deferral
MUST be recorded in issue #272's slice report.

#### Scenario: Deferred backlog untouched
- GIVEN the listed deferred functions, files, and dedup opportunity
- WHEN the slice's PR diffs are reviewed
- THEN none of them changed, and issue #272 stays open with 4 entries remaining

## Acceptance Criteria

- [ ] All ten in-scope functions score `<= 10` via `ruff check --select C901` with ignores neutralized.
- [ ] `uv run ruff check .` and `uv run ruff format --check .` are clean at every stacked-PR tip.
- [ ] `EXPECTED_C901_FILE_COUNT` reads 4 and `pyproject.toml` carries exactly 4 `"C901"` entries at the final tip.
- [ ] Every entry token-drop lands in the same commit as its counter decrement, following the 7->6->5->4 ladder.
- [ ] `uv run pytest` is green (>= 5274 passed, 34 skipped) with coverage `>= 80%` at the final tip.
- [ ] Zero pre-existing test assertions edited (diff shows additions only in touched test files).
- [ ] `extract_agenda_section` has characterization tests committed, in their own commit, before its refactor.
- [ ] All landmine scenarios (falsy-valid checks, exception ordering/propagation, closure atomicity) hold verbatim.
- [ ] Listed deferrals (`create_app` x2, `_default_model_loader`, `trim_turn_silence_with_vad`, `_generate_metadata`, `build_shorts_metadata_context`, `spanish_months`/`date_pattern` dedup) are unchanged and recorded in the slice report.
- [ ] Every PR is <= 400 changed lines with no `size:exception`.
