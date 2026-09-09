# Delta for lint-enforcement (issue #272, slice 4)

## Purpose

Behavior-preserving refactor. Six functions across five files are decomposed
into module-level private helpers (or, for one file, pruned with zero
violations) so cyclomatic complexity drops to `<= 10`, letting their
`pyproject.toml` per-file-ignores entries lose the `"C901"` token. No public
signature, requirement, or observable behavior changes.

## ADDED Requirements

### Requirement: Six slice-4 targets report complexity <= 10 with ignores disabled

`reap_clip_preparer_dag.py` (0 violations), `speaker_turns.py::is_procedural_turn`,
`speaker_turns.py::extract_announcement`, `speaker_normalization.py::normalize_chapter_speakers`,
`speaker_resolution.py::_resolve_speaker_inner`, `candidate_intervals.py::derive_candidate_intervals`,
and `speaker_turn_prepare_dag.py::_prepare_turns_callable` MUST each score `<= 10`
under `uvx ruff check --select C901 --config 'lint.per-file-ignores = {}'`, and
`uvx ruff check .` MUST be clean at every stacked-PR tip.

#### Scenario: Neutralized ruff check passes after extraction
- GIVEN the five modified files after helper extraction
- WHEN `ruff check --select C901` runs with `per-file-ignores` neutralized
- THEN it reports zero C901 violations for all of them

#### Scenario: Full ruff check is green at every tip
- GIVEN any stacked-PR tip in this slice
- WHEN `uvx ruff check .` runs
- THEN it exits 0

### Requirement: Per-file-ignores entries prune in lockstep with the counter ladder

Each entry prune (or token drop) MUST land in the same commit as the matching
`EXPECTED_C901_FILE_COUNT` decrement, following the ladder 13 -> 12 (`reap_clip_preparer_dag.py`
full prune) -> 11 (`speaker_turns.py` token drop) -> 10 (`speaker_normalization.py` full prune)
-> 9 (`speaker_resolution.py` full prune) -> 8 (`candidate_intervals.py` full prune)
-> 7 (`speaker_turn_prepare_dag.py` token drop), never skipping or reordering.

#### Scenario: Entry and counter change atomically
- GIVEN a file whose entry is pruned or token-dropped
- WHEN that commit lands
- THEN `pyproject.toml` and `EXPECTED_C901_FILE_COUNT` change together, never in separate commits

#### Scenario: Ladder holds at every commit tip
- GIVEN the six work units land in ladder order
- WHEN `EXPECTED_C901_FILE_COUNT` is read at each tip
- THEN it follows 13->12->11->10->9->8->7, never skipping ahead

### Requirement: Multi-offender entries drop C901 only when every offender is clean

The `speaker_turns.py` entry (`["C901","F841","SIM102","SIM108"]`) and the
`speaker_turn_prepare_dag.py` entry (`["C901","UP022"]`) MUST keep their
non-C901 codes untouched and MUST drop `"C901"` only after every C901 offender
in that file is clean; a hidden-regression check (`ruff check --select C901`
with ignores neutralized) MUST be re-run immediately before each drop.

#### Scenario: Token dropped only after both offenders are clean
- GIVEN `is_procedural_turn` and `extract_announcement` in `speaker_turns.py`
- WHEN both score `<= 10` with ignores neutralized
- THEN `"C901"` is dropped from that entry and `F841`/`SIM102`/`SIM108` remain

#### Scenario: Early drop is rejected
- GIVEN one offender in a multi-offender file is still `> 10`
- WHEN the hidden-regression check runs before a planned token drop
- THEN the drop does not proceed for that commit

### Requirement: Extraction is a byte-for-byte lift with unchanged public signatures

Each lifted helper body MUST be AST-identical (`ast.dump(include_attributes=False)`)
to its base statements after only the declared normalizations, and the six
refactored functions MUST keep their existing parameter names, order, and
return values for every input already covered by existing tests.

#### Scenario: AST equality holds under declared normalizations
- GIVEN a lifted block and its base-revision counterpart
- WHEN declared normalizations are applied to the base and both are dumped
- THEN the dumps are equal

#### Scenario: Callers are unaffected
- GIVEN the existing callers of the six refactored functions
- WHEN the extraction lands
- THEN no caller or its existing tests require any change

### Requirement: Pre-existing test assertions are never edited

A diff of test files touched by this slice MUST show additions only; no
existing assertion in a pre-existing test may be modified.

#### Scenario: Pre-existing suites stay green untouched
- GIVEN the suites covering the six target functions
- WHEN the extraction is applied
- THEN every pre-existing test passes without any assertion line changed

### Requirement: New helpers land RED-first; candidate_intervals gets pinning tests first

Every new helper MUST have a failing test written before the helper exists.
`derive_candidate_intervals` MUST additionally get direct unit tests, pinned
from literals already proven by its existing CLI-subprocess tests, committed
before its refactor lands.

#### Scenario: RED-first helper test
- GIVEN a new helper has not yet been extracted
- WHEN its test is written
- THEN the test fails against pre-extraction code, then passes after extraction

#### Scenario: Direct pinning tests precede the candidate_intervals refactor
- GIVEN `derive_candidate_intervals` had only subprocess-CLI test coverage
- WHEN its refactor commit is prepared
- THEN direct unit tests pinning current behavior already exist and pass against pre-refactor code

### Requirement: Full suite is green with coverage >= 80% at the final tip

`uv run pytest` MUST pass with coverage `>= 80%` at the slice's final tip;
focused runs during development MAY use `-o addopts=` to bypass the coverage
gate.

#### Scenario: Final tip meets the coverage gate
- GIVEN the final commit of this slice
- WHEN `uv run pytest` runs with default addopts
- THEN it passes and reports coverage `>= 80%`

### Requirement: Deferred functions and files stay untouched

`create_app` (both benchmark servers), `_generate_metadata`, `_default_model_loader`,
`trim_turn_silence_with_vad`, and the multi-violator files (`youtube_channel.py`,
`download.py`, `utils/youtube_downloader.py`) MUST NOT change in this slice, and
each deferral MUST be recorded in issue #272's slice report.

#### Scenario: Deferred backlog untouched
- GIVEN the listed deferred functions and files
- WHEN the slice's PR diffs are reviewed
- THEN none of them changed, and issue #272 stays open with 7 entries remaining

## Acceptance Criteria

- [ ] All six in-scope functions score `<= 10` via `ruff check --select C901` with ignores neutralized.
- [ ] `uvx ruff check .` is clean at every stacked-PR tip.
- [ ] `EXPECTED_C901_FILE_COUNT` reads 7 and `pyproject.toml` carries exactly 7 `"C901"` entries at the final tip.
- [ ] Every entry prune/token-drop lands in the same commit as its counter decrement, following the 13->12->11->10->9->8->7 ladder.
- [ ] `uv run pytest` is green with coverage `>= 80%` at the final tip.
- [ ] Zero pre-existing test assertions edited (diff shows additions only in touched test files).
- [ ] `derive_candidate_intervals` has direct unit tests committed before its refactor.
- [ ] Listed deferrals (`create_app` x2, `_generate_metadata`, `_default_model_loader`, `trim_turn_silence_with_vad`, three multi-violator files) are unchanged and recorded in the slice report.
