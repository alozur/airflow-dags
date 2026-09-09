# Proposal: C901 Backlog Slice 4 (issue #272)

## Problem Statement

`pyproject.toml` still masks McCabe complexity in 13 `per-file-ignores` entries carrying `"C901"` (pinned by `EXPECTED_C901_FILE_COUNT = 13` in `tests/test_ruff_config.py`). While an entry is masked, that file accepts unbounded new complexity with no lint signal. Slices 1-3 (shipped, main `7003b11`) took the backlog 29 -> 13 with a repeatable method; the rest stays masked until each file's offenders are cleared.

## Intent

Apply the same method to 6 functions and drop the counter 13 -> 7, with zero behaviour change and zero API change.

## Scope

### In Scope

| Function | Cx | File | Entry effect |
|---|---|---|---|
| (free prune, 0 violations) | — | `congress_videos/reap_clip_preparer_dag.py` | entry removed |
| `is_procedural_turn` | 12 | `congress_videos/modules/speaker_turns.py` | token drop after both |
| `extract_announcement` | 12 | same file | (F841/SIM102/SIM108 stay) |
| `normalize_chapter_speakers` | 14 | `congress_videos/modules/speaker_normalization.py` | entry removed |
| `_resolve_speaker_inner` | 14 | `congress_videos/modules/speaker_resolution.py` | entry removed |
| `derive_candidate_intervals` | 12 | `benchmarks/pyannote_diarization/candidate_intervals.py` | entry removed (+ new direct pinning tests) |
| `_prepare_turns_callable` | 19 | `congress_videos/speaker_turn_prepare_dag.py` | token drop (UP022 stays) |

### Out of Scope

- `create_app` (both benchmark servers, 14) and `_generate_metadata` (16): decorator/closure restructuring, a design decision not a mechanical lift.
- `_default_model_loader` (14): liftable, but its entry cannot drop until `create_app` lands, so it does not move the counter.
- `trim_turn_silence_with_vad` (12): whole-body try/finally.
- Multi-violator files `youtube_channel.py`, `download.py`, `utils/youtube_downloader.py`: sized for slice 5.
- Any behaviour, signature, or test-assertion change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — behaviour-preserving refactor, no spec-level requirement changes.

## Approach

1. **Hidden-regression check first**: `uvx ruff check --select C901 --config 'lint.per-file-ignores = {}' --output-format concise .` per candidate file before touching any entry.
2. **Byte-for-byte lift**: move statement bodies verbatim into module-level private helpers; no re-expression, no reordering.
3. **AST-equality proof**: `ast.dump(include_attributes=False)` of the refactored callable's inlined equivalent compared against base.
4. **RED-first quirk tests**: one failing test per new helper before it exists.
5. **Zero edits to pre-existing assertions.** New tests only.
6. **Prune + counter decrement in the same commit** (`pyproject.toml` entry and `EXPECTED_C901_FILE_COUNT` never drift).
7. **Multi-offender token-drop rule** (Engram obs #2421): drop `"C901"` from an entry only after every offender in that file is clean; other codes in the entry stay.
8. **Ruff green at every stacked-PR tip.**

## Delivery

Six stacked PRs to `dev` (`stacked-to-main`), ≤400 changed lines each, then one release PR `dev -> main`.

| PR | Work unit | Est. | Counter |
|---|---|---|---|
| 1 | free prune + `is_procedural_turn` + `extract_announcement` | ~220 | 13 -> 11 |
| 2 | `normalize_chapter_speakers` (2 helpers) | ~250-300 | 11 -> 10 |
| 3 | `_resolve_speaker_inner` (2 helpers) | ~300-350 | 10 -> 9 |
| 4 | `derive_candidate_intervals` + pinning tests | ~120-150 | 9 -> 8 |
| 5 | `_prepare_turns_callable` AI-resolution helpers | ~200-250 | 8 |
| 6 | `_prepare_turns_callable` VAD/sidecar/decode helpers + token drop | ~200-250 | 8 -> 7 |

Focused runs use `-o addopts=` to bypass `--cov-fail-under=80`; the full suite and coverage gate run at the final tip.

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/modules/{speaker_turns,speaker_normalization,speaker_resolution}.py` | Modified | Private helpers extracted |
| `congress_videos/speaker_turn_prepare_dag.py` | Modified | Two helper sets extracted from the per-turn loop |
| `benchmarks/pyannote_diarization/candidate_intervals.py` | Modified | Two helpers extracted |
| `congress_videos/reap_clip_preparer_dag.py` | Modified | Ignore entry only |
| `pyproject.toml`, `tests/test_ruff_config.py` | Modified | Entries pruned, counter 13 -> 7 |
| `tests/**` | New | Quirk + pinning tests |

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Cursor / in-place `result` mutation semantics drift in `normalize_chapter_speakers` | Med | Pass the same objects; AST-equality proof; 10 pinning test classes |
| Guard-order drift in `_resolve_speaker_inner` (`group_start_seconds is not None`, `chapter_id or 0`) | Med | Copy guards verbatim; 11 direct tests |
| Early token drop un-masks a remaining offender, breaking ruff at a tip | Med | Hidden-regression check re-run immediately before each prune |
| `derive_candidate_intervals` has only 3 subprocess-CLI tests | Med | PR4 adds direct unit tests from CLI-proven literals before refactoring |
| `_prepare_turns_callable` regressions re-open #282/#321/#322/#342 | Med | Split across PR5/PR6; 56+ existing tests through the public entry point |
| Stacked-PR diff pollution | Low | Retarget/rebase until each child diff shows only its work unit |

## Rollback Plan

Each PR is a self-contained revert: `git revert` the slice commit restores both the helper extraction and its `pyproject.toml`/counter change atomically. Reverting a later PR never requires reverting an earlier one, since each entry prune is independent. If a masked offender surfaces post-merge, restore the `"C901"` token in that entry and bump `EXPECTED_C901_FILE_COUNT` in one commit.

## Dependencies

- Base `origin/main` `1aa5681`; issue #272 remains open after this slice (7 entries left).
- No external services, migrations, or environment changes.

## Success Criteria

- [ ] `uvx ruff check .` green at every stacked-PR tip.
- [ ] `EXPECTED_C901_FILE_COUNT` is 7 and `pyproject.toml` carries exactly 7 `"C901"` entries.
- [ ] All 6 in-scope functions report complexity ≤10 with per-file-ignores disabled.
- [ ] Full `uv run pytest` green with coverage ≥80% at the final tip.
- [ ] Zero pre-existing test assertions edited (diff shows additions only in existing test files).
- [ ] Every PR ≤400 changed lines with no `size:exception`.
