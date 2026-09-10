# Proposal: C901 Backlog Slice 5 (issue #272)

## Problem Statement

`pyproject.toml` still masks McCabe complexity in 7 `per-file-ignores` entries carrying `"C901"` (pinned by `EXPECTED_C901_FILE_COUNT = 7` in `tests/test_ruff_config.py`). The mechanism is the harm: while a `"C901"` token sits in an entry, that **whole file** accepts unbounded new complexity with no lint signal — not just today's offenders. Slices 1-4 (shipped, base `origin/main` `7e3e689`) took the backlog 29 -> 7 with a repeatable method and explicitly deferred the 3 multi-violator files, because a multi-violator file yields no counter movement until **every** offender in it is clean.

## Intent

Apply the same method to the 10 functions in those 3 files and drop the counter **7 -> 4**, with zero behaviour change, zero signature change, and zero API change.

## Scope

### In Scope

| Function | Cx | File | Entry effect |
|---|---|---|---|
| `get_video_details` | 11 | `congress_videos/modules/youtube/youtube_channel.py` | token drop after all 4 |
| `filter_finished_streams` | 13 | same file | (`B007`/`F841`/`SIM102` stay) |
| `extract_session_date` | 14 | same file | |
| `extract_agenda_section` | 17 | same file | |
| `_dedup_overlapping_chapters` | 14 | `congress_videos/modules/youtube/download.py` | token drop after all 3 |
| `identify_interesting_chapters` | 12 | same file | (`B905`/`SIM103`/`SIM108` stay) |
| `_analyze_single_chunk` | 15 | same file | |
| `download_youtube_subtitles` | 11 | `utils/youtube_downloader.py` | token drop after all 3 |
| `download_with_pytubefix` | 11 | same file | (`F841` stays) |
| `download_youtube_video_for_upload` | 11 | same file | |

### Out of Scope

- `create_app` (x2, benchmark servers): decorator/closure restructuring — a design decision, not a mechanical lift.
- `_default_model_loader`: liftable, but its entry cannot drop until `create_app` lands, so it moves no counter.
- `trim_turn_silence_with_vad`: whole-body `try/finally`.
- `_generate_metadata` and `build_shorts_metadata_context` (`reap_shorts_uploader_dag.py`): sized for slice 6.
- The `spanish_months` / `date_pattern` duplication between `extract_agenda_section` and `extract_session_date`: dedup is redesign, not a lift — recommended follow-up issue.
- Any behaviour, signature, API, or pre-existing test-assertion change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — behaviour-preserving refactor, no spec-level requirement changes.

## Approach

Carried unchanged from slices 1-4:

1. **Byte-for-byte lift**: move statement bodies verbatim into module-level private helpers; no re-expression, no reordering.
2. **AST-equality proof**: `ast.dump(include_attributes=False)` of the refactored callable's inlined equivalent compared against base; only slice 4's closed normalization catalogue applies — notably (a) loop-exit `Continue()` -> `Return(None)` and (e) abort-sentinel re-check.
3. **Zero edits to pre-existing test assertions.** New tests only — that is what proves behaviour preservation.
4. **Prune + counter decrement in the SAME commit** (`pyproject.toml` entry and `EXPECTED_C901_FILE_COUNT` never drift).
5. **Multi-offender token-drop rule**: drop `"C901"` from an entry only after EVERY offender in that file is clean; other codes in the entry stay.
6. **Hidden-regression check before each prune**: `uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise <file>`.
7. **`ruff check .` + `ruff format --check .` green at every stacked-PR tip.**

Two slice-5-specific additions:

8. **`extract_agenda_section` has zero test coverage.** RED-first characterization tests land as their **own commit before any lift** — the same treatment slice 4 gave `derive_candidate_intervals`. `sdd-design` must pin the exact literals/fixtures.
9. **`_analyze_single_chunk`'s nested closure `_identify_window` and its sole call site move as one atomic unit** into `_identify_chapters_for_chunk`. The captured local (`summary_text`) stays entirely inside the new helper's scope; no cross-boundary capture is permitted.

## Delivery

Stacked PRs to `dev` (`stacked-to-main`), <= 400 changed lines each, then one release PR `dev -> main`.

| PR | Work unit | File | Risk | Est. lines | Counter |
|---|---|---|---|---|---|
| 1 | `get_video_details` (1 helper) | youtube_channel.py | Low | ~150 | 7 |
| 2 | `filter_finished_streams` (1 helper) | youtube_channel.py | Low-Med | ~220 | 7 |
| 3 | `extract_session_date` (2 helpers) | youtube_channel.py | Medium | ~250 | 7 |
| 4 | `extract_agenda_section`: characterization tests (own commit) + lift + prune + counter | youtube_channel.py | Med-High | ~350 | 7 -> 6 |
| 5 | `_dedup_overlapping_chapters` (1 helper) | download.py | Low-Med | ~200 | 6 |
| 6 | `identify_interesting_chapters` (2 helpers) | download.py | Medium | ~250 | 6 |
| 7 | `_analyze_single_chunk` (2 helpers) + prune + counter | download.py | Med-High | ~300 | 6 -> 5 |
| 8 | `download_youtube_subtitles` (2 helpers) | utils/youtube_downloader.py | Low-Med | ~220 | 5 |
| 9 | `download_with_pytubefix` (3 helpers) + `download_youtube_video_for_upload` (2-3 helpers) + prune + counter | utils/youtube_downloader.py | High | ~380-400 | 5 -> 4 |
| 10 | Release `dev -> main` | — | — | — | 4 |

**Budget risk: PR4 and PR9.** Pre-approved contingencies: split PR4 into 4a (characterization tests only) / 4b (lift + prune + counter); split PR9 into 9a (`download_with_pytubefix` only, no token drop) / 9b (`download_youtube_video_for_upload` + prune + counter). Ordering follows slice 4 — cheapest/purest first within each file, riskiest last — so early truncation still banks counter progress. Focused runs use `-o addopts=` to bypass `--cov-fail-under=80`; the full suite and coverage gate run at the final tip.

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/modules/youtube/youtube_channel.py` | Modified | 4 functions decomposed into ~6 private helpers |
| `congress_videos/modules/youtube/download.py` | Modified | 3 functions decomposed into ~5 private helpers |
| `utils/youtube_downloader.py` | Modified | 3 functions decomposed into ~7 private helpers |
| `pyproject.toml`, `tests/test_ruff_config.py` | Modified | 3 `"C901"` tokens dropped, counter 7 -> 4 |
| `tests/**` | New | Characterization tests for `extract_agenda_section`; helper quirk tests for the other 9 |

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `extract_agenda_section` has zero coverage — a silent regression would ship unnoticed | High | RED-first characterization tests as a separate commit before any lift; no lift without them |
| **Falsy-valid trap 1**: `youtube_channel.py:1235` `if target_section:` treats an empty extracted section as "not found" — pre-existing CORRECT behaviour | Med | Preserve the truthy check verbatim; never "fix" to `is not None`; AST-equality proof |
| **Falsy-valid trap 2**: `youtube_channel.py:1043` `date_offset = i` is legitimately `0` for the first agenda date, identical to the "not found" default at `:1037`; `found_target` is the sole truth-source | Med | Helper contract keeps `found_target` as the only disambiguator; `test_extracts_session_number_for_first_date:376` exercises offset 0 |
| **Except-order chain 1**: `download.py:1403` `except json.JSONDecodeError` precedes `:1406` `except Exception` | Med | Never reorder or merge; the `try` must keep wrapping exactly the same statements |
| **Except-order chain 2**: `utils/youtube_downloader.py:440` `except yt_dlp.utils.DownloadError` precedes `:443` `except Exception` | Med | Same rule; abort-sentinel re-check (norm (e)) at the call site, not a new inner `try` |
| Exception-propagation asymmetry: `get_video_details` aborts the whole function on one API failure; `filter_finished_streams` fails closed per candidate | Med | No helper gets its own `try/except`; harmonizing them is out of scope |
| `download_with_pytubefix`: ffmpeg `subprocess.run` is unwrapped today (TimeoutExpired reaches the outer handler at `:271`), plus two different cleanup paths (`:224-225` success, `:241-243` failure) and a mid-function `return` at `:238` | High | Anchors its own PR (9a contingency); helper stays bare; both cleanup paths copied verbatim |
| `_analyze_single_chunk` cross-helper dependency: `is_single_chapter` at `:1381` reads both helpers' outputs; `interesting_chapters` must stay live in the outer scope | Med | Neither helper may consume it; call-site ordering fixed in design |
| `download.py:1110` `overlap <= 0.0: break` — the `<=` (not `<`) touching-boundary | Low | Copied verbatim; 7 direct `TestDedupOverlappingChapters` tests |
| `download.py:1490` `if not srt_content:` on `_find_srt_chunk`'s documented `""` return — intentional | Low | Do NOT "fix"; documented in design |
| Early token drop un-masks a remaining offender, breaking ruff at a tip | Med | Hidden-regression check re-run immediately before each prune |
| Stacked-PR diff pollution | Low | Retarget/rebase until each child diff shows only its work unit |

## Rollback Plan

Each PR is a self-contained revert: `git revert` of the slice commit restores both the helper extraction and its `pyproject.toml`/counter change atomically. Reverting a later PR never requires reverting an earlier one, since each entry prune is independent. If a masked offender surfaces post-merge, restore the `"C901"` token in that entry and bump `EXPECTED_C901_FILE_COUNT` in one commit.

## Dependencies

- Base `origin/main` `7e3e689`; test baseline 5274 passed / 34 skipped, ruff check + format clean.
- `utils/youtube_downloader.py` is live production code (imported by `youtube_channel.py:18` and `download.py:13`, re-exported through `congress_videos/modules/youtube/__init__.py`, reached by the DagBag-parsed `congress_videos/youtube_channel_monitor_dag.py:35`) — not legacy.
- `scripts/test-airflow-e2e.sh` applies (touches `congress_videos/**` and `utils/**`); run at the final tip, or fall back to `airflow dags list-import-errors` on the NAS if Docker is unavailable.
- Issue #272 remains open after this slice (4 entries left).

## Follow-ups

- New issue: dedup the `spanish_months` dict and `date_pattern` regex shared by `extract_agenda_section` and `extract_session_date` — redesign, deliberately excluded here.
- Slice 6: the remaining 6 deferred functions (`create_app` x2, `_default_model_loader`, `trim_turn_silence_with_vad`, `_generate_metadata`, `build_shorts_metadata_context`).

## Success Criteria

- [ ] `uv run ruff check .` and `uv run ruff format --check .` green at every stacked-PR tip.
- [ ] `EXPECTED_C901_FILE_COUNT` is 4 and `pyproject.toml` carries exactly 4 `"C901"` entries.
- [ ] All 10 in-scope functions report complexity <= 10 with per-file-ignores disabled.
- [ ] Full `uv run pytest` green (>= 5274 passed) with coverage >= 80% at the final tip.
- [ ] Zero pre-existing test assertions edited (diff shows additions only in existing test files).
- [ ] AST-equality proof recorded for every lift, using only the closed normalization catalogue.
- [ ] Every PR <= 400 changed lines with no `size:exception`.
