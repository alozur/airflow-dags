# Exploration: C901 backlog slice 4 (issue #272)

Materialized by the orchestrator from the Engram artifact `sdd/c901-backlog-slice-4/explore` (observation #2617, 2026-09-09). The explore phase ran read-only without a write tool.

## Current state

13 `per-file-ignores` entries carry `"C901"` in `pyproject.toml` (`EXPECTED_C901_FILE_COUNT = 13` in `tests/test_ruff_config.py`). Slices 1-3 (shipped, main `7003b11`) took the backlog 29 -> 13 using byte-for-byte statement-body lifts into module-level private helpers, `ast.dump(include_attributes=False)` equality proofs against base, RED-first quirk tests per helper, zero edits to pre-existing assertions, entry prune plus counter decrement in the same commit, and ruff-green at every stacked-PR tip.

Known trap (Engram obs #2421): a per-file-ignore entry with multiple C901 offenders must have all of them fixed before the `"C901"` token is dropped from that entry. Dropping it early un-masks the remaining offender and breaks ruff at that PR tip.

Measured backlog on origin/main `1aa5681` with ignores disabled: 21 functions in 12 files.

## Candidates read (source + direct tests)

1. `congress_videos/reap_clip_preparer_dag.py` — entry `["C901"]` only, zero C901 violations. Free prune, 0 functions to touch. Low risk; apply must re-verify live before pruning.
2. `congress_videos/modules/speaker_normalization.py::normalize_chapter_speakers` (14, only C901 entry) — 196-line orchestration (Step0 institutional-role loop, Step1 roster-resolution block+loop, Step3 conditional UPDATE). 10 test classes pin every step. Feasible 2-helper split: `_apply_institutional_role_corrections` (Step0 loop) + `_apply_roster_resolution_step` (Step1 block). Full entry prune. Medium risk (cursor + in-place `result` mutation must stay identical).
3. `congress_videos/modules/speaker_resolution.py::_resolve_speaker_inner` (14, only C901 entry) — ~200-line early-return-guard pipeline (SRT window -> prompt -> LLM call -> confidence validation). 11 direct tests. Feasible split: `_build_resolution_prompt_context` + `_call_and_validate_completion`. Full entry prune. Medium risk; preserve the existing `group_start_seconds is not None` / `chapter_id or 0` guards verbatim.
4. `congress_videos/modules/speaker_turns.py::is_procedural_turn` (12) + `::extract_announcement` (12) — entry `["C901","F841","SIM102","SIM108"]`; both offenders must be fixed before dropping the token (other codes stay). Both pure functions; ~30 dedicated tests for the first, a `TestExtractAnnouncement` class for the second. Low risk, best lift targets in the slice.
5. `congress_videos/speaker_turn_prepare_dag.py::_prepare_turns_callable` (19, highest in the backlog; entry `["C901","UP022"]`) — 222-line per-turn loop with two try/except blocks: (a) AI speaker-resolution attribution (~140 lines, nested wide-context re-pass + roster crosscheck + promotion; issues #282/#321/#322/#342) and (b) VAD trim + sidecar + ffmpeg decode + mark-prepared (~33 lines). Plain module function, not a `with DAG` closure, so it is pure-liftable. 56+ tests pin nearly every branch through the public entry point. Block (a) likely needs 2 sub-helpers. Recommend 2 PRs, sequenced last.
6. `benchmarks/pyannote_diarization/candidate_intervals.py::derive_candidate_intervals` (12, only C901 entry) — 51-line pure function (interval merge + gap detection). Only 3 subprocess-CLI tests exist. Promoted into scope with a prerequisite: add direct unit tests pinning current behaviour first, using literals already proven by the CLI tests. Feasible split: `_merge_active_intervals` + `_intervals_to_gaps`. Full entry prune. Low-medium risk.

## New deferrals this slice

- `benchmarks/pyannote_diarization/server.py::create_app` (14) and `benchmarks/yamnet_applause/server.py::create_app` (14) — complexity is McCabe-aggregated from nested `async def` route handlers registered via FastAPI decorators inside the factory. Extracting them changes closure-capture semantics (APIRouter/partial-application restructuring): a design decision, not a mechanical lift. Same class as the deferred `_generate_metadata`.
- `benchmarks/yamnet_applause/server.py::_default_model_loader` (14) — liftable (nested `_run` closure without decorator entanglement) but skipped: the file's entry cannot be dropped until `create_app` is fixed too, so it does not move the counter; natural pairing for a later slice.

## Still deferred, unchanged from slice 3

- `congress_videos/modules/vad_helpers.py::trim_turn_silence_with_vad` (12) — whole-body try/finally.
- `congress_videos/reap_shorts_uploader_dag.py::_generate_metadata` (16) — closure inside `with DAG`.

## Not started (multi-violator files sized for a future slice)

- `congress_videos/modules/youtube/youtube_channel.py` — 4 violators (17/14/13/11); entry also carries B007/F841/SIM102. Natural slice-5 anchor.
- `congress_videos/modules/youtube/download.py` — 3 violators (15/14/12); entry also carries B905/SIM103/SIM108.
- `utils/youtube_downloader.py` — 3 violators (all 11); entry also carries F841.

## Recommended slice-4 scope: 6 functions, 6 stacked PRs, counter 13 -> 7

| PR | Scope | Est. lines | Counter |
|----|-------|-----------|---------|
| PR1 | `reap_clip_preparer_dag.py` free prune + `speaker_turns.py::is_procedural_turn` + `::extract_announcement` (token dropped once both are clean; F841/SIM102/SIM108 remain) | ~220 | 13 -> 11 |
| PR2 | `speaker_normalization.py::normalize_chapter_speakers` (2 helpers), full prune | ~250-300 | 11 -> 10 |
| PR3 | `speaker_resolution.py::_resolve_speaker_inner` (2 helpers), full prune | ~300-350 | 10 -> 9 |
| PR4 | `candidate_intervals.py::derive_candidate_intervals` + new direct pinning tests, full prune | ~120-150 | 9 -> 8 |
| PR5 | `_prepare_turns_callable` helper-set 1 (AI-resolution block, likely 2 sub-helpers), no token drop | ~200-250 | 8 |
| PR6 | `_prepare_turns_callable` helper-set 2 (VAD/sidecar/decode/mark-prepared block) + token drop (UP022 remains) | ~200-250 | 8 -> 7 |

Each PR: stacked PR to `dev` (chain_strategy stacked-to-main), ruff green at its own tip, focused pytest with `-o addopts=` to bypass `--cov-fail-under=80` on partial selections, full suite plus coverage gate at the final tip.

## Hidden-regression check (mandatory first step of apply)

Run `uvx ruff check --select C901 --config 'lint.per-file-ignores = {}' --output-format concise .` fresh, per candidate file, before editing any per-file-ignores entry:

- Re-confirm `reap_clip_preparer_dag.py` is still at 0 violations.
- Before dropping `"C901"` from the `speaker_turns.py` entry, re-verify both offenders are clean.
- Before dropping `"C901"` from the `speaker_turn_prepare_dag.py` entry (PR6), re-verify `_prepare_turns_callable` is fully clean.

## Test-suite facts

`uv run pytest` addopts include `--cov-fail-under=80`; use `-o addopts=` for targeted runs. Baseline ~4270 tests / ~89.9% coverage.

## Product decisions needed

None. Behaviour-preserving refactor: statement-lift only, zero test-assertion edits, zero API changes. Proposal can proceed without a research lane.

## Ready for proposal

Yes.
