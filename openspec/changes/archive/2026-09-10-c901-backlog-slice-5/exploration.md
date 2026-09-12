# Exploration: C901 Backlog Slice 5 (issue #272)

> Mirrored in Engram under topic key `sdd/c901-backlog-slice-5/explore` (observation #2790).

## Current State

Worktree `airflow-dags-wt-272-s5` @ `origin/main 7e3e689`. Slices 1-4 shipped (counter 29 -> 7). Slice 4's
archived proposal (`openspec/changes/archive/2026-09-09-c901-backlog-slice-4/`) explicitly deferred the 3
multi-violator files to slice 5: `congress_videos/modules/youtube/youtube_channel.py` (4 offenders),
`congress_videos/modules/youtube/download.py` (3 offenders), `utils/youtube_downloader.py` (3 offenders) —
10 functions total, counter 7 -> 4 on completion.

### Authoritative measurement (run by the orchestrator, not derived)

    uvx ruff check --select C901 --no-cache \
      --config 'lint.per-file-ignores = {}' --output-format concise .

16 offenders across 7 files on `origin/main` 7e3e689. The 10 in scope for slice 5:

| file | function | Cx |
|---|---|---|
| youtube_channel.py | `extract_agenda_section` | 17 |
| youtube_channel.py | `extract_session_date` | 14 |
| youtube_channel.py | `filter_finished_streams` | 13 |
| youtube_channel.py | `get_video_details` | 11 |
| download.py | `_analyze_single_chunk` | 15 |
| download.py | `_dedup_overlapping_chapters` | 14 |
| download.py | `identify_interesting_chapters` | 12 |
| utils/youtube_downloader.py | `download_with_pytubefix` | 11 |
| utils/youtube_downloader.py | `download_youtube_subtitles` | 11 |
| utils/youtube_downloader.py | `download_youtube_video_for_upload` | 11 |

Out of scope (deferred, as slice 4 deferred them): `create_app` x2 and `_default_model_loader`
(benchmark servers), `trim_turn_silence_with_vad` (whole-body try/finally), `_generate_metadata` and
`build_shorts_metadata_context` (`reap_shorts_uploader_dag.py`).

Test baseline on `origin/main` 7e3e689: **5274 passed, 34 skipped**. `ruff check .` and
`ruff format --check .` both clean.

### Liveness of `utils/youtube_downloader.py` — confirmed live, not legacy

Verified by grep (per the issue #400 precedent where a seemingly-dead dependency was load-bearing):
imported at module scope by `congress_videos/modules/youtube/youtube_channel.py:18` and
`congress_videos/modules/youtube/download.py:13`; both re-exported via
`congress_videos/modules/youtube/__init__.py`'s lazy `__getattr__`; and
`congress_videos/youtube_channel_monitor_dag.py:35` does `from congress_videos.modules import youtube as
yt_channel` — a DagBag-parsed production DAG.

## Affected Areas

- `congress_videos/modules/youtube/youtube_channel.py` — per-file-ignores `["B007", "C901", "F841", "SIM102"]`.
- `congress_videos/modules/youtube/download.py` — per-file-ignores `["B905", "C901", "SIM103", "SIM108"]`.
- `utils/youtube_downloader.py` — per-file-ignores `["C901", "F841"]`.
- `pyproject.toml` — the 3 entries above lose the `"C901"` token only (other codes stay, per the
  multi-offender token-drop rule); `tests/test_ruff_config.py::EXPECTED_C901_FILE_COUNT` 7 -> 4.
- `tests/**` — new RED-first characterization tests for `extract_agenda_section`; new helper-level quirk
  tests for the other 9.

## Per-function anatomy

### youtube_channel.py

**1. `extract_agenda_section` (Cx17, lines 1093-1262)** — highest complexity in the slice. 2 early guards,
outer `for` over `session_date_info["videos"]`, an inner lookup-for-with-break to find the matching agenda
(1145-1148), a date-header regex-match loop (1198-1233) containing a `try/except ValueError`, an `if` match
test, and a nested `for` (1219-1223) to find `next_match` (the section boundary).

Liftable: the whole date-matching loop (1198-1233, including its inner `for` and `try/except`) as one unit ->
`_locate_target_section(agenda_text, date_matches, target_date_dt, spanish_months) -> str | None`; no
`return`/`break`/`continue` crosses the boundary. Also liftable: the matching-agenda loop (1145-1148) ->
`_find_agenda_for_video(agendas, video_id)`. Estimated helper Cx ~7-8 and ~2.

**Landmine (falsy-valid trap).** `if target_section:` (1235) treats an empty-string extracted section
(possible when `start_pos == end_pos`) the same as "not found". This is PRE-EXISTING behaviour: the lift
must preserve the exact truthy check, not "fix" it to `is not None`.

**Zero existing test coverage** — confirmed by grep across `tests/` (independently re-confirmed by the
orchestrator). Needs RED-first characterization tests before any lift, exactly as slice 4 did for
`derive_candidate_intervals`.

**2. `extract_session_date` (Cx14, lines 892-1090)** — shares a near-identical `spanish_months` dict and
`date_pattern` regex with `extract_agenda_section` (dedup opportunity: follow-up, NOT this slice — that is
redesign, not a lift). Guards, `for` over agendas, session-number regex, date-header regex, a date-parsing
loop (984-1010) with its own `try/except`, a sort+log loop, and a target-date-position loop (1037-1047) with
a `break`.

Liftable: `_parse_agenda_dates(date_matches, spanish_months, year_fallback) -> list[dict]` (984-1010, pure,
only `parsed_dates` escapes) and `_locate_target_date_offset(sorted_dates, target_date_obj) ->
tuple[int | None, dict | None, bool]` (1037-1047). Cx14 -> ~9.

**Landmine (falsy-valid trap, the other documented one).** `date_offset = i` at line 1043 is legitimately `0`
when the target date is the FIRST date in the agenda — the same value as the "not found" default at line
1037. The existing code disambiguates via the separate `found_target` boolean, never via `if date_offset:`.
The helper contract MUST keep `found_target` as the sole truth-source.

Covered by 8 direct tests in `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py`
(including `test_extracts_session_number_for_first_date:376`, which does exercise offset=0).

**3. `filter_finished_streams` (Cx13, lines 307-435)** — 3 guards, one batched Data-API call, then a
per-video `for`/`try`/`except` loop with 6 sequential drop-conditions each ending in `continue`, plus one
IO-bound `probe_live_status` call (yt-dlp, network).

Liftable: the whole per-video try body (372-419) -> `_evaluate_finished_stream_candidate(video, by_id,
guard_floor_minutes, cookies_file) -> dict | None`, using normalization (a) `continue -> return None` for
every drop path and `return video` for the keep path (the append moves to the call site). Cx13 -> ~6.

**Landmine (exception propagation).** `probe_live_status` is invoked inside this block, currently wrapped by
the OUTER `except Exception` (420). Moving it into a helper does not change propagation, but the AST-equality
proof must confirm the `try/except` still wraps the SAME statements — do NOT add a new `try/except` inside
the helper. Covered by 14 direct tests.

**4. `get_video_details` (Cx11, lines 438-549)** — 2 guards, one OUTER try wrapping the whole `for` loop (no
per-video try, unlike its sibling above: this function is all-or-nothing on API failure, not fail-closed per
candidate), per-video body with 2 `continue` guards, an ISO-8601 duration regex `if/else`, dict build,
conditional `published_at` add.

Liftable: the whole per-video for-body (476-538) -> `_enrich_one_video(youtube, video, min_hours_since_end)
-> dict | None` (2 continues -> `return None`). Cx11 -> ~6.

**Landmine (behavioural asymmetry vs. #3, HIGH attention).** A single video's `.execute()` failure here
aborts the ENTIRE function (caught only at function level, re-raised as `RuntimeError`). The lift MUST NOT
wrap the new helper in its own `try/except` — that would silently change "abort on first API error" into
"skip and continue". Covered by ~9 direct tests.

### download.py

**5. `_analyze_single_chunk` (Cx15, lines 1264-1408)** — already a module-level private helper from slice
#210; needs further internal decomposition. Outer `try/except` (2 handlers), summary-text building, a
**nested closure `_identify_window`** (1311-1323) capturing the local `summary_text` and passed by reference
into `map_reduce_identify_chapters(identify_fn=_identify_window)`, a threshold branch (map-reduce vs direct),
2 fallback branches, a per-chapter validation loop building `valid_chapters`, single-chapter detection, and 2
except handlers both delegating to `_build_fallback_chunk_entry`.

This is the one function in the slice with a genuine closure concern — but it is liftable, not excludable
like slice 4's `trim_turn_silence_with_vad`, because the closure and its ONLY call site move together as one
cohesive unit: `_identify_chapters_for_chunk(chunk_number, summary_chunk, srt_content, chunk_duration) ->
list[dict]` containing lines 1290-1340. The captured local stays entirely inside the new helper's scope — no
cross-boundary capture. Second liftable block: the validate-and-build loop (1355-1381) ->
`_build_valid_chapters(range_valid, min_chapter_duration, max_optimal_duration) -> list[dict]`. Cx15 -> ~7.

**Landmine (cross-helper data dependency).** `is_single_chapter = len(valid_chapters) == 1 and
len(interesting_chapters) == 1` (1381) reads BOTH helpers' outputs — `interesting_chapters` must stay live in
the outer scope after helper 1 returns; neither helper may consume it.

**Landmine (except-order).** `except json.JSONDecodeError` before `except Exception` (1403/1406) — preserve
order exactly. Covered by 5 direct tests (`TestAnalyzeSingleChunk`).

**6. `_dedup_overlapping_chapters` (Cx14, lines 1055-1147)** — early return, 2 tiny pure nested defs
(`_start_secs`/`_end_secs`, no captured mutable state), sort, a `keep` boolean-mask list, and a pairwise
O(n^2) double `for i / for j` loop with tie-break overlap logic and 2 `break`s (both scoped to the inner
loop, fully inside the lift candidate).

Liftable: the entire double loop (1096-1145) as one unit -> `_mark_overlapping_chapters(sorted_chapters,
keep) -> None`, mutating `keep` in place (mirrors slice 4's `_collect_pattern_spans` in-place precedent); the
two nested defs move alongside it or become parameters. Cx14 -> ~4-5.

**Landmine.** `overlap <= 0.0: break` (1110) — the `<=` (not `<`) boundary for "touching but not
overlapping" must be preserved exactly. Covered by 7 direct tests (`TestDedupOverlappingChapters`).

**7. `identify_interesting_chapters` (Cx12, lines 1411-1578)** — the orchestrator that already delegates the
>45min branch to `_analyze_single_chunk`. 2 guards, outer `for` over videos, inner lookup-for-with-break for
matching `chunked_srt_data` (1463-1466), guard, `try` wrapping an index build plus an inner per-chunk `for`
loop with a `continue` guard and an if/else-shaped branch (optimal-duration inline dict build vs. delegate),
except handler.

Liftable: `_find_srt_chunks_for_video(...)` (1463-1466); the whole per-chunk decision body (1483-1549) ->
`_process_one_chunk(summary_chunk, srt_chunk_index, min_chapter_duration, max_optimal_duration) -> dict`.
Cx12 -> ~8.

**Landmine (falsy-valid, but CORRECT as-is — do NOT "fix").** `srt_content = _find_srt_chunk(...); if not
srt_content:` (1490) treats an empty string as "not found", which matches `_find_srt_chunk`'s documented
contract (returns `""` on no match). Intentional. Covered extensively.

### utils/youtube_downloader.py

**8. `download_with_pytubefix` (Cx11, lines 95-275)** — the riskiest lift in the slice. ImportError guard,
result-dict init, one big outer `try/except` wrapping: a video-stream selection cascade with a
`.filter(lambda s: ...)` (line 156), 2 fallback ifs, a big `if video_stream:` block containing audio-stream
selection, an ffmpeg `subprocess.run` merge with SUCCESS-path file cleanup (224-225) and a DIFFERENT
FAILURE-path cleanup (241-243), and a mid-function `return result` (238); falling through (no explicit else)
to a progressive-stream fallback (245-266) with its own `return`.

Liftable but NOT a clean mechanical lift — both inner blocks contain early `return`s that must become helper
returns via the normalization catalogue (fall-through paths need an explicit `return None`, slice 4 rule (a)),
and the ffmpeg block must preserve BOTH cleanup paths exactly, plus the subprocess timeout. Candidate
helpers: `_select_video_stream(yt, min_resolution)`, `_download_adaptive_with_merge(...)` (highest care),
`_download_progressive_stream(...)`.

**Landmine (exception propagation).** `subprocess.run(...)` is NOT locally try/except-wrapped today; a
`TimeoutExpired` propagates to the function's own outer `except Exception` (271). A lifted helper must stay
bare so this path is unchanged. This function should anchor its own PR. Covered by 3 direct tests.

**9. `download_youtube_video_for_upload` (Cx11, lines 278-447)** — mkdir, a `guard_live_status` early-return
block (329-341), a `use_pytubefix_first` block (348-359) with an early `return result` and a `try/except`
around `_warn_if_not_h264`, format/cookies setup, an outer `try` (405) with a success branch (415-435, its
own try/except) and 2 excepts: `except yt_dlp.utils.DownloadError` (440) BEFORE `except Exception` (443).

Liftable: `_check_live_status_guard(youtube_url, cookies_file, guard_live_status) -> dict | None` (329-341);
`_try_pytubefix_download(...) -> dict | None` (348-359, needs the abort-sentinel-recheck normalization, slice
4 rule (e)); optionally `_finalize_ytdlp_success(file_path, info, youtube_url) -> dict` (415-435). Cx11 ->
~4-6.

**Landmine.** The `DownloadError`/`Exception` except ORDER must not be reordered or merged; if any statement
is extracted from inside the try, the try/except must keep wrapping exactly the same statements. Covered by
~15 direct tests.

**10. `download_youtube_subtitles` (Cx11, lines 830-983)** — cleanest of the three. Default-arg guard, mkdir,
outer `try` with an inner `with yt_dlp.YoutubeDL...` availability check, a per-language `for lang in
languages:` loop (885-933) with its own try/except (`continue` on per-language failure) and a `break` on
first success (contained inside the loop), a "no files downloaded" guard, then a separate `try/except`
(948-973) building the merged subtitle file.

Liftable: `_download_subtitle_files(video_id, output_dir, languages) -> list[dict]` (885-933) and
`_write_merged_subtitle_file(main_subtitle, output_dir, video_id) -> str` (948-973). Cx11 -> ~5.

**Landmine.** `is_auto = "auto" in srt_file.name.lower() or lang == "auto"` must be copied verbatim as one
`or`-expression, not split into two statements. Covered by 4+ direct tests.

## Existing test coverage summary

Nine of the ten functions have DIRECT test coverage today (imported and called by name, not only exercised
transitively). Only `extract_agenda_section` has ZERO coverage anywhere — independently re-confirmed by the
orchestrator with a per-function grep across `tests/`. Every other function needs extended RED-first quirk
tests for the NEW helpers, not new characterization tests for the outer function.

## Slicing proposal

Stacked PRs to `dev` (`stacked-to-main`), <= 400 changed lines each, then one release PR `dev -> main`.
Counter starts at 7; a file's `"C901"` token drops only after ALL its offenders are clean.

| PR | Work unit | File | Risk | Est. lines | Counter |
|---|---|---|---|---|---|
| 1 | `get_video_details` (1 helper) | youtube_channel.py | Low | ~150 | 7 |
| 2 | `filter_finished_streams` (1 helper) | youtube_channel.py | Low-Med | ~220 | 7 |
| 3 | `extract_session_date` (2 helpers) | youtube_channel.py | Medium | ~250 | 7 |
| 4 | `extract_agenda_section` — RED-first characterization tests (separate commit) + lift + prune + counter | youtube_channel.py | Med-High | ~350 | 7 -> 6 |
| 5 | `_dedup_overlapping_chapters` (1 helper) | download.py | Low-Med | ~200 | 6 |
| 6 | `identify_interesting_chapters` (2 helpers) | download.py | Medium | ~250 | 6 |
| 7 | `_analyze_single_chunk` (2 helpers) + prune + counter | download.py | Med-High | ~300 | 6 -> 5 |
| 8 | `download_youtube_subtitles` (2 helpers) | utils/youtube_downloader.py | Low-Med | ~220 | 5 |
| 9 | `download_with_pytubefix` (3 helpers) + `download_youtube_video_for_upload` (2-3 helpers) + prune + counter | utils/youtube_downloader.py | High | ~380-400 | 5 -> 4 |
| 10 | Release `dev -> main` | — | — | — | 4 |

PR4 and PR9 carry the budget risk. Pre-approved contingency: split PR4 into 4a (characterization tests only)
/ 4b (lift + prune + counter); split PR9 into 9a (`download_with_pytubefix` only, no token drop) / 9b
(`download_youtube_video_for_upload` + prune + counter). `sdd-tasks` may merge adjacent low-risk PRs if the
combined estimate stays safely under 400 — each row is anchored by exactly one C901-relevant deliverable so
rows can be recombined without losing the one-deliverable-per-PR discipline. Ordering follows slice 4:
cheapest/purest first within each file, riskiest last, so an early truncation still banks counter progress.

## Risks

- `extract_agenda_section` has zero existing coverage — highest-risk item; no lift without RED-first
  characterization tests first.
- `_analyze_single_chunk`'s nested closure must move as one atomic unit with its sole call site.
- `download_with_pytubefix`'s subprocess/ffmpeg merge has two different cleanup paths plus a mid-function
  early return — highest-effort lift in the slice.
- Two falsy-valid traps recur in `youtube_channel.py` (`target_section` empty-string, `date_offset == 0`);
  both are pre-existing CORRECT behaviour a careless "improvement" would silently break.
- Two except-order chains (`_analyze_single_chunk`, `download_youtube_video_for_upload`) must not be
  reordered or merged.
- `get_video_details` (all-or-nothing on API failure) vs `filter_finished_streams` (fail-closed per
  candidate) — a lift must not harmonize these different exception-propagation behaviours.
- `scripts/test-airflow-e2e.sh` applies (touches `congress_videos/**` and `utils/**`); run at the final tip,
  or fall back to `airflow dags list-import-errors` on the NAS if Docker is unavailable.

## Ready for Proposal

Yes. The method carries over unchanged from slices 1-4. Two items for `sdd-propose`/`sdd-design` to resolve
explicitly: `extract_agenda_section`'s characterization-test contract (which literals/fixtures to pin,
mirroring slice 4's design.md for `derive_candidate_intervals`), and the closure-preserving helper contract
for `_analyze_single_chunk`.
