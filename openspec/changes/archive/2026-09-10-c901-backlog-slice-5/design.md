# Design: C901 Backlog Slice 5 (issue #272)

`next_recommended: sdd-tasks`

Base: worktree `/home/alozur/src/github.com/alozur/airflow-dags-wt-272-s5` @ `origin/main 7e3e689`.
Every line number below was re-read from source **in this phase** and supersedes the exploration where
they differ.

## Technical Approach

Unchanged from slices 1-4: lift statement blocks **byte-for-byte** into module-level private helpers **in
the same module**, placed **immediately above** the outer function, each carrying a
`Lifted verbatim out of <outer> (issue #272)` docstring. No new module, no signature change on any public
name, no behaviour change. Every lift is proven by `ast.dump(include_attributes=False)` equality after a
**closed set of declared normalizations**; every new helper gets RED-first quirk tests; entry prune +
`EXPECTED_C901_FILE_COUNT` decrement live in the **same commit** as the lift they pay for. Redesign of a
lift is out of bounds (slice-2 rev-1 precedent: a dispatch table replaced a lift and shipped a live
`KeyError`).

Helper naming follows what these three files already use: verb-first `snake_case` privates
(`_fetch_video_items_by_id`, `_select_airing_window_matches`, `_build_srt_chunk_index`, `_find_srt_chunk`,
`_validate_chapter_ranges`, `_warn_if_not_h264`).

### Complexity model (reused from slice 4, revalidated here)

Ruff's mccabe counts `if` (+1 per test), each `elif` (+1), `for`/`while` (+1), each `except` handler (+1),
nested `def`/`class` (+1), plus 1 for the function. Bare `else` adds **0**; ternaries, comprehension `if`s,
`and`/`or` and lambdas add **0**. Slice 4 validated it on 5 measured baselines; this phase re-derived **all
10 slice-5 baselines** (11, 13, 14, 17, 14, 12, 15, 11, 11, 11) and reproduced every one exactly. It is
still a **prediction**: `sdd-apply` MUST re-measure before/after with
`uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>` and treat the
measurement, not this table, as authoritative. Cx fires at **>= 11**.

## Per-lift contract

| # | PR | Outer: base Cx -> predicted | File | Lifted base lines | Helper(s) -> predicted Cx | Call-site replacement | Norm |
|---|---|---|---|---|---|---|---|
| 1 | 1 | `get_video_details` 11 -> **9** | `youtube_channel.py` | 481-505 | `_fetch_enrichable_video_details(youtube, video_id, min_hours_since_end) -> tuple[dict, dict] \| None` -> **4** | `details = _fetch_enrichable_video_details(youtube, video_id, min_hours_since_end)` / `if details is None: continue` / `video_details, live_details = details` | (a)x3, (c) |
| 2 | 2 | `filter_finished_streams` 13 -> **7** | same | 374-418 | `_evaluate_finished_stream_candidate(video, video_id, by_id, guard_floor_minutes, cookies_file) -> dict \| None` -> **8** | `candidate = _evaluate_finished_stream_candidate(...)` / `if candidate is not None: kept.append(candidate)` — both **inside the existing `try`** | (a)x6, (b), (g) |
| 3 | 3 | `extract_session_date` 14 -> **9** | same | 985-1010; 1037-1047 | `_parse_agenda_dates(date_matches, spanish_months, target_date_obj) -> list[dict]` -> **4**; `_locate_target_date_offset(sorted_dates, target_date_obj, target_date) -> tuple[int \| None, dict \| None, bool]` -> **3** | `parsed_dates = _parse_agenda_dates(...)`; `date_offset, target_date_info, found_target = _locate_target_date_offset(...)` | (c)x2 |
| 4 | 4 | `extract_agenda_section` 17 -> **8** | same | 1144-1148; 1195 + 1198-1233 | `_find_agenda_for_video(agendas, video_id) -> dict \| None` -> **3**; `_locate_target_section(agenda_text, date_matches, target_date_dt, spanish_months) -> str \| None` -> **8** | `agenda_item = _find_agenda_for_video(agendas, video_id)`; `target_section = _locate_target_section(...)` | (c)x2, (f) |
| 5 | 5 | `_dedup_overlapping_chapters` 14 -> **2** | `download.py` | 1079-1083; 1085-1089; 1096-1145 | `_chapter_start_secs(ch: dict) -> float` -> **2**; `_chapter_end_secs(ch: dict) -> float` -> **2**; `_mark_overlapping_chapters(sorted_chapters, keep) -> None` -> **9** | `sorted_chapters = sorted(chapters, key=_chapter_start_secs)`; `_mark_overlapping_chapters(sorted_chapters, keep)` | (d)x2 |
| 6 | 6 | `identify_interesting_chapters` 12 -> **6** | same | 1461-1466; 1477 + 1479 + 1482-1549 | `_find_srt_chunks_for_video(chunked_srt_data, video_id) -> list[dict]` -> **4**; `_collect_chunk_chapters(summarized_chunks, srt_chunks, min_chapter_duration, max_optimal_duration) -> list[dict]` -> **4** | `srt_chunks = _find_srt_chunks_for_video(chunked_srt_data, video_id)`; `chunks_with_chapters = _collect_chunk_chapters(...)` | (c)x2 |
| 7 | 7 | `_analyze_single_chunk` 15 -> **8** | same | 1291-1340 | `_identify_chapters_for_chunk(chunk_number, summary_chunk, srt_content, chunk_duration, system_prompt, user_prompt_template) -> list[dict]` -> **8** | `interesting_chapters = _identify_chapters_for_chunk(chunk_number, summary_chunk, srt_content, chunk_duration, CHAPTER_IDENTIFICATION_SYSTEM_PROMPT, CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE)` | (c), (d)x2 |
| 8 | 8 | `download_youtube_subtitles` 11 -> **7** | `utils/youtube_downloader.py` | 883-933 | `_download_subtitle_files(youtube_url, video_id, output_dir, languages) -> list[dict]` -> **5** | `downloaded_files = _download_subtitle_files(youtube_url, video_id, output_dir, languages)` | (c) |
| 9 | 9 | `download_with_pytubefix` 11 -> **8** | same | 140-146; 154-172 | `_log_available_streams(yt) -> None` -> **2**; `_select_video_stream(yt, min_resolution)` -> **3** | `_log_available_streams(yt)`; `video_stream = _select_video_stream(yt, min_resolution)` | (c) |
| 10 | 9 | `download_youtube_video_for_upload` 11 -> **8** | same | 329-341; 348-359 | `_check_live_status_guard(youtube_url, cookies_file, guard_live_status) -> dict \| None` -> **3**; `_try_pytubefix_download(youtube_url, output_dir, min_resolution, use_pytubefix_first) -> dict \| None` -> **4** | `skip_result = _check_live_status_guard(...)` / `if skip_result is not None: return skip_result`; `pytubefix_result = _try_pytubefix_download(...)` / `if pytubefix_result is not None: return pytubefix_result` | (e')x2, (g)x2 |

Every outer and every helper lands **<= 10**. Tightest: three at 9 (#1 outer, #3 outer, #5 helper
`_mark_overlapping_chapters`) — contingencies in "Open risks".

### Deviations from the exploration (deliberate, with reasons)

| Function | Exploration said | This design | Why |
|---|---|---|---|
| `get_video_details` | `_enrich_one_video` over the whole for-body 476-538 | narrowed to 481-505 | **Blocking defect in the wider boundary**: `hours`/`minutes`/`seconds` (513-515) are bound only inside `if duration_match:`, and 527 reads them unconditionally. In base they **leak across loop iterations** (a non-`PT…` duration after a matching one reuses the previous video's values; on the first iteration it raises `NameError` -> `RuntimeError`). A per-video helper gets a fresh scope, turning the stale-leak case into a raise. That is a behaviour change. The duration parse and dict build therefore **stay in the caller**. |
| `_analyze_single_chunk` | 2 helpers (+ `_build_valid_chapters`) | **1** helper | One lift already lands the outer at 8. A second helper would need `_flatten_speakers` (imported at 1285) passed as a parameter for no complexity gain. Helper count is a consequence of pure-lift plus <= 10, never a target (slice-4 rule). |
| `identify_interesting_chapters` | `_process_one_chunk` (per-chunk body) | `_collect_chunk_chapters` (**whole** per-chunk loop, 1477+1479+1482-1549) | The per-chunk body ends in `append(...)`+`continue` on three paths, needing three (a)+(b) rewrites and a sentinel protocol. Lifting the loop wholesale keeps both `continue`s verbatim and needs only (c). Pulling `_build_srt_chunk_index` (1479) inside makes the block contiguous, so no initializer relocation is needed either. |
| `download_youtube_subtitles` | 2 helpers | **1** (`_download_subtitle_files`) | 11 -> 7 with one lift; the 948-973 merge block never needs to move. |
| `download_with_pytubefix` | 3 helpers incl. `_download_adaptive_with_merge` | **2 tiny** helpers (140-146, 154-172) | 11 -> 8 without touching the `subprocess.run` merge, either cleanup path, or the mid-function `return`. **The whole 174-243 block stays untouched.** This converts the slice's highest-risk lift into its lowest and is the single most important decision in this design. |

## Normalization catalogue

Slice 4's closed catalogue is **reused by reference**
(`openspec/changes/archive/2026-09-09-c901-backlog-slice-4/design.md`, section "Normalization catalogue").
Recap plus the only extensions slice 5 needs. Anything outside this table is a redesign and stops apply.

| Id | Rule | Slice-5 use |
|---|---|---|
| (a) | loop-exit `Continue()` -> `Return(None)` | #1 (487, 497, 505); #2 (376, 383, 392, 396, 401, 411) |
| (b) | trailing `append(EXPR)` -> `Return(EXPR)` | #2 only (416 `kept.append(video)` -> `return video`; the `else` log branch is unchanged) |
| (c) | appended trailing `Return(<names>)` | #1, #3 x2, #4 x2, #6 x2, #7, #8, #9 |
| (d) | **Name-alias substitution** — substitute the helper-side `Name` back to the base `Name` it replaced before comparing. Slice 4 used it for parameter-for-global; **extended here to Name-for-Name generally**: a renamed module-level function or a parameter standing in for a module-scope constant. Still `Name` -> `Name`; no expression is ever rewritten. | #5 (`_start_secs`->`_chapter_start_secs`, `_end_secs`->`_chapter_end_secs`); #7 (`CHAPTER_IDENTIFICATION_SYSTEM_PROMPT`->`system_prompt`, `CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE`->`user_prompt_template`) |
| (e') | **sentinel re-check, forward form** — the call site gains exactly one `if <lhs> is not None: return <lhs>`. Slice-4 (e) was the abort form (`is None -> return None`); same admissibility test: the sentinel must be unproducible by any legitimate path of the lifted block. Not an AST rewrite — a declared *replacement statement*, checked by proof step 4. | #10 x2 (the guard returns a skip dict or `None`; `download_with_pytubefix` always returns a dict) |
| (f) | inert `= None` initializer relocation across statements that neither read nor write its target, asserted mechanically | #4 only (`target_section = None` at 1195 crosses 1196 `target_date_dt = ...`) |
| (g) | **NEW — terminal `Return(None)` appended to a fall-through path.** Inert: Python already returns `None` on fall-off. Declared so the added statement is auditable rather than silent. | #2, #10 x2 |

Rules (b) and (f) were declared-but-unused / single-use in slice 4; nothing else is new.

## AST-equality proof procedure

Scratch-only `ast_check_s5.py` under the session scratchpad, **never versioned** (slice-1..4 precedent),
run as `uv run python <scratch>/ast_check_s5.py` from the worktree.

1. Read the base file with `git show 7e3e689:<path>`; `ast.parse`; address each block by statement-index
   path (the slice-2/3/4 addressing already in use).
2. Apply declared normalizations **to the base slice only**, counting and printing each by name. (d) is
   applied to the **helper** side instead — the only rewrite that touches shipped code.
3. Strip docstrings from both sides, then compare
   `ast.dump(ast.Module(body=block, type_ignores=[]), include_attributes=False)`. The `def` line, the
   docstring and a (c)/(g)-appended `return` are scaffolding, excluded from the compared block.
4. Compare the **outer function** base vs shipped modulo declared replacements: the statements the lift
   removed must be replaced by exactly the call-site statements in the per-lift table and nothing else.
5. For (f), additionally assert the relocated target appears in neither the `Load` nor the `Store` name set
   of any crossed statement.
6. One line per block: `OK <name> (verbatim)` / `OK <name> (normalized: continue->return x6, append->return)`
   / `MISMATCH <name>` plus a unified dump diff; non-zero exit on any mismatch.
7. The captured `OK` lines are pasted verbatim into apply-progress and every PR body. **That output, not the
   script, is the audit artifact.**

Per-lift proof shapes worth naming explicitly:

- **#5** compares the two moved nested `def`s as whole `FunctionDef` nodes with (d) applied to their `name`
  field only, then the double loop with (d) applied to the two call `Name`s inside it. `overlap <= 0.0`
  (1110) must appear in the shipped dump as `Compare(ops=[LtE()])` — the proof script asserts this literally.
- **#7** compares the `FunctionDef _identify_window` node **inside** the helper against the base node,
  including its `Return` and its `Raise`; then asserts `map_reduce_identify_chapters` is still called with
  `keyword(arg='identify_fn', value=Name('_identify_window'))`.
- **#10** asserts the outer's `Try` node at base 405 still has handlers in order
  `[yt_dlp.utils.DownloadError, Exception]` and that its `body` is unchanged (nothing was extracted from
  inside it).
- **#2** asserts the outer's `Try` handler list is unchanged and that the `try` body is exactly the two
  call-site statements.

## Item 1 (resolved): `extract_agenda_section` characterization-test contract

Zero test coverage — re-confirmed a third time in this phase (`rg extract_agenda_section tests/` -> 0
matches; `tests/.../test_youtube_channel_extended.py` contains only `TestExtractSessionDate`). These land in
**their own commit, before any source change**, green against untouched source, in
`tests/congress_videos/modules/youtube/test_youtube_channel_extended.py`, class
`TestExtractAgendaSection` (adjacent to `TestExtractSessionDate`, same import-inside-the-test idiom).

**Fixture shape** — taken from the real producers, not invented. `agendas` is
`download_and_read_agenda`'s output (keys written at `youtube_channel.py:856-857`:
`video_id, video_title, agenda_url, agenda_file_path, agenda_text`); `session_date_info` is
`extract_session_date`'s output (keys built at 1074-1083).

```python
AGENDA_TEXT = (
    "Sesión nº135\n"
    "MIÉRCOLES, 21 DE MAYO\n"
    "Punto 1: Debate de totalidad\n"
    "JUEVES, 22 DE MAYO\n"
    "Punto 2: Votación\n"
)
AGENDAS = {"total_downloaded": 1, "videos": [{
    "video_id": "v1", "video_title": "Plenaria", "agenda_url": "http://x/a.pdf",
    "agenda_file_path": "/data/agenda.pdf", "agenda_text": AGENDA_TEXT}]}
SESSION_INFO = {"total_processed": 1, "videos": [{
    "video_id": "v1", "video_title": "Plenaria", "target_date": "2025-05-22",
    "session_number": 136, "base_session_number": 135, "date_offset": 1}]}
```
Headers carry no `DE <year>`, so the year falls back to `target_date_dt.year` (1202) = 2025.

| Test name | Pins |
|---|---|
| `test_extracts_section_between_target_header_and_next_header` | `target_date="2025-05-21"` -> `agenda_section == "MIÉRCOLES, 21 DE MAYO\nPunto 1: Debate de totalidad"`, `section_length == 45`(= `len(section)`, asserted as `len`), `full_agenda_file_path == "/data/agenda.pdf"`, `session_number` and `video_title` copied from `session_info`, `total_extracted == 1` |
| `test_last_date_section_runs_to_end_of_document` | `target_date="2025-05-22"` -> `agenda_section == "JUEVES, 22 DE MAYO\nPunto 2: Votación"` (no next match -> `end_pos = len(agenda_text)`), and the result is `.strip()`ped |
| `test_target_date_absent_returns_full_agenda_with_warning` | `target_date="2025-05-23"` -> `agenda_section == AGENDA_TEXT` (full, **unstripped**), `warning.startswith("Could not find section for 2025-05-23")`, and `"section_length" not in entry` and `"full_agenda_file_path" not in entry`. **This is the `if target_section:` else-branch at 1235** |
| `test_no_parseable_date_headers_returns_full_agenda` | `agenda_text = "Sesión nº135\nPunto 1: algo\n"` -> `warning == "Could not parse date headers, returning full agenda"`, `agenda_section == agenda_text` |
| `test_invalid_and_unknown_month_headers_do_not_abort_the_scan` | text `"LUNES, 31 DE FEBRERO\nx\nMARTES, 5 DE FOOBAR\ny\nJUEVES, 22 DE MAYO\nPunto 2\n"`, target `2025-05-22` -> still finds the 22-MAYO section. Pins `if not month_num: continue` (1206) and `except ValueError: continue` (1231) |
| `test_missing_agenda_for_video_id_yields_error_entry` | `session_date_info` names `v2`, `agendas` only has `v1` -> `{"video_id": "v2", "target_date": ..., "error": "No agenda found for this video"}`, `"agenda_section" not in entry` |
| `test_empty_agenda_text_yields_error_entry` | `agenda_text: ""` -> `error == "No agenda text available"` |
| `test_agenda_item_carrying_error_key_yields_error_entry` | non-empty `agenda_text` **plus** `"error": "boom"` -> same `"No agenda text available"` (the `or "error" in agenda_item` half of 1162) |
| `test_empty_inputs_return_zero_extracted` | `({}, SESSION_INFO)` and `(AGENDAS, None)` and `(AGENDAS, {"videos": []})` -> `{"total_extracted": 0, "videos": []}` (guards 1109 / 1113) |
| `test_first_videos_target_date_must_parse` | `session_date_info={"videos": [{"video_id": "v1", "target_date": "31/05/2025"}]}` -> `pytest.raises(ValueError)`. **Pins line 1118**: `target_date_obj` is never read afterwards (masked `F841`) but it is *load-bearing* — it validates the first video's `target_date` and indexes `["videos"][0]`. Deleting it as "dead code" is a behaviour change |

### The empty-string-section case at `:1235` — finding

`if target_section:` is claimed to make an empty extracted section read as "not found". Verified at source:
`end_pos` comes from `next_match.start() > start_pos` (1220) or `len(agenda_text)`, so `end_pos > start_pos`
always, and the slice always begins at the matched header itself — therefore `target_section` after
`.strip()` is **never** `""` for a `date_matches` produced by `re.finditer` over the same `agenda_text`.
**Through the public entry point the empty branch is unreachable today**, and `if target_section:` is
observationally equivalent to `is not None`. It becomes reachable the moment the lift creates
`_locate_target_section`, whose new interface accepts a caller-supplied `date_matches`/`agenda_text` pair
that need not correspond. Consequences, all binding:

1. The characterization suite pins the branch that an empty string *would* take
   (`test_target_date_absent_returns_full_agenda_with_warning`), which is the observable half.
2. The truthy check at 1235 is preserved **verbatim**. It is never "fixed" to `is not None` — post-lift that
   would be a real behaviour change, not a cleanup.
3. The empty-string case is pinned at the new seam by the helper quirk test
   `test_returns_empty_string_when_slice_is_whitespace_only` (below).

## Item 2 (resolved): `_analyze_single_chunk` closure-preserving contract

`_identify_window` (1311-1323) captures the local `summary_text` and is passed **by reference** into
`map_reduce_identify_chapters(identify_fn=_identify_window)` (1338); below the threshold it is called
directly (1340). The closure and its two call sites move as **one atomic unit**.

- **Lifted range**: 1291-1340 — the `summary_text` assembly (1291-1306), the whole `def _identify_window`
  (1311-1323), and the `if len(srt_content) > LARGE_SRT_THRESHOLD:` / `else` dispatch (1329-1340). Nothing
  else. The block is contiguous.
- **Captured state**: `summary_text` becomes a plain local of `_identify_chapters_for_chunk`, built by the
  same statements in the same order; `_identify_window` is defined in that new enclosing scope and captures
  it by the identical mechanism. No cross-boundary capture, no `nonlocal`, no partial/bound-method trick.
- **`chunk_number`, `summary_chunk`, `chunk_duration`, `srt_content`** are already parameters of the outer
  and become parameters of the helper with the same names.
- **The two prompt constants are passed as parameters** (`system_prompt`, `user_prompt_template`), norm (d).
  This is not cosmetic: `from congress_videos.config.ai_prompts import (...)` at 1281-1284 executes
  **before** the `try` at 1289, so an `ImportError` there propagates out of `_analyze_single_chunk`
  uncaught. Moving that import into the helper would put it *inside* the try's reach and silently convert
  that failure into a whole-chunk fallback entry. The import statement therefore **stays at 1281-1284**,
  still used (as the call arguments), so no `F401` appears.
- **`import json` (1279) stays** — `except json.JSONDecodeError` (1403) needs it.
  **`from utils.ai_chapter_analyzer import _flatten_speakers` (1285) stays** — its only use (1372) is not
  lifted.
- **`interesting_chapters` stays live in the outer scope**: the helper *returns* it and the call site binds
  it, so `is_single_chapter = len(valid_chapters) == 1 and len(interesting_chapters) == 1` (1381) reads the
  same object. The validate-and-build loop (1356-1378) is **not** lifted, so no helper consumes it.
- **The inner import at 1334-1336** (`map_reduce_identify_chapters`) moves inside the helper with its
  branch. In base it already sits inside the `try`; the helper is called from inside the same `try`, so its
  `ImportError` reachability is unchanged.
- **Except order** at 1403/1406 (`json.JSONDecodeError` then `Exception`) is untouched; nothing is removed
  from or added to the handler list, and the `try` body loses exactly the lifted statements and gains
  exactly one assignment.

## RED-first quirk tests

One class per helper, importing the private name **inside the test body**, written and observed RED before
the helper exists. A class that only re-tests the outer happy path does not satisfy this contract.

| Helper | Quirks pinned | Test file |
|---|---|---|
| `_fetch_enrichable_video_details` | empty `items` -> `None`; payload without `liveStreamingDetails` -> `None` (not `KeyError`); `actualEndTime` present but ended 11h59m ago with `min_hours_since_end=12` -> `None`, 12h01m -> the 2-tuple; `"…Z"` suffix parsed via `+00:00`; returns `(video_details, live_details)` where `live_details` is the same nested dict; **does not swallow exceptions** — a raising `.execute()` propagates (`pytest.raises`) | `tests/congress_videos/modules/youtube/test_youtube_channel.py` |
| *(characterization, same commit as #1)* `TestGetVideoDetailsDurationLeak` | `"PT1H2M3S"` then `"P0D"` in one call -> the second entry has `duration_seconds == 0` **and** `duration_formatted == "1:02:03"` (stale cross-iteration leak); a lone `"P0D"` -> `RuntimeError` whose message mentions `hours`. Both green on untouched source; they are the guard on the narrowed lift boundary | same |
| `_evaluate_finished_stream_candidate` | falsy `video_id` -> `None`; `by_id` miss -> `None`; `liveBroadcastContent` `"live"`/`"upcoming"` -> `None` but `"none"` passes; **`concurrentViewers: 0` -> `None`** (`is not None`, not truthiness); missing `actualEndTime` -> `None`; elapsed under the floor -> `None` **without calling the probe** (assert not called); probe `"was_live"` -> returns **the same object** (`result is video`); probe `"post_live"` or `None` -> `None`; a raising probe **propagates** (the caller's handler owns fail-closed) | same |
| `_parse_agenda_dates` | unknown month -> skipped, no entry; `31 DE FEBRERO` -> `ValueError` caught, skipped; **`original_index` counts accepted entries only**, so after a skip the indices stay contiguous and no longer match the position in `date_matches`; explicit `DE 2024` beats `target_date_obj.year`; the original `match` object is kept. Literals: text `"LUNES, 5 DE ENERO\nMARTES, 31 DE FEBRERO\nJUEVES, 7 DE OCTUBRE DE 2024\nVIERNES, 8 DE FOOBAR\n"`, `target_date_obj=datetime(2025, 10, 7)` -> exactly 2 entries: `date(2025, 1, 5)`/`original_index 0`, `date(2024, 10, 7)`/`original_index 1` | `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py` |
| `_locate_target_date_offset` | **the falsy-valid trap**: target is the first date -> `(0, entry, True)`; the test asserts `found_target is True` **and** `offset == 0` together; not found -> `(None, None, False)`; duplicate dates -> first index wins (`break`); comparison is `date_info["date"] == target_date_obj.date()` (a `date`, not a `datetime`) | same |
| `_find_agenda_for_video` | first match wins on duplicated `video_id`; `None` when absent; an agenda item **without** a `video_id` key raises `KeyError` (direct subscript at 1146 — pre-existing) | same |
| `_locate_target_section` | `None` when no header matches the target; boundary is the **smallest** start greater than this match's (not list order); target last -> section runs to EOF; result is `.strip()`ped; unknown month and invalid date skip without aborting; `test_returns_empty_string_when_slice_is_whitespace_only`: `date_matches` built over one text, `agenda_text` passed as `"          "` -> returns `""`, **which the caller's `if target_section:` then treats as not-found** | same |
| `_chapter_start_secs` / `_chapter_end_secs` | missing key -> default `"00:00:00"` -> `0.0`; unparseable `"abc"` -> `ValueError` swallowed -> `0.0` (never raises) | `tests/congress_videos/modules/youtube/test_download.py` |
| `_mark_overlapping_chapters` | mutates `keep` in place, returns `None`; **the `<=` boundary**: unsorted input `[a(00:00:00-00:10:00), b(00:10:00-00:20:00), c(00:05:00-00:07:00)]` -> `keep == [True, True, True]`, because `b` touches `a` with `overlap == 0.0` and `break`s the row before `c` is ever compared. With `<` instead of `<=`, `c` would be discarded — this is the only input class where the two operators differ, and it exists **only** at the new seam; `min_dur <= 0.0` -> `continue` (degenerate chapter skipped, not discarded); when the narrower is `i`, `keep[i] = False` **and** the row breaks | same |
| `_find_srt_chunks_for_video` | `None` / `{}` / missing `videos` -> `[]`; first matching video wins (`break`); a matched video without `chunks` -> `[]`; items lacking `video_id` do not raise (`.get`) | same |
| `_collect_chunk_chapters` | `_find_srt_chunk` returning `""` -> `{"chunk_number": n, "error": "No SRT content available"}` (**intentional falsy check** at 1490, matching `_find_srt_chunk`'s documented `""` contract — not an `is None` bug); `chunk_duration == max_optimal_duration` -> whole-chunk path with `reason == "optimal duration"` (`<=`, not `<`); below `min_chapter_duration` -> `reason == "too short"`, still whole-chunk; above max -> delegates to `_analyze_single_chunk` with positional args in the base order; title truncated at 100 chars; missing `duration_minutes` -> `0` -> too short | same |
| `_identify_chapters_for_chunk` | `completion["error"]` truthy -> `RuntimeError` **propagates out of the helper** (the outer handler owns the fallback); `completion["data"] is None` -> `[]`; under `LARGE_SRT_THRESHOLD` -> exactly one `cached_json_completion` call with the full `srt_content` and `model=LLM_CHEAP`; **over threshold -> capture the `identify_fn` kwarg, invoke it with a synthetic window and assert the resulting `user_prompt` contains the same `summary_text`** (`"Chunk 7 (00:00:00 - 01:00:00)"`, the speaker line, `"Topics: a, b"`, `"Summary: …"`) — this is the closure-capture proof; optional sections absent when their keys are missing/empty | same |
| `_download_subtitle_files` | per-language exception -> `continue`, next language attempted, no raise; `break` after the first language that yields files (later languages never attempted); `is_auto` pinned on **both** halves of `"auto" in srt_file.name.lower() or lang == "auto"` — `("v1_es-AUTO.srt", lang="es") -> True`, `("v1_zz.srt", lang="auto") -> True`, `("v1_es.srt", lang="es") -> False`; all languages failing -> `[]`; one entry **per file** in the glob, all sharing the same `language` | `tests/utils/test_youtube_downloader.py` |
| `_log_available_streams` | returns `None`; logs at most the **first 15** streams while the header reports the full count (20 in -> 1 header + 15 detail lines); a stream with `resolution=None` logs `"audio"` | same |
| `_select_video_stream` | prefers mp4 adaptive `>= min_resolution`; falls back to any mp4 adaptive; then to any adaptive; `None` when all three yield `None`; a stream with `resolution=None` is excluded by the `s.resolution and …` short-circuit instead of raising `TypeError` | same |
| `_check_live_status_guard` | `guard_live_status=False` -> `None` **without probing** (assert not called); probe `None` (error) -> `None`, non-blocking; `"was_live"`/`"not_live"` -> `None`; `"post_live"` -> the skip dict with `success False`, `skipped True` and the exact `f"live_status {status!r} not ready — skipped download"` (pin the `!r` quoting) | same |
| `_try_pytubefix_download` | `use_pytubefix_first=False` -> `None` **without calling** `download_with_pytubefix`; success -> returns that exact dict object; a raising `_warn_if_not_h264` is swallowed and the result is still returned; `success False` -> `None` (falls through to yt-dlp) | same |

## Landmine guards (one per lift, all enforceable by the proof script)

| Landmine | Guard |
|---|---|
| `youtube_channel.py:1235` `if target_section:` — `""` reads as not-found | Line is **outside** every lifted range; proof step 4 asserts the outer's `If(test=Name('target_section'))` node is byte-identical. Never rewritten to `is not None`. See the finding above |
| `youtube_channel.py:1043` `date_offset = i` is legitimately `0`; the default at 1037 is the same value | `_locate_target_date_offset` returns the 3-tuple with `found_target` as the **sole** disambiguator; the caller keeps `if not found_target:` verbatim; the helper never tests `if date_offset:`. Existing `test_extracts_session_number_for_first_date:376` plus the new offset-0 quirk test |
| `youtube_channel.py:1118` `target_date_obj` unused (masked `F841`) but load-bearing | Not touched, not deleted; pinned by `test_first_videos_target_date_must_parse` |
| `download.py:1110` `overlap <= 0.0` | Copied verbatim inside `_mark_overlapping_chapters`; proof asserts `LtE()` literally; the unsorted-input quirk test makes the operator observable |
| `download.py:1403/1406` except order | Nothing is removed from the outer `try` body except the lifted block; handler list asserted unchanged and in order |
| `download.py:1490` `if not srt_content:` on `""` | Moves verbatim inside `_collect_chunk_chapters`; documented as **intentional** (matches `_find_srt_chunk`'s `""` contract); quirk test pins `""` -> error entry. Do NOT "fix" |
| `utils/youtube_downloader.py:440/443` except order | The entire `try` at 405 and both handlers stay in the outer; **nothing is extracted from inside that try** |
| `get_video_details` all-or-nothing vs `filter_finished_streams` fail-closed | **No new helper contains a `try`/`except`.** Proof asserts zero `Try` nodes in `_fetch_enrichable_video_details` and in `_evaluate_finished_stream_candidate`; quirk tests assert propagation in both |
| `download_with_pytubefix` `subprocess.run` unwrapped, two cleanup paths (224-225 / 241-243), mid-function `return` at 238 | Lifts are confined to 140-146 and 154-172. **Lines 174-275 are not touched at all**; proof step 4 asserts the outer's statement list from 174 onwards is byte-identical to base |
| `download_youtube_subtitles:915` `is_auto = … or …` | One `or`-expression, moved verbatim, never split; both halves pinned |
| `_analyze_single_chunk` closure + `interesting_chapters` liveness | Contract above; proof asserts the `identify_fn=` keyword still binds `Name('_identify_window')` and that 1381 still reads both names |

## Delivery: 9 stacked PRs + 1 release PR

Strategy `auto-chain` / `stacked-to-main`: PR1 targets `dev`, each later PR targets the previous PR's
branch, then one release PR `dev -> main`. **Never merge a parent with `--delete-branch`** (orphans
children irrecoverably — #436->#437 precedent). One deliverable work unit per PR; helper(s) + call site +
quirk tests + `pyproject.toml` entry + counter decrement travel in the **same commit**.

Estimates below are derived from the line ranges in the per-lift table using
`changed = 2 x moved_lines + ~8 scaffolding + call site + tests` (a lift both deletes and re-adds its body).

| PR | Work unit | Moved lines | Est. changed (add+del) | `pyproject.toml` | Counter |
|---|---|---|---|---|---|
| 1 | `get_video_details` + duration-leak characterization tests | 25 | **~125** | none | 7 |
| 2 | `filter_finished_streams` | 45 | **~185** | none | 7 |
| 3 | `extract_session_date` (2 helpers) | 37 | **~200** | none | 7 |
| 4 | `extract_agenda_section`: characterization tests (commit 1) + 2 helpers (commit 2) + prune + counter | 42 | **~320 (risk)** | line 127 -> `["B007", "F841", "SIM102"]` | 7 -> 6 |
| 5 | `_dedup_overlapping_chapters` (2 moved defs + 1 helper) | 61 | **~240** | none | 6 |
| 6 | `identify_interesting_chapters` (2 helpers) | 79 | **~270** | none | 6 |
| 7 | `_analyze_single_chunk` (1 helper) + prune + counter | 50 | **~190** | line 126 -> `["B905", "SIM103", "SIM108"]` | 6 -> 5 |
| 8 | `download_youtube_subtitles` (1 helper) | 51 | **~185** | none | 5 |
| 9 | `download_with_pytubefix` (2 helpers) + `download_youtube_video_for_upload` (2 helpers) + prune + counter | 51 | **~275** | line 162 -> `["F841"]` | 5 -> 4 |
| 10 | Release PR `dev -> main` | — | — | — | 4 |

**PR4 is the only budget risk (~320 of 400, ~80 lines headroom)** — it carries both a full characterization
suite and the slice's largest lift. `sdd-apply` MUST run `git diff --shortstat` at that boundary before
opening it; the pre-approved contingency stands: **4a** = characterization tests only (no source change, no
prune, function still at 17 and still masked, counter unmoved — the body must say so explicitly) / **4b** =
the two helpers + prune + counter.

**PR9 is no longer a budget risk.** The exploration's ~380-400 estimate assumed
`_download_adaptive_with_merge`; this design does not lift it, so PR9 lands at ~275. The 9a/9b contingency
is retained but is not expected to fire; if it does, 9a = `download_with_pytubefix` only, **no token drop**,
9b = `download_youtube_video_for_upload` + prune + counter.

No `size:exception` is expected anywhere. `EXPECTED_C901_FILE_COUNT` ends at **4**, matching the 4 surviving
entries (both benchmark `server.py`, `vad_helpers.py`, `reap_shorts_uploader_dag.py`). All three edited code
lists stay sorted, as `test_every_code_list_is_sorted_and_deduped` requires.

## Verification design (exact sequence, in order)

Per lift, before writing any source:

1. `uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>` — record the
   measured base Cx of the target function; it must match the per-lift table.
2. Write the quirk tests, run them, **observe RED**:
   `uv run pytest <test_file>::<Class> -o addopts=` -> `ImportError`/`AttributeError`. Capture the output.

Per lift, after writing:

3. `uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>` — outer and
   helper both **<= 10**; record both numbers.
4. `uv run python <scratch>/ast_check_s5.py` — all `OK` lines, exit 0. Paste into the PR body.
5. `uv run pytest <touched test files> -o addopts=` — green.

Immediately before each prune (PR4, PR7, PR9), **after rebasing onto the actual PR base**:

6. `uvx ruff check --select C901 --no-cache --config 'lint.per-file-ignores = {}' --output-format concise <file>`
   -> **zero** offenders for that file. A `per-file-ignores` entry suppresses the whole file, so pruning it
   un-suppresses every offender in it, including any `dev` gained after this worktree left
   `origin/main 7e3e689`. This is the exact slice-3 defect.
7. Edit `pyproject.toml` and `EXPECTED_C901_FILE_COUNT` **in the same commit** as the lift.

Per PR tip:

8. In a disposable detached worktree at the tip sha (`git worktree add --detach <scratch>/tip-N <sha>`;
   remove afterwards): `uv run ruff check .` **and** `uv run ruff format --check .` — both green. A red tip
   is an undeliverable PR that propagates red to every descendant.
9. `git diff --shortstat <base>..<tip>` -> <= 400.
10. `git diff <base>..<tip> -- tests/ | rg '^-[^-]'` -> **empty** (zero deletions/modifications in
    pre-existing test files; additions only).

Final tip only:

11. `uv run pytest` with the coverage gate — **>= 5274 passed, 34 skipped**, coverage >= 80%.
12. `bash scripts/test-airflow-e2e.sh` (the slice touches `congress_videos/**` and `utils/**`). If it
    reports `unavailable`, a NAS `airflow dags list-import-errors` after triggering `git_sync_dag` is owed
    before merging to `main`.

## Rollback design

Per-PR `git revert` of the merge commit. Each PR is independently revertible because helper(s), call site,
quirk tests, the `pyproject.toml` entry and the counter move together in one commit, so a revert leaves
`TestC901BaselineCoverage` green and the file in a suppressed-but-working state.

**Three ordering constraints**, each the generalization of slice 4's PR5/PR6 pairing: a file's `"C901"`
token is dropped by the **last** PR touching that file, so reverting an earlier PR alone restores an
offender in a file that is no longer masked, turning `ruff check .` red.

- PR1, PR2, PR3 may only be reverted **together with, or after,** PR4 (`youtube_channel.py`).
- PR5, PR6 may only be reverted **together with, or after,** PR7 (`download.py`).
- PR8 may only be reverted **together with, or after,** PR9 (`utils/youtube_downloader.py`).

Every PR body must state its pairing. No migration, no schema, no deployed-state change. Full undo is
`revert to dev` plus a `git_sync_dag` trigger on **each** NAS scheduler (dev and prod are separate stacks),
then confirm `airflow dags list-import-errors` is empty. If a masked offender surfaces post-merge, restore
the `"C901"` token in that entry and bump `EXPECTED_C901_FILE_COUNT` in one commit.

## Tradeoffs

| Question | Options | Decision |
|---|---|---|
| `get_video_details` lift boundary | whole for-body (476-538), as explored | **481-505 only.** The wider boundary silently converts a cross-iteration variable leak into a `NameError`. Correctness of the lift outranks a tidier helper |
| `_dedup_overlapping_chapters` nested defs | pass `_start_secs`/`_end_secs` as parameters (outer -> 6, zero movement) | **Promote both to module level** (`_chapter_start_secs`/`_chapter_end_secs`). Gives `_mark_overlapping_chapters` a 2-parameter interface instead of 4 callable-carrying ones, makes both accessors directly testable, and takes the outer to 2. Cost: norm (d), which is mechanical |
| `_identify_chapters_for_chunk` prompt constants | move the `from …ai_prompts import` into the helper (4 params) | **Pass the two constants (6 params).** The import sits *before* the `try` in base; relocating it into the helper moves it *inside* the try's reach and converts a hard `ImportError` into a silent fallback. A wide but honest signature is the price of a pure lift (slice-4 precedent) |
| `_collect_chunk_chapters` boundary | per-chunk body (`_process_one_chunk`), as explored | **Whole loop, plus `_build_srt_chunk_index` inside.** Keeps both `continue`s verbatim, makes the block contiguous, and needs only (c) instead of three append+continue rewrites |
| `download_with_pytubefix` | lift the adaptive merge block for a lower outer Cx | **Do not touch 174-275.** Two ~10-line lifts already land it at 8. The subprocess, its unwrapped `TimeoutExpired`, both cleanup paths and the mid-function `return` never move. Highest-risk lift in the slice becomes the lowest |
| Helper count per function | match the exploration's forecast | **Arithmetic decides.** Four functions need fewer helpers than forecast. Helper count is a consequence of pure-lift plus <= 10, never a target |
| AST proof tooling | version a `scripts/` tool with its own tests | **Scratch-only.** One-shot verification evidence, not product code; versioning it spends ~90 lines of review budget and pulls throwaway tooling into the DagBag walk's tree |
| Lift in place vs. a new `_helpers` module | a new module keeps files shorter | **In place, immediately above the outer.** Two of the three files are reached by the DagBag walk; a new module under `congress_videos/` whose text contains "airflow" and "dag" breaks the scheduler in safe mode (recorded gotcha). It also breaks the reviewer's "the block did not change modules" assurance |
| PR ordering | riskiest first, to fail fast | **Cheapest and purest first within each file, riskiest last** (slice-4 rule). An early truncation still banks counter progress |

## Threat Matrix

**N/A — no routing, shell command, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary is introduced or modified.** `utils/youtube_downloader.py` does contain a
subprocess boundary (the ffmpeg merge at 202-220, `FFMPEG_MERGE_TIMEOUT_SECS`), and `download.py`/
`youtube_channel.py` reach network I/O (`yt_dlp`, the YouTube Data API). **This design places all of them
outside every lifted range** and makes that an enforced invariant (proof step 4 for lift #9 asserts lines
174-275 of `download_with_pytubefix` are byte-identical). No helper constructs a command, alters an
argument, changes a timeout, or adds an exception handler. The uncommitted proof script shells out only to
`git show` on a fixed revision under the operator's control.

## Migration / Rollout

No migration. After merge to `dev`, trigger `git_sync_dag` on **each** NAS scheduler and confirm
`airflow dags list-import-errors` is empty. Same after the release PR to `main`.

## Open risks

- **Predicted vs. measured complexity.** Every Cx above is hand-derived from a model that reproduced all 10
  slice-5 baselines exactly, but it is still a prediction. Three items have only 1 point of headroom, each
  with a pre-approved contingency — one additional in-place helper from the same function using the same
  catalogue, never a redesign, never an early token drop:
  - `get_video_details` at 9 -> lift 520-535 into
    `_build_enriched_video(video, video_details, live_details, video_id, duration_seconds, hours, minutes, seconds) -> dict`
    (norm (c)). Passing the leaked names as arguments preserves the leak: they are evaluated in the caller's
    frame at the call site, so a `NameError` still fires there.
  - `extract_session_date` at 9 -> lift 1032-1034 into `_log_agenda_dates(sorted_dates) -> None` (Cx 2, no
    normalization).
  - `_mark_overlapping_chapters` at 9 -> lift the inner `for j` loop (1099-1145) into
    `_scan_overlaps_from(sorted_chapters, keep, i) -> None` (Cx 6); both `break`s stay inner-loop `break`s,
    so the lift is contiguous with zero normalization.
- **`dev` drift.** New offenders merged to `dev` in any of the three files since `7e3e689` would make a
  prune red at its tip. Mitigated by re-running the hidden-regression check **after** rebasing onto
  `origin/dev`, not only on the local base.
- **`extract_agenda_section` zero coverage.** The characterization suite is the only safety net; PR4 must
  not be squashed into a single commit, or the reviewer loses the ability to check the suite out and see it
  green against untouched source.
- **`_analyze_single_chunk` closure.** Only 5 existing direct tests. The closure-capture quirk test
  (capturing `identify_fn` and invoking it) is mandatory, not optional — it is the only mechanical proof
  that `summary_text` is still captured.
- **PR4 headroom** (~80 lines) — measured contingency defined in Delivery.
- Issue #272 stays open after this slice (4 entries left, deferred to slice 6).
