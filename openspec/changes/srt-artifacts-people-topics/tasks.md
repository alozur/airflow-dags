# Tasks: SRT artifacts for shorts + mentioned-people and topic analysis

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | PR1 ~335, PR2 ~352, PR3 ~400 |
| 400-line budget risk | Medium |
| Chained PRs recommended | Yes |
| Suggested split | PR1 → PR2 → PR3 → release PR (dev → main) |
| Delivery strategy | auto-chain |
| Chain strategy | sequential PRs to `dev` (session override of design's stacked-branch suggestion — see Delivery) |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: Medium

If a PR overruns, cut in this order (never by deleting tests/docs/comments/blank lines):
- **PR1** (~335): 1) move `docs/ARCHITECTURE.md` NAS-layout edit to the release PR; 2) parametrize the three unsafe-`clip_id` cases into one test.
- **PR2** (~352): 1) move the `youtube_chapters_schema.sql` dev mirror to PR3; 2) parametrize the drop-reason tests (unknown slug / low confidence / non-numeric) into one table; 3) trim the module docstring rationale.
- **PR3** (~400, at budget — apply cut 1 up front): 1) move the `docs/PIPELINE.md` turn/upload-flow edit to the release PR; 2) parametrize the malformed-output cases; 3) keep `update_chapter_content_analysis` as one merged helper (already assumed).

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Short SRT sidecar beside every Reap clip, best-effort | PR 1 | `uv run pytest -n auto tests/congress_videos/test_srt_helpers.py tests/congress_videos/config/test_paths.py tests/congress_videos/test_reap_processor_dag.py` | `uv run python congress_videos/reap_processor_dag.py` (import check); `bash scripts/test-airflow-e2e.sh` (DAG touched) | Revert commit; inert `.srt` files left on disk are harmless |
| 2 | Migration 045 + roster-gated mentioned-people resolver | PR 2 | `uv run pytest -n auto tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_mentioned_people_resolution.py` | N/A — pure module + static SQL, no DAG touched | Revert commit; manual `ALTER TABLE video_chapters DROP COLUMN IF EXISTS mentioned_participant_slugs` + revert mirrors; no reads exist until PR3 |
| 3 | Topic extractor + upload-DAG hook, independent persistence | PR 3 | `uv run pytest -n auto tests/congress_videos/modules/test_topic_extraction.py tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/modules/test_database_chapters.py` | `uv run python congress_videos/youtube_upload_dag.py` (import check); `bash scripts/test-airflow-e2e.sh` (DAG touched) | Revert commit; both columns stay populated but unread |

Requirement legend (spec code → title), referenced as `[code]` on each task:
- `[R2]` Short clip SRT sidecar in the canonical shorts directory · `[R3]` Short SRT window derivation with fallback · `[R4]` Reuse of an existing non-empty sidecar · `[R5]` Explicit failure outcomes, never silent · `[R6]` Distinct canonical destinations per artifact type — *(short-video-srt-artifacts)*
- `[M1]` Dedicated LLM call over persisted chapter SRT · `[M2]` Roster-gated slug validity · `[M3]` Deduplicated slug array persisted per cardinality · `[M4]` Distinct from speaker resolution · `[M5]` Never raise on malformed output · `[M6]` Cacheable per content revision · `[M7]` Failure isolation from topic extraction · `[M8]` Schema migration and drift guard — *(chapter-mentioned-people)*
- `[T1]` Dedicated LLM call independent of mentioned-people · `[T2]` Normalized, deduplicated, capped output · `[T3]` Persisted per cardinality with stable ordering · `[T4]` Never raise on malformed output · `[T5]` Upload-time hook on shared preparation path · `[T6]` Cacheable per content revision · `[T7]` Documented column source-of-truth and metric distinction — *(chapter-topic-extraction)*

## Phase 1: PR1 — Shorts SRT sidecar (Closes #431)

- [x] 1.1 RED: `tests/congress_videos/config/test_paths.py` — assert `get_chapter_short_srt_path(...)` returns `{shorts_dir}/{clip_id}.srt`, sibling of the `.mp4` path. `[R2]`
- [x] 1.2 GREEN: `congress_videos/config/paths.py` — implement `get_chapter_short_srt_path`. `[R2]`
- [x] 1.3 RED: `tests/congress_videos/test_srt_helpers.py::TestWriteShortSrtSidecar::test_writes_srt_next_to_clip_mp4`. `[R2]`
- [x] 1.4 GREEN: `congress_videos/srt_helpers.py` — `write_short_srt_sidecar` happy path: `find_srt_for_chapter(canonical_dir=chapter dir)` → `_parse_srt_blocks` → write (D1). `[R2]`
- [x] 1.5 RED: `test_timestamps_are_retimed_to_clip_origin` — first block starts at `00:00:00,000`. `[R3]`
- [x] 1.6 GREEN: re-time via `_window_srt_blocks` to clip origin (D2). `[R3]`
- [x] 1.7 RED: `test_existing_non_empty_srt_is_reused_not_rewritten` (sentinel bytes unchanged + `caplog` INFO has the path). `[R4]`
- [x] 1.8 GREEN: reuse branch — existing non-empty `.srt` short-circuits, no rewrite. `[R4]`
- [x] 1.9 RED: `test_null_pretrim_offsets_fall_back_to_full_chapter_span` (parametrized: both `None`, start-only `None`, end-only `None`, non-numeric). `[R3]`
- [x] 1.10 GREEN: window `[chap_start+pretrim_start, chap_start+pretrim_end]`; **either** offset `NULL`/non-numeric → full chapter span (D3). `[R3]`
- [x] 1.11 RED: `test_window_outside_chapter_span_falls_back_with_warning` (F2 guard). `[R3]`
- [x] 1.12 GREEN: validity guard — inverted/disjoint window falls back to full chapter span + WARNING (D3). `[R3]`
- [x] 1.13 RED: `test_zero_blocks_writes_no_file_and_warns`. `[R5]`
- [x] 1.14 GREEN: zero-blocks branch — `None`, no file created or truncated, WARNING. `[R5]`
- [x] 1.15 RED: `test_missing_source_srt_returns_none_and_warns`. `[R5]`
- [x] 1.16 GREEN: no-source-SRT branch — `None`, WARNING. `[R5]`
- [x] 1.17 RED: `test_unreadable_source_srt_returns_none_and_writes_no_file` (`mocker.patch("builtins.open", side_effect=OSError)`). `[R5]`
- [x] 1.18 GREEN: confirm `_parse_srt_blocks` OSError path falls into the zero-blocks branch (existing helper — no new code beyond wiring; test locks the contract). `[R5]`
- [x] 1.19 RED: `test_unsafe_clip_id_refuses` (parametrized: `"../../etc/passwd"`, `"a/b"`, `""`). Threat matrix control.
- [x] 1.20 GREEN: `_SAFE_CLIP_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")` full-match gate inside `write_short_srt_sidecar` (D12). Threat matrix control.
- [x] 1.21 RED: `test_oserror_on_write_returns_none_and_leaves_no_tmp` (`mocker.patch("os.replace", side_effect=OSError)`). `[R5]`
- [x] 1.22 GREEN: `tmp_path.write_text` + `os.replace`; on `OSError` unlink tmp, return `None`, WARNING with `exc_info=True`. `[R5]`
- [x] 1.23 RED: `tests/congress_videos/modules/test_database_chapters.py` — `db.get_chapter_srt_context(chapter_id)` returns `{video_id, start_time, end_time, session_date}` or `None` when the chapter is missing.
- [x] 1.24 GREEN: `congress_videos/modules/database.py` — implement `get_chapter_srt_context` (LEFT JOIN `youtube_source_videos`, mirrors `get_chapter_metadata`).
- [x] 1.25 RED: `tests/congress_videos/test_reap_processor_dag.py::test_sensor_still_downloads_mp4_when_sidecar_raises` — patch `write_short_srt_sidecar` to raise; assert `poke(...) is True` **and** `insert_video_short_clip` was called. `[R2]`
- [x] 1.26 GREEN: `congress_videos/reap_processor_dag.py` — hook `write_short_srt_sidecar` into `ReapJobSensor.poke` in a `try/except` right after MP4 download + `insert_video_short_clip`, reading `ti.xcom_pull("claimed_clip")` for pre-trim offsets (D4) and `db.get_chapter_srt_context` for chapter bounds; `except Exception` → WARNING, continue to the next clip. `[R2]` `[R6]`
- [ ] 1.27 DOCS: `docs/ARCHITECTURE.md` NAS layout (lines 73-78) — add `.../shorts/{clip_id}.mp4 + {clip_id}.srt`, note the short SRT is a chapter/pre-trim-window approximation. *(cut candidate 1 if over budget)* — **CUT applied**: PR1's authored diff was ~709 changed lines even after this cut and the clip_id-parametrize cut, both applied per this table; deferred to the release PR per task 4.6.
- [x] 1.28 DOCS: `docs/PIPELINE.md` shorts pipeline (lines 88-94) — state the sensor writes `{clip_id}.srt` beside the MP4, best-effort, never failing the run.
- [x] 1.29 VERIFY: `uv run pytest -n auto tests/congress_videos/test_srt_helpers.py tests/congress_videos/config/test_paths.py tests/congress_videos/test_reap_processor_dag.py tests/congress_videos/modules/test_database_chapters.py`.
- [x] 1.30 VERIFY: `uv run ruff check` and `uv run ruff format --check` on all touched files (baseline drift gate — keep new code clean).
- [x] 1.31 VERIFY: `uv run python congress_videos/reap_processor_dag.py` (DAG import check).
- [x] 1.32 VERIFY: `uv run pytest -n auto` (full suite green) and `bash scripts/test-airflow-e2e.sh` (gated on `congress_videos/**`).

## Phase 2: PR2 — Migration 045 + mentioned-people resolver

**size:exception** — actual authored diff (excluding `openspec/`) is 6 files
changed, 594 insertions(+), 3 deletions(-) = 597 changed lines, against the
400-line budget and the ~352-line forecast. All three design-sanctioned cuts
were applied (youtube_chapters_schema.sql dev mirror deferred to PR3/task
2.4; the three drop-reason tests — unknown slug, low confidence, non-numeric
confidence — merged into one parametrized table instead of two test classes;
module docstring trimmed from 21 to 7 lines). No further reduction is
possible without deleting tests or docs, which is forbidden. The overrun is
driven by the test file (309 lines) providing full RED/GREEN coverage for
six independent gates (empty-input, roster, confidence, dedup, cap,
malformed-response, never-raises, prompt-content) on a resolver of
comparable complexity to `chapter_speaker_resolution.py` + its own 250-line
test file (405 lines combined) — this module's combined module+test size
(205 + 309 = 514) is in the same order of magnitude once the additional
dedup/cap gates (absent from the speaker resolver, which validates a
caller-supplied mention list rather than an open-ended LLM-found result) are
accounted for.

- [x] 2.1 CREATE: `congress_videos/sql/migrations/045_add_chapter_mentioned_people.sql` — `ALTER TABLE video_chapters ADD COLUMN IF NOT EXISTS mentioned_participant_slugs TEXT[]` + `COMMENT ON COLUMN` for both `mentioned_participant_slugs` and `topics`; DOWN block stays a **comment**, never live SQL (the runner executes the whole file in one transaction — an uncommented DOWN self-reverts its own UP). `[M8]` `[T7]`
- [x] 2.2 RED: `tests/congress_videos/sql/test_production_schema.py` — append `"mentioned_participant_slugs"` to `TABLE_COLUMNS["video_chapters"]` and bump both `133 columns` comments (lines 78, 484) to `134`; this turns the existing parametrized `test_column_present_in_block` RED. `[M8]`
- [x] 2.3 GREEN: `congress_videos/sql/production_schema.sql` mirror — trailing comma on `upload_verified_at TIMESTAMPTZ`, then `-- Added by migration 045 (mentioned people, issue #432)` + `mentioned_participant_slugs TEXT[]` with no trailing comma before `);`; comment line must contain no literal `);`. `[M8]`
- [x] 2.4 GREEN: `congress_videos/sql/youtube_chapters_schema.sql` dev mirror — same last-column comma discipline. `[M8]` — deferred from PR2 (over budget there) to PR3; completed in the PR3 batch.
- [x] 2.5 VERIFY: `uv run pytest -n auto tests/congress_videos/sql/test_production_schema.py`. No live-DB migration test exists in this repo (existing `test_migration_0NN.py` files are static SQL assertions only) — do not add one; live application is a rollout step (Phase 4), not a pytest task.
- [x] 2.6 RED: `tests/congress_videos/modules/test_mentioned_people_resolution.py::test_returns_empty_and_ok_false_on_empty_text_or_empty_roster` (no `completion_fn` call, asserted via a spy). `[M1]`
- [x] 2.7 GREEN: CREATE `congress_videos/modules/mentioned_people_resolution.py` — `MentionedPerson`/`MentionedPeopleResult` dataclasses, `resolve_mentioned_people` empty-input guard. `[M1]`
- [x] 2.8 RED: `test_zero_one_and_multiple_people_resolved` (parametrized). `[M3]`
- [x] 2.9 GREEN: happy-path parsing of `completion_fn` response, `ok=True`, `.slugs` property. `[M3]`
- [x] 2.10 RED: `test_slug_absent_from_roster_is_dropped_and_logged` (`caplog` INFO has the raw name). `[M2]`
- [x] 2.11 GREEN: roster gate — drop when `participant_slug` falsy or absent from `roster_by_slug`, INFO-log the raw name (D11). `[M2]`
- [x] 2.12 RED: `test_low_confidence_and_non_numeric_confidence_dropped` (parametrized). `[M2]`
- [x] 2.13 GREEN: confidence gate — drop when `float(confidence)` fails or `< MENTIONED_PEOPLE_MIN_CONFIDENCE (0.80)` (D11). `[M2]`
- [x] 2.14 RED: `test_duplicate_slugs_deduplicated_first_seen_order`. `[M3]`
- [x] 2.15 GREEN: dedup by slug, first-seen order. `[M3]`
- [x] 2.16 RED: `test_capped_at_max_mentioned_people`. `[M3]`
- [x] 2.17 GREEN: cap at `MAX_MENTIONED_PEOPLE = 12`. `[M3]`
- [x] 2.18 RED: `test_malformed_response_returns_ok_false` (parametrized: `{"error": ...}`, `{"data": None}`, missing key, non-list). `[M5]`
- [x] 2.19 GREEN: `error`/missing-`data` handling → `ok=False`, ensures no downstream clobbering write. `[M5]`
- [x] 2.20 RED: `test_never_raises_on_completion_fn_exception`. `[M5]`
- [x] 2.21 GREEN: wrap the `completion_fn` call in `try/except` → `ok=False`, never raise. `[M5]`
- [x] 2.22 RED: `test_prompt_states_speaker_is_not_a_mention` (asserts on the prompt constant text). `[M4]`
- [x] 2.23 GREEN: `congress_videos/config/ai_prompts.py` — `MENTIONED_PEOPLE_SYSTEM_PROMPT` (must carry verbatim: *"The person who is SPEAKING is not automatically a mentioned person. Include only people REFERRED TO in the transcript content."*) + `MENTIONED_PEOPLE_USER_TEMPLATE`; wire the lazily-imported default `completion_fn = utils.llm_cache.cached_json_completion`, model `LLM_CHEAP`, text truncated to `MENTIONED_PEOPLE_MAX_CHARS = 20_000`, roster block `slug | display_name | party`. `[M1]` `[M4]` `[M6]`
- [x] 2.24 VERIFY: `uv run pytest -n auto tests/congress_videos/modules/test_mentioned_people_resolution.py`.
- [x] 2.25 VERIFY: `uv run ruff check` and `uv run ruff format --check` on all touched files.
- [x] 2.26 VERIFY: `uv run pytest -n auto` (full suite green). No DAG file touched in PR2 — no DAG import check needed.

## Phase 3: PR3 — Topic extraction + upload-DAG hooks (Closes #432)

**size:exception** — actual authored diff (excluding `openspec/`) is 9 files
changed, 679 insertions(+), 2 deletions(-) = 681 changed lines, against the
400-line budget and the ~400-line forecast. All three design-sanctioned PR3
cuts were applied: (1) `docs/PIPELINE.md` turn/upload-flow edit (task 3.29)
deferred to the release PR up front, per the design's explicit instruction
that PR3 is "at budget — apply cut 1 up front"; (2) the malformed-output
tests are parametrized (one `pytest.mark.parametrize` table) rather than
four separate test methods, matching the resolver module's own pattern;
(3) `update_chapter_content_analysis` is the single merged helper the
design already assumed (D10), not two separate UPDATE methods. No further
reduction is possible without deleting tests, docs, or comments, which is
forbidden. The overrun is driven by the same shape of pressure as PR2: a
second LLM-backed module (`topic_extraction.py`, ~116 lines + its own
~133-line test file) plus the upload-hook wiring itself
(`_analyze_chapter_content`, ~90 lines) and its own dedicated coverage
across four new test classes in `test_youtube_upload_dag.py` (~206 lines)
— covering the F1 correction's extra `get_chapter_srt_context` read, the
skip-on-missing-context branch, and the independent-failure-isolation
matrix the design flagged as pushing PR3 from ~365 to ~400 lines before any
implementation began. Reported honestly as `size:exception` rather than
iterating further to force the number down.

- [x] 3.1 RED: `tests/congress_videos/modules/test_topic_extraction.py::test_topics_normalized_lowercase_trimmed_whitespace_collapsed`. `[T2]`
- [x] 3.2 GREEN: CREATE `congress_videos/modules/topic_extraction.py` — `TopicsResult` dataclass, `extract_topics` normalization: `strip()` → `lower()` → collapse internal whitespace. `[T2]`
- [x] 3.3 RED: `test_topics_deduplicated_preserving_first_seen_order`. `[T2]`
- [x] 3.4 GREEN: dedup on the normalised string, first-seen order. `[T2]`
- [x] 3.5 RED: `test_overlong_topic_dropped` (> 60 chars). `[T2]`
- [x] 3.6 GREEN: drop topics longer than `MAX_TOPIC_CHARS = 60`. `[T2]`
- [x] 3.7 RED: `test_capped_at_max_topics`. `[T2]`
- [x] 3.8 GREEN: truncate to `MAX_TOPICS = 8`. `[T2]`
- [x] 3.9 RED: `test_no_topics_returns_ok_true_empty`. `[T3]`
- [x] 3.10 GREEN: empty-topics-but-`ok=True` path — distinct from a failed extraction. `[T3]`
- [x] 3.11 RED: `test_malformed_output_returns_ok_false` (parametrized). `[T4]`
- [x] 3.12 GREEN: error/malformed handling; wrap `completion_fn` in `try/except` → `ok=False`, never raise. `[T4]`
- [x] 3.13 GREEN: `congress_videos/config/ai_prompts.py` — topics prompt pair, schema `{"topics": ["<short topic label>", ...]}`, instructed concise Spanish noun phrases, not sentences; wire the lazily-imported `cached_json_completion` default with a cache key distinct from `resolve_mentioned_people`. `[T1]` `[T6]`
- [x] 3.14 VERIFY: `uv run pytest -n auto tests/congress_videos/modules/test_topic_extraction.py`.
- [x] 3.15 RED: `tests/congress_videos/modules/test_database_chapters.py::test_update_uses_bound_parameters` (threat-matrix control — LLM-derived values always bound; only column names come from a fixed literal allow-list).
- [x] 3.16 GREEN: `congress_videos/modules/database.py` — `update_chapter_content_analysis(chapter_id, *, mentioned_slugs=None, topics=None)`: builds `SET` from non-`None` kwargs, one statement, one round trip, no-op (`False`) when both are `None` (D10).
- [x] 3.17 RED: `tests/congress_videos/test_youtube_upload_dag.py::test_analysis_uses_chapter_window_not_turn_window` — turn row built from the **real** `uploadable_turns` column list (no `start_time`/`end_time`); mock `db.get_chapter_srt_context` to return the chapter span; group span a strict subset; assert the analyses receive chapter-scoped text. `[T5]`
- [x] 3.18 GREEN: `congress_videos/youtube_upload_dag.py` — add `_analyze_chapter_content(chapter_id, blocks, db)`: `ctx = db.get_chapter_srt_context(chapter_id)` in `try/except`, `chapter_text` via `chapter_window_blocks(blocks, ctx["start_time"], ctx["end_time"])` (D6, D7, F1). `[T5]` `[M1]`
- [x] 3.19 RED: `test_missing_chapter_context_skips_both_analyses` (parametrized: `get_chapter_srt_context` returns `None` and raises); assert `update_chapter_content_analysis` never called and the upload continues. `[T5]` `[M1]`
- [x] 3.20 GREEN: skip-both-analyses branch on missing/failed context, WARNING, nothing written. `[T5]` `[M1]`
- [x] 3.21 RED: `test_one_analysis_failing_persists_the_other` (patch `resolve_mentioned_people` to raise; assert the UPDATE still carries `topics` and no `mentioned_participant_slugs`). `[M7]`
- [x] 3.22 GREEN: independent `try/except` around `resolve_mentioned_people` and `extract_topics`; a failed analysis contributes no column (D9). `[M7]`
- [x] 3.23 RED: `test_empty_topics_does_not_overwrite` (asserts `topics` absent from the UPDATE kwargs). `[T3]`
- [x] 3.24 GREEN: persist gate — write a column only when `ok=True`; `ok=True` + empty people → write `{}`; `ok=True` + empty topics → skip the write, log INFO (D9). `[M3]` `[T3]`
- [x] 3.25 RED: `test_db_failure_does_not_fail_the_upload` (`update_chapter_content_analysis` raising).
- [x] 3.26 GREEN: wrap the persistence call in `try/except` → WARNING, upload continues.
- [x] 3.27 GREEN: wire `_analyze_chapter_content(...)` into the shared path of `_prepare_thumbnail_config`, after `blocks` is parsed, outside the `is_turn` branch (D6). `[T5]`
- [x] 3.28 DOCS: `docs/ARCHITECTURE.md` `video_chapters` column list (lines 189-197) — add `mentioned_participant_slugs[]`, re-document `topics[]` as the upload-time source of truth. `[M4]` `[T7]`
- [ ] 3.29 DOCS: `docs/PIPELINE.md` turn/upload flow (lines 75-81) — state the upload prep derives mentioned people and topics from the chapter SRT window, independently persisted. *(cut candidate 1 — move to the release PR if PR3 is over budget; the design already flags PR3 as "at budget", apply this cut up front)* `[T5]` — **CUT applied**: deferred to the release PR (task 4.6) per the design's explicit up-front instruction.
- [x] 3.30 VERIFY: `uv run pytest -n auto tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/modules/test_database_chapters.py`.
- [x] 3.31 VERIFY: `uv run ruff check` and `uv run ruff format --check` on all touched files.
- [x] 3.32 VERIFY: `uv run python congress_videos/youtube_upload_dag.py` (DAG import check).
- [x] 3.33 VERIFY: `uv run pytest -n auto` (full suite green); `bash scripts/test-airflow-e2e.sh` reports **unavailable** — Docker daemon not running in this environment (`docker info` fails); run manually before merge per `CLAUDE.md`.

## Phase 4: Delivery

- [ ] 4.1 Commits: Conventional Commits (`feat`, `fix`, `docs`, `test`, `chore`), no AI attribution. One commit per work-unit RED+GREEN pair where practical; keep tests with the behavior they verify and docs with the user-visible change they explain (work-unit-commits skill).
- [ ] 4.2 PR1 body references `Closes #431`. PR2 body references `#432` without a closing keyword (its work is a dependency, not the full fix). PR3 body references `Closes #432`.
- [ ] 4.3 Chain strategy override: this session's delivery is **sequential PRs to `dev`** — PR1 opens first; PR2 opens only after PR1 merges to `dev`; PR3 opens only after PR2 merges to `dev`. This supersedes the design's stacked-branch suggestion (PR2 on PR1's branch, PR3 on PR2's branch), which was written before the session's `chain_strategy` was fixed. Each PR is a clean diff against `dev` at open time.
- [ ] 4.4 Migration rollout (PR2/PR3 boundary): apply migration 045 via the `run_migrations` DAG on `dev` first, confirm with `\d video_chapters` on the live schema (`information_schema` is blind to the `airflow` role — use `pg_attribute`), then pre-apply on `production` **before** merging PR3 to `main`, closing the code-without-migration window.
- [ ] 4.5 After PR1 merges: file a follow-up issue for F2 — `pretrim_start_secs`/`pretrim_end_secs` carry absolute source-video seconds when `pretrim_used_srt=TRUE` but chapter-relative seconds when `FALSE`, while ffmpeg always applies them chapter-relative, so the SRT-selected pre-trim window can land on the wrong content. Pre-existing defect, out of scope here; the D3 validity guard makes it visible instead of silently emitting an empty sidecar.
- [ ] 4.6 Release PR `dev` → `main` after PR3 merges, carrying any cut docs edits (1.27, 3.29 if deferred) and the migration pre-application confirmation.
