# Implementation Tasks: Speaker-Turn Detection Inside video_chapters

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | 700–950 (module ~300, DAG ~150, migration ~30, tests ~300+) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 = migration + pure module + module tests; PR 2 = Docker wrapper + DAG + DAG-load test + persistence upsert |
| Delivery strategy | auto-chain |
| Chain strategy | feature-branch-chain |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: feature-branch-chain
400-line budget risk: High

## Delivery Boundaries

- Use a dedicated clean worktree before any implementation mutation; the dirty primary workspace is never a mutation target.
- Deliver as a feature-branch chain: PR 1 targets the feature/tracker branch; PR 2 targets PR 1. Keep each PR below 400 changed lines.
- Do not modify `vad_helpers.py`, `srt_helpers.py`, `participants_db.py`, `youtube_upload_dag.py`, or any existing migration file. Import only.
- `confirmed_block_duration_seconds` MUST NOT appear as a threshold predicate anywhere in production code.

## Task 0 — Migration Number Verification (pre-work, blocking)

- [x] **VERIFY:** Before creating any file, list `congress_videos/sql/migrations/` on the `dev` branch (`git show dev:congress_videos/sql/migrations/ --name-only` or equivalent) to find the current highest migration number. The new file MUST be named `{highest+1}_create_speaker_turns.sql`. The spec states the number is **≥ 021**; confirm or adjust. Record the resolved number; all subsequent migration tasks use it. **Verification:** the filename matches highest+1 and is unique across both the working branch and `dev`. **Rollback:** no files created yet; no action needed. <!-- sdd-owner: implementation -->
<!-- VERIFIED: dev branch contains 020 and 021; highest+1 = 022. File named 022_create_speaker_turns.sql. -->

---

## PR 1 — Migration + Pure Module

### Phase 1: Migration DDL

- [x] **RED:** add `tests/congress_videos/test_speaker_turns_migration.py` with a test that parses the SQL file for the new migration and asserts: `CREATE TABLE IF NOT EXISTS speaker_turns` is present; `REFERENCES video_chapters(chapter_id)` FK is present; `UNIQUE (chapter_id, start_seconds)` constraint is present; `CHECK (confidence >= 0 AND confidence <= 1)` is present; `CHECK (source IN ('acoustic','text_confirmed','text_named'))` is present; both indexes (`idx_speaker_turns_chapter`, `idx_speaker_turns_name`) are present; `ON DELETE CASCADE` is present on the FK. **Verification:** `uv run pytest tests/congress_videos/test_speaker_turns_migration.py -o addopts=` fails (file does not exist). **Rollback:** remove the new test file. <!-- sdd-owner: implementation -->
<!-- NOTE: Test placed at tests/congress_videos/sql/test_migration_022.py (following existing convention). RED confirmed: 10 failed. -->
- [x] **GREEN:** create `congress_videos/sql/migrations/{resolved_number}_create_speaker_turns.sql` with the DDL from the design: `speaker_turns` table (`turn_id SERIAL PK`, `chapter_id INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE`, `start_seconds NUMERIC NOT NULL`, `end_seconds NUMERIC NOT NULL`, `speaker_label TEXT NOT NULL`, `resolved_name TEXT`, `confidence NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1)`, `source TEXT NOT NULL CHECK (source IN ('acoustic','text_confirmed','text_named'))`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, `UNIQUE (chapter_id, start_seconds)`), plus `idx_speaker_turns_chapter` and `idx_speaker_turns_name` indexes. **Verification:** `uv run pytest tests/congress_videos/test_speaker_turns_migration.py -o addopts=` passes green. **Rollback:** delete the migration file; revert the test. <!-- sdd-owner: implementation -->
<!-- GREEN: 10/10 passed. File: congress_videos/sql/migrations/022_create_speaker_turns.sql -->

### Phase 2: `Turn` Dataclass and Module Skeleton

- [x] **RED:** add `tests/congress_videos/modules/test_speaker_turns.py`; import `Turn` from `congress_videos.modules.speaker_turns` and assert it is a frozen dataclass with fields `start_seconds: float`, `end_seconds: float`, `speaker_label: str`, `resolved_name: str | None`, `confidence: float`, `source: str`. Also assert module-level constants `GAP_MERGE_SECONDS = 1.0`, `PINGPONG_B_MAX_SECONDS = 5.0`, `PINGPONG_RETURN_SECONDS = 10.0` exist. **Verification:** test fails (module does not exist). **Rollback:** remove the test file. <!-- sdd-owner: implementation -->
<!-- RED confirmed: 42 tests failed. Tests for all phases (2–7) written together in one file. -->
- [x] **GREEN:** create `congress_videos/modules/speaker_turns.py` with the `Turn` frozen dataclass, `DiarizeFn` type alias, and the three named constants. No logic yet. **Verification:** the dataclass/constants assertions pass green. **Rollback:** delete the module. <!-- sdd-owner: implementation -->
<!-- GREEN: Full module implemented; 42/42 passed. -->

### Phase 3: President-Announcement Extractor (pure)

Satisfies: Text Gate and Name Resolution requirement; scenarios "Announcement phrase confirms and names", "Announcement phrase is absent — fuzzy fallback fires", "Announcement phrase rejects a noisy acoustic change", "confirmed_block_duration_seconds is never consulted".

- [x] **RED:** in `tests/congress_videos/modules/test_speaker_turns.py` add `extract_announcement` tests covering: (a) block containing `"Tiene la palabra el señor García"` in the `[t-30, t+30]` window returns `("García", True)`; (b) block containing `"Tiene la palabra la señora Martínez"` returns `("Martínez", True)`; (c) block containing `"Tiene la palabra su señoría"` returns `(None, True)`; (d) block containing `"Gracias, señoría"` returns `(None, True)`; (e) no matching block in window returns `(None, False)`; (f) matching block outside the window is ignored; (g) accent-tolerant match (`señor` with varied accents); (h) closest preceding block (within 15–30 s before `t`) is preferred when multiple blocks match. All use SRT-block list fixtures — no file I/O. **Verification:** run focused test and observe failures. **Rollback:** remove the new test cases. <!-- sdd-owner: implementation -->
- [x] **GREEN:** implement `extract_announcement(srt_blocks: list[dict], t: float, window: float = 30.0) -> tuple[str | None, bool]` in `speaker_turns.py`. Compile three regex patterns once at module level (case-insensitive, accent-tolerant via `unicodedata.normalize`): `tiene la palabra (?:el señor|la señora)\s+(?P<name>[\wÁÉÍÓÚÑáéíóúñ.\- ]+?)(?:[.,]|$)`, `tiene la palabra su señoría`, `gracias,?\s+señoría`. Scan blocks intersecting `[t − window, t + window]`; prefer the closest preceding block within 15–30 s before `t` for name capture. Return `(raw_name_or_None, phrase_found)`. **Verification:** all `extract_announcement` tests pass. **Rollback:** revert only the extractor function. <!-- sdd-owner: implementation -->
- [x] **REFACTOR:** extract compiled regex patterns to module-level constants; ensure the window scanning and preference logic is a single cohesive function with no side effects. Re-run extractor tests. **Rollback:** revert only this refactor unit. <!-- sdd-owner: implementation -->
<!-- REFACTOR: Regex constants _RE_NAMED, _RE_SU_SENORIA, _RE_GRACIAS_SENORIA at module level. All 8 extractor tests pass. -->

### Phase 4: Postprocessing Pipeline (pure)

Satisfies: Postprocessing Pipeline requirement; scenarios "Sub-second gap merged", "Ping-pong A→B→A collapsed", "Adjacent turns with same resolved name merged".

- [x] **RED:** add postprocessing tests in `test_speaker_turns.py` for each step as standalone functions: (a) `_merge_gaps`: two consecutive same-label segments with gap `0.7 s` → merged into one; two with gap `1.1 s` → not merged; (b) `_collapse_pingpong`: `A(60 s)→B(4 s)→A(90 s)` where B duration `< PINGPONG_B_MAX_SECONDS` and return gap `< PINGPONG_RETURN_SECONDS` → collapsed to one A; `A(60 s)→B(6 s)→A(90 s)` (B exceeds max) → not collapsed; (c) `_merge_same_name`: two adjacent segments with different `speaker_label` but same non-null `resolved_name` "María Luisa García" → merged; two with `resolved_name=None` → not merged. Use plain dicts/`Turn` instances as fixtures. **Verification:** tests fail (functions not implemented). **Rollback:** remove the new test cases. <!-- sdd-owner: implementation -->
- [x] **GREEN:** implement `_merge_gaps(segments: list[dict]) -> list[dict]`, `_collapse_pingpong(segments: list[dict]) -> list[dict]`, and `_merge_same_name(turns: list[Turn]) -> list[Turn]` as pure functions in `speaker_turns.py`. Constants `GAP_MERGE_SECONDS`, `PINGPONG_B_MAX_SECONDS`, `PINGPONG_RETURN_SECONDS` are already defined. **Verification:** all three postprocessing tests pass green. **Rollback:** revert only these three functions. <!-- sdd-owner: implementation -->
<!-- GREEN: 12 postprocessing tests pass (4 per function). -->

### Phase 5: Text Gate Integration

Satisfies: Text Gate requirement; "confirmed_block_duration_seconds is never consulted"; source/confidence routing table from design.

- [x] **RED:** add text-gate tests in `test_speaker_turns.py` for `_apply_text_gate(segments, srt_blocks, name_resolver)`: (a) acoustic change with announcement phrase naming a resolvable participant → `source="text_named"`, `confidence=0.95`, `resolved_name` set; (b) phrase found but name not resolvable → `source="text_confirmed"`, `confidence=0.80`, `resolved_name=None`; (c) no phrase in window AND segments on both sides resolve to same speaker label → segment dropped (noise rejection); (d) no phrase in window but different labels → `source="acoustic"`, `confidence=0.50`; (e) `confirmed_block_duration_seconds` field present in segment dict but NEVER read as a threshold (assert it has no effect on outcome). Inject a fake `name_resolver` callable. **Verification:** tests fail (function not implemented). **Rollback:** remove these test cases. <!-- sdd-owner: implementation -->
- [x] **GREEN:** implement `_apply_text_gate(segments: list[dict], srt_blocks: list[dict], name_resolver: Callable) -> list[Turn]` in `speaker_turns.py`. For each segment call `extract_announcement`; route `source`/`confidence` per the design table; drop same-speaker noise (both sides same label + no phrase); build `Turn` instances. Never read `confirmed_block_duration_seconds` as a threshold. **Verification:** text-gate tests pass green. **Rollback:** revert only this function. <!-- sdd-owner: implementation -->
<!-- GREEN: 5 text-gate tests pass. confirmed_block_duration_seconds never consulted. -->

### Phase 6: `detect_turns` Orchestrator

Satisfies: Chapter-Bounded Turn Detection; Injectable Diarization Backend; all postprocessing scenarios end-to-end; graceful degradation (missing-video, missing-SRT).

- [x] **RED:** add `detect_turns` integration tests in `test_speaker_turns.py`: (a) stub `diarize_fn` returning fixed change dicts → pipeline completes, returns `Turn[]` without launching Docker; (b) missing-video: `diarize_fn` raises/returns empty when `wav_path=None` or `chapter` has no source video → returns `[]`, no exception; (c) missing-SRT: `srt_blocks=[]` → acoustic-only turns with `source="acoustic"` and `confidence <= 0.50`, `resolved_name` nullable; (d) empty `diarize_fn` output (no changes) → returns `[]`; (e) end-to-end: gap-merge + ping-pong + text-gate + same-name all applied in order. All with stubbed `diarize_fn` and `name_resolver`. **Verification:** tests fail (function not implemented). **Rollback:** remove these test cases. <!-- sdd-owner: implementation -->
- [x] **GREEN:** implement `detect_turns(chapter: dict, srt_blocks: list[dict], diarize_fn: DiarizeFn, name_resolver: Callable = lookup_participant_fuzzy) -> list[Turn]` in `speaker_turns.py`. Orchestrate: call `diarize_fn(wav_path, chapter_offset_seconds)` → acoustic changes; convert to candidate segments; run `_merge_gaps` → `_collapse_pingpong` → `_apply_text_gate` → `_merge_same_name`; return `Turn[]`. Never touches Airflow context, DB, or Docker. Import `lookup_participant_fuzzy` from `participants_db` for the default `name_resolver`; import `find_srt_for_chapter`, `_parse_srt_blocks`, `extract_audio_wav`, `_find_source_video` for use by the DAG layer (not inside this function). **Verification:** all `detect_turns` tests pass green. **Rollback:** revert only this function. <!-- sdd-owner: implementation -->
- [x] **REFACTOR:** confirm no mutation of input dicts, all thresholds accessed through named constants, no `shell=True` in any call, and no import of pyannote/torch. Re-run full `test_speaker_turns.py`. **Rollback:** revert only this refactor unit. <!-- sdd-owner: implementation -->
<!-- REFACTOR: Verified — no shell=True, no pyannote/torch, no dict mutation (uses dict() / {**seg,...}), constants used throughout. 42/42 pass. -->

### Phase 7: Persistence Upsert (pure helper)

Satisfies: Idempotent Persistence requirement; scenarios "Idempotent re-run updates without duplicating rows", "source values are constrained to the declared enum".

- [x] **RED:** add `_upsert_turns` tests in `test_speaker_turns.py` using a fake DB cursor (a mock with `execute` and `fetchall`): (a) first call inserts `N` rows; (b) second call with same `chapter_id`/`start_seconds` → row count unchanged; (c) assert the SQL string passed to `cursor.execute` contains `ON CONFLICT (chapter_id, start_seconds) DO UPDATE SET`; (d) assert `updated_at = NOW()` or equivalent is in the update clause. No live DB required. **Verification:** tests fail. **Rollback:** remove test cases. <!-- sdd-owner: implementation -->
- [x] **GREEN:** implement `_upsert_turns(cursor, chapter_id: int, turns: list[Turn]) -> None` in `speaker_turns.py`. For each `Turn` execute `INSERT INTO speaker_turns (...) VALUES (...) ON CONFLICT (chapter_id, start_seconds) DO UPDATE SET end_seconds=EXCLUDED.end_seconds, speaker_label=EXCLUDED.speaker_label, resolved_name=EXCLUDED.resolved_name, confidence=EXCLUDED.confidence, source=EXCLUDED.source, updated_at=NOW()`. Never call `cursor.commit()` (the DAG controls transactions). **Verification:** upsert tests pass green. **Rollback:** revert only this function. <!-- sdd-owner: implementation -->
<!-- GREEN: 5 upsert tests pass. ON CONFLICT present, updated_at set, commit never called. -->

---

## PR 2 — Docker Wrapper + DAG + DAG-Load Test

### Phase 8: Diarization Docker Subprocess Wrapper

Satisfies: Injectable Diarization Backend requirement; scenarios "Production diarizer runs isolated", "Test stub replaces Docker diarizer"; threat matrix rows (shell injection, network egress, resource caps).

- [ ] **RED:** add `tests/congress_videos/modules/test_speaker_turns_docker.py` testing `docker_diarize_fn(wav_path, chapter_offset_seconds)` with `subprocess.run` mocked: (a) assert arg vector contains `docker`, `run`, `--rm`, `--network`, `none`, `--memory`, `4g`, `--cpuset-cpus`; (b) assert `--network none` is present; (c) assert `--memory 4g` is present; (d) assert `shell=True` is NOT passed to `subprocess.run`; (e) assert wav path is mounted read-only (`-v {wav_dir}:/in:ro`); (f) assert output dir is mounted writable (`-v {out_dir}:/out`); (g) assert non-int / path-traversal chapter_ids are rejected before any subprocess call; (h) subprocess timeout is adaptive (proportional to audio duration). Mock `subprocess.run` to return a fake `changes.json`. **Verification:** tests fail (function not implemented). **Rollback:** remove this test file. <!-- sdd-owner: implementation -->
- [ ] **GREEN:** implement `docker_diarize_fn(wav_path: str, chapter_offset_seconds: float) -> list[dict]` in `congress_videos/modules/speaker_turns.py` (or a dedicated `speaker_turns_docker.py` if cleaner). Build arg list for `subprocess.run` (no `shell=True`): `docker run --rm --network none --memory 4g --cpuset-cpus 0-1 -v {wav_dir}:/in:ro -v {out_dir}:/out pyannote/speaker-diarization-community-1 --input /in/{wav_name} --output /out/changes.json`. Derive adaptive timeout (`base + factor * audio_duration`). Parse `/out/changes.json` into the wire format; never import pyannote or torch. **Verification:** subprocess-mock tests pass green. **Rollback:** revert only this function. <!-- sdd-owner: implementation -->
- [ ] **REFACTOR:** confirm arg vector is built from module-derived path constants (not user text), timeout is bounded, and the output temp directory is cleaned up after parsing. Re-run docker tests. **Rollback:** revert only this refactor unit. <!-- sdd-owner: implementation -->

### Phase 9: Thin DAG `speaker_turns_dag.py`

Satisfies: On-Demand Standalone DAG requirement; scenarios "DAG loads without import errors", "DAG run processes at most LIMIT chapters", "DAG is not chained into youtube_upload_dag"; graceful degradation per-chapter.

- [ ] **RED:** add `tests/congress_videos/test_speaker_turns_dag.py` — DAG-load smoke test: import `speaker_turns_dag` and assert `speaker_turns_dag` is registered in the DagBag with no import errors; assert `schedule_interval` is `None`; assert `youtube_upload_dag.py` contains no reference to `speaker_turns_dag` or `speaker_turns` as a task dependency. **Verification:** tests fail (DAG file does not exist). **Rollback:** remove this test file. <!-- sdd-owner: implementation -->
- [ ] **GREEN:** create `congress_videos/speaker_turns_dag.py` as a thin on-demand DAG following the `generic_thumbnail_generator_dag` pattern. `schedule=None`. Conf keys: `limit` (default 10), optional `chapter_ids` list. Task graph: `select_chapters` (xcom_task, query `uploadable_chapters` view up to LIMIT or filter by `chapter_ids`) → `process_chapters` (per chapter: call `_find_source_video` + `extract_audio_wav` to produce a transient WAV scoped to the chapter window; call `find_srt_for_chapter` + `_parse_srt_blocks` + filter blocks to chapter window; call `detect_turns(chapter, srt_blocks, docker_diarize_fn)`; call `_upsert_turns`). Per-chapter exception handling: missing source video → log warning + skip (DAG succeeds); missing SRT → pass `srt_blocks=[]` for acoustic-only. Use `xcom_task` from `utils.airflow_helpers`. Never self-triggers; never references `youtube_upload_dag`. **Verification:** DAG-load smoke test passes green; `uv run pytest tests/congress_videos/test_speaker_turns_dag.py -o addopts=` is green. **Rollback:** delete `speaker_turns_dag.py` and revert the DAG-load test. <!-- sdd-owner: implementation -->
- [ ] **REFACTOR:** confirm the DAG contains no business logic (all logic in `speaker_turns.py`), no hard-coded paths, and no `youtube_upload_dag` references. Re-run DAG-load test and inspect `git diff -- congress_videos/youtube_upload_dag.py` is empty. **Rollback:** revert only this DAG refactor unit. <!-- sdd-owner: implementation -->

### Phase 10: Reuse Verification (no-change assertion)

Satisfies: Reuse of Existing Helpers Without Modification requirement; scenario "Helper functions are called unchanged".

- [ ] **VERIFY (no new code):** run `uv run pytest tests/congress_videos/modules/test_vad_helpers.py tests/congress_videos/modules/test_srt_helpers.py tests/congress_videos/ -k "participant" -o addopts=` and confirm all pre-existing tests pass without modification. Run `git diff -- congress_videos/modules/vad_helpers.py congress_videos/srt_helpers.py congress_videos/modules/participants_db.py` and confirm all three files are unmodified. Record pass/fail. **Rollback:** if any existing test broke, revert the offending import or call site in `speaker_turns.py`/`speaker_turns_dag.py`; no change to helper files is permitted. <!-- sdd-owner: implementation -->

---

## Final Verification and Handoff

- [ ] Run `uv run pytest` from the dedicated worktree; record the exact pass/fail count and distinguish any pre-existing environment/collection failure from change-caused failures. Confirm the test suite covers: all postprocessing steps (gap merge, ping-pong, same-name merge), text gate (named, confirmed, noise-rejected, acoustic fallback), `detect_turns` orchestrator (stub `diarize_fn`, missing-video, missing-SRT), persistence upsert (idempotency), Docker subprocess arg vector (security + resource caps), and the DAG-load smoke test. **Rollback:** revert PRs in reverse chain order (PR 2 → PR 1); fuzzy/upload behavior is unaffected; drop `speaker_turns` table if migration was applied. <!-- sdd-owner: implementation -->
- [ ] Run `bash scripts/test-airflow-e2e.sh` once (this change touches `congress_videos/**`); record pass/fail/unavailable. If Docker is unavailable on the CI host, record `unavailable` (not a failure) and note that it must be run manually before merge. **Rollback:** no additional action; e2e is read-only with respect to this change's rollback path. <!-- sdd-owner: implementation -->
- [ ] Confirm the final diff is limited to: `congress_videos/sql/migrations/{resolved_number}_create_speaker_turns.sql`, `congress_videos/modules/speaker_turns.py` (optionally `speaker_turns_docker.py` if extracted), `congress_videos/speaker_turns_dag.py`, `tests/congress_videos/test_speaker_turns_migration.py`, `tests/congress_videos/modules/test_speaker_turns.py`, `tests/congress_videos/modules/test_speaker_turns_docker.py`, `tests/congress_videos/test_speaker_turns_dag.py`. No existing DAG, migration, or module signature changed. `git diff -- congress_videos/youtube_upload_dag.py` is empty. <!-- sdd-owner: implementation -->

## Parent Lifecycle Actions

- [ ] Start or reuse bounded review for each frozen PR slice after its source-mutating work and tests are complete; review PR 1 before PR 2. <!-- sdd-owner: parent -->
- [ ] Confirm PR 1 targets the feature/tracker branch and PR 2 targets PR 1; confirm neither slice exceeds the 400-line budget; split test fixtures into a data-only child if needed. <!-- sdd-owner: parent -->
- [ ] After both PRs merge to `dev`, close GitHub issue #86 and confirm issues #17 and #88 reference the new `speaker_turns` table as an available input. <!-- sdd-owner: parent -->
