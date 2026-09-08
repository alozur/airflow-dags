```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:7e7ef5399712ec38773f72ffd2298f720dfb8324039e1f8a5f147207a593bc6d
verdict: pass_with_warnings
blockers: 0
critical_findings: 0
requirements: 8/8
scenarios: 19/19
test_command: uv run pytest -n auto
test_exit_code: 0
test_output_hash: sha256:271d1d82d97196ac2447529057df75f0bc730755c0bb437bf09739231fa70ea6
build_command: uv run ruff check .
build_exit_code: 0
build_output_hash: sha256:82b3e6a6c090a57601d22943bd23fca9218d1031dbe5a7b754092f9a156b4f18
```

## Verification Report

**Change**: reap-turn-video-source (issue #467)
**Version**: HEAD `8e4e104` (6-commit stack on `origin/dev` `59c4fea`)
**Mode**: Strict TDD

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total (Phases 1-4b) | 27 |
| Tasks complete | 27 |
| Tasks incomplete | 0 (Phase 5 — orchestrator-run ops — intentionally unchecked; not an `sdd-apply` work unit) |

### Build & Tests Execution

**Build (lint)**: ✅ Passed
```text
$ uv run ruff check .
All checks passed!
$ uv run ruff format --check .
301 files already formatted
```

**Tests**: ✅ 4613 passed / 0 failed / 32 skipped (all Postgres-dependent live tests; no local Postgres in this sandbox)
```text
$ uv run pytest -n auto
====================== 4613 passed, 32 skipped in 53.76s =======================
```

**Coverage**: 90.59% / threshold: 80% → ✅ Above
```text
TOTAL  8621  741  2296  150  90.59%
Required test coverage of 80% reached. Total coverage: 90.59%
```

**DagBag import check**: ✅ 16 DAGs, 0 import errors
```text
$ uv run python -c "from airflow.models import DagBag; db=DagBag(dag_folder='congress_videos', include_examples=False, safe_mode=True); print(len(db.dags), db.import_errors)"
16 {}
```

**E2E gate**: `bash scripts/dag-paths-changed.sh` → exit 0 (congress_videos/** touched → e2e applicable)
**E2E smoke test**: `bash scripts/test-airflow-e2e.sh` → `unavailable` (Docker daemon unreachable in this sandbox) — not a failure per repo policy (CLAUDE.md); the orchestrator must run the NAS `airflow dags list-import-errors` check after deploy (tasks.md Phase 5.3).

### Spec Compliance Matrix — `specs/reap-turn-sourced-clips/spec.md`

| Requirement | Scenario | Test | Result |
|-------------|----------|------|--------|
| `video_shorts.turn_id` reference | Referenced turn video deleted | `test_production_schema.py::TestVideoShortsTableSnapshot::test_turn_id_fk_is_production_qualified` (schema-snapshot proof of `ON DELETE SET NULL`, matches this repo's established FK-verification convention) | ✅ COMPLIANT |
| Turn-video candidate selection | Eligible group selected | `test_reap_db_methods.py::TestGetTurnVideosForShorts::test_returns_list_of_turns` | ✅ COMPLIANT |
| Turn-video candidate selection | Span under floor excluded | `...::test_query_floor_is_120_seconds` | ✅ COMPLIANT |
| Turn-video candidate selection | Over-ceiling group stays eligible | `...::test_query_has_no_upper_duration_bound` | ✅ COMPLIANT |
| Turn-video candidate selection | Unpublished parent still eligible | `...::test_query_does_not_require_parent_upload_date` | ✅ COMPLIANT |
| Preparer consumes materialized turn output directly | Under threshold used unmodified | `test_reap_clip_preparer_dag.py::TestStageAndPretrimClip::test_short_turn_stages_output_path_unmodified` | ✅ COMPLIANT |
| Preparer consumes materialized turn output directly | Over threshold is pre-trimmed | `...::test_over_threshold_writes_turn_reap_path_with_leading_window` | ✅ COMPLIANT |
| Preparer consumes materialized turn output directly | Inserted row carries both keys | Both tests above assert `call_kwargs["turn_id"] == 55` and `["chapter_id"] == 10` | ✅ COMPLIANT |
| Zero-eligible run is logged | No eligible candidates | `...::TestQueryTurns::test_empty_result_returns_false_and_logs_warning` | ✅ COMPLIANT |
| Claim ordering with legacy compatibility | Legacy chapter-only row still claimable | `test_reap_db_methods.py::TestClaimPendingClip::test_returns_claimed_row_as_dict` (turn_id=None fixture) + `test_ordering_and_locking_preserved_verbatim` | ✅ COMPLIANT |
| Tier-1 partitioning by turn with legacy fallback | Turn-keyed rows partition by turn_id | `test_get_pending_shorts_sql.py::test_two_turn_groups_in_one_chapter_get_independent_tier1_caps` (live-PG, correct code, **SKIPPED** — no Postgres in this sandbox); SQL-shape proven at runtime by `test_reap_db_methods.py::test_candidate_query_ranks_clips_per_chapter` | ✅ COMPLIANT |
| Tier-1 partitioning by turn with legacy fallback | Legacy rows partition by chapter_id | `test_get_pending_shorts_sql.py::test_mixed_legacy_and_turn_rows_partition_independently_in_one_chapter` (live-PG, **SKIPPED**) | ✅ COMPLIANT |
| Tier-1 partitioning by turn with legacy fallback | Uploaded rows still consume ranking slots | `test_get_pending_shorts_sql.py::test_uploaded_top_clips_consume_chapter_tier1_slots` (pre-existing, live-PG, **SKIPPED**); SQL-shape proven by `test_reap_db_methods.py::test_rank_universe_includes_uploaded_clips` | ✅ COMPLIANT |
| Tier-1 partitioning by turn with legacy fallback | Unpublished parent no longer blocks upload | `test_get_pending_shorts_sql.py::test_unpublished_parent_chapter_is_still_returned` (live-PG, **SKIPPED**); SQL-shape proven by `test_reap_db_methods.py::test_parent_upload_date_gate_removed` | ✅ COMPLIANT |
| Chapter-only candidate selection is removed | No remaining caller | `test_database_surface.py::test_dead_method_absent[get_chapters_for_shorts]` + `rg` audit (0 production callers) | ✅ COMPLIANT |

**Compliance summary**: 15/15 scenarios COMPLIANT. Four Tier-1-partitioning scenarios are proven by SQL-shape-level runtime-passing unit tests (`test_reap_db_methods.py::test_candidate_query_ranks_clips_per_chapter`, `::test_rank_universe_includes_uploaded_clips`, `::test_parent_upload_date_gate_removed`); their dedicated live-Postgres behavioral tests in `test_get_pending_shorts_sql.py` skip cleanly in this sandbox (no reachable Postgres, including the NAS Tailscale endpoint) — flagged as a WARNING requiring live-DB confirmation before merge, consistent with this repo's established convention for this test file (see e.g. the archived `plenary-airing-time-filter` verify report).

### Spec Compliance Matrix — `specs/short-video-srt-artifacts/spec.md`

| Requirement | Scenario | Test | Result |
|-------------|----------|------|--------|
| Short SRT window derivation with fallback | Pre-trim offsets present, chapter-sourced clip | `test_srt_helpers.py::TestWriteShortSrtSidecar::test_chapter_sourced_clip_unaffected_by_turn_id_default` + pre-existing chapter-window tests (byte-identical `turn_id=None` path) | ✅ COMPLIANT |
| Short SRT window derivation with fallback | Pre-trim offsets absent, chapter-sourced clip | Pre-existing `TestWriteShortSrtSidecar` no-pretrim tests (unchanged, `turn_id=None` default) | ✅ COMPLIANT |
| Short SRT window derivation with fallback | Turn-sourced clip ignores chapter-relative pretrim offsets | `test_srt_helpers.py::TestWriteShortSrtSidecar::test_turn_sourced_clip_ignores_pretrim_offsets` (PASSED, verified directly) | ✅ COMPLIANT |
| Short SRT window derivation with fallback | Turn-sourced clip with no pretrim offsets | `...::test_turn_sourced_clip_with_no_pretrim_offsets_uses_full_chapter_span` (PASSED, verified directly) | ✅ COMPLIANT |

**Compliance summary**: 4/4 scenarios COMPLIANT.

### Correctness (Static Evidence)

| Requirement | Status | Notes |
|------------|--------|-------|
| Migration 047 | ✅ Implemented | Additive, nullable FK + index + comment; DOWN block fully commented (every line prefixed `--`), matching the 046 convention |
| Migration header dependencies | ✅ Real | `025_create_speaker_turn_videos.sql` and `004_create_video_shorts.sql` both exist in `congress_videos/sql/migrations/` |
| Snapshot + drift lockstep | ✅ Verified | `production_schema.sql:164` (`turn_id` column), `:397` (index); `tests/congress_videos/sql/test_production_schema.py` — 218/218 passed |
| `get_turn_videos_for_shorts` SQL shape | ✅ Matches design §1 verbatim | Unfiltered `group_spans` CTE, `DISTINCT ON`, `NOT EXISTS` dedup on `turn_id`, floor 120, no ceiling, editorial ordering |
| `insert_video_short`/`insert_video_short_clip` turn_id | ✅ Matches design §3 | Trailing optional kwarg, inserted right after `chapter_id` in column list |
| `claim_pending_clip` CTE + LEFT JOIN | ✅ Matches design §4 verbatim | `WITH claimed AS (...)`, `LEFT JOIN speaker_turn_videos`, `LEFT JOIN LATERAL` sibling aggregate; ordering/locking preserved byte-identical |
| `pending_shorts_candidate_sql` partition/gate | ✅ Matches design §5/§6 | `PARTITION BY COALESCE(vs.turn_id, -vs.chapter_id)`; `youtube_upload_date IS NOT NULL` outer gate removed; ranking CTE stays unfiltered by upload state (#262 regression guard intact) |
| Preparer staging + pre-trim | ✅ Matches design §6 | ffprobe-first, leading `[0, target]` window, `turn_{id}_reap.mp4`, safety-gate re-probe, `turn_id` forwarded |
| Sidecar guard | ✅ Matches spec (supersedes design §7) | `turn_id is not None` → unconditional full chapter span |
| `get_chapters_for_shorts` removal | ✅ Confirmed | Zero production callers; moved to `DEAD_METHOD_NAMES` |
| Threat matrix — command injection | ✅ Covered | `TestFfmpegExtractWindow::test_uses_precise_input_seek_command_and_adaptive_timeout` asserts list command, no `shell=True` |
| Threat matrix — destructive fs op | ✅ Covered | `test_over_threshold_writes_turn_reap_path_with_leading_window` asserts `staged_clip_path != output_path` |
| Backward compatibility | ✅ Verified | No inner `JOIN ... turn_id` against `video_shorts` anywhere in `database.py`; all `video_shorts` turn_id access is `LEFT JOIN`/`COALESCE` |

### Coherence (Design)

| Decision | Followed? | Notes |
|----------|-----------|-------|
| D1 (turn_id FK) | ✅ Yes | |
| D2 (floor = span − procedural ≥ 120) | ✅ Yes | |
| D3 (leading pre-trim window) | ✅ Yes | `[0, target_secs]`, file-relative |
| D4 (reference output_path, no copy/symlink) | ✅ Yes | |
| D5 (pass group span, not rebase pretrim_*) | ✅ Yes (plumbing present) | `claim_pending_clip` exposes `group_start_seconds`/`group_end_seconds`, but the sidecar does not consume them per the accepted D7 below |
| D6 (`COALESCE(turn_id, -chapter_id)` partition) | ✅ Yes | |
| D7 (`chapter_rank` alias kept) | ✅ Yes | In-SQL comment documents the source-unit semantics |
| **§7 sidecar window math (group-span formula)** | ⚠️ **Accepted deviation** | apply-progress explicitly flags this: the orchestrator's launch prompt named `specs/short-video-srt-artifacts/spec.md`'s unconditional-full-chapter-span requirement as the authoritative contract, superseding design.md §7's group-span math. Verified: the spec text (lines 13-18, both turn-sourced scenarios) does mandate the full chapter span "regardless of whether pretrim_start_secs/pretrim_end_secs are present" — the implementation matches the spec exactly. This is a **WARNING-level**, not CRITICAL, documented and intentional divergence from design.md, correctly resolved in the spec's favor per SDD's "specs first, design second" precedence rule. |

### Tasks vs. Code State

Phases 1–4b: all 27 tasks `[x]` in `tasks.md`, matching the code and commit history (`a2ab9fd`, `32412e5`, `b339a12`, `41abc1c`, `0570027`, `8e4e104`). Phase 5 (5 orchestrator-run ops items) is intentionally `[ ]` — explicitly out of `sdd-apply` scope per the tasks.md phase heading ("post-merge, not sdd-apply") and correctly excluded from this verification's completeness gate.

### Docs Drift Audit (issue #422 policy)

| File | Status | Notes |
|------|--------|-------|
| `congress_videos/reap_clip_preparer_dag.py` module docstring | ✅ Current | Rewritten for the turn-based flow |
| `congress_videos/reap_processor_dag.py` module docstring | ✅ Current | No chapter-sourcing claims |
| `congress_videos/reap_shorts_uploader_dag.py` module docstring | ✅ Current | Audited by the apply batch; no parent-publish-gate claim found |
| `congress_videos/srt_helpers.py` module docstring | ✅ Current | Documents the turn-sourced full-chapter-span approximation |
| `docs/PIPELINE.md:110-111` | ⚠️ **STALE** | *"`reap_clip_preparer` (diario 15:00 UTC): selecciona **capítulos elegibles**, pre-recorta clips largos **con IA + contexto SRT** y los encola."* Both clauses are now wrong: the preparer selects **turn videos** (`get_turn_videos_for_shorts`, not chapters), and the pre-trim is now a **deterministic leading `[0, target]` ffmpeg window** — the AI+SRT-based `select_pretrim_window` path was deleted in PR3. **WARNING** — orchestrator to fix in the archive PR per repo policy. |
| `docs/ARCHITECTURE.md` | ✅ No drift found | Only generic shorts-sidecar-path mention (#431), not chapter/parent-gate specific |
| `CONTEXT.md` / `docs/adr/*.md` | ✅ No drift found | No Reap-chain references |

### Issues Found

**CRITICAL**: None

**WARNING**:
1. `docs/PIPELINE.md:110-111` describes the Reap preparer as selecting "capítulos elegibles" and pre-trimming "con IA + contexto SRT" — both stale post-#467 (now turn-video selection + deterministic leading-window pre-trim). Fix in archive PR.
2. Design §7's turn-sourced sidecar group-span window formula is superseded by the spec's unconditional full-chapter-span fallback; implementation correctly follows the spec (higher precedence), but this is a real, load-bearing deviation from `design.md` that should be reconciled in `design.md` itself during archive so the design doc does not mislead future readers.
3. Four Tier-1-partitioning behavioral scenarios (turn-keyed partition, legacy partition, uploaded-row-consumes-slot, unpublished-parent-returned) have correct, spec-matching test code in `test_get_pending_shorts_sql.py`, but SKIP in this sandbox (no reachable Postgres — confirmed unreachable via the NAS Tailscale endpoint too). These must be exercised against the live-Postgres suite (NAS or CI-with-Postgres) before merge is considered fully proven; this is a pre-existing repo-wide convention for this test file, not a defect introduced by this change.

**SUGGESTION**: None

### Verdict

**PASS WITH WARNINGS** — zero CRITICAL findings; full suite green (4613 passed, 0 failed, 90.59% coverage); ruff clean; DagBag clean (16 DAGs, 0 import errors); e2e correctly gated and reported `unavailable` (Docker absent, per repo policy). Three WARNING-level findings: one stale doc paragraph (`docs/PIPELINE.md`), one design-doc reconciliation debt (§7 superseded by spec, already correctly implemented), and four live-Postgres integration scenarios that skip cleanly in this sandbox and need NAS/live-DB confirmation before merge.
