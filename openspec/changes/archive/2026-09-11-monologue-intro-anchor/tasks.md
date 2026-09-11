# Tasks: Monologue Intro Anchor (issue #613)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~210 (code+tests only; SDD docs excluded from code PR) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR to `dev` |
| Delivery strategy | auto-chain |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Signal column + window-start extension, tests only, openspec untracked | PR 1 (code) | `uv run pytest tests/congress_videos/modules/test_database.py tests/congress_videos/modules/test_monologue_speaker_window.py -q` | Local DagBag import check (`dags list-import-errors`); Docker e2e optional (N/A if unavailable) | Revert single PR; no migration, no persisted-state change |
| 2 | openspec change folder archive | PR 2 (later, separate SDD archive PR) | N/A — docs only | N/A — no runtime behavior change | Revert single docs PR |

## Phase 1: Foundation — SQL signal

- [x] 1.1 RED: `tests/congress_videos/modules/test_database.py` — add `TestSelectUnpreparedTurnsChapterFirstSubstantive`: assert SQL contains `is_chapter_first_substantive`, `BOOL_OR`, `NOT EXISTS`, `st2.chapter_id = st.chapter_id`, `>= 30.0`, partitioned by `stv.output_path` (SQL-shape mock pattern, see existing class at `test_database.py:727`).
- [x] 1.2 GREEN: `congress_videos/modules/database.py` — add `SUBSTANTIVE_TURN_MIN_SECS = 30.0` module constant and the `is_chapter_first_substantive` column (D1/D2) to `select_unprepared_turns` (near line 1807); update docstring.
- [x] 1.3 RED: `tests/congress_videos/modules/test_mark_turn_resolved_live.py` — add live row-level test (skips without Postgres): chapter A (10s blip + 400s turn → flag true), chapter B (earlier 40s procedural turn filtered + long turn → flag false). Note in the test docstring: NAS disposable DB route via Tailscale (`postgres_shared:5433`) is an optional local-verification path, not required for CI.
- [x] 1.4 GREEN: confirm 1.3 passes against a reachable Postgres, or confirm the skip fires cleanly when no DB is configured.

## Phase 2: Core Implementation — window-start extension

- [x] 2.1 RED: `tests/congress_videos/modules/test_monologue_speaker_window.py` — add `monologue_window_start` unit tests: turn-335 geometry (chapter_start 14235.84, anchor 14416.84, signal true → window_start 14115.84); mid-chapter non-regression (signal false → `max(0, anchor-120)` byte-identical); fallbacks (missing/unparseable `chapter_start`, gap > 300s, `chapter_start >= anchor` → legacy value); clamp (`chapter_start < 120` → 0).
- [x] 2.2 GREEN: `congress_videos/modules/monologue_speaker_window.py` — add `MONOLOGUE_INTRO_MAX_GAP_SECS = 300.0` constant, import `_chapter_span` from `speaker_resolution` (D4), implement pure helper `monologue_window_start(turn, anchor)` per D3.
- [x] 2.3 GREEN: `congress_videos/modules/monologue_speaker_window.py` — add keyword-only `window_start: float | None = None` param to `select_preceding_window` (D5); `None` preserves the legacy `max(0, anchor_seconds - window_seconds)` formula byte-identically.
- [x] 2.4 RED: `test_monologue_speaker_window.py` — boundary tests reusing the extended window: block at exactly `window_start` included; block at `window_start - 0.001` excluded; block at `anchor` excluded; block overlapping anchor (`anchor-1` to `anchor+x`) included.
- [x] 2.5 GREEN: `congress_videos/modules/monologue_speaker_window.py` — wire `_resolve_monologue_inner` to call `monologue_window_start(turn, anchor)` and pass the result as `window_start=` to `select_preceding_window`; update `build_resolution_audit` call site so `window_start_seconds` records the effective (possibly extended) value; add one INFO log line (turn_id, gap) only when an extension applies.
- [x] 2.6 RED+GREEN: `test_monologue_speaker_window.py` — full turn-335 fixture end-to-end through `resolve_monologue_speaker`/`_resolve_monologue_inner`: with flag true, announcement resolves and audit `window_start_seconds == 14115.84`; with flag false, returns `None` and makes zero LLM calls (assert `completion_fn` not invoked).

## Phase 3: Regression coverage

- [x] 3.1 RED+GREEN: `test_monologue_speaker_window.py` — "gap above cap keeps standard window" scenario: signal true, `anchor - chapter_start > 300` → `window_start == max(0, anchor - 120)`.
- [x] 3.2 RED+GREEN: `test_monologue_speaker_window.py` — "extended window still clamps at zero" scenario: signal true, `min(anchor, chapter_start) - 120 < 0` → `window_start == 0`.
- [x] 3.3 Confirm prepare-DAG tests unchanged: mocked rows lacking `is_chapter_first_substantive` key fall back to legacy behavior via `turn.get(...)` falsy default — no new assertions needed, run existing `speaker_turn_prepare_dag` tests to confirm no drift.

## Phase 4: Verification

- [x] 4.1 Full suite: `uv run pytest -n auto` — all green, no regressions outside touched files.
- [x] 4.2 Lint: `uv run ruff check congress_videos/modules/database.py congress_videos/modules/monologue_speaker_window.py tests/congress_videos/modules/test_database.py tests/congress_videos/modules/test_monologue_speaker_window.py tests/congress_videos/modules/test_mark_turn_resolved_live.py` and `uv run ruff format --check` on the same files; confirm no C901 violation (neither file carries a `pyproject.toml` per-file `C901` ignore — new logic must stay under `max-complexity = 10`).
- [x] 4.3 Local DagBag check: `python congress_videos/speaker_turn_prepare_dag.py` (or equivalent local import) to confirm no import errors; run `bash scripts/test-airflow-e2e.sh` only if Docker is available (touches `congress_videos/**` per CLAUDE.md trigger) — report `unavailable` otherwise, not a failure.
- [x] 4.4 Confirm `openspec/changes/monologue-intro-anchor/` stays untracked/unstaged in the code PR diff (ships later via separate SDD archive PR, precedent #555/#608/#610).
