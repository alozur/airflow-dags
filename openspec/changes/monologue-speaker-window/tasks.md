# Tasks: Monologue Speaker Window (issue #430)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~1090 total (A1 ~280, A2a ~250, A2b ~230, B ~120, C ~210) |
| 400-line budget risk | High for the whole change; each chained slice stays under budget |
| Chained PRs recommended | Yes |
| Suggested split | A1 → A2a → A2b → B → C, stacked-to-main |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: High

Open design questions resolved here: (a) the A1/A2a/A2b split is accepted as forecast; (b) each
phase ends with a "Measure" task — if the measured diff exceeds ~350 lines, split that slice again
before opening its PR.

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|---|---|---|---|---|---|
| A1 | Window selection + 4 prompts | PR 1 (`feat/430-a1-window-selection`, base `dev`) | `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -k "window or prompt"` | `bash scripts/test-airflow-e2e.sh` (congress_videos/** trigger) | Delete the module + prompt constants; nothing imports them |
| A2a | Step 1 + Step 2 (isolated) | PR 2 (base A1 branch) | `... -k "identify_floor_holder or resolve_announced_identity"` | same e2e trigger | Revert to A1's inert module |
| A2b | Orchestrator + audit | PR 3 (base A2a branch) | `... -k "resolve_monologue_speaker or audit or pregate"` | same e2e trigger | Revert to A2a; both steps stay usable/tested |
| B | Migration 046 + `mark_turn_resolved(evidence=)` | PR 4 (base A2b branch) | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database_speaker_resolution.py` | live-PG opt-in via NAS `postgres_shared:5433` (`-o addopts=`), else N/A | Revert code; leave the nullable column, or run the commented DOWN manually |
| C | Routing + caller rewiring + docs | PR 5 (base B branch) | `uv run pytest tests/congress_videos/test_speaker_turn_prepare_dag.py` | `uv run python congress_videos/speaker_turn_prepare_dag.py` + e2e trigger | Revert this commit: monologue returns to `resolve_speaker`, module goes inert |

## Phase 1 — A1: Window Selection + Prompts (`feat/430-a1-window-selection`, base `dev`)

- [x] 1.1 RED: `tests/congress_videos/modules/test_monologue_speaker_window.py` (create) — 6 boundary
      cases for *Preceding Window Selection* (anchor-120 in; anchor-120-0.001 out; at-anchor out;
      overlapping in; anchor<120 clamps to 0; `group_start_seconds` incl. `0.0` overrides
      `start_seconds`). Run: fails (module missing).
- [x] 1.2 GREEN: `congress_videos/modules/monologue_speaker_window.py` (create) —
      `MONOLOGUE_WINDOW_SECS`, `MONOLOGUE_RESOLUTION_METHOD`, `turn_anchor_seconds`,
      `select_preceding_window`. Same test passes.
- [x] 1.3 RED: prompt-contract tests — `MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT` carries the
      addressee, `found=false`, and verbatim-evidence rules; user templates interpolate only
      `window_text` / `announced_name_or_role`+`evidence`+`participant_roster`.
- [x] 1.4 GREEN: `congress_videos/config/ai_prompts.py` (modify) — add the 4 prompt constants from
      design.md verbatim, after `SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE`.
- [x] 1.5 Quality: `uv run ruff check congress_videos/modules/monologue_speaker_window.py
      congress_videos/config/ai_prompts.py tests/congress_videos/modules/test_monologue_speaker_window.py
      && uv run ruff format --check` (same paths).
- [x] 1.6 Targeted: `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v`.
- [x] 1.7 Full: `uv run pytest -n auto`.
- [x] 1.8 Measure: `git diff --stat dev...HEAD -- . ':!openspec'` vs ~280-line forecast; resplit if
      it exceeds ~350.

## Phase 2 — A2a: LLM Steps (`feat/430-a2a-llm-steps`, base A1 branch)

- [x] 2.1 RED: `identify_floor_holder` seam pass-through (mock-echo cases incl. `García`,
      `X` for the addressee example, `Ruiz`); error-response cases (`error` set / `data` missing /
      `data` not a dict → `FloorHolder()` sentinel + one WARNING, no exception); raise propagates
      uncaught at this seam.
- [x] 2.2 GREEN: add `FloorHolder` dataclass + `identify_floor_holder` to
      `monologue_speaker_window.py`.
- [x] 2.3 RED: `resolve_announced_identity` roster/confidence gates (0.80 accept, 0.79 reject, slug
      outside roster reject, non-numeric confidence reject); payload-scope test (no window text
      beyond the evidence quote, roster present); same error-response and raise cases as 2.1.
- [x] 2.4 GREEN: add `AnnouncedIdentity` dataclass + `resolve_announced_identity`.
- [x] 2.5 Quality: `uv run ruff check` + `ruff format --check` on the module and test file.
- [x] 2.6 Targeted: `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v`.
- [x] 2.7 Full: `uv run pytest -n auto`.
- [x] 2.8 Measure: `git diff --stat <A1-branch>...HEAD -- . ':!openspec'` vs ~250-line forecast.
      Landed at 423 authored lines with 4 separate identify_floor_holder mock-echo tests + 2
      separate resolve_announced_identity confidence-boundary tests; collapsed both pairs into
      one `@pytest.mark.parametrize` test each (identical coverage, no scenario dropped) to land
      at 395 authored lines — under the 400-line budget with no `size:exception` needed. Both
      steps and all their tests stayed on this single branch; no further split into a separate
      identity-step branch was required.

## Phase 3 — A2b: Orchestrator (`feat/430-a2b-orchestrator`, base A2a branch)

- [x] 3.1 RED: pre-gate no-call (no announcement phrase → `None`, `completion_fn` call count 0);
      Step-1 payload-exclusion (sentinel text outside the window absent from every captured prompt).
- [x] 3.2 GREEN: `_load_turn_blocks` + `build_resolution_audit` in `monologue_speaker_window.py`.
- [x] 3.3 RED: `found=false` stops before Step 2 (call count 1); evidence not locatable in window
      blocks → `None`; audit JSON has exactly the 7 keys incl. `method="monologue_window_v1"`;
      result shape has `participant_slug`/`confidence`/`evidence`/`audit`.
- [x] 3.4 RED: never-raise end to end — `completion_fn` raising on Step 1, and on Step 2 → `None`
      + one WARNING (`caplog`). NOTE: the opt-in `@pytest.mark.live_llm` test described in
      design.md's Testing Strategy is NOT assigned to any Phase 1-3 task and was not added here;
      `live_llm` marker registration is deferred until a task explicitly schedules it.
- [x] 3.5 GREEN: `_resolve_monologue_inner` + `resolve_monologue_speaker` (try/except wrapper).
- [x] 3.6 Quality: `uv run ruff check` + `ruff format --check` on the module and test file.
- [x] 3.7 Targeted: `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v`.
- [x] 3.8 Full: `uv run pytest -n auto`.
- [x] 3.9 Measure: `git diff --stat <A2a-branch>...HEAD -- . ':!openspec'` vs ~230-line forecast.
      Landed at 320 authored lines (308 insertions + 12 deletions) on the first pass — under the
      400-line budget with no collapsing needed, following the parametrize-from-the-start
      discipline applied in the A2a budget correction.

## Phase 4 — B: Evidence Migration (`feat/430-b-evidence-migration`, base A2b branch)

- [x] 4.1 Confirm migration number 046 is still free (renumber if PR #449/045 landed differently).
      Confirmed via `ls congress_videos/sql/migrations/ | tail -3`: 045 (mentioned people, issue
      #432, from PR #449) already exists on this branch; 046 is free — no renumbering needed.
- [x] 4.2 Create `congress_videos/sql/migrations/046_add_speaker_resolution_evidence.sql` — exact
      SQL from design.md (`ADD COLUMN IF NOT EXISTS`, DOWN block fully commented per 044).
- [x] 4.3 RED: `test_production_schema.py` column-tuple test expects
      `speaker_resolution_evidence` in `TABLE_COLUMNS["speaker_turn_videos"]` — fails.
- [x] 4.4 GREEN: `congress_videos/sql/production_schema.sql` (modify) — add
      `speaker_resolution_evidence TEXT` after `speaker_resolution_method`, extend the folded
      header comment with `+ 046`.
- [x] 4.5 RED: `tests/congress_videos/modules/test_database_speaker_resolution.py` (+3 SQL-shape
      tests) — SQL contains `speaker_resolution_evidence` with `evidence=`, absent without it;
      5-positional-arg call leaves the SQL byte-identical. Implemented as one
      `@pytest.mark.parametrize` test (2 cases: provided/omitted) + one byte-identical golden-string
      test, per the parametrize-from-the-start discipline.
- [x] 4.6 GREEN: `congress_videos/modules/database.py` — `mark_turn_resolved(..., evidence:
      str | None = None)` exactly as in design.md; WHERE clause and `logger.info` untouched.
- [x] 4.7 `tests/congress_videos/modules/test_mark_turn_resolved_live.py` (modify) — +1 column in
      `_SCHEMA_SQL`, +1 opt-in live round-trip test against NAS `postgres_shared:5433`. NAS
      unreachable from this sandbox (no Tailscale) — the new test skips cleanly alongside the 2
      pre-existing live tests in this file, same as every prior slice's Docker/NAS harness.
- [x] 4.8 Quality: `uv run ruff check` + `ruff format --check` on changed files.
- [x] 4.9 Targeted: `uv run pytest tests/congress_videos/sql/test_production_schema.py
      tests/congress_videos/modules/test_database_speaker_resolution.py -v`.
- [x] 4.10 Full: `uv run pytest -n auto`.
- [ ] 4.11 Apply migration 046 to dev, then to prod, BEFORE Phase 5 merges to main. NOT DONE in
      this apply slice — this is a deployment/ops action requiring live dev/prod DB access this
      sandboxed worktree does not have (confirmed: NAS `postgres_shared:5433` is unreachable, no
      Tailscale). Flagged for the orchestrator/maintainer to run before Phase 5 (C) merges to main.
- [x] 4.12 Measure: `git diff --stat <A2b-branch>...HEAD -- . ':!openspec'` vs ~120-line forecast.
      Landed at 145 authored lines (142 insertions + 3 deletions) on the first pass — under the
      400-line budget, no collapsing needed.

## Phase 5 — C: Routing + Wiring (`feat/430-c-routing`, base B branch)

- [ ] 5.1 RED: 3 routing tests in `tests/congress_videos/test_speaker_turn_prepare_dag.py` —
      non-qa turn calls `resolve_monologue_speaker` not `resolve_speaker`; `turn_type='qa'` calls
      `resolve_speaker` not the monologue resolver; qa-promotion wide re-pass still calls
      `resolve_speaker` with `turn_type='qa'`.
- [ ] 5.2 GREEN: `congress_videos/speaker_turn_prepare_dag.py` — routing at the resolve-speakers
      step (`turn_type != 'qa'` → new resolver) and `evidence=winner.get("audit") or
      winner.get("evidence") or None` at the `mark_turn_resolved` call site, per design.md.
      Docstring updated (no new "airflow"/"dag" trigger words).
- [ ] 5.3 Rewire the caller suite (`test_speaker_turn_prepare_dag.py`): the 23
      `patch("...resolve_speaker")` sites patch the new resolver for non-qa `_make_turn()` turns;
      the 10 `mark_turn_resolved.assert_called_once_with(...)` + 2 `assert_not_called` gain the
      `evidence=` kwarg; `TestQaPromotionReresolution._run` uses two mocks (narrow → monologue,
      wide → `resolve_speaker`) instead of one `side_effect` list.
- [ ] 5.4 `docs/PIPELINE.md` (modify) — one paragraph on the two-step monologue resolution.
- [ ] 5.5 Grep `docs/` and root `*.md` for stale `120`/"intro window"/`resolve_speaker` mentions of
      the old single-call window and update them.
- [ ] 5.6 Import check: `uv run python congress_videos/speaker_turn_prepare_dag.py`.
- [ ] 5.7 Non-regression check: `git diff dev...HEAD --
      tests/congress_videos/modules/test_speaker_resolution.py` is empty; run
      `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` green.
- [ ] 5.8 Draft follow-up-issue text in apply-progress notes: "reassess `resolve_speaker`'s narrow
      branch after monologue routing (#430)" — for the orchestrator to file post-merge.
- [ ] 5.9 Quality: `uv run ruff check` + `ruff format --check` on changed files.
- [ ] 5.10 Targeted: `uv run pytest tests/congress_videos/test_speaker_turn_prepare_dag.py -v`.
- [ ] 5.11 Full: `uv run pytest -n auto`.
- [ ] 5.12 Measure: `git diff --stat <B-branch>...HEAD -- . ':!openspec'` vs ~210-line forecast.

## Commit Plan

Work units per phase (conventional commits, no attribution trailers): one commit per RED+GREEN
pair (tests travel with the code they prove), one `chore(sdd): update tasks.md/apply-progress`
commit per phase for bookkeeping. Example: `feat(speaker-resolution): add monologue window
selection and prompts`, `feat(speaker-resolution): add floor-holder and identity LLM steps`,
`feat(speaker-resolution): add monologue orchestrator with never-raise contract`,
`feat(db): persist speaker resolution evidence (migration 046)`,
`feat(speaker-resolution): route monologue turns to the two-step resolver`.
