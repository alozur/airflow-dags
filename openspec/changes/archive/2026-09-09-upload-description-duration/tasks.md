# Tasks: Publish the real clip duration in YouTube descriptions (turn rows)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~110 (24 impl + 85 test) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|---|---|---|---|---|---|
| 1 | Derive turn-row duration + tests, wired at the existing branch point | PR 1 (single) | `uv run pytest tests/congress_videos/modules/youtube/test_youtube_ai.py -v` | N/A — pure function, no DB/network; full-suite run substitutes for a runtime scenario | Revert the `youtube_ai.py` hunk + test additions; no migration, DAG, or schema change |

Requirement key: R1 group derivation, R2 individual-span fallback, R3 non-derivable omits line, R4 minute rounding, R5 chapter unchanged.

## Phase 1: RED — Failing Tests First

- [x] 1.1 Add `_make_turn_video()` fixture to `tests/congress_videos/modules/youtube/test_youtube_ai.py`: mirror migration 044's `uploadable_turns` columns verbatim, `Decimal` seconds fields, **no `duration_minutes` key**; keep alongside `_make_top_video()`.
- [x] 1.2 RED [R1]: group span `Decimal(400)-Decimal(10)-Decimal(0)` = 390s → `"7 minutos"` (banker's rounding would give 6).
- [x] 1.3 RED [R2]: group fields `None`; fallback `start=Decimal(100), end=Decimal(560)` → 460s → `"8 minutos"`.
- [x] 1.4 RED [R3]: all span fields `None`/absent → description contains no `⏱️ Duración:` line.
- [x] 1.5 RED [R3]: group span `500-500-0` = 0s → no `⏱️ Duración:` line, `duration_estimated` is `"N/A"`.
- [x] 1.6 RED [R4]: span 45s → `"1 minutos"`; description never contains `"0 minutos"`.
- [x] 1.7 RED [R3, coercion guard]: non-numeric `group_start_seconds="bad"` fed through `generate_youtube_metadata_for_selected_videos` — call does not raise; no `⏱️ Duración:` line.
- [x] 1.8 RED [R5]: chapter `_make_top_video()` (`duration_minutes=10`) via the same entry point → `"10 minutos"` (no such assertion exists today; regression pin).
- [x] 1.9 Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_ai.py -k duration -v`; confirm every new test FAILS (helper does not exist yet).

## Phase 2: GREEN — Minimal Implementation

- [x] 2.1 Add `import math` to the top-level import block of `congress_videos/modules/youtube/youtube_ai.py`.
- [x] 2.2 Add module-level `_turn_duration_metadata(video: dict) -> dict` in `congress_videos/modules/youtube/youtube_ai.py` (near line 172, before `build_youtube_chapters_block`): derive the span via the group formula, else the individual-span fallback, else non-derivable; coerce the result to `float` **exactly once** (`seconds = float(raw_span)`) before any division; wrap the whole derivation in `try/except (TypeError, ValueError)`, returning `{"duration_seconds": 0, "duration_estimated": "N/A"}` on a bad input type; if `seconds <= 0` return the same N/A dict; else `minutes = max(1, math.floor(seconds / 60.0 + 0.5))` and return `{"duration_seconds": int(seconds), "duration_estimated": f"{minutes} minutos"}`.
- [x] 2.3 At `congress_videos/modules/youtube/youtube_ai.py:294-299`, replace the unconditional `duration_minutes` read with `if "turn_id" in video: video_metadata = _turn_duration_metadata(video)`, keeping the `else` branch's existing chapter computation byte-identical.
- [x] 2.4 Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_ai.py -v`; confirm all tests (new + existing) PASS.

## Phase 3: REFACTOR / VERIFY

- [x] 3.1 Re-check `_turn_duration_metadata` against the total-function contract (never raises, never returns `None`); adjust naming/comments only, no behavior change; re-run 2.4's command and confirm still GREEN.
- [x] 3.2 Run `uv run ruff check .` and `uv run ruff format --check .` (the exact CI gate in `.github/workflows/lint.yml`); fix any findings in the two touched files.
- [x] 3.3 Run the full suite: `uv run pytest`; confirm no regressions and coverage stays ≥ 80% (`--cov-fail-under=80` in `pyproject.toml`).

Total: 15 tasks across 3 phases.
