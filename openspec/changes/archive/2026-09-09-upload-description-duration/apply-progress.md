# Apply Progress: upload-description-duration

## Status: Complete — 15/15 tasks done

All three phases (RED, GREEN, REFACTOR/VERIFY) from `tasks.md` are complete in a single
apply batch. No continuation needed.

## Summary

Added `_turn_duration_metadata(video) -> dict` in `congress_videos/modules/youtube/youtube_ai.py`,
a total function that derives a turn row's published duration from the grouped span
(`group_end_seconds - group_start_seconds - procedural_seconds`, matching the
`uploadable_turns` 300s eligibility gate from migration 044), falling back to the
individual span (`end_seconds - start_seconds`) when any group field is missing/`None`,
and returning `{"duration_seconds": 0, "duration_estimated": "N/A"}` when neither span is
derivable or the derived span is `<= 0`.

Wired at `generate_youtube_metadata_for_selected_videos`'s existing duration read: turn
rows (`"turn_id" in video`) now call the new helper; chapter rows keep the byte-identical
`video.get("duration_minutes", 0)` read.

## Implementation notes / deviations

- Followed task 2.2's stricter coercion contract over the design.md snippet, per the
  apply brief's explicit instruction: a single named coercion point
  (`seconds = float(raw_span)`) after computing `raw_span` via native (Decimal-capable)
  arithmetic, with the whole derivation (arithmetic + coercion) wrapped in one
  `try/except (TypeError, ValueError)`. This differs from design.md's per-operand
  `float()` coercion but satisfies the same requirements/spec scenarios; design.md was
  advisory here per the task instructions.
- Rounding uses `math.floor(seconds / 60.0 + 0.5)` (half away from zero), not `round()`,
  per design decision — pinned by the 390s → "7 minutos" test (banker's rounding would
  give 6).
- No DB migration, DAG, or `database.py` change — matches the design's stated scope.

## Files changed

- `congress_videos/modules/youtube/youtube_ai.py` — `import math`; new
  `_turn_duration_metadata` helper; turn/chapter branch at the metadata call site.
- `tests/congress_videos/modules/youtube/test_youtube_ai.py` — `_make_turn_video()`
  fixture (mirrors migration 044's `uploadable_turns` column list verbatim, `Decimal`
  seconds, no `duration_minutes`); `TestTurnRowDurationDerivation` with 7 tests covering
  R1-R5 from the spec plus the coercion guard and the chapter regression pin.

Diff size: 186 changed lines (60 impl, 132 test — including comments/docstrings), well
under the 400-line review budget. Single PR, no `size:exception`.

## Verification (real output)

- `uv run pytest tests/congress_videos/modules/youtube/test_youtube_ai.py -v --no-cov`:
  **32 passed** (7 new + 1 regression pin + 24 pre-existing, all green).
- `uv run ruff check .`: **All checks passed!**
- `uv run ruff format --check .`: **305 files already formatted** (no reformatting needed).
- `uv run pytest` (full suite): **4836 passed, 34 skipped** (skips are pre-existing
  Postgres-dependent live tests — no local Postgres in this environment — unrelated to
  this change). Coverage: **90.80%** (`--cov-fail-under=80` requirement met).

## Commits

- `test(youtube-ai): add RED turn-duration regression tests (#514)`
- `feat(youtube-ai): derive turn row duration from group/individual span (#514)`
- `docs(sdd): mark upload-description-duration tasks complete (#514)`

## Next recommended phase

`sdd-verify`
