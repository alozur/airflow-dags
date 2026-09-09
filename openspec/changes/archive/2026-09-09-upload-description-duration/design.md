# Design: Publish the real clip duration in YouTube descriptions

## Technical Approach

`generate_youtube_metadata_for_selected_videos` is dict-shape polymorphic: it receives
chapter rows (`uploadable_chapters`) or turn rows (`uploadable_turns`). Only the chapter
shape carries `duration_minutes`. We add one turn branch that derives the published span
from columns the turn row already has, behind a single module-level helper. No schema,
no DAG, no `database.py`, no view change. Downstream (`generate_youtube_description`)
is untouched because the helper returns the exact `video_metadata` dict it already reads.

## Architecture Decisions

### Decision: One module-level helper, not inline branching

| Option | Tradeoff | Decision |
|---|---|---|
| `_turn_duration_metadata(video) -> dict` at module level | Derivation + fallback + rounding + N/A contract behind a one-argument interface; call site is 2 lines; chapter expression stays literally where it is | **Chosen** |
| Inline branching in the loop | Adds ~15 lines to a function already >70 lines; mixes three concerns at one indentation level | Rejected |
| Two helpers (derive + format) | Splits an atomic contract across a seam nothing varies across | Rejected |

**Signature / return contract** — total function, never raises, never returns `None`:

```python
def _turn_duration_metadata(video: dict) -> dict:
    """{"duration_seconds": int, "duration_estimated": str}. Non-derivable -> (0, "N/A")."""
```

Tests exercise the public seam `generate_youtube_metadata_for_selected_videos`, not this helper.

### Decision: `math.floor(x + 0.5)`, not `round()`

`start_seconds`/`end_seconds`/`group_*`/`procedural_seconds` are SQL `NUMERIC`
(`production_schema.sql:254,280`), so psycopg2 yields `decimal.Decimal`. Both `round()`
and `Decimal`'s default context use **banker's rounding**: `round(6.5) == 6`. The
contract demands half **away from zero**. Since the branch only runs for
`published_seconds > 0`, `math.floor(x + 0.5)` is exactly half-away-from-zero and needs
no `decimal` context plumbing. Rejected: `Decimal.quantize(ROUND_HALF_UP)` — correct but
drags a second numeric domain into a float-friendly formatting path.

### Decision: missing key and `None` collapse into one branch

`dict.get(k)` returns `None` for both an absent key and a SQL `NULL`. Both map to the
same outcome, so `is not None` is the whole test — no sentinel, no `in` checks.

## Interfaces / Contracts

```python
import math  # new module import

def _turn_duration_metadata(video):
    group = [video.get(k) for k in ("group_start_seconds", "group_end_seconds", "procedural_seconds")]
    if all(value is not None for value in group):
        start, end, procedural = (float(value) for value in group)
        seconds = end - start - procedural
    else:
        start, end = video.get("start_seconds"), video.get("end_seconds")
        seconds = float(end) - float(start) if start is not None and end is not None else 0.0
    if seconds <= 0:
        return {"duration_seconds": 0, "duration_estimated": "N/A"}
    return {
        "duration_seconds": int(seconds),
        "duration_estimated": f"{max(1, math.floor(seconds / 60.0 + 0.5))} minutos",
    }
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/modules/youtube/youtube_ai.py` | Modify | `import math`; add `_turn_duration_metadata`; branch at lines 294-299 |
| `tests/congress_videos/modules/youtube/test_youtube_ai.py` | Modify | Add `_make_turn_video()` fixture + 4 tests |

**Edit point** — `youtube_ai.py:294-299`, before:

```python
duration_minutes = video.get("duration_minutes", 0)
video_metadata = {"duration_seconds": int(duration_minutes * 60),
                  "duration_estimated": f"{int(duration_minutes)} minutos"}
```

after (chapter arm byte-identical; `youtube_upload_dag.py:384`'s discriminator):

```python
if "turn_id" in video:
    video_metadata = _turn_duration_metadata(video)
else:
    duration_minutes = video.get("duration_minutes", 0)
    video_metadata = {...}  # unchanged
```

## Data Flow

    uploadable_turns row ──→ _turn_duration_metadata ──→ video_metadata
                                                              │
      youtube_ai.py:95  duration = get("duration_estimated","N/A")
                                                              │
      youtube_ai.py:125  if duration != "N/A": render "⏱️ Duración: {duration}"

Guard chain confirmed unchanged: `"N/A"` now reaches line 95 for non-derivable rows, so
line 125 omits the block — the behaviour the guard was always written for but never saw.

## Testing Strategy

| Layer | What | Approach |
|---|---|---|
| Unit | 4 vertical slices at the public function | `mocker.patch` of `generate_chat_completion` + `construct_session_link` |

**RED first**: group-derived duration. `_make_turn_video()` mirrors the migration `044`
column list verbatim — `turn_id, output_path, chapter_id, resolved_name, start_seconds,
end_seconds, interest_score, group_start_seconds, group_end_seconds, procedural_seconds,
video_id, chapter_title, description, relevance_score, key_speakers, session_number,
session_date, materialized_at, prepared_at, resolved_participant_slug,
speaker_resolution_confidence, speaker_resolution_method` — with **no `duration_minutes`**
and `Decimal` seconds values, so it cannot drift into chapter shape. It coexists with
`_make_top_video()` (lines 68-78), which keeps its own passing test.

1. RED: group span 0/400/10 → 390s → `"7 minutos"` (banker's would give 6); no `"0 minutos"`.
2. Group fields `None` → fallback `end-start` 100..560 → 460s → `"8 minutos"`.
3. All spans `None` → `"⏱️ Duración:"` absent from the description.
4. Chapter fixture regression → still `"10 minutos"`.

**Determinism**: literal fixture values, both OpenAI/network calls mocked, no DB, no
clock read (`datetime.now` is only touched by the untested `_SPANISH_MONTHS` path).

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification,
or process-integration boundary.

## Migration / Rollout

No migration required. Descriptions are computed at build time; revert is one hunk.

## Size Forecast

~24 lines in `youtube_ai.py`, ~85 test lines → **~110 authored code+test lines**; ~300
including SDD markdown. Single PR. 400-line budget risk: **Low**.

## Open Questions

- None.
