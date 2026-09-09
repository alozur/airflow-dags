# Design: shorts_metadata XCom TZ Normalization (issue #546)

## Technical Approach

Option A, applied at the XCom transport boundary: `_generate_metadata`
(`congress_videos/reap_shorts_uploader_dag.py`) wraps the two raw DB rows in
`utc_normalize_row` inside the `metadata_list.append(...)` literal (lines 468-479), so the
`shorts_metadata` payload pushed at line 481 contains no non-UTC fixed-offset datetime.
The regression guard round-trips the WHOLE pushed value through the real
`XComEncoder`/`XComDecoder`, mirroring the #309 test pair in
`tests/congress_videos/test_youtube_upload_dag.py:2436-2502`.

## Architecture Decisions

### D1 — Normalize at the append site, not early in the function

| Option | Tradeoff | Decision |
|---|---|---|
| Normalize `ch` right after `ch = ch or {}` (line 344) | Log line 351-358 would print UTC; `ch` diverges from what the DB returned for every in-task consumer (`_resolve_speakers`, `build_shorts_metadata_context`) for zero benefit | Rejected |
| Normalize only inside `metadata_list.append` | One greppable boundary; in-task `ch` stays the DB row; matches `youtube_upload_dag.py:536`, which also normalizes at the XCom return, not at query time | **Chosen** |

**Operator-facing log is unchanged**: `ch.get("updated_at")` at line 355 keeps printing the
raw psycopg2 value with its server-session offset. That is deliberate — the #433/D1 log is
evidence of the DB snapshot, and this change is a transport fix that must not alter
diagnostic output.

### D2 — Normalize `turn_speaker_row` as well

`get_turn_speaker_slug` (`database.py:2055-2077`) projects no datetime column today, so this
call is defensive. It earns its one line: `speaker_turn_videos` already has TIMESTAMPTZ
`materialized_at`/`prepared_at` that the SELECT merely does not project — adding one is a
one-line SQL edit, which is precisely how #512 reintroduced this class. `utc_normalize_row`
is pure and returns non-dict input unchanged, so the `turn_speaker_row is None` path keeps
its exact current payload with no extra guard. The invariant becomes readable from two
adjacent lines ("every DB row crossing this boundary is normalized") instead of "one is,
one isn't — go find out why".

### D3 — `_xcom_round_trip` stays file-local (third copy)

Extraction to a shared test helper would touch `tests/utils/test_airflow_helpers.py` and
`tests/congress_videos/test_youtube_upload_dag.py` for a two-line function, adding import
churn to a production-blocking hotfix under a 400-line budget. A bug-pin that must stay
red-raising forever is also stronger self-contained: a shared helper is one edit that could
silently weaken three suites. Define it ONCE at module level in
`test_reap_uploader_dag.py`, byte-identical to `tests/utils/test_airflow_helpers.py:21-23`
(that file's module-level form, not the function-local copies in `test_youtube_upload_dag.py`).
Revisit extraction only if a fourth site appears.

### D4 — Fixture strategy: extend both shared fixtures, no dedicated fixture

Blast radius verified by grep over `tests/congress_videos/test_reap_uploader_dag.py`:
`updated_at` appears **zero** times, and **no** test asserts on `metadata[...]["chapter"]`.
The field never reaches the AI prompt (`TestGenerateMetadataByteCompatibility:1319` builds
from title/speakers/topics/scoring_reasoning), the footer, or the session line. Adding it is
non-breaking.

| Fixture | Value | Why |
|---|---|---|
| `_make_chapter_metadata` (line 523) | `datetime(..., tzinfo=timezone(timedelta(hours=2)))` | Models the **DB row**; must carry the production hazard. Every existing `_generate_metadata` test then exercises the crashing shape — a fixture that omits the field can never catch #512 |
| `_make_short_meta` (line 882) `chapter` | `datetime(..., tzinfo=UTC)` | Models **t2's post-fix output** consumed by t2b. A non-zero offset here would model an unreachable state and duplicate the t2 bug-pin |

This refines the proposal's "both fixtures gain a NON-ZERO offset": non-zero is correct only
for the fixture that stands in for psycopg2.

### D5 — Import placement

Module-level, in the existing sorted `utils.*` block, as new line 35 between
`from utils.ai_helpers import ...` and `from utils.env_loader import ...` (isort `I` is
enforced; `ai_helpers` < `airflow_helpers`). Identical shape to `youtube_upload_dag.py:62`.
In tests, follow that file's own style: module-level `from datetime import UTC, datetime,
timedelta, timezone` (the fixtures need it), function-local `import json` +
`from airflow.utils.json import XComDecoder, XComEncoder`.

## Data Flow

    get_chapter_metadata ─┐
                          ├─→ utc_normalize_row ─→ metadata_list ─→ XCom "shorts_metadata"
    get_turn_speaker_slug ┘        (boundary)                              │
                                                                          ├─→ t2b pull/re-push
    logging.info(ch["updated_at"])  ← raw, pre-boundary                    └─→ t3 pull

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/reap_shorts_uploader_dag.py` | Modify | +1 import (line ~35), 2 values wrapped at lines 476-477 |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modify | +2 imports, `_xcom_round_trip`, 2 fixture fields, 3 tests |
| `utils/airflow_helpers.py` | Unchanged | Reused |

## Testing Strategy

| Test | Assertion |
|---|---|
| T1 bug-pin | `_xcom_round_trip([...raw `_make_chapter_metadata()`...])` raises `ValueError, match="ZoneInfo keys must be normalized relative paths"`. Never normalizes; stays red-raising forever |
| T2 primary | Run `_generate_metadata` (DB mocked, `os.path.exists`→`False`), round-trip the ENTIRE `ti.xcom_store["shorts_metadata"]`; `chapter["updated_at"]` is a `datetime`, `.utcoffset() == timedelta(0)`, equal to the fixture instant |
| T3 t2b | Round-trip `_verify_final_copy`'s re-pushed `shorts_metadata` (line 592, third serialization point) — guards a future t2b that re-queries the DB |

Dict-equality against the `_make_ti` fake store is NOT acceptable evidence: it never
serializes.

## Lint / Size Guard

Checked by reading `pyproject.toml` and counting lines with grep (this phase has no shell):
`congress_videos/reap_shorts_uploader_dag.py` is **755 lines** → ~759 after the change, under
800. `[tool.ruff.lint.per-file-ignores]` line 129 already lists this file as
`["B905", "C901"]` (issue #272 backlog), and wrapping two expressions adds **no** branches,
so cyclomatic complexity is unchanged and no baseline entry is added or invalidated.
`line-length = 120`; both new lines are far shorter. `sdd-apply` must still run
`uv run ruff check .` and `uv run ruff format --check .` — this is a static reading, not an
executed gate.

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary changes. (`_generate_metadata` shells out to ffmpeg, but this
change does not touch that path.)

## Why this keeps happening

#309 converged on a convention (`utc_normalize_row` at XCom boundaries) but left it
unenforced: nothing fails when a new call site skips it, and the fixtures for the new site
omitted the TIMESTAMPTZ field entirely, so #512 could add a raw-row XCom hop and stay green.
Convention plus green tests is not a guard when the fixture cannot express the failure. What
actually prevents recurrence is the fixture carrying a non-zero offset by default, so any
future payload built from it must survive a real serializer round trip. A repo-wide check
over all 61 `xcom_push` sites is the durable answer and is deliberately deferred to a
follow-up issue.

## Migration / Rollout

No migration. Effective in production only after `main` + NAS `git_sync`; every
`reap_shorts_uploader` run keeps failing until then. Rollback = revert the single commit.

## Open Questions

None.
