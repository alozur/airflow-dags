# Exploration: shorts_metadata XCom tz normalization (issue #546)

## Current State

`congress_videos/reap_shorts_uploader_dag.py::_generate_metadata` (t2) calls
`db.get_chapter_metadata(chapter_id)` -> `ch` and, when `turn_id` is set,
`db.get_turn_speaker_slug(turn_id)` -> `turn_speaker_row`. Both raw dicts are
appended UN-NORMALIZED into each `metadata_list` entry (lines 468-479) under
keys `"chapter"` and `"turn_speaker_row"`, then pushed to XCom key
`shorts_metadata`. `_verify_final_copy` (t2b) pulls that same key back
(`ti.xcom_pull(key="shorts_metadata")`, line 513) and, for each entry, calls
`_copy_verification_evidence(meta.get("chapter"), meta.get("turn_speaker_row"))`
(line 526), which reads ONLY these fields:

- from `chapter`: `mentioned_participant_slugs, title, description, topics,
  speakers, key_speakers, scoring_reasoning, session_number, session_date`.
- from `turn_speaker_row`: `resolved_participant_slug,
  speaker_resolution_confidence, speaker_resolution_method`.

`updated_at` (present in `ch` for the `_generate_metadata` logging line only,
line 356) is NEVER read by `_copy_verification_evidence`. t2b re-pushes the
(possibly corrected) `shorts_metadata` at line 592, and t3
(`_trigger_youtube_upload`) pulls it again at line 607 — three independent
deserialization points share the same un-normalized payload.

## Root cause — precise, not "at least one field"

Traced every column actually SELECTed:

- `get_chapter_metadata` (`database.py:2028-2053`) SELECTs
  `vc.chapter_id, title, description, speakers, key_speakers, topics,
  scoring_reasoning, relevance_score, youtube_video_id,
  mentioned_participant_slugs, updated_at` plus `ysv.session_number,
  session_date` (LEFT JOIN youtube_source_videos). Only `vc.updated_at` is
  `TIMESTAMPTZ` (`production_schema.sql:109`, `video_chapters.updated_at
  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP`). `session_date` comes from
  `youtube_source_videos.session_date DATE` (naive, schema.sql:36) —
  unaffected.
- `get_turn_speaker_slug` (`database.py:2055-2077`) SELECTs only `turn_id,
  resolved_participant_slug, speaker_resolution_confidence,
  speaker_resolution_method` — NONE are datetime-typed
  (`speaker_turn_videos` has TIMESTAMPTZ `materialized_at`/`prepared_at`, but
  this query never selects them). **`turn_speaker_row` carries zero
  TIMESTAMPTZ risk under the current SQL.** The issue body's framing ("at
  least one TIMESTAMPTZ column in `ch`/`turn_speaker_row`") is imprecise —
  the actual and ONLY culprit is `chapter["updated_at"]`.

Why `pending_shorts` (t1's own XCom, carrying raw `video_shorts.*` via
`pending_shorts_candidate_sql`, `database.py:82-126`) does NOT crash despite
also being un-normalized: `video_shorts.created_at`/`updated_at`/
`copy_verified_at` are declared `TIMESTAMP` (no time zone,
`production_schema.sql:150-151,176`), so psycopg2 returns naive datetimes —
`_to_utc` would attach UTC if normalized, but even un-normalized a naive
datetime round-trips through Airflow's XCom serializer fine (no ZoneInfo
crash). This confirms the crash class is specific to `TIMESTAMPTZ`-typed
columns with a non-UTC session offset, matching issues #163/#303/#309.

## utc_normalize_row / utc_normalize_rows (utils/airflow_helpers.py:54-93)

- `_to_utc(value)`: non-datetime passes through; naive datetime gets `UTC`
  ATTACHED via `.replace(tzinfo=UTC)` (never `.astimezone()`, which would
  wrongly assume system-local); tz-aware gets `.astimezone(UTC)`.
- `utc_normalize_row(row)`: `dict -> new dict` (pure, never mutates), applies
  `_to_utc` to every value. Non-dict input passes through unchanged. FLAT
  ONLY — does not walk nested dicts/lists. `Decimal` and other non-datetime
  types pass through untouched (this is a documented, out-of-scope
  limitation shared with the code it replaced).
- `utc_normalize_rows(rows)`: `list -> list` via `utc_normalize_row` per
  element. No `None` guard by design (a bare `TypeError` on `None` is
  preferred over a silent `[]`, per the docstring).

## Established convention (issue #309, confirmed in this worktree)

`youtube_upload_dag.py:536` — `_run_get_uploadable_item` already does
`return {"item": utc_normalize_row(turns[0]), "item_type": "turn"}`,
converged from the old `_sanitize_row_for_xcom` (deleted by #309). This is
the ONE existing production call site of `utc_normalize_row` in the repo.

Critically, `youtube_upload_dag.py`'s OWN `_verify_final_copy`
(`_copy_verification_evidence`, lines 631-685) takes `db, *, chapter_id,
turn_id` and calls `db.get_chapter_metadata` / `db.get_turn_speaker_slug`
ITSELF, inside the task — it never carries a raw chapter/turn row through
XCom at all. **Verified: the issue's claim that the long-form uploader is
unaffected is correct** — confirmed by reading `youtube_upload_dag.py:631-685`
directly, not inferred.

Repo-wide grep of `get_chapter_metadata\(|get_turn_speaker_slug\(` confirms
exactly two call sites in the whole DAG surface: `youtube_upload_dag.py`
(same-task re-query, safe) and `reap_shorts_uploader_dag.py`
(`_generate_metadata`, the buggy one). `reap_clip_preparer_dag.py` and
`reap_processor_dag.py` only push scalar counts (e.g. `clips_queued`), not
raw rows. **Scope is confirmed narrow: this is a single-DAG,
single-append-site bug.**

## Two candidate fixes

### (A) Normalize `ch` / `turn_speaker_row` before `metadata_list.append`

Mirrors #309's established pattern exactly:

```python
metadata_list.append({
    ...,
    "chapter": utc_normalize_row(ch),
    "turn_speaker_row": utc_normalize_row(turn_speaker_row),
})
```

- Pros: matches the established repo convention (#303/#309), minimal diff
  (2 lines + 1 import), generically neutralizes ANY future TIMESTAMPTZ
  column added to either SELECT without requiring an audit, keeps
  `_copy_verification_evidence`'s existing signature/logic untouched.
- Cons: still carries an entire raw DB row (including fields
  `_copy_verification_evidence` never reads, e.g. `relevance_score`,
  `youtube_video_id`, `chapter_id`) through XCom — the architectural smell
  the issue's own "Cause" section flags, just tz-safe now instead of
  eliminated.
- Effort: Low.

### (B) Project only the scalar fields `_verify_final_copy` needs

- Would carry, from `chapter`: `mentioned_participant_slugs, title,
  description, topics, speakers, key_speakers, scoring_reasoning,
  session_number, session_date` (drops `chapter_id, relevance_score,
  youtube_video_id, updated_at, source_video_title, source_video_url` —
  none read by `_copy_verification_evidence`). From `turn_speaker_row`:
  `resolved_participant_slug, speaker_resolution_confidence,
  speaker_resolution_method`.
- None of the projected fields are TIMESTAMPTZ, so this eliminates the bug
  class entirely rather than normalizing around it — no `utc_normalize_row`
  call needed at all for this XCom entry.
- Changes `_copy_verification_evidence`'s call contract: it currently
  extracts fields itself from full `chapter`/`turn_speaker_row` dicts
  (`_copy_verification_evidence(meta.get("chapter"),
  meta.get("turn_speaker_row"))`, line 526) — narrowing the carried dict to
  only the fields it reads is a no-op for its internal logic as long as key
  names are preserved (it never reads a field outside that projected set
  today), but it is a slightly larger diff and a second place
  (`_generate_metadata`'s append site) that must be kept in lockstep with
  whatever `_copy_verification_evidence` reads if that function's field list
  ever grows.
- Pros: narrower payload, no raw DB row leaves the DB layer via XCom at all,
  addresses the issue's own architectural critique directly.
- Cons: diverges from the #309 established "normalize the whole row"
  convention; requires enumerating fields at TWO points that must be kept in
  sync (`_generate_metadata`'s projection, `_copy_verification_evidence`'s
  reads); does not generically protect against a future TIMESTAMPTZ column
  added to `get_chapter_metadata`'s SELECT (silent reintroduction risk).
- Effort: Low-Medium.

## Existing test coverage — the fixture gap

`tests/congress_videos/test_reap_uploader_dag.py`:

- `_make_ti()` (line 113) builds a `MagicMock`-backed FAKE XCom store — a
  plain Python dict via `xcom_push.side_effect`/`xcom_pull.side_effect`. It
  NEVER round-trips through Airflow's real `XComEncoder`/`XComDecoder`, so
  even a raw tz-broken dict would pass silently today.
- `_make_chapter_metadata()` (line 523) never includes `updated_at` in its
  base dict at all — the exact field that crashes is absent from every
  existing chapter fixture.
- `_make_short_meta()` (line 882) builds `"chapter": {"title": "Debate",
  "mentioned_participant_slugs": None}` — also missing `updated_at`.
- `TestVerifyFinalCopyShorts` (line 894) exercises `_verify_final_copy`
  entirely through the fake `_make_ti` store — no test in this class can
  ever catch a serializer-level bug.

This exactly matches the repo memory pattern ("fixtures with int hide both
bugs"): the gap here is fixtures using naive/absent datetimes plus a fake
XCom store that never serializes.

The established real-round-trip convention already exists in THIS repo:
`tests/utils/test_airflow_helpers.py:21-23` and
`tests/congress_videos/test_youtube_upload_dag.py:2455-2502` both define

```python
def _xcom_round_trip(value):
    return json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)
```

from `airflow.utils.json import XComDecoder, XComEncoder`. The #309 test
suite pairs this with TWO tests per fix: one proving the RAW (unfixed) row
breaks with `pytest.raises(ValueError, match="ZoneInfo keys must be
normalized relative paths")`, and one proving the NORMALIZED row survives
the round trip as a real `datetime` with `.utcoffset() == timedelta(0)`
(`test_youtube_upload_dag.py:2412-2502`). A genuine regression test for
issue #546 must mirror this exact pair, applied to the `shorts_metadata`
XCom value built by `_generate_metadata`, using a fixture `ch` whose
`updated_at` is a tz-aware `datetime` with a NON-ZERO fixed offset (e.g.
`timezone(timedelta(hours=2))`, matching `_make_turn_row`'s convention in
`test_youtube_upload_dag.py`) — not a naive datetime, and not the fake
`_make_ti` store. A plain dict-equality assertion against the fake store
would NOT be a genuine regression test; it must go through
`json.dumps(..., cls=XComEncoder)` / `json.loads(..., cls=XComDecoder)`.

## Affected Areas

- `congress_videos/reap_shorts_uploader_dag.py` — `_generate_metadata`
  (append site, ~line 468-479), `_verify_final_copy` (consumer, unaffected
  by the fix itself but is where the crash currently surfaces).
- `tests/congress_videos/test_reap_uploader_dag.py` — `_make_chapter_metadata`,
  `_make_short_meta`, plus new regression tests near
  `TestGenerateMetadataFooter`/`TestVerifyFinalCopyShorts`.
- `utils/airflow_helpers.py` — reused unchanged (`utc_normalize_row`), only
  if Option A is chosen.

## Recommendation

Option A (normalize via `utc_normalize_row`) for the minimal, convention-
matching fix that closes the reported crash with the least risk and the
smallest diff, consistent with #303/#309. Option B is a legitimate follow-up
architectural improvement (reduce XCom payload, remove raw-row coupling)
that the propose/design phase can consider as a stretch goal or a separate
follow-up issue, since it changes more surface for no additional bug-fix
value in the current call graph (verified: `_copy_verification_evidence`
never reads `updated_at` either way).

## Risks

- `Decimal`-typed fields are NOT touched by `utc_normalize_row` (documented,
  pre-existing, out of scope) — `ch`/`turn_speaker_row` in this DAG have no
  `Decimal` fields today (`speaker_resolution_confidence` is `DOUBLE
  PRECISION` -> Python `float`, not `Decimal`), so this is not a live risk
  for issue #546, only a note for future column additions.
- `pending_shorts` XCom (t1) also carries raw, un-normalized `video_shorts.*`
  rows; currently safe only because those columns are `TIMESTAMP` (naive),
  not `TIMESTAMPTZ` — a latent risk if that table's `created_at`/`updated_at`
  are ever migrated to `TIMESTAMPTZ`. Out of scope for #546 but worth a
  one-line note in the proposal/design so it isn't silently reintroduced.
- Whichever option is chosen, `meta["chapter"]` is later logged at
  `_generate_metadata` line 356 BEFORE the append — normalizing `ch` earlier
  in the function (before the log line) vs. only at append time is a design
  choice with no functional difference for the crash but changes what the
  operator-facing log shows (UTC-normalized vs raw offset `updated_at`).

## Ready for Proposal

Yes. Root cause is pinned to one field (`chapter["updated_at"]`), scope is
confirmed to a single DAG/single append site, both fix options are fully
specified with exact code-level pros/cons, and the regression-test gap plus
the exact convention to close it (`XComEncoder`/`XComDecoder` round trip,
mirroring `test_youtube_upload_dag.py`'s #309 test pair) are identified.
No open questions block `sdd-propose`.
