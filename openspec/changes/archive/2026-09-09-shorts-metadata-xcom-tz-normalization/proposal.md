# Proposal: shorts_metadata XCom TZ Normalization (issue #546)

## Intent

`reap_shorts_uploader` is broken in production: zero shorts publish. `_generate_metadata`
(t2) appends the raw `get_chapter_metadata` row into XCom `shorts_metadata`.
`video_chapters.updated_at` is TIMESTAMPTZ, so psycopg2 returns an unnamed fixed-offset
datetime; every downstream pull (t2b, t3) raises `ValueError: ZoneInfo keys must be
normalized relative paths`. Fourth instance of this class (#163, #303, #309), reintroduced
by #512. Production-blocking.

## Scope

### In Scope
- `utc_normalize_row` on `chapter` and `turn_speaker_row` at the append site.
- Regression pair via real `XComEncoder`/`XComDecoder`: raw row RAISES; fixed payload
  survives with `.utcoffset() == timedelta(0)`.
- Durable guard: round-trip the WHOLE pushed `shorts_metadata` value, so any future raw
  TIMESTAMPTZ field in that payload fails the test.
- Fixtures `_make_chapter_metadata` / `_make_short_meta` gain `updated_at` with a NON-ZERO
  offset (`timezone(timedelta(hours=2))`).

### Out of Scope
- Repo-wide XCom guard. Evidence: 61 `xcom_push` sites across 12 DAGs; auditing them
  exceeds the 400-line budget and this crash's evidence. **Follow-up issue.**
- Option B (project only scalar fields). **Follow-up issue.**
- `pending_shorts` (t1) also carries raw `video_shorts.*` rows — safe only because those
  columns are naive `TIMESTAMP`. Latent; noted, not fixed.

## Capabilities

### New Capabilities
- `xcom-row-serialization`: DB rows crossing XCom MUST be UTC-normalized and MUST survive
  an `XComEncoder`/`XComDecoder` round trip.

### Modified Capabilities
None — `final-copy-verification` semantics are unchanged; this is a transport defect.

## Approach

Option A. Matches the convention `youtube_upload_dag.py:536` and
`video_analytics_dag.py:211` already follow, ~6 changed lines, keeps
`_copy_verification_evidence`'s signature intact, and neutralizes any future TIMESTAMPTZ
column in either SELECT without an audit. `utc_normalize_row(None)` returns `None`, so the
`turn_speaker_row is None` path stays safe.

Option B rejected: it needs two field lists kept in lockstep and still lets a NEW
TIMESTAMPTZ column reintroduce the crash silently. Accepted tradeoff: a full raw row still
crosses XCom — the smell survives, tz-safe.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/reap_shorts_uploader_dag.py` | Modified | Import + 2 normalized values (~6 lines) |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modified | Fixtures + 3 round-trip tests (~110 lines) |
| `utils/airflow_helpers.py` | Unchanged | Reused |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Fix merged, prod still broken | High | Effective only after `main` + NAS `git_sync`; every run fails until then |
| Test asserts the mock store, not the serializer | Med | Dict-equality against `_make_ti` is NOT acceptable |
| Class returns via another push site | Med | Follow-up issue; this guards one payload |
| `Decimal`/nested values untouched | Low | Pre-existing, documented; no such fields here |

## Rollback

Revert the single commit. Behavior returns to today's crash; no schema, migration or state
change to undo.

## Dependencies

None — `utc_normalize_row` already exists.

## Success Criteria

- [ ] Test proves RAW payload raises `ValueError, match="ZoneInfo keys must be normalized relative paths"`.
- [ ] Test proves FIXED payload round-trips with `.utcoffset() == timedelta(0)`.
- [ ] `uv run pytest` green; `dags list-import-errors` empty.
- [ ] Post-`git_sync`, a run reaches t3 and publishes.
- [ ] Two follow-up issues filed.

## Size Estimate

Code + tests: **~116 changed lines** — one PR, within the 400-line budget. SDD markdown adds
~300-380; ship as a separate `docs(sdd)` commit so it does not consume reviewer budget.
