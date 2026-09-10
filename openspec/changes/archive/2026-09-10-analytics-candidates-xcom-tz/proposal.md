# Proposal: video_analytics_actions candidates XCom TZ Normalization (issue #605)

## Intent

**Problem**: `video_analytics_actions` crashes at `evaluate_candidates`. `_run_select_candidates`
pushes the raw `get_unactioned_snapshots()` rows to XCom `candidates` (and returns them as
`return_value`). `video_analytics_snapshots.collected_at` is TIMESTAMPTZ, so psycopg2 returns an
unnamed fixed-offset datetime and `xcom_pull(key="candidates")` raises `ValueError: ZoneInfo keys
must be normalized relative paths`. This is the fifth instance of the class (#163, #303, #309, #546).
A second crash is hidden behind it: `_run_evaluate_candidates` spreads `**candidate` into each
`decisions` entry, so `record_no_ops` and `apply_actions` would fail the same way on `decisions`.

## Scope

### In Scope
- Wrap `db.get_unactioned_snapshots()` in `utc_normalize_rows()` inside `_run_select_candidates`,
  before `xcom_push(key="candidates")` and `return`.
- Regression tests that use the real `XComEncoder`/`XComDecoder`, with `collected_at` set to
  `timezone(timedelta(hours=2))`:
  - raw `candidates` payload raises the ZoneInfo `ValueError` (pin);
  - normalized `candidates` payload survives with `.utcoffset() == timedelta(0)`;
  - derived `decisions` payload (from `_run_evaluate_candidates`) survives the round trip.

### Out of Scope
- Repo-wide XCom push audit (tracked outside this change).
- `get_chosen_thumbnail` rows: they are never forwarded to XCom, so they are safe today.
- Pull-site normalization in `evaluate`/`record_no_ops`/`apply_actions` (rejected, see Approach).

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `xcom-row-serialization`: extend the contract from `shorts_metadata` to the
  `video_analytics_actions` `candidates` push site and its derived `decisions` payload
  (ADDED requirements; the existing shorts requirements are unchanged; Purpose widened).

## Approach

Normalize once, at the push site. This is the convention `video_analytics_dag.py:211` and
`youtube_upload_dag.py:536` already follow. The normalized `collected_at` carries named UTC
through the `**candidate` spread, which closes both crash sites. The same object is pushed and
returned, so `return_value` is covered too. We rejected per-pull normalization because it needs
three call sites and one is easy to miss.

No behavior change. `evaluate_action` never reads `collected_at`, and `_snapshot_age_days` only
does instant arithmetic, which is invariant under `.astimezone(UTC)`.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/video_analytics_actions_dag.py` | Modified | Import + wrapped call (~3 lines) |
| `tests/congress_videos/test_video_analytics_actions_dag.py` | Modified | Real-serializer round-trip tests (~60-90 lines) |
| `utils/airflow_helpers.py` | Unchanged | `utc_normalize_rows` reused |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Tests assert the MagicMock store instead of the serializer | Med | Tests MUST use `XComEncoder`/`XComDecoder` |
| Fix merged but prod still crashes | High | Only takes effect after `main` plus NAS `git_sync` |
| Another TIMESTAMPTZ column is added to the SELECT | Low | Row-wide normalization covers it |

## Rollback Plan

Revert the single commit. Behavior goes back to today's crash. There is no schema, migration or
data state to undo.

## Dependencies

None. `utc_normalize_rows` already exists and has tests.

## Success Criteria

- [ ] The raw `candidates` payload raises `ValueError` matching "ZoneInfo keys must be normalized relative paths".
- [ ] Normalized `candidates` and derived `decisions` round-trip with `.utcoffset() == timedelta(0)`.
- [ ] `uv run pytest` is green; `dags list-import-errors` is empty.
- [ ] After `git_sync`, a run reaches `apply_actions` with no ZoneInfo error.
- [ ] Single PR to `dev`, under 400 authored lines.
