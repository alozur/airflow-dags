# Delta for XCom Row Serialization

## Purpose (updated)

Widen this capability's Purpose paragraph to also cover
`video_analytics_actions_dag.py::_run_select_candidates`'s `candidates` XCom
push (and its `return_value`) and the `decisions` payload derived from it in
`_run_evaluate_candidates` — the fifth recurrence of this defect class (#163,
#303, #309, #546).

## ADDED Requirements

### Requirement: Candidates row normalization at the XCom push site

For every row returned by `db.get_unactioned_snapshots()`, `_run_select_candidates`
MUST apply `utc_normalize_rows` to the full result list before the value is
pushed via `ti.xcom_push(key="candidates", value=...)` and before it is
returned as the task's `return_value`. The pushed value and the returned
value MUST be the same normalized object — no un-normalized copy MUST be
sent to XCom or returned by the callable.

#### Scenario: Candidates with a tz-aware collected_at are normalized before push

- GIVEN `get_unactioned_snapshots()` returns rows whose `collected_at` is a
  tz-aware `datetime` with a non-zero fixed UTC offset (e.g.
  `timezone(timedelta(hours=2))`)
- WHEN `_run_select_candidates` runs
- THEN the value passed to `ti.xcom_push(key="candidates", ...)` is the
  output of `utc_normalize_rows` applied to the raw rows, not the raw rows
  themselves
- AND the value returned by `_run_select_candidates` (`return_value`) is that
  same normalized value

#### Scenario: Empty candidate list normalizes without raising

- GIVEN `get_unactioned_snapshots()` returns an empty list
- WHEN `_run_select_candidates` runs
- THEN `utc_normalize_rows` returns `[]`
- AND no exception is raised

### Requirement: Pushed candidates and derived decisions survive the real XCom serializer

The `candidates` value pushed by `_run_select_candidates`, and the `decisions`
value pushed by `_run_evaluate_candidates` (each candidate dict extended via
`**candidate` with `decision`/`views`/`median_views`/`sample_size`), MUST
survive a round trip through Airflow's real `XComEncoder`/`XComDecoder`
(`json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)`) without
raising, when the source `collected_at` carried a non-zero fixed UTC offset
before normalization. A test asserting this against a `MagicMock`-backed fake
XCom store does NOT satisfy this requirement — the round trip MUST use the
real serializer classes.

#### Scenario: Normalized candidates payload round-trips without raising

- GIVEN a `candidates` payload built by `_run_select_candidates` from rows
  with a non-zero fixed-offset `collected_at`
- WHEN the payload is serialized with `XComEncoder` and deserialized with
  `XComDecoder`
- THEN no exception is raised

#### Scenario: Derived decisions payload round-trips without raising

- GIVEN a `decisions` payload built by `_run_evaluate_candidates` from
  normalized `candidates` (each entry still carrying `collected_at`)
- WHEN the payload is serialized with `XComEncoder` and deserialized with
  `XComDecoder`
- THEN no exception is raised

### Requirement: Normalized collected_at survives round trip as real UTC, same instant

A tz-aware `collected_at` value normalized via `utc_normalize_rows` MUST
survive the `XComEncoder`/`XComDecoder` round trip as a genuine `datetime`
instance whose `.utcoffset()` equals `timedelta(0)`, representing the same
absolute point in time as the original fixed-offset value.

#### Scenario: Round-tripped collected_at is UTC and instant-preserving

- GIVEN a normalized `candidates` payload whose `collected_at` originated
  from a non-zero fixed UTC offset (e.g. `timezone(timedelta(hours=2))`)
- WHEN the payload round-trips through `XComEncoder`/`XComDecoder`
- THEN the decoded `collected_at` is a `datetime` instance
- AND its `.utcoffset()` equals `timedelta(0)`
- AND it is equal (as an instant) to the original fixed-offset value

### Requirement: Un-normalized candidates payload is pinned as failing

A `candidates` payload built from raw, un-normalized rows carrying a
non-zero fixed UTC offset in `collected_at` MUST fail the
`XComEncoder`/`XComDecoder` round trip with `ValueError` matching `"ZoneInfo
keys must be normalized relative paths"`. This scenario exists to prove the
regression guard has teeth — it MUST keep failing if normalization is ever
removed or bypassed from `_run_select_candidates`.

#### Scenario: Raw candidates payload raises ZoneInfo ValueError

- GIVEN a `candidates` payload containing raw (un-normalized) rows whose
  `collected_at` carries a non-zero fixed UTC offset
- WHEN the payload is round-tripped through `XComEncoder`/`XComDecoder`
- THEN a `ValueError` is raised matching `"ZoneInfo keys must be normalized
  relative paths"`

### Requirement: Downstream candidate/decision consumption is unaffected by normalization

`_run_evaluate_candidates`, `evaluate_action`, `_run_record_no_ops`, and
`_snapshot_age_days` MUST consume the normalized `candidates`/`decisions`
payloads with no change to decision or age-computation behavior.
`evaluate_action` never reads `collected_at`. `_snapshot_age_days` performs
only instant arithmetic (`datetime.now(tz) - collected_at`), which is
invariant under `.astimezone(UTC)`; this capability is a transport fix only
and MUST NOT alter the action decided for a candidate or the computed
snapshot age.

#### Scenario: evaluate_action decision is unchanged by normalization

- GIVEN a candidate dict whose `collected_at` was normalized from a
  non-zero fixed UTC offset, with `views`, `median_views`, `sample_size`,
  `checkpoint`, and `prior_actions` unchanged in value
- WHEN `_run_evaluate_candidates` calls `evaluate_action` for that candidate
- THEN the decided `action` literal is identical to what `evaluate_action`
  would return given the same non-`collected_at` inputs from the raw,
  un-normalized row

#### Scenario: Snapshot age in days is unchanged by normalization

- GIVEN a `collected_at` value normalized from a non-zero fixed UTC offset
  to UTC
- WHEN `_snapshot_age_days` is called with the normalized value versus the
  raw, un-normalized value
- THEN both calls return the same integer day count
