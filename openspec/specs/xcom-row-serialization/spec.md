# XCom Row Serialization Specification

## Purpose

Database rows placed into an Airflow XCom payload MUST be safe to round-trip
through Airflow's real `XComEncoder`/`XComDecoder`. A `TIMESTAMPTZ` column
returned by psycopg2 as an unnamed fixed-offset `datetime` breaks that
serializer with `ValueError: ZoneInfo keys must be normalized relative
paths`. This capability defines the normalization contract for
`reap_shorts_uploader_dag.py::_generate_metadata`'s `shorts_metadata`
payload — the fourth recurrence of this defect class (#163, #303, #309).

## Requirements

### Requirement: DB row normalization at the XCom append site

For every dict sourced from a database row (`chapter`, `turn_speaker_row`)
that `_generate_metadata` places into a `shorts_metadata` entry, the system
MUST apply `utc_normalize_row` to that dict before the entry is appended to
`metadata_list` and pushed via `ti.xcom_push(key="shorts_metadata", ...)`.
A `None` row (the no-turn branch) MUST remain `None` after normalization and
MUST NOT raise.

#### Scenario: Chapter row with a tz-aware updated_at is normalized before push

- GIVEN a chapter row whose `updated_at` is a tz-aware `datetime` with a
  non-zero fixed UTC offset (e.g. `timezone(timedelta(hours=2))`)
- WHEN `_generate_metadata` appends this chapter to `metadata_list`
- THEN the appended `"chapter"` value is the output of `utc_normalize_row`
  applied to the raw row, not the raw row itself

#### Scenario: Missing turn stays None through normalization

- GIVEN a chapter with no associated `turn_id`, so `turn_speaker_row` is `None`
- WHEN `_generate_metadata` appends this chapter to `metadata_list`
- THEN the appended `"turn_speaker_row"` value is `None`
- AND no exception is raised

### Requirement: Pushed payload survives the real XCom serializer

The complete `shorts_metadata` value that `_generate_metadata` pushes MUST
survive a round trip through Airflow's real `XComEncoder`/`XComDecoder`
(`json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)`) without
raising, including when the source `chapter["updated_at"]` carried a
non-zero fixed UTC offset before normalization. A test asserting this
against the `MagicMock`-backed fake XCom store used elsewhere in this test
module (`_make_ti`) does NOT satisfy this requirement — the round trip MUST
use the real serializer classes.

#### Scenario: Normalized payload round-trips without raising

- GIVEN a `shorts_metadata` payload built by `_generate_metadata` from a
  chapter row with a non-zero fixed-offset `updated_at`
- WHEN the payload is serialized with `XComEncoder` and deserialized with
  `XComDecoder`
- THEN no exception is raised

### Requirement: Normalized offset survives round trip as real UTC

A tz-aware `updated_at` value that was normalized via `utc_normalize_row`
MUST survive the `XComEncoder`/`XComDecoder` round trip as a genuine
`datetime` instance whose `.utcoffset()` equals `timedelta(0)`.

#### Scenario: Round-tripped updated_at is UTC

- GIVEN a normalized `shorts_metadata` payload whose chapter `updated_at`
  originated from a non-zero fixed UTC offset
- WHEN the payload round-trips through `XComEncoder`/`XComDecoder`
- THEN the decoded `chapter["updated_at"]` is a `datetime` instance
- AND its `.utcoffset()` equals `timedelta(0)`

### Requirement: Un-normalized payload is pinned as failing

A `shorts_metadata` payload built from a raw, un-normalized chapter row
carrying a non-zero fixed UTC offset in `updated_at` MUST fail the
`XComEncoder`/`XComDecoder` round trip with `ValueError` matching
`"ZoneInfo keys must be normalized relative paths"`. This scenario exists to
prove the regression guard has teeth — it MUST keep failing if normalization
is ever removed or bypassed.

#### Scenario: Raw payload raises ZoneInfo ValueError

- GIVEN a `shorts_metadata` payload containing the raw (un-normalized)
  chapter row, whose `updated_at` carries a non-zero fixed UTC offset
- WHEN the payload is round-tripped through `XComEncoder`/`XComDecoder`
- THEN a `ValueError` is raised matching `"ZoneInfo keys must be normalized
  relative paths"`

### Requirement: Downstream consumption is unaffected by normalization

`_verify_final_copy` and `_trigger_youtube_upload` MUST consume the
normalized `shorts_metadata` payload with no change to verification or
publication behavior. This capability is a transport fix only; it MUST NOT
alter which fields `_copy_verification_evidence` reads or how it evaluates
them.

#### Scenario: Verification evidence is unchanged by normalization

- GIVEN a normalized chapter/turn_speaker_row pair whose
  verification-relevant fields (e.g. `mentioned_participant_slugs`, `title`,
  `resolved_participant_slug`) are unchanged in value by normalization
- WHEN `_verify_final_copy` extracts `_copy_verification_evidence` from the
  normalized payload
- THEN the extracted evidence values are identical to those that would be
  extracted from the raw, un-normalized row
