# Delta for Final Copy Verification

## MODIFIED Requirements

### Requirement: Hard-Rejection Asymmetry

A `reject` verdict MUST block publication only for the turn title, via the
existing fail-loud path. For description, thumbnail text and the entire
shorts path, a `reject` MUST be persisted and surfaced observably as a
non-blocking WARNING log plus an XCom push, and publication MUST proceed
with the existing safe fallback; this change MUST NOT introduce a new
fail-loud path for those fields/paths, and such a `reject` alone MUST NOT
fail the daily upload gate.
(Previously: surfaced "via the accumulator" — folded into the same
turn-video list that also raises.)

#### Scenario: Turn-title reject blocks publication

- GIVEN the verifier returns `reject` for the turn title with no
  evidence-supported correction
- WHEN the upload seam evaluates the verdict
- THEN publication is aborted via the existing raise, unchanged from today

#### Scenario: Shorts description reject does not block publication

- GIVEN the verifier returns `reject` for a short's description
- WHEN the upload seam evaluates the verdict
- THEN the verdict is persisted, logged at WARNING and pushed to XCom,
  and the short still publishes with the existing fallback description

### Requirement: Fallback on Unavailable or Inconclusive Verification

If the verifier is unavailable, times out, or returns malformed output, the
system MUST preserve the existing safe publication fallback and MUST make
the outcome observable as a non-blocking WARNING log plus an XCom push; no
DB write occurs. On the turn-video upload path, an inconclusive verdict
alone MUST NOT fail the daily upload gate (`check_upload_failures`).
(Previously: surfaced "via the existing accumulator" — the same list
that also raises on blocking findings.)

#### Scenario: Verifier failure preserves existing behavior

- GIVEN the verifier call fails or returns unparseable output
- WHEN the upload seam evaluates the result
- THEN the original title/description publish unchanged, no audit row is
  written, and the failure is logged at WARNING and pushed to XCom

## ADDED Requirements

### Requirement: Non-Blocking Copy-Verification Findings at the Upload Gate

On the turn-video path, `check_upload_failures` MUST treat
verifier-produced findings — inconclusive verdict, description/
thumbnail-text `reject`, discarded unsupported correction, skipped
audit write, unlanded thumbnail-text regeneration — as non-blocking:
each MUST be logged at WARNING and pushed to a dedicated XCom, and none
MUST cause `check_upload_failures` to raise alone.

It MUST still raise on these blocking sources, independent of
copy-verification findings: chapter DB-write failures, videos published
without their custom thumbnail, turn-marking problems, or a missing
`copy_verification` XCom payload. A missing payload MUST raise even
alone — it is currently the only failure signal for that path (e.g.
upstream `upload_config=None` from failed turn extraction or a missing
`output_path`); this preserves existing behavior and stays out of scope
for narrowing.

When both kinds occur, the raise MUST carry only the blocking findings'
text; copy-verification findings are still logged and pushed. A clean
run MUST NOT raise, and the pushed XCom MUST reflect no findings.

#### Scenario: Soft findings alone do not fail the gate

- GIVEN the only findings on the run are copy-verification findings (for
  example an inconclusive verdict and a discarded unsupported correction)
- WHEN `check_upload_failures` runs
- THEN it does not raise, each finding is logged at WARNING, and all
  findings are pushed to the dedicated XCom

#### Scenario: Missing copy_verification payload still raises

- GIVEN the `copy_verification` XCom is missing (for example because
  `upload_config` was `None` due to failed turn extraction or a missing
  `output_path`)
- WHEN `check_upload_failures` runs
- THEN it raises, even though no other blocking source reports a problem

#### Scenario: Mixed blocking and soft findings raise with blocking text only

- GIVEN the run has both a blocking finding (for example a chapter
  DB-write failure) and a copy-verification finding
- WHEN `check_upload_failures` runs
- THEN it raises with only the blocking finding's text, and the
  copy-verification finding is still logged at WARNING and pushed to the
  dedicated XCom

#### Scenario: Clean run pushes an empty findings list

- GIVEN the run has no blocking findings and no copy-verification findings
- WHEN `check_upload_failures` runs
- THEN it does not raise and the dedicated XCom push carries an empty
  findings list
