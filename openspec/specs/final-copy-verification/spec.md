# Final Copy Verification Specification

## Purpose

Before a turn video or short is published to YouTube, an independent LLM
verifier checks the generated title, description and thumbnail text against
trusted evidence (speaker, mentioned people, party, topics). It corrects
title/description within bounds, flags unsupported thumbnail text, and
leaves an auditable record of the verdict.

## Requirements

### Requirement: Independent Verification Call

The system MUST verify final copy with an LLM call distinct from the title,
thumbnail-text and description generators. The call MUST receive title,
thumbnail text and description as separate named fields plus the trusted
evidence context (speaker slug/display name, mentioned people, party,
topics), and MUST check politician names, party names, spelling, grammar
and Spanish language use.

#### Scenario: Verifier runs after generation, before publication

- GIVEN a turn's title, description and thumbnail text have been generated
- WHEN the upload seam prepares to publish
- THEN a verifier call runs with title, description and thumbnail text as
  separate fields plus speaker/party/topic evidence, before the video is
  handed to the publish step

### Requirement: Verdict and Findings Schema

The verifier's result MUST distinguish `pass`, `correctable` and `reject`
and MUST include field-level findings (`field`, `category`, `detail`) for
any issue found.

#### Scenario: Consistent copy passes with no findings

- GIVEN title, description and thumbnail text are accurate per evidence
- WHEN verification runs
- THEN the verdict is `pass` with an empty findings list

### Requirement: Bounded Correction Constrained to Evidence

The system MUST correct only `title` and `description`, at most one
correction round, followed by a mandatory recheck of the corrected text.
A correction MUST be derivable from the supplied evidence and MUST NOT
invent identities, party affiliations or claims; an unsupported correction
MUST be discarded and the original value published.

#### Scenario: Politician-name correction is applied and rechecked

- GIVEN the title misspells a politician's name present in evidence
- WHEN verification returns `correctable` with a name finding
- THEN the corrected title is applied, the rechecked copy passes, and the
  corrected title is what publishes

#### Scenario: Unsupported claim correction is discarded

- GIVEN the description asserts a claim absent from evidence and no
  evidence-backed correction is available
- WHEN verification flags the claim
- THEN no correction is written and the original description publishes

### Requirement: Party Mismatch Detection Avoids Free-Text Variant False Positives

The system MUST flag a party finding only when copy names a party that
contradicts the evidence, not when it merely differs in abbreviation,
regional-federation naming or formatting. Uncertain cases MUST resolve to
no finding.

#### Scenario: True positive — contradicting party

- GIVEN evidence shows the speaker's party is PSOE
- WHEN the copy states the speaker belongs to VOX
- THEN verification returns a `party_name` finding for that field

#### Scenario: False-positive avoidance — same-party variant

- GIVEN evidence shows the speaker's party as `PSE-EE (PSOE)`
- WHEN the copy states the speaker's party as PSOE
- THEN verification returns no party finding

### Requirement: Thumbnail Text Flagged Without Correction

Thumbnail text MUST be verified but never auto-corrected, because it is
already baked into the chosen thumbnail image. On the long-form upload path,
a `thumbnail_text` finding MUST also drive at most one bounded, non-blocking
regeneration attempt, governed entirely by the `thumbnail-text-regeneration`
capability. The verifier itself MUST NOT be changed to correct the field, and
triggering a regeneration MUST NOT block or delay publication.
(Previously: a `thumbnail_text` finding was recorded and persisted with no
downstream effect beyond the audit row.)

#### Scenario: Thumbnail text finding is flagged only

- GIVEN thumbnail text names a politician not present in evidence
- WHEN verification runs
- THEN a `thumbnail_text` finding is recorded and persisted, and no
  correction is attempted for that field by the verifier itself

#### Scenario: Long-form finding drives a bounded downstream regeneration

- GIVEN a long-form turn's verification returns a `thumbnail_text` finding
- WHEN the upload path evaluates the verdict
- THEN a bounded regeneration attempt is claimed and triggered per the
  `thumbnail-text-regeneration` capability, without the verifier itself
  producing a corrected value for that field

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

### Requirement: Audit Persistence

Verdict, original value, corrected value (if any) and findings MUST persist
to the corresponding row in a bounded, idempotent write keyed to the
verified content. No write occurs when the verdict is inconclusive or a
correction is unsupported.

#### Scenario: Correction persists with both original and corrected values

- GIVEN a correctable title finding produced an evidence-backed correction
- WHEN the audit write runs
- THEN the row records the verdict, original title, corrected title and
  findings, retrievable after publication

#### Scenario: Idempotent re-run does not duplicate the audit write

- GIVEN a verified row already has an audit record for this content
- WHEN the same verification step is retried for the same content
- THEN the write updates the same row without creating a duplicate record


## ADDED Requirements

### Requirement: Speaker Evidence Keeps Raw and Canonical Names Distinct

The evidence bundle built for the final-copy verifier MUST carry the
speaker's raw roster name and canonical curated name in two distinct,
never-interchanged fields: `display_name` (raw, ground-truth identity from
the participant roster) and `short_name` (canonical curated form). Neither
field MUST be dropped, renamed, or substituted for the other. The same
raw/canonical pairing MUST hold for each entry under `mencionados` that
carries a resolvable slug.

Dropping either field, or having one silently take the other's value, is a
contract violation: it breaks the documented data contract and can silently
mislead any future code or prompt wording that begins trusting the key
label, even where today's verifier output is unaffected.

#### Scenario: Resolvable slug keeps raw and canonical names apart

- GIVEN a speaker slug resolves to a roster participant with a curated
  canonical name
- WHEN the evidence bundle is built
- THEN `speaker.display_name` holds the raw roster name, `speaker.short_name`
  holds the canonical name, and the two values differ

#### Scenario: Unmapped slug does not conflate the two fields

- GIVEN a speaker slug resolves to a roster participant but has no entry in
  the canonical name catalogue
- WHEN the evidence bundle is built
- THEN `speaker.short_name` is `None` while `speaker.display_name` still
  holds the raw roster name, proving neither field falls back to the other

#### Scenario: Mentioned-person entries follow the same pairing

- GIVEN a `mencionados` entry carries a resolvable slug
- WHEN the evidence bundle is built
- THEN that entry's `display_name` and `short_name` are populated
  independently, following the same raw/canonical split as `speaker`

### Requirement: Evidence Bundle Shape Parity Across Upload Paths

Both evidence-bundle builders — the turn-video upload path and the shorts
upload path — MUST produce a bundle with the same recursive key structure
for equivalent inputs, regardless of the two builders' differing call
signatures (one reads from the database by scalar id, the other accepts
pre-fetched dicts).

#### Scenario: Equivalent inputs yield matching bundle shape

- GIVEN both builders are given inputs describing the same chapter, turn and
  speaker resolution outcome
- WHEN each builder produces its evidence bundle
- THEN the two bundles have identical keys at every level of nesting

#### Scenario: Unresolved speaker still yields matching bundle shape

- GIVEN both builders are given inputs where the speaker slug is missing or
  unmapped
- WHEN each builder produces its evidence bundle
- THEN the two bundles still have identical keys at every level of nesting,
  even though `display_name` and `short_name` values are absent or `None`
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
