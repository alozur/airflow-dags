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
already baked into the chosen thumbnail image.

#### Scenario: Thumbnail text finding is flagged only

- GIVEN thumbnail text names a politician not present in evidence
- WHEN verification runs
- THEN a `thumbnail_text` finding is recorded and persisted, and no
  correction is attempted for that field

### Requirement: Hard-Rejection Asymmetry

A `reject` verdict MUST block publication only for the turn title, via the
existing fail-loud path. For description, thumbnail text and the entire
shorts path, a `reject` MUST be persisted and surfaced observably, and
publication MUST proceed with the existing safe fallback; this change
MUST NOT introduce a new fail-loud path for those fields/paths.

#### Scenario: Turn-title reject blocks publication

- GIVEN the verifier returns `reject` for the turn title with no
  evidence-supported correction
- WHEN the upload seam evaluates the verdict
- THEN publication is aborted via the existing raise, unchanged from today

#### Scenario: Shorts description reject does not block publication

- GIVEN the verifier returns `reject` for a short's description
- WHEN the upload seam evaluates the verdict
- THEN the verdict is persisted, surfaced via the accumulator, and the
  short still publishes using the existing fallback description

### Requirement: Fallback on Unavailable or Inconclusive Verification

If the verifier is unavailable, times out, or returns malformed output, the
system MUST preserve the existing safe publication fallback and MUST make
the outcome observable via the existing accumulator; no DB write occurs.

#### Scenario: Verifier failure preserves existing behavior

- GIVEN the verifier call fails or returns unparseable output
- WHEN the upload seam evaluates the result
- THEN the original title/description publish unchanged, no audit row is
  written, and the failure appears in the accumulator's findings

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
