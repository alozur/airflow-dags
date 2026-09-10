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
