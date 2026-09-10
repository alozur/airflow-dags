# Delta for Final Copy Verification

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
