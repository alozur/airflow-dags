# Delta for Final Copy Verification

## MODIFIED Requirements

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
