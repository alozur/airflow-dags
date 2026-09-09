# Canonical politician display names

How the curated slug → public-name catalogue
(`congress_videos/catalogs/politician_display_names.v1.json`) is scoped,
reviewed, and extended. See
[`openspec/changes/canonical-politician-display-names/design.md`](../openspec/changes/canonical-politician-display-names/design.md)
for the module design and consumer wiring; this doc only covers what changes
after that PR ships — keeping the catalogue correct.

## Ownership and scope

The catalogue is a **presentation** concern: it decides what a title or
thumbnail *renders* for a politician who has already been identified. It is
deliberately separate from identity resolution — a participant's canonical
identity is always the `congress_participants.slug` produced upstream, and
that slug never changes because of this catalogue. `canonical_display_name`
only maps an already-resolved slug to a preferred short public name; it never
participates in deciding who a speaker is.

The `congress_videos` maintainer owns the catalogue. Any change to
`politician_display_names.v1.json` needs a PR, like any other code change —
there is no separate approval process.

## The initial roster and its selection criterion

The criterion is mechanical and reproducible, not editorial: **a participant
is in the roster if they appeared at least twice across
`video_chapters.resolved_participant_slug` and
`speaker_turn_videos.resolved_participant_slug`.** Editorial judgment only
picks the *display form* for each admitted slug (e.g. a bare surname vs. a
disambiguated "First Surname") — it never decides who is admitted.

Re-derive the candidate set with:

```sql
SELECT slug, COUNT(*) AS appearances
FROM (
    SELECT resolved_participant_slug AS slug
    FROM video_chapters
    WHERE resolved_participant_slug IS NOT NULL

    UNION ALL

    SELECT resolved_participant_slug AS slug
    FROM speaker_turn_videos
    WHERE resolved_participant_slug IS NOT NULL
) appearances
GROUP BY slug
HAVING COUNT(*) >= 2
ORDER BY appearances DESC;
```

As of authoring (2026-09-09), this query returns **11 people out of only 21**
who have ever appeared with a resolved slug at all. The other 10 resolved
slugs appeared once and are correctly absent from the catalogue — one
appearance is not enough evidence that a curated short form is worth the
editorial debt it creates (see below).

## Review cadence

Review the catalogue **quarterly**, and immediately after either of these
triggers:

- **A general election.** Party leadership and prominence can shift enough
  that a previously-safe bare surname (or the editorial judgment behind it)
  is no longer accurate.
- **A cabinet reshuffle.** A minister's portfolio changing affects whether a
  short form still reads as unambiguous and current.

Both triggers exist because curated short forms are **editorial debt by
design**: `full_name` and the token-subsequence rule (below) only prevent an
*invented* shortened form, they cannot detect that a form is *stale*. Only a
human review catches that.

## How to add or change a mapping

1. Add or edit an entry in `congress_videos/catalogs/politician_display_names.v1.json`.
   Each entry needs `participant_slug`, `display_name`, `full_name`,
   `ambiguous`, `selection_note` (state the appearance count and why the
   short form is safe), and a complete `provenance` block (`publisher`,
   `reference_url`, `evidence_note`, `reviewed_on`).
2. The loader (`congress_videos/modules/politician_display_names.py`) enforces
   these invariants at load time — a violation raises `CatalogValidationError`
   and fails DAG import loudly:
   - **`full_name` subsequence rule**: the normalized `display_name` must be
     a token subsequence of the normalized `full_name`. This is what
     mechanically prevents inventing a shortened form that isn't actually
     derived from the person's real name — every token in `display_name`
     must appear, in order, inside `full_name`.
   - **No duplicate `participant_slug`** across all entries — a hard failure.
   - **No colliding normalized `display_name`** over the resolvable
     (non-`ambiguous`) set — a hard failure. Two different people must never
     render the same short name.
3. Run the catalogue test suite before opening a PR:

   ```
   uv run pytest tests/congress_videos/test_politician_display_names.py
   ```

### Worked example: the `Rodríguez` collision

`isabel-rodriguez-garcia` and `javier-rodriguez-palacios` both reduce to a
bare surname of `Rodríguez` — the collision rule rejects mapping either of
them to plain `Rodríguez`. Both are instead disambiguated with a first name
(`Isabel Rodríguez`, `Javier Rodríguez`). A third person with the same
surname, `jose-antonio-rodriguez-salas`, is deliberately **outside** the
roster (below the ≥2-appearance threshold) and simply falls back to full-name
behaviour. This is the concrete case the collision rule and the appearance
threshold exist to handle — read it before adding any new `Rodríguez`,
`García`, or other common-surname entry.

## Silent-degradation caveat

`canonical_display_name` **never raises** and the catalogue loads **lazily**
(on first call, cached per process). This means a broken bundled catalogue —
malformed JSON, a duplicate slug, a collision — does **not** fail DAG import.
It silently degrades: every call returns `None`, every consumer falls back to
its pre-existing full-name behaviour, and the failure is logged as a single
`ERROR` the first time it's hit.

Because of that silent fallback, **the bundled-catalogue CI test
(`tests/congress_videos/test_politician_display_names.py`, the case that
loads the real `politician_display_names.v1.json`) is the real gate.** It
must never be skipped or weakened — it is the only thing that turns a broken
catalogue into a loud CI failure instead of a silent, unnoticed regression in
production titles.

## What is deliberately not canonicalised

- **Mentioned people always render their full name.** The catalogue is only
  ever consulted for the resolved subject of a title, thumbnail, or short —
  never for a person merely referenced in speech.
- **A bare surname is only safe for the subject of the video.** Rendering a
  bare surname for someone who is merely mentioned would be far more likely
  to be ambiguous or misleading than for the identified speaker the content
  is actually about, which is why wiring never routes a mentioned-person slug
  through `canonical_display_name`.
