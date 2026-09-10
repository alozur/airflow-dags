# Exploration: Coverage for the canonical/raw display-name split in `_copy_verification_evidence`

Issue: #544 — `test(final-copy-verification): cover the canonical-vs-raw display-name split in _copy_verification_evidence`
Date: 2026-09-10
Phase: explore (read-only; grounded against `feat/544-copy-evidence-name-split` off `main` @ 56a8180)

## Current State

### 1. The function exists in both files — confirmed

- `congress_videos/youtube_upload_dag.py:631`
  ```python
  def _copy_verification_evidence(db, *, chapter_id: int | None, turn_id: int | None) -> dict:
  ```
  Performs its own two DB reads: `db.get_chapter_metadata(chapter_id)` and `db.get_turn_speaker_slug(turn_id)`,
  then `lookup_participant_by_slug(slug)` and `canonical_display_name(slug)`.

- `congress_videos/reap_shorts_uploader_dag.py:228`
  ```python
  def _copy_verification_evidence(chapter: dict | None, turn_speaker_row: dict | None) -> dict:
  ```
  Takes the chapter/turn-speaker dicts already fetched upstream by `_generate_metadata`; no DB reads of its
  own. Its docstring says it "Mirrors `_copy_verification_evidence` in `youtube_upload_dag.py` exactly,
  minus the two DB reads it performs internally there."

### 2. Correction to the issue: they are NOT identical source

The issue calls the helper "deliberately duplicated". That is true of the **returned bundle shape**, but the
**signatures differ by design** — one does DB I/O and takes scalar ids, the other takes pre-fetched dicts.

What is verified identical, by direct diff, is the returned dict: same keys, same key order, same inline
comments (`# raw`, `# canonical (#511)`).

**This determines how the parity test must be written.** A naive `inspect.getsource()` comparison would
false-positive immediately on the intentionally different signatures — precisely the trap the issue itself
warns about. Parity must be asserted on **produced output shape**, never on source text.

### 3. The evidence bundle — real keys (identical in both files)

```python
return {
    "speaker": {
        "slug": slug,
        "display_name": participant.get("display_name"),   # raw — ground-truth identity
        "short_name": canonical_display_name(slug),        # canonical (#511)
        "party": participant.get("party"),
        "parliamentary_group": participant.get("parliamentary_group"),
        "resolution_confidence": ...,
        "resolution_method": ...,
    },
    "chapter": {"title", "description", "topics", "speakers", "key_speakers",
                "scoring_reasoning", "session_number", "session_date"},
    "mencionados": mencionados,  # tri-valued: "no analizado" | [] | [{slug, display_name, short_name, party}]
}
```

`display_name` = raw, `short_name` = canonical. Confirmed against D5's own table in
`openspec/changes/archive/2026-09-09-verify-final-copy-before-publication/design.md:211-225`.

### 4. `canonical_display_name` (#511)

`congress_videos/modules/politician_display_names.py:194`. Loads a curated, versioned, provenance-required
JSON catalogue lazily (cached per process, including cached failures).

**It never raises.** It returns `None` for: missing/blank slug, unmapped slug, an `ambiguous`-flagged entry,
or a catalogue load failure. Callers must treat `None` as "fall back to the raw name" — both DAG files do.

### 5. The raw `display_name`

Comes from `lookup_participant_by_slug(slug)` in `congress_videos/modules/participants_db.py:174` — an exact
(non-fuzzy) slug match over `CongressParticipantsDB().get_all_participants()`. Returns `None` if not found,
and the field is always read via `.get("display_name")`, so it can legitimately be `None`/absent (unresolved
speaker, unmapped mentioned-participant slug).

### 6. The consumer — where the issue overstates the failure mode

Traced `evidence` into `congress_videos/modules/final_copy_verification.py`. Exactly two consumers:

- **`_flatten_evidence_strings(evidence)`**, used by `is_contained()` for the correction-containment token
  allowlist. It is **key-agnostic**: it recursively flattens every string value in the dict tree regardless
  of which key holds it. Swapping `display_name` and `short_name` would produce the **exact same** allowed
  token set.
- **`json.dumps(evidence, sort_keys=True, ...)`**, embedded verbatim as `evidence_block` into the LLM prompt
  and into `compute_content_version`'s sha256. The LLM does see the key labels, but the system prompt
  (`ai_prompts.py:958`) never tells the model to treat one key as more authoritative than the other — it
  only says "la evidencia es la única fuente de verdad."

**Conclusion.** The issue's claim that "the verifier would silently reason over the wrong ground truth" is
**stronger than the code supports**. A swap would violate the *documented data contract* (design.md D5's
explicit raw/canonical table) and would silently break any future code or prompt wording that starts
trusting the key label — but it would very likely **not** change today's verdicts, because both the full
name and the short name remain present in the evidence JSON either way.

The real failure mode is **documented-contract drift with no test to catch it**. The coverage gap is
genuine; the spec should state the failure mode precisely rather than repeat the issue's stronger framing.

### 7. Existing test layout

- `tests/congress_videos/test_youtube_upload_dag.py` (`_make_ti` fixture at line 20) and
  `tests/congress_videos/test_reap_uploader_dag.py` both have `TestVerifyFinalCopy(Shorts)` classes, but
  **every one of those tests mocks `final_copy_verification.verify_final_copy` wholesale** and never
  inspects the `evidence` dict passed to it. Grep confirms: zero `evidence=` assertions, zero direct
  imports or calls of `_copy_verification_evidence` anywhere in the suite. **Prior coverage is zero.**
- The established local idiom for canonical/raw cross-seam assertions is
  `TestShortsCrossSeamDisplayNameConsistency` (`test_reap_uploader_dag.py:1333`): call the real helpers
  directly with a small `_lookup_stub(roster)` fixture and assert on returned values.
- `lookup_participant_by_slug` and `canonical_display_name` are imported by name into each DAG module, so
  the patch targets are `congress_videos.youtube_upload_dag.lookup_participant_by_slug` / `.canonical_display_name`
  and the `reap_shorts_uploader_dag` equivalents (existing patches at `test_youtube_upload_dag.py:2320,4081`).

### 8. Is this really test-only? YES — checked first, as the highest-risk unknown

Both functions are **plain top-level module functions** — unindented, not closures nested inside a
DAG-building function or a `with DAG(...) as dag:` block. They are freely importable today:

```python
from congress_videos.youtube_upload_dag import _copy_verification_evidence
```

**No production code change is required.**

## Affected Areas

- `tests/congress_videos/test_youtube_upload_dag.py` — new test class calling `_copy_verification_evidence`
  directly (`db` as a MagicMock exposing `.get_chapter_metadata`/`.get_turn_speaker_slug`; patch
  `lookup_participant_by_slug`).
- `tests/congress_videos/test_reap_uploader_dag.py` — new test class calling it directly with plain
  `chapter`/`turn_speaker_row` dicts.
- **No production files change.**

## Approaches

### 1. Per-file unit tests + one output-shape parity test (RECOMMENDED)

For each DAG file: call the helper directly with a resolvable slug and assert
`["speaker"]["short_name"]` is the canonical value while `["speaker"]["display_name"]` is the raw roster
value and the two differ; repeat with an unmapped slug (`short_name is None`, `display_name` still raw) to
prove the fields are never conflated. Then one test building equivalent fixtures for both files and
asserting the two returned dicts have identical keys, recursively.

- **Pros**: maps directly onto all four acceptance criteria; catches swap, drop, and shape divergence;
  reuses the existing idiom; zero production risk.
- **Cons**: three named behaviours rather than one.
- **Effort**: Low.

### 2. `inspect.getsource()` text-diff parity — NOT RECOMMENDED

- **Cons**: the two functions have intentionally different signatures and docstrings, so a source diff fails
  on harmless differences instead of on what matters. Scoping it to the `return {...}` block via string
  slicing would be brittle to reformatting.

### 3. Single shared parametrized test module

- **Cons**: the differing call signatures force a per-module adapter anyway, adding indirection for little
  gain across only two call sites; and no shared cross-DAG test module exists today — test files mirror DAG
  files 1:1, consistent with the codebase's no-cross-import convention.

## Recommendation

**Approach 1**, with the spec stating the failure mode as *"violates the documented raw/canonical contract,
with no test to catch it"* rather than the issue's *"silently reasons over the wrong ground truth"*.

## Risks

- None to production — genuinely test-only; both target functions are already importable.
- **Framing risk**: if the spec copies the issue's stronger claim verbatim, a future reviewer tracing the
  actual consumer will reasonably push back. State the real failure mode.
- `db` in the long-form signature is untyped; a test double needs only `.get_chapter_metadata()` and
  `.get_turn_speaker_slug()`, satisfied by a `MagicMock()`, consistent with `TestVerifyFinalCopy._patch_db`.

## Ready for Proposal

Yes. Scope is small, self-contained and test-only; every code citation in the issue has been checked against
real code, with one clarifying correction on the consumer's actual behaviour.
