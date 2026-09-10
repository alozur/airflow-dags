# Proposal: Cover the canonical/raw display-name split in `_copy_verification_evidence`

Issue: #544 · Base: `feat/544-copy-evidence-name-split` off `main` @ 56a8180

## Intent

**Problem.** `_copy_verification_evidence` is duplicated in `congress_videos/youtube_upload_dag.py:631`
and `congress_videos/reap_shorts_uploader_dag.py:228`. Both emit a `speaker` bundle where `display_name`
is the **raw** roster name and `short_name` is the **canonical** name from `canonical_display_name` (#511).
That split is a documented contract (archived `verify-final-copy-before-publication/design.md` D5) with
**zero test coverage**: no test imports or calls either helper, and every `TestVerifyFinalCopy*` test mocks
`verify_final_copy` wholesale without inspecting `evidence`.

**Failure mode, stated accurately.** Swapping or dropping either key violates the documented contract with
nothing to catch it, and silently breaks future code or prompt wording that starts trusting the key label.
It would *not* change today's verdicts — the sole programmatic consumer, `_flatten_evidence_strings`, is
key-agnostic and yields an identical token allowlist either way. This is contract drift, not the issue's
stronger "reasons over the wrong ground truth" claim.

## Scope

### In Scope
- Direct unit tests for the helper in `tests/congress_videos/test_youtube_upload_dag.py`.
- Equivalent direct tests in `tests/congress_videos/test_reap_uploader_dag.py`.
- One parity test asserting both helpers return recursively identical key sets.
- Unmapped-slug case: `short_name is None`, `display_name` still raw — fields never conflated.

### Out of Scope
- Any production change. Both helpers are already plain top-level importable functions.
- Deduplicating the two copies into a shared module.
- `inspect.getsource()` parity — rejected: signatures differ by design (DB reads + scalar ids vs.
  pre-fetched dicts), so a source diff fails on deliberate differences instead of on what matters.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `final-copy-verification`: require that the evidence bundle keeps raw `display_name` and canonical
  `short_name` distinct, and that both DAG builders emit the same bundle shape.

## Approach

Exploration Approach 1, following the local idiom at `test_reap_uploader_dag.py:1333`
(`TestShortsCrossSeamDisplayNameConsistency`): call the real helper, patch
`congress_videos.<dag_module>.lookup_participant_by_slug` / `.canonical_display_name`, use a `MagicMock`
`db` exposing `.get_chapter_metadata`/`.get_turn_speaker_slug`. Assert on returned values only. Parity is
asserted on **produced output shape**, never on source text.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | New test class, direct helper calls |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modified | New test class + parity test |
| `congress_videos/**` | None | No production change |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Parity test over-couples to incidental shape | Med | Compare key sets recursively; no cross-file value assertions |
| Framing drifts back to the issue's stronger claim | Med | Spec states contract-drift, not wrong-ground-truth |
| Fixture roster diverges from real `participants_db` | Low | Mirror the existing `_lookup_stub(roster)` idiom |

## Rollback Plan

Revert the branch. Test-only: deleting the new test classes restores the prior state exactly. No
migration, no deploy step, no production surface touched.

## Dependencies

None. `canonical_display_name` (#511) is shipped and never raises — returns `None` on missing, unmapped,
ambiguous, or catalogue-load failure.

## Success Criteria

- [ ] Each helper is called directly by at least one test; no wholesale `verify_final_copy` mock.
- [ ] Resolvable slug: `display_name` = raw, `short_name` = canonical, and the two differ.
- [ ] Unmapped slug: `short_name is None` while `display_name` stays raw.
- [ ] Parity test fails if either bundle gains, loses, or renames a key.
- [ ] `uv run pytest` green; zero lines changed under `congress_videos/`.
