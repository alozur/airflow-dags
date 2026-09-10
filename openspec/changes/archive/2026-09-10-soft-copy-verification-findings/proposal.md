# Proposal: Soft Copy-Verification Findings (issue #604)

## Intent

**Problem**: `check_upload_failures` (t9, `youtube_upload_dag`) folds every
`_copy_verification_problems` finding into the blocking accumulator. The
2026-09-08T17:00 prod run turned red with only "discarded 4 unsupported
correction(s); published original copy", a designed non-blocking fallback.
By t9 the video is already published, so these findings carry no re-upload
consequence and must not fail the run on their own.

**Success**: verifier findings stay observable (WARNING log + XCom) without
failing t9. Real upload failures still fail it.

## Scope

### In Scope
- Split the single call site in `_check_upload_failures`: `payload is None` stays blocking, and verifier findings go to one `logging.warning` each plus one XCom push (key named in design, e.g. `copy_verification_findings`).
- Soft: inconclusive, description/thumbnail-text reject, discarded unsupported correction, skipped audit write, unlanded thumbnail-text regeneration.
- Blocking, unchanged: chapter DB-write failures, unpublished thumbnails, `_turn_marking_problems`, missing `copy_verification` XCom.
- Tests in `TestCheckUploadFailures`, plus a docstring update.

### Out of Scope
- `_copy_verification_problems` signature, messages, and its 9 pinned tests.
- Title-reject raise in `_verify_final_copy`.
- `reap_shorts_uploader_dag.py` (it already logs these findings and pushes them to XCom without failing).
- Alerting on the new XCom key.

## Capabilities

### New Capabilities
- None

### Modified Capabilities
- `final-copy-verification`: "Hard-Rejection Asymmetry" and "Fallback on Unavailable or Inconclusive Verification" say findings surface "via the accumulator". They need rewording to non-failing log + XCom surfacing, plus a requirement that these findings alone MUST NOT fail t9 while a missing payload still does.

## Approach

This is approach A from the exploration. Keep the helper pure. At the call site, pull the payload. If it is `None`, append the missing-XCom sentence to the blocking `problems` list. Otherwise call the helper, then log and push its output without adding it to `problems`. The t9 raise condition stays the same. This matches the shorts DAG, which logs these findings and pushes them to XCom without failing.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/youtube_upload_dag.py` (`_check_upload_failures`) | Modified | Call-site split, log + XCom |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Soft-alone, soft+blocking, missing-payload, XCom cases |
| `openspec/specs/final-copy-verification/spec.md` | Delta | Surfacing wording |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Operators lose the red-run signal for copy findings | Med | Document the WARNING + XCom channel in the PR |
| A missing XCom can happen legitimately: if t6 pushes `upload_config=None` (turn extraction failed or `output_path` missing), t6b skips its push, t7 pushes empty results and t9 still raises | Med | The decision stands. This behavior already exists today. Design should state it and may pin it with a test |

## Rollback Plan

Revert the single PR. It has no migration, no schema change and no persisted-state change.

## Dependencies

- None

## Success Criteria

- [ ] t9 does not raise when copy-verification findings are the only findings, and each finding is logged at WARNING and pushed to XCom.
- [ ] Soft findings combined with blocking ones raise with the blocking text only.
- [ ] A missing `copy_verification` XCom still raises.
- [ ] `uv run pytest` passes and the diff stays under 400 lines.
