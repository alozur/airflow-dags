# Tasks: Cover the canonical/raw display-name split in `_copy_verification_evidence`

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~230 additions, 0 deletions |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | auto-chain |
| Chain strategy | pending (not needed — single PR) |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Direct-call coverage + parity, both DAG test files, one commit | PR 1 (only) | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/test_reap_uploader_dag.py` | N/A — no DAG structure/import graph/runtime boundary touched (design.md Testing Strategy) | Delete the new test classes/helpers in both files; zero production files touched |

## Traceability

| Spec requirement / scenario | Task(s) |
|---|---|
| Req: Raw/canonical distinct — Resolvable slug scenario | 2.1, 3.1 |
| Req: Raw/canonical distinct — Unmapped slug scenario | 2.2, 3.2 |
| Req: Raw/canonical distinct — Mentioned-person scenario | 2.3, 3.3 |
| Req: Bundle shape parity — Equivalent inputs scenario | 4.1, 4.2 |
| Req: Bundle shape parity — Unresolved speaker scenario | 4.1, 4.2 |

## Phase 1: Test Fixtures (per file, no shared conftest per D2)

- [x] 1.1 In `tests/congress_videos/test_youtube_upload_dag.py`, add a local ~6-line roster-keyed lookup helper mirroring `_lookup_stub` (`test_reap_uploader_dag.py:1152`), returning a `.get(slug)`-style participant dict or `None`, for use as `side_effect=` on `lookup_participant_by_slug`.
- [x] 1.2 In `tests/congress_videos/test_reap_uploader_dag.py`, add the same local roster-keyed lookup helper (do not import `_lookup_stub` across test modules per D2). **Deviation**: the helper already exists at module level in this file (line 1152, used by `TestBuildShortsMetadataContext`); reused it directly instead of redefining a duplicate name in the same module — this satisfies D2's intent (no cross-test-module import) without a name clash.
- [x] 1.3 In `tests/congress_videos/test_reap_uploader_dag.py`, add the `_key_shape(value)` recursive helper from design.md D4 (dict → sorted-key dict of shapes, list → list of shapes, leaf → `None`).

## Phase 2: `youtube_upload_dag.py` — RED then GREEN (write assertion, prove it fails, then pass)

- [x] 2.1 RED: add `resolvable_slug_splits_raw_and_canonical` calling `_copy_verification_evidence(db, chapter_id=1, turn_id=2)` with `db` a `MagicMock()` (`.get_chapter_metadata`/`.get_turn_speaker_slug` only, per `TestVerifyFinalCopy._patch_db`), patching `congress_videos.youtube_upload_dag.lookup_participant_by_slug` (via 1.1's roster) and `.canonical_display_name` with **distinguishable sentinels** (e.g. `"RAW Foo"` vs `"CANON-X"`, never a substring pair — design D1). Assert `speaker["display_name"] == "RAW Foo"`, `speaker["short_name"] == "CANON-X"`, and the two differ. Mutation check: swap the two sentinel return values locally and confirm the assertion fails; then restore.
- [x] 2.2 RED then GREEN: add `unmapped_slug_keeps_raw_and_nulls_canonical` — `canonical_display_name` side_effect returns `None`; assert `short_name is None` and `display_name` still equals the raw sentinel (no fallback conflation). Mutation check: temporarily make the raw name fall back to the canonical value and confirm the assertion catches it.
- [x] 2.3 RED then GREEN: add `mentioned_entries_split_raw_and_canonical` using `mentioned_participant_slugs` set to **two non-empty slugs** (never `None`/`[]` — design D4/spec non-negotiable #2), with the roster-keyed side_effect resolving each slug to a distinct sentinel pair; assert each `mencionados[i]["display_name"]`/`["short_name"]` pair matches its own roster entry and no entry inherits the speaker's name. Mutation check: collapse the roster to a single shared sentinel and confirm the assertion fails.

## Phase 3: `reap_shorts_uploader_dag.py` — mirror Phase 2

- [x] 3.1 RED then GREEN: add `resolvable_slug_splits_raw_and_canonical` calling `_copy_verification_evidence(chapter, turn_speaker_row)` with plain dicts (no `db`), same sentinel/patch-target discipline as 2.1 but against `congress_videos.reap_shorts_uploader_dag.lookup_participant_by_slug`/`.canonical_display_name`.
- [x] 3.2 RED then GREEN: add `unmapped_slug_keeps_raw_and_nulls_canonical`, mirroring 2.2.
- [x] 3.3 RED then GREEN: add `mentioned_entries_split_raw_and_canonical` with the same two-non-empty-slug fixture rule as 2.3.

## Phase 4: Cross-Module Parity

- [x] 4.1 RED: in `tests/congress_videos/test_reap_uploader_dag.py`, add `both_helpers_emit_identical_bundle_shape` importing both `_copy_verification_evidence` functions (established idiom: `TestShortsCrossSeamDisplayNameConsistency`, line 1333). Build one shared `chapter`/`turn_speaker_row` dict pair, wire a `MagicMock` `db` per D3 for the long-form call, patch both modules' lookups with the same roster-keyed side_effect, call both helpers, and assert `_key_shape(long_form) == _key_shape(shorts)`. Never compare values or use `inspect.getsource()`. Mutation check: locally rename or drop one key in one function's fixture-equivalent expectation and confirm the assertion fails.
- [x] 4.2 RED then GREEN: extend 4.1 (or add a second case) with an unresolved-speaker input (`speaker_slug=None` / unmapped) for both builders; assert key-shape equality still holds even though `display_name`/`short_name` values are absent or `None`.

## Phase 5: Verification

- [x] 5.1 Run `uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/test_reap_uploader_dag.py` — all new and existing tests green.
- [x] 5.2 Run `git diff --stat` against `origin/main` and confirm only paths under `tests/` and `openspec/` appear — zero `congress_videos/**` lines changed.
- [x] 5.3 Run full `uv run pytest` (repo-wide) to confirm no regression outside the two touched files.
