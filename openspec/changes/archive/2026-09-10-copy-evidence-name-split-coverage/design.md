# Design: Cover the canonical/raw display-name split in `_copy_verification_evidence`

Issue #544 · Branch `feat/544-copy-evidence-name-split` off `main` @ 56a8180 · **Test-only, zero production edits.**

## Technical Approach

Both helpers are already plain top-level importable functions (exploration §8), so there is nothing to
refactor. The whole change is three named behaviours per DAG test file plus one parity test, all calling
the real helper and asserting on returned values.

Existing coverage is zero: every `TestVerifyFinalCopy*` test mocks `verify_final_copy` wholesale. We add
direct calls; we do not touch those tests.

## Architecture Decisions

### D1 — Test doubles: patch both module-level lookups with distinguishable sentinels

`lookup_participant_by_slug` and `canonical_display_name` are imported *by name* into each DAG module, so
patch targets are `congress_videos.<dag_module>.<name>` (precedent: `test_youtube_upload_dag.py:2320`).
`db` is a bare `MagicMock()` exposing only `.get_chapter_metadata` / `.get_turn_speaker_slug`, mirroring
`TestVerifyFinalCopy._patch_db`.

| Option | Tradeoff | Decision |
|---|---|---|
| Patch both, sentinel values (`"RAW Foo"` / `"CANON"`) | No data-file coupling; failure message names the swapped key | **Chosen** |
| Real curated catalogue (`pedro-sanchez-…` → `"Sánchez"`), as `TestShortsCrossSeamDisplayNameConsistency` does | Binds to reality, but couples this test to a versioned JSON catalogue and the canonical value is a *substring* of the raw one, which muddies a swap failure | Rejected |

The contract under test is **which key holds which value**, not whether the catalogue is correct — that is
already covered by `test_politician_display_names.py`, and the real-catalogue cross-seam proof stays in
`TestShortsCrossSeamDisplayNameConsistency`. No coverage is lost.

### D2 — Roster-keyed `side_effect`, defined locally per test file

`_lookup_stub(roster)` (`test_reap_uploader_dag.py:1152`) is a *dependency-injection* stub, passed as an
argument to `build_shorts_metadata_context`. `_copy_verification_evidence` takes no lookup parameter, so it
can only be reused as `side_effect=`. A roster (not `return_value=`) is **required**: `mencionados` resolves
several distinct slugs, and a single `return_value` would hand every mentioned person the speaker's name.

**Decision**: mirror the `_lookup_stub` *shape* with a small module-local helper in each test file; do not
import `_lookup_stub` across test modules. `tests/conftest.py` holds only generic infrastructure doubles
(psycopg2, openai, TaskInstance) — no domain rosters — and test files mirror DAG files 1:1. Duplicating
~6 lines beats a cross-test-module import or inflating the shared conftest.

### D3 — Parity by input equivalence, not by adapter

The two signatures differ only by the two DB reads (reap's own docstring: "minus the two DB reads it
performs internally there"). So "equivalent inputs" is literally one pair of dicts, fed directly to the
shorts helper and through a `MagicMock` db to the long-form one:

```python
db = MagicMock()
db.get_chapter_metadata.return_value = CHAPTER
db.get_turn_speaker_slug.return_value = TURN_SPEAKER
long_form = yt._copy_verification_evidence(db, chapter_id=1, turn_id=2)
shorts = reap._copy_verification_evidence(CHAPTER, TURN_SPEAKER)
```

Four patches (two per module) with the same roster. That three-line `db` wiring **is** the documented
difference — no abstraction layer is warranted for two call sites.

### D4 — Recursive key shape, leaves discarded

```python
def _key_shape(value):
    if isinstance(value, dict):
        return {k: _key_shape(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_key_shape(v) for v in value]
    return None  # leaf: values deliberately NOT compared
```

Depth reached: top level (`speaker`, `chapter`, `mencionados`), `speaker`'s 7 keys, `chapter`'s 8 keys, and
each `mencionados` entry's 4 keys. Leaves collapse to `None`, so no cross-file value coupling (proposal risk
1). List *length* is still compared — deliberate: a dropped mentioned entry is real divergence.

**The parity fixture MUST set `mentioned_participant_slugs` to two non-empty slugs.** `mencionados` is
tri-valued: `None` → the string `"no analizado"` (a leaf → `None`), `[]` → an empty list. Both compare equal
trivially and the nested 4-key comparison would prove nothing.

### D5 — The parity test lives in `test_reap_uploader_dag.py`

| Option | Tradeoff | Decision |
|---|---|---|
| `test_reap_uploader_dag.py` | Reap's docstring is what *claims* the mirror, so the guard belongs with the claimant; `TestShortsCrossSeamDisplayNameConsistency` already imports two production modules in one test, so cross-module import is the established idiom for cross-seam parity | **Chosen** |
| New `test_copy_verification_evidence_parity.py` | Creates a third home nobody reads and breaks the 1:1 mirror for one test | Rejected |
| `test_youtube_upload_dag.py` | Long-form is the source of truth, not the follower | Rejected |

## File Changes

| File | Action | Description |
|---|---|---|
| `tests/congress_videos/test_youtube_upload_dag.py` | Modify | New class: 3 direct-call tests + local roster stub |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modify | New class: 3 direct-call tests + parity test + `_key_shape` |
| `congress_videos/**` | None | Zero production lines changed |

## Testing Strategy

| # | Test (per DAG file) | Asserts |
|---|---|---|
| 1 | `resolvable_slug_splits_raw_and_canonical` | `display_name` = raw, `short_name` = canonical, and they differ |
| 2 | `unmapped_slug_keeps_raw_and_nulls_canonical` | `short_name is None`; `display_name` still raw — no fallback conflation |
| 3 | `mentioned_entries_split_raw_and_canonical` | Same D5 contract at its second call site inside the function |
| 4 | `both_helpers_emit_identical_bundle_shape` (reap file only) | `_key_shape` equality; fails on any added/dropped/renamed key |

Command: `uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/test_reap_uploader_dag.py`.
No integration or e2e layer — no DAG structure, import graph, or runtime boundary changes.

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary. Test-only change.

## Review Workload

~230 added lines, zero deletions, zero production files. **Single PR, comfortably inside the 400-line
budget** — `sdd-tasks` should not chain PRs. One work-unit commit per test file is acceptable; one commit
for the whole change is equally defensible since neither file's tests deliver the contract alone.

## Migration / Rollout

No migration required. Rollback = delete the new test classes.

## Open Questions

None.
