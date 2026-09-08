# Proposal: prevent turn uploads from publishing their parent chapter

## Problem

Issue #499: a successful speaker-turn upload carries `chapter_id` for tracking and is incorrectly processed as a full-chapter upload. The parent is marked uploaded, so `uploadable_turns` hides its pending sibling turns forever.

## Scope

### In scope

- Treat a successful upload detail containing `turn_id` as a turn-only result in `mark_chapter_uploads`.
- Preserve successful legacy chapter marking when the detail has no `turn_id`.
- Add a repeatable, guarded backfill command for the documented affected chapters (263–266 and 519).
- Cover both marking branches and the backfill qualification/SQL contract with tests.

### Out of scope

- Altering the `uploadable_turns` eligibility rule.
- Re-enabling chapter selection in the production uploader.
- Changing turn upload/thumbnail marking.
- Executing the production repair without a separate, exact approval.

## Success criteria

1. Publishing a turn does not update its parent chapter.
2. The legacy chapter path still writes the chapter upload state.
3. The repair only resets a documented chapter when an uploaded sibling turn carries the same YouTube id as the parent and a pending sibling exists.
4. A genuine legacy chapter upload is never selected for repair.
5. Focused tests and `uv run pytest` pass.

## Delivery

One review-bounded bug-fix PR linked to #499. Production repair runs only after merge/deploy and a read-only preflight confirms the guarded target set.
