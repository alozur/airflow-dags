# Design: isolate marking and repair proven parent pollution

## Decision 1: use `turn_id` as the branch discriminator

`upload_details` already has `turn_id` only for the turn path. `mark_chapter_uploads` therefore skips every successful detail whose `turn_id is not None`; it does not infer type from `chapter_id`, which is intentionally present on both paths. The turn-marker remains the sole owner of the turn write.

**Alternative rejected:** remove `chapter_id` from turn configs. Other downstream tracking (including thumbnail backfill) relies on it, and changing that envelope is wider and less reliable than enforcing the boundary at the chapter-marking seam.

## Decision 2: explicit result detail for skipped turns

The chapter marker records `{status: "skipped", reason: "turn_upload"}` for observability. It does not count this as a failed update or record a chapter failure.

## Decision 3: standalone guarded repair command

A Python command under `scripts/` uses `PostgresConnection`, defaults to read-only dry-run, and requires `--execute` for mutation. It has a fixed issue-derived chapter allowlist and one parameterized candidate query. The execute query repeats the qualification predicates while locking/updating, so a changed row between dry-run and write is not reset accidentally.

The three parent fields are cleared together. Retaining a YouTube id/date while claiming the chapter was not uploaded would create a contradictory lifecycle record; the matched turn-video rows remain the authoritative publication record.

## Qualification predicate

```sql
vc.chapter_id = ANY(%s)
AND vc.is_uploaded_to_youtube = TRUE
AND vc.youtube_video_id IS NOT NULL
AND EXISTS (uploaded sibling with stv.youtube_video_id = vc.youtube_video_id)
AND EXISTS (prepared, unuploaded, non-abandoned sibling)
```

The equal YouTube id ties the parent mark to an actual turn upload. A genuine old full-chapter upload lacks that evidence and cannot qualify.

## Rollout

1. Merge and deploy the code fix first, preventing new pollution.
2. Run the repair in dry-run mode on production and compare the returned ids with the approved list.
3. Obtain explicit authorization for that exact output.
4. Run `--execute`, then query `production.uploadable_turns` to verify the pending prepared turns are visible.

Rollback of code is a normal Git revert. The repair command reports candidates before writing; an operational rollback would reapply the recorded three parent fields only from the preflight receipt, never guess them.
