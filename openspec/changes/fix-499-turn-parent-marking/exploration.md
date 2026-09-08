# Exploration: isolate turn uploads from parent chapter state

## Source

GitHub issue #499, approved by repository maintainer `alozur` on 2026-09-08.

## Observed flow

`youtube_upload_dag` selects only `item_type="turn"`. Its upload config deliberately carries both `turn_id` and `chapter_id` for tracking. After the child uploader returns, the DAG runs `mark_chapter_uploads` and `mark_turn_uploads` in parallel. The former currently treats every successful detail containing `chapter_id` as a whole-chapter upload.

Therefore a successful turn marks both its own `speaker_turn_videos` rows and its parent `video_chapters` row. `uploadable_turns` correctly excludes `vc.is_uploaded_to_youtube = TRUE` for legacy full-chapter uploads, but this unintended parent mark hides pending siblings.

## Production read-only audit

A read-only query from `airflow-scheduler-prod` established that chapters 263–266 and 519 all have:

- `video_chapters.is_uploaded_to_youtube = TRUE`;
- one or more uploaded turn-video rows whose YouTube id equals the parent chapter's YouTube id; and
- still-pending, non-abandoned turn-video rows.

This matching YouTube-id evidence distinguishes these rows from a genuine pre-#171 whole-chapter upload. In particular, chapter 519 has uploaded turn 323 and pending prepared turns 320–322, including turn 321. The same causal pattern exists in chapters 263–266.

## Constraints

- Preserve the legacy chapter branch: a successful detail without `turn_id` must still invoke `mark_chapter_uploaded`.
- Keep the existing failure-recording behaviour unchanged.
- The live repair must be explicitly targeted and transactionally guarded. It must clear the erroneous chapter upload fields together: `is_uploaded_to_youtube`, `youtube_video_id`, and `youtube_upload_date`.
- The production mutation is deferred until its exact qualifying set has been shown and separately approved.

## Next phase

`proposal` → `spec` → `design` → strict-TDD implementation.
