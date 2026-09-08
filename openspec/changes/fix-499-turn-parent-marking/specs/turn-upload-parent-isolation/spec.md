# Turn Upload Parent Isolation Specification

## Requirement: turn result does not publish parent chapter

When `mark_chapter_uploads` receives a successful upload detail with a non-null `turn_id`, it SHALL NOT call `db.mark_chapter_uploaded`, even when the detail also contains a valid `chapter_id` and `youtube_video_id`.

#### Scenario: successful turn result

- **GIVEN** a successful detail containing `turn_id`, `chapter_id`, and `youtube_video_id`
- **WHEN** chapter marking runs
- **THEN** no chapter-upload write occurs
- **AND** the result records the detail as skipped because it is a turn upload.

## Requirement: legacy chapter result remains supported

A successful upload detail without a `turn_id`, but with `chapter_id` and `youtube_video_id`, SHALL mark its chapter uploaded.

#### Scenario: successful legacy chapter result

- **GIVEN** a successful detail with a chapter id and YouTube id but no turn id
- **WHEN** chapter marking runs
- **THEN** it calls `db.mark_chapter_uploaded(chapter_id, youtube_video_id)` exactly once.

## Requirement: guarded retrospective repair

The repair command SHALL default to dry-run and only qualify chapters 263, 264, 265, 266, and 519 that meet all of these conditions:

1. The chapter is marked uploaded and has a non-null YouTube id.
2. At least one of its turn-video rows is uploaded with that same YouTube id.
3. At least one sibling turn-video row is prepared, not uploaded, and not abandoned.

On `--execute`, it SHALL reset all three parent upload fields (`is_uploaded_to_youtube`, `youtube_video_id`, `youtube_upload_date`) only for rows still satisfying those predicates inside the update transaction.

#### Scenario: true legacy chapter is protected

- **GIVEN** a marked chapter with no uploaded sibling turn carrying its YouTube id
- **WHEN** the repair preflight runs
- **THEN** it is not a repair candidate and is never updated.

#### Scenario: orphaned prepared sibling becomes eligible

- **GIVEN** a qualified chapter with a pending prepared sibling turn
- **WHEN** the repair executes
- **THEN** the parent chapter is no longer marked uploaded
- **AND** the sibling can satisfy `uploadable_turns`' parent-upload gate.
