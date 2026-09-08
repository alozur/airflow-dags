# Tasks: issue #499

- [x] **RED:** add a failing unit test proving a successful detail with `turn_id`, `chapter_id`, and `youtube_video_id` does not call `mark_chapter_uploaded`.
- [x] **GREEN:** make `mark_chapter_uploads` skip turn results with an observable `turn_upload` reason; retain the legacy chapter branch.
- [x] **TRIANGULATE:** add an explicit test for `turn_id=0`/non-null handling and run the focused marking suite.
- [ ] **RED:** add script tests for candidate qualification, dry-run behaviour, execute opt-in, and the repeated safe update predicate.
- [ ] **GREEN:** implement the guarded repair command with fixed allowed chapter ids and transaction semantics.
- [ ] **REFACTOR:** document invocation and remove duplication while focused tests remain green.
- [ ] **VERIFY:** run `uv run pytest tests/congress_videos/modules/test_upload_marking.py tests/congress_videos/scripts/test_repair_orphaned_turn_chapters.py` and `uv run pytest`.
- [ ] **VERIFY:** run `bash scripts/test-airflow-e2e.sh`; record pass or Docker-unavailable.
- [ ] **OPERATIONS (after merge/deploy):** dry-run the repair on production; seek exact approval for its returned candidate list before `--execute`.
