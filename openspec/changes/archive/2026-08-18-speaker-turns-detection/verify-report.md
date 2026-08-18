# Verify Report: speaker-turns-detection (issue #86)

**Verdict:** PASS WITH WARNINGS (0 CRITICAL)
**Branch verified:** feat/issue-86-speaker-turns-dag (PR1 #90 + PR2 #93)
**Mode:** Strict TDD

## Tests
- Full suite: **2083 passed, 1 skipped** (pre-existing skip, unrelated).
- Total coverage **86.96%** (gate 80%). `speaker_turns.py` **94.89%**, `speaker_turns_dag.py` 91.67%.
- e2e (`bash scripts/test-airflow-e2e.sh`): **PASS** — `list-import-errors is empty`.

## Spec completeness
All 26 spec scenarios have passing covering tests: chapter-bounded detection
without file mutation; injectable diarization; text gate + naming
(text_named 0.95 / text_confirmed 0.80 / acoustic 0.50); `confirmed_block_duration_seconds`
never used as a threshold; postprocessing (gap merge <1.0s, A→B→A ping-pong
collapse, same-name merge); idempotent `ON CONFLICT(chapter_id,start_seconds)`;
graceful degradation (missing video → skip, missing SRT → acoustic-only);
on-demand DAG `schedule=None`, not chained into youtube_upload_dag.

## Design conformance
Turn dataclass, Airflow-free `detect_turns`, isolated Docker wrapper
(`--network none`, `--memory 4g`, WAV read-only, no `shell=True`), migration
`022` (dev had 020+021). Reuse files (`vad_helpers`, `srt_helpers`,
`participants_db`, `youtube_upload_dag`) unmodified.

## Warnings (non-blocking)
- **W1** — `dag_id="speaker_turns"` (no `_dag` suffix): matches repo convention
  (`generic_thumbnail_generator`). Not a defect.
- **W2** — RESOLVED: added `test_named_phrase_but_unresolvable_name_gives_text_confirmed`
  covering the phrase-with-name-but-unresolvable branch (module cov → 94.89%).

## Post-verify quality
`/simplify` applied two cleanups (DAG conf read; test fake-runner reuse). All
tests remained green.
