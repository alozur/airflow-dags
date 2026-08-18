# Speaker-Turn Detection Specification

## Purpose

Detect and persist named speaker-turn boundaries **within** existing publishable
`video_chapters` (15–45 min thematic segments) so that downstream products
(best-moment selection #17, per-speaker materialization #88) can consume
resolved sub-turns without video cutting or re-encoding. Signal fusion — acoustic
boundaries from pyannote diarization gated and named by SRT president-announcement
phrases — provides precision-first attribution. The pipeline is fully additive:
no existing DAG, table, or module is mutated.

---

## Requirements

### Requirement: Chapter-Bounded Turn Detection Without Video Mutation

The system MUST read publication-intended `video_chapters` rows (carrying
`chapter_id`, `video_id`, `session_date`, `start_time`, `end_time`) and detect
speaker-turn boundaries within each chapter's time window. The system MUST NOT
cut, re-encode, move, rename, or otherwise mutate any video or audio file as
part of turn detection. Audio extraction for diarization MUST be transient and
scoped to the chapter window only.

#### Scenario: Turn detection reads chapter bounds and leaves files intact

- GIVEN a publishable `video_chapters` row with a known `video_id`, `start_time`,
  and `end_time`
- WHEN the turn-detection pipeline runs for that chapter
- THEN speaker-turn rows are persisted in `speaker_turns` for the chapter
- AND no video or audio file is created, modified, moved, or deleted in the
  source-video storage location

#### Scenario: Diarization is bounded to the chapter window

- GIVEN a chapter that covers minutes 30–75 of a three-hour session video
- WHEN diarization runs for that chapter
- THEN the acoustic analysis input covers only the 45-minute chapter window
- AND the full-session audio is never passed to the diarizer

---

### Requirement: Injectable Diarization Backend

Turn boundaries MUST be produced by a pyannote diarization call that is supplied
via an injectable `diarize_fn` parameter. The default production implementation
MUST invoke pyannote as an isolated Docker subprocess (`--network none`, 4 GiB
memory cap). Tests MUST be able to substitute a stub `diarize_fn` that returns
pre-built boundary lists without Docker, network access, or GPU. The production
diarizer MUST NOT be imported into the Airflow image.

#### Scenario: Test stub replaces Docker diarizer

- GIVEN a `diarize_fn` stub is injected that returns a fixed list of
  `(start_secs, end_secs, speaker_label)` tuples
- WHEN the speaker-turn module processes a chapter
- THEN the pipeline completes without launching a Docker subprocess
- AND the resulting `speaker_turns` rows reflect the stub boundaries

#### Scenario: Production diarizer runs isolated

- GIVEN no `diarize_fn` override is provided
- WHEN the speaker-turn module processes a chapter in production
- THEN a Docker subprocess is launched for the pyannote container with
  `--network none` and a 4 GiB memory cap
- AND pyannote is not imported into the Airflow worker process

---

### Requirement: Text Gate and Name Resolution via President-Announcement Phrases

For each acoustic boundary detected at time `t`, the system MUST search SRT
blocks in the window `[t − 30 s, t + 30 s]` for a president-announcement phrase
of the form "Tiene la palabra el señor / la señora X". This text signal MUST
serve both as a confirmation gate (accepts or rejects the acoustic change) and
as the primary name source. When no announcement phrase is found, the system
MUST fall back to `lookup_participant_fuzzy` for name resolution and MUST record
the turn with a lower confidence value. The system MUST NOT use
`confirmed_block_duration_seconds` as an acceptance threshold.

#### Scenario: Announcement phrase confirms and names an acoustic change

- GIVEN an acoustic boundary at time `t`
- AND an SRT block in `[t − 30 s, t + 30 s]` contains the phrase
  "Tiene la palabra la señora García"
- WHEN signal fusion runs
- THEN the turn is persisted with `source = text_named`, `resolved_name` set to
  the fuzzy-resolved participant name for "García", and a higher confidence value

#### Scenario: Announcement phrase is absent — fuzzy fallback fires

- GIVEN an acoustic boundary at time `t`
- AND no SRT block in `[t − 30 s, t + 30 s]` contains an announcement phrase
- WHEN signal fusion runs
- THEN `lookup_participant_fuzzy` is invoked with the acoustic speaker label
- AND the turn is persisted with `source = acoustic`, `resolved_name` set to
  the fuzzy result (nullable), and a lower confidence value

#### Scenario: Announcement phrase rejects a noisy acoustic change

- GIVEN an acoustic boundary detected at time `t`
- AND the SRT window contains no announcement phrase
- AND postprocessing determines the boundary is a same-speaker pitch shift
  (the acoustic segment on both sides resolves to the same speaker label)
- WHEN signal fusion and postprocessing run
- THEN the acoustic change is not persisted as a distinct turn

#### Scenario: confirmed_block_duration_seconds is never consulted

- GIVEN any acoustic boundary regardless of its duration in seconds
- WHEN signal fusion evaluates the boundary
- THEN the acceptance or rejection decision is made without consulting
  `confirmed_block_duration_seconds`

---

### Requirement: Postprocessing Pipeline

The system MUST apply four postprocessing steps in order after raw acoustic
boundaries are produced and before persistence:

1. **Gap merge.** Adjacent segments attributed to the same speaker label with a
   gap of less than 1.0 s MUST be merged into one turn.
2. **Ping-pong collapse.** An A → B → A pattern where the total B duration falls
   within a short window (to be specified in design, derived from Fase 0 data)
   MUST be collapsed into a single A turn.
3. **Text gate.** Each surviving acoustic change MUST be evaluated against the
   SRT announcement window as specified in the Text Gate requirement above.
4. **Same-name merge.** After name resolution, adjacent turns whose `resolved_name`
   values are identical MUST be merged into one turn.

#### Scenario: Sub-second gap between same-speaker segments is merged

- GIVEN two consecutive acoustic segments attributed to speaker label "SPEAKER_01"
  separated by a 0.7 s gap
- WHEN the gap-merge step runs
- THEN the two segments are merged into one turn
- AND no turn boundary is recorded at the 0.7 s gap

#### Scenario: Ping-pong A→B→A is collapsed

- GIVEN three consecutive turns: A (60 s), B (4 s), A (90 s)
  where B's duration falls within the ping-pong collapse window
- WHEN the ping-pong collapse step runs
- THEN the three segments are collapsed into one A turn
- AND the B segment is not persisted

#### Scenario: Adjacent turns with the same resolved name are merged

- GIVEN two consecutive acoustic segments attributed to different speaker labels
  ("SPEAKER_00" and "SPEAKER_02") that both resolve to the same
  `resolved_name` "María Luisa García"
- WHEN the same-name merge step runs
- THEN the two segments are merged into one turn with
  `resolved_name = "María Luisa García"`

---

### Requirement: Idempotent Persistence in speaker_turns Table

The system MUST store each detected sub-turn in a new `speaker_turns` table
with columns `turn_id` (PK), `chapter_id` (FK → `video_chapters`),
`start_seconds` (numeric), `end_seconds` (numeric), `speaker_label` (text),
`resolved_name` (text, nullable), `confidence` (numeric 0–1), and
`source` (enum: `acoustic`, `text_confirmed`, `text_named`). The table MUST
enforce `UNIQUE(chapter_id, start_seconds)`. Every upsert MUST use
`ON CONFLICT (chapter_id, start_seconds) DO UPDATE` so that re-running the
pipeline updates existing rows and never creates duplicates.

The migration file MUST be the next sequential number above the current highest
migration on the integration branch at the time of tasks execution (highest + 1,
**≥ 021**). The exact number MUST be re-verified during the tasks phase against
`congress_videos/sql/migrations/` on the `dev` branch before the file is created.

#### Scenario: Idempotent re-run updates without duplicating rows

- GIVEN a chapter has already been processed and its turns are in `speaker_turns`
- WHEN the pipeline runs again for the same chapter
- THEN the row count for that `chapter_id` is unchanged
- AND `start_seconds`, `end_seconds`, `speaker_label`, `resolved_name`,
  `confidence`, and `source` values are updated to reflect the latest run

#### Scenario: source values are constrained to the declared enum

- GIVEN a turn row is about to be persisted
- WHEN `source` is set
- THEN its value is exactly one of: `acoustic`, `text_confirmed`, `text_named`
- AND no other value is accepted by the database constraint

---

### Requirement: Graceful Degradation

The system MUST handle missing inputs without failing the DAG run.

**Missing source video.** When `_find_source_video` returns no result for a
chapter, the system MUST log a warning, skip that chapter, and continue
processing remaining chapters. The DAG run MUST succeed.

**Missing SRT.** When `find_srt_for_chapter` returns no SRT path for a chapter,
the system MUST run acoustic-only processing. Persisted turns MUST have
`source = acoustic` and a lower confidence value than text-confirmed turns.
The DAG run MUST succeed.

#### Scenario: Missing source video — chapter skipped, DAG succeeds

- GIVEN a chapter references a `video_id` for which no source video file exists
  on the worker's storage
- WHEN the pipeline processes that chapter
- THEN a warning is logged identifying the chapter
- AND no `speaker_turns` rows are written for that chapter
- AND the DAG task succeeds and continues to the next chapter

#### Scenario: Missing SRT — acoustic-only rows with lower confidence

- GIVEN a chapter has a locatable source video
- AND `find_srt_for_chapter` returns no SRT path for that chapter
- WHEN the pipeline processes that chapter
- THEN speaker-turn rows are persisted with `source = acoustic`
- AND each row's `confidence` value is below the threshold used for
  text-confirmed turns
- AND `resolved_name` is nullable (may be null when fuzzy lookup also fails)
- AND the DAG task succeeds

---

### Requirement: On-Demand Standalone DAG

A DAG named `speaker_turns_dag` with `schedule=None` MUST be created as an
independent, triggerable pipeline following the `generic_thumbnail_generator_dag`
pattern. Each DAG run MUST process at most a configured LIMIT of
publication-intended chapters (from the `uploadable_chapters` view or equivalent
query) and MUST NOT self-trigger subsequent batches. Re-queuing is handled
externally by the operator re-triggering the DAG. The DAG MUST NOT be chained
into `youtube_upload_dag` or any other existing DAG.

#### Scenario: DAG loads without import errors

- GIVEN the `speaker_turns_dag.py` file is present in the DAGs folder
- WHEN Airflow loads all DAGs
- THEN `speaker_turns_dag` is registered with no import errors
- AND no existing DAG is modified

#### Scenario: DAG run processes at most LIMIT chapters

- GIVEN the `uploadable_chapters` view returns 50 eligible chapters
  and the configured LIMIT is 10
- WHEN the DAG is triggered without additional conf overrides
- THEN exactly 10 chapters are processed in the run
- AND the remaining 40 chapters are not processed in that run

#### Scenario: DAG is not chained into youtube_upload_dag

- GIVEN `youtube_upload_dag.py` in its post-change state
- WHEN the DAG file is inspected
- THEN it contains no reference to `speaker_turns_dag` or `speaker_turns`
  tasks as upstream or downstream dependencies

---

### Requirement: Reuse of Existing Helpers Without Modification

The system MUST reuse the following existing components without changing their
signatures, behavior, or test coverage:

- `extract_audio_wav(video_path, wav_path, start_secs, duration_secs)` from
  `congress_videos/modules/vad_helpers.py`
- `_find_source_video(target_date, video_id)` from
  `congress_videos/modules/vad_helpers.py`
- `find_srt_for_chapter(video_id, chapter_id, session_date)` from
  `congress_videos/srt_helpers.py`
- `_parse_srt_blocks(path)` from `congress_videos/srt_helpers.py`
- `lookup_participant_fuzzy(name)` from
  `congress_videos/modules/participants_db.py`

The new `speaker_turns` module MUST call these functions through their existing
public interfaces. No new parameter, return-type change, or behavioral change to
these functions is permitted as part of this change.

#### Scenario: Helper functions are called unchanged

- GIVEN the post-change versions of `vad_helpers.py`, `srt_helpers.py`, and
  `participants_db.py`
- WHEN their test suites run
- THEN all pre-existing tests pass without modification
- AND no new parameter or return-type change is introduced by this change

---

### Requirement: Test Coverage with Injectable Boundaries

The system MUST provide unit tests for the `speaker_turns` module that exercise
turn detection, signal fusion, each postprocessing step, persistence, and
graceful degradation using a stub `diarize_fn`. Tests MUST run without Docker,
without network access, and without a live database. A DAG-load test MUST verify
that `speaker_turns_dag` registers without import errors.

#### Scenario: All postprocessing scenarios are covered by unit tests

- GIVEN stub `diarize_fn` returning controlled boundary sequences
- WHEN the unit-test suite runs
- THEN tests assert correct output for: gap merge (gap < 1.0 s), gap non-merge
  (gap ≥ 1.0 s), ping-pong collapse, same-name merge, missing-video skip,
  missing-SRT acoustic-only output, text-gate acceptance, and text-gate
  rejection (noise)

---

### Requirement: Scope Boundaries

The system SHALL NOT implement video cutting, clip materialization (#88), silence
or applause trimming (#87), recall measurement, best-moment selection (#17), or
diarization confidence recall tuning. The `youtube_upload_dag` DAG MUST remain
unchanged in structure and dependency graph. No existing table schema or module
public interface is permitted to change as part of this delivery.

#### Scenario: Deferred scope remains excluded

- GIVEN implementation planning for this change
- WHEN work is selected
- THEN it excludes video cutting, clip materialization, silence trimming,
  applause detection, recall measurement, `youtube_upload_dag` modifications,
  and any schema or interface change to existing modules
