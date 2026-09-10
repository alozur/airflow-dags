"""
Congress YouTube Chapter Uploader DAG

Publishes one speaker turn per day to YouTube. Runs at 19:00 UTC; the daily
long-form cap is DAILY_LONG_FORM_UPLOAD_LIMIT = 1.

Selection is turn-only: _run_get_uploadable_item calls
db.get_uploadable_turns(limit=1), which runs `SELECT * FROM uploadable_turns
LIMIT 1` against production.uploadable_turns with no external ORDER BY. The
view's own ordering therefore decides who takes the single daily slot
(migration 044):

    COALESCE(interest_score, 1) DESC,
    relevance_score DESC,
    session_date DESC,
    materialized_at ASC,   -- FIFO tie-break (issue #328)
    turn_id ASC            -- total-order backstop

uploadable_turns admits a turn whose video is materialized and prepared, not
uploaded, not abandoned, whose source chapter has relevance_score >= 2 and is
itself not uploaded, which is not procedural (issue #143), and whose published
duration clears 300 seconds (issue #234).

Naming: the task ids extract_chapter_videos / mark_chapters_uploaded, the XCom
keys chapter_extraction_results / chapter_upload_updates and this DAG's own id
keep the word "chapter" deliberately. A task id and an XCom key are persisted
identity in Airflow, so renaming them would orphan task and XCom history. Since
issue #171 they carry speaker turns; the chapter branch is still in this module
but is unreachable in production, because selection only ever returns
item_type="turn". See the "Nomenclatura" subsection in docs/DAGS.md.
"""

import dataclasses
import logging
import os
import time
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.models import XCom
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from congress_videos.config import speaker_normalization_config as snc
from congress_videos.config.paths import get_video_chapter_dir
from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers
from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people
from congress_videos.modules.participants_db import (
    get_participants_roster,
    lookup_participant_by_slug,
    lookup_participant_fuzzy,
)
from congress_videos.modules.politician_display_names import canonical_display_name
from congress_videos.modules.speaker_placeholders import is_placeholder
from congress_videos.modules.topic_extraction import extract_topics
from congress_videos.modules.upload_marking import mark_chapter_uploads, mark_turn_uploads
from congress_videos.srt_helpers import (
    _parse_srt_blocks,
    chapter_window_blocks,
    find_srt_for_chapter,
)
from utils.airflow_helpers import ensure_project_data_directory, utc_normalize_row, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "development")
IS_DEVELOPMENT = POSTGRES_SCHEMA == "development"

# The scheduled 19:00 UTC run uploads only when at least one chapter is queued.
# This is a calendar-day cap for long-form chapter uploads; database quota data
# reports successful uploads_today, while limit=1 remains a selection safeguard.
DAILY_LONG_FORM_UPLOAD_LIMIT = 1
STALE_RUN_TOLERANCE_MINUTES = int(os.getenv("CHAPTER_UPLOADER_STALE_RUN_TOLERANCE_MINUTES", "30"))
_THUMBNAIL_DAG_ID = "generic_thumbnail_generator"
_THUMBNAIL_RESULT_TASK_ID = "thumbnail_result"

# Bounded poll loop for a triggered thumbnail-text regeneration (issue #545,
# design.md D2): 100 x 10s = 1000s. Measured production regenerations reach
# p50=214s, p95=888s, max=3989s — 1000s clears p95 with ~13% headroom, but
# the max EXCEEDS this bound, so the timeout branch below is a routinely
# exercised path, not an edge case. Deliberately its OWN env var/constant
# pair, distinct from _THUMBNAIL_MAX_POLLS in video_analytics_actions_dag.py
# (~30 min, post-publication) — this task runs pre-publication and cannot
# tolerate that longer wait.
_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10
_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv("UPLOAD_THUMBNAIL_REGEN_MAX_POLLS", "100"))


def _is_scheduled_run(dag_run) -> bool:
    """True for scheduler-created runs; a missing dag_run is treated as scheduled."""
    if dag_run is None:
        return True  # Backward-compatible for direct callable invocation.
    run_type = getattr(dag_run, "run_type", None)
    return getattr(run_type, "value", run_type) == "scheduled"


def _counts_toward_daily_quota(dag_run) -> bool:
    """Only scheduled runs consume the scheduled daily publishing slot (issue #500)."""
    return _is_scheduled_run(dag_run)


def should_upload(**context):
    """Return True only when the run is current, the daily cap is unspent and the queue is non-empty.

    Used as the python_callable for t1_skip (ShortCircuitOperator).
    Receives full Airflow context via **context (REQ-GATE-01).

    The staleness guard exists to drop scheduled runs replayed by a git_sync
    re-parse. A manual run is an operator decision: it inherits the previous
    cron interval as data_interval_end, so the guard would reject every manual
    run started more than the tolerance after the schedule tick (issue #500).
    """
    dag_run = context.get("dag_run")
    data_interval_end = context.get("data_interval_end")
    if data_interval_end and _is_scheduled_run(dag_run):
        now = datetime.now(UTC)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "Skipping stale re-parse replay: data_interval_end=%s is %s behind now=%s (tolerance=%dm)",
                data_interval_end,
                staleness,
                now,
                STALE_RUN_TOLERANCE_MINUTES,
            )
            return False
    ti = context["ti"]
    upload_quota = ti.xcom_pull(key="upload_quota") or {}
    queue_size = upload_quota.get("queue_size", 0)
    uploads_today = upload_quota.get("uploads_today", 0)
    if uploads_today >= DAILY_LONG_FORM_UPLOAD_LIMIT and _counts_toward_daily_quota(dag_run):
        logging.info(
            "Skipping upload: %d long-form chapter upload(s) already recorded today (daily limit=%d)",
            uploads_today,
            DAILY_LONG_FORM_UPLOAD_LIMIT,
        )
        return False

    return queue_size > 0


# ---------------------------------------------------------------------------
# Thumbnail pipeline helpers
# ---------------------------------------------------------------------------


def _speaker_mentions_from_entries(entries: list) -> list[str]:
    """Extract raw name strings from a list of dict-or-str speaker entries."""
    names = []
    for entry in entries or []:
        name = entry.get("name") if isinstance(entry, dict) else entry
        if name:
            names.append(name)
    return names


def _resolve_chapter_speaker(chapter: dict, key_speakers: list, db) -> tuple[str | None, list]:
    """Resolve a chapter's primary speaker via the roster-validated resolver.

    Second-chance resolution for chapters that slipped monitor-time
    resolution (issue #263). Extracts mentions from key_speakers first,
    then speakers, filters placeholders, dedupes, and calls
    resolve_chapter_speakers against the current participant roster. On a
    primary match, persists the slug (never-override write) and returns a
    canonicalized key_speakers list so title and photo derive from the same
    resolver call.

    This function NEVER raises: any internal error yields
    ``(None, key_speakers)`` unchanged.

    Args:
        chapter: Chapter row dict (must contain 'chapter_id').
        key_speakers: The chapter's raw key_speakers list.
        db: CongressionalVideoDB instance used for the write-back.

    Returns:
        ``(participant_slug | None, key_speakers)`` — key_speakers is
        canonicalized in place only when a primary match is found.
    """
    try:
        raw_names = _speaker_mentions_from_entries(key_speakers) + _speaker_mentions_from_entries(
            chapter.get("speakers")
        )

        mentions: list[str] = []
        seen: set[str] = set()
        for name in raw_names:
            if not is_placeholder(name) and name not in seen:
                mentions.append(name)
                seen.add(name)

        if not mentions:
            return None, key_speakers

        participants = get_participants_roster()
        resolution = resolve_chapter_speakers(mentions, participants)
        primary = resolution.primary
        if primary is None:
            return None, key_speakers

        chapter_id = chapter.get("chapter_id")
        if chapter_id is not None:
            db.mark_chapter_resolved(chapter_id, primary.participant_slug)

        canonical_key_speakers = []
        for entry in key_speakers or []:
            name = entry.get("name") if isinstance(entry, dict) else entry
            match = resolution.by_mention.get(name) if name else None
            if match is None:
                canonical_key_speakers.append(entry)
            elif isinstance(entry, dict):
                canonical_key_speakers.append({**entry, "name": match.display_name})
            else:
                canonical_key_speakers.append(match.display_name)

        return primary.participant_slug, canonical_key_speakers
    except Exception as exc:
        logging.warning(
            "_resolve_chapter_speaker: unexpected exception for chapter_id=%s: %s — slug=None",
            chapter.get("chapter_id"),
            exc,
        )
        return None, key_speakers


def _turn_window_bound(primary, fallback, default: float) -> float:
    """First float-coercible of *primary*/*fallback*, else *default*.

    Group bounds arrive as ``Decimal`` (NUMERIC columns); ``chapter_window_blocks``
    silently rejects that type, so coercion happens here, per-field (#341 F1).
    """
    for value in (primary, fallback):
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _analyze_chapter_content(chapter_id: int, blocks: list[dict], db) -> None:
    """Derive mentioned-people and topics for one chapter, upload-time (issue #432).

    Runs on the shared path of `_prepare_thumbnail_config`, after `blocks` is
    parsed and outside the turn-only branch. `uploadable_turns` carries no
    `start_time`/`end_time` (design F1), so the chapter's own SRT bounds come
    from `db.get_chapter_srt_context(chapter_id)`, never from the turn row.

    Each analysis is independently try/excepted: a failure in one MUST NOT
    discard or block the other (design D9). The persist gate writes a
    column only when that analysis returned `ok=True`; an `ok=True` empty
    people result is a real finding and is written, but an `ok=True` empty
    topics result would clobber a pre-existing value and is skipped instead
    (design D9). The final persistence call always runs — `update_chapter_
    content_analysis` itself no-ops when both kwargs are None (design D10)
    — and is itself try/excepted so a DB failure never fails the upload.
    """
    try:
        ctx = db.get_chapter_srt_context(chapter_id)
    except Exception as exc:
        logging.warning(
            "_analyze_chapter_content: get_chapter_srt_context failed for chapter_id=%s: %s — skipping analyses",
            chapter_id,
            exc,
        )
        return

    if ctx is None:
        logging.warning(
            "_analyze_chapter_content: no chapter context for chapter_id=%s — skipping analyses",
            chapter_id,
        )
        return

    chapter_text = " ".join(
        b["text"] for b in chapter_window_blocks(blocks, ctx.get("start_time"), ctx.get("end_time"))
    )
    if not chapter_text:
        logging.warning(
            "_analyze_chapter_content: empty chapter window for chapter_id=%s — skipping analyses",
            chapter_id,
        )
        return

    mentioned_slugs: list[str] | None = None
    try:
        roster = get_participants_roster()
        people_result = resolve_mentioned_people(chapter_text, roster)
        if people_result.ok:
            mentioned_slugs = list(people_result.slugs)
    except Exception as exc:
        logging.warning(
            "_analyze_chapter_content: resolve_mentioned_people failed for chapter_id=%s: %s",
            chapter_id,
            exc,
        )

    topics: list[str] | None = None
    try:
        topics_result = extract_topics(chapter_text)
        if topics_result.ok and topics_result.topics:
            topics = list(topics_result.topics)
        elif topics_result.ok:
            logging.info(
                "_analyze_chapter_content: extraction found no topics for chapter_id=%s — leaving topics untouched",
                chapter_id,
            )
    except Exception as exc:
        logging.warning(
            "_analyze_chapter_content: extract_topics failed for chapter_id=%s: %s",
            chapter_id,
            exc,
        )

    try:
        db.update_chapter_content_analysis(chapter_id, mentioned_slugs=mentioned_slugs, topics=topics)
    except Exception as exc:
        logging.warning(
            "_analyze_chapter_content: update_chapter_content_analysis failed for chapter_id=%s: %s",
            chapter_id,
            exc,
        )


def _turn_speaker_fields(chapter: dict) -> tuple[list[str], str | None]:
    """Resolve ``(key_speakers, slug)`` for a turn row.

    Lifted verbatim out of ``_prepare_thumbnail_config`` (issue #272): anchors
    key_speakers to the turn's own ``resolved_name`` and fuzzy-resolves its slug,
    falling back to the persisted ``resolved_participant_slug`` (and its
    display_name) when the name is empty. Lookup failures degrade, never raise.
    """
    # Turn path: anchor key_speakers to the turn's own speaker (resolved_name).
    # Use resolved_name directly for the fuzzy slug lookup.
    resolved_name = chapter.get("resolved_name") or ""
    if resolved_name:
        key_speakers = [resolved_name]
        slug = None
        try:
            participant = lookup_participant_fuzzy(resolved_name)
            slug = participant.get("slug") if participant else None
        except Exception as exc:
            logging.warning(
                "_prepare_thumbnail_config: turn speaker resolution failed for "
                "turn_id=%s resolved_name=%r: %s — setting slug=None",
                chapter.get("turn_id"),
                resolved_name,
                exc,
            )
            slug = None
    else:
        # Fallback to the AI-resolved slug persisted on the turn row
        # (resolved_participant_slug, issue #131 LLM name resolution) —
        # mirrors the chapter branch's slug-first precedence below.
        fallback_slug = chapter.get("resolved_participant_slug") or None
        slug = fallback_slug
        key_speakers = []
        if fallback_slug:
            try:
                participant = lookup_participant_by_slug(fallback_slug)
                if participant and participant.get("display_name"):
                    key_speakers = [participant["display_name"]]
            except Exception as exc:
                logging.warning(
                    "_prepare_thumbnail_config: turn slug lookup failed for "
                    "turn_id=%s resolved_participant_slug=%r: %s — "
                    "key_speakers stays empty",
                    chapter.get("turn_id"),
                    fallback_slug,
                    exc,
                )
    return key_speakers, slug


def _prepare_thumbnail_config(chapter: dict, db) -> dict:
    """Build the thumbnail-generation config dict for a single chapter or turn.

    Handles two row types:
    - Chapter rows (from uploadable_chapters view): use key_speakers list, resolved_participant_slug,
      and SRT window from start_time/end_time strings.
    - Turn rows (from uploadable_turns view, identified by presence of 'turn_id'): anchor
      key_speakers to [resolved_name], use resolved_name for slug lookup, and use start_seconds/
      end_seconds (float seconds) for the SRT window.

    Args:
        chapter: Chapter or turn row from the database view.
        db: CongressionalVideoDB instance (kept for task-call compatibility).

    Returns:
        Dict with keys: chapter_id, debate_summary, domain, session,
        slug (may be None on fallback), key_speakers, optionally srt_fragment.
    """
    is_turn = "turn_id" in chapter

    chapter_id = chapter.get("chapter_id")
    title = chapter.get("chapter_title", "")
    description = chapter.get("description", "")
    session_number = chapter.get("session_number")
    session_date = chapter.get("session_date")

    # debate_summary: title + description (D6)
    debate_summary = f"{title}\n{description}" if description else title

    # session label (D6)
    if session_number is not None:
        session = f"Sesión {session_number}"
    else:
        session = str(session_date) if session_date else None

    if is_turn:
        key_speakers, slug = _turn_speaker_fields(chapter)
    else:
        # Chapter path: read the authoritative slug written by monitor-time
        # resolution first (issue #263). Only when it is NULL does this seam
        # call the roster-validated resolver as a second chance — the raw
        # lookup_participant_fuzzy fallback is retired for chapters.
        key_speakers = chapter.get("key_speakers") or []
        slug = chapter.get("resolved_participant_slug") or None
        if not slug and snc.ENABLED:
            slug, key_speakers = _resolve_chapter_speaker(chapter, key_speakers, db)

    config = {
        "chapter_id": chapter_id,
        "debate_summary": debate_summary,
        "domain": "congreso",
        "session": session,
        "slug": slug,
        "key_speakers": key_speakers,
    }

    # Thread the canonical output_path for turn-type items (Slice 4a).
    # Chapter-type items never have output_path — key is absent by design.
    if is_turn:
        config["output_path"] = chapter.get("output_path")

    # Resolve SRT fragment for lapidary quote extraction (issue #57).
    # Note (issue #146 Fix C): the turn subtitles.srt sidecar is now written
    # exclusively by the speaker_turn_prepare DAG. The upload path no
    # longer writes it; srt_fragment below is still needed for the lapidary
    # thumbnail quote (chapter path) and is harmless for turns.
    video_id = chapter.get("video_id")
    canonical_dir = (
        str(get_video_chapter_dir(str(video_id), chapter_id))
        if video_id is not None and chapter_id is not None
        else None
    )
    srt_path = (
        find_srt_for_chapter(
            str(video_id),
            chapter_id,
            str(session_date) if session_date else None,
            canonical_dir=canonical_dir,
        )
        if video_id is not None
        else None
    )
    if srt_path is None:
        logging.warning(
            "_prepare_thumbnail_config: no SRT resolved (video_id=%s chapter_id=%s "
            "turn_id=%s) — srt_fragment omitted, thumbnail falls back to invented copy",
            video_id,
            chapter_id,
            chapter.get("turn_id"),
        )
        return config

    blocks = _parse_srt_blocks(srt_path)

    if chapter_id is not None:
        _analyze_chapter_content(chapter_id, blocks, db)

    if is_turn:
        # Grouped clips (#129/#231) publish the GROUP span, not the
        # representative turn's own narrow span (issue #341).
        w_start = _turn_window_bound(chapter.get("group_start_seconds"), chapter.get("start_seconds"), 0.0)
        w_end = _turn_window_bound(chapter.get("group_end_seconds"), chapter.get("end_seconds"), 99 * 3600)
    else:
        w_start = chapter.get("start_time", "00:00:00,000")
        w_end = chapter.get("end_time", "99:59:59,999")

    fragment = " ".join(b["text"] for b in chapter_window_blocks(blocks, w_start, w_end))[:10_000]
    if fragment:
        config["srt_fragment"] = fragment
    else:
        logging.warning(
            "_prepare_thumbnail_config: empty SRT window (video_id=%s chapter_id=%s "
            "turn_id=%s window=%s..%s blocks=%d) — srt_fragment omitted",
            video_id,
            chapter_id,
            chapter.get("turn_id"),
            w_start,
            w_end,
            len(blocks),
        )

    return config


def _extract_metadata_description(
    youtube_metadata_results: dict | None,
) -> str:
    """Extract description from youtube_metadata_results XCom payload.

    Handles both dict-wrapped values (``{"description": "..."}`` nested dict)
    and plain string values. Returns ``""`` on any missing or empty input.

    Title is NOT extracted here (issue #245): the turn branch of
    _prepare_upload_config sources the title from this run's thumbnail_result
    XCom instead, since generate_youtube_metadata_for_selected_videos no
    longer generates a title.

    Args:
        youtube_metadata_results: The XCom value pushed by _generate_youtube_metadata.
            Shape: ``{"topic_metadata": [{"description": {...}|str}]}``.

    Returns:
        The description as a string (may be empty).
    """
    topic = (youtube_metadata_results or {}).get("topic_metadata") or []
    if not topic:
        return ""
    entry = topic[0]
    d = entry.get("description") or {}
    return d.get("description", "") if isinstance(d, dict) else str(d)


def _run_get_uploadable_item(db) -> dict | None:
    """Select the next item from the turn queue only.

    Returns a dict with keys:
      - 'item': the row dict (turn), UTC-normalized
      - 'item_type': 'turn'
    Returns None when the turn queue is empty.

    The row is UTC-normalized (issues #163, #309): psycopg2 returns the
    TIMESTAMPTZ columns materialized_at/prepared_at with an unnamed non-UTC
    fixed offset, which Airflow serializes with an empty tz name and then
    cannot deserialize on xcom_pull. Values stay datetime-typed, so no
    consumer needs to re-parse.
    """
    turns = db.get_uploadable_turns(limit=1)
    if turns:
        return {"item": utc_normalize_row(turns[0]), "item_type": "turn"}

    return None


def _thumbnail_failure(chapter_id: int | None) -> dict:
    return {
        "chapter_id": chapter_id,
        "success": False,
        "output_path": None,
        "title": None,
    }


def _unpublished_thumbnail_labels(upload_details: list | None) -> list[str]:
    """Label every video that published without its custom thumbnail.

    ``thumbnail_success`` is four-valued: ``True`` (thumbnail set), ``False``
    (YouTube's ``thumbnails.set`` call failed), ``None`` (no custom thumbnail
    was requested for this video), or the key absent entirely (the
    no-results fallback shape built by ``trigger_upload_with_config`` when
    the triggered DAG returns no XCom). ONLY the literal value ``False`` is a
    failure — see design D2 (issue #320).
    """
    if not upload_details:
        return []

    labels = []
    for detail in upload_details:
        # Deliberate identity check, not truthiness: `not detail.get(...)`
        # would also match None/missing (no thumbnail requested) and wrongly
        # flag every legitimate upload that never asked for a custom one.
        if detail.get("thumbnail_success") is False:
            youtube_video_id = detail.get("youtube_video_id") or "<unknown>"
            labels.append(
                f"{youtube_video_id} (chapter_id={detail.get('chapter_id')}, turn_id={detail.get('turn_id')})"
            )
    return labels


def _turn_marking_problems(turn_updates: dict | None) -> list[str]:
    """Describe every turn-marking finding worth failing the daily upload gate.

    Returns finished operator-facing sentences, ready to append to the
    `_check_upload_failures` `problems` accumulator. Empty list == clean.

    `turn_updates` is the `turn_upload_updates` XCom, shaped
    `{"updated_turns", "failed_updates", "details"}` — note there is NO
    `recorded_failures` key: turns have no DB-persisted failure concept
    (accepted asymmetry, proposal D3; follow-up issue tracks it).

    `None` means the XCom is absent, which is always an anomaly (issue #332
    design D3): reported as a finding, never as a short-circuit raise, so it
    cannot mask the chapter or thumbnail findings.
    """
    if turn_updates is None:
        return ["turn_upload_updates XCom missing after mark_turns_uploaded succeeded"]

    def _label(detail: dict) -> str:
        turn_id = detail.get("turn_id")
        if turn_id is not None:
            return f"turn_id={turn_id}"
        return f"output_path={detail.get('output_path')}"

    details = turn_updates.get("details") or []
    count = turn_updates.get("failed_updates") or 0

    failed_details = [d for d in details if d.get("status") == "failed"]
    not_found_details = [
        d for d in details if d.get("status") == "skipped" and d.get("reason") == "output_path_not_found"
    ]

    problems = []
    if failed_details or count > 0:
        n = max(count, len(failed_details))
        labels = [_label(d) for d in failed_details]
        problems.append(
            f"Turn upload failures: {n} DB-update failure(s) after a successful "
            f"publish. Turns: {labels}. Those rows are still "
            f"is_uploaded_to_youtube=FALSE and WILL be re-uploaded by the next "
            f"19:00 UTC run — mark them manually."
        )

    if not_found_details:
        labels = [_label(d) for d in not_found_details]
        problems.append(
            f"{len(not_found_details)} turn video(s) published but matched no "
            f"speaker_turn_videos row: {labels}. Zero rows matched is never a "
            f"benign no-op (the UPDATE has no is_uploaded_to_youtube=FALSE "
            f"predicate) — these will be re-uploaded by the next run."
        )

    return problems


def _copy_verification_evidence(db, *, chapter_id: int | None, turn_id: int | None) -> dict:
    """Assemble the evidence bundle for final-copy verification (issue #512,
    design.md D5). Both the canonical short_name (#511) and the raw
    display_name are included and clearly labelled — the verifier must
    never receive only one of them.

    ``mencionados`` is tri-valued (design.md D5): NULL renders
    ``"no analizado"`` (not yet analysed — must never be misread as "nobody
    mentioned"), an empty list renders ``[]`` (analysed, nobody mentioned),
    a populated list renders the resolved entries.
    """
    chapter = (db.get_chapter_metadata(chapter_id) if chapter_id is not None else None) or {}

    speaker_row = (db.get_turn_speaker_slug(turn_id) if turn_id is not None else None) or {}
    slug = speaker_row.get("resolved_participant_slug")
    participant = (lookup_participant_by_slug(slug) if slug else None) or {}

    mentioned_slugs = chapter.get("mentioned_participant_slugs")
    if mentioned_slugs is None:
        mencionados: object = "no analizado"
    else:
        mencionados = []
        for mentioned_slug in mentioned_slugs:
            mentioned_participant = (lookup_participant_by_slug(mentioned_slug) if mentioned_slug else None) or {}
            mencionados.append(
                {
                    "slug": mentioned_slug,
                    "display_name": mentioned_participant.get("display_name"),  # raw
                    "short_name": canonical_display_name(mentioned_slug),  # canonical
                    "party": mentioned_participant.get("party"),
                }
            )

    return {
        "speaker": {
            "slug": slug,
            "display_name": participant.get("display_name"),  # raw — ground-truth identity
            "short_name": canonical_display_name(slug),  # canonical (#511)
            "party": participant.get("party"),
            "parliamentary_group": participant.get("parliamentary_group"),
            "resolution_confidence": speaker_row.get("speaker_resolution_confidence"),
            "resolution_method": speaker_row.get("speaker_resolution_method"),
        },
        "chapter": {
            "title": chapter.get("title"),
            "description": chapter.get("description"),
            "topics": chapter.get("topics"),
            "speakers": chapter.get("speakers"),
            "key_speakers": chapter.get("key_speakers"),
            "scoring_reasoning": chapter.get("scoring_reasoning"),
            "session_number": chapter.get("session_number"),
            "session_date": chapter.get("session_date"),
        },
        "mencionados": mencionados,
    }


def _thumbnail_brief_text(thumbnail_row: dict | None) -> str | None:
    """Return the verifiable text from a video_thumbnails row's
    art_direction_brief JSONB (design.md D5). NULL and the legacy plain-string
    shape (video_analytics_actions_dag.py:298) both omit the field entirely —
    only the current dict-with-"text" shape yields anything to verify."""
    if not thumbnail_row:
        return None
    brief = thumbnail_row.get("art_direction_brief")
    if isinstance(brief, dict):
        text = brief.get("text")
        if isinstance(text, str) and text:
            return text
    return None


def _copy_verification_problems(payload: dict | None) -> list[str]:
    """Describe final-copy verification findings worth failing the daily
    upload gate for (issue #512, design.md D7). Shaped like
    _turn_marking_problems: returns finished operator-facing sentences,
    appended to the _check_upload_failures `problems` accumulator.

    `payload` is the `copy_verification` XCom
    (`{verdict, findings, corrected_applied, persisted, content_version,
    thumbnail_regen_landed}`). `None` means the XCom is missing entirely —
    always an anomaly once a turn was actually verified — reported as a
    finding, never a short-circuit raise, so it cannot mask the other
    findings.

    A title `reject` never reaches this function: it raises upstream in
    `_verify_final_copy`, before the XCom is ever pushed (the locked
    hard-rejection asymmetry). A successfully applied correction is a
    success story, not a finding, even though its originating findings are
    still kept in the payload for audit purposes.

    Issue #545 / design.md D4: a `thumbnail_text` finding that did NOT land
    a regeneration (timeout, trigger/child failure, invalid result, or the
    attempt budget already exhausted) is an operator-facing signal too —
    informational and non-blocking, exactly like every other finding here,
    never a reason to fail `t6b` itself.
    """
    if payload is None:
        return ["copy_verification XCom missing after prepare_upload_config succeeded"]

    verdict = payload.get("verdict")
    findings = payload.get("findings") or []
    corrected_applied = bool(payload.get("corrected_applied"))
    problems: list[str] = []

    if verdict == "inconclusive":
        problems.append(
            "Final-copy verification inconclusive (verifier failure, timeout or "
            "malformed response); published unchanged, no audit persisted"
        )

    if verdict == "reject":
        fields = sorted({f.get("field") for f in findings if isinstance(f, dict) and f.get("field")})
        problems.append(
            f"Final-copy verification rejected (fields={fields or ['unknown']}); published unchanged, review required"
        )

    unsupported = [f for f in findings if isinstance(f, dict) and f.get("category") == "unsupported_claim"]
    if unsupported and not corrected_applied:
        problems.append(
            f"Final-copy verification discarded {len(unsupported)} unsupported correction(s); published original copy"
        )

    if verdict in ("pass", "correctable", "reject") and not payload.get("persisted"):
        problems.append("Final-copy verification audit write was skipped (stale-copy guard)")

    has_thumbnail_text_finding = any(isinstance(f, dict) and f.get("field") == "thumbnail_text" for f in findings)
    if has_thumbnail_text_finding and not payload.get("thumbnail_regen_landed"):
        problems.append(
            "Final-copy verification flagged thumbnail text and regeneration did not land; "
            "published with the existing thumbnail"
        )

    return problems


def _write_title_provenance(payload: object, key: str | None, db=None) -> dict:
    """Persist a title-generation input payload (issue #549), never blocking publication.

    Follows the failure-isolation convention from
    ``congress_videos/modules/upload_marking.py`` (~lines 60-108), NOT the
    bare ``record_copy_verification_*`` call-site shape: catches any DB
    exception, logs it, and returns a ``"failed"`` outcome instead of
    propagating. A ``payload``/``key`` absence is not an error — it is
    recorded as ``"skipped"``. ``rowcount == 0`` (design D3/C4: the write
    carries no ``IS DISTINCT FROM`` guard) is a loud ``"no_row"`` outcome,
    never treated as success.

    Args:
        payload: The candidate ``title_generation_input`` value pulled from
            the child DAG's result (may be missing, ``None``, or malformed).
        key: The write key — MUST be ``thumbnail_config["output_path"]`` (the
            turn's own ``video.mp4``), never the child result's
            ``output_path`` (the reconciled ``thumbnail.png``).
        db: CongressionalVideoDB instance (injected for testability; created
            internally when None).

    Returns:
        ``{"status": "written" | "no_row" | "failed" | "skipped", "rows": int, "error": str | None}``.
    """
    if not (isinstance(payload, dict) and payload and key):
        return {"status": "skipped", "rows": 0, "error": None}

    from congress_videos.modules.database import CongressionalVideoDB

    try:
        rows = (db or CongressionalVideoDB()).record_title_generation_input_turn(key, payload=payload)
    except Exception as exc:
        logging.error("title provenance write failed for output_path=%r: %s", key, exc)
        return {"status": "failed", "rows": 0, "error": str(exc)}

    if not rows:
        logging.warning("title provenance: 0 rows matched for output_path=%r — key mismatch", key)
        return {"status": "no_row", "rows": 0, "error": None}
    return {"status": "written", "rows": rows, "error": None}


def trigger_thumbnail_generation(ti, db=None, **context) -> str | None:
    """Run the generic thumbnail DAG and retain its result for upload configuration.

    Args:
        ti: Airflow TaskInstance.
        db: CongressionalVideoDB instance (injected for testability; created
            internally when None), matching how ``_prepare_thumbnail_config``
            already receives one.
    """
    thumbnail_config = ti.xcom_pull(key="thumbnail_config") or {}
    chapter_id = thumbnail_config.get("chapter_id")
    required_values = ("chapter_id", "debate_summary", "session", "domain")
    if not all(thumbnail_config.get(key) for key in required_values):
        logging.info(
            "Thumbnail input incomplete for chapter_id=%s; uploading without custom thumbnail",
            chapter_id,
        )
        ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
        return None

    child_conf = {
        "youtube_video_id": str(chapter_id),
        **{key: thumbnail_config[key] for key in required_values},
        "slug": thumbnail_config.get("slug"),
        "key_speakers": thumbnail_config.get("key_speakers") or [],
    }
    if "srt_fragment" in thumbnail_config:
        child_conf["srt_fragment"] = thumbnail_config["srt_fragment"]
    if thumbnail_config.get("output_path"):
        child_conf["output_path"] = thumbnail_config["output_path"]
    try:
        dag_run = trigger_dag_api(
            dag_id=_THUMBNAIL_DAG_ID,
            conf=child_conf,
            run_id=f"chapter_thumbnail_{context['run_id']}",
        )
    except Exception as exc:
        logging.warning("Could not trigger thumbnail DAG for chapter_id=%s: %s", chapter_id, exc)
        ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
        return None

    child_run_id = dag_run.run_id
    ti.xcom_push(key="thumbnail_dag_run_id", value=child_run_id)
    logging.info("Triggered thumbnail DAG run: %s", child_run_id)

    while True:
        time.sleep(10)
        dag_run.refresh_from_db()
        if dag_run.state not in ["success", "failed"]:
            continue

        if dag_run.state != "success":
            logging.warning("Thumbnail DAG run %s failed", child_run_id)
            ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
            return child_run_id

        result = XCom.get_one(
            dag_id=_THUMBNAIL_DAG_ID,
            task_id=_THUMBNAIL_RESULT_TASK_ID,
            key="return_value",
            run_id=child_run_id,
        )
        if not (
            isinstance(result, dict)
            and result.get("success") is True
            and result.get("chapter_id") == chapter_id
            and isinstance(result.get("output_path"), str)
            and result["output_path"]
            and isinstance(result.get("title"), str)
            and result["title"]
        ):
            logging.warning("Thumbnail DAG run %s returned no valid result", child_run_id)
            ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
            return child_run_id

        # Issue #549: persist the title-generator input payload keyed by the
        # TURN's own output_path (thumbnail_config["output_path"] — never
        # result["output_path"], which is the child DAG's reconciled
        # thumbnail.png). Optional in the strict validation above; never
        # blocks publication.
        provenance = _write_title_provenance(
            result.get("title_generation_input"),
            thumbnail_config.get("output_path"),
            db=db,
        )
        ti.xcom_push(key="title_provenance", value=provenance)

        ti.xcom_push(key="thumbnail_result", value=result)
        return child_run_id


_REGEN_REQUIRED_CONF_KEYS = ("chapter_id", "debate_summary", "session", "domain")


def _build_regen_child_conf(thumbnail_config: dict, output_path: str, prior_brief: dict | None) -> dict | None:
    """Build ``generic_thumbnail_generator``'s child ``conf`` for a bounded
    thumbnail-text regeneration (issue #545), the SAME way
    ``trigger_thumbnail_generation`` (t4) builds it, from the SAME
    ``thumbnail_config`` XCom.

    Returns ``None`` when any of the four required scalar values
    (``_REGEN_REQUIRED_CONF_KEYS``) is missing/empty — t4's own guard idiom:
    the caller must not trigger and must record a non-blocking
    ``trigger_failed`` outcome instead.
    """
    if not all(thumbnail_config.get(key) for key in _REGEN_REQUIRED_CONF_KEYS):
        return None

    chapter_id = thumbnail_config.get("chapter_id")
    child_conf: dict = {
        "youtube_video_id": str(chapter_id),
        **{key: thumbnail_config[key] for key in _REGEN_REQUIRED_CONF_KEYS},
        "slug": thumbnail_config.get("slug"),
        "key_speakers": thumbnail_config.get("key_speakers") or [],
    }
    if "srt_fragment" in thumbnail_config:
        child_conf["srt_fragment"] = thumbnail_config["srt_fragment"]
    if prior_brief:
        child_conf["previous_brief"] = prior_brief
    # design.md D6: sibling isolation is guaranteed by FILE — always override
    # with the triggering turn's own output_path, never trust whatever
    # thumbnail_config itself carries under that key.
    child_conf["output_path"] = output_path
    return child_conf


def _regenerate_flagged_thumbnail(
    thumbnail_config: dict,
    output_path: str,
    prior_brief: dict | None,
    run_id: str,
    db=None,
) -> dict | None:
    """Trigger and poll one bounded thumbnail-text regeneration attempt (issue #545).

    Wired from ``t6b`` (``_verify_final_copy``) via ``_claim_and_regenerate_thumbnail``.
    Modeled on ``video_analytics_actions_dag.py::_poll_thumbnail_dag_run``'s
    BOUNDED loop shape (design.md D2) — never this module's own
    ``trigger_thumbnail_generation``, whose unbounded ``while True`` is a
    pre-existing risk, not a template here.

    The child ``conf`` is built the SAME way ``trigger_thumbnail_generation``
    (t4) builds it, from the same ``thumbnail_config`` XCom, because
    ``generic_thumbnail_generator``'s own ``validate_input`` requires
    ``youtube_video_id``, ``chapter_id``, ``debate_summary``, ``session`` and
    ``domain`` (all five, not merely the four scalar values named in
    ``_REGEN_REQUIRED_CONF_KEYS`` — ``youtube_video_id`` is derived from
    ``chapter_id`` exactly as t4 does). When any of the four scalar values is
    missing/empty, this function follows t4's own guard idiom: it does NOT
    trigger, records ``trigger_failed`` (best-effort), and returns — the
    same non-blocking contract as every other outcome below.

    Seven outcomes converge on the SAME non-blocking contract (design.md D4):
    an incomplete conf, a trigger failure, a child DAG ``failed`` state, a
    malformed/missing child result (including a returned path that does not
    exist on disk — D5), a poll timeout, and a landed success all record
    their outcome via ``record_thumbnail_text_regeneration_outcome`` and
    return WITHOUT ever raising. The trigger/poll body is one
    ``try/except Exception`` — the same catch-and-return shape as
    ``_write_title_provenance`` above — so a bug here, including one during
    polling that nobody anticipated, can never fail the enclosing ``t6b``
    task and block publication. Measured production regenerations reach up
    to 3989s, which EXCEEDS this function's own 1000s bound, so the timeout
    branch is a routinely exercised path, not an edge case.

    Args:
        thumbnail_config: The ``thumbnail_config`` XCom pushed once per DAG
            run by ``_prepare_thumbnail_config`` (t3) — the SAME source t4
            reads. Supplies ``chapter_id``/``debate_summary``/``session``/
            ``domain``/``slug``/``key_speakers``/``srt_fragment`` for the
            child conf. Its own ``output_path`` (if any) is ALWAYS
            overridden below by the ``output_path`` argument — see D6.
        output_path: The triggering turn's own ``video.mp4`` path. Becomes
            the child DAG's ``conf["output_path"]``, unconditionally
            overriding anything ``thumbnail_config`` might carry, so the
            child's write is confined to this turn's own directory
            (design.md D6: sibling isolation is guaranteed by file, never by
            the shared ``video_thumbnails`` DB row).
        prior_brief: The brief snapshotted at claim time (the ``prior_brief``
            passed into ``claim_thumbnail_text_regeneration``), forwarded as
            ``previous_brief`` so the regeneration steers away from the
            flagged original. Omitted from the child conf entirely when
            falsy — best-effort steering, never a hard requirement.
        run_id: The enclosing DAG run's ``run_id``, used to build a
            deterministic, traceable child ``run_id``.
        db: CongressionalVideoDB instance (injected for testability;
            created internally when None), matching how
            ``trigger_thumbnail_generation`` already receives one.

    Returns:
        The child DAG's ``thumbnail_result`` XCom dict on a landed success,
        or ``{"outcome": ..., "error": ...}`` for any of
        ``trigger_failed``/``child_failed``/``invalid_result``/``timeout``.
        Never raises, and never returns anything else.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    database = db or CongressionalVideoDB()

    def _record(outcome: str, *, error: str | None = None, regenerated_brief: dict | None = None) -> None:
        # design.md D4 point 3 / _write_title_provenance shape: a DB outage
        # on this best-effort bookkeeping write must never become a
        # publication outage.
        try:
            database.record_thumbnail_text_regeneration_outcome(
                output_path,
                outcome=outcome,
                error=error,
                regenerated_brief=regenerated_brief,
            )
        except Exception as exc:
            logging.error(
                "_regenerate_flagged_thumbnail: recording outcome=%s for output_path=%r failed: %s",
                outcome,
                output_path,
                exc,
            )

    child_conf = _build_regen_child_conf(thumbnail_config, output_path, prior_brief)
    if child_conf is None:
        error = (
            f"thumbnail regeneration input incomplete for output_path={output_path!r} "
            f"(chapter_id={thumbnail_config.get('chapter_id')}); skipping regeneration trigger"
        )
        logging.info(error)
        _record("trigger_failed", error=error)
        return {"outcome": "trigger_failed", "error": error}

    child_run_id = f"thumbnail_text_regen_{run_id}"

    try:
        dag_run = trigger_dag_api(
            dag_id=_THUMBNAIL_DAG_ID,
            conf=child_conf,
            run_id=child_run_id,
        )

        for _poll in range(_THUMBNAIL_REGEN_MAX_POLLS):
            time.sleep(_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS)
            dag_run.refresh_from_db()
            if dag_run.state not in ("success", "failed"):
                continue

            if dag_run.state != "success":
                error = f"thumbnail regeneration DAG run {dag_run.run_id} failed"
                logging.warning(error)
                _record("child_failed", error=error)
                return {"outcome": "child_failed", "error": error}

            result = XCom.get_one(
                dag_id=_THUMBNAIL_DAG_ID,
                task_id=_THUMBNAIL_RESULT_TASK_ID,
                key="return_value",
                run_id=dag_run.run_id,
            )
            if not (
                isinstance(result, dict)
                and result.get("success") is True
                and isinstance(result.get("output_path"), str)
                and result["output_path"]
                and os.path.exists(result["output_path"])
            ):
                error = f"thumbnail regeneration DAG run {dag_run.run_id} returned no valid result"
                logging.warning(error)
                _record("invalid_result", error=error)
                return {"outcome": "invalid_result", "error": error}

            _record("applied", regenerated_brief=result)
            return result

        error = (
            f"thumbnail regeneration for output_path={output_path!r} timed out after "
            f"{_THUMBNAIL_REGEN_MAX_POLLS * _THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS}s"
        )
        logging.warning(error)
        _record("timeout", error=error)
        return {"outcome": "timeout", "error": error}
    except Exception as exc:
        logging.exception(
            "_regenerate_flagged_thumbnail: regeneration failed for output_path=%r: %s",
            output_path,
            exc,
        )
        _record("trigger_failed", error=str(exc))
        return {"outcome": "trigger_failed", "error": str(exc)}


def _claim_and_regenerate_thumbnail(
    db,
    *,
    output_path: str,
    thumbnail_config: dict,
    prior_brief: dict | None,
    run_id: str,
) -> dict | None:
    """Claim a bounded regeneration attempt and, if claimed, trigger it (issue #545).

    Wired from ``t6b`` (``_verify_final_copy``) whenever a ``thumbnail_text``
    finding is present. The claim call itself is wrapped in its own
    ``try/except``: a claim-time DB exception converges on the SAME
    non-blocking contract as every outcome inside
    ``_regenerate_flagged_thumbnail`` (design.md D4) — never raise, publish
    as-is.

    A falsy claim (``None``) is NOT an error and is never logged as one: it
    means either the per-video attempt budget is already exhausted, OR —
    intentionally, by design (design.md D3 / spec note 8) — the item has no
    matching ``speaker_turn_videos`` row at all. Chapter items fall into the
    second case: their own extraction ``output_path`` never appears in
    ``speaker_turn_videos`` (that table is turn-scoped only), so
    ``claim_thumbnail_text_regeneration`` naturally returns ``None`` for
    them and this function triggers nothing. This is the CORRECT, intended
    behaviour for chapter items — not a bug to "fix" by special-casing
    ``item_type`` here.

    Returns:
        The child DAG's result dict on a landed regeneration, or one of
        ``_regenerate_flagged_thumbnail``'s failure-mode dicts, or ``None``
        when no attempt was claimed (exhausted, no row, or a claim-time
        exception).
    """
    try:
        claimed = db.claim_thumbnail_text_regeneration(output_path, prior_brief=prior_brief)
    except Exception as exc:
        logging.exception(
            "_claim_and_regenerate_thumbnail: claim failed for output_path=%r: %s",
            output_path,
            exc,
        )
        return None

    if not claimed:
        logging.info(
            "_claim_and_regenerate_thumbnail: attempt not claimed for output_path=%r "
            "(exhausted, at ceiling, or no speaker_turn_videos row)",
            output_path,
        )
        return None

    return _regenerate_flagged_thumbnail(thumbnail_config, output_path, prior_brief, run_id, db=db)


def _apply_thumbnail_regeneration_if_flagged(
    db,
    ti,
    *,
    video: dict,
    output_path: str | None,
    findings,
    run_id: str | None,
    chosen_thumbnail_row: dict | None,
) -> tuple[bool, bool]:
    """Extracted from ``_verify_final_copy`` (t6b) to keep its cyclomatic
    complexity bounded. Runs the entire issue #545 regeneration seam for one
    verdict: no-op when there is no ``thumbnail_text`` finding or no
    ``output_path``, otherwise claim-then-trigger via
    ``_claim_and_regenerate_thumbnail`` and, on a landed result whose file
    still exists on disk, swap ``video["thumbnail_file"]`` in place
    (design.md D5).

    Returns:
        ``(landed, mutated)`` — ``landed`` is ``True`` whenever the
        regeneration itself succeeded (``result["success"] is True``,
        i.e. outcome ``applied``), independent of the local disk swap;
        ``mutated`` is ``True`` only when ``video["thumbnail_file"]`` was
        actually rewritten. Both are ``False`` for every non-blocking
        failure mode (timeout, trigger/child failure, invalid result, not
        claimed, or a claim-time exception) — this function never raises,
        matching every function it calls.
    """
    if not (output_path and any(f.field == "thumbnail_text" for f in findings)):
        return False, False

    thumbnail_config = ti.xcom_pull(key="thumbnail_config") or {}
    prior_brief = (chosen_thumbnail_row or {}).get("art_direction_brief")
    regen_result = _claim_and_regenerate_thumbnail(
        db,
        output_path=output_path,
        thumbnail_config=thumbnail_config,
        prior_brief=prior_brief,
        run_id=run_id,
    )
    if not (isinstance(regen_result, dict) and regen_result.get("success") is True):
        return False, False

    landed = True
    regen_path = regen_result.get("output_path")
    # design.md D5: a returned path that does not exist on disk is never
    # swapped in — _regenerate_flagged_thumbnail already only records
    # "applied" for a result whose path exists, but this is the
    # load-bearing check for the actual XCom mutation.
    if regen_path and os.path.exists(regen_path):
        video["thumbnail_file"] = regen_path
        return landed, True
    return landed, False


def _backfill_thumbnail_video_id(ti, db=None) -> None:
    """Back-fill the real YouTube video ID in video_thumbnails after upload.

    Reads thumbnail_result and upload_results XComs. Calls
    ``db.update_thumbnail_youtube_video_id`` only when the thumbnail
    generation succeeded (``success is True``).

    Args:
        ti: Airflow TaskInstance.
        db: CongressionalVideoDB instance (injected for testability; created
            internally when None).
    """
    thumbnail_result = ti.xcom_pull(key="thumbnail_result") or {}
    if not thumbnail_result.get("success"):
        logging.info("_backfill_thumbnail_video_id: thumbnail_result.success is False — skipping back-fill")
        return

    upload_results = ti.xcom_pull(key="upload_results") or {}
    upload_details = upload_results.get("upload_details", [])

    chapter_id = thumbnail_result.get("chapter_id")

    # Find the matching upload detail for this chapter
    youtube_video_id = None
    for detail in upload_details:
        if detail.get("chapter_id") == chapter_id:
            youtube_video_id = detail.get("youtube_video_id")
            break

    if youtube_video_id is None:
        logging.warning(
            "_backfill_thumbnail_video_id: no youtube_video_id found for chapter_id=%s in upload_details",
            chapter_id,
        )
        return

    if db is None:
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()

    db.update_thumbnail_youtube_video_id(chapter_id=chapter_id, youtube_video_id=youtube_video_id)
    logging.info(
        "_backfill_thumbnail_video_id: chapter_id=%s → %r",
        chapter_id,
        youtube_video_id,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "congress_youtube_chapter_uploader",
    default_args=default_args,
    description="Upload top congressional video chapters to YouTube based on relevance score",
    schedule="0 19 * * *",  # Run once daily at 19:00 UTC; gate enforces one long video per calendar day
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["congress", "youtube", "chapters"],
    params={
        "max_chapters": 1,
        "min_relevance_score": 2,
        "isTesting": False,
        "dry_run": False,  # Set to True to run the full pipeline without triggering the YouTube upload
    },
) as dag:
    # Step 0: Ensure data directory exists
    t0 = PythonOperator(
        task_id="ensure_data_directory",
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory("congress_videos"),
            "data_directory_path",
        ),
    )

    # Step 1: Check daily upload quota
    # Queries DB for uploads today and pending queue size.
    # Returns {queue_size, uploads_today}.
    def _run_check_upload_quota(ti, **context):
        """Query DB for uploads today and pending queue size.

        Pushes XCom key 'upload_quota':
          {'uploads_today', 'queue_size', 'turns_pending'}
        """
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        min_relevance_score = context["params"].get("min_relevance_score", 2)

        chapters_uploaded_today = db.count_chapters_uploaded_today()
        turns_uploaded_today = db.count_turns_uploaded_today()
        uploads_today = chapters_uploaded_today + turns_uploaded_today

        chapters_pending = db.count_pending_uploadable_chapters(min_relevance_score)
        turns_pending = db.count_pending_uploadable_turns()
        queue_size = chapters_pending + turns_pending

        result = {
            "uploads_today": uploads_today,
            "queue_size": queue_size,
            "turns_pending": turns_pending,
        }
        logging.info(
            "Upload quota: %d today (%d chapters + %d turn videos), %d chapters + %d turns in queue",
            uploads_today,
            chapters_uploaded_today,
            turns_uploaded_today,
            chapters_pending,
            turns_pending,
        )
        ti.xcom_push(key="upload_quota", value=result)
        return result

    t1_quota = PythonOperator(
        task_id="check_upload_quota",
        python_callable=_run_check_upload_quota,
    )

    # Step 1b: Short-circuit based on queue_size vs time-of-day threshold
    t1_skip = ShortCircuitOperator(
        task_id="skip_if_quota_reached",
        python_callable=should_upload,
    )

    # Step 2: Turn-only queue; None when empty (no chapter fallback) (limit=1 per run)
    def _run_get_uploadable_item_task(ti):
        """Select upload item from the turn queue only.

        Pushes 'uploadable_item' XCom key with dict:
          {'item': <row dict>, 'item_type': 'turn'}
        or {'item': None, 'item_type': None} when the turn queue is empty.
        """
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        result = _run_get_uploadable_item(db)
        if result is None:
            result = {"item": None, "item_type": None}
        ti.xcom_push(key="uploadable_item", value=result)
        logging.info(
            "Selected uploadable item: item_type=%s, item=%s",
            result.get("item_type"),
            result.get("item", {}).get("chapter_id") if result.get("item") else None,
        )

    def _generate_youtube_metadata(ti):
        from congress_videos.modules.youtube import youtube_ai

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        # Pass a single-item list matching the chapter-list contract expected by youtube_ai
        items = [item] if item else []
        return xcom_task(
            ti,
            lambda: youtube_ai.generate_youtube_metadata_for_selected_videos(items),
            "youtube_metadata_results",
        )

    def _run_prepare_thumbnail_config(ti):
        """Resolve speaker name and build thumbnail config struct for the item (turn or chapter)."""
        from congress_videos.modules.database import CongressionalVideoDB

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        db = CongressionalVideoDB()
        result = _prepare_thumbnail_config(item, db)
        ti.xcom_push(key="thumbnail_config", value=result)

    def _run_generate_thumbnail(ti):
        """Trigger the generic thumbnail DAG for both turns and chapters (issue #169).

        Turn items now go through the same thumbnail generation path as chapters:
        _prepare_thumbnail_config already resolves the turn's speaker slug and
        sets output_path, so trigger_thumbnail_generation writes thumbnail.png to
        the canonical #133 turn directory.
        """
        return trigger_thumbnail_generation(ti, run_id=ti.run_id)

    def _extract_chapter_videos(ti):
        """Extract chapter video via ffmpeg, or use the pre-materialized turn video path."""
        from congress_videos.modules import video_splitter

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        item_type = uploadable.get("item_type")

        if item_type == "turn":
            # Turn video already exists at output_path — no ffmpeg extraction needed.
            output_path = item.get("output_path")
            success = bool(output_path)
            result = {
                "total_chapters": 1,
                "successful_extractions": 1 if success else 0,
                "failed_extractions": 0 if success else 1,
                "results": [
                    {
                        "chapter_id": item.get("chapter_id"),
                        "turn_id": item.get("turn_id"),
                        "video_id": item.get("video_id"),
                        "success": success,
                        "output_path": output_path,
                        "file_size_mb": None,
                        "duration_seconds": None,
                        "error": None if success else "turn output_path missing",
                    }
                ],
            }
            ti.xcom_push(key="chapter_extraction_results", value=result)
            return result
        else:
            # Chapter: call video_splitter (ffmpeg-based extraction)
            chapters = [item] if item else []
            return xcom_task(
                ti,
                lambda: video_splitter.extract_chapters_from_video(
                    chapters,
                    ti.xcom_pull(key="data_directory_path"),
                ),
                "chapter_extraction_results",
            )

    def _format_session_date(session_date) -> str:
        """Render a session date as Spanish ``DD/MM/AAAA``.

        Accepts a ``date``/``datetime`` or any stringifiable value. An ISO
        ``YYYY-MM-DD`` string is reformatted; anything unparseable is passed through
        unchanged rather than raising, since the intro card is never allowed to block
        a publication.
        """
        if hasattr(session_date, "strftime"):
            return session_date.strftime("%d/%m/%Y")
        text = str(session_date)
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            return text

    def _build_intro_card_text(session_number, session_date) -> tuple[str, str]:
        """Build the Spanish intro-card ``titulo``/``descripcion`` from session metadata.

        The card names the institution, not just an ordinal: a long-form video reaches
        YouTube viewers with no surrounding context, so ``"Sesión 193"`` alone
        identifies nothing. ``titulo`` reads
        ``"Sesión {n} del Congreso de los Diputados"``, degrading to
        ``"Congreso de los Diputados"`` when no session number is known.

        ``descripcion`` carries the session date in Spanish ``DD/MM/AAAA`` form (issue
        #558 asked for that format; an earlier revision emitted the raw ISO value), or
        an empty string (the ``intro_sesion`` renderer already tolerates no subtitle).

        Args:
            session_number: The session's ordinal number, or ``None``.
            session_date: The session's date (any stringifiable value), or ``None``.

        Returns:
            ``(titulo, descripcion)`` tuple.

        Raises:
            ValueError: When both ``session_number`` and ``session_date`` are absent —
                there is nothing to render on the card.
        """
        if session_number is None and not session_date:
            raise ValueError(
                "_build_intro_card_text: both session_number and session_date are "
                "missing — cannot build the session intro card text."
            )

        institution = "Congreso de los Diputados"
        titulo = f"Sesión {session_number} del {institution}" if session_number is not None else institution
        descripcion = _format_session_date(session_date) if session_date else ""
        return titulo, descripcion

    def _apply_intro_overlay(ti):
        """Burn the 5-second session intro card into the extracted video before upload.

        New task t5b, between t5 (``extract_chapter_videos``) and t6
        (``prepare_upload_config``). Runs ``apply_overlays()`` in-process — the same
        in-process precedent as the chapter branch of ``_extract_chapter_videos`` —
        and overwrites ``output_path`` on the ``chapter_extraction_results`` XCom for
        the CURRENT RUN ONLY. Records ``original_output_path`` for diagnosis.

        This task imports no database module and issues no ``db.*`` write:
        ``speaker_turn_videos.output_path`` is never touched (issue #558, D4). The
        overlaid file is a same-directory ``_edited`` sibling, so the 4 sidecars
        (``title.txt``, ``description.txt``, ``thumbnail.png``, ``subtitles.srt``)
        still resolve for ``prepare_orador_upload_config`` unchanged (t6 needs zero
        code changes — it reads whatever ``output_path`` this task leaves behind).

        Pass-through (mirrors t6's tolerance of upstream extraction failure): a
        missing/failed/empty ``chapter_extraction_results``, or a missing
        ``output_path``, logs and leaves the XCom untouched — this is not this
        task's failure to report.

        Fail-loud (issue #558, D3): every failure CAUSED by this task — a guard trip,
        a missing font, an ffmpeg failure, a missing overlaid output file, or absent
        session metadata — raises. Never a silent skip, never publishing the
        un-overlaid source in place of a failed overlay.
        """
        from congress_videos.config.video_editor_config import get_domain_config
        from congress_videos.modules.video_editor import (
            INTRO_WINDOW_SECONDS,
            OVERLAY_MAX_TIMEOUT_SECONDS,
            _default_output_path,
            apply_overlays,
            validate_editor_input,
        )

        extraction_results = ti.xcom_pull(key="chapter_extraction_results") or {}
        results = extraction_results.get("results") or []
        if not results or not results[0].get("success"):
            logging.info("_apply_intro_overlay: no successful chapter_extraction_results — skipping intro overlay")
            return None

        source_path = results[0].get("output_path")
        if not source_path:
            logging.info("_apply_intro_overlay: output_path missing — skipping intro overlay")
            return None

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        titulo, descripcion = _build_intro_card_text(item.get("session_number"), item.get("session_date"))

        start, end = INTRO_WINDOW_SECONDS
        output_path = _default_output_path(source_path)
        conf = {
            "domain": "congreso",
            "source_path": source_path,
            "overlays": [
                {
                    "tipo": "intro_sesion",
                    "tiempo_inicio": start,
                    "tiempo_fin": end,
                    "titulo": titulo,
                    "descripcion": descripcion,
                }
            ],
        }

        # D5: validate BEFORE apply_overlays. apply_overlays never calls this
        # itself, and _load_font silently degrades a missing font into a garbage
        # default-font card reported as success — validate here to fail loud
        # instead, before any ffmpeg process spawns.
        validate_editor_input(conf)

        domain_cfg = get_domain_config("congreso")
        apply_overlays(
            source_path,
            output_path,
            conf["overlays"],
            domain_cfg,
            max_timeout=OVERLAY_MAX_TIMEOUT_SECONDS,
        )

        if not os.path.exists(output_path):
            raise RuntimeError(
                "_apply_intro_overlay: apply_overlays reported success but the "
                f"overlaid output file is missing: {output_path!r}"
            )

        results[0]["original_output_path"] = source_path
        results[0]["output_path"] = output_path
        ti.xcom_push(key="chapter_extraction_results", value=extraction_results)
        logging.info(
            "_apply_intro_overlay: intro card applied; output_path=%r (original=%r)",
            output_path,
            source_path,
        )
        return extraction_results

    def _prepare_upload_config(ti, **context):
        """Build upload config for the selected item (turn or chapter).

        Turn items: read pre-prepared sidecars via prepare_orador_upload_config.
          - Zero AI calls, zero ffmpeg calls, zero thumbnail triggers.
          - Validates sidecar presence; raises FileNotFoundError if any is missing.
        Chapter items: unchanged path via prepare_chapter_upload_config + AI metadata.
        """
        from congress_videos.modules.youtube import prepare_chapter_upload_config
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        dry_run = context.get("params", {}).get("dry_run", False)
        is_testing = context.get("params", {}).get("isTesting", False)
        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item_type = uploadable.get("item_type")

        if item_type == "turn":
            # Turn path: read pre-written sidecars; build a minimal extraction_results
            # wrapper so the generic uploader receives the same envelope shape.
            extraction_results = ti.xcom_pull(key="chapter_extraction_results") or {}
            results = extraction_results.get("results") or []
            if not results or not results[0].get("success"):
                logging.warning(
                    "_prepare_upload_config: turn extraction result missing or failed — skipping upload config"
                )
                ti.xcom_push(key="upload_config", value=None)
                return None

            output_path = results[0].get("output_path") or ""
            if not output_path:
                logging.warning("_prepare_upload_config: turn output_path missing — skipping")
                ti.xcom_push(key="upload_config", value=None)
                return None

            # Issue #245: the turn title comes from THIS run's thumbnail_result XCom,
            # never from the deprecated metadata title generator or from stale
            # sidecar file presence. A stale thumbnail.png would otherwise pass
            # _REQUIRED_SIDECARS and mask a failed thumbnail run.
            thumbnail_result = ti.xcom_pull(key="thumbnail_result") or {}
            title = thumbnail_result.get("title")
            if thumbnail_result.get("success") is not True or not isinstance(title, str) or not title.strip():
                raise ValueError(
                    "Turn upload aborted: thumbnail pipeline produced no usable title "
                    f"(chapter_id={results[0].get('chapter_id')}, "
                    f"turn_id={results[0].get('turn_id')}, output_path={output_path}); "
                    "refusing to publish a fallback title (issue #245)."
                )

            # Issue #169: overwrite title.txt/description.txt from fresh 19:00 AI metadata
            # BEFORE prepare_orador_upload_config reads them from disk.
            # This ensures fresh 19:00 AI output wins over any stale prepare-side sidecars.
            from congress_videos.modules.youtube.youtube_upload import _write_orador_sidecars

            youtube_metadata_results = ti.xcom_pull(key="youtube_metadata_results")
            fresh_desc = _extract_metadata_description(youtube_metadata_results)
            fresh_title = title.strip()
            _write_orador_sidecars(output_path, fresh_title, fresh_desc)
            logging.info(
                "_prepare_upload_config: turn path — overwrote sidecars from XCom metadata, title=%r",
                fresh_title[:60] if fresh_title else "",
            )

            turn_config = prepare_orador_upload_config(
                output_path=output_path,
                is_testing=is_testing,
            )
            # Add turn_id + chapter_id from extraction result for upload tracking
            turn_config["chapter_id"] = results[0].get("chapter_id")
            turn_config["turn_id"] = results[0].get("turn_id")
            turn_config["video_id"] = results[0].get("video_id")

            from congress_videos.config.youtube_channels import (
                DEFAULT_CHANNEL,
                resolve_token_path,
            )

            config = {
                "token_file": resolve_token_path(DEFAULT_CHANNEL, "upload"),
                "videos": [turn_config],
            }
            logging.info(
                "_prepare_upload_config: turn path — sidecars read from %s, title=%r",
                os.path.dirname(output_path),
                turn_config.get("title", "")[:60],
            )
            ti.xcom_push(key="upload_config", value=config)
            return config

        # Chapter path (unchanged).
        return xcom_task(
            ti,
            lambda: prepare_chapter_upload_config(
                ti.xcom_pull(key="chapter_extraction_results"),
                ti.xcom_pull(key="youtube_metadata_results"),
                thumbnail_result=ti.xcom_pull(key="thumbnail_result"),
                is_testing=is_testing,
                dry_run=dry_run,
            ),
            "upload_config",
        )

    def _verify_final_copy(ti, **context):
        """Verify the turn's final publication copy before it ships (issue #512).

        New task t6b, between t6 (prepare_upload_config) and t7
        (trigger_youtube_upload). Reads title/description from
        `upload_config["videos"][0]` — the LAST mutable representation,
        already sidecar-round-tripped by prepare_orador_upload_config —
        NEVER the upstream thumbnail_result / _extract_metadata_description
        XComs (design.md: "Verify the last mutable representation, never an
        upstream copy").

        Hard-rejection asymmetry (design.md D2/D7, locked): a `reject`
        verdict blocks publication ONLY for the title, reusing the ValueError
        fail-loud convention already established at this seam (issue #245).
        Description and thumbnail-text findings are recorded and surfaced
        through the `_check_upload_failures` accumulator; publication
        proceeds with the existing values.

        Chapter items reach this task too (both paths push `upload_config`
        with the same "videos" shape); an item with no videos to verify
        (upstream skip/failure) is a silent no-op — the anomaly, if any, was
        already an upstream failure that `_check_upload_failures` covers
        through its own findings.
        """
        from congress_videos.modules.database import CongressionalVideoDB
        from congress_videos.modules.final_copy_verification import (
            compute_content_version,
            verify_final_copy,
        )
        from congress_videos.modules.youtube.youtube_upload import _write_orador_sidecars

        config = ti.xcom_pull(key="upload_config")
        videos = (config or {}).get("videos") or []
        if not videos:
            logging.info("_verify_final_copy: no upload_config videos — skipping verification")
            return None

        video = videos[0]
        chapter_id = video.get("chapter_id")
        turn_id = video.get("turn_id")
        output_path = video.get("video_file")
        original_title = video.get("title") or ""
        original_description = video.get("description") or ""

        db = CongressionalVideoDB()
        evidence = _copy_verification_evidence(db, chapter_id=chapter_id, turn_id=turn_id)
        chosen_thumbnail_row = db.get_chosen_thumbnail(chapter_id) if chapter_id is not None else None
        thumbnail_text = _thumbnail_brief_text(chosen_thumbnail_row)

        verdict = verify_final_copy(
            title=original_title,
            description=original_description,
            thumbnail_text=thumbnail_text,
            evidence=evidence,
        )

        if not verdict.ok:
            logging.info(
                "_verify_final_copy: inconclusive verdict (chapter_id=%s, turn_id=%s) — publishing unchanged",
                chapter_id,
                turn_id,
            )
            ti.xcom_push(
                key="copy_verification",
                value={
                    "verdict": "inconclusive",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": False,
                    "content_version": "",
                    "thumbnail_regen_landed": False,
                },
            )
            return None

        if verdict.verdict == "reject" and any(f.field == "title" for f in verdict.findings):
            raise ValueError(
                "Turn upload aborted: final-copy verification rejected the title "
                f"(chapter_id={chapter_id}, turn_id={turn_id}, output_path={output_path}); "
                "refusing to publish a flagged title (issue #512)."
            )

        # Issue #545: bounded, non-blocking thumbnail-text regeneration.
        # Placed strictly AFTER the title-reject raise above (never before
        # it), so the locked hard-rejection asymmetry (design.md D2/D7) can
        # never be reordered or suppressed by anything below this line.
        mutated = False

        if verdict.correction_applied:
            video["title"] = verdict.title
            video["description"] = verdict.description
            if output_path:
                _write_orador_sidecars(output_path, verdict.title, verdict.description)
            mutated = True

        thumbnail_regen_landed, regen_mutated = _apply_thumbnail_regeneration_if_flagged(
            db,
            ti,
            video=video,
            output_path=output_path,
            findings=verdict.findings,
            run_id=context.get("run_id"),
            chosen_thumbnail_row=chosen_thumbnail_row,
        )
        mutated = mutated or regen_mutated

        # Non-negotiable (issue #545): ONE push covers BOTH the correction
        # mutation and the thumbnail-file swap. This push MUST NOT live only
        # inside `if verdict.correction_applied:` — a landed regeneration
        # with no title/description correction would otherwise be silently
        # dropped and t7 would publish the pre-attempt thumbnail.
        if mutated:
            ti.xcom_push(key="upload_config", value=config)

        # Stale-copy guard (design.md D3): recompute from the values actually
        # about to be published, immediately before the write. A mismatch
        # means the copy changed since verification — never persist a
        # correction against copy the verifier never saw.
        recomputed_version = compute_content_version(
            title=verdict.title,
            description=verdict.description,
            thumbnail_text=thumbnail_text,
            evidence=evidence,
        )
        persisted = False
        if output_path and recomputed_version == verdict.content_version:
            db.record_copy_verification_turn(
                output_path,
                verdict=verdict.verdict,
                findings=[dataclasses.asdict(f) for f in verdict.findings],
                original_title=original_title,
                original_description=original_description,
                corrected_title=verdict.title if verdict.correction_applied else None,
                corrected_description=verdict.description if verdict.correction_applied else None,
                thumbnail_text=thumbnail_text,
                content_version=verdict.content_version,
            )
            persisted = True
        else:
            logging.warning(
                "_verify_final_copy: stale-copy guard skipped the audit write "
                "(chapter_id=%s, turn_id=%s, output_path=%s)",
                chapter_id,
                turn_id,
                output_path,
            )

        ti.xcom_push(
            key="copy_verification",
            value={
                "verdict": verdict.verdict,
                "findings": [dataclasses.asdict(f) for f in verdict.findings],
                "corrected_applied": verdict.correction_applied,
                "persisted": persisted,
                "content_version": verdict.content_version,
                "thumbnail_regen_landed": thumbnail_regen_landed,
            },
        )
        return None

    def _run_backfill_thumbnail_video_id(ti):
        """Back-fill youtube_video_id in video_thumbnails after upload completes."""
        _backfill_thumbnail_video_id(ti)

    t1_item = PythonOperator(
        task_id="get_uploadable_item",
        python_callable=_run_get_uploadable_item_task,
    )

    # Step 2: Generate YouTube metadata for the selected item (turn or chapter)
    t2 = PythonOperator(
        task_id="generate_youtube_metadata",
        python_callable=_generate_youtube_metadata,
    )

    # Step 3 (new): Prepare thumbnail config — resolve speaker, build config struct
    t3_prepare = PythonOperator(
        task_id="prepare_thumbnail_config",
        python_callable=_run_prepare_thumbnail_config,
    )

    # Step 4 (new): Generate thumbnail via the generic thumbnail DAG
    t4_generate = PythonOperator(
        task_id="generate_thumbnail",
        python_callable=_run_generate_thumbnail,
    )

    # Step 5: Extract video — turn path reused directly; chapter extracted via ffmpeg
    t5 = PythonOperator(
        task_id="extract_chapter_videos",
        python_callable=_extract_chapter_videos,
    )

    # Step 5b (new, issue #558): burn the session intro card into the extracted
    # video before upload; overwrites output_path on chapter_extraction_results
    # in-memory only, for this run.
    t5b = PythonOperator(
        task_id="apply_intro_overlay",
        python_callable=_apply_intro_overlay,
    )

    # Step 6: Prepare upload configuration for generic YouTube uploader DAG
    t6 = PythonOperator(
        task_id="prepare_upload_config",
        python_callable=_prepare_upload_config,
    )

    # Step 6b (new, issue #512): verify the final copy before publication
    # (issue #545, design.md D4 — regression guard, do NOT "fix" this):
    # deliberately NO `execution_timeout=` on this operator. Its bounded
    # in-code thumbnail-regeneration poll (_regenerate_flagged_thumbnail,
    # up to _THUMBNAIL_REGEN_MAX_POLLS * _THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS)
    # is the only guarantee against an unbounded wait. An Airflow task
    # timeout here would FAIL t6b, SKIP t7, and convert a soft,
    # non-blocking thumbnail-text finding into a hard publication block —
    # exactly what #545 exists to prevent.
    t6b = PythonOperator(
        task_id="verify_final_copy",
        python_callable=_verify_final_copy,
    )

    # Step 7: Trigger generic YouTube uploader DAG and wait for completion
    def trigger_upload_with_config(ti, **context):
        """Trigger the generic YouTube uploader DAG with config from XCom."""
        import time

        from airflow.models import XCom

        if context.get("params", {}).get("dry_run", False):
            logging.info("dry_run=True — skipping YouTube upload")
            ti.xcom_push(key="upload_results", value={"upload_details": []})
            return None

        # Get config from XCom
        config = ti.xcom_pull(key="upload_config")

        if not config:
            logging.warning("No upload config found, skipping upload")
            ti.xcom_push(key="upload_results", value={"upload_details": []})
            return None

        # Trigger the DAG
        logging.info(f"Triggering generic_youtube_uploader with {len(config.get('videos', []))} videos")
        dag_run = trigger_dag_api(
            dag_id="generic_youtube_uploader",
            conf=config,
            run_id=f"chapter_upload_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")

        # Wait for completion
        logging.info("Waiting for upload to complete...")
        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ["success", "failed"]:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

                # Pull upload results from the triggered DAG
                upload_results = XCom.get_many(
                    execution_date=dag_run.execution_date,
                    dag_ids=["generic_youtube_uploader"],
                    task_ids=["upload_videos"],
                    key="return_value",
                    limit=1,
                )

                if upload_results:
                    results_data = upload_results[0].value
                    logging.info(f"Retrieved upload results: {results_data}")
                    ti.xcom_push(key="upload_results", value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    # Create results based on config and DAG state
                    upload_details = []
                    for video_config in config.get("videos", []):
                        upload_details.append(
                            {
                                "chapter_id": video_config.get("chapter_id"),
                                "video_id": video_config.get("video_id"),
                                "video_file": video_config.get("video_file"),
                                "success": dag_run.state == "success",
                                "youtube_video_id": None,
                                "error": "Upload failed - no results available" if dag_run.state == "failed" else None,
                            }
                        )
                    ti.xcom_push(key="upload_results", value={"upload_details": upload_details})

                return dag_run.run_id

    def _check_upload_failures(ti):
        """Raise after DB writes so failures are visible in the Airflow UI.

        Accumulates four independent findings into ONE exception (issue #320
        design D6, extended by issue #332): chapter DB-recorded upload
        failures, videos published without their custom thumbnail, turn
        DB-update failures, and turn output_path_not_found/missing-XCom
        findings. A first-wins raise would hide later findings permanently —
        the DB writes are already committed and the XComs are immutable, so a
        retry would just re-raise the same earlier error forever without the
        other findings ever surfacing.
        """
        updates = ti.xcom_pull(key="chapter_upload_updates")
        if updates is None:
            raise Exception("chapter_upload_updates XCom missing after mark_chapters_uploaded succeeded")

        problems = []

        recorded = updates.get("recorded_failures", 0)
        failed = updates.get("failed_updates", 0)
        if recorded > 0 or failed > 0:
            bad = [d for d in updates.get("details", []) if d.get("status") in ("failure_recorded", "failed")]
            problems.append(
                f"Chapter upload failures: {recorded} recorded, {failed} "
                f"DB-update failures. Chapters: {[d.get('chapter_id') for d in bad]}"
            )

        # upload_results missing/None/empty is deliberately benign here (design
        # D3): the chapter_upload_updates-missing raise above already owns
        # reporting a missing-XCom condition, and a second raise for
        # upload_results would double-report it and could mask the more
        # specific message above.
        upload_results = ti.xcom_pull(key="upload_results") or {}
        thumbnail_labels = _unpublished_thumbnail_labels(upload_results.get("upload_details"))
        if thumbnail_labels:
            problems.append(
                f"{len(thumbnail_labels)} video(s) uploaded without their custom "
                f"thumbnail (DB writes already committed). Re-run "
                f"set_thumbnail_for_video for: {', '.join(thumbnail_labels)}"
            )

        # NEW (issue #332). Missing turn_upload_updates is a finding, not a
        # short-circuit raise: it must not mask the two findings above.
        problems.extend(_turn_marking_problems(ti.xcom_pull(key="turn_upload_updates")))

        # NEW (issue #512). A reject on description/thumbnail text, a
        # discarded unsupported correction, an inconclusive verdict, or a
        # skipped audit write is a finding, not a short-circuit raise: it
        # must not mask the findings above. A title reject never reaches
        # here — it already raised in _verify_final_copy.
        problems.extend(_copy_verification_problems(ti.xcom_pull(key="copy_verification")))

        if problems:
            raise Exception(" | ".join(problems))

        logging.info("No chapter upload failures recorded")

    t7 = PythonOperator(
        task_id="trigger_youtube_upload",
        python_callable=trigger_upload_with_config,
    )

    # Step 8: Update database to mark chapters as uploaded
    def _run_mark_chapters_uploaded(ti, **context):
        """Mark chapters as uploaded to YouTube after a successful upload.

        Pushes XCom key 'chapter_upload_updates'.
        """
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        upload_results = ti.xcom_pull(key="upload_results")
        result = mark_chapter_uploads(
            db,
            upload_results,
            counts_toward_daily_quota=_counts_toward_daily_quota(context.get("dag_run")),
        )
        ti.xcom_push(key="chapter_upload_updates", value=result)
        return result

    t8_db = PythonOperator(
        task_id="mark_chapters_uploaded",
        python_callable=_run_mark_chapters_uploaded,
    )

    # Step 8c: Mark turn videos as uploaded (runs in parallel with mark_chapters_uploaded)
    def _run_mark_turns_uploaded(ti, **context):
        """Mark speaker turn videos as uploaded to YouTube after a successful upload.

        Pushes XCom key 'turn_upload_updates'.
        """
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        upload_results = ti.xcom_pull(key="upload_results")
        result = mark_turn_uploads(
            db,
            upload_results,
            counts_toward_daily_quota=_counts_toward_daily_quota(context.get("dag_run")),
        )
        ti.xcom_push(key="turn_upload_updates", value=result)
        return result

    t8_turns = PythonOperator(
        task_id="mark_turns_uploaded",
        python_callable=_run_mark_turns_uploaded,
    )

    # Step 8b: Back-fill youtube_video_id in video_thumbnails
    t8_backfill = PythonOperator(
        task_id="backfill_thumbnail_video_id",
        python_callable=_run_backfill_thumbnail_video_id,
    )

    # Step 9: Alert when chapter upload failures were recorded (after DB write)
    t9 = PythonOperator(
        task_id="check_upload_failures",
        python_callable=_check_upload_failures,
    )

    # Task dependencies (16 tasks total)
    # t0 > t1_quota > t1_skip > t1_item > t2 > t3_prepare > t4_generate > t5 > t5b > t6 > t6b >
    #   t7 > [t8_db, t8_turns] > t8_backfill > t9
    (
        t0
        >> t1_quota
        >> t1_skip
        >> t1_item
        >> t2
        >> t3_prepare
        >> t4_generate
        >> t5
        >> t5b
        >> t6
        >> t6b
        >> t7
        >> [t8_db, t8_turns]
        >> t8_backfill
        >> t9
    )
