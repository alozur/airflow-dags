"""Speaker Turn Prepare DAG (issue #146).

Nightly preparation of up to N=2 unprepared TURN items. Generates all required
sidecars, validates video integrity with an ffmpeg decode check, then sets
prepared_at as the upload readiness gate.

Schedule: 0 2 * * * UTC (off-peak; 02:00 avoids monitor scrape, reap, and upload
windows). The upload DAG runs at 0 19 * * *.

Design constraints (non-negotiable):
- Strictly sequential: one PythonOperator, pool="nas_ffmpeg", pool_slots=1.
- No dynamic task mapping (.expand() is prohibited).
- prepared_at set ONLY after: all sidecars on disk AND ffmpeg decode check rc==0.
- Failures are self-healing: prepared_at stays NULL, retry next night.
"""

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.config.constants import SPEAKER_TURN_PREPARE_DAG_ID
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.speaker_placeholders import is_placeholder
from congress_videos.modules.speaker_resolution import (
    QA_WIDE_CONTEXT_ENABLED,
    SPEAKER_RESOLUTION_MIN_CONFIDENCE,
    resolve_speaker,
)
from congress_videos.modules.speaker_roster_crosscheck import (
    chapter_roster_mentions,
    crosscheck_slug,
)
from congress_videos.modules.participants_db import CongressParticipantsDB
from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad
from utils.env_loader import load_env_if_local


def get_all_participants() -> list[dict]:
    """Fetch all participants from DB. Extracted for monkeypatching in tests."""
    return CongressParticipantsDB().get_all_participants()

load_env_if_local()

logger = logging.getLogger(__name__)

DAG_ID = SPEAKER_TURN_PREPARE_DAG_ID

# ---------------------------------------------------------------------------
# Internal helpers (testable pure functions)
# ---------------------------------------------------------------------------


def _display_name_for(participants: list[dict], slug: str) -> str | None:
    """Look up a participant's canonical display_name by slug (issue #342:
    shared by both the narrow and the wide resolution pass)."""
    return next((p["display_name"] for p in participants if p["slug"] == slug), None)


def _is_qa_promotion_signal(previous_name: str | None, resolved_name: str | None) -> bool:
    """Rule 4 (issue #282): true when the pre-resolution name and the newly
    resolved display_name are two distinct real (non-placeholder, non-NULL)
    names. Defensive on None/empty inputs (issue #342)."""
    if not previous_name or not resolved_name:
        return False
    if is_placeholder(previous_name) or is_placeholder(resolved_name):
        return False
    return previous_name.casefold() != resolved_name.strip().casefold()


def _validate_keep_intervals(raw) -> list[tuple[float, float]] | None:
    """Coerce/validate a ``keep_intervals`` JSONB value (issue #143).

    Boundary validation (repo rule — validate at system boundaries): the
    value comes back from the DB as a list, a JSON-encoded string (some
    psycopg2/RealDictCursor configurations), or ``None``.

    Returns a start-sorted list of ``(float, float)`` tuples with every
    non-positive-duration entry dropped, or ``None`` when the value is
    missing, empty, or malformed in any way — signalling the caller to fall
    back to the legacy single-window path. NEVER raises.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None
    if not isinstance(raw, list):
        return None

    intervals: list[tuple[float, float]] = []
    for item in raw:
        try:
            start, end = item
            start = float(start)
            end = float(end)
        except (TypeError, ValueError):
            return None  # one malformed entry invalidates the whole value
        if end > start:
            intervals.append((start, end))

    if not intervals:
        return None
    intervals.sort(key=lambda iv: iv[0])
    return intervals


def _write_turn_sidecars(
    turn: dict,
    *,
    trim_start_secs: float = 0.0,
    trim_end_secs: float = 0.0,
) -> None:
    """Write subtitles.srt for a turn from windowed SRT source.

    After issue #169 unify-upload-metadata: title.txt and description.txt are
    now generated at 19:00 upload time by the uploader DAG, not here. This
    function writes only the SRT sidecar so prepared_at semantics remain clean:
    video + srt + decode-check only.

    After issue #175 vad-trim: when VAD trim was applied, the SRT window is
    narrowed by the trim offsets so timestamps align with the trimmed MP4.

    After issue #143 (procedural-turn excision): when the row carries a
    valid, non-empty ``keep_intervals`` (the EXECUTED cut boundaries
    recorded at materialize time), the SRT is retimed from THAT — never
    from the raw ``group_start_seconds``/``group_end_seconds`` span — via
    ``_window_srt_blocks_multi``. This is correct whether excision happened
    (len>1) or not (len==1, values equal the turn/group's own bounds), and
    fixes edge-only excision: a single collapsed keep interval can be
    NARROWER than the raw group span, and using the raw span there would
    misalign captions with the actually-cut video. Rows with no valid
    ``keep_intervals`` (pre-#143 legacy rows, or a malformed/missing value)
    fall back to the legacy group_start/group_end single window.

    Args:
        turn: Row dict from select_unprepared_turns.
        trim_start_secs: Seconds trimmed from the start (0.0 = no trim).
        trim_end_secs: Seconds trimmed from the end (0.0 = no trim).

    Raises:
        Exception: Any sidecar write failure propagates so prepared_at is NOT set.
    """
    from congress_videos.srt_helpers import (
        _parse_srt_blocks,
        _serialize_srt_blocks,
        _window_srt_blocks,
        _window_srt_blocks_multi,
        find_srt_for_chapter,
    )

    output_path = turn.get("output_path") or ""
    video_dir = os.path.dirname(output_path)

    # Write subtitles.srt from windowed SRT fragment.
    video_id = turn.get("video_id")
    chapter_id = turn.get("chapter_id")
    session_date = turn.get("session_date")

    srt_path = (
        find_srt_for_chapter(
            str(video_id),
            chapter_id,
            str(session_date) if session_date else None,
        )
        if video_id is not None
        else None
    )

    if srt_path is not None:
        blocks = _parse_srt_blocks(srt_path)
        keep_intervals = _validate_keep_intervals(turn.get("keep_intervals"))

        if keep_intervals is not None:
            # Issue #143: retime from the EXECUTED cut boundaries, not the
            # raw group span. Apply the VAD trim to the first interval's
            # start and the last interval's end only; drop anything that
            # collapses to zero/negative duration after trimming.
            adjusted = list(keep_intervals)
            first_start, first_end = adjusted[0]
            adjusted[0] = (first_start + trim_start_secs, first_end)
            last_start, last_end = adjusted[-1]
            adjusted[-1] = (last_start, last_end - trim_end_secs)
            adjusted = [(s, e) for s, e in adjusted if e > s]
            windowed = _window_srt_blocks_multi(blocks, adjusted) if adjusted else []
        else:
            # Legacy path (issue #151): keep_intervals is NULL/missing/malformed
            # — pre-#143 rows. Use group extent when available (grouped-turn
            # clip spans multiple individual turns sharing one output_path).
            # Fall back to per-turn start/end for single-turn rows that do
            # not carry group_* keys.
            window_start = float(
                turn.get("group_start_seconds", turn.get("start_seconds", 0))
            )
            window_end = float(
                turn.get("group_end_seconds", turn.get("end_seconds", 99 * 3600))
            )
            # Issue #175: narrow window by VAD trim offsets so SRT timestamps
            # align with the (possibly trimmed) MP4. Zero offsets → unchanged.
            window_start += trim_start_secs
            window_end -= trim_end_secs
            windowed = _window_srt_blocks(blocks, window_start, window_end)
    else:
        windowed = []

    srt_out = os.path.join(video_dir, "subtitles.srt")
    with open(srt_out, "w", encoding="utf-8") as f:
        f.write(_serialize_srt_blocks(windowed) if windowed else "")

    logger.info(
        "_write_turn_sidecars: subtitles.srt written for turn_id=%d at %s",
        turn.get("turn_id"),
        video_dir,
    )


def _run_ffmpeg_decode_check(path: str) -> int:
    """Fully decode a video via ffmpeg to verify integrity.

    Uses ``ffmpeg -f null -`` (ffprobe has no null output muxer and always
    returns rc=1, which means prepared_at is never set — live-confirmed on
    prod 2026-08-22). A non-zero return code means the file is truncated or
    corrupt. Does not raise on failure; returns the process return code.
    """
    result = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.returncode


def _prepare_turns_callable() -> None:
    """Callable for the prepare_turns PythonOperator task.

    Selects up to N=2 unprepared turns and processes each sequentially:
    1. Write subtitles.srt sidecar.
    2. Run ffmpeg decode integrity check on video.mp4.
    3. Set prepared_at (only when all prior steps succeeded).

    After issue #169 unify-upload-metadata: thumbnail generation and
    title/description sidecar writing are now owned by the 19:00 upload DAG.
    prepared_at semantics: video + srt + decode-check only.

    Any failure within a turn's steps leaves prepared_at NULL; the next
    nightly run retries all steps from scratch.
    """
    db = CongressionalVideoDB()
    turns = db.select_unprepared_turns(limit=2)

    if not turns:
        logger.info("_prepare_turns_callable: no unprepared turns; nothing to do")
        return

    logger.info("_prepare_turns_callable: %d turn(s) selected for preparation", len(turns))

    # Fetch all participants once per DAG loop — injected into resolve_speaker per turn.
    # Failure here degrades to no-attribution (never blocks prepare).
    try:
        participants = get_all_participants()
    except Exception as exc:
        logger.warning(
            "_prepare_turns_callable: failed to fetch participants (%s) — resolution skipped for all turns",
            exc,
        )
        participants = []

    for turn in turns:
        turn_id = turn["turn_id"]
        output_path = turn.get("output_path") or ""

        logger.info(
            "_prepare_turns_callable: preparing turn_id=%d output_path=%s",
            turn_id,
            output_path,
        )

        # Step 1.5: AI speaker resolution (issue #177).
        # Never blocks preparation — wrapped in its own try/except.
        try:
            # Captured BEFORE any in-memory patch below (issue #282 rule 4):
            # the promotion hook compares this pre-resolution name against
            # the newly resolved display_name.
            previous_name = (turn.get("resolved_name") or "").strip()

            already_resolved = (
                turn.get("resolved_participant_slug")
                and float(turn.get("speaker_resolution_confidence") or 0)
                >= SPEAKER_RESOLUTION_MIN_CONFIDENCE
            )
            if not already_resolved:
                narrow = resolve_speaker(turn, participants)
                if narrow is not None:
                    narrow_slug = narrow["participant_slug"]
                    narrow_name = _display_name_for(participants, narrow_slug)

                    # issue #342: compute the qa-promotion signal ONCE, from
                    # the narrow result, before any write. Promotion is
                    # STICKY on this signal — the (possibly widened) winner
                    # below only decides which slug is persisted, never
                    # whether promotion fires (preserves #282 rule 4 exactly).
                    promote_signal = _is_qa_promotion_signal(previous_name, narrow_name)
                    mentions = chapter_roster_mentions(
                        turn.get("key_speakers"), turn.get("speakers")
                    )

                    winner, winner_name, winner_verdict = narrow, narrow_name, None
                    wide_slug = None
                    if promote_signal and QA_WIDE_CONTEXT_ENABLED:
                        # Re-resolve with turn_type='qa' on a shallow copy
                        # (never mutate turn) so speaker_resolution's
                        # qa-widened prompt path can disambiguate a
                        # monologue-truncated narrow pass.
                        try:
                            wide = resolve_speaker({**turn, "turn_type": "qa"}, participants)
                        except Exception as exc:
                            logger.warning(
                                "_prepare_turns_callable: turn_id=%d wide qa "
                                "re-resolution raised (%s) — falling back to "
                                "the narrow result",
                                turn_id,
                                exc,
                            )
                            wide = None
                        if wide is not None:
                            wide_slug = wide["participant_slug"]
                            wide_name = _display_name_for(participants, wide_slug)
                            # A wide-reject is silent (audit line below
                            # records it) — the WARNING is reserved for the
                            # final (narrow) verdict below.
                            if wide_name and crosscheck_slug(wide_name, mentions) != "reject":
                                winner, winner_name, winner_verdict = wide, wide_name, "ok"

                    if winner_verdict is None:
                        # Gate B (issue #321): cross-check the winner's
                        # canonical display_name against the chapter's own
                        # key_speakers/speakers rosters before persisting.
                        # Rejection withholds BOTH the DB write and the
                        # in-memory resolved_name patch — the incident this
                        # guards against is a wrong name reaching the
                        # thumbnail/title sidecar seam via the patch, not
                        # only via the DB write.
                        winner_verdict = crosscheck_slug(winner_name or "", mentions)

                    promoted = False
                    if winner_verdict == "reject":
                        logger.warning(
                            "_prepare_turns_callable: turn_id=%d chapter_id=%s "
                            "roster cross-check REJECTED slug=%r display_name=%r "
                            "against mentions=%r — write withheld",
                            turn_id,
                            turn.get("chapter_id"),
                            winner["participant_slug"],
                            winner_name,
                            mentions,
                        )
                    else:
                        # Gate A (issue #321): mark_turn_resolved now scopes the
                        # write to sibling rows sharing this turn's speaker_label.
                        db.mark_turn_resolved(
                            output_path,
                            winner["participant_slug"],
                            winner["confidence"],
                            "ai_srt_context",
                            turn_id,
                        )
                        # Patch in-memory so thumbnail/title steps see the real name.
                        if winner_name:
                            turn["resolved_name"] = winner_name
                            # Rule 4 (issue #282): promotion is sticky on
                            # promote_signal — never re-evaluated against
                            # the winner's name. Promote-only; never demotes.
                            if promote_signal:
                                db.promote_turn_type_to_qa(output_path)
                                promoted = True
                        logger.info(
                            "_prepare_turns_callable: turn_id=%d resolved → slug=%r",
                            turn_id,
                            winner["participant_slug"],
                        )

                    if promote_signal:
                        # issue #342: one audit INFO line per re-pass event.
                        logger.info(
                            "_prepare_turns_callable: qa_reresolution turn_id=%d "
                            "output_path=%s previous_name=%r narrow_slug=%r "
                            "wide_slug=%r winner_slug=%r verdict=%s promoted=%s",
                            turn_id,
                            output_path,
                            previous_name,
                            narrow_slug,
                            wide_slug,
                            winner["participant_slug"],
                            winner_verdict,
                            promoted,
                        )
                else:
                    logger.info(
                        "_prepare_turns_callable: turn_id=%d — no speaker resolved; continuing without attribution",
                        turn_id,
                    )
            else:
                logger.debug(
                    "_prepare_turns_callable: turn_id=%d already resolved (slug=%s conf=%.2f); skipping AI call",
                    turn_id,
                    turn.get("resolved_participant_slug"),
                    float(turn.get("speaker_resolution_confidence") or 0),
                )
        except Exception as exc:
            logger.warning(
                "_prepare_turns_callable: turn_id=%d resolution step failed (%s) — continuing without attribution",
                turn_id,
                exc,
            )

        try:
            # Step 0.5: VAD silence trim (issue #175).
            # Best-effort: trim_turn_silence_with_vad never raises and returns (0.0, 0.0) on
            # any failure, so preparation continues normally with the original file.
            # Applies uniformly to monologue and qa turns (no turn_type branching).
            trim_start, trim_end = trim_turn_silence_with_vad(output_path)

            # Step 1: Write subtitles.srt sidecar (window narrowed by VAD offsets).
            _write_turn_sidecars(turn, trim_start_secs=trim_start, trim_end_secs=trim_end)

            # Step 2: ffmpeg decode integrity check (validates trimmed or original MP4).
            rc = _run_ffmpeg_decode_check(output_path)
            if rc != 0:
                logger.warning(
                    "_prepare_turns_callable: ffmpeg decode check failed for turn_id=%d "
                    "(rc=%d) — prepared_at NOT set; will retry next night",
                    turn_id,
                    rc,
                )
                continue

            # Step 3: Atomic readiness flip — called LAST.
            db.mark_turn_prepared(turn_id)
            logger.info(
                "_prepare_turns_callable: turn_id=%d prepared successfully", turn_id
            )

        except Exception as exc:
            logger.warning(
                "_prepare_turns_callable: turn_id=%d preparation failed (%s) "
                "— prepared_at NOT set; will retry next night",
                turn_id,
                exc,
            )
            continue


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # No retries — failures self-heal on next nightly run
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description=(
        "PREPARE phase: generates sidecars and validates up to 2 speaker "
        "turn videos; sets prepared_at readiness gate before upload. "
        "Triggered by speaker_turn_videos chain (schedule=None)."
    ),
    schedule=None,
    start_date=datetime(2026, 8, 21),
    catchup=False,
    max_active_runs=1,
    tags=["congress", "speaker-turns", "prepare"],
) as dag:
    prepare_turns = PythonOperator(
        task_id="prepare_turns",
        python_callable=_prepare_turns_callable,
        pool="nas_ffmpeg",
        pool_slots=1,
    )
