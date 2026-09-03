"""
Video Analytics Actions DAG (issue #102)

Write-capable child DAG triggered by video_analytics's terminal task after
every hourly snapshot collection. Reads NULL-action_taken snapshot rows,
decides per-row via the pure evaluate_action() function, records no-op
decisions (capped/cold_start/ok) directly, and for underperforming rows
within their lifetime cap, claims the row BEFORE any external call,
regenerates via the existing generic_thumbnail_generator DAG, publishes
with the upload-purpose OAuth token, and finalizes action_taken +
action_detail.

Tasks: select_candidates -> evaluate_candidates -> record_no_ops -> apply_actions

Token isolation: this DAG uses ONLY the 'upload'-purpose token. The
collector (video_analytics) uses ONLY the 'analytics' (read-only) token —
see that DAG's module docstring.

Claim-before-act (issue #102 design): apply_actions runs with retries=0.
Before any external call it claims the row via a rowcount-gated
UPDATE ... WHERE action_taken IS NULL (CongressionalVideoDB.claim_snapshot_action).
A killed/interrupted run leaves the row 'in_progress' — terminal for the
automated loop. Re-processing requires a maintainer to manually reset
action_taken to NULL. 'failed' is likewise terminal, not auto-retried.

`previous_brief` steering: since migration 043 (#292), video_thumbnails
persists each option's own finalized art-direction brief JSON in
``art_direction_brief``. When the chosen row carries a non-NULL brief,
apply_actions forwards it verbatim as ``previous_brief`` in the child
DAG's trigger conf, independent of the title checkpoint — art_direct()
injects the "REINTENTO" anti-convergence instruction from it. Historical
rows written before migration 043 have a NULL brief and degrade to
steering via `previous_archetype` and `previous_title` (at the title
checkpoint) only.
"""

import logging
import os
import time
from datetime import UTC, datetime, timedelta, timezone

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.models import XCom
from airflow.operators.python import PythonOperator

from congress_videos.config.analytics_config import TITLE_UPDATE_CHECKPOINTS
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path
from utils.env_loader import load_env_if_local

load_env_if_local()

# Upload-purpose token — this DAG NEVER uses the read-only 'analytics' token.
_UPLOAD_TOKEN_FILE = os.getenv(
    "YOUTUBE_TOKEN_FILE",
    resolve_token_path(DEFAULT_CHANNEL, "upload"),
)

_THUMBNAIL_DAG_ID = "generic_thumbnail_generator"
_THUMBNAIL_RESULT_TASK_ID = "thumbnail_result"

_NO_OP_DECISIONS = {"capped", "cold_start", "ok"}
_REGENERATE_DECISIONS = {"thumbnail_regenerated", "thumbnail_and_title_regenerated"}

# Bounded poll loop for the triggered thumbnail DAG (10s interval).
_THUMBNAIL_POLL_INTERVAL_SECONDS = 10
_THUMBNAIL_MAX_POLLS = int(os.getenv("ANALYTICS_ACTION_THUMBNAIL_MAX_POLLS", "180"))  # ~30 min
# Log a progress line every N polls (~60s at a 10s interval) so a long wait
# is visible while it happens, not only in hindsight (issue #311, D7).
_POLL_PROGRESS_EVERY = 6


def _run_select_candidates(ti):
    """Return NULL-action_taken snapshot rows joined to conf fields.

    Pushes XCom key 'candidates'.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB()
    result = db.get_unactioned_snapshots()
    logging.info("video_analytics_actions: %d unactioned candidates found", len(result))
    ti.xcom_push(key="candidates", value=result)
    return result


def _run_evaluate_candidates(ti):
    """Decide the action_taken literal for every candidate via evaluate_action().

    Reads the 'candidates' XCom, fetches checkpoint medians and per-video
    action history once (batched), then evaluates each row. Pushes XCom key
    'decisions' — each candidate dict extended with decision/views/
    median_views/sample_size.
    """
    from congress_videos.modules.database import CongressionalVideoDB
    from congress_videos.modules.video_analytics import evaluate_action

    candidates = ti.xcom_pull(key="candidates") or []
    if not candidates:
        logging.info("video_analytics_actions: no candidates to evaluate")
        ti.xcom_push(key="decisions", value=[])
        return []

    db = CongressionalVideoDB()
    medians = db.get_checkpoint_view_medians()
    youtube_video_ids = list({c["youtube_video_id"] for c in candidates})
    history = db.get_video_action_history(youtube_video_ids)

    decisions = []
    for candidate in candidates:
        checkpoint = candidate["checkpoint"]
        median_info = medians.get(checkpoint) or {}
        median_views = median_info.get("median_views") or 0
        sample_size = median_info.get("sample_size") or 0
        views = (candidate.get("metrics") or {}).get("views") or 0
        prior_actions = history.get(
            candidate["youtube_video_id"], {"thumbnail": 0, "title": 0}
        )

        decision = evaluate_action(
            views=views,
            median_views=median_views,
            sample_size=sample_size,
            checkpoint=checkpoint,
            prior_actions=prior_actions,
        )
        decisions.append({
            **candidate,
            "decision": decision,
            "views": views,
            "median_views": float(median_views) if median_views else 0.0,
            "sample_size": sample_size,
        })
        logging.info(
            "video_analytics_actions: snapshot_id=%s yt_id=%s checkpoint=%s "
            "decision=%s views=%s median=%s sample=%s",
            candidate.get("snapshot_id"),
            candidate.get("youtube_video_id"),
            checkpoint,
            decision,
            views,
            median_views,
            sample_size,
        )

    ti.xcom_push(key="decisions", value=decisions)
    return decisions


def _no_op_detail(row: dict) -> dict:
    return {
        "checkpoint": row["checkpoint"],
        "views": row["views"],
        "median_views": row["median_views"],
        "sample_size": row["sample_size"],
    }


def _run_record_no_ops(ti):
    """Persist capped/cold_start/ok decisions directly — no claim, no
    external call. Runs BEFORE apply_actions so this cheap bookkeeping
    survives an action failure downstream.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB()
    decisions = ti.xcom_pull(key="decisions") or []
    no_ops = [d for d in decisions if d["decision"] in _NO_OP_DECISIONS]

    for row in no_ops:
        db.mark_action_taken(
            snapshot_id=row["snapshot_id"],
            action=row["decision"],
            detail=_no_op_detail(row),
        )

    logging.info("video_analytics_actions: recorded %d no-op decisions", len(no_ops))
    return {"recorded_no_ops": len(no_ops)}


def _snapshot_age_days(collected_at) -> int | None:
    """Whole days between a snapshot's measurement and now, or None.

    The first production run applied 13-day-old measurements, so how stale
    the decision's input was belongs in the log next to the wait. Note that
    ``collected_at`` is only projected as of issue #311 — rows built before
    that, and every hand-built test fixture, pass None and must degrade
    rather than crash.
    """
    if not isinstance(collected_at, datetime):
        return None
    # Match awareness: datetime.now(None) is naive, datetime.now(tz) is aware.
    return (datetime.now(collected_at.tzinfo) - collected_at).days


def _poll_thumbnail_dag_run(
    dag_run,
    *,
    snapshot_id=None,
    checkpoint=None,
    snapshot_age_days=None,
) -> dict:
    """Poll a triggered generic_thumbnail_generator run to completion.

    Bounded by _THUMBNAIL_MAX_POLLS — never blocks forever. Returns
    {"success": False, "error": ...} on failure/timeout, or the child DAG's
    thumbnail_result XCom dict on success.

    Logs on entry and every _POLL_PROGRESS_EVERY polls (issue #311, D7): a
    bounded ~30-minute loop that prints nothing is indistinguishable from a
    hung task while it is happening, which is how #311's latency went
    unnoticed until someone read the timestamps afterwards. The context
    arguments are keyword-only and optional so the loop stays callable
    without them.
    """
    child_run_id = dag_run.run_id

    logging.info(
        "video_analytics_actions: polling thumbnail DAG run %s "
        "(snapshot_id=%s checkpoint=%s snapshot_age_days=%s) — waiting up to %ds",
        child_run_id,
        snapshot_id,
        checkpoint,
        snapshot_age_days,
        _THUMBNAIL_MAX_POLLS * _THUMBNAIL_POLL_INTERVAL_SECONDS,
    )

    for poll in range(_THUMBNAIL_MAX_POLLS):
        time.sleep(_THUMBNAIL_POLL_INTERVAL_SECONDS)
        dag_run.refresh_from_db()
        if dag_run.state not in ("success", "failed"):
            if (poll + 1) % _POLL_PROGRESS_EVERY == 0:
                logging.info(
                    "video_analytics_actions: still waiting on thumbnail DAG run %s "
                    "— state=%s elapsed=%ds (snapshot_id=%s)",
                    child_run_id,
                    dag_run.state,
                    (poll + 1) * _THUMBNAIL_POLL_INTERVAL_SECONDS,
                    snapshot_id,
                )
            continue

        if dag_run.state != "success":
            return {
                "success": False,
                "error": f"thumbnail DAG run {child_run_id} failed",
            }

        result = XCom.get_one(
            dag_id=_THUMBNAIL_DAG_ID,
            task_id=_THUMBNAIL_RESULT_TASK_ID,
            key="return_value",
            run_id=child_run_id,
        )
        if not (
            isinstance(result, dict)
            and result.get("success") is True
            and isinstance(result.get("output_path"), str)
            and result["output_path"]
        ):
            return {
                "success": False,
                "error": f"thumbnail DAG run {child_run_id} returned no valid result",
            }
        return result

    return {
        "success": False,
        "error": f"thumbnail DAG run {child_run_id} timed out after "
        f"{_THUMBNAIL_MAX_POLLS * _THUMBNAIL_POLL_INTERVAL_SECONDS}s",
    }


def _apply_one_action(db, row: dict, run_id: str) -> dict:
    """Claim, regenerate, publish, and finalize a single regenerate-decision row.

    Returns {"snapshot_id", "youtube_video_id", "action", "applied", "error"}.
    ``applied`` is quad-valued per key ("thumbnail"/"title"): True (published),
    False (attempted and failed), None (never attempted) — see issue #317.
    """
    snapshot_id = row["snapshot_id"]

    claimed = db.claim_snapshot_action(snapshot_id)
    if not claimed:
        logging.info(
            "video_analytics_actions: snapshot_id=%s already claimed — skipping",
            snapshot_id,
        )
        return {"snapshot_id": snapshot_id, "action": "skipped_already_claimed"}

    # Snapshot prior state BEFORE triggering — persist_results upserts in
    # place on the child DAG's success and would destroy this otherwise.
    chosen = db.get_chosen_thumbnail(row["chapter_id"]) or {}
    prior = {
        "archetype": chosen.get("archetype"),
        "title": chosen.get("openai_title"),
        "local_path": chosen.get("local_path"),
    }

    detail: dict = {
        "checkpoint": row["checkpoint"],
        "views": row["views"],
        "median_views": row["median_views"],
        "sample_size": row["sample_size"],
        "ratio": (row["views"] / row["median_views"]) if row["median_views"] else None,
        "prior": prior,
        "new": {},
        "collisions": {},
        "applied": {"thumbnail": None, "title": None},
    }

    def _result(action: str) -> dict:
        return {
            "snapshot_id": snapshot_id,
            "youtube_video_id": row["youtube_video_id"],
            "action": action,
            "applied": dict(detail["applied"]),  # copy: never alias the recorded detail
            "error": detail.get("error"),
            "failure": detail.get("failure"),
        }

    is_title_checkpoint = row["checkpoint"] in TITLE_UPDATE_CHECKPOINTS

    child_conf = {
        "youtube_video_id": str(row["youtube_video_id"]),
        "chapter_id": row["chapter_id"],
        "debate_summary": (
            f"{row.get('chapter_title', '')}\n{row.get('description', '')}"
            if row.get("description")
            else row.get("chapter_title", "")
        ),
        "session": (
            f"Sesión {row['session_number']}"
            if row.get("session_number") is not None
            else (str(row.get("session_date")) if row.get("session_date") else None)
        ),
        "domain": "congreso",
        "slug": row.get("resolved_participant_slug"),
        "key_speakers": row.get("key_speakers") or [],
        "previous_archetype": chosen.get("archetype"),
    }
    prior_brief = chosen.get("art_direction_brief")
    if isinstance(prior_brief, dict) and prior_brief:
        child_conf["previous_brief"] = prior_brief
    if is_title_checkpoint and chosen.get("openai_title"):
        child_conf["previous_title"] = chosen["openai_title"]

    child_run_id = f"analytics_action_{snapshot_id}_{run_id}"
    try:
        dag_run = trigger_dag_api(
            dag_id=_THUMBNAIL_DAG_ID,
            conf=child_conf,
            run_id=child_run_id,
        )
    except Exception as exc:
        logging.warning(
            "video_analytics_actions: could not trigger thumbnail DAG for "
            "snapshot_id=%s: %s",
            snapshot_id,
            exc,
        )
        detail["error"] = str(exc)
        # stage="trigger" (D6): the exception carries no HTTP classification —
        # trigger_dag_api failures are Airflow-internal, not YouTube errors.
        detail["failure"] = {"stage": "trigger", "permanent": None, "status": None, "reason": None}
        db.mark_action_taken(snapshot_id=snapshot_id, action="failed", detail=detail)
        return _result("failed")

    detail["thumbnail_dag_run_id"] = dag_run.run_id

    thumb_result = _poll_thumbnail_dag_run(
        dag_run,
        snapshot_id=snapshot_id,
        checkpoint=row["checkpoint"],
        snapshot_age_days=_snapshot_age_days(row.get("collected_at")),
    )
    if not thumb_result.get("success"):
        detail["error"] = thumb_result.get("error")
        # stage="thumbnail_dag" (D6): the child DAG failed or timed out
        # before any YouTube call was ever made — nothing to classify.
        detail["failure"] = {"stage": "thumbnail_dag", "permanent": None, "status": None, "reason": None}
        db.mark_action_taken(snapshot_id=snapshot_id, action="failed", detail=detail)
        return _result("failed")

    from utils.youtube_helpers import (
        get_authenticated_youtube_service,
        set_thumbnail_for_video,
        update_video_title,
    )

    youtube = get_authenticated_youtube_service(_UPLOAD_TOKEN_FILE)

    thumb_publish = set_thumbnail_for_video(
        youtube, row["youtube_video_id"], thumb_result["output_path"]
    )
    detail["new"]["local_path"] = thumb_result.get("output_path")
    detail["new"]["title"] = thumb_result.get("title")
    detail["youtube"] = {"thumbnail": thumb_publish}
    detail["applied"]["thumbnail"] = thumb_publish.get("success") is True

    if not thumb_publish.get("success"):
        detail["error"] = thumb_publish.get("error")
        # stage="youtube_thumbnail" (D6): thumb_publish carries
        # classify_youtube_error's output on real HttpErrors (issue #311).
        detail["failure"] = {
            "stage": "youtube_thumbnail",
            "permanent": thumb_publish.get("permanent"),
            "status": thumb_publish.get("status"),
            "reason": thumb_publish.get("reason"),
        }
        db.mark_action_taken(snapshot_id=snapshot_id, action="failed", detail=detail)
        return _result("failed")

    if is_title_checkpoint:
        try:
            title_publish = update_video_title(
                youtube, row["youtube_video_id"], thumb_result.get("title")
            )
        except ValueError as exc:
            # Issue #317: update_video_title raises pre-API on a blank title.
            # Convert to the SAME recorded-failure shape every other publish
            # error already takes, so the batch finishes and EVERY claimed
            # row is recorded. Per-row isolation, not a swallow: the row is
            # durably marked "failed" with applied.title=False below.
            # Turning the whole task RED on that record is issue #311's.
            title_publish = {"success": False, "error": f"blank title refused: {exc}"}
        detail["youtube"]["title"] = title_publish
        detail["applied"]["title"] = title_publish.get("success") is True
        if not title_publish.get("success"):
            detail["error"] = title_publish.get("error")
            # stage="youtube_title" (D6): the blank-title ValueError guard
            # (#317) builds a dict without classification keys — .get()
            # falls back to None, correctly landing it in the "unclassified"
            # bucket of _action_failure_problems rather than crashing.
            detail["failure"] = {
                "stage": "youtube_title",
                "permanent": title_publish.get("permanent"),
                "status": title_publish.get("status"),
                "reason": title_publish.get("reason"),
            }
            db.mark_action_taken(snapshot_id=snapshot_id, action="failed", detail=detail)
            return _result("failed")

    new_chosen = db.get_chosen_thumbnail(row["chapter_id"]) or {}
    detail["new"]["archetype"] = new_chosen.get("archetype")
    detail["collisions"] = {
        "archetype": (
            prior["archetype"] is not None
            and new_chosen.get("archetype") == prior["archetype"]
        ),
        "title": (
            is_title_checkpoint
            and prior["title"] is not None
            and thumb_result.get("title") == prior["title"]
        ),
    }

    final_action = row["decision"]
    db.mark_action_taken(snapshot_id=snapshot_id, action=final_action, detail=detail)
    return _result(final_action)


def _action_failure_problems(results: list) -> list:
    """Describe every failed analytics action worth failing apply_actions.

    Returns finished operator-facing sentences; an empty list means clean.
    Rows with ``action != "failed"`` (including ``"skipped_already_claimed"``)
    are not failures. Permanent and transient/unclassified failures get
    SEPARATE sentences — they need different remedies and must stay
    separately diagnosable (#332 D2). A ``"failed"`` row missing its
    ``failure`` key is still reported, as unclassified — ``.get()`` on ``{}``
    renders every field as ``None`` rather than crashing.
    """
    permanent_labels = []
    other_labels = []
    for row in results:
        if row.get("action") != "failed":
            continue
        failure = row.get("failure") or {}
        label = (
            f"snapshot_id={row.get('snapshot_id')} stage={failure.get('stage')} "
            f"status={failure.get('status')} reason={failure.get('reason')}"
        )
        if failure.get("permanent") is True:
            permanent_labels.append(label)
        else:
            other_labels.append(label)

    problems = []
    if permanent_labels:
        problems.append(
            f"{len(permanent_labels)} analytics action(s) failed PERMANENTLY and are now "
            f"action_taken='failed' (terminal — a human must reset the row; these will NOT "
            f"clear on their own): {', '.join(permanent_labels)}."
        )
    if other_labels:
        problems.append(
            f"{len(other_labels)} analytics action(s) failed transiently or unclassified: "
            f"{', '.join(other_labels)}."
        )
    return problems


def _run_apply_actions(ti, **context):
    """Claim, regenerate, publish, and finalize every regenerate-decision row.

    retries=0 at the task level (see DAG definition) — claim_snapshot_action
    is the actual safety net against a killed/retried run causing a double
    regeneration.

    Every claimed row is processed and recorded FIRST (issue #311, D1) —
    a single exception is raised only ONCE, after the loop, naming every
    failed row. Aborting mid-loop would permanently hide later findings,
    since 'failed' is a terminal action_taken.
    """
    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB()
    decisions = ti.xcom_pull(key="decisions") or []
    to_apply = [d for d in decisions if d["decision"] in _REGENERATE_DECISIONS]

    run_id = context.get("run_id", "manual")
    results = [_apply_one_action(db, row, run_id) for row in to_apply]

    logging.info("video_analytics_actions: applied %d action(s)", len(results))

    problems = _action_failure_problems(results)
    if problems:
        raise Exception(" | ".join(problems))

    return {"applied": len(results), "results": results}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "video_analytics_actions",
    default_args=default_args,
    description=(
        "Write-capable child DAG: evaluates underperformance and regenerates "
        "thumbnail/title for eligible videos (issue #102)."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["congress", "youtube", "analytics", "actions"],
) as dag:

    t0_select = PythonOperator(
        task_id="select_candidates",
        python_callable=_run_select_candidates,
    )

    t1_evaluate = PythonOperator(
        task_id="evaluate_candidates",
        python_callable=_run_evaluate_candidates,
    )

    t2_record_no_ops = PythonOperator(
        task_id="record_no_ops",
        python_callable=_run_record_no_ops,
    )

    # retries=0: claim-before-act makes an Airflow-level retry unsafe — a
    # retried run would re-select rows the first attempt already claimed as
    # 'in_progress' were it not for the DB-level rowcount gate; more
    # importantly a retry after a partial success (thumbnail published, task
    # then failed before finalizing) must NEVER re-trigger a second
    # publication. See claim_snapshot_action / Claim-before-act retry
    # semantics in the design.
    t3_apply = PythonOperator(
        task_id="apply_actions",
        python_callable=_run_apply_actions,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    t0_select >> t1_evaluate >> t2_record_no_ops >> t3_apply
