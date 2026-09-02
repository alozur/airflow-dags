"""Pure branch logic for marking chapter/turn uploads — issue #227.

Extracted verbatim from the deleted `PostgreSQLOperator`'s
`mark_chapters_uploaded` / `mark_turns_uploaded` dispatch branches so the
thin DAG-side callables in `youtube_upload_dag.py` stay a few lines each.

No Airflow imports here — kept import-safe / side-effect-free so the DagBag
walk never touches this module directly (only through the DAG callables).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def mark_chapter_uploads(db, upload_results: dict | None) -> dict:
    """Mark chapters as uploaded to YouTube after a successful upload run.

    Args:
        db: A `CongressionalVideoDB` instance (or compatible mock).
        upload_results: The `upload_results` XCom payload, shaped
            `{"upload_details": [{"chapter_id", "youtube_video_id", "success", "error"?}, ...]}`.

    Returns:
        `{"updated_chapters", "failed_updates", "recorded_failures", "details"}`.
    """
    if not upload_results or not upload_results.get("upload_details"):
        logger.info("No upload results to process")
        return {"updated_chapters": 0, "failed_updates": 0, "details": []}

    updated_count = 0
    failed_count = 0
    recorded_failures = 0
    details = []

    for upload_detail in upload_results["upload_details"]:
        chapter_id = upload_detail.get("chapter_id")
        youtube_video_id = upload_detail.get("youtube_video_id")
        success = upload_detail.get("success", False)

        if success and chapter_id and youtube_video_id:
            try:
                db.mark_chapter_uploaded(chapter_id, youtube_video_id)
                updated_count += 1
                details.append({
                    "chapter_id": chapter_id,
                    "youtube_video_id": youtube_video_id,
                    "status": "updated",
                })
                logger.info("Marked chapter %s as uploaded: %s", chapter_id, youtube_video_id)
            except Exception as e:
                failed_count += 1
                details.append({
                    "chapter_id": chapter_id,
                    "status": "failed",
                    "error": str(e),
                })
                logger.error("Failed to mark chapter %s: %s", chapter_id, e)
        elif not success:
            if chapter_id:
                try:
                    db.record_chapter_upload_failure(chapter_id, upload_detail.get("error"))
                    recorded_failures += 1
                    details.append({
                        "chapter_id": chapter_id,
                        "status": "failure_recorded",
                    })
                    logger.info("Recorded upload failure for chapter %s", chapter_id)
                except Exception as e:
                    failed_count += 1
                    details.append({
                        "chapter_id": chapter_id,
                        "status": "failed",
                        "error": str(e),
                    })
                    logger.error(
                        "Failed to RECORD upload failure for chapter %s in the "
                        "database (this attempt's failure count is now uncounted): %s",
                        chapter_id, e,
                    )
            else:
                logger.warning("Skipping failed upload with no chapter_id")
                details.append({
                    "chapter_id": chapter_id,
                    "status": "skipped",
                    "reason": "upload_failed_no_chapter_id",
                })
        elif success and (not chapter_id or not youtube_video_id):
            logger.warning(
                "Upload succeeded but missing fields: chapter_id=%s, youtube_video_id=%s. "
                "Full upload_detail: %s",
                chapter_id, youtube_video_id, upload_detail,
            )
            details.append({
                "chapter_id": chapter_id,
                "status": "skipped",
                "reason": f"missing_fields: chapter_id={chapter_id}, youtube_video_id={youtube_video_id}",
            })

    result = {
        "updated_chapters": updated_count,
        "failed_updates": failed_count,
        "recorded_failures": recorded_failures,
        "details": details,
    }
    logger.info(
        "Updated %d chapters, %d failed, %d failures recorded",
        updated_count, failed_count, recorded_failures,
    )
    return result


def mark_turn_uploads(db, upload_results: dict | None) -> dict:
    """Mark speaker turn videos as uploaded to YouTube after a successful upload run.

    Primary match key is `turn_id`; falls back to `output_path` (the
    `video_file` field) when `turn_id` is absent (issue #230).

    Args:
        db: A `CongressionalVideoDB` instance (or compatible mock).
        upload_results: The `upload_results` XCom payload, shaped
            `{"upload_details": [{"turn_id", "youtube_video_id", "video_file", "success"}, ...]}`.

    Returns:
        `{"updated_turns", "failed_updates", "thumbnail_markers",
        "thumbnail_marker_failures", "details"}`.
    """
    if not upload_results or not upload_results.get("upload_details"):
        logger.info("No turn upload results to process")
        return {"updated_turns": 0, "failed_updates": 0, "details": []}

    updated_count = 0
    failed_count = 0
    thumbnail_markers = 0
    thumbnail_marker_failures = 0
    details = []

    for upload_detail in upload_results["upload_details"]:
        turn_id = upload_detail.get("turn_id")
        youtube_video_id = upload_detail.get("youtube_video_id")
        output_path = upload_detail.get("video_file")
        success = upload_detail.get("success", False)

        if success and turn_id and youtube_video_id:
            try:
                db.mark_turns_uploaded(turn_id=turn_id, youtube_video_id=youtube_video_id)
                updated_count += 1
                details.append({
                    "turn_id": turn_id,
                    "youtube_video_id": youtube_video_id,
                    "status": "updated",
                    "matched_by": "turn_id",
                })
                logger.info("Marked turn %s as uploaded: %s", turn_id, youtube_video_id)
            except Exception as e:
                failed_count += 1
                details.append({
                    "turn_id": turn_id,
                    "status": "failed",
                    "error": str(e),
                })
                logger.error("Failed to mark turn %s: %s", turn_id, e)
        elif success and youtube_video_id and output_path:
            try:
                rows_matched = db.mark_turns_uploaded_by_output_path(output_path, youtube_video_id)
                if rows_matched:
                    updated_count += 1
                    details.append({
                        "turn_id": turn_id,
                        "youtube_video_id": youtube_video_id,
                        "status": "updated",
                        "matched_by": "output_path",
                        "output_path": output_path,
                    })
                    logger.info(
                        "Marked turn(s) at output_path=%r as uploaded: %s",
                        output_path, youtube_video_id,
                    )
                else:
                    details.append({
                        "turn_id": turn_id,
                        "status": "skipped",
                        "reason": "output_path_not_found",
                        "matched_by": "output_path",
                        "output_path": output_path,
                    })
                    logger.warning("No turn rows matched output_path=%r", output_path)
            except Exception as e:
                failed_count += 1
                details.append({
                    "turn_id": turn_id,
                    "status": "failed",
                    "error": str(e),
                    "matched_by": "output_path",
                    "output_path": output_path,
                })
                logger.error("Failed to mark turn(s) at output_path=%r: %s", output_path, e)
        else:
            details.append({
                "turn_id": turn_id,
                "status": "skipped",
                "reason": "upload_failed_or_missing_fields",
            })

        # Independent of the two success branches above. Identity check, not
        # truthiness -- thumbnail_success is FOUR-valued; only literal False
        # is a failure (issue #320, _unpublished_thumbnail_labels).
        if success and upload_detail.get("thumbnail_success") is False:
            try:
                rows = db.mark_turn_thumbnail_republish_needed(
                    output_path=output_path,
                    turn_id=turn_id,
                    error_message=upload_detail.get("thumbnail_error"),
                )
                thumbnail_markers += 1
                details.append({
                    "turn_id": turn_id,
                    "output_path": output_path,
                    "status": "thumbnail_republish_marked",
                    "rows": rows,
                })
            except Exception as e:
                thumbnail_marker_failures += 1
                details.append({
                    "turn_id": turn_id,
                    "output_path": output_path,
                    "status": "thumbnail_marker_failed",
                    "error": str(e),
                })
                logger.error(
                    "Failed to RECORD thumbnail republish marker for turn %s "
                    "(output_path=%r); now unrecorded: %s", turn_id, output_path, e,
                )

    result = {
        "updated_turns": updated_count,
        "failed_updates": failed_count,
        "thumbnail_markers": thumbnail_markers,
        "thumbnail_marker_failures": thumbnail_marker_failures,
        "details": details,
    }
    logger.info("Marked %d turns uploaded, %d failed", updated_count, failed_count)
    return result
