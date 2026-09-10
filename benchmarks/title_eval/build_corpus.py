"""Assemble the versioned title-evaluation corpus from raw production extracts.

The corpus has two halves that answer two different questions:

``observed.jsonl``
    Every title this channel has actually published, joined to its live
    YouTube outcome. This half is *not* replayable — it exists to mine
    failure modes and to ground the scoring rubric in real copy rather than
    in taste alone.

``replay.jsonl``
    Items whose generator inputs are frozen, so a candidate prompt can be
    re-run on exactly the input a historical title was produced from and
    compared against a baseline. Only inputs that survive in the database
    qualify; see ``REPLAY_GAPS`` in the README for what does not.

Both halves carry an empty ``labels`` slot. Nothing in this module scores a
title: the human labelling pass fills that slot, and the judge is calibrated
against it afterwards.

Usage::

    uv run python benchmarks/title_eval/build_corpus.py \
        --raw-dir <dir with the extract outputs> \
        --out-dir benchmarks/title_eval/corpus/v1
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
from typing import Any

# Generator identity per content kind, as wired on main (issue #510 mapping).
# Long-form chapter uploads have been unreachable from the scheduled DAG since
# issue #171 made selection turn-only, so their items are historical evidence
# rather than a live target.
GENERATORS = {
    "chapter": "thumbnail_generation.generate_title (legacy path, unreachable since #171)",
    "turn": "thumbnail_generation.generate_title",
    "short": "reap_shorts_uploader_dag._generate_metadata",
}


def read_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: pathlib.Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_published(raw_dir: pathlib.Path) -> dict[str, dict]:
    """Index the fetched YouTube metadata by video id, keeping the first hit.

    Several turn rows share one grouped upload, so the id list fed to the API
    contains duplicates and the same video can come back in two batches.
    """
    published: dict[str, dict] = {}
    for item in read_jsonl(raw_dir / "published_titles.jsonl"):
        published.setdefault(item["youtube_video_id"], item)
    return published


def outcome_of(published: dict | None) -> dict:
    if not published:
        return {}
    return {
        "views": int(published.get("view_count") or 0),
        "likes": int(published.get("like_count") or 0),
        "comments": int(published.get("comment_count") or 0),
        "published_at": published.get("published_at"),
        "duration": published.get("duration"),
    }


def debate_summary(chapter_title: str | None, description: str | None) -> str | None:
    """Rebuild the generator's ``summary`` argument.

    Mirrors ``_prepare_thumbnail_config`` in ``youtube_upload_dag``: title and
    description joined by a newline, title alone when there is no description.
    """
    if not chapter_title:
        return None
    return f"{chapter_title}\n{description}" if description else chapter_title


def build_chapter_items(raw_dir: pathlib.Path, published: dict[str, dict]) -> list[dict]:
    items = []
    for row in read_json(raw_dir / "chapter_context.json"):
        video_id = row.get("youtube_video_id")
        live = published.get(video_id)
        summary = debate_summary(row.get("chapter_title"), row.get("chapter_description"))
        chosen = row.get("chosen_option") or {}
        # `best` is the Pikzels response the generator was handed. It is only
        # recoverable for chapters whose chosen thumbnail row survived, so the
        # rest are observational even though their summary is intact.
        best = (
            {
                "style": chosen.get("style"),
                "label": chosen.get("label"),
                "prompt": chosen.get("prompt"),
                "archetype": chosen.get("archetype"),
            }
            if chosen.get("prompt")
            else None
        )
        items.append(
            {
                "item_id": f"chapter-{row['chapter_id']}",
                "kind": "chapter",
                "generator": GENERATORS["chapter"],
                "replayable": bool(summary and best),
                "inputs": {
                    "summary": summary,
                    "best": best,
                    "key_speakers": row.get("key_speakers"),
                    "participant_slug": row.get("resolved_participant_slug"),
                    # A rolling `LIMIT 5` window at generation time; the
                    # as-of set was never persisted, so replays must pass an
                    # explicit list rather than trust this null.
                    "sibling_titles": None,
                    "forbidden_title": None,
                },
                "baseline": {
                    "stored_title": chosen.get("openai_title"),
                    "published_title": (live or {}).get("published_title"),
                },
                "context": {
                    "chapter_id": row["chapter_id"],
                    "source_video_id": row.get("source_video_id"),
                    "youtube_video_id": video_id,
                    "chapter_title": row.get("chapter_title"),
                    "topics": row.get("topics"),
                    "speakers": row.get("speakers"),
                    "duration_minutes": row.get("duration_minutes"),
                    "relevance_score": row.get("relevance_score"),
                    "session_date": row.get("session_date"),
                    "session_number": row.get("session_number"),
                },
                "outcome": outcome_of(live),
                "labels": None,
            }
        )
    return items


def build_turn_items(raw_dir: pathlib.Path, published: dict[str, dict]) -> list[dict]:
    """One item per turn row.

    Turn rows outnumber turn videos because grouped uploads publish several
    consecutive turns as a single video, so the same ``youtube_video_id`` and
    published title legitimately repeat across items.
    """
    items = []
    for row in read_json(raw_dir / "turn_context.json"):
        video_id = row.get("youtube_video_id")
        live = published.get(video_id)
        summary = debate_summary(row.get("chapter_title"), row.get("chapter_description"))
        # Turn-path key_speakers: the turn's own resolved name when present,
        # otherwise whatever the chapter carries.
        resolved_name = row.get("resolved_name")
        key_speakers = [resolved_name] if resolved_name else row.get("key_speakers")
        items.append(
            {
                "item_id": f"turn-{row['turn_id']}",
                "kind": "turn",
                "generator": GENERATORS["turn"],
                # `best` comes from a live Pikzels call per upload and is not
                # stored per turn, so no turn item is replayable end to end
                # without re-running thumbnail generation.
                "replayable": False,
                "inputs": {
                    "summary": summary,
                    "best": None,
                    "key_speakers": key_speakers,
                    "participant_slug": row.get("resolved_participant_slug") or row.get("chapter_participant_slug"),
                    "sibling_titles": None,
                    "forbidden_title": None,
                },
                "baseline": {
                    "stored_title": None,
                    "published_title": (live or {}).get("published_title"),
                },
                "context": {
                    "turn_id": row["turn_id"],
                    "chapter_id": row.get("chapter_id"),
                    "youtube_video_id": video_id,
                    "turn_type": row.get("turn_type"),
                    "speaker_label": row.get("speaker_label"),
                    "resolved_name": resolved_name,
                    "speaker_resolution_method": row.get("speaker_resolution_method"),
                    "speaker_resolution_confidence": row.get("speaker_resolution_confidence"),
                    "chapter_title": row.get("chapter_title"),
                    "topics": row.get("topics"),
                    "start_seconds": row.get("start_seconds"),
                    "end_seconds": row.get("end_seconds"),
                    "interest_score": row.get("interest_score"),
                    "is_procedural": row.get("is_procedural"),
                },
                "outcome": outcome_of(live),
                "labels": None,
            }
        )
    return items


def build_short_items(raw_dir: pathlib.Path, published: dict[str, dict]) -> list[dict]:
    items = []
    for row in read_json(raw_dir / "short_context.json"):
        video_id = row.get("youtube_video_id")
        live = published.get(video_id)
        items.append(
            {
                "item_id": f"short-{row['short_id']}",
                "kind": "short",
                "generator": GENERATORS["short"],
                # The shorts prompt is driven by a Whisper transcript produced
                # on the fly from the clip and never persisted, so no shorts
                # item can be replayed from database state alone.
                "replayable": False,
                "inputs": {
                    "chapter_title": row.get("chapter_title"),
                    "topics": row.get("topics"),
                    "mentioned_participant_slugs": row.get("mentioned_participant_slugs"),
                    "scoring_reasoning": row.get("scoring_reasoning"),
                    "resolved_name": row.get("turn_resolved_name"),
                    "transcript": None,
                },
                "baseline": {
                    "stored_title": None,
                    "published_title": (live or {}).get("published_title"),
                },
                "context": {
                    "short_id": row["short_id"],
                    "chapter_id": row.get("chapter_id"),
                    "turn_id": row.get("turn_id"),
                    "youtube_video_id": video_id,
                    "reap_virality_score": row.get("reap_virality_score"),
                    "pretrim_start_secs": row.get("pretrim_start_secs"),
                    "pretrim_end_secs": row.get("pretrim_end_secs"),
                    "created_at": row.get("created_at"),
                },
                "outcome": outcome_of(live),
                "labels": None,
            }
        )
    return items


def write_jsonl(path: pathlib.Path, rows: list[dict]) -> str:
    payload = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=pathlib.Path, required=True)
    parser.add_argument("--out-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    published = load_published(args.raw_dir)

    items = (
        build_chapter_items(args.raw_dir, published)
        + build_turn_items(args.raw_dir, published)
        + build_short_items(args.raw_dir, published)
    )

    observed = [i for i in items if i["baseline"]["published_title"]]
    replay = [i for i in items if i["replayable"]]

    observed_hash = write_jsonl(args.out_dir / "observed.jsonl", observed)
    replay_hash = write_jsonl(args.out_dir / "replay.jsonl", replay)

    def by_kind(rows: list[dict]) -> dict[str, int]:
        return {k: sum(1 for r in rows if r["kind"] == k) for k in GENERATORS}

    manifest = {
        "corpus_version": "v1",
        "issue": 510,
        "extracted_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "source": "production schema of the congress_videos database, plus the YouTube Data API v3",
        "generators": GENERATORS,
        "observed": {
            "count": len(observed),
            "by_kind": by_kind(observed),
            "distinct_videos": len({i["context"]["youtube_video_id"] for i in observed}),
            "sha256": observed_hash,
        },
        "replay": {
            "count": len(replay),
            "by_kind": by_kind(replay),
            "sha256": replay_hash,
        },
        "labels": "empty on purpose — the human labelling pass fills the `labels` slot",
    }
    (args.out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
