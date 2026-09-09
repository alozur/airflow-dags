"""Failure-mode reconnaissance over the observed half of the title corpus.

This is the "look at your data" pass that precedes any rubric. It reports
aggregates and a handful of representative examples per kind, never the whole
corpus, so the output stays readable.

Nothing here scores a title. It surfaces candidate failure modes for a human
to confirm or reject during the labelling pass.

Usage::

    uv run python benchmarks/title_eval/analyze_observed.py
"""

from __future__ import annotations

import collections
import json
import pathlib
import re
import statistics

CORPUS = pathlib.Path(__file__).parent / "corpus" / "v1" / "observed.jsonl"

COURTESY = ("el señor ", "la señora ", "señor ", "señora ", "don ", "doña ")
GENERIC_LEAD = re.compile(
    r"^\s*(el|la)\s+(ministr[oa]|president[ea]|diputad[oa]|portavoz|gobierno|congreso|"
    r"parlamento|oposición|señor|señora)\b",
    re.IGNORECASE,
)


def load() -> list[dict]:
    """Read the corpus, keeping one row per published video.

    Grouped turn uploads produce several corpus items that share one video, so
    counting items rather than videos would over-weight those chapters.
    """
    seen: dict[str, dict] = {}
    for line in CORPUS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        video_id = item["context"].get("youtube_video_id")
        if not video_id or video_id in seen:
            continue
        seen[video_id] = {
            "kind": item["kind"],
            "title": item["baseline"]["published_title"] or "",
            "views": item["outcome"].get("views", 0),
        }
    return list(seen.values())


def first_word(title: str) -> str:
    match = re.match(r"[¿¡\"'\s]*([\wÁÉÍÓÚÜÑáéíóúüñ]+)", title)
    return match.group(1).lower() if match else ""


def main() -> None:
    records = load()
    print(f"distinct published videos: {len(records)}")
    print(f"by kind: {dict(collections.Counter(r['kind'] for r in records))}\n")

    for kind in ("chapter", "turn", "short"):
        subset = [r for r in records if r["kind"] == kind]
        if not subset:
            continue
        lengths = [len(r["title"]) for r in subset]
        views = sorted(r["views"] for r in subset)
        dupes = collections.Counter(r["title"] for r in subset)
        repeated = {t: c for t, c in dupes.items() if c > 1}

        print(f"--- {kind.upper()} (n={len(subset)}) ---")
        print(
            f"  chars      min={min(lengths)} median={int(statistics.median(lengths))} "
            f"max={max(lengths)} over_90={sum(1 for x in lengths if x > 90)}"
        )
        print(
            f"  views      median={int(statistics.median(views))} p90={views[int(len(views) * 0.9)]} "
            f"max={max(views)} zero={sum(1 for v in views if v == 0)}"
        )
        print(f"  duplicates {len(repeated)} titles reused across {sum(repeated.values())} videos")
        print(f"  leads with a generic role: {sum(1 for r in subset if GENERIC_LEAD.match(r['title']))}")
        print(
            f"  parliamentary courtesy form: {sum(1 for r in subset if any(w in r['title'].lower() for w in COURTESY))}"
        )
        print(f"  question mark: {sum(1 for r in subset if '?' in r['title'] or '¿' in r['title'])}")
        print(f"  top opening words: {collections.Counter(first_word(r['title']) for r in subset).most_common(6)}")
        print()

    print("=== titles reused across the most videos ===")
    for title, count in collections.Counter(r["title"] for r in records).most_common(8):
        if count < 2:
            break
        print(f"  {count:3d}x  {title[:86]}")

    print("\n=== highest view counts ===")
    for r in sorted(records, key=lambda x: -x["views"])[:10]:
        print(f"  {r['views']:7d}  [{r['kind']:7s}] {r['title'][:80]}")


if __name__ == "__main__":
    main()
