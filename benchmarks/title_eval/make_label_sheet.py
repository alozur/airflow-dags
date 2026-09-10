"""Draw a stratified labelling slice from the observed corpus.

The whole corpus is far too large to judge by hand, and judging a uniform
random sample would spend most of the effort on unremarkable titles. This
draws a slice that deliberately over-samples the shapes the reconnaissance
pass flagged, then pads with a random remainder so the rubric is not fitted
only to the failures.

The output is a JSONL sheet with an empty ``label`` block per row. Fill it in
by hand; ``verdict`` and ``why`` are the two fields that matter, and ``why``
matters more — it is what the rubric and the judge are derived from.

Usage::

    uv run python benchmarks/title_eval/make_label_sheet.py --size 60
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random
import re

CORPUS = pathlib.Path(__file__).parent / "corpus" / "v1" / "observed.jsonl"
DEFAULT_OUT = pathlib.Path(__file__).parent / "corpus" / "v1" / "label_sheet.jsonl"

GENERIC_LEAD = re.compile(
    r"^\s*(el|la)\s+(ministr[oa]|president[ea]|diputad[oa]|portavoz|gobierno|congreso|"
    r"parlamento|oposición|señor|señora)\b",
    re.IGNORECASE,
)
COURTESY = re.compile(r"\b(el señor|la señora|señor|señora|don|doña)\s+[A-ZÁÉÍÓÚÑ]", re.IGNORECASE)

# Each stratum names a shape worth judging deliberately. The label is carried
# into the sheet as `stratum` so a later disagreement can be traced back to
# the reason the row was picked.
STRATA = {
    "leads_with_generic_role": lambda item, t: bool(GENERIC_LEAD.match(t)),
    "parliamentary_courtesy_form": lambda item, t: bool(COURTESY.search(t)),
    "at_or_over_90_chars": lambda item, t: len(t) >= 90,
    "top_decile_views": lambda item, t: item["outcome"].get("views", 0) >= 15000,
    "zero_views": lambda item, t: item["outcome"].get("views", 0) == 0,
    "question_headline": lambda item, t: t.strip().startswith(("¿", "?")),
}
PER_STRATUM = 8


def load_unique() -> list[dict]:
    seen: dict[str, dict] = {}
    for line in CORPUS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        video_id = item["context"].get("youtube_video_id")
        title = item["baseline"].get("published_title")
        if not video_id or not title or video_id in seen:
            continue
        seen[video_id] = item
    return list(seen.values())


def duplicate_titles(items: list[dict]) -> set[str]:
    counts: dict[str, int] = {}
    for item in items:
        title = item["baseline"]["published_title"]
        counts[title] = counts.get(title, 0) + 1
    return {t for t, c in counts.items() if c > 1}


def sheet_row(item: dict, stratum: str) -> dict:
    return {
        "item_id": item["item_id"],
        "kind": item["kind"],
        "stratum": stratum,
        "title": item["baseline"]["published_title"],
        "views": item["outcome"].get("views"),
        "chapter_title": item["context"].get("chapter_title"),
        "topics": item["context"].get("topics"),
        "resolved_name": item["context"].get("resolved_name"),
        "youtube_url": f"https://www.youtube.com/watch?v={item['context']['youtube_video_id']}",
        "label": {
            # good | weak | bad — your call, not the model's
            "verdict": None,
            # The field that actually matters: what is wrong (or right) and why.
            "why": None,
            # Optional: a title you would have shipped instead.
            "better_title": None,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", type=int, default=60)
    parser.add_argument("--seed", type=int, default=510, help="fixed so the slice is reproducible")
    parser.add_argument("--out", type=pathlib.Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    items = load_unique()
    dupes = duplicate_titles(items)

    picked: dict[str, dict] = {}

    def take(candidates: list[dict], stratum: str, limit: int) -> None:
        rng.shuffle(candidates)
        for item in candidates:
            if len(picked) >= args.size:
                return
            if item["item_id"] in picked:
                continue
            picked[item["item_id"]] = sheet_row(item, stratum)
            limit -= 1
            if limit <= 0:
                return

    take([i for i in items if i["baseline"]["published_title"] in dupes], "reused_title", PER_STRATUM)
    for stratum, predicate in STRATA.items():
        take([i for i in items if predicate(i, i["baseline"]["published_title"])], stratum, PER_STRATUM)
    # Pad with an unbiased remainder so the rubric is not fitted to failures alone.
    take(list(items), "random", args.size)

    rows = list(picked.values())
    args.out.write_text(
        "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows),
        encoding="utf-8",
    )

    by_stratum: dict[str, int] = {}
    for row in rows:
        by_stratum[row["stratum"]] = by_stratum.get(row["stratum"], 0) + 1
    print(f"wrote {len(rows)} rows to {args.out}")
    print(json.dumps(by_stratum, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
